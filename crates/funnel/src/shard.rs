//! A shard thread owns one aggregator and one limiter, so the hot path takes
//! no locks. The router sends each series to exactly one shard.

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};

use crate::aggregator::{self, Aggregator, Bucket, Reject};
use crate::limiter::{self, Budgets, Limiter, TenantReport};
use crate::protocol::{parse_line, take_tenant};
use crate::series::{normalize_tags, series_id};
use crate::stats::Stats;
use crate::unix_now;

/// Lines for one shard, as text, so routing needs no per-line allocation.
pub struct Batch {
    pub now: u64,
    pub default_tenant: Arc<str>,
    pub text: String,
    pub lines: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub aggregator: aggregator::Config,
    pub limiter: limiter::Config,
    pub flush_every: Duration,
    /// How often every open window is flushed early as a partial bucket.
    /// Windows the aggregator still holds are lost in a crash and invisible
    /// to queries, so this bounds both. Readers and compaction merge the
    /// partial buckets, which every aggregate here allows. `None` waits for
    /// windows to close.
    pub checkpoint_every: Option<Duration>,
}

pub struct Shard {
    pub batches: Sender<Batch>,
    pub handle: JoinHandle<()>,
}

/// What a shard last reported, for `/stats`.
#[derive(Clone, Debug, Default)]
pub struct ShardReport {
    pub tenants: Vec<TenantReport>,
    pub open_buckets: usize,
    pub memory_bytes: usize,
}

/// Latest report from each shard, indexed by shard.
pub type Reports = Arc<Mutex<Vec<ShardReport>>>;

fn report(agg: &Aggregator, limiter: &Limiter) -> ShardReport {
    ShardReport {
        tenants: limiter.report(),
        open_buckets: agg.open_buckets(),
        memory_bytes: agg.memory_bytes(),
    }
}

pub fn spawn(
    index: usize,
    config: Config,
    queue: usize,
    budgets: Arc<Budgets>,
    out: Sender<Vec<Bucket>>,
    stats: Arc<Stats>,
    reports: Reports,
) -> Shard {
    let (tx, rx) = crossbeam_channel::bounded(queue);
    let limiter = Limiter::new(config.limiter, budgets);
    let handle = std::thread::Builder::new()
        .name(format!("shard-{index}"))
        .spawn(move || run(index, config, limiter, rx, out, stats, reports))
        .expect("spawn shard thread");
    Shard {
        batches: tx,
        handle,
    }
}

fn run(
    index: usize,
    config: Config,
    mut limiter: Limiter,
    rx: Receiver<Batch>,
    out: Sender<Vec<Bucket>>,
    stats: Arc<Stats>,
    reports: Reports,
) {
    let mut agg = Aggregator::new(config.aggregator);
    let mut next_flush = Instant::now() + config.flush_every;
    let mut next_checkpoint = config.checkpoint_every.map(|c| Instant::now() + c);
    loop {
        match rx.recv_deadline(next_flush) {
            Ok(batch) => process(&batch, &mut agg, &mut limiter, &stats),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if Instant::now() >= next_flush {
            next_flush += config.flush_every;
            let now = unix_now();
            limiter.tick(now);
            let buckets = match (next_checkpoint, config.checkpoint_every) {
                (Some(at), Some(every)) if Instant::now() >= at => {
                    next_checkpoint = Some(at + every);
                    agg.flush_all()
                }
                _ => agg.flush(now),
            };
            reports.lock().unwrap()[index] = report(&agg, &limiter);
            if !buckets.is_empty() && out.send(buckets).is_err() {
                return;
            }
        }
    }
    let _ = out.send(agg.flush_all());
    reports.lock().unwrap()[index] = report(&agg, &limiter);
}

/// Applies one batch to a shard's aggregator and limiter. Public for benchmarks.
pub fn process(batch: &Batch, agg: &mut Aggregator, limiter: &mut Limiter, stats: &Stats) {
    let (mut accepted, mut cardinality, mut too_old, mut future, mut overflow) = (0, 0, 0, 0, 0);
    for raw in batch.text.lines() {
        // The router already validated these lines.
        let Ok(mut line) = parse_line(raw) else {
            continue;
        };
        let Ok(tenant) = take_tenant(&mut line) else {
            continue;
        };
        let tenant = tenant.unwrap_or(&batch.default_tenant);
        normalize_tags(&mut line.tags);
        let id = series_id(tenant, line.name, line.kind, &line.tags);
        // Time checks come first so that a point about to be rejected does
        // not claim a series slot.
        if let Err(reject) = agg.check_time(batch.now, &line) {
            match reject {
                Reject::TooOld => too_old += 1,
                _ => future += 1,
            }
            continue;
        }
        if !limiter.check(batch.now, tenant, id) {
            cardinality += 1;
            continue;
        }
        match agg.insert_prepared(batch.now, id, tenant, &line) {
            Ok(()) => accepted += 1,
            Err(Reject::TooOld) => too_old += 1,
            Err(Reject::Future) => future += 1,
            Err(Reject::Overflow) => overflow += 1,
        }
    }
    Stats::add(&stats.lines_accepted, accepted);
    Stats::add(&stats.rejected_cardinality, cardinality);
    Stats::add(&stats.rejected_too_old, too_old);
    Stats::add(&stats.rejected_future, future);
    Stats::add(&stats.rejected_overflow, overflow);
}
