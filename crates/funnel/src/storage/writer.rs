//! Moves flushed buckets from the shards into the store.
//!
//! Buckets wait in memory for one segment (10 seconds by default) and are
//! then written as one level-0 file per tenant. A crash loses at most that
//! segment. While they wait, queries read them from [`Pending`], so results
//! include them; they leave `Pending` only once their file is committed, so
//! there is no moment where a bucket is in neither place.
//!
//! One file per tenant per segment is fine for tens of tenants. With
//! thousands, a single write-ahead log shared by all tenants would cost one
//! sync per segment instead of one per tenant.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use super::store::Store;
use crate::aggregator::Bucket;
use crate::stats::Stats;

/// Flushed buckets not yet in a committed file, per tenant.
pub type Pending = Arc<Mutex<HashMap<String, Vec<Bucket>>>>;

pub fn spawn(
    store: Arc<Store>,
    segment: Duration,
    rx: Receiver<Vec<Bucket>>,
    pending: Pending,
    stats: Arc<Stats>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("writer".into())
        .spawn(move || run(&store, segment, rx, &pending, &stats))
        .expect("spawn writer thread")
}

fn run(
    store: &Store,
    segment: Duration,
    rx: Receiver<Vec<Bucket>>,
    pending: &Pending,
    stats: &Stats,
) {
    let mut next_write = Instant::now() + segment;
    loop {
        match rx.recv_deadline(next_write) {
            Ok(buckets) => {
                Stats::add(&stats.buckets_flushed, buckets.len() as u64);
                let mut pending = pending.lock().unwrap();
                for b in buckets {
                    pending.entry(b.series.tenant.clone()).or_default().push(b);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if Instant::now() >= next_write {
            next_write += segment;
            write_all(store, pending, stats);
        }
    }
    write_all(store, pending, stats);
}

fn write_all(store: &Store, pending: &Pending, stats: &Stats) {
    // Held while writing so a bucket is always either pending or committed
    // when a query looks. Writes happen once per segment and take
    // milliseconds, so queries rarely wait.
    let mut pending = pending.lock().unwrap();
    for (tenant, buckets) in pending.drain() {
        match store.add(&tenant, &buckets) {
            Ok(0) => {}
            Ok(bytes) => {
                Stats::add(&stats.blocks_written, 1);
                Stats::add(&stats.bytes_written, bytes as u64);
            }
            Err(e) => {
                // Holding failed buckets would grow memory without bound
                // while the disk is unusable, so they are dropped and
                // counted.
                tracing::error!(tenant, "writing block: {e}");
                Stats::add(&stats.buckets_lost_write, buckets.len() as u64);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::{Aggregator, Config as AggConfig};
    use crate::protocol::parse_line;
    use crate::storage::store::Config;

    fn buckets(tenant: &str, start: u64) -> Vec<Bucket> {
        let mut agg = Aggregator::new(AggConfig::default());
        for i in 0..6 {
            let line = format!("req:1|c|#route:/a|T{}", start + i * 10);
            agg.insert(start + 100, tenant, &mut parse_line(&line).unwrap())
                .unwrap();
        }
        agg.flush_all()
    }

    #[test]
    fn writes_per_tenant_and_flushes_on_close() {
        let dir = std::env::temp_dir().join(format!("funnel-writer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = Arc::new(
            Store::open(Config {
                dir: dir.clone(),
                ..Config::default()
            })
            .unwrap(),
        );
        let (tx, rx) = crossbeam_channel::unbounded();
        let stats = Arc::new(Stats::default());
        let pending = Pending::default();
        let handle = spawn(
            store.clone(),
            Duration::from_secs(3600),
            rx,
            pending.clone(),
            stats.clone(),
        );
        tx.send(buckets("acme", 1000)).unwrap();
        tx.send(buckets("globex", 1000)).unwrap();
        // Before the segment ends, buckets are pending, not on disk.
        let deadline = Instant::now() + Duration::from_secs(5);
        while pending
            .lock()
            .unwrap()
            .values()
            .map(Vec::len)
            .sum::<usize>()
            < 12
        {
            assert!(Instant::now() < deadline, "buckets never became pending");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(store.files("acme", 0, u64::MAX).is_empty());
        drop(tx);
        handle.join().unwrap();
        let s = stats.snapshot();
        assert_eq!((s.buckets_flushed, s.blocks_written), (12, 2));
        assert!(pending.lock().unwrap().is_empty());
        assert_eq!(store.files("acme", 0, u64::MAX).len(), 1);
        assert_eq!(store.files("globex", 0, u64::MAX).len(), 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
