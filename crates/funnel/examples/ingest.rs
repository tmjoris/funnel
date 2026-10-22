//! Measures the CPU cost of the ingest path without the network: routing
//! (parse, hash, pick a shard) and shard processing (limit check and
//! aggregation), each on one thread, in nanoseconds per line.
//!
//!     cargo run --release -p funnel --example ingest -- [seconds of workload]

use std::sync::Arc;
use std::time::Instant;

use funnel::aggregator::{Aggregator, Config};
use funnel::limiter::{self, Budgets, Limiter};
use funnel::router::Router;
use funnel::shard;
use funnel::stats::Stats;

const SHARDS: usize = 2;

fn main() {
    let secs: u64 = std::env::args().nth(1).map_or(30, |s| s.parse().unwrap());
    let mut workload = loadgen::Workload::new(loadgen::Config::default());
    let mut lines = Vec::new();
    // Chunks of about 64 KB, like the UDP receiver's batches.
    let mut chunks: Vec<(u64, String)> = Vec::new();
    for _ in 0..secs {
        lines.clear();
        let now = workload.next_second(&mut lines);
        let mut chunk = String::new();
        for l in &lines {
            chunk.push_str(l);
            chunk.push('\n');
            if chunk.len() > 64 * 1024 {
                chunks.push((now, std::mem::take(&mut chunk)));
            }
        }
        chunks.push((now, chunk));
    }
    let total: u64 = chunks.iter().map(|(_, c)| c.lines().count() as u64).sum();

    let stats = Arc::new(Stats::default());
    let (txs, rxs): (Vec<_>, Vec<_>) = (0..SHARDS).map(|_| crossbeam_channel::unbounded()).unzip();
    let router = Router::new(txs, stats.clone());
    let tenant: Arc<str> = "default".into();

    let t = Instant::now();
    for (now, chunk) in &chunks {
        router.route(*now, &tenant, chunk);
    }
    let route_ns = t.elapsed().as_nanos() as f64 / total as f64;

    let budgets = Arc::new(Budgets::default());
    let mut process_ns = 0.0;
    for rx in &rxs {
        let batches: Vec<_> = rx.try_iter().collect();
        let mut agg = Aggregator::new(Config {
            max_bytes: usize::MAX,
            ..Config::default()
        });
        let mut limiter = Limiter::new(
            limiter::Config {
                max_series: usize::MAX,
                ..limiter::Config::default()
            },
            budgets.clone(),
        );
        let t = Instant::now();
        for b in &batches {
            shard::process(b, &mut agg, &mut limiter, &stats);
            // Flush as the shard would, so window state stays realistic.
            agg.flush(b.now);
        }
        process_ns += t.elapsed().as_nanos() as f64;
    }
    let process_ns = process_ns / total as f64;
    let s = stats.snapshot();
    assert_eq!(s.lines_accepted, total, "every line should be accepted");
    println!(
        "{total} lines: route {route_ns:.0} ns/line, process {process_ns:.0} ns/line, \
         total {:.0} ns/line = {:.2} M lines/s per core",
        route_ns + process_ns,
        1e3 / (route_ns + process_ns)
    );
}
