//! What downsampling saves: the same workload stored at 10-second, 1-minute
//! and 10-minute resolution, each through a real store and its compaction.
//!
//!     cargo run --release -p funnel --example downsample -- [minutes]

use std::time::Duration;

use funnel::aggregator::{Aggregator, Bucket, Config};
use funnel::protocol::parse_line;
use funnel::storage::store::{Config as StoreConfig, Downsample, Store};

fn main() {
    let minutes: u64 = std::env::args().nth(1).map_or(30, |m| m.parse().unwrap());
    let mut workload = loadgen::Workload::new(loadgen::Config::default());
    let start = workload.now();
    let mut agg = Aggregator::new(Config {
        max_bytes: usize::MAX,
        ..Config::default()
    });
    let mut segments: Vec<Vec<Bucket>> = Vec::new();
    let mut lines = Vec::new();
    for _ in 0..minutes * 60 {
        lines.clear();
        let now = workload.next_second(&mut lines);
        for line in &lines {
            agg.insert(now, "default", &mut parse_line(line).unwrap())
                .unwrap();
        }
        if now % 10 == 9 {
            segments.push(agg.flush(now));
        }
    }
    segments.push(agg.flush_all());
    let buckets: usize = segments.iter().map(Vec::len).sum();
    println!("{minutes} min of workload, {buckets} 10-second buckets\n");
    println!(
        "{:>11} {:>12} {:>10} {:>13}",
        "resolution", "bytes", "vs 10 s", "GB per 30 days"
    );

    let mut base = 0.0;
    for resolution in [None, Some(60), Some(600)] {
        let dir = std::env::temp_dir().join(format!("funnel-downsample-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = Store::open(StoreConfig {
            dir: dir.clone(),
            settle_secs: 0,
            delete_after: Duration::ZERO,
            downsample: resolution
                .map(|r| {
                    vec![Downsample {
                        after_secs: 1,
                        resolution_secs: r,
                    }]
                })
                .unwrap_or_default(),
            ..StoreConfig::default()
        })
        .unwrap();
        for segment in &segments {
            store.add("default", segment).unwrap();
        }
        // Far enough ahead that every span is closed and old enough.
        store.compact(start + minutes * 60 + 86_400).unwrap();
        let bytes: u64 = store
            .files("default", 0, u64::MAX)
            .iter()
            .map(|f| std::fs::metadata(store.path("default", f)).unwrap().len())
            .sum();
        let bytes = bytes as f64;
        if resolution.is_none() {
            base = bytes;
        }
        println!(
            "{:>10}s {:>12.0} {:>9.1}x {:>13.2}",
            resolution.unwrap_or(10),
            bytes,
            base / bytes,
            bytes / minutes as f64 * 60.0 * 24.0 * 30.0 / 1e9
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
