//! Runs the generated workload through the real rollup, storage and query
//! path and prints percentiles, to compare with exact answers computed from
//! the raw points elsewhere (e.g. ClickHouse's quantileExact).
//!
//!     cargo run --release -p funnel --example query_check -- 10 /r3 [resolution]
//!
//! With a resolution in seconds, everything is downsampled to it first;
//! sketches merge exactly, so whole-range percentiles must not change.

use funnel::aggregator::{Aggregator, Config};
use funnel::protocol::parse_line;
use funnel::query::{Agg, Query, run};
use funnel::storage::store::{Config as StoreConfig, Store};
use funnel::storage::writer::Pending;

fn main() {
    let minutes: u64 = std::env::args().nth(1).map_or(10, |m| m.parse().unwrap());
    let route = std::env::args().nth(2).unwrap_or_else(|| "/r3".into());
    let resolution: Option<u64> = std::env::args().nth(3).map(|r| r.parse().unwrap());
    let dir = std::env::temp_dir().join(format!("funnel-query-check-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = Store::open(StoreConfig {
        dir: dir.clone(),
        downsample: resolution
            .map(|r| {
                vec![funnel::storage::store::Downsample {
                    after_secs: 1,
                    resolution_secs: r,
                }]
            })
            .unwrap_or_default(),
        ..StoreConfig::default()
    })
    .unwrap();

    let mut workload = loadgen::Workload::new(loadgen::Config::default());
    let mut agg = Aggregator::new(Config::default());
    let mut lines = Vec::new();
    let start = workload.now();
    for _ in 0..minutes * 60 {
        lines.clear();
        let now = workload.next_second(&mut lines);
        for line in &lines {
            agg.insert(now, "default", &mut parse_line(line).unwrap())
                .unwrap();
        }
        // One file per 10 seconds, like the writer.
        if now % 10 == 9 {
            store.add("default", &agg.flush(now)).unwrap();
        }
    }
    store.add("default", &agg.flush_all()).unwrap();
    store.compact(start + minutes * 60 + 86_400).unwrap();

    for q in [0.5, 0.99, 0.999] {
        let result = run(
            &store,
            &Pending::default(),
            &Query {
                tenant: "default".into(),
                metric: "http.duration".into(),
                from: 0,
                to: u64::MAX,
                filters: vec![("route".into(), route.clone())],
                group_by: Some(vec![]),
                step: 1 << 40,
                agg: Some(Agg::Quantile(q)),
            },
        )
        .unwrap();
        println!(
            "http.duration route={route} p{}: {:.2} ({} files, {} buckets)",
            q * 100.0,
            result.series[0].points[0].1,
            result.files_read,
            result.buckets_read
        );
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
