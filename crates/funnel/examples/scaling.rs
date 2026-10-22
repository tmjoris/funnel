//! How rollup storage grows with traffic: the same 7,000 series at several
//! request rates, stored as compressed blocks.
//!
//!     cargo run --release -p funnel --example scaling -- [minutes] [rates...]

use funnel::aggregator::{Aggregator, Bucket, Config};
use funnel::protocol::parse_line;
use funnel::storage::block;

fn main() {
    let mut args = std::env::args().skip(1);
    let minutes: u64 = args.next().map_or(2, |m| m.parse().unwrap());
    let rates: Vec<f64> = {
        let r: Vec<f64> = args.map(|r| r.parse().unwrap()).collect();
        if r.is_empty() {
            vec![50.0, 500.0, 2000.0]
        } else {
            r
        }
    };
    println!(
        "{:>12} {:>12} {:>8} {:>9} {:>11} {:>9} {:>10} {:>14}",
        "req/s/host", "points", "series", "buckets", "bytes", "B/bucket", "B/point", "MB/series/yr"
    );
    for rate in rates {
        let mut workload = loadgen::Workload::new(loadgen::Config {
            requests_per_host: rate,
            ..loadgen::Config::default()
        });
        let mut agg = Aggregator::new(Config {
            max_bytes: usize::MAX,
            ..Config::default()
        });
        let mut buckets: Vec<Bucket> = Vec::new();
        let mut points = 0u64;
        let mut lines = Vec::new();
        for _ in 0..minutes * 60 {
            lines.clear();
            let now = workload.next_second(&mut lines);
            points += lines.len() as u64;
            for line in &lines {
                agg.insert(now, "default", &mut parse_line(line).unwrap())
                    .unwrap();
            }
            buckets.extend(agg.flush(now));
            lines.clear();
        }
        buckets.extend(agg.flush_all());
        let mut series: Vec<_> = buckets.iter().map(|b| b.series.clone()).collect();
        series.sort_by(|a, b| (&a.name, &a.tags).cmp(&(&b.name, &b.tags)));
        series.dedup();
        let bytes = zstd::bulk::compress(&block::encode("default", &buckets), 3)
            .unwrap()
            .len();
        let per_series_year =
            bytes as f64 / series.len() as f64 * (365.0 * 24.0 * 60.0 / minutes as f64);
        println!(
            "{rate:>12} {points:>12} {:>8} {:>9} {bytes:>11} {:>9.1} {:>10.3} {:>14.2}",
            series.len(),
            buckets.len(),
            bytes as f64 / buckets.len() as f64,
            bytes as f64 / points as f64,
            per_series_year / 1e6,
        );
    }
}
