//! Writes the raw points of the generated workload as TSV for ClickHouse:
//! metric, tags as a ClickHouse Map literal, timestamp in milliseconds and
//! value. Same workload, seed and timestamps as the `storage` example.
//!
//!     cargo run --release -p funnel --example export_raw -- 10 > raw.tsv

use std::io::{BufWriter, Write};

use funnel::protocol::parse_line;
use funnel::series::normalize_tags;

fn main() -> std::io::Result<()> {
    let minutes: u64 = std::env::args().nth(1).map_or(10, |m| m.parse().unwrap());
    let mut workload = loadgen::Workload::new(loadgen::Config::default());
    let mut out = BufWriter::with_capacity(1 << 20, std::io::stdout().lock());
    let mut lines = Vec::new();
    for _ in 0..minutes * 60 {
        lines.clear();
        let now = workload.next_second(&mut lines);
        for (i, line) in lines.iter().enumerate() {
            let mut l = parse_line(line).unwrap();
            normalize_tags(&mut l.tags);
            let tags: Vec<String> = l.tags.iter().map(|(k, v)| format!("'{k}':'{v}'")).collect();
            let ms = now * 1000 + (i as u64 * 1000 / lines.len() as u64);
            writeln!(
                out,
                "{}\t{{{}}}\t{ms}\t{}",
                l.name,
                tags.join(","),
                l.values[0]
            )?;
        }
    }
    out.flush()
}
