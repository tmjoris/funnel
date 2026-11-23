//! Measures what the rollup rule buys on dashboard-shaped queries.
//!
//! cargo run --release -p funnel-lake --example bench -- DIR [DAYS] [HOSTS]

use std::path::{Path, PathBuf};
use std::time::Instant;

use datafusion::prelude::SessionContext;
use funnel_lake::{Lake, RESOLUTIONS};
use rand::rngs::StdRng;

mod fleet;
use fleet::{EPOCH, HOUR, dashboards, hour};
use rand::SeedableRng;

const RUNS: usize = 5;

async fn timed(ctx: &SessionContext, sql: &str) -> (f64, usize) {
    let start = Instant::now();
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let rows = batches.iter().map(|b| b.num_rows()).sum();
    (start.elapsed().as_secs_f64() * 1000.0, rows)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn dir_stats(path: &Path) -> (u64, usize) {
    let (mut bytes, mut files) = (0, 0);
    for entry in std::fs::read_dir(path).into_iter().flatten().flatten() {
        if entry.path().extension().is_some_and(|e| e == "parquet") {
            bytes += entry.metadata().unwrap().len();
            files += 1;
        }
    }
    (bytes, files)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(&args[1]);
    let days: i64 = args.get(2).map_or(7, |v| v.parse().unwrap());
    let hosts: usize = args.get(3).map_or(200, |v| v.parse().unwrap());

    let lake = Lake::open(&dir).await?;
    let mut rng = StdRng::seed_from_u64(42);
    let started = Instant::now();
    let mut points = 0usize;
    for h in 0..days * 24 {
        let rows = hour(&mut rng, hosts, h);
        points += rows.len();
        lake.publish("acme", &rows, EPOCH + (h + 1) * HOUR).await?;
    }
    println!(
        "published {points} rows in {:.0} s\n",
        started.elapsed().as_secs_f64()
    );

    println!("| table | rows-per-series ratio | Parquet MB | files | bytes per 10 s row |");
    println!("|---|---|---|---|---|");
    for r in RESOLUTIONS {
        let (bytes, files) = dir_stats(&dir.join("warehouse/funnel").join(r.table).join("data"));
        println!(
            "| {} | 1/{} | {:.1} | {} | {:.3} |",
            r.table,
            r.seconds / 10,
            bytes as f64 / 1e6,
            files,
            bytes as f64 / points as f64
        );
    }

    let (with_rule, stats) = lake.session(true, None).await?;
    let (without_rule, _) = lake.session(false, None).await?;
    println!(
        "\n| dashboard | table chosen | without rule ms | with rule ms | speedup | rows returned |"
    );
    println!("|---|---|---|---|---|---|");
    for (name, sql) in dashboards(days) {
        let before = stats.rewrites.load(std::sync::atomic::Ordering::Relaxed);
        let (mut off, mut on, mut rows) = (Vec::new(), Vec::new(), 0);
        for _ in 0..RUNS {
            off.push(timed(&without_rule, &sql).await.0);
            let (ms, n) = timed(&with_rule, &sql).await;
            on.push(ms);
            rows = n;
        }
        let moved = stats.rewrites.load(std::sync::atomic::Ordering::Relaxed) > before;
        let table = if moved {
            format!(
                "{} s",
                stats
                    .last_seconds
                    .load(std::sync::atomic::Ordering::Relaxed)
            )
        } else {
            "10 s (unchanged)".to_string()
        };
        let (off, on) = (median(off), median(on));
        println!(
            "| {name} | {table} | {off:.0} | {on:.0} | {:.1}x | {rows} |",
            off / on
        );
    }
    Ok(())
}
