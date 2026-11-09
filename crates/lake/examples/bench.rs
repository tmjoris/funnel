//! Measures what the rollup rule buys on dashboard-shaped queries.
//!
//! cargo run --release -p funnel-lake --example bench -- DIR [DAYS] [HOSTS]

use std::path::{Path, PathBuf};
use std::time::Instant;

use datafusion::prelude::SessionContext;
use funnel_lake::{Lake, RESOLUTIONS, Row};
use funnel_sketch::DDSketch;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// 2026-01-01 00:00:00 UTC.
const EPOCH: i64 = 1_767_225_600;
const HOUR: i64 = 3600;
const RUNS: usize = 5;

fn row(metric: &str, host: usize, kind: &'static str, ts: i64) -> Row {
    Row {
        tenant: "acme".into(),
        metric: metric.into(),
        tags: format!("host=h{host:04}"),
        kind,
        ts,
        count: 0.0,
        sum: 0.0,
        min: None,
        max: None,
        last: None,
        last_at: None,
        sketch: None,
    }
}

/// One hour of a simulated fleet: a request counter, a queue gauge and a latency distribution per
/// host, every 10 seconds.
fn hour(rng: &mut StdRng, hosts: usize, hour: i64) -> Vec<Row> {
    let mut rows = Vec::with_capacity(360 * hosts * 3);
    for tick in 0..360 {
        let ts = EPOCH + hour * HOUR + tick * 10;
        for host in 0..hosts {
            let mut counter = row("http.requests", host, "counter", ts);
            counter.sum = f64::from(rng.random_range(50..150));
            rows.push(counter);

            let mut gauge = row("queue.depth", host, "gauge", ts);
            let value = f64::from(rng.random_range(0..40));
            gauge.count = 1.0;
            gauge.sum = value;
            gauge.min = Some(value);
            gauge.max = Some(value);
            gauge.last = Some(value);
            gauge.last_at = Some(ts);
            rows.push(gauge);

            let mut sketch = DDSketch::default();
            for _ in 0..20 {
                // Mostly fast, with a slow tail.
                let base: f64 = rng.random_range(5.0..40.0);
                let tail = if rng.random_bool(0.02) {
                    rng.random_range(10.0..50.0)
                } else {
                    1.0
                };
                sketch.add(base * tail);
            }
            let mut dist = row("http.duration", host, "distribution", ts);
            dist.count = sketch.count();
            dist.sum = sketch.sum();
            dist.min = sketch.min();
            dist.max = sketch.max();
            dist.sketch = Some(sketch);
            rows.push(dist);
        }
    }
    rows
}

fn stamp(hours: i64, extra_seconds: i64) -> String {
    let seconds = hours * HOUR + extra_seconds;
    let days = seconds / 86_400;
    format!(
        "TIMESTAMP '2026-01-{:02}T{:02}:{:02}:{:02}'",
        days + 1,
        seconds % 86_400 / 3600,
        seconds % 3600 / 60,
        seconds % 60
    )
}

fn dashboards(days: i64) -> Vec<(String, String)> {
    let end = days * 24;
    let range = |from: i64, to: i64, extra: i64| {
        format!("ts >= {} AND ts < {}", stamp(from, 0), stamp(to, extra))
    };
    vec![
        (
            "p99 latency, last hour, per minute".into(),
            format!(
                "SELECT date_bin(INTERVAL '60 seconds', ts) t, quantile(sketch, 0.99) v FROM metrics \
                 WHERE metric = 'http.duration' AND {} GROUP BY 1 ORDER BY 1",
                range(end - 1, end, 0)
            ),
        ),
        (
            "request total, last day, per 10 minutes".into(),
            format!(
                "SELECT date_bin(INTERVAL '600 seconds', ts) t, sum(sum) v FROM metrics \
                 WHERE metric = 'http.requests' AND {} GROUP BY 1 ORDER BY 1",
                range(end - 24, end, 0)
            ),
        ),
        (
            format!("p99 latency, last {days} days, per hour"),
            format!(
                "SELECT date_bin(INTERVAL '3600 seconds', ts) t, quantile(sketch, 0.99) v FROM metrics \
                 WHERE metric = 'http.duration' AND {} GROUP BY 1 ORDER BY 1",
                range(0, end, 0)
            ),
        ),
        (
            format!("peak queue per host, last {days} days"),
            format!(
                "SELECT tags, max(max) v FROM metrics WHERE metric = 'queue.depth' AND {} GROUP BY 1 ORDER BY 1",
                range(0, end, 0)
            ),
        ),
        (
            format!("request total, {days} days, range ending mid-minute (rule must stay fine)"),
            format!(
                "SELECT sum(sum) v FROM metrics WHERE metric = 'http.requests' AND {}",
                range(0, end - 1, 10)
            ),
        ),
    ]
}

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
