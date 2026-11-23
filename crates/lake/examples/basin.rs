//! Publishes the simulated fleet to an Iceberg REST catalog (Cloudflare Basin Catalog) and times the
//! dashboard queries reading back from object storage, with the rollup rule off and on.
//!
//! FUNNEL_CATALOG_URI=... FUNNEL_WAREHOUSE=... BASIN_TOKEN=... \
//!   cargo run --release -p funnel-lake --example basin -- [DAYS] [HOSTS] [RUNS]
//!
//! Rows go to the `bench` tenant. Hours already published are skipped, so a cut-short run resumes.
use std::sync::atomic::Ordering;
use std::time::Instant;

use datafusion::prelude::SessionContext;
use funnel_lake::Lake;
use rand::SeedableRng;
use rand::rngs::StdRng;

mod fleet;
use fleet::{EPOCH, HOUR, dashboards, hour};

const TENANT: &str = "bench";

async fn timed(ctx: &SessionContext, sql: &str) -> anyhow::Result<(f64, usize)> {
    let start = Instant::now();
    let batches = ctx.sql(sql).await?.collect().await?;
    let rows = batches.iter().map(|b| b.num_rows()).sum();
    Ok((start.elapsed().as_secs_f64() * 1000.0, rows))
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("{name} is not set"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let days: i64 = args.get(1).map_or(7, |v| v.parse().unwrap());
    let hosts: usize = args.get(2).map_or(200, |v| v.parse().unwrap());
    let runs: usize = args.get(3).map_or(3, |v| v.parse().unwrap());

    let lake = Lake::open_rest(
        &env("FUNNEL_CATALOG_URI")?,
        &env("FUNNEL_WAREHOUSE")?,
        &env("BASIN_TOKEN")?,
    )
    .await?;

    // The generator is seeded, so every hour is drawn in order even when it is skipped.
    let done = lake.watermark(TENANT).await?.unwrap_or(EPOCH);
    let mut rng = StdRng::seed_from_u64(42);
    let started = Instant::now();
    let (mut points, mut published) = (0usize, 0);
    for h in 0..days * 24 {
        let mut rows = hour(&mut rng, hosts, h);
        let until = EPOCH + (h + 1) * HOUR;
        if until <= done {
            continue;
        }
        for row in &mut rows {
            row.tenant = TENANT.into();
        }
        points += rows.len();
        lake.publish(TENANT, &rows, until).await?;
        published += 1;
        if published % 24 == 0 {
            println!(
                "published {published} hours ({points} rows) in {:.0} s",
                started.elapsed().as_secs_f64()
            );
        }
    }
    println!(
        "published {published} new hours, {points} rows, in {:.0} s\n",
        started.elapsed().as_secs_f64()
    );

    let (with_rule, stats) = lake.session(true, Some(TENANT)).await?;
    let (without_rule, _) = lake.session(false, Some(TENANT)).await?;
    println!("| dashboard | table chosen | without rule ms | with rule ms | speedup | rows |");
    println!("|---|---|---|---|---|---|");
    for (name, sql) in dashboards(days) {
        let before = stats.rewrites.load(Ordering::Relaxed);
        let (mut off, mut on, mut rows) = (Vec::new(), Vec::new(), 0);
        for _ in 0..runs {
            off.push(timed(&without_rule, &sql).await?.0);
            let (ms, n) = timed(&with_rule, &sql).await?;
            on.push(ms);
            rows = n;
        }
        let table = if stats.rewrites.load(Ordering::Relaxed) > before {
            format!("{} s", stats.last_seconds.load(Ordering::Relaxed))
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
