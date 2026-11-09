//! Proves the planning rule changes the cost of a query and never its answer: every generated
//! query runs with the rule on and off, and the results must be the same.

use datafusion::arrow::array::{Array, RecordBatch};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use funnel_lake::{Lake, Row};
use funnel_sketch::DDSketch;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::Ordering;

/// 2026-01-01 00:00:00 UTC.
const EPOCH: i64 = 1_767_225_600;
const HOUR: i64 = 3600;
const HOURS: i64 = 6;

fn base(tenant: &str, metric: &str, tags: &str, kind: &'static str, ts: i64) -> Row {
    Row {
        tenant: tenant.into(),
        metric: metric.into(),
        tags: tags.into(),
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

fn hour_of_rows(rng: &mut StdRng, tenant: &str, hour: i64) -> Vec<Row> {
    let mut rows = Vec::new();
    for tick in 0..HOUR / 10 {
        let ts = EPOCH + hour * HOUR + tick * 10;
        for host in ["a", "b", "c"] {
            let tags = format!("host={host}");
            let value = f64::from(rng.random_range(1..1000));

            let mut counter = base(tenant, "req", &tags, "counter", ts);
            counter.count = 1.0;
            counter.sum = value;
            rows.push(counter);

            let mut gauge = base(tenant, "queue", &tags, "gauge", ts);
            gauge.count = 1.0;
            gauge.sum = value;
            gauge.min = Some(value);
            gauge.max = Some(value);
            gauge.last = Some(value);
            gauge.last_at = Some(ts);
            rows.push(gauge);

            let mut sketch = DDSketch::default();
            sketch.add(value);
            sketch.add(value * 3.0);
            let mut dist = base(tenant, "lat", &tags, "distribution", ts);
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

async fn lake(dir: &std::path::Path, seed: u64) -> Lake {
    let mut rng = StdRng::seed_from_u64(seed);
    let lake = Lake::open(dir).await.unwrap();
    for hour in 0..HOURS {
        let rows = hour_of_rows(&mut rng, "acme", hour);
        lake.publish("acme", &rows, EPOCH + (hour + 1) * HOUR)
            .await
            .unwrap();
    }
    lake
}

fn stamp(offset: i64) -> String {
    format!(
        "TIMESTAMP '2026-01-01T{:02}:{:02}:{:02}'",
        offset / 3600,
        offset / 60 % 60,
        offset % 60
    )
}

fn random_query(rng: &mut StdRng) -> String {
    let aggregates = [
        "sum(sum)",
        "sum(count)",
        "min(min)",
        "max(max)",
        "quantile(sketch, 0.99)",
    ];
    let agg = aggregates[rng.random_range(0..aggregates.len())];
    let metric = ["req", "queue", "lat"][rng.random_range(0..3)];
    let step = [10, 60, 90, 600, 1800, 3600, 7200][rng.random_range(0..7)];
    let grain = [10, 60, 600, 3600][rng.random_range(0..4)];

    let mut filters = vec![format!("metric = '{metric}'")];
    if rng.random_bool(0.3) {
        filters.push("tags = 'host=a'".into());
    }
    if rng.random_bool(0.7) {
        let hours = rng.random_range(0..HOURS);
        filters.push(format!("ts >= {}", stamp(hours * HOUR / grain * grain)));
    }
    if rng.random_bool(0.7) {
        // May run past what is published, up to an hour beyond.
        let end =
            (rng.random_range(1..=(HOURS + 1)) * HOUR - grain * rng.random_range(0..3)).max(grain);
        filters.push(format!("ts < {}", stamp(end)));
    }
    if rng.random_bool(0.15) {
        filters.push("max > 100".into());
    }
    let group_tags = rng.random_bool(0.5);
    let tags = if group_tags { ", tags" } else { "" };
    let time = rng.random_bool(0.8);
    let (bucket, order) = if time {
        (
            format!("date_bin(INTERVAL '{step} seconds', ts) AS bucket"),
            "1".to_string(),
        )
    } else {
        ("metric".to_string(), "1".to_string())
    };
    let group_ordinal = if group_tags { "1, 2" } else { "1" };
    format!(
        "SELECT {bucket}{tags}, {agg} AS v FROM metrics WHERE {} GROUP BY {group_ordinal} ORDER BY {order}{}",
        filters.join(" AND "),
        if group_tags { ", 2" } else { "" }
    )
}

fn cells(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for b in batches {
        for row in 0..b.num_rows() {
            out.push(
                b.columns()
                    .iter()
                    .map(|c: &std::sync::Arc<dyn Array>| array_value_to_string(c, row).unwrap())
                    .collect(),
            );
        }
    }
    out
}

fn same(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => (x - y).abs() <= 1e-9 * x.abs().max(y.abs()),
        _ => false,
    }
}

async fn run(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    cells(&ctx.sql(sql).await.unwrap().collect().await.unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rule_never_changes_an_answer() {
    let dir = tempfile::tempdir().unwrap();
    let lake = lake(dir.path(), 1).await;
    let (with_rule, stats) = lake.session(true).await.unwrap();
    let (without_rule, _) = lake.session(false).await.unwrap();

    let mut rng = StdRng::seed_from_u64(99);
    let mut nonempty = 0;
    for _ in 0..200 {
        let sql = random_query(&mut rng);
        let a = run(&without_rule, &sql).await;
        let b = run(&with_rule, &sql).await;
        assert_eq!(a.len(), b.len(), "row count differs for {sql}");
        for (x, y) in a.iter().zip(&b) {
            let equal = x.iter().zip(y).all(|(p, q)| same(p, q));
            assert!(equal, "{sql}\nwithout rule: {x:?}\nwith rule:    {y:?}");
        }
        nonempty += usize::from(!a.is_empty());
    }
    assert!(nonempty > 150, "only {nonempty} queries returned rows");
    let rewrites = stats.rewrites.load(Ordering::Relaxed);
    assert!(
        rewrites > 40,
        "the rule only fired {rewrites} times, so this proved little"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dashboard_query_reads_the_hourly_table() {
    let dir = tempfile::tempdir().unwrap();
    let lake = lake(dir.path(), 2).await;
    let (ctx, stats) = lake.session(true).await.unwrap();
    let sql = format!(
        "SELECT date_bin(INTERVAL '1 hour', ts) b, quantile(sketch, 0.5) FROM metrics \
         WHERE metric = 'lat' AND ts >= {} AND ts < {} GROUP BY 1 ORDER BY 1",
        stamp(0),
        stamp(3 * HOUR)
    );
    assert_eq!(run(&ctx, &sql).await.len(), 3);
    assert_eq!(stats.last_seconds.load(Ordering::Relaxed), 3600);
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_that_cannot_move_stay_on_the_fine_table() {
    let dir = tempfile::tempdir().unwrap();
    let lake = lake(dir.path(), 3).await;
    let (ctx, stats) = lake.session(true).await.unwrap();
    for sql in [
        "SELECT count(*) FROM metrics",
        "SELECT sum(sum) FROM metrics WHERE max > 100",
        "SELECT sum(sum) FROM metrics WHERE ts >= TIMESTAMP '2026-01-01T00:00:10'",
        "SELECT sum(sum) FROM metrics WHERE ts <= TIMESTAMP '2026-01-01T01:00:00'",
        "SELECT avg(sum) FROM metrics",
        "SELECT ts, sum FROM metrics LIMIT 1",
        "SELECT sum(sum) FROM metrics GROUP BY date_bin(INTERVAL '90 seconds', ts)",
    ] {
        run(&ctx, sql).await;
        assert_eq!(stats.rewrites.load(Ordering::Relaxed), 0, "moved: {sql}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn publishing_the_same_hour_twice_adds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(dir.path()).await.unwrap();
    let mut rng = StdRng::seed_from_u64(4);
    let rows = hour_of_rows(&mut rng, "acme", 0);
    let until = EPOCH + HOUR;
    lake.publish("acme", &rows, until).await.unwrap();
    lake.publish("acme", &rows, until).await.unwrap();
    assert_eq!(lake.watermark("acme").await.unwrap(), Some(until));
    let (ctx, _) = lake.session(false).await.unwrap();
    let count = run(&ctx, "SELECT count(*) FROM metrics").await;
    assert_eq!(count[0][0], rows.len().to_string());
}

#[tokio::test(flavor = "multi_thread")]
async fn tenants_keep_separate_watermarks() {
    let dir = tempfile::tempdir().unwrap();
    let lake = Lake::open(dir.path()).await.unwrap();
    let mut rng = StdRng::seed_from_u64(5);
    let a = hour_of_rows(&mut rng, "a", 0);
    lake.publish("a", &a, EPOCH + HOUR).await.unwrap();
    assert_eq!(lake.watermark("a").await.unwrap(), Some(EPOCH + HOUR));
    assert_eq!(lake.watermark("b").await.unwrap(), None);
}
