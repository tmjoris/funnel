//! A simulated fleet shared by the benchmark examples: 10-second rows for a request counter, a
//! queue gauge and a latency distribution per host, plus the dashboard queries run over them.
use funnel_lake::Row;
use funnel_sketch::DDSketch;
use rand::Rng;
use rand::rngs::StdRng;

/// 2026-01-01 00:00:00 UTC.
pub const EPOCH: i64 = 1_767_225_600;
pub const HOUR: i64 = 3600;

pub fn row(metric: &str, host: usize, kind: &'static str, ts: i64) -> Row {
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
pub fn hour(rng: &mut StdRng, hosts: usize, hour: i64) -> Vec<Row> {
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

pub fn stamp(hours: i64, extra_seconds: i64) -> String {
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

pub fn dashboards(days: i64) -> Vec<(String, String)> {
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
