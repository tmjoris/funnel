//! Publishes closed hours from the block store to Iceberg.
//!
//! The block store stays the system of record for recent data and `GET /query`. This adds a
//! second, open copy for long-range SQL. An hour is published once every file touching it has been
//! compacted, so partial buckets have already landed. Points that arrive after an hour is
//! published reach `/query` but not the lake.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result};
use funnel_lake::{Lake, Row};
use tokio::runtime::Handle;

use crate::aggregator::Value;
use crate::protocol::Kind;
use crate::storage::block::Reader;
use crate::storage::store::Store;
use crate::unix_now;

const HOUR: u64 = 3600;
const POLL: Duration = Duration::from_secs(30);

/// Runs the publisher until `stop` is set.
pub fn spawn(
    lake: Arc<Lake>,
    store: Arc<Store>,
    rt: Handle,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("lake-publisher".into())
        .spawn(move || {
            let mut waited = POLL;
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(500));
                waited += Duration::from_millis(500);
                if waited < POLL {
                    continue;
                }
                waited = Duration::ZERO;
                if let Err(e) = rt.block_on(publish_ready(&lake, &store, unix_now())) {
                    tracing::error!("lake publish: {e:#}");
                }
            }
        })
        .expect("spawn publisher")
}

/// Publishes every hour that is ready, oldest first. Returns how many were published.
pub async fn publish_ready(lake: &Lake, store: &Store, now: u64) -> Result<usize> {
    let mut published = 0;
    for tenant in store.tenants() {
        let files = store.files(&tenant, 0, u64::MAX);
        let Some(oldest) = files.first().map(|f| f.first) else {
            continue;
        };
        let start = match lake.watermark(&tenant).await? {
            Some(w) => u64::try_from(w).context("negative watermark")?,
            None => oldest - oldest % HOUR,
        };
        let settled = store.config().settle_secs + store.config().span_secs;
        let mut hour = start;
        while hour + HOUR + settled <= now {
            match hour_rows(store, &tenant, hour)? {
                Some(rows) if !rows.is_empty() => {
                    lake.publish(&tenant, &rows, i64::try_from(hour + HOUR)?)
                        .await?;
                    published += 1;
                }
                Some(_) => {}
                // Not compacted yet, and later hours must not overtake it.
                None => break,
            }
            hour += HOUR;
        }
    }
    Ok(published)
}

/// The rows of one hour, or `None` if some file touching it is not compacted yet.
fn hour_rows(store: &Store, tenant: &str, hour: u64) -> Result<Option<Vec<Row>>> {
    let end = hour + HOUR - 1;
    let files = store.files(tenant, hour, end);
    if files.iter().any(|f| f.level == 0) {
        return Ok(None);
    }
    let mut rows = Vec::new();
    for file in &files {
        let bytes = store.read(tenant, file)?;
        let reader = Reader::open(&bytes).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        for entry in &reader.index {
            let tags = tags_text(&entry.tags);
            for (start, value) in reader.series(entry).map_err(|e| anyhow::anyhow!("{e:?}"))? {
                if (hour..=end).contains(&start) {
                    rows.push(to_row(
                        tenant,
                        &entry.name,
                        &tags,
                        entry.kind,
                        start,
                        value,
                    )?);
                }
            }
        }
    }
    Ok(Some(rows))
}

fn tags_text(tags: &[(String, String)]) -> String {
    let sorted: BTreeMap<&str, &str> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    sorted
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn to_row(
    tenant: &str,
    metric: &str,
    tags: &str,
    kind: Kind,
    start: u64,
    value: Value,
) -> Result<Row> {
    let mut row = Row {
        tenant: tenant.to_owned(),
        metric: metric.to_owned(),
        tags: tags.to_owned(),
        kind: match kind {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
            Kind::Distribution => "distribution",
        },
        ts: i64::try_from(start)?,
        count: 0.0,
        sum: 0.0,
        min: None,
        max: None,
        last: None,
        last_at: None,
        sketch: None,
    };
    match value {
        Value::Counter(c) => row.sum = c,
        Value::Gauge(g) => {
            row.count = g.count as f64;
            row.sum = g.sum;
            row.min = Some(g.min);
            row.max = Some(g.max);
            row.last = Some(g.last);
            row.last_at = Some(i64::try_from(g.last_at)?);
        }
        Value::Distribution(s) => {
            row.count = s.count();
            row.sum = s.sum();
            row.min = s.min();
            row.max = s.max();
            row.sketch = Some(*s);
        }
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::{Aggregator, Config as AggConfig};
    use crate::protocol::parse_line;
    use crate::storage::store::Config as StoreConfig;

    const T0: u64 = 1_767_225_600;

    fn store(dir: &std::path::Path) -> Store {
        Store::open(StoreConfig {
            dir: dir.to_path_buf(),
            ..StoreConfig::default()
        })
        .unwrap()
    }

    fn write(store: &Store, lines: &[(u64, String)]) {
        let mut agg = Aggregator::new(AggConfig::default());
        for (now, line) in lines {
            agg.insert(*now, "acme", &mut parse_line(line).unwrap())
                .unwrap();
        }
        store.add("acme", &agg.flush_all()).unwrap();
    }

    fn sample_hour(start: u64) -> Vec<(u64, String)> {
        let mut lines = Vec::new();
        for i in 0..60u64 {
            let t = start + i * 60;
            lines.push((t, format!("req:2|c|#host:a|T{t}")));
            lines.push((t, format!("lat:{}|ms|#host:a|T{t}", 10 + i)));
            lines.push((t, format!("queue:{i}|g|#host:a|T{t}")));
        }
        lines
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn compacted_hours_reach_sql_and_match_the_native_query() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(&dir.path().join("blocks"));
        let lake = Lake::open(&dir.path().join("lake")).await.unwrap();
        write(&s, &sample_hour(T0));
        let now = T0 + 3 * HOUR;
        assert_eq!(
            publish_ready(&lake, &s, now).await.unwrap(),
            0,
            "level-0 files must wait"
        );
        s.compact(now).unwrap();
        assert_eq!(publish_ready(&lake, &s, now).await.unwrap(), 1);
        assert_eq!(
            publish_ready(&lake, &s, now).await.unwrap(),
            0,
            "published twice"
        );

        let out = lake
            .query(
                "SELECT sum(sum) AS total FROM metrics WHERE metric = 'req'",
                "acme",
            )
            .await
            .unwrap();
        assert_eq!(String::from_utf8(out.json).unwrap(), r#"[{"total":120.0}]"#);
        assert_eq!(out.rollup_seconds, Some(3600));

        let out = lake
            .query(
                "SELECT max(max) AS m FROM metrics WHERE metric = 'queue'",
                "acme",
            )
            .await
            .unwrap();
        assert_eq!(String::from_utf8(out.json).unwrap(), r#"[{"m":59.0}]"#);
    }

    #[test]
    fn tags_are_rendered_sorted() {
        let tags = vec![
            ("z".to_owned(), "1".to_owned()),
            ("a".to_owned(), "2".to_owned()),
        ];
        assert_eq!(tags_text(&tags), "a=2,z=1");
    }
}
