//! A small query engine over block files: one metric, a time range, tag
//! filters, optional grouping, a step and one aggregate per point.
//!
//! Everything reduces to [`Value::merge`]: partial buckets of one window,
//! several series grouped together and several 10-second buckets folded
//! into one step all merge the same way. That is why distributions are
//! stored as mergeable sketches: the p99 across 100 hosts comes from merging
//! their sketches, which averaging 100 per-host p99s cannot reproduce.
//!
//! Results include buckets the writer has not written yet. Windows the
//! aggregator is still filling (at most a bucket width plus the grace
//! period, 15 seconds by default) are not visible: a 10-second summary does
//! not exist until its window closes.

use std::collections::BTreeMap;

use serde::Serialize;

use crate::aggregator::Value;
use crate::protocol::Kind;
use crate::storage::block;
use crate::storage::store::Store;
use crate::storage::writer::Pending;

/// Upper bound on buckets one query may decode, to keep a single request
/// from monopolising the server.
pub const MAX_BUCKETS: usize = 5_000_000;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Agg {
    Sum,
    Rate,
    Last,
    Min,
    Max,
    Avg,
    Count,
    Quantile(f64),
}

impl Agg {
    pub fn parse(s: &str) -> Option<Agg> {
        Some(match s {
            "sum" => Agg::Sum,
            "rate" => Agg::Rate,
            "last" => Agg::Last,
            "min" => Agg::Min,
            "max" => Agg::Max,
            "avg" => Agg::Avg,
            "count" => Agg::Count,
            _ => {
                let q: f64 = s.strip_prefix('p')?.parse().ok()?;
                if !(0.0..=100.0).contains(&q) {
                    return None;
                }
                Agg::Quantile(q / 100.0)
            }
        })
    }

    fn default_for(kind: Kind) -> Agg {
        match kind {
            Kind::Counter => Agg::Sum,
            Kind::Gauge => Agg::Last,
            Kind::Distribution => Agg::Quantile(0.5),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Query {
    pub tenant: String,
    pub metric: String,
    pub from: u64,
    pub to: u64,
    pub filters: Vec<(String, String)>,
    /// `None`: one result per series. `Some(tags)`: merge series that share
    /// the values of `tags`; an empty list merges every matching series.
    pub group_by: Option<Vec<String>>,
    /// Seconds per output point; buckets are merged into steps.
    pub step: u64,
    pub agg: Option<Agg>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct Series {
    pub tags: BTreeMap<String, String>,
    /// `[step start, value]`; steps with no data are absent.
    pub points: Vec<(u64, f64)>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct QueryResult {
    pub metric: String,
    pub kind: Option<&'static str>,
    pub series: Vec<Series>,
    pub files_read: usize,
    pub buckets_read: usize,
}

#[derive(Debug, PartialEq)]
pub enum QueryError {
    BadRequest(String),
    TooLarge,
    Io(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::BadRequest(m) => f.write_str(m),
            QueryError::TooLarge => write!(f, "query reads more than {MAX_BUCKETS} buckets"),
            QueryError::Io(m) => write!(f, "storage error: {m}"),
        }
    }
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Counter => "counter",
        Kind::Gauge => "gauge",
        Kind::Distribution => "distribution",
    }
}

type GroupKey = Vec<(String, String)>;
/// A pending bucket copied out under the lock: kind, tags, start, value.
type Recent = (Kind, Vec<(String, String)>, u64, Value);

/// Collects matching buckets into groups and steps, merging as it goes.
struct Merger<'q> {
    q: &'q Query,
    kind: Option<Kind>,
    /// Per group and step: the merged value and the widest bucket in it.
    groups: BTreeMap<GroupKey, BTreeMap<u64, (Value, u64)>>,
    buckets_read: usize,
}

impl Merger<'_> {
    fn matches(&self, tag: impl Fn(&str) -> Option<String>) -> bool {
        self.q
            .filters
            .iter()
            .all(|(k, v)| tag(k).as_deref() == Some(v.as_str()))
    }

    /// Adds one series' buckets; callers pass sources oldest first, which
    /// is the order gauge merges expect.
    fn add(
        &mut self,
        kind: Kind,
        tags: &[(String, String)],
        width: u64,
        buckets: impl IntoIterator<Item = (u64, Value)>,
    ) -> Result<(), QueryError> {
        match self.kind {
            None => self.kind = Some(kind),
            Some(k) if k != kind => {
                return Err(QueryError::BadRequest(format!(
                    "{} is stored as both {} and {}",
                    self.q.metric,
                    kind_name(k),
                    kind_name(kind)
                )));
            }
            Some(_) => {}
        }
        let key: GroupKey = match &self.q.group_by {
            None => tags.to_vec(),
            Some(by) => by
                .iter()
                .map(|t| {
                    let v = tags.iter().find(|(k, _)| k == t).map_or("", |(_, v)| v);
                    (t.clone(), v.to_owned())
                })
                .collect(),
        };
        let (from, to, step) = (self.q.from, self.q.to, self.q.step);
        let steps = self.groups.entry(key).or_default();
        for (start, value) in buckets {
            if start < from || start > to {
                continue;
            }
            self.buckets_read += 1;
            if self.buckets_read > MAX_BUCKETS {
                return Err(QueryError::TooLarge);
            }
            match steps.get_mut(&(start - start % step)) {
                Some((acc, w)) => {
                    acc.merge(&value).map_err(|_| {
                        QueryError::Io("bucket kinds disagree within a series".into())
                    })?;
                    *w = (*w).max(width);
                }
                None => {
                    steps.insert(start - start % step, (value, width));
                }
            }
        }
        Ok(())
    }
}

/// Runs a query over the tenant's files and its buckets not yet written.
pub fn run(store: &Store, pending: &Pending, q: &Query) -> Result<QueryResult, QueryError> {
    if q.step == 0 || q.from > q.to {
        return Err(QueryError::BadRequest(
            "need step > 0 and from <= to".into(),
        ));
    }
    let io = |e: &dyn std::fmt::Display| QueryError::Io(e.to_string());
    let mut m = Merger {
        q,
        kind: None,
        groups: BTreeMap::new(),
        buckets_read: 0,
    };

    let files = store.files(&q.tenant, q.from, q.to);
    for file in &files {
        let bytes = store.read(&q.tenant, file).map_err(|e| io(&e))?;
        let reader = block::Reader::open(&bytes).map_err(|e| io(&e))?;
        for entry in reader.find(&q.metric) {
            if entry.last < q.from || entry.first > q.to {
                continue;
            }
            if !m.matches(|k| entry.tag(k).map(str::to_owned)) {
                continue;
            }
            let series = reader.series(entry).map_err(|e| io(&e))?;
            m.add(entry.kind, &entry.tags, reader.resolution, series)?;
        }
    }

    // Newest last: buckets flushed but not yet in a committed file.
    let recent: Vec<Recent> = {
        let pending = pending.lock().unwrap();
        pending
            .get(&q.tenant)
            .into_iter()
            .flatten()
            .filter(|b| b.series.name == q.metric && b.start >= q.from && b.start <= q.to)
            .filter(|b| m.matches(|k| b.series.tag(k).map(str::to_owned)))
            .map(|b| {
                (
                    b.series.kind,
                    b.series.tags.clone(),
                    b.start,
                    b.value.clone(),
                )
            })
            .collect()
    };
    for (kind, tags, start, value) in recent {
        m.add(kind, &tags, store.config().bucket_secs, [(start, value)])?;
    }

    let agg = match (q.agg, m.kind) {
        (Some(a), _) => a,
        (None, Some(k)) => Agg::default_for(k),
        (None, None) => Agg::Sum,
    };
    if let Some(k) = m.kind {
        check_agg(k, agg)?;
    }
    let series = m
        .groups
        .into_iter()
        .map(|(tags, steps)| Series {
            tags: tags.into_iter().collect(),
            points: steps
                .into_iter()
                .filter_map(|(t, (v, width))| value_of(&v, agg, q.step.max(width)).map(|x| (t, x)))
                .collect(),
        })
        .collect();
    Ok(QueryResult {
        metric: q.metric.clone(),
        kind: m.kind.map(kind_name),
        series,
        files_read: files.len(),
        buckets_read: m.buckets_read,
    })
}

fn check_agg(kind: Kind, agg: Agg) -> Result<(), QueryError> {
    let ok = match kind {
        Kind::Counter => matches!(agg, Agg::Sum | Agg::Rate),
        Kind::Gauge => matches!(agg, Agg::Last | Agg::Min | Agg::Max | Agg::Avg | Agg::Count),
        Kind::Distribution => !matches!(agg, Agg::Last | Agg::Rate),
    };
    if ok {
        Ok(())
    } else {
        Err(QueryError::BadRequest(format!(
            "{agg:?} does not apply to a {}",
            kind_name(kind)
        )))
    }
}

/// `span` is the time a point covers: the step, or the bucket width when
/// downsampled buckets are wider than the step. Rates divide by it.
fn value_of(v: &Value, agg: Agg, span: u64) -> Option<f64> {
    match (v, agg) {
        (Value::Counter(c), Agg::Sum) => Some(*c),
        (Value::Counter(c), Agg::Rate) => Some(*c / span as f64),
        (Value::Gauge(g), Agg::Last) => Some(g.last),
        (Value::Gauge(g), Agg::Min) => Some(g.min),
        (Value::Gauge(g), Agg::Max) => Some(g.max),
        (Value::Gauge(g), Agg::Avg) => Some(g.sum / g.count as f64),
        (Value::Gauge(g), Agg::Count) => Some(g.count as f64),
        (Value::Distribution(s), Agg::Quantile(q)) => s.quantile(q),
        (Value::Distribution(s), Agg::Count) => Some(s.count()),
        (Value::Distribution(s), Agg::Sum) => Some(s.sum()),
        (Value::Distribution(s), Agg::Avg) => s.mean(),
        (Value::Distribution(s), Agg::Min) => s.min(),
        (Value::Distribution(s), Agg::Max) => s.max(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregator::{Aggregator, Config as AggConfig};
    use crate::protocol::parse_line;
    use crate::storage::store::Config as StoreConfig;
    use std::path::Path;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("funnel-query-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn open(dir: &Path) -> Store {
        Store::open(StoreConfig {
            dir: dir.to_path_buf(),
            ..StoreConfig::default()
        })
        .unwrap()
    }

    /// Writes one level-0 file per call, like one writer segment.
    fn segment(dir: &Path, lines: &[(u64, String)]) {
        let mut agg = Aggregator::new(AggConfig::default());
        for (now, line) in lines {
            agg.insert(*now, "acme", &mut parse_line(line).unwrap())
                .unwrap();
        }
        open(dir).add("acme", &agg.flush_all()).unwrap();
    }

    fn run_dir(dir: &Path, q: &Query) -> Result<QueryResult, QueryError> {
        run(&open(dir), &Pending::default(), q)
    }

    fn query(metric: &str) -> Query {
        Query {
            tenant: "acme".into(),
            metric: metric.into(),
            from: 0,
            to: u64::MAX,
            filters: vec![],
            group_by: None,
            step: 10,
            agg: None,
        }
    }

    fn setup(name: &str) -> PathBuf {
        let dir = temp_dir(name);
        let mut first = Vec::new();
        for i in 0..6u64 {
            let t = 1000 + i * 10;
            for host in ["a", "b"] {
                first.push((t, format!("req:2|c|#host:{host},route:/x")));
                first.push((
                    t,
                    format!(
                        "lat:{}|ms|#host:{host}",
                        if host == "a" { 10 } else { 1000 }
                    ),
                ));
            }
            first.push((t, format!("queue:{i}|g|#host:a")));
        }
        segment(&dir, &first);
        // A later segment holding a late point for the window at 1000.
        segment(
            &dir,
            &[(1100, "req:5|c|#host:a,route:/x|T1003".to_string())],
        );
        dir
    }

    #[test]
    fn per_series_counters_merge_partial_buckets() {
        let dir = setup("partial");
        let r = run_dir(
            &dir,
            &Query {
                filters: vec![("host".into(), "a".into())],
                ..query("req")
            },
        )
        .unwrap();
        assert_eq!(r.kind, Some("counter"));
        assert_eq!(r.files_read, 2);
        assert_eq!(r.series.len(), 1);
        assert_eq!(r.series[0].points[0], (1000, 7.0));
        assert_eq!(r.series[0].points[1], (1010, 2.0));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn group_all_and_step_and_rate() {
        let dir = setup("group");
        let q = Query {
            group_by: Some(vec![]),
            step: 60,
            agg: Some(Agg::Rate),
            ..query("req")
        };
        let r = run_dir(&dir, &q).unwrap();
        assert_eq!(r.series.len(), 1);
        // Steps start at multiples of 60: [960, 1020) holds the windows at
        // 1000 and 1010 (2 hosts * 2 windows * 2, plus the late 5) and
        // [1020, 1080) the four windows from 1020 to 1050.
        assert_eq!(
            r.series[0].points,
            vec![(960, 13.0 / 60.0), (1020, 16.0 / 60.0)]
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn merged_percentile_is_not_an_average_of_percentiles() {
        let dir = setup("pct");
        let q = Query {
            group_by: Some(vec![]),
            step: 3600,
            agg: Some(Agg::Quantile(0.9)),
            ..query("lat")
        };
        let r = run_dir(&dir, &q).unwrap();
        // Half the values are 10, half 1000: p90 is about 1000, while
        // averaging the two hosts' p90s would give about 505.
        let p90 = r.series[0].points[0].1;
        assert!((p90 - 1000.0).abs() < 10.0, "{p90}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn group_by_tag_and_gauges() {
        let dir = setup("gauge");
        let r = run_dir(
            &dir,
            &Query {
                group_by: Some(vec!["host".into()]),
                ..query("lat")
            },
        )
        .unwrap();
        assert_eq!(r.series.len(), 2);
        assert_eq!(r.series[0].tags.get("host").map(String::as_str), Some("a"));
        let g = run_dir(
            &dir,
            &Query {
                step: 60,
                agg: Some(Agg::Max),
                ..query("queue")
            },
        )
        .unwrap();
        assert_eq!(g.series[0].points, vec![(960, 1.0), (1020, 5.0)]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn pending_buckets_are_visible_and_merge_with_files() {
        let dir = setup("pending");
        let store = open(&dir);
        let pending = Pending::default();
        let mut agg = Aggregator::new(AggConfig::default());
        agg.insert(
            1200,
            "acme",
            &mut parse_line("req:4|c|#host:a,route:/x|T1005").unwrap(),
        )
        .unwrap();
        pending
            .lock()
            .unwrap()
            .insert("acme".into(), agg.flush_all());
        let q = Query {
            filters: vec![("host".into(), "a".into())],
            ..query("req")
        };
        let r = run(&store, &pending, &q).unwrap();
        // 2 + the late 5 from files, plus 4 still pending.
        assert_eq!(r.series[0].points[0], (1000, 11.0));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn time_range_and_errors() {
        let dir = setup("errors");
        let r = run_dir(
            &dir,
            &Query {
                from: 1020,
                to: 1030,
                ..query("req")
            },
        )
        .unwrap();
        assert!(
            r.series
                .iter()
                .all(|s| s.points.iter().all(|(t, _)| (1020..=1030).contains(t)))
        );
        assert!(run_dir(&dir, &query("missing")).unwrap().series.is_empty());
        let bad = run_dir(
            &dir,
            &Query {
                agg: Some(Agg::Quantile(0.5)),
                ..query("req")
            },
        );
        assert!(matches!(bad, Err(QueryError::BadRequest(_))));
        assert!(
            run_dir(
                &dir,
                &Query {
                    step: 0,
                    ..query("req")
                }
            )
            .is_err()
        );
        let Some(Agg::Quantile(q)) = Agg::parse("p99.9") else {
            panic!()
        };
        assert!((q - 0.999).abs() < 1e-12);
        assert_eq!(Agg::parse("p101"), None);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
