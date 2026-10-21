//! Rolls points up into fixed time windows per series.
//!
//! Windows are keyed by start time in a `BTreeMap`, so flushing pops from
//! the front. A window closes `grace` seconds after its end; points that
//! arrive for a closed window are rejected as late rather than reopening it,
//! which keeps every flushed bucket final.

use std::collections::BTreeMap;
use std::sync::Arc;

use funnel_sketch::DDSketch;

use crate::protocol::{Kind, Line};
use crate::series::{IdMap, SeriesId, SeriesKey, normalize_tags, series_id};

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub bucket_secs: u64,
    pub grace_secs: u64,
    /// Points stamped further than this into the future are rejected.
    pub max_future_secs: u64,
    /// Open buckets across all windows; new buckets past this are rejected.
    pub max_buckets: usize,
    pub sketch: funnel_sketch::Config,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bucket_secs: 10,
            grace_secs: 5,
            max_future_secs: 60,
            max_buckets: 1_000_000,
            sketch: funnel_sketch::Config::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Gauge {
    pub min: f64,
    pub max: f64,
    pub last: f64,
    pub sum: f64,
    pub count: u64,
}

impl Gauge {
    fn new(v: f64) -> Self {
        Gauge {
            min: v,
            max: v,
            last: v,
            sum: v,
            count: 1,
        }
    }

    fn add(&mut self, v: f64) {
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        self.last = v;
        self.sum += v;
        self.count += 1;
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Counter(f64),
    Gauge(Gauge),
    Distribution(Box<DDSketch>),
}

impl Value {
    pub fn kind(&self) -> Kind {
        match self {
            Value::Counter(_) => Kind::Counter,
            Value::Gauge(_) => Kind::Gauge,
            Value::Distribution(_) => Kind::Distribution,
        }
    }
}

/// A closed bucket, ready to be written.
#[derive(Clone, Debug)]
pub struct Bucket {
    pub start: u64,
    pub series: Arc<SeriesKey>,
    pub value: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    /// The point's window has already been flushed.
    Late,
    /// The timestamp is too far ahead of the receive time.
    Future,
    /// `max_buckets` is reached and the point needs a new bucket.
    Overflow,
}

struct SeriesEntry {
    key: Arc<SeriesKey>,
    /// Open windows holding a bucket for this series.
    windows: u32,
}

pub struct Aggregator {
    config: Config,
    windows: BTreeMap<u64, IdMap<Value>>,
    series: IdMap<SeriesEntry>,
    buckets: usize,
    /// Every window starting before this has been flushed.
    closed_before: u64,
}

impl Aggregator {
    pub fn new(config: Config) -> Self {
        assert!(config.bucket_secs > 0);
        Aggregator {
            config,
            windows: BTreeMap::new(),
            series: IdMap::default(),
            buckets: 0,
            closed_before: 0,
        }
    }

    pub fn open_buckets(&self) -> usize {
        self.buckets
    }

    pub fn open_series(&self) -> usize {
        self.series.len()
    }

    /// Adds every value on `line`. `now` is the receive time in unix seconds.
    /// Returns the series id so callers can account for it.
    pub fn insert(
        &mut self,
        now: u64,
        tenant: &str,
        line: &mut Line<'_>,
    ) -> Result<SeriesId, Reject> {
        normalize_tags(&mut line.tags);
        let id = series_id(tenant, line.name, line.kind, &line.tags);
        self.insert_prepared(now, id, tenant, line).map(|()| id)
    }

    /// Like [`insert`](Self::insert) for a line whose tags are normalized and
    /// whose series id the caller already computed.
    pub fn insert_prepared(
        &mut self,
        now: u64,
        id: SeriesId,
        tenant: &str,
        line: &Line<'_>,
    ) -> Result<(), Reject> {
        if line.values.is_empty() {
            return Ok(());
        }
        let ts = line.timestamp.unwrap_or(now);
        if ts > now.saturating_add(self.config.max_future_secs) {
            return Err(Reject::Future);
        }
        let start = ts - ts % self.config.bucket_secs;
        if start < self.closed_before {
            return Err(Reject::Late);
        }
        let window = self.windows.entry(start).or_default();
        match window.get_mut(&id) {
            Some(value) => merge(value, line),
            None => {
                if self.buckets >= self.config.max_buckets {
                    if window.is_empty() {
                        self.windows.remove(&start);
                    }
                    return Err(Reject::Overflow);
                }
                window.insert(id, new_value(line, &self.config.sketch));
                self.buckets += 1;
                self.series
                    .entry(id)
                    .or_insert_with(|| SeriesEntry {
                        key: Arc::new(SeriesKey::new(tenant, line)),
                        windows: 0,
                    })
                    .windows += 1;
            }
        }
        Ok(())
    }

    /// Closes every window that ended at least `grace` seconds before `now`.
    pub fn flush(&mut self, now: u64) -> Vec<Bucket> {
        let width = self.config.bucket_secs;
        let cutoff = now.saturating_sub(self.config.grace_secs);
        // Windows with start + width <= cutoff are closed.
        self.closed_before = self.closed_before.max(cutoff - cutoff % width);
        self.drain_before(self.closed_before)
    }

    /// Closes all windows, e.g. on shutdown.
    pub fn flush_all(&mut self) -> Vec<Bucket> {
        self.drain_before(u64::MAX)
    }

    fn drain_before(&mut self, end: u64) -> Vec<Bucket> {
        let mut out = Vec::new();
        while let Some(entry) = self.windows.first_entry() {
            if *entry.key() >= end {
                break;
            }
            let (start, window) = entry.remove_entry();
            out.reserve(window.len());
            for (id, value) in window {
                let series = self.series.get_mut(&id).expect("series of open bucket");
                let key = series.key.clone();
                series.windows -= 1;
                if series.windows == 0 {
                    self.series.remove(&id);
                }
                out.push(Bucket {
                    start,
                    series: key,
                    value,
                });
            }
        }
        self.buckets -= out.len();
        out
    }
}

/// A bucket holding `line`, which has at least one value.
fn new_value(line: &Line<'_>, sketch: &funnel_sketch::Config) -> Value {
    let mut value = match line.kind {
        Kind::Counter => Value::Counter(0.0),
        Kind::Gauge => {
            let mut g = Gauge::new(line.values[0]);
            line.values[1..].iter().for_each(|&v| g.add(v));
            return Value::Gauge(g);
        }
        Kind::Distribution => Value::Distribution(Box::new(DDSketch::new(*sketch))),
    };
    merge(&mut value, line);
    value
}

fn merge(value: &mut Value, line: &Line<'_>) {
    match value {
        Value::Counter(sum) => {
            for v in &line.values {
                *sum += v / line.sample_rate;
            }
        }
        Value::Gauge(g) => {
            line.values.iter().for_each(|&v| g.add(v));
        }
        Value::Distribution(sketch) => {
            let weight = 1.0 / line.sample_rate;
            for &v in &line.values {
                sketch.add_n(v, weight);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::parse_line;

    fn agg() -> Aggregator {
        Aggregator::new(Config::default())
    }

    fn put(a: &mut Aggregator, now: u64, line: &str) -> Result<SeriesId, Reject> {
        a.insert(now, "t", &mut parse_line(line).unwrap())
    }

    #[test]
    fn counters_sum_with_sample_rate() {
        let mut a = agg();
        put(&mut a, 100, "hits:1|c").unwrap();
        put(&mut a, 101, "hits:2:3|c").unwrap();
        put(&mut a, 109, "hits:1|c|@0.5").unwrap();
        assert_eq!(a.open_buckets(), 1);
        let out = a.flush(115);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].start, 100);
        assert!(matches!(out[0].value, Value::Counter(s) if s == 8.0));
    }

    #[test]
    fn gauges_keep_min_max_last_sum() {
        let mut a = agg();
        for v in [5, 1, 9, 3] {
            put(&mut a, 100, &format!("q:{v}|g")).unwrap();
        }
        let out = a.flush_all();
        let Value::Gauge(g) = out[0].value else {
            panic!()
        };
        assert_eq!(
            g,
            Gauge {
                min: 1.0,
                max: 9.0,
                last: 3.0,
                sum: 18.0,
                count: 4
            }
        );
    }

    #[test]
    fn distributions_use_sketch_with_weights() {
        let mut a = agg();
        put(&mut a, 100, "lat:10:20|ms").unwrap();
        put(&mut a, 100, "lat:30|ms|@0.1").unwrap();
        let out = a.flush_all();
        let Value::Distribution(s) = &out[0].value else {
            panic!()
        };
        assert_eq!(s.count(), 12.0);
        assert_eq!(s.max(), Some(30.0));
    }

    #[test]
    fn windows_split_by_timestamp_and_series() {
        let mut a = agg();
        put(&mut a, 100, "a:1|c").unwrap();
        put(&mut a, 110, "a:1|c").unwrap();
        put(&mut a, 110, "a:1|c|#env:prod").unwrap();
        put(&mut a, 110, "a:1|g").unwrap();
        put(&mut a, 125, "a:1|c|T105").unwrap();
        assert_eq!(a.open_buckets(), 4);
        assert_eq!(a.open_series(), 3);
    }

    #[test]
    fn flush_respects_grace_and_rejects_late_points() {
        let mut a = agg();
        put(&mut a, 100, "a:1|c").unwrap();
        // Window [100, 110) closes at 115.
        assert!(a.flush(114).is_empty());
        assert_eq!(a.flush(115).len(), 1);
        assert_eq!(put(&mut a, 115, "a:1|c|T109"), Err(Reject::Late));
        assert!(put(&mut a, 115, "a:1|c|T110").is_ok());
        assert_eq!(a.open_series(), 1);
    }

    #[test]
    fn late_cutoff_advances_without_data() {
        let mut a = agg();
        assert!(a.flush(1000).is_empty());
        assert_eq!(put(&mut a, 1000, "a:1|c|T900"), Err(Reject::Late));
    }

    #[test]
    fn rejects_far_future() {
        let mut a = agg();
        assert_eq!(put(&mut a, 100, "a:1|c|T161"), Err(Reject::Future));
        assert!(put(&mut a, 100, "a:1|c|T160").is_ok());
    }

    #[test]
    fn overflow_caps_buckets_but_updates_existing() {
        let mut a = Aggregator::new(Config {
            max_buckets: 2,
            ..Config::default()
        });
        put(&mut a, 100, "a:1|c").unwrap();
        put(&mut a, 100, "b:1|c").unwrap();
        assert_eq!(put(&mut a, 100, "c:1|c"), Err(Reject::Overflow));
        assert_eq!(put(&mut a, 120, "a:1|c"), Err(Reject::Overflow));
        assert!(put(&mut a, 100, "a:5|c").is_ok());
        assert_eq!(a.flush_all().len(), 2);
        assert!(put(&mut a, 120, "c:1|c").is_ok());
    }

    #[test]
    fn series_freed_after_last_window_flushes() {
        let mut a = agg();
        put(&mut a, 100, "a:1|c").unwrap();
        put(&mut a, 110, "a:1|c").unwrap();
        assert_eq!(a.flush(115).len(), 1);
        assert_eq!(a.open_series(), 1);
        assert_eq!(a.flush(125).len(), 1);
        assert_eq!(a.open_series(), 0);
    }
}
