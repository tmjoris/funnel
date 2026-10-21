//! Rolls points up into fixed time windows per series.
//!
//! A live window flushes `grace` seconds after it ends. Points can still
//! arrive for a window that has already been flushed (buffering agents,
//! clock skew, a restart that flushed open windows early), and up to
//! `max_past_secs` old they are accepted: they open a new, partial bucket
//! for that window, which waits one bucket width for more late points
//! before it flushes. The same series and window can therefore appear in
//! more than one flushed bucket, and readers combine them with
//! [`Value::merge`]. Every aggregate here merges exactly, which is what
//! makes accepting late data cheap.

use std::collections::BTreeMap;
use std::sync::Arc;

use funnel_sketch::DDSketch;

use crate::protocol::{Kind, Line};
use crate::series::{IdMap, SeriesId, SeriesKey, normalize_tags, series_id};

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub bucket_secs: u64,
    pub grace_secs: u64,
    /// Points older than this, relative to the receive time, are rejected.
    pub max_past_secs: u64,
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
            max_past_secs: 24 * 3600,
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
    /// Timestamp of `last`, so merged partial buckets keep the newest value.
    pub last_at: u64,
    pub sum: f64,
    pub count: u64,
}

impl Gauge {
    fn new(v: f64, at: u64) -> Self {
        Gauge {
            min: v,
            max: v,
            last: v,
            last_at: at,
            sum: v,
            count: 1,
        }
    }

    fn add(&mut self, v: f64, at: u64) {
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        // Ties go to the value processed later.
        if at >= self.last_at {
            self.last = v;
            self.last_at = at;
        }
        self.sum += v;
        self.count += 1;
    }

    fn merge(&mut self, other: &Gauge) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        if other.last_at >= self.last_at {
            self.last = other.last;
            self.last_at = other.last_at;
        }
        self.sum += other.sum;
        self.count += other.count;
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Counter(f64),
    Gauge(Gauge),
    Distribution(Box<DDSketch>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KindMismatch;

impl Value {
    pub fn kind(&self) -> Kind {
        match self {
            Value::Counter(_) => Kind::Counter,
            Value::Gauge(_) => Kind::Gauge,
            Value::Distribution(_) => Kind::Distribution,
        }
    }

    /// Combines two buckets of the same series and window. For gauges,
    /// `other` wins a tie on `last_at`, so pass buckets oldest first.
    pub fn merge(&mut self, other: &Value) -> Result<(), KindMismatch> {
        match (self, other) {
            (Value::Counter(a), Value::Counter(b)) => *a += b,
            (Value::Gauge(a), Value::Gauge(b)) => a.merge(b),
            (Value::Distribution(a), Value::Distribution(b)) => {
                a.merge(b).map_err(|_| KindMismatch)?
            }
            _ => return Err(KindMismatch),
        }
        Ok(())
    }
}

/// A flushed bucket, ready to be written. Possibly partial; see the module
/// docs.
#[derive(Clone, Debug)]
pub struct Bucket {
    pub start: u64,
    pub series: Arc<SeriesKey>,
    pub value: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    /// Older than `max_past_secs`.
    TooOld,
    /// The timestamp is too far ahead of the receive time.
    Future,
    /// `max_buckets` is reached and the point needs a new bucket.
    Overflow,
}

struct Window {
    flush_at: u64,
    buckets: IdMap<Value>,
}

struct SeriesEntry {
    key: Arc<SeriesKey>,
    /// Open windows holding a bucket for this series.
    windows: u32,
}

pub struct Aggregator {
    config: Config,
    windows: BTreeMap<u64, Window>,
    series: IdMap<SeriesEntry>,
    buckets: usize,
}

impl Aggregator {
    pub fn new(config: Config) -> Self {
        assert!(config.bucket_secs > 0);
        Aggregator {
            config,
            windows: BTreeMap::new(),
            series: IdMap::default(),
            buckets: 0,
        }
    }

    pub fn open_buckets(&self) -> usize {
        self.buckets
    }

    pub fn open_series(&self) -> usize {
        self.series.len()
    }

    /// Checks a line's timestamp without storing anything, so callers can
    /// reject it before it uses up a series slot in the limiter.
    pub fn check_time(&self, now: u64, line: &Line<'_>) -> Result<(), Reject> {
        let ts = line.timestamp.unwrap_or(now);
        if ts > now.saturating_add(self.config.max_future_secs) {
            Err(Reject::Future)
        } else if ts.saturating_add(self.config.max_past_secs) < now {
            Err(Reject::TooOld)
        } else {
            Ok(())
        }
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
        self.check_time(now, line)?;
        let ts = line.timestamp.unwrap_or(now);
        let width = self.config.bucket_secs;
        let start = ts - ts % width;
        let live_close = start + width + self.config.grace_secs;
        let window = self.windows.entry(start).or_insert_with(|| Window {
            // A window opened after its normal close is collecting late
            // points; give it one bucket width to gather more.
            flush_at: if live_close > now {
                live_close
            } else {
                now + width
            },
            buckets: IdMap::default(),
        });
        match window.buckets.get_mut(&id) {
            Some(value) => add_line(value, line, ts),
            None => {
                if self.buckets >= self.config.max_buckets {
                    if window.buckets.is_empty() {
                        self.windows.remove(&start);
                    }
                    return Err(Reject::Overflow);
                }
                window
                    .buckets
                    .insert(id, new_value(line, ts, &self.config.sketch));
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

    /// Flushes every window whose time has come.
    pub fn flush(&mut self, now: u64) -> Vec<Bucket> {
        let due: Vec<u64> = self
            .windows
            .iter()
            .filter(|(_, w)| w.flush_at <= now)
            .map(|(start, _)| *start)
            .collect();
        self.drain(due)
    }

    /// Flushes all windows, e.g. on shutdown.
    pub fn flush_all(&mut self) -> Vec<Bucket> {
        let all: Vec<u64> = self.windows.keys().copied().collect();
        self.drain(all)
    }

    fn drain(&mut self, starts: Vec<u64>) -> Vec<Bucket> {
        let mut out = Vec::new();
        for start in starts {
            let window = self.windows.remove(&start).expect("window to drain");
            out.reserve(window.buckets.len());
            for (id, value) in window.buckets {
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
fn new_value(line: &Line<'_>, ts: u64, sketch: &funnel_sketch::Config) -> Value {
    let mut value = match line.kind {
        Kind::Counter => Value::Counter(0.0),
        Kind::Gauge => {
            let mut g = Gauge::new(line.values[0], ts);
            line.values[1..].iter().for_each(|&v| g.add(v, ts));
            return Value::Gauge(g);
        }
        Kind::Distribution => Value::Distribution(Box::new(DDSketch::new(*sketch))),
    };
    add_line(&mut value, line, ts);
    value
}

fn add_line(value: &mut Value, line: &Line<'_>, ts: u64) {
    match value {
        Value::Counter(sum) => {
            for v in &line.values {
                *sum += v / line.sample_rate;
            }
        }
        Value::Gauge(g) => {
            line.values.iter().for_each(|&v| g.add(v, ts));
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
                last_at: 100,
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
    fn flush_respects_grace() {
        let mut a = agg();
        put(&mut a, 100, "a:1|c").unwrap();
        // Window [100, 110) closes at 115.
        assert!(a.flush(114).is_empty());
        assert_eq!(a.flush(115).len(), 1);
    }

    #[test]
    fn late_points_form_a_partial_bucket_that_merges() {
        let mut a = agg();
        put(&mut a, 100, "a:1|c").unwrap();
        put(&mut a, 100, "g:5|g").unwrap();
        let first = a.flush(115);
        assert_eq!(first.len(), 2);
        // Late points for the flushed window are accepted and held for one
        // bucket width (10s) before flushing.
        put(&mut a, 120, "a:2|c|T105").unwrap();
        put(&mut a, 125, "a:3|c|T109").unwrap();
        put(&mut a, 125, "g:7|g|T106").unwrap();
        assert!(a.flush(129).is_empty());
        let second = a.flush(130);
        assert_eq!(second.len(), 2);
        assert!(second.iter().all(|b| b.start == 100));

        let by_kind = |out: &[Bucket], k: Kind| {
            out.iter()
                .find(|b| b.value.kind() == k)
                .unwrap()
                .value
                .clone()
        };
        let mut counter = by_kind(&first, Kind::Counter);
        counter.merge(&by_kind(&second, Kind::Counter)).unwrap();
        assert!(matches!(counter, Value::Counter(s) if s == 6.0));

        let mut gauge = by_kind(&first, Kind::Gauge);
        gauge.merge(&by_kind(&second, Kind::Gauge)).unwrap();
        let Value::Gauge(g) = gauge else { panic!() };
        assert_eq!((g.min, g.max, g.last, g.count), (5.0, 7.0, 7.0, 2));
    }

    #[test]
    fn gauge_last_follows_timestamps_not_arrival() {
        let mut a = agg();
        put(&mut a, 100, "g:1|g|T108").unwrap();
        put(&mut a, 100, "g:2|g|T103").unwrap();
        let Value::Gauge(g) = a.flush_all()[0].value else {
            panic!()
        };
        assert_eq!((g.last, g.last_at), (1.0, 108));
    }

    #[test]
    fn merge_rejects_mismatched_kinds() {
        let mut c = Value::Counter(1.0);
        assert_eq!(
            c.merge(&Value::Gauge(Gauge::new(1.0, 0))),
            Err(KindMismatch)
        );
    }

    #[test]
    fn rejects_too_old() {
        let mut a = agg();
        let now = 100_000;
        assert_eq!(put(&mut a, now, "a:1|c|T13599"), Err(Reject::TooOld));
        assert!(put(&mut a, now, "a:1|c|T13600").is_ok());
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
