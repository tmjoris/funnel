//! One rollup row, and how rows merge into coarser ones.

use std::collections::BTreeMap;

use funnel_sketch::DDSketch;

/// A summary of one series over one time bucket.
///
/// Every field merges exactly, so a coarser row is the same summary a coarser bucket would have
/// produced from the raw points. That property is what lets the planner swap tables.
#[derive(Clone, Debug)]
pub struct Row {
    pub tenant: String,
    pub metric: String,
    /// Sorted `key=value` pairs joined by commas.
    pub tags: String,
    pub kind: &'static str,
    /// Bucket start, seconds since the epoch.
    pub ts: i64,
    pub count: f64,
    pub sum: f64,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub last: Option<f64>,
    /// Timestamp of `last`, so merging keeps the newest value.
    pub last_at: Option<i64>,
    pub sketch: Option<DDSketch>,
}

impl Row {
    /// Merges `other`, a row of the same series, into this one.
    pub fn merge(&mut self, other: &Row) -> Result<(), funnel_sketch::MergeError> {
        self.count += other.count;
        self.sum += other.sum;
        self.min = merge_option(self.min, other.min, f64::min);
        self.max = merge_option(self.max, other.max, f64::max);
        if other.last_at > self.last_at {
            self.last = other.last;
            self.last_at = other.last_at;
        }
        match (&mut self.sketch, &other.sketch) {
            (Some(a), Some(b)) => a.merge(b)?,
            (None, Some(b)) => self.sketch = Some(b.clone()),
            _ => {}
        }
        Ok(())
    }
}

fn merge_option(a: Option<f64>, b: Option<f64>, f: fn(f64, f64) -> f64) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(f(a, b)),
        (a, b) => a.or(b),
    }
}

/// Merges rows into buckets of `resolution` seconds, returning them sorted by series and time,
/// the order that compresses best and makes time ranges cheap to skip.
pub fn coarsen(rows: &[Row], resolution: i64) -> Result<Vec<Row>, funnel_sketch::MergeError> {
    let mut merged: BTreeMap<(&str, &str, &str, i64), Row> = BTreeMap::new();
    for row in rows {
        let start = row.ts - row.ts.rem_euclid(resolution);
        match merged.get_mut(&(&row.tenant, &row.metric, &row.tags, start)) {
            Some(acc) => acc.merge(row)?,
            None => {
                let mut first = row.clone();
                first.ts = start;
                merged.insert((&row.tenant, &row.metric, &row.tags, start), first);
            }
        }
    }
    Ok(merged.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gauge(ts: i64, value: f64) -> Row {
        Row {
            tenant: "t".into(),
            metric: "m".into(),
            tags: "host=a".into(),
            kind: "gauge",
            ts,
            count: 1.0,
            sum: value,
            min: Some(value),
            max: Some(value),
            last: Some(value),
            last_at: Some(ts),
            sketch: None,
        }
    }

    #[test]
    fn coarsening_aligns_buckets_and_merges_every_field() {
        let rows = [gauge(70, 1.0), gauge(80, 5.0), gauge(125, 2.0)];
        let out = coarsen(&rows, 60).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].ts, 60);
        assert_eq!((out[0].count, out[0].sum), (2.0, 6.0));
        assert_eq!((out[0].min, out[0].max), (Some(1.0), Some(5.0)));
        assert_eq!(out[1].ts, 120);
    }

    #[test]
    fn last_keeps_the_newest_value_whatever_the_order() {
        let out = coarsen(&[gauge(80, 5.0), gauge(70, 1.0)], 60).unwrap();
        assert_eq!(out[0].last, Some(5.0));
    }

    #[test]
    fn negative_timestamps_round_down() {
        assert_eq!(coarsen(&[gauge(-10, 1.0)], 60).unwrap()[0].ts, -60);
    }
}
