//! DDSketch: a mergeable quantile sketch with a relative-error guarantee.
//!
//! For any quantile `q`, the returned value is within `alpha` relative error
//! of the exact value at rank `floor(q * (n - 1))`, as long as the bins
//! holding that rank were not collapsed (see [`DDSketch::collapsed`]).
//! Count, sum, min and max are exact.
//!
//! Reference: Masson, Rim, Lee, "DDSketch: A Fast and Fully-Mergeable
//! Quantile Sketch with Relative-Error Guarantees", VLDB 2019.

mod encoding;
pub mod mapping;
pub mod store;

pub use encoding::DecodeError;
pub use mapping::{Mapping, MappingKind};

use mapping::MIN_INDEXABLE;
use store::{Store, UNIT};

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub alpha: f64,
    pub mapping: MappingKind,
    /// Upper bound on bins per sign. 2048 bins at alpha = 1% cover values
    /// spanning a factor of about 1e17, so collapsing is rare in practice.
    pub max_bins: u32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            alpha: 0.01,
            mapping: MappingKind::Cubic,
            max_bins: 2048,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DDSketch {
    mapping: Mapping,
    positive: Store,
    negative: Store,
    /// Counts are in `store::UNIT`ths of a value.
    zero: u64,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl Default for DDSketch {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl DDSketch {
    pub fn new(config: Config) -> Self {
        DDSketch {
            mapping: Mapping::new(config.mapping, config.alpha),
            positive: Store::new(config.max_bins),
            negative: Store::new(config.max_bins),
            zero: 0,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    pub fn config(&self) -> Config {
        Config {
            alpha: self.mapping.alpha(),
            mapping: self.mapping.kind(),
            max_bins: self.positive.max_bins(),
        }
    }

    /// Adds one value. NaN and infinities are ignored and reported as `false`.
    #[inline]
    pub fn add(&mut self, x: f64) -> bool {
        self.add_units(x, UNIT, 1.0)
    }

    /// Adds `x` with weight `n`, e.g. `1 / rate` for a StatsD value sent with
    /// a sample rate. Weights need not be whole: they are stored in units of
    /// 1/65536, so 1/0.3 is kept to within about one part in a million.
    /// Non-finite values, and weights below 1/65536 or above about 1.4e14,
    /// are ignored and reported as `false`.
    #[inline]
    pub fn add_n(&mut self, x: f64, n: f64) -> bool {
        let units = (n * UNIT as f64).round();
        if !(units >= 1.0 && units < (1u64 << 63) as f64) {
            return false;
        }
        self.add_units(x, units as u64, n)
    }

    #[inline]
    fn add_units(&mut self, x: f64, units: u64, weight: f64) -> bool {
        if !x.is_finite() {
            return false;
        }
        if x >= MIN_INDEXABLE {
            self.positive.add(self.mapping.index(x), units);
        } else if x <= -MIN_INDEXABLE {
            self.negative.add(self.mapping.index(-x), units);
        } else {
            self.zero = self.zero.saturating_add(units);
        }
        self.count = self.count.saturating_add(units);
        self.sum += x * weight;
        self.min = self.min.min(x);
        self.max = self.max.max(x);
        true
    }

    /// Total weight; the number of values when every weight is 1.
    pub fn count(&self) -> f64 {
        self.count as f64 / UNIT as f64
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn sum(&self) -> f64 {
        self.sum
    }

    pub fn min(&self) -> Option<f64> {
        (!self.is_empty()).then_some(self.min)
    }

    pub fn max(&self) -> Option<f64> {
        (!self.is_empty()).then_some(self.max)
    }

    pub fn mean(&self) -> Option<f64> {
        (!self.is_empty()).then(|| self.sum / self.count())
    }

    /// Whether the bin limit forced low bins to merge, which voids the
    /// guarantee for the lowest positive (or highest negative) quantiles.
    pub fn collapsed(&self) -> bool {
        self.positive.collapsed() || self.negative.collapsed()
    }

    /// Estimated value at quantile `q` in `[0, 1]`.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.is_empty() || !(0.0..=1.0).contains(&q) {
            return None;
        }
        if q == 0.0 {
            return Some(self.min);
        }
        if q == 1.0 {
            return Some(self.max);
        }
        // Rank in units; UNIT is a power of two, so this scales exactly.
        let rank = q * (self.count as f64 - UNIT as f64);
        let mut seen: u64 = 0;
        // Negative values ascend as their magnitude index descends.
        for (i, c) in self.negative.iter().rev() {
            seen = seen.saturating_add(c);
            if seen as f64 > rank {
                return Some(self.clamp(-self.mapping.value(i)));
            }
        }
        seen = seen.saturating_add(self.zero);
        if seen as f64 > rank {
            return Some(self.clamp(0.0));
        }
        for (i, c) in self.positive.iter() {
            seen = seen.saturating_add(c);
            if seen as f64 > rank {
                return Some(self.clamp(self.mapping.value(i)));
            }
        }
        Some(self.max)
    }

    /// The exact answer lies in [min, max], so clamping never adds error.
    fn clamp(&self, v: f64) -> f64 {
        v.clamp(self.min, self.max)
    }

    /// Merges `other` into `self`. Both must use the same alpha and mapping.
    pub fn merge(&mut self, other: &DDSketch) -> Result<(), MergeError> {
        if self.mapping != other.mapping {
            return Err(MergeError);
        }
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
        self.zero = self.zero.saturating_add(other.zero);
        self.count = self.count.saturating_add(other.count);
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        Ok(())
    }

    /// Heap bytes held by the bin arrays.
    pub fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.positive.memory_bytes() + self.negative.memory_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeError;

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("sketches use different mappings")
    }
}

impl std::error::Error for MergeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let s = DDSketch::default();
        assert_eq!(s.quantile(0.5), None);
        assert_eq!(s.min(), None);
    }

    #[test]
    fn exact_stats_and_quantiles() {
        let mut s = DDSketch::default();
        for x in 1..=1000 {
            s.add(x as f64);
        }
        assert_eq!(s.count(), 1000.0);
        assert_eq!(s.sum(), 500500.0);
        assert_eq!(s.min(), Some(1.0));
        assert_eq!(s.max(), Some(1000.0));
        let p99 = s.quantile(0.99).unwrap();
        assert!((p99 - 990.0).abs() <= 990.0 * 0.01, "{p99}");
    }

    #[test]
    fn mixed_signs_and_zero() {
        let mut s = DDSketch::default();
        for x in [-100.0, -1.0, 0.0, 0.0, 5.0] {
            s.add(x);
        }
        assert_eq!(s.quantile(0.0), Some(-100.0));
        assert!((s.quantile(0.25).unwrap() + 1.0).abs() <= 0.01);
        assert_eq!(s.quantile(0.5), Some(0.0));
        assert_eq!(s.quantile(1.0), Some(5.0));
    }

    #[test]
    fn ignores_non_finite() {
        let mut s = DDSketch::default();
        assert!(!s.add(f64::NAN));
        assert!(!s.add(f64::INFINITY));
        assert!(s.is_empty());
    }

    #[test]
    fn merge_rejects_different_alpha() {
        let mut a = DDSketch::default();
        let b = DDSketch::new(Config {
            alpha: 0.02,
            ..Config::default()
        });
        assert_eq!(a.merge(&b), Err(MergeError));
    }
}
