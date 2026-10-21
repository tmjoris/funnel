//! Maps positive values to integer bin indexes so that every bin spans a
//! ratio of at most `gamma = (1 + alpha) / (1 - alpha)`.
//!
//! Any bin `[lo, hi]` with `hi / lo <= gamma` can be represented by
//! `2 * lo * hi / (lo + hi)`, which is within `alpha` relative error of every
//! value in the bin. That is the whole DDSketch guarantee; the mappings below
//! only differ in how cheaply they compute the index.

/// Smallest value that gets its own bin. Smaller magnitudes (subnormals) are
/// counted in the zero bin.
pub const MIN_INDEXABLE: f64 = f64::MIN_POSITIVE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MappingKind {
    /// `ceil(ln(x) / ln(gamma))`. Optimal bin count, one `ln` per insert.
    Logarithmic = 0,
    /// Reads the exponent bits and approximates `log2` of the significand with
    /// a cubic. About 1% more bins than `Logarithmic`, no transcendental call.
    Cubic = 1,
}

// Cubic approximation of log2(1 + t) on [0, 1]: P(0) = 0 and P(1) = 1, so the
// approximation is continuous across powers of two.
const A: f64 = 6.0 / 35.0;
const B: f64 = -3.0 / 5.0;
const C: f64 = 10.0 / 7.0;

/// The approximate log f(x) = e + P(m - 1) for x = 2^e * m moves at least
/// `MIN_SLOPE` units per unit of ln(x): d f / d ln(x) = P'(t) * (1 + t), whose
/// minimum on [0, 1] is at t = 0, where it equals C. A bin of width 1 / k in
/// f-space therefore spans at most 1 / (k * C) in ln-space.
const MIN_SLOPE: f64 = C;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Mapping {
    kind: MappingKind,
    alpha: f64,
    gamma: f64,
    multiplier: f64,
}

impl Mapping {
    pub fn new(kind: MappingKind, alpha: f64) -> Self {
        assert!(alpha > 0.0 && alpha < 1.0, "alpha must be in (0, 1)");
        let gamma = (1.0 + alpha) / (1.0 - alpha);
        let multiplier = match kind {
            MappingKind::Logarithmic => 1.0 / gamma.ln(),
            MappingKind::Cubic => 1.0 / (gamma.ln() * MIN_SLOPE),
        };
        Mapping {
            kind,
            alpha,
            gamma,
            multiplier,
        }
    }

    pub fn kind(&self) -> MappingKind {
        self.kind
    }

    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    pub fn gamma(&self) -> f64 {
        self.gamma
    }

    /// Bin index for `x`. `x` must be finite and at least `MIN_INDEXABLE`.
    #[inline]
    pub fn index(&self, x: f64) -> i32 {
        debug_assert!(x >= MIN_INDEXABLE && x.is_finite());
        let log = match self.kind {
            MappingKind::Logarithmic => x.ln(),
            MappingKind::Cubic => approx_log2(x),
        };
        (log * self.multiplier).ceil() as i32
    }

    /// Smallest value that maps to `index` (exclusive bound of the bin below).
    pub fn lower_bound(&self, index: i32) -> f64 {
        self.inverse((index - 1) as f64 / self.multiplier)
    }

    /// Largest value that maps to `index`.
    pub fn upper_bound(&self, index: i32) -> f64 {
        self.inverse(index as f64 / self.multiplier)
    }

    /// Representative value of a bin, within `alpha` of everything in it.
    pub fn value(&self, index: i32) -> f64 {
        let lo = self.lower_bound(index);
        let hi = self.upper_bound(index);
        2.0 * lo * hi / (lo + hi)
    }

    fn inverse(&self, log: f64) -> f64 {
        match self.kind {
            MappingKind::Logarithmic => log.exp(),
            MappingKind::Cubic => approx_exp2(log),
        }
    }
}

#[inline]
fn approx_log2(x: f64) -> f64 {
    const MANTISSA: u64 = (1 << 52) - 1;
    let bits = x.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i64 - 1023;
    let t = f64::from_bits((bits & MANTISSA) | 1.0f64.to_bits()) - 1.0;
    exponent as f64 + ((A * t + B) * t + C) * t
}

/// Inverse of `approx_log2`. Only used at query time, so a few Newton steps
/// are fine; P is strictly increasing on [0, 1] (P' >= 26/35 there).
fn approx_exp2(y: f64) -> f64 {
    let exponent = y.floor();
    let r = y - exponent;
    let mut t = r;
    for _ in 0..8 {
        let p = ((A * t + B) * t + C) * t - r;
        let dp = (3.0 * A * t + 2.0 * B) * t + C;
        t = (t - p / dp).clamp(0.0, 1.0);
    }
    (1.0 + t) * 2f64.powi(exponent as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bins_respect_gamma() {
        for kind in [MappingKind::Logarithmic, MappingKind::Cubic] {
            for alpha in [0.001, 0.01, 0.05] {
                let m = Mapping::new(kind, alpha);
                for index in -2000..2000 {
                    let ratio = m.upper_bound(index) / m.lower_bound(index);
                    assert!(
                        ratio <= m.gamma() * (1.0 + 1e-12),
                        "{kind:?} {alpha} {index}"
                    );
                }
            }
        }
    }

    #[test]
    fn index_lands_inside_its_bounds() {
        let m = Mapping::new(MappingKind::Cubic, 0.01);
        let mut x = 1e-300;
        while x < 1e300 {
            let i = m.index(x);
            assert!(m.lower_bound(i) <= x * (1.0 + 1e-12) && x <= m.upper_bound(i) * (1.0 + 1e-12));
            x *= 1.37;
        }
    }

    #[test]
    fn cubic_is_continuous_at_powers_of_two() {
        for e in -10..10 {
            let x = 2f64.powi(e);
            let below = approx_log2(x * (1.0 - 1e-15));
            assert!((approx_log2(x) - below).abs() < 1e-9);
        }
    }
}
