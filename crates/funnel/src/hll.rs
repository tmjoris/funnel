//! HyperLogLog with 2^12 one-byte registers (4 KiB, about 1.6% standard
//! error), fed with series ids that are already uniform 64-bit hashes.
//!
//! Flajolet, Fusy, Gandouet, Meunier, "HyperLogLog: the analysis of a
//! near-optimal cardinality estimation algorithm", 2007, with the
//! linear-counting correction for small cardinalities.

const P: u32 = 12;
const M: usize = 1 << P;

#[derive(Clone)]
pub struct HyperLogLog {
    registers: Box<[u8; M]>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        HyperLogLog {
            registers: Box::new([0; M]),
        }
    }
}

impl HyperLogLog {
    /// Adds a hash. Returns whether a register changed, i.e. whether the
    /// estimate can have moved.
    #[inline]
    pub fn insert(&mut self, hash: u64) -> bool {
        let index = (hash >> (64 - P)) as usize;
        // Rank of the first set bit in the remaining 52 bits, 1-based; the
        // sentinel bit caps it at 53 when they are all zero.
        let rest = (hash << P) | (1 << (P - 1));
        let rank = rest.leading_zeros() as u8 + 1;
        let slot = &mut self.registers[index];
        if rank > *slot {
            *slot = rank;
            true
        } else {
            false
        }
    }

    pub fn estimate(&self) -> f64 {
        let m = M as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let mut sum = 0.0;
        let mut zeros = 0;
        for &r in self.registers.iter() {
            sum += 2f64.powi(-(r as i32));
            zeros += (r == 0) as usize;
        }
        let raw = alpha * m * m / sum;
        if raw <= 2.5 * m && zeros > 0 {
            m * (m / zeros as f64).ln()
        } else {
            raw
        }
    }

    pub fn merge(&mut self, other: &HyperLogLog) {
        for (a, b) in self.registers.iter_mut().zip(other.registers.iter()) {
            *a = (*a).max(*b);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh3::xxh3_64;

    fn estimate(n: u64, salt: u64) -> f64 {
        let mut h = HyperLogLog::default();
        for i in 0..n {
            h.insert(xxh3_64(&(i ^ salt << 40).to_le_bytes()));
        }
        h.estimate()
    }

    #[test]
    fn empty_is_zero() {
        assert_eq!(HyperLogLog::default().estimate(), 0.0);
    }

    #[test]
    fn within_five_percent_across_scales() {
        // Five standard errors is about 8%; 5% keeps the test meaningful
        // while staying deterministic for these inputs.
        for n in [100, 1_000, 10_000, 100_000, 1_000_000] {
            let e = estimate(n, 1);
            let err = (e - n as f64).abs() / n as f64;
            assert!(err < 0.05, "n={n} estimate={e:.0} err={err:.3}");
        }
    }

    #[test]
    fn duplicates_do_not_count() {
        let mut h = HyperLogLog::default();
        for _ in 0..10 {
            for i in 0u64..1000 {
                h.insert(xxh3_64(&i.to_le_bytes()));
            }
        }
        assert!((h.estimate() - 1000.0).abs() < 50.0);
    }

    #[test]
    fn merge_is_union() {
        let mut a = HyperLogLog::default();
        let mut b = HyperLogLog::default();
        for i in 0u64..20_000 {
            let h = xxh3_64(&i.to_le_bytes());
            if i < 15_000 {
                a.insert(h);
            }
            if i >= 5_000 {
                b.insert(h);
            }
        }
        a.merge(&b);
        assert!((a.estimate() - 20_000.0).abs() < 1_000.0);
    }
}
