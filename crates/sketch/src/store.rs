//! Dense array of bin counts covering a contiguous index range.
//!
//! When the range of non-empty bins would exceed `max_bins`, the lowest bins
//! are folded into the lowest kept bin. Low quantiles lose their guarantee,
//! high quantiles (the ones latency dashboards care about) keep it.
//!
//! Counts are whole numbers of [`UNIT`]ths of a value, like cents of a
//! dollar, so that sampled values can carry fractional weights (a StatsD
//! value sent at `@0.3` stands for 3.33 values) while every add stays an
//! integer add. Adds saturate instead of wrapping; a store overflows only
//! after about 2.8e14 values.

/// Count units per value.
pub const UNIT: u64 = 1 << 16;

#[derive(Clone, Debug)]
pub struct Store {
    bins: Vec<u64>,
    /// Index of `bins[0]`.
    offset: i32,
    /// Lowest and highest non-empty indexes; meaningless while `count == 0`.
    min: i32,
    max: i32,
    count: u64,
    max_bins: u32,
    collapsed: bool,
}

impl Store {
    pub fn new(max_bins: u32) -> Self {
        assert!(max_bins > 0);
        Store {
            bins: Vec::new(),
            offset: 0,
            min: 0,
            max: 0,
            count: 0,
            max_bins,
            collapsed: false,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether any counts were folded into a higher bin.
    pub fn collapsed(&self) -> bool {
        self.collapsed
    }

    pub fn max_bins(&self) -> u32 {
        self.max_bins
    }

    #[inline]
    pub fn add(&mut self, index: i32, n: u64) {
        if n == 0 {
            return;
        }
        let rel = index.wrapping_sub(self.offset);
        if self.count > 0 && rel >= 0 && (rel as usize) < self.bins.len() {
            // Fast path: inside the allocated window, and the window is never
            // wider than max_bins + padding, so only the span check remains.
            if index >= self.min && index <= self.max {
                let slot = &mut self.bins[rel as usize];
                *slot = slot.saturating_add(n);
                self.count = self.count.saturating_add(n);
                return;
            }
        }
        self.add_slow(index, n);
    }

    fn add_slow(&mut self, mut index: i32, n: u64) {
        let (mut lo, hi) = if self.count == 0 {
            (index, index)
        } else {
            (self.min.min(index), self.max.max(index))
        };
        if (hi as i64 - lo as i64 + 1) > self.max_bins as i64 {
            lo = hi - (self.max_bins as i32 - 1);
            index = index.max(lo);
            self.collapse_below(lo);
        }
        let was_empty = self.count == 0;
        self.ensure(lo, hi);
        let slot = &mut self.bins[(index - self.offset) as usize];
        *slot = slot.saturating_add(n);
        self.count = self.count.saturating_add(n);
        self.min = if was_empty {
            index
        } else {
            self.min.min(index)
        };
        self.max = if was_empty {
            index
        } else {
            self.max.max(index)
        };
    }

    /// Folds every count below `lo` into bin `lo`.
    fn collapse_below(&mut self, lo: i32) {
        if self.count == 0 || self.min >= lo {
            return;
        }
        // Counts only move between bins, so the total stays as it is.
        let mut folded: u64 = 0;
        for i in self.min..lo.min(self.max + 1) {
            let slot = &mut self.bins[(i - self.offset) as usize];
            folded = folded.saturating_add(*slot);
            *slot = 0;
        }
        if folded == 0 {
            return;
        }
        self.collapsed = true;
        let max = self.max.max(lo);
        self.min = lo;
        self.ensure(lo, max);
        let slot = &mut self.bins[(lo - self.offset) as usize];
        *slot = slot.saturating_add(folded);
        self.max = max;
    }

    /// Makes `bins` cover `[lo, hi]`, dropping anything outside the new window
    /// (callers have already moved those counts) and padding for growth.
    fn ensure(&mut self, lo: i32, hi: i32) {
        let end = self.offset as i64 + self.bins.len() as i64;
        if !self.bins.is_empty() && lo >= self.offset && (hi as i64) < end {
            return;
        }
        let span = (hi - lo + 1) as usize;
        let pad = (span / 2 + 8).min(self.max_bins as usize) as i32;
        let new_offset = lo.saturating_sub(pad);
        let new_len = (hi.saturating_add(pad) - new_offset + 1) as usize;
        let mut bins = vec![0u64; new_len];
        if self.count > 0 {
            for i in self.min.max(new_offset)..=self.max.min(new_offset + new_len as i32 - 1) {
                bins[(i - new_offset) as usize] = self.bins[(i - self.offset) as usize];
            }
        }
        self.bins = bins;
        self.offset = new_offset;
    }

    /// Non-empty bins in ascending index order.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (i32, u64)> + '_ {
        let (lo, hi) = if self.count == 0 {
            (1, 0)
        } else {
            (self.min, self.max)
        };
        (lo..=hi).filter_map(move |i| {
            let c = self.bins[(i - self.offset) as usize];
            (c > 0).then_some((i, c))
        })
    }

    pub fn merge(&mut self, other: &Store) {
        self.collapsed |= other.collapsed;
        if other.count == 0 {
            return;
        }
        let (lo, hi) = if self.count == 0 {
            (other.min, other.max)
        } else {
            (self.min.min(other.min), self.max.max(other.max))
        };
        if (hi as i64 - lo as i64 + 1) > self.max_bins as i64 {
            // Needs collapsing; the per-bin path already handles that.
            for (i, c) in other.iter() {
                self.add(i, c);
            }
            return;
        }
        // Fits without collapsing: one resize, then add the overlapping
        // slices element-wise.
        self.ensure(lo, hi);
        let dst = (other.min - self.offset) as usize;
        let src = (other.min - other.offset) as usize;
        let len = (other.max - other.min + 1) as usize;
        for (d, s) in self.bins[dst..dst + len]
            .iter_mut()
            .zip(&other.bins[src..src + len])
        {
            *d = d.saturating_add(*s);
        }
        self.count = self.count.saturating_add(other.count);
        self.min = lo;
        self.max = hi;
    }

    pub fn memory_bytes(&self) -> usize {
        self.bins.capacity() * std::mem::size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_in_both_directions() {
        let mut s = Store::new(1000);
        for i in [5, -20, 300, 0, -20] {
            s.add(i, 1);
        }
        assert_eq!(
            s.iter().collect::<Vec<_>>(),
            [(-20, 2), (0, 1), (5, 1), (300, 1)]
        );
        assert!(!s.collapsed());
    }

    #[test]
    fn collapses_lowest_bins() {
        let mut s = Store::new(10);
        for i in 0..20 {
            s.add(i, 1);
        }
        assert_eq!(s.count(), 20);
        let bins: Vec<_> = s.iter().collect();
        assert_eq!(bins.first(), Some(&(10, 11)));
        assert_eq!(bins.last(), Some(&(19, 1)));
        assert!(s.collapsed());
        // A value far above pushes everything into one bin.
        s.add(1000, 1);
        assert_eq!(s.iter().collect::<Vec<_>>(), [(991, 20), (1000, 1)]);
    }

    #[test]
    fn low_value_after_collapse_is_folded_up() {
        let mut s = Store::new(4);
        s.add(10, 1);
        s.add(13, 1);
        s.add(-100, 1);
        assert_eq!(s.iter().collect::<Vec<_>>(), [(10, 2), (13, 1)]);
    }

    #[test]
    fn saturates_instead_of_wrapping() {
        let mut s = Store::new(10);
        s.add(1, u64::MAX - 1);
        s.add(1, 5);
        s.add(2, 5);
        assert_eq!(s.count(), u64::MAX);
        assert_eq!(s.iter().next(), Some((1, u64::MAX)));
    }
}
