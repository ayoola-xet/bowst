//! A log-linear latency histogram.
//!
//! Values below [`SUB_BUCKETS`] nanoseconds are counted exactly. Above that, each power of
//! two is split into [`SUB_BUCKETS`] equal buckets, so a value is placed in a bucket at most
//! `1 / SUB_BUCKETS` (about 3%) wider than the value itself. Reported percentiles are the
//! upper bound of their bucket, capped at the largest value recorded: they never understate a
//! latency and overstate it by at most about 3%.
//!
//! Integer arithmetic only: percentiles are requested in parts per million (`990_000` is the
//! 99th percentile), so no floating point is involved.

/// Sub-buckets per power of two, as a power of two.
const SUB_BITS: u32 = 5;
/// Sub-buckets per power of two. Also the number of values counted exactly.
pub(crate) const SUB_BUCKETS: u64 = 1 << SUB_BITS;
/// Groups: one exact group, then one per power of two from `2^5` to `2^63`.
const GROUPS: usize = 60;
/// Total buckets.
const BUCKETS: usize = GROUPS * 32;
const _: () = assert!(GROUPS == 64 - 5 + 1 && SUB_BITS == 5 && SUB_BUCKETS == 32);

/// Durations in nanoseconds, bucketed for percentiles. See the module docs.
#[derive(Clone, Debug)]
pub struct LatencyHistogram {
    counts: Box<[u64; BUCKETS]>,
    count: u64,
    max: u64,
    min: u64,
}

/// Percentiles and extremes of a histogram, in nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LatencySummary {
    /// Values recorded.
    pub count: u64,
    /// Smallest value (exact).
    pub min: u64,
    /// Median.
    pub p50: u64,
    /// 90th percentile.
    pub p90: u64,
    /// 99th percentile.
    pub p99: u64,
    /// 99.9th percentile.
    pub p999: u64,
    /// Largest value (exact).
    pub max: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    /// An empty histogram. Allocates its buckets (about 15 KiB) once, here.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counts: Box::new([0; BUCKETS]),
            count: 0,
            max: 0,
            min: u64::MAX,
        }
    }

    /// Records one value. Constant time, no allocation: safe on the hot path.
    #[inline]
    pub fn record(&mut self, nanos: u64) {
        if let Some(slot) = self.counts.get_mut(bucket_of(nanos)) {
            *slot = slot.saturating_add(1);
        }
        self.count = self.count.saturating_add(1);
        self.max = self.max.max(nanos);
        self.min = self.min.min(nanos);
    }

    /// Values recorded.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Largest value recorded, or 0 if empty.
    #[must_use]
    pub fn max(&self) -> u64 {
        self.max
    }

    /// Smallest value recorded, or 0 if empty.
    #[must_use]
    pub fn min(&self) -> u64 {
        if self.count == 0 { 0 } else { self.min }
    }

    /// The value at `ppm` parts per million (`500_000` is the median, `999_000` the 99.9th
    /// percentile): the smallest bucket upper bound with at least that share of values at or
    /// below it, capped at [`max`](Self::max). 0 if empty. `ppm` above one million is treated
    /// as one million.
    #[must_use]
    pub fn value_at_ppm(&self, ppm: u32) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let rank = rank_for(self.count, ppm.min(1_000_000));
        let mut seen: u64 = 0;
        for (index, &n) in self.counts.iter().enumerate() {
            seen = seen.saturating_add(n);
            if seen >= rank {
                return upper_bound(index).min(self.max);
            }
        }
        self.max
    }

    /// Standard percentiles and extremes. Scans every bucket: not for the hot path.
    #[must_use]
    pub fn summary(&self) -> LatencySummary {
        LatencySummary {
            count: self.count,
            min: self.min(),
            p50: self.value_at_ppm(500_000),
            p90: self.value_at_ppm(900_000),
            p99: self.value_at_ppm(990_000),
            p999: self.value_at_ppm(999_000),
            max: self.max,
        }
    }

    /// Adds every value recorded in `other`.
    pub fn merge(&mut self, other: &Self) {
        for (mine, theirs) in self.counts.iter_mut().zip(other.counts.iter()) {
            *mine = mine.saturating_add(*theirs);
        }
        self.count = self.count.saturating_add(other.count);
        self.max = self.max.max(other.max);
        self.min = self.min.min(other.min);
    }

    /// Forgets every value, keeping the allocation.
    pub fn reset(&mut self) {
        self.counts.fill(0);
        self.count = 0;
        self.max = 0;
        self.min = u64::MAX;
    }
}

/// The 1-based rank of the value at `ppm` parts per million of `count` values, rounded up so
/// the result is never below the requested share. At least 1.
fn rank_for(count: u64, ppm: u32) -> u64 {
    let scaled = u128::from(count).saturating_mul(u128::from(ppm));
    let rank = scaled.div_ceil(1_000_000);
    u64::try_from(rank).unwrap_or(u64::MAX).max(1)
}

/// Bucket index of a value.
#[inline]
pub(crate) fn bucket_of(value: u64) -> usize {
    if value < SUB_BUCKETS {
        return usize::try_from(value).unwrap_or(0);
    }
    // `value >= 32`, so its highest set bit `top` is in 5..=63 and nothing below can wrap.
    let top = 63_u32.wrapping_sub(value.leading_zeros());
    let shift = top.wrapping_sub(SUB_BITS);
    let group = u64::from(shift.wrapping_add(1));
    // `value >> shift` is in `32..64`.
    let sub = (value >> shift).wrapping_sub(SUB_BUCKETS);
    let index = group.wrapping_mul(SUB_BUCKETS).wrapping_add(sub);
    usize::try_from(index).unwrap_or(BUCKETS.saturating_sub(1))
}

/// Largest value that falls in bucket `index`.
pub(crate) fn upper_bound(index: usize) -> u64 {
    if index >= BUCKETS {
        return u64::MAX;
    }
    let index = u64::try_from(index).unwrap_or(u64::MAX);
    if index < SUB_BUCKETS {
        return index;
    }
    let group = index / SUB_BUCKETS;
    let sub = index % SUB_BUCKETS;
    // `group` is in 1..60, so `shift <= 58` and `(32 + sub) << shift` fits in 64 bits.
    let Ok(shift) = u32::try_from(group.saturating_sub(1)) else {
        return u64::MAX;
    };
    let low = SUB_BUCKETS.saturating_add(sub).checked_shl(shift);
    let width = 1_u64.checked_shl(shift);
    match (low, width) {
        (Some(low), Some(width)) => low.saturating_add(width.saturating_sub(1)),
        _ => u64::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn small_values_are_exact() {
        for v in 0..SUB_BUCKETS {
            assert_eq!(upper_bound(bucket_of(v)), v);
        }
    }

    #[test]
    fn buckets_are_contiguous_and_cover_every_value() {
        // Each bucket starts right after the previous one ends, up to u64::MAX.
        let mut expected_low = 0_u64;
        for index in 0..BUCKETS {
            let high = upper_bound(index);
            assert_eq!(bucket_of(expected_low), index, "low end of bucket {index}");
            assert_eq!(bucket_of(high), index, "high end of bucket {index}");
            if high == u64::MAX {
                assert_eq!(index, BUCKETS - 1);
                return;
            }
            expected_low = high + 1;
        }
        panic!("buckets end before u64::MAX");
    }

    #[test]
    fn empty_histogram_reports_zeros() {
        let h = LatencyHistogram::new();
        assert_eq!(h.summary(), LatencySummary::default());
    }

    #[test]
    fn percentiles_of_a_known_distribution() {
        let mut h = LatencyHistogram::new();
        for v in 1..=1_000 {
            h.record(v);
        }
        let s = h.summary();
        assert_eq!((s.count, s.min, s.max), (1_000, 1, 1_000));
        // Exact rank values are 500, 900, 990 and 999; buckets overstate by at most ~3%.
        for (got, exact) in [(s.p50, 500), (s.p90, 900), (s.p99, 990), (s.p999, 999)] {
            assert!(
                got >= exact && got <= exact + exact / 32,
                "{got} vs {exact}"
            );
        }
    }

    #[test]
    fn merge_and_reset() {
        let (mut a, mut b) = (LatencyHistogram::new(), LatencyHistogram::new());
        a.record(10);
        b.record(1_000_000);
        a.merge(&b);
        assert_eq!((a.count(), a.min(), a.max()), (2, 10, 1_000_000));
        a.reset();
        assert_eq!(a.summary(), LatencySummary::default());
    }

    #[test]
    fn extreme_values_do_not_overflow() {
        let mut h = LatencyHistogram::new();
        h.record(u64::MAX);
        h.record(0);
        assert_eq!(h.value_at_ppm(1_000_000), u64::MAX);
        assert_eq!(h.value_at_ppm(0), 0);
        assert_eq!(h.value_at_ppm(u32::MAX), u64::MAX);
    }

    proptest! {
        /// A bucket's width is at most 1/32 of its lower bound.
        #[test]
        fn relative_error_is_bounded(value in 0_u64..u64::MAX) {
            let high = upper_bound(bucket_of(value));
            prop_assert!(high >= value);
            prop_assert!(high - value <= value / SUB_BUCKETS);
        }

        /// Any percentile is at or above the exact value of that rank, and within one bucket
        /// of it.
        #[test]
        fn percentiles_bracket_the_exact_value(
            mut values in proptest::collection::vec(0_u64..10_000_000_000, 1..400),
            ppm in 0_u32..=1_000_000,
        ) {
            let mut h = LatencyHistogram::new();
            for &v in &values {
                h.record(v);
            }
            values.sort_unstable();
            let rank = rank_for(u64::try_from(values.len()).unwrap(), ppm);
            let exact = values[usize::try_from(rank).unwrap() - 1];
            let got = h.value_at_ppm(ppm);
            prop_assert!(got >= exact, "{got} < exact {exact}");
            prop_assert!(got - exact <= exact / SUB_BUCKETS, "{got} too far above {exact}");
            prop_assert!(got <= *values.last().unwrap());
        }
    }
}
