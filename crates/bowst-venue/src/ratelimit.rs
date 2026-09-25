//! Token-bucket rate limiting for venue request budgets (REST weight, order counts).
//!
//! Pure: the current time is passed in. The bucket can also be corrected from the venue's own
//! usage reports (for example Binance's `X-MBX-USED-WEIGHT-1M`), so other clients on the same
//! IP or account are accounted for.

use core::time::Duration;

use bowst_core::MonoTime;

/// A token bucket refilled evenly over a window.
#[derive(Clone, Debug)]
pub struct TokenBucket {
    capacity: u64,
    window: Duration,
    /// Tokens scaled by `window` nanoseconds, so refills stay exact integers.
    scaled: u128,
    last: MonoTime,
}

impl TokenBucket {
    /// A full bucket allowing `capacity` tokens per `window`.
    #[must_use]
    pub fn new(capacity: u64, window: Duration, now: MonoTime) -> Self {
        let window = window.max(Duration::from_nanos(1));
        Self {
            capacity,
            window,
            scaled: u128::from(capacity).saturating_mul(window.as_nanos()),
            last: now,
        }
    }

    fn refill(&mut self, now: MonoTime) {
        let elapsed = now.saturating_since(self.last).as_nanos();
        self.last = self.last.max(now);
        let full = u128::from(self.capacity).saturating_mul(self.window.as_nanos());
        self.scaled = self
            .scaled
            .saturating_add(elapsed.saturating_mul(u128::from(self.capacity)))
            .min(full);
    }

    /// Whole tokens available at `now`.
    pub fn available(&mut self, now: MonoTime) -> u64 {
        self.refill(now);
        u64::try_from(self.scaled.checked_div(self.window.as_nanos()).unwrap_or(0))
            .unwrap_or(u64::MAX)
    }

    /// Takes `cost` tokens, or returns how long to wait until they will be available.
    ///
    /// # Errors
    /// The wait, when there are not enough tokens now. A cost above capacity can never be
    /// satisfied and returns `Duration::MAX`.
    pub fn try_take(&mut self, cost: u64, now: MonoTime) -> Result<(), Duration> {
        if cost > self.capacity {
            return Err(Duration::MAX);
        }
        self.refill(now);
        let needed = u128::from(cost).saturating_mul(self.window.as_nanos());
        if self.scaled >= needed {
            self.scaled = self.scaled.saturating_sub(needed);
            return Ok(());
        }
        let missing = needed.saturating_sub(self.scaled);
        // Nanoseconds until `missing` scaled tokens accrue at `capacity` per nanosecond, rounded up.
        let wait = missing.div_ceil(u128::from(self.capacity.max(1)));
        Err(Duration::from_nanos(
            u64::try_from(wait).unwrap_or(u64::MAX),
        ))
    }

    /// Applies the venue's report that `used` tokens of the current window are spent. Only
    /// ever lowers the balance: the venue's count wins when it is higher than ours.
    pub fn observe_used(&mut self, used: u64, now: MonoTime) {
        self.refill(now);
        let remaining =
            u128::from(self.capacity.saturating_sub(used)).saturating_mul(self.window.as_nanos());
        self.scaled = self.scaled.min(remaining);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(ms: u64) -> MonoTime {
        MonoTime::from_nanos(ms * 1_000_000)
    }

    #[test]
    fn spends_and_refills_evenly() {
        let mut bucket = TokenBucket::new(6_000, Duration::from_secs(60), at(0));
        assert_eq!(bucket.try_take(5_000, at(0)), Ok(()));
        assert_eq!(bucket.available(at(0)), 1_000);
        // 100 tokens per second refill.
        assert_eq!(bucket.try_take(1_100, at(0)), Err(Duration::from_secs(1)));
        assert_eq!(bucket.try_take(1_100, at(1_000)), Ok(()));
        assert_eq!(bucket.available(at(1_000)), 0);
        // Never exceeds capacity.
        assert_eq!(bucket.available(at(10_000_000)), 6_000);
    }

    #[test]
    fn venue_reports_only_lower_the_balance() {
        let mut bucket = TokenBucket::new(100, Duration::from_secs(1), at(0));
        bucket.observe_used(70, at(0));
        assert_eq!(bucket.available(at(0)), 30);
        bucket.observe_used(10, at(0));
        assert_eq!(
            bucket.available(at(0)),
            30,
            "a lower report must not add tokens"
        );
    }

    #[test]
    fn impossible_costs_are_reported() {
        let mut bucket = TokenBucket::new(10, Duration::from_secs(1), at(0));
        assert_eq!(bucket.try_take(11, at(0)), Err(Duration::MAX));
    }

    #[test]
    fn time_going_backwards_is_harmless() {
        let mut bucket = TokenBucket::new(10, Duration::from_secs(1), at(5_000));
        assert_eq!(bucket.try_take(10, at(5_000)), Ok(()));
        assert_eq!(bucket.available(at(1_000)), 0);
    }
}
