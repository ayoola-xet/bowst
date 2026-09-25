//! Exponential reconnect backoff with jitter, shared by every venue connection.
//!
//! Jitter spreads reconnects from many connections (and many engines) so a venue outage is
//! not followed by a synchronized reconnect storm that trips its connection-rate limits.

use core::time::Duration;

/// Exponential backoff state. Pure: randomness is passed in.
#[derive(Clone, Debug)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    attempt: u32,
}

impl Backoff {
    /// Starts at `initial`, doubling per attempt up to `max`.
    #[must_use]
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self {
            initial,
            max: max.max(initial),
            attempt: 0,
        }
    }

    /// Delay before the next attempt: a uniformly jittered value between half and all of the
    /// current step (`random` is any uniformly random `u32`). Advances to the next step.
    pub fn next_delay(&mut self, random: u32) -> Duration {
        let step = self
            .initial
            .checked_mul(1_u32.checked_shl(self.attempt).unwrap_or(u32::MAX))
            .unwrap_or(self.max)
            .min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        let half = step.checked_div(2).unwrap_or_default();
        // `random / u32::MAX` of the remaining half, in nanoseconds.
        let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        let jitter = u128::from(spread)
            .saturating_mul(u128::from(random))
            .checked_div(u128::from(u32::MAX))
            .unwrap_or(0);
        half.saturating_add(Duration::from_nanos(
            u64::try_from(jitter).unwrap_or(u64::MAX),
        ))
    }

    /// Resets to the initial delay, after a connection has proven healthy.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Attempts since the last reset.
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_up_to_the_cap_with_jitter_in_range() {
        let mut backoff = Backoff::new(Duration::from_millis(100), Duration::from_secs(2));
        let steps = [100, 200, 400, 800, 1600, 2000, 2000];
        for step in steps {
            let step = Duration::from_millis(step);
            let low = backoff.clone().next_delay(0);
            let high = backoff.clone().next_delay(u32::MAX);
            assert_eq!(low, step / 2);
            assert_eq!(high, step);
            let mid = backoff.next_delay(u32::MAX / 2);
            assert!(mid > low && mid < high);
        }
        assert_eq!(backoff.attempts(), 7);
        backoff.reset();
        assert_eq!(backoff.next_delay(u32::MAX), Duration::from_millis(100));
    }

    #[test]
    fn survives_many_attempts_without_overflow() {
        let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(30));
        for _ in 0..200 {
            assert!(backoff.next_delay(u32::MAX) <= Duration::from_secs(30));
        }
    }
}
