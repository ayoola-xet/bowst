//! Timestamps and clocks.
//!
//! Two distinct timestamp types prevent mixing clocks: [`MonoTime`] for measuring latency and
//! ordering local events, and [`WallTime`] for comparing with venue timestamps. Pure code
//! receives time from a [`Clock`] instead of reading it, so tests and replays are
//! deterministic.

use core::cell::Cell;
use core::time::Duration;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Defines a `u64` nanosecond timestamp newtype with the shared helpers.
macro_rules! nanos_timestamp {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Wraps a nanosecond count.
            #[must_use]
            pub const fn from_nanos(nanos: u64) -> Self {
                Self(nanos)
            }

            /// The nanosecond count.
            #[must_use]
            pub const fn as_nanos(self) -> u64 {
                self.0
            }

            /// Time elapsed since `earlier`, or zero if `earlier` is later.
            #[must_use]
            pub const fn saturating_since(self, earlier: Self) -> Duration {
                Duration::from_nanos(self.0.saturating_sub(earlier.0))
            }

            /// This timestamp plus `span`, or `None` on overflow.
            #[must_use]
            pub fn checked_add(self, span: Duration) -> Option<Self> {
                let nanos = u64::try_from(span.as_nanos()).ok()?;
                self.0.checked_add(nanos).map(Self)
            }
        }
    };
}

nanos_timestamp!(
    /// Nanoseconds on the process-local monotonic clock. Only meaningful within one process.
    MonoTime
);

nanos_timestamp!(
    /// Nanoseconds since the Unix epoch on the (chrony-disciplined) system clock.
    WallTime
);

impl WallTime {
    /// Converts a venue timestamp in Unix milliseconds. `None` on overflow.
    #[must_use]
    pub const fn from_unix_millis(millis: u64) -> Option<Self> {
        match millis.checked_mul(1_000_000) {
            Some(nanos) => Some(Self(nanos)),
            None => None,
        }
    }
}

/// A source of time.
pub trait Clock {
    /// Current monotonic time.
    fn mono(&self) -> MonoTime;
    /// Current wall-clock time.
    fn wall(&self) -> WallTime;
}

/// The real system clocks.
#[derive(Clone, Copy, Debug)]
pub struct SystemClock {
    origin: Instant,
}

impl SystemClock {
    /// Creates a clock whose monotonic time starts near zero now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    #[inline]
    fn mono(&self) -> MonoTime {
        // Saturates after ~584 years of uptime.
        MonoTime(u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }

    #[inline]
    fn wall(&self) -> WallTime {
        // A system clock before 1970 is a host fault. It reads as 0, which every freshness
        // check treats as stale, so the engine fails closed.
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        WallTime(u64::try_from(since_epoch.as_nanos()).unwrap_or(u64::MAX))
    }
}

/// A clock moved by hand, for tests, simulation and replay.
#[derive(Debug, Default)]
pub struct ManualClock {
    mono: Cell<u64>,
    wall: Cell<u64>,
}

impl ManualClock {
    /// Creates a clock at the given times.
    #[must_use]
    pub const fn new(mono: MonoTime, wall: WallTime) -> Self {
        Self {
            mono: Cell::new(mono.0),
            wall: Cell::new(wall.0),
        }
    }

    /// Moves both clocks forward by `span`, saturating at the maximum.
    pub fn advance(&self, span: Duration) {
        let nanos = u64::try_from(span.as_nanos()).unwrap_or(u64::MAX);
        self.mono.set(self.mono.get().saturating_add(nanos));
        self.wall.set(self.wall.get().saturating_add(nanos));
    }

    /// Sets the wall clock, for example to replay a recorded timestamp.
    pub fn set_wall(&self, wall: WallTime) {
        self.wall.set(wall.0);
    }
}

impl Clock for ManualClock {
    fn mono(&self) -> MonoTime {
        MonoTime(self.mono.get())
    }

    fn wall(&self) -> WallTime {
        WallTime(self.wall.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_is_monotonic() {
        let clock = SystemClock::new();
        let a = clock.mono();
        let b = clock.mono();
        assert!(b >= a);
        assert!(clock.wall() > WallTime::from_unix_millis(1_700_000_000_000).unwrap());
    }

    #[test]
    fn manual_clock_advances_both_clocks() {
        let clock = ManualClock::new(MonoTime::from_nanos(10), WallTime::from_nanos(1_000));
        clock.advance(Duration::from_nanos(5));
        assert_eq!(clock.mono(), MonoTime::from_nanos(15));
        assert_eq!(clock.wall(), WallTime::from_nanos(1_005));
        assert_eq!(
            clock.mono().saturating_since(MonoTime::from_nanos(10)),
            Duration::from_nanos(5)
        );
        assert_eq!(
            MonoTime::from_nanos(1).saturating_since(MonoTime::from_nanos(9)),
            Duration::ZERO
        );
    }

    #[test]
    fn converts_venue_millis() {
        assert_eq!(
            WallTime::from_unix_millis(1_500),
            Some(WallTime::from_nanos(1_500_000_000))
        );
        assert_eq!(WallTime::from_unix_millis(u64::MAX), None);
    }
}
