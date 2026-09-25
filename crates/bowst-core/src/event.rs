//! Envelope for events crossing thread boundaries.

use crate::time::{Clock, MonoTime, WallTime};

/// A value with the local times at which it was received.
///
/// Every inbound venue event is stamped once, as close to the socket read as possible. The
/// monotonic stamp drives latency measurement; the wall stamp is compared with venue
/// timestamps and written to the journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamped<T> {
    /// Monotonic receive time.
    pub mono: MonoTime,
    /// Wall-clock receive time.
    pub wall: WallTime,
    /// The event.
    pub value: T,
}

impl<T> Stamped<T> {
    /// Stamps `value` with the current time of `clock`.
    #[inline]
    pub fn now(clock: &impl Clock, value: T) -> Self {
        Self {
            mono: clock.mono(),
            wall: clock.wall(),
            value,
        }
    }
}
