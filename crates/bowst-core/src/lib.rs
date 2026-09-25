//! Core building blocks shared by every Bowst crate.
//!
//! - [`fixed`]: exact decimal numbers and conversion to and from venue ticks and lots.
//! - [`units`]: [`Price`], [`Qty`] and [`Side`], the integer types used for all money math.
//! - [`ids`]: venue, instrument and client-order identifiers.
//! - [`instrument`]: instruments, their venue symbols and order rules.
//! - [`time`]: monotonic and wall-clock timestamps behind a [`Clock`] trait.
//! - [`ring`]: a bounded, lock-free single-producer/single-consumer channel.
//! - [`bytes_ring`]: the same for variable-length byte records (journal traffic).
//! - [`alloc_counter`]: a global allocator wrapper used by tests to prove hot paths do not allocate.
//!
//! Nothing in this crate performs I/O. Everything on the hot path is allocation-free
//! after construction.
// Hot-path crate: every index and every arithmetic operation must be checked (CLAUDE.md §3).
// Tests are exempt so they can state expected values plainly.
#![cfg_attr(
    not(test),
    deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)
)]

pub mod alloc_counter;
pub mod bytes_ring;
pub mod event;
pub mod fixed;
pub mod ids;
pub mod instrument;
pub mod ring;
pub mod time;
pub mod units;

pub use event::Stamped;
pub use fixed::{Dec, FixedError, Increment, Rounding};
pub use ids::{ClientOrderId, ClientOrderIdGen, InstrumentId, VenueId};
pub use instrument::{Instrument, InstrumentTable, Symbol};
pub use time::{Clock, ManualClock, MonoTime, SystemClock, WallTime};
pub use units::{Price, Qty, Side};
