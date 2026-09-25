//! Level-2 order books for Bowst.
//!
//! - [`Ladder`]: one side of a book, sorted, bounded and allocation-free after construction.
//! - [`Book`]: bids and asks for one instrument.
//! - [`BookSync`]: keeps a book in step with a venue's snapshot-plus-delta feed, detects
//!   sequence gaps and crossed books, and only exposes the book while it is known to be
//!   correct.
//!
//! Everything here is pure and deterministic: no I/O, no clock reads.
#![cfg_attr(
    not(test),
    deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)
)]

mod book;
mod ladder;
mod sync;

pub use book::{Book, LevelUpdate};
pub use ladder::{Applied, Ladder, Level};
pub use sync::{BookSync, DeltaOutcome, SeqRange, SyncConfig, SyncError, SyncStats};

use bowst_core::{Price, Qty, Side};

/// Invalid book input or configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BookError {
    /// A ladder must hold at least one level.
    #[error("book capacity must be at least 1")]
    ZeroCapacity,
    /// Prices must be positive.
    #[error("invalid {side:?} price {price:?}")]
    InvalidPrice {
        /// Side of the update.
        side: Side,
        /// Offending price.
        price: Price,
    },
    /// Quantities must not be negative, and snapshot levels must be positive.
    #[error("invalid {side:?} quantity {qty:?} at {price:?}")]
    InvalidQty {
        /// Side of the update.
        side: Side,
        /// Price of the update.
        price: Price,
        /// Offending quantity.
        qty: Qty,
    },
    /// A snapshot listed the same price twice on one side.
    #[error("duplicate {side:?} level at {price:?}")]
    DuplicateLevel {
        /// Side of the snapshot.
        side: Side,
        /// Repeated price.
        price: Price,
    },
}
