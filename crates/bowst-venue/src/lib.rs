//! Venue adapters for Bowst.
//!
//! - [`json`]: the allocation-free JSON reader every JSON venue decoder uses.
//! - [`levels`]: shared decoding of price levels into book types.
//! - [`binance`]: Binance Spot decoders.
//!
//! Decoders are pure: bytes in, typed events out. They validate everything and convert
//! prices and quantities exactly (never rounding), so malformed or unexpected venue data is
//! an error and never reaches the book.
#![cfg_attr(
    not(test),
    deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)
)]

pub mod binance;
pub mod json;
pub mod levels;

use bowst_core::Symbol;
use bowst_core::fixed::FixedError;

use crate::json::JsonError;

/// A venue message that could not be decoded. The message must be treated as lost, which
/// invalidates any book it was meant for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// Malformed JSON.
    #[error("malformed JSON: {0}")]
    Json(#[from] JsonError),
    /// A required field was absent.
    #[error("missing field {0:?}")]
    MissingField(&'static str),
    /// A field held a value this decoder does not handle.
    #[error("unexpected value for field {0:?}")]
    UnexpectedValue(&'static str),
    /// The symbol is not a configured instrument. `None` if it is not even a valid symbol.
    #[error("unknown symbol {0:?}")]
    UnknownSymbol(Option<Symbol>),
    /// A sequence range whose first number is after its last.
    #[error("invalid sequence range {first}..={last}")]
    InvalidSequence {
        /// First sequence number.
        first: u64,
        /// Last sequence number.
        last: u64,
    },
    /// A price, quantity or increment that is not a valid exact decimal for the instrument.
    #[error("invalid number in field {field:?}: {error}")]
    InvalidNumber {
        /// Field being decoded.
        field: &'static str,
        /// Why it was rejected.
        error: FixedError,
    },
    /// More levels than the decoder's pre-allocated capacity.
    #[error("more than {limit} levels in one message")]
    TooManyLevels {
        /// Configured capacity.
        limit: usize,
    },
}
