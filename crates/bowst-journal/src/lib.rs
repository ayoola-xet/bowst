//! Append-only binary event journal (README §8).
//!
//! Every inbound venue message, applied snapshot and status change is journaled with its
//! receive timestamps, so any session can be replayed exactly for post-mortems and regression
//! tests. The hot thread only copies bytes into a lock-free ring ([`JournalProducer`]); a
//! background thread checksums them and writes rotated segment files ([`start`]).
//!
//! - [`format`](mod@format): the on-disk format and a pure, fuzzed record decoder.
//! - [`JournalProducer`] / [`JournalHandle`]: writing.
//! - [`JournalReader`]: reading back, with checksum verification.
//!
//! The journal never blocks the hot path. If the writer falls behind, records are dropped,
//! counted, and marked in the journal by a [`Kind::GAP`](format::Kind::GAP) record, and
//! [`JournalHealth`] turns `Degraded`; if disk writes fail it turns `Failed`. The risk layer
//! refuses new orders unless the journal is healthy.
#![cfg_attr(
    not(test),
    deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)
)]

pub mod format;
mod journal;
mod reader;

pub use journal::{
    JournalConfig, JournalError, JournalHandle, JournalHealth, JournalProducer, JournalStats, start,
};
pub use reader::{JournalReader, ReadError, Record};
