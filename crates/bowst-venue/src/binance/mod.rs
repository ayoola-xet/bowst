//! Binance Spot.
//!
//! - [`depth`]: diff-depth stream updates and REST depth snapshots, which feed
//!   [`bowst_book::BookSync`] using Binance's documented `lastUpdateId` / `U` / `u` procedure.
//! - [`exchange_info`]: symbol rules (tick size, lot step, minimum notional).
//! - [`rest`]: REST endpoints, request weights and instrument loading.
//! - [`md`]: the live market-data session (stream, snapshots, resync, reconnect).
//! - [`books`]: the deterministic decode-and-book core shared by the live session and replay.
//! - [`replay`]: replaying a journaled session through that same core.

pub mod books;
pub mod depth;
pub mod exchange_info;
pub mod md;
pub mod replay;
pub mod rest;
