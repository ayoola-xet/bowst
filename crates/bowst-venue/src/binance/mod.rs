//! Binance Spot.
//!
//! - [`depth`]: diff-depth stream updates and REST depth snapshots, which feed
//!   [`bowst_book::BookSync`] using Binance's documented `lastUpdateId` / `U` / `u` procedure.
//! - [`exchange_info`]: symbol rules (tick size, lot step, minimum notional).

pub mod depth;
pub mod exchange_info;
