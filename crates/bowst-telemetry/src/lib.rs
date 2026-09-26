//! Telemetry for Bowst: allocation-free measurement on the hot path, summarized elsewhere.
//!
//! - [`LatencyHistogram`]: records durations in nanoseconds in constant time without
//!   allocating, and reports percentiles with a bounded relative error.
//! - [`exposition`]: writes metrics in the Prometheus text format.
//! - [`server`]: a minimal, loopback-by-default HTTP endpoint serving `GET /metrics`.
//!
//! Recording is the only operation meant for the hot path. Summaries scan the whole histogram
//! and belong on a periodic, off-path schedule.
#![cfg_attr(
    not(test),
    deny(clippy::indexing_slicing, clippy::arithmetic_side_effects)
)]

pub mod exposition;
mod histogram;
pub mod server;

pub use histogram::{LatencyHistogram, LatencySummary};
