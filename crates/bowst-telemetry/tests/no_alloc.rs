//! Recording must never allocate: it runs on the hot path.

#![allow(clippy::unwrap_used)]

use bowst_core::alloc_counter::{CountingAllocator, count_allocations};
use bowst_telemetry::LatencyHistogram;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

#[test]
fn recording_does_not_allocate() {
    let mut h = LatencyHistogram::new();
    let ((), allocations) = count_allocations(|| {
        let mut v: u64 = 1;
        for _ in 0..100_000 {
            h.record(v);
            v = v.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1) >> 20;
        }
    });
    assert_eq!(allocations, 0);
    assert_eq!(h.count(), 100_000);
}
