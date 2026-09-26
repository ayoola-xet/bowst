//! Cost of recording one latency sample (hot path) and of a summary (off path).

#![allow(missing_docs)]

use std::hint::black_box;

use bowst_telemetry::LatencyHistogram;
use criterion::{Criterion, criterion_group, criterion_main};

fn record(c: &mut Criterion) {
    let mut h = LatencyHistogram::new();
    let mut v: u64 = 1;
    c.bench_function("histogram_record", |b| {
        b.iter(|| {
            v = v.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            h.record(black_box(v >> 44));
        });
    });
    c.bench_function("histogram_summary", |b| b.iter(|| black_box(h.summary())));
}

criterion_group!(benches, record);
criterion_main!(benches);
