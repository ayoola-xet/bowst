//! Byte ring benchmarks: the cost of handing a typical venue message to the journal thread.

// Benchmarks are test code: failing fast on a broken setup is the right behavior.
#![allow(missing_docs, clippy::unwrap_used)]

use std::hint::black_box;
use std::time::Instant;

use bowst_core::bytes_ring::{self, WriteError};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

/// Median recorded Binance diff-depth message size.
const MESSAGE: usize = 459;

fn same_thread(c: &mut Criterion) {
    let (mut tx, mut rx) = bytes_ring::channel(1 << 20).unwrap();
    let message = [7_u8; MESSAGE];
    let mut group = c.benchmark_group("bytes_ring");
    group.throughput(Throughput::Bytes(u64::try_from(MESSAGE).unwrap()));
    group.bench_function("push_read_459b_same_thread", |b| {
        b.iter(|| {
            tx.push(black_box(&message)).unwrap();
            rx.read_with(|r| black_box(r.len()))
        });
    });
    group.finish();
}

fn cross_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("bytes_ring");
    group.throughput(Throughput::Elements(1));
    group.bench_function("throughput_459b_cross_thread", |b| {
        b.iter_custom(|iters| {
            let (mut tx, mut rx) = bytes_ring::channel(1 << 22).unwrap();
            let consumer = std::thread::spawn(move || {
                let mut left = iters;
                while left > 0 {
                    if rx.read_with(|r| black_box(r.len())).is_some() {
                        left -= 1;
                    } else {
                        std::hint::spin_loop();
                    }
                }
            });
            let message = [7_u8; MESSAGE];
            let start = Instant::now();
            for _ in 0..iters {
                while let Err(WriteError::Full) = tx.push(&message) {
                    std::hint::spin_loop();
                }
            }
            consumer.join().unwrap();
            start.elapsed()
        });
    });
    group.finish();
}

criterion_group!(benches, same_thread, cross_thread);
criterion_main!(benches);
