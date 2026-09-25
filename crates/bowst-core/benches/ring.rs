//! Ring buffer benchmarks. Phase 0 target: cross-thread handoff within noise of the host's
//! bare atomic-signal floor (`baseline_atomic_signal_one_way`).

// Benchmarks are test code: failing fast on a broken setup is the right behavior.
#![allow(missing_docs, clippy::unwrap_used)]

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bowst_core::ring::{self, Consumer, Producer};
use criterion::{Criterion, criterion_group, criterion_main};

/// A message filling one ring slot's cache line (`ring::SINGLE_LINE_PAYLOAD` bytes), the
/// size budget for hot-path events.
type Msg = [u64; 7];

fn same_thread(c: &mut Criterion) {
    let (mut tx, mut rx) = ring::channel::<Msg>(1024).unwrap();
    c.bench_function("ring/push_pop_same_thread", |b| {
        b.iter(|| {
            tx.push(black_box([1; 7])).unwrap();
            black_box(rx.pop())
        });
    });
}

fn send_spin(tx: &mut Producer<Msg>, msg: Msg) {
    while tx.push(msg).is_err() {
        std::hint::spin_loop();
    }
}

fn recv_spin(rx: &mut Consumer<Msg>) -> Msg {
    loop {
        if let Some(msg) = rx.pop() {
            return msg;
        }
        std::hint::spin_loop();
    }
}

/// Round trip between two busy-polling threads, halved to give one-way handoff latency.
fn cross_thread_handoff(c: &mut Criterion) {
    let (mut ping_tx, mut ping_rx) = ring::channel::<Msg>(64).unwrap();
    let (mut pong_tx, mut pong_rx) = ring::channel::<Msg>(64).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let echo_stop = Arc::clone(&stop);
    let echo = std::thread::spawn(move || {
        while !echo_stop.load(Ordering::Relaxed) {
            if let Some(msg) = ping_rx.pop() {
                send_spin(&mut pong_tx, msg);
            } else {
                std::hint::spin_loop();
            }
        }
    });

    c.bench_function("ring/one_way_handoff_cross_thread", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for i in 0..iters {
                send_spin(&mut ping_tx, [i; 7]);
                black_box(recv_spin(&mut pong_rx));
            }
            let elapsed = start.elapsed();
            elapsed / 2
        });
    });

    stop.store(true, Ordering::Relaxed);
    echo.join().unwrap();
}

/// Hardware floor for the handoff above: two padded atomics, one per direction, bounced
/// between the same two threads with no ring at all. This is the least any pair of one-way
/// channels can cost on the host, so the gap between it and the ring handoff is the ring's
/// own overhead. Absolute numbers depend heavily on the host (virtualized cores, pinning).
fn core_to_core_baseline(c: &mut Criterion) {
    #[repr(align(128))]
    struct Padded(AtomicU64);

    let request = Arc::new(Padded(AtomicU64::new(0)));
    let reply = Arc::new(Padded(AtomicU64::new(0)));
    let (echo_request, echo_reply) = (Arc::clone(&request), Arc::clone(&reply));
    let echo = std::thread::spawn(move || {
        let mut expected = 1;
        loop {
            let seen = echo_request.0.load(Ordering::Acquire);
            if seen == u64::MAX {
                return;
            }
            if seen == expected {
                echo_reply.0.store(expected, Ordering::Release);
                expected += 1;
            } else {
                std::hint::spin_loop();
            }
        }
    });

    let mut next = 1;
    c.bench_function("ring/baseline_atomic_signal_one_way", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                request.0.store(next, Ordering::Release);
                while reply.0.load(Ordering::Acquire) != next {
                    std::hint::spin_loop();
                }
                next += 1;
            }
            start.elapsed() / 2
        });
    });

    request.0.store(u64::MAX, Ordering::Release);
    echo.join().unwrap();
}

/// Sustained one-directional throughput with the consumer on another thread.
fn cross_thread_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring");
    group.throughput(criterion::Throughput::Elements(1));
    group.bench_function("throughput_cross_thread", |b| {
        b.iter_custom(|iters| {
            let (mut tx, mut rx) = ring::channel::<Msg>(4096).unwrap();
            let consumer = std::thread::spawn(move || {
                for _ in 0..iters {
                    black_box(recv_spin(&mut rx));
                }
            });
            let start = Instant::now();
            for i in 0..iters {
                send_spin(&mut tx, [i; 7]);
            }
            consumer.join().unwrap();
            start.elapsed().max(Duration::from_nanos(1))
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    same_thread,
    core_to_core_baseline,
    cross_thread_handoff,
    cross_thread_throughput
);
criterion_main!(benches);
