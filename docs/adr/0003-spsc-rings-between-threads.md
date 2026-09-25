# 0003. Lock-free SPSC rings as the only hot-path channel

- Status: Accepted
- Date: 2026-09-25

## Context
Market data, strategy and order entry run on separate pinned threads (README §3). They need a handoff that never blocks, never allocates and adds as little latency as possible. Mutexes and general-purpose channels can block, allocate or bounce shared cache lines between cores on every message.

## Decision
- Threads exchange data only through `bowst_core::ring`: a bounded single-producer/single-consumer ring of `Copy` values, allocated once at startup.
- Each slot holds a sequence number and the value on one 64-byte-aligned cache line. The consumer learns a value is ready from the slot itself, so a handoff moves a single cache line between cores for messages up to 56 bytes (`SINGLE_LINE_PAYLOAD`). Hot-path message types are designed to fit.
- Only the producer writes slots. The consumer publishes its read position on its own padded cache line after every element; the producer reads it only when its cached copy says the ring looks full. "Full" is therefore always exact.
- A full ring returns the value to the producer. Back-pressure handling is an explicit decision at each call site (for market data this means marking the book stale and resyncing, never silently dropping updates).
- Each side can detect that the other has gone away (`is_abandoned`) and must then fail closed.
- The implementation is written in-house (a small amount of reviewed `unsafe` code) rather than taken from a crate, so its memory ordering, layout and failure behavior are fully under our control. It is covered by a model-based property test, a multi-threaded ordering test, Miri, and zero-allocation tests.

## Consequences
- One producer and one consumer per ring. Fan-in (several venues into one strategy thread) uses one ring per producer, polled in turn.
- Messages must be fixed-size `Copy` types, which also keeps the hot path allocation-free.
- Handoff latency is benchmarked in `crates/bowst-core/benches/ring.rs`; CI compiles the benchmarks on every PR.

## Measurements (development VM, 4 vCPU, no core pinning)

Three layouts were benchmarked with `crates/bowst-core/benches/ring.rs`:

| Layout | One-way handoff | Saturated throughput |
|---|---|---|
| Shared head/tail counters, each side caching the other's | 190–238 ns | 6.5 ns/msg |
| Per-slot sequence, consumer frees slots by rewriting them | ~154 ns | 5.0 ns/msg |
| **Per-slot sequence, consumer publishes head (chosen)** | **~120–143 ns** | 15 ns/msg |
| Floor: two bare atomics signalling between the same threads | ~131–141 ns | n/a |

The chosen layout matches the hardware floor, meaning the ring adds no measurable latency of its own. Its saturated throughput is lower because a full ring makes the producer re-read `head` often. A full ring is already an alarm condition for us, and 15 ns per message is still over 60 million messages a second, far above any venue feed. Batching the `head` publication restored throughput but let the producer see "full" up to a quarter-ring early, which the model-based test caught; exact semantics were kept.

Absolute numbers on production hardware (isolated, pinned cores) will be lower. The benchmark reports the floor alongside the ring so the comparison stays valid on any host.
