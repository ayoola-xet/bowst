# 0001. Rust for the trading engine

- Status: Accepted
- Date: 2026-09-25

## Context
The engine must react to market data in microseconds with predictable tail latency, and it holds live orders and real capital. Candidates were TypeScript, C++ and Rust.

## Decision
The whole trading engine is written in Rust. TypeScript may be used later for a read-only operator dashboard that never sits on the trading path.

## Consequences
- No garbage-collector pauses, so tail latency is under our control. TypeScript was rejected because GC and event-loop stalls leave stale quotes exposed during fast moves.
- Memory safety and data-race freedom are checked by the compiler. C++ was rejected because memory corruption in a trading system is a financial incident.
- `unsafe` is confined to `bowst-core`, documented block by block, and checked under Miri in CI.
- The toolchain is pinned in `rust-toolchain.toml`; upgrades are deliberate PRs.
