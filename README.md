# Bowst — Multi-Venue Market Maker

**Status:** Phase 0 complete. Phase 1 (market data) in progress: the live market-data path is built (order books, Binance Spot decoding, WebSocket/TLS transport and the market-data session in `crates/bowst-venue`, plus the `bin/bowst-md` tool). Remaining for Phase 1: the event journal, telemetry, and the 72-hour soak run. No trading code yet.

Bowst is a low-latency, multi-venue market-making engine. It keeps two-sided quotes on one or more trading venues, controls inventory, and enforces hard risk limits on every order before it leaves the process. It is being built to trade real capital, so correctness and risk control come before speed. Speed is the second priority, and a close one.

**Current scope: spot markets on Binance and Bybit**, with the venue layer built so similar centralized exchanges (for example OKX, Bitget, Gate, KuCoin) can be added as adapters without touching the engine. Perpetuals, margin and other asset classes are out of scope for now. The design keeps room for them (§9.4) but none of that work is scheduled.

Engineering standards for everyone working in this repository are in [`CLAUDE.md`](CLAUDE.md).

---

## Contents

1. [Language decision: Rust](#1-language-decision-rust)
2. [Latency: targets and where the time goes](#2-latency-targets-and-where-the-time-goes)
3. [System architecture](#3-system-architecture)
4. [Hot-path design rules](#4-hot-path-design-rules)
5. [Components](#5-components)
6. [Quoting strategy](#6-quoting-strategy)
7. [Risk controls](#7-risk-controls)
8. [State, recovery and reconciliation](#8-state-recovery-and-reconciliation)
9. [Venue integration](#9-venue-integration)
10. [Testing and validation](#10-testing-and-validation)
11. [Observability and operations](#11-observability-and-operations)
12. [Security](#12-security)
13. [Deployment and infrastructure](#13-deployment-and-infrastructure)
14. [Repository layout](#14-repository-layout)
15. [Phased build plan](#15-phased-build-plan)
16. [Go-live checklist](#16-go-live-checklist)
17. [Open decisions for the business](#17-open-decisions-for-the-business)

---

## 1. Language decision: Rust

**Decision: Rust for the entire trading engine.** TypeScript may be used later for a read-only operator dashboard. It never goes on the trading path.

| Criterion | Rust | TypeScript (Node/Bun) | C++ |
|---|---|---|---|
| Hot-path latency | Native code with no GC. Single-digit µs is achievable. | JIT with GC pauses of 1–50 ms at unpredictable times. | Native code with no GC, same class as Rust. |
| Tail latency (p99.9) | Predictable. We control memory. | Poor. GC and event-loop stalls dominate. | Predictable. |
| Memory safety | Checked by the compiler. No use-after-free and no data races in safe code. | Safe, but slow. | Manual. A whole class of bugs that crash or corrupt state goes unchecked. |
| Concurrency | Data races are compile errors. Lock-free crates are mature. | Single-threaded, or workers with message passing. | Possible, but easy to get wrong. |
| Crypto venue ecosystem | Good: tokio, tungstenite, reqwest, simd-json, sonic-rs. | Very good, but the language is too slow. | Weaker for crypto venues. More in-house work. |
| Hiring and velocity | Moderate. | High. | Moderate, with slower reviews. |

**Why not TypeScript:** The problem is not average speed. It is unpredictable pauses. A market maker that stalls for 20 ms during a volatile move gets picked off, because its stale quotes fill at bad prices. That is the main way market makers lose money, and it cannot be engineered out of a garbage-collected runtime.

**Why not C++:** C++ matches Rust on speed, but it has no compiler-checked memory safety. In a system that holds live orders and real money, a memory-corruption bug is a financial incident. Rust gives the same performance and removes that risk.

**Toolchain:** Stable Rust pinned in `rust-toolchain.toml`. Release profile: `opt-level=3`, `lto="fat"`, `codegen-units=1`, `panic="abort"` (a panic kills the process and the venue-side cancel-on-disconnect cleans up, see §7), `target-cpu` set for the production host.

---

## 2. Latency: targets and where the time goes

"Millisecond fast" is not ambitious enough for the code we control. It is also the wrong thing to focus on on its own. Total reaction time has three parts:

```
  venue matching engine
        │  (1) venue → us: network + venue gateway      ~0.1 ms colocated … 5–50 ms cross-region
        ▼
  ┌──────────────── Bowst process ────────────────┐
  │ (2) decode → book → strategy → risk → encode  │   target: < 20 µs p50, < 100 µs p99.9
  └───────────────────────────────────────────────┘
        │  (3) us → venue: network + venue order gateway    same as (1)
        ▼
```

- **(2) Internal tick-to-order:** This is what the code controls. Targets are **p50 < 20 µs** and **p99.9 < 100 µs**, measured from the socket read timestamp to the socket write timestamp. That is 10–50× faster than "millisecond fast".
- **(1) and (3) Network and venue:** These usually dominate by orders of magnitude, and **code cannot fix them**. Only **hosting location** fixes them: the same cloud region or availability zone as the venue's matching engine, or colocation where the venue offers it. See §13.
- **Venue rate limits** often bind before latency does. The quoting engine must spend its order and cancel budget carefully (§6.4).

Every release is benchmarked against these numbers (§10.5). A regression beyond 10% on p99 blocks the release.

---

## 3. System architecture

```
                       ┌──────────────────────────────────────────────────────────┐
                       │                     Bowst process                        │
 Venue A WS/FIX ──────►│  MD Gateway A ─┐                                         │
 Venue B WS/FIX ──────►│  MD Gateway B ─┼─► Book Builder ─► Fair Value / Signals  │
 Venue C WS/FIX ──────►│  MD Gateway C ─┘   (per instrument)        │             │
                       │                                            ▼             │
                       │                                   Quoting Strategy       │
                       │                                            │ intents     │
                       │                                            ▼             │
                       │   Position / PnL ◄──── fills ───── Risk Gate (pre-trade) │
                       │        ▲                                   │ approved    │
                       │        │                                   ▼             │
                       │        └────────── Order Manager (OMS state machine)     │
                       │                                            │             │
 Venue A order API ◄───│─────────────── Order Gateway A/B/C ◄───────┘             │
                       │                                                          │
                       │  ┌─ Journal (append-only event log, off hot path) ─────┐ │
                       │  └─ Metrics / Logs (off hot path) ─────────────────────┘ │
                       └───────────────▲──────────────────────────────────────────┘
                                       │ control plane (auth, mTLS)
                         Operator CLI / API: kill switch, limits, params, status
```

**Threading model:**

| Thread | Pinned core | Work |
|---|---|---|
| `md-io-{venue}` | isolated | Busy-polls sockets, decodes market data, updates books, publishes to SPSC rings. |
| `strategy-{group}` | isolated | Consumes book updates and fills, computes quotes, runs the risk gate, drives the OMS, encodes orders. |
| `oe-io-{venue}` | isolated | Order-entry sockets: sends orders, receives acks, fills and rejects. |
| `journal` | shared | Drains an SPSC ring into an mmap-ed append-only log. |
| `control` | shared | Tokio runtime for the control API, metrics endpoint, REST reconciliation and config reloads. |

Threads communicate **only** through bounded, lock-free single-producer/single-consumer ring buffers: fixed-size `Copy` structs for hot-path events (`bowst_core::ring`), and variable-length byte records for journal traffic (`bowst_core::bytes_ring`). There are no mutexes on the hot path and no shared mutable state. Each instrument is owned by exactly one strategy thread.

---

## 4. Hot-path design rules

These are enforced in code review and by tests (§10.5).

1. **No heap allocation after warm-up.** Pre-size all buffers, books, order pools and strings. In CI, a counting global allocator asserts zero allocations on the hot path.
2. **No locks, no syscalls except socket I/O, no blocking.** Logging on the hot path writes a fixed-size binary record to a ring. Formatting happens on another thread.
3. **Fixed-point numbers only.** Prices are `i64` ticks and quantities are `i64` lots, scaled per instrument. `f64` is allowed only inside the strategy's model math, and results are rounded to ticks and lots with an explicit rounding direction (bids round down, asks round up). Money never goes through floats.
4. **Fast parsing.** Use `sonic-rs` or `simd-json` for JSON venues, with zero-copy decoding into borrowed buffers. Where a venue offers a binary protocol (SBE, FIX, native binary), use it.
5. **Cache-friendly data.** Each book side is a pre-allocated array sorted with the best level last, not a tree (ADR 0004). Hot structs are `#[repr(C)]` and cache-line aligned, with no false sharing between threads. Messages passed between threads fit in one ring slot's cache line (`ring::SINGLE_LINE_PAYLOAD`, 56 bytes).
6. **Busy-polling on isolated cores.** Use `isolcpus`/`nohz_full`, IRQ affinity away from trading cores, and `SCHED_FIFO` where permitted.
7. **Time.** Use `CLOCK_MONOTONIC` via TSC for latency and `CLOCK_REALTIME` (chrony-disciplined) for venue timestamps. Every event carries both.
8. **Branch-predictable failure paths.** Risk rejects are cold paths marked `#[cold]`.

---

## 5. Components

| Crate | Responsibility | Key points |
|---|---|---|
| `bowst-core` | Shared types: `Price`, `Qty`, `InstrumentId`, `VenueId`, `OrderId`, events, SPSC ring, clock. | `no_std`-friendly. No I/O. |
| `bowst-book` | L2 (and L3 where available) order book per instrument. Applies snapshots and deltas. Checks sequence gaps. | A sequence gap or checksum mismatch marks the book **invalid**, the instrument stops quoting, and the book resyncs. |
| `bowst-venue` | `Venue` trait plus one adapter per venue (market data, order entry, REST, auth, rate limiter, symbol mapping). | Each adapter normalizes to `bowst-core` events. There is a conformance test suite every adapter must pass. |
| `bowst-oms` | Order state machine: `PendingNew → Live → PendingCancel/PendingReplace → Filled/Cancelled/Rejected`. Client-order-ID generation. In-flight tracking. | Handles fill-before-ack, cancel-reject-because-filled, duplicate and out-of-order messages. |
| `bowst-risk` | Pre-trade gate (synchronous, hot path) and post-trade monitors (async). Kill switch. | See §7. Fails closed. |
| `bowst-strategy` | Fair value, spread, skew and quote ladder. Pure function of (state, params) → desired quotes. | Deterministic. No I/O. Fully unit-testable. |
| `bowst-position` | Positions, average cost, realized and unrealized PnL, fees and rebates per venue and globally. | Reconciled against venue balances (§8). |
| `bowst-journal` | Append-only binary event log (mmap). Every inbound and outbound event is recorded. | Drives deterministic replay and post-mortems. |
| `bowst-sim` | Exchange simulator: matching engine with queue position, latency injection and venue-specific quirks. | Used for backtests, integration tests and chaos tests. |
| `bowst-control` | Authenticated control API (gRPC or HTTPS with mTLS) plus the `bowstctl` CLI. | Kill switch, pause/resume per instrument, parameter updates, status. |
| `bowst-telemetry` | Metrics (Prometheus), structured logs, latency histograms (HDR). | All off the hot path. |
| `bowst` (bin) | Wires everything together from config. Handles startup and shutdown sequencing. | |

---

## 6. Quoting strategy

The strategy is a pure, deterministic module. v1 ships a proven baseline. Alpha signals are added only after the plumbing has been proven.

### 6.1 Fair value
- v1: a volume-weighted microprice from the local book, optionally blended with a reference venue's mid (for example, the most liquid venue for the asset).
- Later: order-flow imbalance, trade-flow signals, and cross-venue lead/lag.

### 6.2 Spread and inventory skew (Avellaneda–Stoikov family)
- Reservation price: `r = fair − q · γ · σ² · τ`, where `q` is inventory, `γ` is risk aversion and `σ` is short-horizon volatility (EWMA).
- Half-spread: `δ = max(min_edge, f(σ, γ, κ))`, floored so every fill clears fees minus rebates by `min_edge` ticks.
- Inventory skew moves both quotes toward flattening. At a soft inventory limit the side that adds inventory is withdrawn. At the hard limit the risk gate blocks it regardless of what the strategy wants.

### 6.3 Quote ladder
- N levels per side with configurable size and distance growth.
- Sizes are clipped by available balance, the risk limits and the venue's minimum and step sizes.

### 6.4 Order-update policy (rate-limit aware)
- Only amend or replace when the desired price moves more than `requote_threshold` ticks, or when size drifts more than a threshold. This keeps queue position and saves rate-limit budget.
- Use native amend where the venue supports it. Otherwise use cancel/new with in-flight accounting.
- A per-venue token bucket mirrors the venue's published limits at 80% utilization. When the budget is low, the top of book gets priority.

### 6.5 Spot-specific constraints
- **No shorting.** An ask can only be quoted against base currency we actually hold, and a bid only against quote currency we hold. Inventory is the pair of free balances (base, quote) per venue, not a signed position. The strategy targets a base/quote split (for example 50/50 by value) and skews toward it.
- **Balances are the limit.** Open orders lock balance on the venue. The risk gate tracks locked and free balance locally and never sends an order the venue would reject for insufficient funds.
- **Rebalancing** between assets and between venues (transfers, or a taker trade to restore the target split) is an explicit, rate-limited, operator-visible action, never a side effect of quoting.
- **Fees** depend on the VIP tier and discounts (for example paying fees in BNB on Binance). The fee schedule is loaded per account at startup and refreshed, and the minimum edge (§6.2) uses the live maker fee.
- **Exchange filters** (tick size, lot step, minimum notional, percent-price bands) are loaded from the venue at startup and enforced locally before sending, so filter rejects count as bugs.

### 6.6 Defensive behavior
- **Stale data:** if a book has not updated within `max_book_age` (venue-specific, for example 500 ms), pull quotes for that instrument.
- **Volatility spike:** if short-horizon σ exceeds a threshold, widen spreads by a multiplier or pull quotes.
- **Adverse selection monitor:** track markout PnL at 1s, 5s and 30s after each fill. If markouts stay negative, widen automatically and raise an alert.

---

## 7. Risk controls

**Principles:** Risk checks sit in the order path. They are not advisory. Every check **fails closed**: if something is unknown, the order is blocked and quotes are pulled. The strategy cannot bypass the risk gate, because the order gateway only accepts orders carrying a risk-approved token type.

### 7.1 Pre-trade (synchronous, every order, < 1 µs budget)
| Check | Action on breach |
|---|---|
| Kill switch engaged (global, venue or instrument) | Reject |
| Instrument not in `Quoting` state (book invalid, paused, halted) | Reject |
| Price outside ±X% or ±N ticks of fair value (fat finger) | Reject + alert |
| Order qty > max order size, or notional > max order notional | Reject |
| Resulting base inventory (worst case, all open orders filling) outside the min/max band | Reject the side that pushes it further out |
| Open order count > max per instrument or venue | Reject |
| Order rate over the local budget | Reject / defer |
| Would cross our own resting order (self-trade) | Reject, or use the venue's STP mode |
| Insufficient free balance after locks (local view) | Reject |
| Violates venue filters (tick, lot step, min notional, price band) | Reject + alert (it is a bug) |

### 7.2 Post-trade and portfolio (async monitors, ≤ 10 ms cadence)
- Realized + unrealized PnL drawdown per instrument, venue, day and all-time high-water mark leads to a **pause**, then a **kill**.
- Fill-rate anomaly (for example, more than N fills in T ms on one side) leads to a pause, because it can mean the fair value is wrong.
- Position divergence between internal state and the venue report (§8) leads to a kill.
- Cross-venue net exposure limit per asset.
- Latency watchdog: if internal p99 or venue round-trip exceeds thresholds, widen or pause.

### 7.3 Kill switch
- **Triggers:** operator (`bowstctl kill`), any hard monitor, process health checks, or loss of a venue connection.
- **Action:** stop new orders, cancel all open orders (bulk cancel where supported), verify cancellation over REST, then leave the positions in place. Automatic flattening is off by default because it is dangerous in a crash, and it is a separate operator action.
- **Belt and braces:** enable venue-side **cancel-on-disconnect / dead-man's switch** wherever offered, refreshed by a heartbeat. If Bowst dies, the venue pulls our orders.
- An **independent watchdog process** on a separate host holds read-only and cancel-only keys. It cancels everything if the main engine's heartbeat stops.

### 7.4 Limits configuration
- Limits live in a versioned, signed config file. Hot-reload is allowed only to **tighten** limits. Loosening needs a restart and a second person's approval (four-eyes).

---

## 8. State, recovery and reconciliation

- **Event sourcing:** every market data event, order intent, risk decision, venue message and control command is journaled with both timestamps. Engine state is a deterministic function of the journal.
- **Startup sequence:** load config, then connect order entry and query all open orders, positions and balances over REST, then **cancel all unknown or open orders**, then subscribe to market data, build and validate books, then pass warm-up checks, then enable quoting instrument by instrument. The engine never trusts state from a previous run over venue truth.
- **Continuous reconciliation:** every N seconds, compare internal positions, balances and open orders with the venue REST view. A small drift from in-flight messages is tolerated inside a time window. A persistent mismatch triggers the kill switch.
- **Unknown order state** (timeout with no ack): mark it `Unknown`, count it against limits as if live, and query it by client order ID until resolved.
- **Replay:** `bowst replay <journal>` reproduces a session bit-for-bit for post-mortems and regression tests.

---

## 9. Venue integration

The architecture is venue-agnostic. Every venue implements one trait:

```rust
pub trait Venue {
    fn market_data(&mut self, poll: &mut Poller) -> Option<MdEvent>;   // non-blocking
    fn send(&mut self, cmd: &OrderCommand) -> Result<(), SendError>;  // non-blocking, pre-risk-approved
    fn order_events(&mut self, poll: &mut Poller) -> Option<ExecEvent>;
    fn rest(&self) -> &dyn VenueRest;          // snapshots, reconciliation (control thread only)
    fn capabilities(&self) -> &VenueCaps;      // amend, bulk cancel, STP, cancel-on-disconnect, post-only, limits
}
```

Each adapter handles authentication and signing, the connection lifecycle (heartbeats, reconnect with backoff, resubscribe and resnapshot), sequence validation, symbol and tick/lot normalization, error-code mapping, and the venue's rate limits.

**Adapter conformance suite:** every adapter must pass the same scenario tests against recorded venue traffic and the venue's testnet before it can be enabled: gap recovery, reconnect mid-order, fill-before-ack, cancel of a filled order, rejects, rate-limit responses and maintenance windows.

### 9.1 Shared venue plumbing (write once, reuse everywhere)

Binance, Bybit and similar exchanges differ in message formats but share the same mechanics. Those mechanics live in one shared module, and each adapter only supplies the venue-specific parts:

| Shared building block | What an adapter supplies |
|---|---|
| WebSocket protocol (allocation-free, ADR 0006) and connection manager: connect, ping/pong, reconnect with jittered backoff, forced reconnect before the venue's connection lifetime expires | URLs, ping format, lifetime |
| Snapshot + delta book sync with sequence validation and resync | How to fetch a snapshot and which fields carry the sequence numbers |
| Request signing (HMAC-SHA256, Ed25519, RSA) | Which fields are signed and in what order |
| Rate limiter (token buckets for weight, order count and connection limits) | The limit table and the response headers that report usage |
| Symbol, tick, lot and filter normalization | The exchange-info endpoint and field mapping |
| Error mapping into a single `VenueError` enum | The venue's error codes |

A new "similar" exchange should mostly be a new decoder, a signing config and a limits table.

### 9.2 Binance Spot (venue 1)

| Area | Plan |
|---|---|
| Market data | Diff-depth WebSocket stream plus REST snapshot, synchronized with Binance's documented `lastUpdateId` / `U` / `u` procedure. Trade stream for flow signals. Evaluate the binary (SBE) market data streams in Phase 8. |
| Order entry | WebSocket API (persistent session, Ed25519 key, session logon) for lowest latency. REST is fallback and reconciliation only. Evaluate the FIX API for order entry and drop copy. |
| Order updates | User data stream over the WebSocket API. |
| Order types | `LIMIT_MAKER` (post-only) for all quotes. Cancel-replace and amend-keep-priority (quantity reduction) where they save rate-limit budget. |
| Self-trade prevention | Venue STP mode set on every order, plus our own local check. |
| Rate limits | Request weight per minute, order count per 10 s and per day, connection limits, all tracked from the usage headers. |
| Hosting | AWS Tokyo (ap-northeast-1), confirmed by measured round-trip before committing. |

### 9.3 Bybit Spot (venue 2)

| Area | Plan |
|---|---|
| Market data | V5 public WebSocket order-book topic (snapshot + delta, update ID and sequence checks) and public trades. |
| Order entry | V5 WebSocket trade API for lowest latency. REST for fallback and reconciliation. Batch place and cancel where available for spot. |
| Order updates | V5 private WebSocket `order`, `execution` and `wallet` topics. |
| Order types | Post-only limit orders. Native amend. |
| Self-trade prevention | Venue SMP setting plus our own local check. |
| Cancel on disconnect | Disconnection cancel protection (DCP), if enabled for spot on our account. |
| Rate limits | Per-endpoint and per-UID limits, tracked from response headers. |
| Hosting | Measure round-trip from candidate AWS regions (Singapore and Tokyo first) before choosing. |

Venue details above are the plan. Each one is checked against the venue's current API documentation and testnet at the start of that venue's phase, and differences are recorded as an ADR. Any safety feature a venue lacks (for example cancel on disconnect) is covered by the independent watchdog (§7.3).

### 9.4 Room to grow

Perpetuals, margin and more venues are out of scope today. The design keeps room for them without building them now: instruments carry a `kind` (only `Spot` exists today), inventory accounting sits behind the `bowst-position` interface, and the risk gate is a list of rules so margin rules can be added later. Nothing speculative is implemented until it is scheduled.

---

## 10. Testing and validation

Nothing goes live without passing all of these layers, in this order.

1. **Unit and property tests** (`proptest`): book invariants (never crossed after valid updates, levels sorted), OMS transitions (every venue message order), risk-gate truth tables, and fixed-point rounding.
2. **Fuzzing** (`cargo-fuzz`): every venue message decoder. Malformed input must never panic.
3. **Adapter conformance** (§9) against recorded traffic and testnets.
4. **Simulation and backtest:** the strategy runs against `bowst-sim` fed with recorded L2/L3 data, including realistic latency and queue position. Report PnL, markouts, inventory paths and fill rates.
5. **Performance gates:** criterion micro-benchmarks and an end-to-end tick-to-order benchmark measured with HDR histograms. CI fails on any hot-path allocation (counting-allocator tests) and on benchmarks that no longer compile. Failing CI on a *latency* regression needs a dedicated, pinned benchmark host, because shared CI runners vary by more than the 10% threshold. That host is set up with production hosting in Phase 5. Until then, hot-path PRs attach before/after numbers (CLAUDE.md §3).
6. **Chaos tests:** kill the process mid-session, drop sockets, inject sequence gaps, delay acks, and duplicate or reorder messages. The engine must always recover to a safe state (no unknown orders, no uncapped exposure).
7. **Paper trading:** live market data with simulated fills, running for at least 2 weeks.
8. **Shadow / minimum size live:** real orders at the venue minimum size with tight limits, for at least 2 weeks.
9. **Staged capital ramp:** limits raised step by step (for example 1% → 5% → 25% → 100% of target), each step gated by the review metrics in §16.

CI (GitHub Actions) runs `fmt`, `clippy -D warnings`, tests, doc build, Miri on all `unsafe` code, `cargo-deny` (licenses and advisories), `cargo-audit`, a benchmark build, and a 60-second fuzz run of every decoder on every PR. `main` is protected and releases are tagged and reproducible.

---

## 11. Observability and operations

- **Metrics (Prometheus → Grafana):** tick-to-order latency histograms, venue round-trip, book age, quote uptime per instrument, fill rate, markouts, position, PnL (gross, fees, net), rate-limit headroom, reconnects, rejects by reason and risk-gate blocks by rule.
- **Alerts (PagerDuty/Telegram/Slack):** kill switch fired, reconciliation mismatch, drawdown threshold, venue disconnect beyond N seconds, latency SLO breach, quote uptime below target, and clock drift.
- **Logs:** a binary hot-path log decoded off-thread into structured JSON, shipped to central storage.
- **Runbooks** in `docs/runbooks/` for each alert: what it means, the first 5 minutes, and escalation.
- **Daily report:** PnL attribution (spread capture vs. inventory vs. fees and rebates), volume, uptime and incidents.

---

## 12. Security

- **API keys:** trade-only permissions with **withdrawals disabled**, IP-allowlisted to production hosts, and a separate key per venue and environment. Stored in a secrets manager (for example AWS Secrets Manager or Vault), loaded into memory at startup, never logged and never in the repo.
- **Watchdog keys** (§7.3) are cancel-only or read-only where the venue supports it.
- **Control plane:** mTLS, a bastion or VPN only, never exposed to the internet. Every command is audit-logged with the operator's identity.
- **Supply chain:** `cargo-deny` and `cargo-audit`, a pinned lockfile, a minimal dependency set, and dependency upgrades reviewed.
- **Access:** production access is least-privilege and MFA-protected. Deploys go only through CI-built, signed artifacts.

---

## 13. Deployment and infrastructure

- **Placement decides network latency.** Run in the venue's primary region and availability zone: AWS Tokyo (ap-northeast-1) is the expected location for Binance, and Bybit's region is chosen by measurement (§9.3). Measure round-trip from candidate hosts before choosing.
- **One engine per venue region.** Cross-region coordination (net exposure, global kill) goes over a low-bandwidth control channel, never on the hot path.
- **Hosts:** bare-metal or metal-class cloud instances. No noisy neighbors, and use a dedicated NIC where available. Linux tuning: `isolcpus`, `nohz_full`, `rcu_nocbs`, disabled C-states and frequency scaling, huge pages, IRQ affinity and tuned busy-polling. Kernel-bypass networking (AF_XDP/DPDK/Onload) is a later optimization, used only where venue-side latency makes it worthwhile.
- **Clock:** chrony with a PTP/NTP hardware source where available. Drift beyond 1 ms raises an alert.
- **Deploys:** blue/green per venue. Deploy only during low-activity windows. Startup always goes through the safe startup sequence (§8). Rollback means redeploying the previous signed artifact.

---

## 14. Repository layout

```
bowst/
├── Cargo.toml                 # workspace
├── rust-toolchain.toml
├── crates/
│   ├── bowst-core/
│   ├── bowst-book/
│   ├── bowst-venue/           # trait + adapters/ (one module per venue)
│   ├── bowst-oms/
│   ├── bowst-risk/
│   ├── bowst-strategy/
│   ├── bowst-position/
│   ├── bowst-journal/
│   ├── bowst-sim/
│   ├── bowst-control/         # API + bowstctl CLI
│   └── bowst-telemetry/
├── bin/bowst/                 # engine entrypoint
├── bin/bowst-md/              # market-data operator tool (live books, health)
├── bin/bowst-watchdog/        # independent cancel-all watchdog
├── config/                    # example configs (no secrets)
├── benches/                   # end-to-end latency benchmarks
├── fuzz/
├── tests/                     # integration, conformance, chaos
├── deploy/                    # host tuning, systemd units, IaC
└── docs/
    ├── adr/                   # architecture decision records
    └── runbooks/
```

---

## 15. Phased build plan

Each phase has exit criteria. A phase is not done until its criteria are met and reviewed. Durations assume a small senior team (2–4 engineers) and are estimates.

| Phase | Scope | Exit criteria | Est. |
|---|---|---|---|
| **0. Foundations** | Workspace, CI, core types (fixed-point, IDs, events), SPSC ring, clock, allocation-counting allocator, ADRs. | CI green with lint, test, Miri, supply-chain and bench-build jobs. Ring handoff within noise of the host's bare atomic-signal floor. | 1–2 wk |
| **1. Market data** | Shared venue plumbing (§9.1) and the Binance Spot adapter (market data only), book builder with gap and resync handling, journal, telemetry. | 72 h continuous run with zero undetected gaps. Book matches venue snapshots. Decode + book update < 5 µs p99. | 2–3 wk |
| **2. Order entry + OMS** | Binance Spot order entry, OMS state machine, reconciliation, safe startup and shutdown, cancel-on-disconnect. | Conformance suite passes on testnet. Chaos tests leave zero orphaned orders. | 3–4 wk |
| **3. Risk + control plane** | Full pre-trade gate, monitors, kill switch, watchdog process, `bowstctl`, limit config signing. | Every risk rule has a test proving it blocks. Kill-to-all-cancelled < 1 s verified on testnet. | 2–3 wk |
| **4. Strategy + simulator** | Baseline quoting (§6), `bowst-sim`, backtest harness, markout analytics. | Positive expected net PnL after fees in the backtest over multiple regimes, with sane inventory. End-to-end tick-to-order < 20 µs p50. | 3–4 wk |
| **5. Paper → min-size live** | Production hosting, dashboards, alerts, runbooks, paper then minimum-size live on Binance Spot. | 2 weeks paper plus 2 weeks minimum-size with no unexplained reconciliation breaks and no risk-rule misses. Live markouts consistent with the sim. | 4 wk |
| **6. Capital ramp (Binance)** | Staged limit increases. | Review at each step: PnL, markouts, drawdown, uptime and incidents. | 4+ wk |
| **7. Bybit Spot** | Bybit adapter on the shared plumbing, cross-venue fair value, global inventory limits, cross-venue rebalancing. | Bybit repeats phases 1–6 in reduced form. The adapter should need little new plumbing code; if it does, the shared layer is fixed first. | 4–6 wk |
| **7b. Similar venues** | Further spot exchanges (for example OKX), one at a time. | Same as Phase 7. | ongoing |
| **8. Performance hardening** | Kernel bypass, binary protocols, profile-guided optimization, where measurements justify it. | Measured improvement in live fill quality, not just benchmarks. | ongoing |

The earliest realistic date for **meaningful live capital is about 4–5 months** from the start of Phase 0. Skipping the validation phases is how market makers lose money quickly.

---

## 16. Go-live checklist

Every item needs a named owner and sign-off before real capital is enabled on a venue.

- [ ] All §10 test layers pass on the release commit.
- [ ] Kill switch tested live on the production venue (orders placed, then killed, all cancelled, verified over REST).
- [ ] Venue cancel-on-disconnect verified by killing the process with `kill -9`.
- [ ] Watchdog verified by stopping the engine heartbeat.
- [ ] Reconciliation clean for 7 consecutive days at minimum size.
- [ ] API keys: withdrawals disabled, IP-allowlisted, stored in the secrets manager, rotation procedure documented.
- [ ] Alerts route to an on-call human 24/7, and a paging test has been received.
- [ ] Runbooks exist for every alert. At least one operator other than the author has done a dry run.
- [ ] Limits file reviewed and signed by two people.
- [ ] Legal and compliance sign-off for each venue and jurisdiction (§17).

---

## 17. Open decisions for the business

The architecture above holds either way, but these answers change the build order and the adapter work:

**Decided:** spot only, Binance first, Bybit second, similar exchanges after that (§9).

1. **Trading pairs.** Which pairs to start with on Binance? Deep pairs (for example BTC/USDT) are competitive and tight. Mid-liquidity pairs usually pay better spreads but carry more inventory risk.
2. **Account tier.** Current VIP tier and fee rates on each venue. Maker fees decide whether a spread is profitable at all.
3. **Venue market-maker programs.** Are we joining official MM programs (fee rebates, uptime and spread obligations)? The obligations become hard strategy constraints.
4. **Capital and limits.** Starting capital per venue, max drawdown tolerance per day and in total, and the target inventory range.
5. **Hosting budget.** Cloud in the venue region is the default. Bare-metal colocation where offered costs more and is faster.
6. **Legal entity, licensing and jurisdiction.** Market making may need registration depending on venue and jurisdiction. This must be resolved before live trading. Note: the development environment is in a location both venues restrict (`api.binance.com` returns HTTP 451 and Bybit returns HTTP 403). Test fixtures are recorded from Binance's official public market-data mirror (`data-api.binance.vision`), which serves public data only. Whether the business may trade on each venue depends on the trading entity's jurisdiction and the venue's terms, not on where servers are placed, and needs legal confirmation. Geo-restrictions are never circumvented.
7. **Team.** Who owns on-call, risk-limit sign-off and the four-eyes approvals?

---

*Architecture changes are recorded as ADRs in `docs/adr/`. This README is updated in the same PR as any change that contradicts it.*
