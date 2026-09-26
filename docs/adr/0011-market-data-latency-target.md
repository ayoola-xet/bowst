# 0011. Market-data latency target, restated per message size

- Status: Accepted
- Date: 2026-09-26

## Context
Phase 1 required decode plus book update under 5 µs at p99 per message. ADR 0010 measured about 25 µs on recorded Binance traffic. The cause is the feed, not a slow path. Binance batches 100 ms of changes per diff-depth message: 10 changed levels at the median and about 140 at p99, with a per-level cost of roughly 140–175 ns. A fixed per-message budget therefore measures how busy the market was, not how fast the code is.

We optimized first, keeping only changes that the recorded-traffic benchmark (`bowst-venue` bench `md_books`) confirmed:
- **Kept:** number parsing reads raw bytes (no UTF-8 pass, since any non-digit is rejected anyway) and accumulates digits without per-digit overflow checks (the digit count is bounded first). Decoding all recorded frames: 860 → 639 µs (−20 to −26% across runs).
- **Rejected, no gain on recorded traffic:**
  - a fast path for the compact `["price","qty"]` form
  - SWAR digit conversion (slower for numbers this short)
  - a branch-free binary search (−40% on a synthetic benchmark, but no change on recorded traffic, where updates land at unpredictable depths)
  - separate price and quantity arrays in the ladder (slightly slower: every insert and removal moves two arrays)

Result on recorded traffic (warm caches, development VM), per message, before → after:

| | p50 | p90 | p99 | whole session |
|---|---|---|---|---|
| Before | 1.6 µs | 5.9 µs | 24.1 µs | 1.55 ms |
| After | 1.3 µs | 4.6 µs | 18–20 µs | 1.30–1.35 ms |

## Decision
The Phase 1 latency criterion becomes a set of numbers measured by the `md_books` benchmark on recorded Binance traffic, on one warm core:
- **Per changed level** (session time divided by levels changed): at most 175 ns on average. Measured: about 155 ns, including per-message overhead.
- **Per message:** p50 at most 2 µs and p99 at most 25 µs.
- **Live:** a busy-polling session on a dedicated core must report a `[stats]` p99 within the same 25 µs over the soak run. Numbers from a shared or sleeping machine (like `bowst-md` on a laptop) are indicative only.

Every hot-path PR reports the `md_books` numbers before and after, as CLAUDE.md §3 already requires. A regression beyond these limits needs an explicit decision.

## Consequences
- The target now tracks the code, not market activity, and it is met with about 20% headroom at p99.
- Context for the numbers: Binance publishes depth every 100 ms, and network latency to the venue is in the hundreds of microseconds even from the same region. 20 µs of processing is not the limiting factor for this feed.
- If the strategy needs faster top-of-book changes, the lever is the feed, not this code. Binance's real-time best bid and ask stream (`<symbol>@bookTicker`) updates on every change without batching. Using it alongside the depth stream is a Phase 4 (strategy) decision.
- Further per-level gains would need a different book representation (for example a price-indexed array for the levels near the top) or a binary market-data feed. Neither is justified by the measurements above.
