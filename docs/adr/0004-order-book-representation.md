# 0004. Order book as a sorted array with the best level last

- Status: Accepted
- Date: 2026-09-25

## Context
Each instrument's level-2 book is updated on every market-data message, and the strategy reads its top levels on every decision. Crypto spot books are deep (Binance snapshots go to 5,000 levels per side) and prices span a wide range of ticks, but almost all updates land within the first few levels of the best price.

The README originally proposed a price-indexed array ("ladder") around the mid. With small ticks, the range of prices a deep book spans is hundreds of thousands of ticks, so that layout is mostly empty memory, and finding the next best level after the best one is removed means scanning empty slots.

## Decision
- Each side is a pre-allocated array of `(price, qty)` levels sorted from worst to best, so the best level is last. Changes near the top move only the few levels above them.
- Lookups scan the best 16 levels linearly, then binary-search the rest.
- The array never grows past its configured capacity, so updates never allocate. When full, the worst level is dropped and the side records a *horizon*: the worst price it still knows exactly. Updates beyond the horizon are ignored until the next snapshot, so every level the book reports is exact.
- Sequencing is separate from the data structure: `BookSync` applies the shared snapshot-plus-delta rules (README §9.1) and only exposes the book while it is live. Gaps, invalid data and crossed books clear it and wait for a new snapshot.

## Consequences
- Measured on the development VM (`crates/bowst-book/benches/book.rs`, 1,000 levels per side): top-level update 11 ns, insert and remove near the top 17 ns, update 500 levels deep 53 ns, 20-level delta message 279 ns, best bid and ask 2 ns.
- A 16-level linear scan was chosen over 8 because it halved the 20-level delta cost (540 ns to 279 ns) for 10 ns more on rare deep updates. It will be re-tuned against recorded Binance traffic in Phase 1.
- Removing the worst level when full moves the whole array. This only happens when the book is at capacity, so capacity is set above the venue's snapshot depth.
