# 0005. In-house allocation-free JSON reader for venue messages

- Status: Accepted
- Date: 2026-09-25

## Context
Binance and Bybit Spot publish market data as JSON. Every message is decoded on the market-data thread, so decoding sits directly on the hot path. The engine's rules require no allocation after warm-up (CLAUDE.md §1.6), exact conversion of prices and quantities (ADR 0002), and that malformed input returns an error rather than panicking.

General-purpose options were considered:
- `serde_json` into typed structs allocates `Vec`s for level arrays and `String`s for escaped text.
- SIMD parsers (`simd-json`, `sonic-rs`) are fast on large documents, but their zero-allocation behavior for our exact access pattern would have to be verified and then re-verified on every upgrade. They also add substantial dependency trees.

## Decision
- `bowst_venue::json::Reader`: a small, strict pull reader over the received bytes. It validates structure, limits nesting depth, borrows strings from the input and never allocates. Every JSON venue adapter uses it (DRY).
- Level arrays (`[["price","qty"], ...]`, shared by Binance and Bybit) are decoded by one shared helper (`bowst_venue::levels`).
- Prices and quantities are parsed directly into ticks and lots with `Increment::parse_units`. For power-of-ten increments it checks exactness without dividing; anything unusual falls back to the general exact path, so results and errors are identical (property-tested).
- Decoders are fuzzed in CI, seeded with real recorded messages.

## Consequences
- Measured on the development VM with recorded Binance traffic: a typical 10-level diff-depth message decodes in about 1.27 µs, and a 1,000-level snapshot in about 180 µs, with zero allocations. The JSON walk itself is about half of that; the rest is string handling and number conversion.
- Fuzzing found a panic on non-ASCII input in the number fast path before merge. It is fixed, and the crashing inputs are kept as permanent seeds.
- Binance's binary (SBE) market-data streams avoid JSON entirely and are planned for Phase 8, where they will be measured against this baseline.
