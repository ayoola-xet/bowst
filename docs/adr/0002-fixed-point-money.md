# 0002. Fixed-point integers for prices and quantities

- Status: Accepted
- Date: 2026-09-25

## Context
Binary floating point cannot represent most decimal prices exactly (`0.1 + 0.2 != 0.3`). Rounding errors in prices, sizes or balances cause venue rejects at best and wrong positions or crossed quotes at worst. Venues send numbers as decimal strings and define per-instrument tick sizes and lot steps, which are not always powers of ten (for example a 0.05 tick or a lot step of 5).

## Decision
- Venue text is parsed straight into an exact decimal (`Dec`: `i128` mantissa and decimal scale), never through `f64`.
- Every instrument has two `Increment`s (tick size and lot step). Prices are stored as `Price` (whole ticks, `i64`) and quantities as `Qty` (whole lots, `i64`).
- Every conversion from decimal to ticks or lots names its rounding direction. Quotes use `Side::passive_rounding()`: bids round down and asks round up, so rounding can never push a quote through fair value. Venue data uses `Rounding::Exact` so malformed data is detected rather than silently rounded.
- `f64` is allowed only inside strategy model math, converted back with an explicit rounding direction.
- All arithmetic in `bowst-core` is checked; overflow is an error, never a wrap.

## Consequences
- Money math is exact and deterministic, which also makes replays reproducible bit for bit.
- Hot-path comparisons and additions are plain `i64` operations.
- Code must carry each instrument's increments wherever decimals are produced or consumed. This is deliberate: the increment is part of the value's meaning.
