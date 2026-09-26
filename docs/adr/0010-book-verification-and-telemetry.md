# 0010. Book verification and market-data telemetry

- Status: Accepted
- Date: 2026-09-26

## Context
Phase 1 exit criteria (README §15) require that books match venue snapshots and that decode plus book update stays under 5 µs at p99. Sequence checks catch lost messages, but not a book that is wrong for another reason: a decoding or book bug, or a venue that sends inconsistent data. Nothing measured latency on the live path.

## Decision
- **Verification (fail closed).** Every `verify_every` (default 60 s), one live instrument, taken in turn, is verified:
  - A second ("shadow") `BookSync`, allocated once at startup, receives the same deltas as the live book from the moment verification starts.
  - A fresh REST snapshot is loaded into it.
  - As soon as both books have applied the same last update ID, their top 100 levels per side are compared. That is well inside the 1,000-level snapshot, so levels the live book knows beyond the snapshot's edge are never compared.
  - A difference takes the live book down with `book differs from a fresh snapshot`, and it resynchronizes.
  - A snapshot too old to bridge, a failed request, a gap on the live book, a reconnect, or no verdict within `verify_timeout` (30 s) abandons the verification without a verdict.
  - The logic lives in the pure `MdBooks` core. The start and the snapshot are journaled (`MD_VERIFY_START`, `MD_VERIFY_SNAPSHOT`), so a replay reproduces verifications and mismatches exactly.
- **Latency.** The session times `MdBooks::apply` (decode plus book update, excluding journaling and the handler) for every message. It records the time in a log-linear histogram (`bowst-telemetry`): 32 sub-buckets per power of two, so percentiles overstate by at most about 3% and never understate. Recording takes constant time and never allocates. There is no floating point: percentiles are requested in parts per million.
- **Reports.** Every `report_every` (default 10 s) the handler receives an `MdReport`: cumulative counters and the interval's latency summary. `bowst-md` prints it as a `[stats]` line.
- **Metrics export** (Prometheus) is not part of this change. It needs the control-plane HTTP endpoint and will read the same `MdReport`.

## Consequences
- Book correctness is checked continuously against the venue, not only at resync. With three instruments, each is verified every three minutes, at a REST cost of 10 weight per verification.
- A verification briefly doubles delta-application work for one instrument (the shadow book), and costs one snapshot's memory, allocated at startup.
- Measured on recorded Binance traffic with warm caches on the development VM: about 175 ns per changed level, half decoding and half book update. Messages carry 100 ms of batched changes: 10 levels at the median and about 140 at p99. Per message, that gives p50 1.8 µs, mean about 4 µs and **p99 about 25–29 µs, well above the 5 µs target**. Live on the same VM, where the tool sleeps between messages, p99 was about 40 µs. The target as written cannot be met without roughly a fivefold cut in per-level cost. Whether to optimize, restate the target per level, or both is an open decision, recorded in README §15.
