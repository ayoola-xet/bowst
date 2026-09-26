# Runbook: market data

Applies to the Binance Spot market-data session (`bowst_venue::binance::md`) and the `bowst-md` tool. Every status below is emitted as an `MdStatus` event.

## Quick check

```sh
cargo run --release -p bowst-md -- --symbols BTCUSDT,ETHUSDT --seconds 60
```

Healthy output: every symbol `LIVE`, a spread of at least 1 tick (never 0 or negative), steady updates per second, a `[stats]` line every 10 seconds, and a summary with `0 book invalidations`, `1 connections` and `0 mismatched` verifications.

## `InstrumentDown` (one instrument)

**Meaning:** that instrument's book is unusable. Quoting on it must stop immediately; the session already does this by not reporting the book. Reasons:

| Reason | Cause | Expected recovery |
|---|---|---|
| `sequence gap` | A diff-depth message was lost (network, venue, or our own buffer) | Automatic: a new snapshot is fetched, usually within 1 to 2 seconds |
| `crossed book` | Best bid at or above best ask after an update, which means lost or misapplied data | Automatic resync. **Repeated crossings are a bug**: capture logs and escalate |
| `invalid ...` / `duplicate ...` | The venue sent data that fails validation | Automatic resync. If it repeats, the venue changed its format: escalate |
| `book differs from a fresh snapshot ...` | Verification rebuilt the book from a fresh snapshot and it did not match ours, although every sequence check passed | Automatic resync. **Always escalate**: see below |

**First five minutes:**
1. Check whether it recovers (`InstrumentLive` for the same instrument within about 5 seconds).
2. If it flaps (down and up repeatedly), check `SnapshotFailed` messages and the host's network.
3. If only one instrument flaps, check the venue's status page for that symbol (maintenance, delisting).

## Verification mismatch (`book differs from a fresh snapshot`)

**Meaning:** once a minute, one instrument's book (in turn) is rebuilt from a fresh REST snapshot plus the same deltas, and the top 100 levels per side are compared with the live book (ADR 0010). A difference means our book was wrong without any gap being visible: a decoding or book bug, or the venue sending inconsistent data. The book is taken down and resynchronized, so quoting stops on that instrument until it is live again.

**What to do:**
1. Confirm it recovered (`live` for the same instrument).
2. Keep the journal. The status text names the side, level and update ID, and `bowst-md --replay` reproduces the exact event.
3. Escalate with the journal and the status line. A single mismatch is a correctness incident even if it recovered.

## `[stats]` lines

Every 10 seconds: messages in the interval, decode-and-apply latency (p50, p99, p99.9 and max of the time to decode one message and apply it to its book), and cumulative verification counts (`ok`, `mismatched`, `abandoned`). `abandoned` means a verification produced no verdict: the snapshot failed, was older than the buffered deltas, or the connection reset. An occasional one is harmless; a steady rise means snapshots are failing (see below).

Latency depends on message size: Binance batches 100 ms of changes into each message, and the cost is roughly proportional to the number of changed levels. Numbers from a shared or sleeping machine (like `bowst-md`, which sleeps when idle) are higher than on an isolated, busy-polling core.

## `SnapshotFailed`

**Meaning:** a REST depth request failed. It is retried automatically.

- `HTTP status 429`: we hit the REST weight limit. Snapshot requests pause for `Retry-After`. Check whether another process on the same IP is using weight; lower `rest_weight_per_minute` if needed.
- `HTTP status 418`: **the IP is banned** for ignoring 429s. Stop every process on the host, find the cause, and wait out the ban. Escalate immediately.
- Timeouts or TLS errors: network or venue trouble; see `Disconnected` below.

## `Disconnected` (all instruments)

**Meaning:** every book is down until the session reconnects and resyncs. Reconnects use jittered exponential backoff (250 ms, doubling to 30 s).

| Reason | Cause |
|---|---|
| `no data for ...` | The connection went silent (no data and no pings) for `stale_after` |
| `connection reached its maximum age` | Planned: reconnect before Binance's 24-hour cutoff. Expect a brief resync once a day |
| `server closed the connection` | Venue-side close, often maintenance |
| `undecodable message` | The venue sent something we cannot parse. **Escalate**: the stream format may have changed |
| `connection closed by peer` / I/O errors | Network trouble |
| TLS errors | Certificate problem; check the host's trust store and clock |

**First five minutes:** if reconnects keep failing for more than a minute, check the venue's status page and API announcements, DNS, and outbound connectivity from the host (`curl` the REST endpoint).

## Configuration reference

Defaults are set in `MdConfig::new`: 1,000-level snapshots, `stale_after` 60 s, `max_connection_age` 23 h, backoff from 250 ms to 30 s, 1,200 REST weight per minute (Binance allows 6,000 per IP), verification every 60 s (abandoned after 30 s without a verdict), and a report every 10 s.

## Alerts

Prometheus alert rules are in `docs/deploy/prometheus/bowst-alerts.yml`; setup is in `docs/deploy/monitoring.md`. Each alert below links here.

### Alert: BowstMdScrapeFailing
**Critical.** Prometheus cannot reach the metrics endpoint: the process exited, hung, or was started without `--metrics`. Check `systemctl status` or the terminal, and the end of the log for a panic or exit message. If the process is running but the endpoint does not answer, capture a stack dump before restarting.

### Alert: BowstMdStalled
**Critical.** The session has not reported for over a minute although the endpoint answers: the market-data thread is stuck. Books cannot be trusted. Capture the log and a stack dump, then restart. Treat it as a bug and escalate.

### Alert: BowstMdDisconnected
**Critical.** The WebSocket has been down for over 2 minutes. See `Disconnected` above: check the venue status page, DNS and outbound connectivity.

### Alert: BowstMdInstrumentDown
**Critical.** One book has been down for over 2 minutes while connected: resynchronization keeps failing. See `InstrumentDown` and `SnapshotFailed` above; check for 429 or 418 responses and whether the symbol is in maintenance.

### Alert: BowstMdVerificationMismatch
**Critical.** A live book differed from a fresh snapshot. See "Verification mismatch" above: keep the journal and escalate, even if the book recovered.

### Alert: BowstMdBooksFlapping
**Warning.** More than 5 book invalidations in 10 minutes. Usually network loss or venue trouble; check whether the reasons are gaps (network), crossed books or mismatches (escalate), and whether one symbol or all are affected.

### Alert: BowstMdVerificationsNotRunning
**Warning.** No verification has passed in 15 minutes while connected. Snapshots for verification are failing or being abandoned: check `SnapshotFailed` statuses and REST weight usage.

### Alert: BowstMdLatencyHigh
**Warning.** Decode-and-apply p99 has been above 25 µs (ADR 0011) for 10 minutes. On a laptop or shared VM this is expected. On production hardware, check that the market-data thread has an isolated core, that the host is not overloaded, and whether message sizes grew (a volatile market). Compare with the `md_books` benchmark on the same build.

### Alert: BowstJournalNotOk
**Critical.** The event journal dropped records or stopped persisting, so the audit trail is incomplete (ADR 0009). Check disk space and I/O errors in the log. Trading must not run without a healthy journal; `bowst-md` keeps streaming, but the soak run fails its acceptance criteria.

