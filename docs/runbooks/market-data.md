# Runbook: market data

Applies to the Binance Spot market-data session (`bowst_venue::binance::md`) and the `bowst-md` tool. Every status below is emitted as an `MdStatus` event.

## Quick check

```sh
cargo run --release -p bowst-md -- --symbols BTCUSDT,ETHUSDT --seconds 60
```

Healthy output: every symbol `LIVE`, a spread of at least 1 tick (never 0 or negative), steady updates per second, and a summary with `0 book invalidations` and `1 connections`.

## `InstrumentDown` (one instrument)

**Meaning:** that instrument's book is unusable. Quoting on it must stop immediately; the session already does this by not reporting the book. Reasons:

| Reason | Cause | Expected recovery |
|---|---|---|
| `sequence gap` | A diff-depth message was lost (network, venue, or our own buffer) | Automatic: a new snapshot is fetched, usually within 1 to 2 seconds |
| `crossed book` | Best bid at or above best ask after an update, which means lost or misapplied data | Automatic resync. **Repeated crossings are a bug**: capture logs and escalate |
| `invalid ...` / `duplicate ...` | The venue sent data that fails validation | Automatic resync. If it repeats, the venue changed its format: escalate |

**First five minutes:**
1. Check whether it recovers (`InstrumentLive` for the same instrument within about 5 seconds).
2. If it flaps (down and up repeatedly), check `SnapshotFailed` messages and the host's network.
3. If only one instrument flaps, check the venue's status page for that symbol (maintenance, delisting).

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

Defaults are set in `MdConfig::new`: 1,000-level snapshots, `stale_after` 60 s, `max_connection_age` 23 h, backoff from 250 ms to 30 s, 1,200 REST weight per minute (Binance allows 6,000 per IP).
