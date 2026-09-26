# 0012. Prometheus metrics export

- Status: Accepted
- Date: 2026-09-26

## Context
Phase 1 needs the market-data session to be observable while it runs: books live or down, message rates, verification results, latency and journal health. README §11 names Prometheus and Grafana. Metrics must not slow the hot path, and nothing may expose an unauthenticated service to the internet (README §12).

## Decision
- **Format:** `bowst-telemetry` gains a Prometheus text-format writer (`exposition`) and a minimal HTTP endpoint (`server`), both with the standard library only (no new dependencies).
  - The writer checks metric and label names, escapes label values, and writes durations as exact decimal seconds from integer nanoseconds.
- **Endpoint:** serves only `GET /metrics`, on its own thread, one request at a time. Every connection is closed after the response.
  - Requests are bounded: 8 KiB of headers and a 2 s I/O timeout.
  - After responding, it half-closes and drains the client briefly, so its reply is not lost to a TCP reset.
  - It binds only to loopback unless the caller explicitly allows otherwise. Remote access goes through an SSH tunnel or VPN.
- **Rendering:** happens on the endpoint's thread for each scrape, from state the application already keeps. The market-data thread only hands over its existing 10-second `MdReport` (now including run-long latency totals). Nothing is added to the per-message path.
- **Latency:** exported as a Prometheus summary. Quantiles cover the last report interval; `_sum` and `_count` cover the whole run (`LatencyHistogram` now tracks a sum).
- **Staleness:** a `bowst_md_last_report_timestamp_seconds` gauge makes a stalled market-data thread visible even while the endpoint still answers.
- **Alerts:** example rules (`docs/deploy/prometheus/bowst-alerts.yml`), each with a runbook entry, validated with `promtool`.

## Consequences
- Prometheus and Grafana can watch the soak run and, later, the engine, using the same library.
- A slow client can delay other scrapes by up to the I/O timeout. At normal scrape intervals (15 s) that is harmless; the endpoint is not a general web server.
- Pushing alerts to a phone or chat needs Alertmanager, which is not part of this change.
- Only `bowst-md` exports metrics today. The engine adds its own metrics (orders, fills, risk blocks, PnL) with the same writer when those components exist.
