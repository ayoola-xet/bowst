# Monitoring with Prometheus and Grafana

`bowst-md` serves Prometheus metrics when started with `--metrics ADDR`. This guide covers scraping them, loading the alert rules, and viewing them in Grafana, on the same machine or from your laptop.

## 1. Enable the endpoint

```sh
bowst-md --symbols BTCUSDT,ETHUSDT --seconds 259200 --journal ~/bowst-soak/journal --metrics 127.0.0.1:9184
curl -s http://127.0.0.1:9184/metrics | head
```

The endpoint has no authentication, so `bowst-md` only accepts a loopback address. `--metrics-allow-remote` overrides this. Do not use it on a machine reachable from the internet (README §12). To look from another machine, tunnel over SSH instead:

```sh
ssh -N -L 9184:127.0.0.1:9184 user@server    # then open http://127.0.0.1:9184/metrics locally
```

For the systemd unit in `soak-test.md`, add `--metrics 127.0.0.1:9184` to `ExecStart`. The unit's `RestrictAddressFamilies=AF_INET AF_INET6` already allows it.

## 2. Metrics

Every market-data metric carries `venue="binance"`.

| Metric | Type | Meaning |
|---|---|---|
| `bowst_md_connected` | gauge | 1 while the WebSocket is connected |
| `bowst_md_instrument_live{symbol}` | gauge | 1 while that book is live and usable |
| `bowst_md_messages_total` | counter | Messages received |
| `bowst_md_deltas_applied_total` | counter | Depth updates applied to live books |
| `bowst_md_book_invalidations_total` | counter | Books taken down (gap, bad data, crossed, verification mismatch) |
| `bowst_md_connections_total` | counter | Connections opened |
| `bowst_md_snapshots_requested_total` / `_applied_total` | counter | REST snapshots requested, and those that brought a book live |
| `bowst_md_verifications_total{result}` | counter | Book verifications: `passed`, `mismatch`, `abandoned` (ADR 0010) |
| `bowst_md_apply_latency_seconds` | summary | Decode plus book update per message: quantiles over the last 10 s, sum and count over the run (ADR 0011) |
| `bowst_md_last_report_timestamp_seconds` | gauge | Unix time of the latest 10-second report; stops advancing if the market-data thread stalls |
| `bowst_journal_state{state}` | gauge | 1 for the current state: `ok`, `degraded`, `failed` (only when journaling) |
| `bowst_journal_dropped_records_total` | counter | Records the journal could not accept |
| `bowst_build_info{version}`, `bowst_start_time_seconds` | gauge | Build and process start time |

Counters reset when the process restarts. Use `rate()` and `increase()`, which handle resets.

## 3. Prometheus

Install Prometheus from <https://prometheus.io/download/>, or with your package manager (`brew install prometheus` on macOS). Then use this `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
rule_files:
  - /path/to/bowst/docs/deploy/prometheus/bowst-alerts.yml
scrape_configs:
  - job_name: bowst-md
    static_configs:
      - targets: ["127.0.0.1:9184"]
```

```sh
promtool check config prometheus.yml
prometheus --config.file=prometheus.yml --web.listen-address=127.0.0.1:9090
```

Open <http://127.0.0.1:9090>. **Status → Targets** should show `bowst-md` as up, and **Alerts** lists the rules. `bowst-alerts.yml` defines nine alerts, and each links to its section in `docs/runbooks/market-data.md`. They cover:
- the process being down or stalled
- a disconnect, or a book down for more than 2 minutes
- a verification mismatch, or verifications not running
- books flapping
- p99 latency above target
- the journal not ok

Routing alerts to a phone or chat needs Alertmanager, which is not configured here.

## 4. Grafana

Install Grafana (`brew install grafana`, or <https://grafana.com/grafana/download>), add Prometheus at `http://127.0.0.1:9090` as a data source, and start with these panels:

| Panel | Query |
|---|---|
| Books live | `bowst_md_instrument_live` |
| Messages per second | `rate(bowst_md_messages_total[1m])` |
| Decode + apply p50 / p99 | `bowst_md_apply_latency_seconds{quantile=~"0.5\|0.99"}` |
| Mean decode + apply | `rate(bowst_md_apply_latency_seconds_sum[5m]) / rate(bowst_md_apply_latency_seconds_count[5m])` |
| Book invalidations per hour | `increase(bowst_md_book_invalidations_total[1h])` |
| Verifications | `increase(bowst_md_verifications_total[1h])` by `result` |
| Report age | `time() - bowst_md_last_report_timestamp_seconds` |
| Journal state | `bowst_journal_state == 1` |

Latency numbers from a laptop or shared VM, where `bowst-md` sleeps between messages, are well above those of a dedicated busy-polling core. Expect `BowstMdLatencyHigh` to fire there (ADR 0011).
