//! Prometheus metrics for `bowst-md`: what the market-data session and journal are doing,
//! rendered on each scrape from state the tool already keeps (see `docs/deploy/monitoring.md`).

use bowst_core::{InstrumentTable, WallTime};
use bowst_journal::JournalHealth;
use bowst_telemetry::exposition::{Exposition, MetricType, Value};
use bowst_venue::binance::md::{MdReport, MdStats};

/// Label value identifying the venue on every market-data metric.
const VENUE: &str = "binance";

/// Session state the metrics are rendered from, updated by the market-data handler and the
/// watch loop.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MetricsState {
    /// When the process started.
    pub(crate) started: WallTime,
    /// Whether the WebSocket is connected.
    pub(crate) connected: bool,
    /// The latest periodic report and when it arrived.
    pub(crate) report: Option<(MdReport, WallTime)>,
    /// Journal health, when journaling.
    pub(crate) journal: Option<JournalHealth>,
    /// Records the journal dropped, as last reported.
    pub(crate) journal_dropped: u64,
}

/// Renders every metric. `live` gives each instrument's book status by instrument ID.
pub(crate) fn render(
    state: &MetricsState,
    instruments: &InstrumentTable,
    live: impl Fn(u32) -> bool,
) -> String {
    let mut e = Exposition::new();
    process(&mut e, state);
    connection(&mut e, state, instruments, live);
    session(&mut e, state);
    journal(&mut e, state);
    debug_assert_eq!(e.rejected(), 0, "invalid metric or label name");
    e.finish()
}

fn process(e: &mut Exposition, state: &MetricsState) {
    e.family(
        "bowst_build_info",
        MetricType::Gauge,
        "Always 1; labels identify the build.",
    );
    e.sample(
        "bowst_build_info",
        &[("version", env!("CARGO_PKG_VERSION"))],
        Value::Int(1),
    );
    e.family(
        "bowst_start_time_seconds",
        MetricType::Gauge,
        "Unix time the process started.",
    );
    e.sample(
        "bowst_start_time_seconds",
        &[],
        Value::Nanos(state.started.as_nanos()),
    );
}

fn connection(
    e: &mut Exposition,
    state: &MetricsState,
    instruments: &InstrumentTable,
    live: impl Fn(u32) -> bool,
) {
    let venue = [("venue", VENUE)];
    e.family(
        "bowst_md_connected",
        MetricType::Gauge,
        "1 while the market-data WebSocket is connected.",
    );
    e.sample(
        "bowst_md_connected",
        &venue,
        Value::Int(u64::from(state.connected)),
    );
    e.family(
        "bowst_md_instrument_live",
        MetricType::Gauge,
        "1 while the instrument's book is live and may be used; 0 while it is down.",
    );
    for instrument in instruments.iter() {
        e.sample(
            "bowst_md_instrument_live",
            &[("venue", VENUE), ("symbol", instrument.symbol.as_str())],
            Value::Int(u64::from(live(instrument.id.get()))),
        );
    }
}

fn session(e: &mut Exposition, state: &MetricsState) {
    let venue = [("venue", VENUE)];
    let (md_stats, report_at) = state
        .report
        .map_or((MdStats::default(), None), |(r, at)| (r.stats, Some(at)));
    let counters: [(&str, &str, u64); 6] = [
        (
            "bowst_md_messages_total",
            "Market-data messages received.",
            md_stats.messages,
        ),
        (
            "bowst_md_deltas_applied_total",
            "Depth updates applied to live books.",
            md_stats.deltas_applied,
        ),
        (
            "bowst_md_book_invalidations_total",
            "Books taken down by a gap, invalid data, a crossed book or a verification mismatch.",
            md_stats.book_invalidations,
        ),
        (
            "bowst_md_connections_total",
            "WebSocket connections opened.",
            md_stats.connections,
        ),
        (
            "bowst_md_snapshots_requested_total",
            "REST depth snapshots requested (resyncs and verifications).",
            md_stats.snapshots_requested,
        ),
        (
            "bowst_md_snapshots_applied_total",
            "Snapshots that brought a book live.",
            md_stats.snapshots_applied,
        ),
    ];
    for (name, help, value) in counters {
        e.family(name, MetricType::Counter, help);
        e.sample(name, &venue, Value::Int(value));
    }
    e.family(
        "bowst_md_verifications_total",
        MetricType::Counter,
        "Book verifications against fresh snapshots, by result.",
    );
    for (result, value) in [
        ("passed", md_stats.verifications_passed),
        ("mismatch", md_stats.verification_mismatches),
        ("abandoned", md_stats.verifications_abandoned),
    ] {
        e.sample(
            "bowst_md_verifications_total",
            &[("venue", VENUE), ("result", result)],
            Value::Int(value),
        );
    }

    if let Some((report, _)) = state.report {
        e.family(
            "bowst_md_apply_latency_seconds",
            MetricType::Summary,
            "Time to decode one message and apply it to its book. Quantiles cover the last report interval; sum and count cover the whole run.",
        );
        e.summary(
            "bowst_md_apply_latency_seconds",
            &venue,
            &report.latency,
            &report.latency_total,
        );
    }
    e.family(
        "bowst_md_last_report_timestamp_seconds",
        MetricType::Gauge,
        "Unix time of the latest session report (every 10 s); stops advancing if the market-data thread stalls.",
    );
    e.sample(
        "bowst_md_last_report_timestamp_seconds",
        &venue,
        Value::Nanos(report_at.map_or(0, WallTime::as_nanos)),
    );
}

fn journal(e: &mut Exposition, state: &MetricsState) {
    if let Some(health) = state.journal {
        e.family(
            "bowst_journal_state",
            MetricType::Gauge,
            "1 for the journal's current state: ok, degraded (records dropped) or failed (not persisting).",
        );
        let current = match health {
            JournalHealth::Ok => "ok",
            JournalHealth::Degraded { .. } => "degraded",
            JournalHealth::Failed => "failed",
        };
        for state_name in ["ok", "degraded", "failed"] {
            e.sample(
                "bowst_journal_state",
                &[("state", state_name)],
                Value::Int(u64::from(state_name == current)),
            );
        }
        e.family(
            "bowst_journal_dropped_records_total",
            MetricType::Counter,
            "Records the journal could not accept.",
        );
        e.sample(
            "bowst_journal_dropped_records_total",
            &[],
            Value::Int(state.journal_dropped),
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bowst_core::{Dec, Increment, Instrument, InstrumentId, Symbol, VenueId};
    use bowst_telemetry::LatencySummary;
    use std::time::Duration;

    fn instruments() -> InstrumentTable {
        InstrumentTable::new(
            ["BTCUSDT", "ETHUSDT"]
                .iter()
                .enumerate()
                .map(|(i, s)| Instrument {
                    id: InstrumentId::new(u32::try_from(i).unwrap()),
                    venue: VenueId::Binance,
                    symbol: Symbol::new(s).unwrap(),
                    tick: Increment::parse("0.01").unwrap(),
                    lot: Increment::parse("0.0001").unwrap(),
                    min_notional: Dec::parse("5").unwrap(),
                })
                .collect(),
        )
        .unwrap()
    }

    fn state() -> MetricsState {
        let latency = LatencySummary {
            count: 100,
            sum: 500_000,
            min: 900,
            p50: 1_300,
            p90: 4_500,
            p99: 18_000,
            p999: 50_000,
            max: 60_000,
        };
        MetricsState {
            started: WallTime::from_nanos(1_700_000_000_000_000_000),
            connected: true,
            report: Some((
                MdReport {
                    interval: Duration::from_secs(10),
                    stats: MdStats {
                        messages: 2_144,
                        deltas_applied: 2_092,
                        book_invalidations: 1,
                        connections: 1,
                        snapshots_requested: 4,
                        snapshots_applied: 3,
                        verifications_passed: 1,
                        verification_mismatches: 0,
                        verifications_abandoned: 0,
                    },
                    latency,
                    latency_total: LatencySummary {
                        count: 2_144,
                        sum: 9_000_000,
                        ..latency
                    },
                },
                WallTime::from_nanos(1_700_000_075_500_000_000),
            )),
            journal: Some(JournalHealth::Ok),
            journal_dropped: 0,
        }
    }

    #[test]
    fn renders_every_metric() {
        let text = render(&state(), &instruments(), |id| id == 0);
        for line in [
            "bowst_md_connected{venue=\"binance\"} 1",
            "bowst_md_instrument_live{venue=\"binance\",symbol=\"BTCUSDT\"} 1",
            "bowst_md_instrument_live{venue=\"binance\",symbol=\"ETHUSDT\"} 0",
            "bowst_md_messages_total{venue=\"binance\"} 2144",
            "bowst_md_book_invalidations_total{venue=\"binance\"} 1",
            "bowst_md_verifications_total{venue=\"binance\",result=\"passed\"} 1",
            "bowst_md_verifications_total{venue=\"binance\",result=\"mismatch\"} 0",
            "bowst_md_apply_latency_seconds{venue=\"binance\",quantile=\"0.99\"} 0.000018000",
            "bowst_md_apply_latency_seconds_sum{venue=\"binance\"} 0.009000000",
            "bowst_md_apply_latency_seconds_count{venue=\"binance\"} 2144",
            "bowst_md_last_report_timestamp_seconds{venue=\"binance\"} 1700000075.500000000",
            "bowst_start_time_seconds 1700000000.000000000",
            "bowst_journal_state{state=\"ok\"} 1",
            "bowst_journal_state{state=\"failed\"} 0",
            "bowst_journal_dropped_records_total 0",
            "# TYPE bowst_md_apply_latency_seconds summary",
            "# TYPE bowst_md_messages_total counter",
        ] {
            assert!(
                text.lines().any(|l| l == line),
                "missing {line:?} in\n{text}"
            );
        }
    }

    #[test]
    fn every_line_is_valid_exposition_text() {
        let text = render(&state(), &instruments(), |_| true);
        let mut families = std::collections::BTreeSet::new();
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest.split(' ').next().unwrap();
                assert!(
                    families.insert(name.to_owned()),
                    "family {name} declared twice"
                );
                continue;
            }
            if line.starts_with("# HELP ") {
                continue;
            }
            // `name{labels} value` or `name value`, with a numeric value and a declared family.
            let (series, value) = line.rsplit_once(' ').unwrap();
            assert!(
                value.parse::<u64>().is_ok()
                    || value
                        .split_once('.')
                        .is_some_and(|(a, b)| a.parse::<u64>().is_ok() && b.len() == 9),
                "bad value in {line:?}"
            );
            let name = series.split('{').next().unwrap();
            let family = ["_sum", "_count"]
                .iter()
                .find_map(|suffix| {
                    name.strip_suffix(suffix)
                        .filter(|base| families.contains(*base))
                })
                .unwrap_or(name);
            assert!(families.contains(family), "sample {name} before its family");
        }
    }

    #[test]
    fn before_the_first_report_counters_are_zero_and_latency_is_absent() {
        let mut state = state();
        state.report = None;
        state.journal = None;
        let text = render(&state, &instruments(), |_| false);
        assert!(text.contains("bowst_md_messages_total{venue=\"binance\"} 0\n"));
        assert!(
            text.contains(
                "bowst_md_last_report_timestamp_seconds{venue=\"binance\"} 0.000000000\n"
            )
        );
        assert!(!text.contains("bowst_md_apply_latency_seconds"));
        assert!(!text.contains("bowst_journal"));
    }

    #[test]
    fn journal_state_follows_health() {
        let mut state = state();
        state.journal = Some(JournalHealth::Degraded { dropped: 7 });
        state.journal_dropped = 7;
        let text = render(&state, &instruments(), |_| true);
        assert!(text.contains("bowst_journal_state{state=\"degraded\"} 1\n"));
        assert!(text.contains("bowst_journal_state{state=\"ok\"} 0\n"));
        assert!(text.contains("bowst_journal_dropped_records_total 7\n"));
    }
}
