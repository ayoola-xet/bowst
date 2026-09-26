//! Writing metrics in the Prometheus text exposition format (version 0.0.4).
//!
//! Integer values only: counts are written as integers and durations as exact decimal
//! seconds derived from integer nanoseconds, so no floating point is involved.
//!
//! Metric and label names are checked when they are written. An invalid name is a
//! programming error: it is left out of the output and counted in
//! [`Exposition::rejected`], and tests assert that count is zero.

use core::fmt::Write as _;

use crate::LatencySummary;

/// The content type of the text this module writes.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Prometheus metric types used here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricType {
    /// Only ever increases (resets when the process restarts).
    Counter,
    /// Can go up and down.
    Gauge,
    /// Quantiles over a recent window, plus a cumulative sum and count.
    Summary,
}

impl MetricType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Summary => "summary",
        }
    }
}

/// A sample value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Value {
    /// A plain count or level.
    Int(u64),
    /// A duration in nanoseconds, written as seconds.
    Nanos(u64),
}

/// Builds one exposition text. See the module docs.
#[derive(Debug, Default)]
pub struct Exposition {
    out: String,
    rejected: usize,
}

impl Exposition {
    /// An empty exposition.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts a metric family: its `# HELP` and `# TYPE` lines. Write each family once, then
    /// its samples.
    pub fn family(&mut self, name: &str, kind: MetricType, help: &str) {
        if !valid_metric_name(name) {
            self.rejected = self.rejected.saturating_add(1);
            return;
        }
        // Writing to a String cannot fail.
        let _ = writeln!(self.out, "# HELP {name} {}", escape_help(help));
        let _ = writeln!(self.out, "# TYPE {name} {}", kind.as_str());
    }

    /// Writes one sample.
    pub fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: Value) {
        if !valid_metric_name(name) || !labels.iter().all(|(k, _)| valid_label_name(k)) {
            self.rejected = self.rejected.saturating_add(1);
            return;
        }
        self.out.push_str(name);
        if !labels.is_empty() {
            self.out.push('{');
            for (index, (key, value)) in labels.iter().enumerate() {
                if index > 0 {
                    self.out.push(',');
                }
                self.out.push_str(key);
                self.out.push_str("=\"");
                push_escaped_label_value(&mut self.out, value);
                self.out.push('"');
            }
            self.out.push('}');
        }
        self.out.push(' ');
        push_value(&mut self.out, value);
        self.out.push('\n');
    }

    /// Writes a summary's samples: quantiles from `window` (recent values), and `_sum` and
    /// `_count` from `total` (every value since start). Durations are in nanoseconds.
    pub fn summary(
        &mut self,
        name: &str,
        labels: &[(&str, &str)],
        window: &LatencySummary,
        total: &LatencySummary,
    ) {
        let quantiles = [
            ("0.5", window.p50),
            ("0.9", window.p90),
            ("0.99", window.p99),
            ("0.999", window.p999),
        ];
        let mut with_quantile: Vec<(&str, &str)> =
            Vec::with_capacity(labels.len().saturating_add(1));
        for (quantile, nanos) in quantiles {
            with_quantile.clear();
            with_quantile.extend_from_slice(labels);
            with_quantile.push(("quantile", quantile));
            self.sample(name, &with_quantile, Value::Nanos(nanos));
        }
        self.sample(&format!("{name}_sum"), labels, Value::Nanos(total.sum));
        self.sample(&format!("{name}_count"), labels, Value::Int(total.count));
    }

    /// Metrics or samples left out because a name was invalid. Always zero in correct code.
    #[must_use]
    pub fn rejected(&self) -> usize {
        self.rejected
    }

    /// The finished text.
    #[must_use]
    pub fn finish(self) -> String {
        self.out
    }
}

fn valid_metric_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_' || b == b':')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
}

fn valid_label_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !name.starts_with("__")
}

fn escape_help(help: &str) -> String {
    help.replace('\\', "\\\\").replace('\n', "\\n")
}

fn push_escaped_label_value(out: &mut String, value: &str) {
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

fn push_value(out: &mut String, value: Value) {
    match value {
        Value::Int(v) => {
            let _ = write!(out, "{v}");
        }
        Value::Nanos(nanos) => {
            let _ = write!(
                out,
                "{}.{:09}",
                nanos / 1_000_000_000,
                nanos % 1_000_000_000
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_families_and_samples() {
        let mut e = Exposition::new();
        e.family(
            "bowst_md_messages_total",
            MetricType::Counter,
            "Messages received.",
        );
        e.sample("bowst_md_messages_total", &[], Value::Int(42));
        e.family(
            "bowst_md_instrument_live",
            MetricType::Gauge,
            "1 while the book is live.",
        );
        e.sample(
            "bowst_md_instrument_live",
            &[("venue", "binance"), ("symbol", "BTCUSDT")],
            Value::Int(1),
        );
        assert_eq!(e.rejected(), 0);
        assert_eq!(
            e.finish(),
            "# HELP bowst_md_messages_total Messages received.\n\
             # TYPE bowst_md_messages_total counter\n\
             bowst_md_messages_total 42\n\
             # HELP bowst_md_instrument_live 1 while the book is live.\n\
             # TYPE bowst_md_instrument_live gauge\n\
             bowst_md_instrument_live{venue=\"binance\",symbol=\"BTCUSDT\"} 1\n"
        );
    }

    #[test]
    fn durations_are_exact_seconds() {
        let mut e = Exposition::new();
        for nanos in [0, 1, 1_234, 999_999_999, 1_000_000_000, 61_500_000_000] {
            e.sample("t", &[], Value::Nanos(nanos));
        }
        assert_eq!(
            e.finish(),
            "t 0.000000000\nt 0.000000001\nt 0.000001234\nt 0.999999999\nt 1.000000000\nt 61.500000000\n"
        );
    }

    #[test]
    fn summaries_use_window_quantiles_and_total_sum_and_count() {
        let window = LatencySummary {
            count: 3,
            sum: 6_000,
            min: 1_000,
            p50: 2_000,
            p90: 3_000,
            p99: 3_000,
            p999: 3_000,
            max: 3_000,
        };
        let total = LatencySummary {
            count: 10,
            sum: 25_000,
            ..window
        };
        let mut e = Exposition::new();
        e.summary("lat_seconds", &[("venue", "binance")], &window, &total);
        assert_eq!(
            e.finish(),
            "lat_seconds{venue=\"binance\",quantile=\"0.5\"} 0.000002000\n\
             lat_seconds{venue=\"binance\",quantile=\"0.9\"} 0.000003000\n\
             lat_seconds{venue=\"binance\",quantile=\"0.99\"} 0.000003000\n\
             lat_seconds{venue=\"binance\",quantile=\"0.999\"} 0.000003000\n\
             lat_seconds_sum{venue=\"binance\"} 0.000025000\n\
             lat_seconds_count{venue=\"binance\"} 10\n"
        );
    }

    #[test]
    fn label_values_and_help_are_escaped() {
        let mut e = Exposition::new();
        e.family("m", MetricType::Gauge, "line one\nback\\slash");
        e.sample("m", &[("reason", "a \"quoted\"\nvalue\\")], Value::Int(1));
        assert_eq!(
            e.finish(),
            "# HELP m line one\\nback\\\\slash\n# TYPE m gauge\nm{reason=\"a \\\"quoted\\\"\\nvalue\\\\\"} 1\n"
        );
    }

    #[test]
    fn invalid_names_are_left_out_and_counted() {
        let mut e = Exposition::new();
        e.family("9starts_with_digit", MetricType::Gauge, "x");
        e.sample("has-dash", &[], Value::Int(1));
        e.sample("ok", &[("__reserved", "x")], Value::Int(1));
        e.sample("ok", &[("bad-label", "x")], Value::Int(1));
        e.sample("ok", &[("fine_label", "x")], Value::Int(1));
        assert_eq!(e.rejected(), 4);
        assert_eq!(e.finish(), "ok{fine_label=\"x\"} 1\n");
    }
}
