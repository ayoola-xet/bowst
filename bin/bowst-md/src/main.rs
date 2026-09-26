//! `bowst-md`: streams live Binance Spot order books and prints their health once a second.
//!
//! Used to validate market data end to end (Phase 1 soak test) and to watch a venue during
//! operations. It runs the same session code as the engine.
//!
//! ```text
//! bowst-md [--symbols BTCUSDT,ETHUSDT] [--seconds 60] [--stream URL] [--rest URL] [--journal DIR]
//!          [--metrics ADDR [--metrics-allow-remote]]
//! bowst-md --replay DIR
//! ```
//!
//! `--metrics ADDR` serves Prometheus metrics at `http://ADDR/metrics` (for example
//! `127.0.0.1:9184`). Only loopback addresses are accepted unless `--metrics-allow-remote` is
//! given: the endpoint has no authentication (see `docs/deploy/monitoring.md`).
//!
//! `--journal DIR` records the session (every raw message, applied snapshot and reset) for
//! exact replay. `--replay DIR` replays a recorded journal through the same book logic and
//! prints the final books and a report.
//!
//! Every 10 seconds it prints a `[stats]` line: messages, decode-and-apply latency percentiles,
//! and the results of verifying books against fresh snapshots (one instrument per minute).
//!
//! Defaults point at Binance's public market-data endpoints (`data-stream.binance.vision`,
//! `data-api.binance.vision`), which serve public data only. The exit code is non-zero if any
//! instrument never went live, or if any book failed verification.

// Operator CLI: printing to the terminal is its purpose.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod metrics;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bowst_book::Book;
use bowst_core::{
    Clock, Instrument, InstrumentId, InstrumentTable, Price, Qty, Symbol, SystemClock, WallTime,
};
use bowst_journal::format::Kind;
use bowst_journal::{JournalHandle, JournalHealth, JournalReader};
use bowst_telemetry::LatencySummary;
use bowst_telemetry::server::{MetricsConfig, MetricsError, MetricsServer};
use bowst_venue::binance::md::{MdConfig, MdHandler, MdReport, MdSession, MdStats, MdStatus};
use bowst_venue::binance::replay::parse_description;
use bowst_venue::binance::rest::load_instruments;
use bowst_venue::net::TlsConfig;
use metrics::MetricsState;

const DEFAULT_STREAM: &str = "wss://data-stream.binance.vision";
const DEFAULT_REST: &str = "https://data-api.binance.vision";

struct Args {
    symbols: Vec<Symbol>,
    seconds: u64,
    stream: String,
    rest: String,
    journal: Option<PathBuf>,
    replay: Option<PathBuf>,
    metrics: Option<SocketAddr>,
    metrics_allow_remote: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        symbols: vec![Symbol::new("BTCUSDT").ok_or("bad default")?],
        seconds: 30,
        stream: DEFAULT_STREAM.into(),
        rest: DEFAULT_REST.into(),
        journal: None,
        replay: None,
        metrics: None,
        metrics_allow_remote: false,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = || iter.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--symbols" => {
                args.symbols = value()?
                    .split(',')
                    .map(|s| {
                        Symbol::new(&s.trim().to_ascii_uppercase())
                            .ok_or(format!("bad symbol {s:?}"))
                    })
                    .collect::<Result<_, _>>()?;
            }
            "--seconds" => {
                args.seconds = value()?.parse().map_err(|_| "--seconds must be a number")?;
            }
            "--stream" => args.stream = value()?,
            "--rest" => args.rest = value()?,
            "--journal" => args.journal = Some(value()?.into()),
            "--replay" => args.replay = Some(value()?.into()),
            "--metrics" => {
                args.metrics = Some(
                    value()?
                        .parse()
                        .map_err(|_| "--metrics needs an address such as 127.0.0.1:9184")?,
                );
            }
            "--metrics-allow-remote" => args.metrics_allow_remote = true,
            "-h" | "--help" => {
                return Err(
                    "usage: bowst-md [--symbols A,B] [--seconds N] [--stream URL] [--rest URL] [--journal DIR]\n                [--metrics ADDR [--metrics-allow-remote]]\n       bowst-md --replay DIR"
                        .into(),
                );
            }
            other => return Err(format!("unknown argument {other:?} (see --help)")),
        }
    }
    Ok(args)
}

/// Latest top of book per instrument, shared with the printing thread.
#[derive(Default)]
struct Board {
    tops: BTreeMap<u32, Top>,
    live: BTreeMap<u32, bool>,
    metrics: MetricsState,
}

#[derive(Clone, Copy, Default)]
struct Top {
    bid: Option<(Price, Qty)>,
    ask: Option<(Price, Qty)>,
    depth: (usize, usize),
    updates: u64,
    event_time: WallTime,
}

/// Records the top of each book. Tool-only: takes a lock per update, which the engine's
/// hot path never does.
struct Recorder {
    board: Arc<Mutex<Board>>,
    symbols: Vec<Symbol>,
    /// Messages counted at the previous report.
    reported_messages: u64,
}

impl MdHandler for Recorder {
    fn on_book(&mut self, instrument: InstrumentId, book: &Book, event_time: WallTime) {
        if let Ok(mut board) = self.board.lock() {
            let top = board.tops.entry(instrument.get()).or_default();
            top.bid = book.best_bid().map(|l| (l.price, l.qty));
            top.ask = book.best_ask().map(|l| (l.price, l.qty));
            top.depth = (
                book.side(bowst_core::Side::Buy).len(),
                book.side(bowst_core::Side::Sell).len(),
            );
            top.updates = top.updates.saturating_add(1);
            top.event_time = event_time;
        }
    }

    fn on_status(&mut self, status: MdStatus) {
        let name = |id: InstrumentId| {
            self.symbols
                .get(usize::try_from(id.get()).unwrap_or(usize::MAX))
                .map_or_else(|| format!("#{}", id.get()), ToString::to_string)
        };
        match &status {
            MdStatus::InstrumentLive(id) => {
                if let Ok(mut board) = self.board.lock() {
                    board.live.insert(id.get(), true);
                }
                println!("[status] {} live", name(*id));
            }
            MdStatus::InstrumentDown { instrument, reason } => {
                if let Ok(mut board) = self.board.lock() {
                    board.live.insert(instrument.get(), false);
                }
                println!("[status] {} DOWN: {reason}", name(*instrument));
            }
            MdStatus::SnapshotFailed { instrument, reason } => {
                println!("[status] {} snapshot failed: {reason}", name(*instrument));
            }
            MdStatus::Disconnected { reason } => {
                if let Ok(mut board) = self.board.lock() {
                    board.live.values_mut().for_each(|live| *live = false);
                    board.metrics.connected = false;
                }
                println!("[status] disconnected: {reason}");
            }
            MdStatus::Connecting => println!("[status] connecting"),
            MdStatus::Connected => {
                if let Ok(mut board) = self.board.lock() {
                    board.metrics.connected = true;
                }
                println!("[status] connected");
            }
        }
    }

    fn on_report(&mut self, report: &MdReport) {
        if let Ok(mut board) = self.board.lock() {
            board.metrics.report = Some((*report, SystemClock::new().wall()));
        }
        let messages = report.stats.messages.saturating_sub(self.reported_messages);
        self.reported_messages = report.stats.messages;
        println!(
            "[stats] {}s: {messages} msgs, decode+apply {}; verified {} ok, {} mismatched, {} abandoned",
            report.interval.as_secs(),
            latency_text(&report.latency),
            report.stats.verifications_passed,
            report.stats.verification_mismatches,
            report.stats.verifications_abandoned,
        );
    }
}

/// One board line: best bid and ask in decimal units, spread, depth and the given rate text.
fn board_line(instrument: &Instrument, top: &Top, live: bool, rate: &str) -> String {
    let side = |level: Option<(Price, Qty)>| {
        level.map_or_else(
            || "-".to_owned(),
            |(p, q)| {
                format!(
                    "{} @ {}",
                    q.to_dec(instrument.lot),
                    p.to_dec(instrument.tick)
                )
            },
        )
    };
    let spread = match (top.bid, top.ask) {
        (Some((bid, _)), Some((ask, _))) => ask.checked_sub(bid).map_or(0, Price::get),
        _ => 0,
    };
    format!(
        "{:<10} {:<5} bid {:<28} ask {:<28} spread {spread:>4} ticks  depth {}/{}  {rate}",
        instrument.symbol.as_str(),
        if live { "LIVE" } else { "down" },
        side(top.bid),
        side(top.ask),
        top.depth.0,
        top.depth.1,
    )
}

fn print_board(board: &Board, instruments: &InstrumentTable, previous: &mut BTreeMap<u32, u64>) {
    for instrument in instruments.iter() {
        let id = instrument.id.get();
        let top = board.tops.get(&id).copied().unwrap_or_default();
        let rate = top
            .updates
            .saturating_sub(previous.insert(id, top.updates).unwrap_or(0));
        let live = board.live.get(&id).copied().unwrap_or(false);
        println!(
            "{}",
            board_line(instrument, &top, live, &format!("{rate:>3} upd/s"))
        );
    }
}

/// The instruments of the journal's sessions, from their session-start records. `None` if the
/// journal holds no session. Every session must describe the same instruments, so one set of
/// labels is right for the whole replay.
fn journal_instruments(dir: &Path) -> Result<Option<InstrumentTable>, String> {
    let mut reader = JournalReader::open(dir).map_err(|e| e.to_string())?;
    let mut found: Option<InstrumentTable> = None;
    while let Some(record) = reader.next_record().map_err(|e| e.to_string())? {
        if record.header.kind != Kind::SESSION_START {
            continue;
        }
        let text = String::from_utf8_lossy(record.payload);
        let (_, table) = parse_description(&text).map_err(|e| e.to_string())?;
        match &found {
            Some(first) if !same_instruments(first, &table) => {
                return Err(
                    "its sessions describe different instruments; replay each run's journal separately"
                        .into(),
                );
            }
            Some(_) => {}
            None => found = Some(table),
        }
    }
    Ok(found)
}

fn same_instruments(a: &InstrumentTable, b: &InstrumentTable) -> bool {
    a.iter().len() == b.iter().len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
}

/// Replays a journal and prints the final books.
fn replay_journal(dir: &Path) -> ExitCode {
    let instruments = match journal_instruments(dir) {
        Ok(Some(instruments)) => instruments,
        Ok(None) => {
            eprintln!("the journal holds no session");
            return ExitCode::FAILURE;
        }
        Err(e) => {
            eprintln!("cannot replay journal: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut reader = match JournalReader::open(dir) {
        Ok(reader) => reader,
        Err(e) => {
            eprintln!("cannot open journal: {e}");
            return ExitCode::FAILURE;
        }
    };
    let board = Arc::new(Mutex::new(Board::default()));
    let mut recorder = Recorder {
        board: Arc::clone(&board),
        symbols: instruments.iter().map(|i| i.symbol).collect(),
        reported_messages: 0,
    };
    let report = match bowst_venue::binance::replay::replay(&mut reader, &mut recorder) {
        Ok(report) => report,
        Err(e) => {
            eprintln!("replay failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "replayed {} session(s): {} messages, {} snapshots, {} verification snapshots, {} resets, {} records missing{}",
        report.sessions,
        report.messages,
        report.snapshots,
        report.verifications,
        report.resets,
        report.dropped,
        if report.torn_tail {
            ", torn final record (unclean shutdown)"
        } else {
            ""
        }
    );
    if let Ok(board) = board.lock() {
        for instrument in instruments.iter() {
            let id = instrument.id.get();
            let top = board.tops.get(&id).copied().unwrap_or_default();
            let live = board.live.get(&id).copied().unwrap_or(false);
            println!(
                "{}",
                board_line(instrument, &top, live, &format!("{} updates", top.updates))
            );
        }
    }
    if report.dropped > 0 {
        eprintln!("journal is incomplete: replay is not exact after the first gap");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

/// Prints the board once a second for `duration`. Returns the instruments that were ever live.
fn watch(
    board: &Mutex<Board>,
    instruments: &InstrumentTable,
    duration: Duration,
    journal: Option<&JournalHandle>,
) -> BTreeMap<u32, bool> {
    let started = Instant::now();
    let mut previous = BTreeMap::new();
    let mut ever_live = BTreeMap::new();
    while started.elapsed() < duration {
        thread::sleep(Duration::from_secs(1));
        if let Ok(mut board) = board.lock() {
            if let Some(journal) = journal {
                let health = journal.health();
                board.metrics.journal = Some(health);
                if let JournalHealth::Degraded { dropped } = health {
                    board.metrics.journal_dropped = dropped;
                }
            }
            println!("--- {:>4}s", started.elapsed().as_secs());
            print_board(&board, instruments, &mut previous);
            for (id, live) in &board.live {
                if *live {
                    ever_live.insert(*id, true);
                }
            }
        }
    }
    ever_live
}

/// Nanoseconds as microseconds with two decimals, without floating point.
fn micros(nanos: u64) -> String {
    format!("{}.{:02} µs", nanos / 1_000, nanos % 1_000 / 10)
}

fn latency_text(latency: &LatencySummary) -> String {
    if latency.count == 0 {
        return "no samples".into();
    }
    format!(
        "p50 {} p99 {} p99.9 {} max {}",
        micros(latency.p50),
        micros(latency.p99),
        micros(latency.p999),
        micros(latency.max)
    )
}

fn print_summary(stats: &MdStats, latency: &LatencySummary) {
    println!(
        "summary: {} messages, {} deltas applied, {} book invalidations, {} connections, {}/{} snapshots applied/requested",
        stats.messages,
        stats.deltas_applied,
        stats.book_invalidations,
        stats.connections,
        stats.snapshots_applied,
        stats.snapshots_requested
    );
    println!(
        "latency: decode+apply over {} messages: {}",
        latency.count,
        latency_text(latency)
    );
    println!(
        "verification: {} passed, {} mismatched, {} abandoned",
        stats.verifications_passed, stats.verification_mismatches, stats.verifications_abandoned
    );
}

/// Serves Prometheus metrics when `--metrics` was given. The server stops when the returned
/// value is dropped.
fn start_metrics(
    args: &Args,
    board: &Arc<Mutex<Board>>,
    instruments: &InstrumentTable,
) -> Result<Option<MetricsServer>, MetricsError> {
    let Some(addr) = args.metrics else {
        return Ok(None);
    };
    let mut config = MetricsConfig::new(addr);
    config.allow_remote = args.metrics_allow_remote;
    let (board, instruments) = (Arc::clone(board), instruments.clone());
    let server = MetricsServer::start(config, move || {
        board.lock().map_or_else(
            |_| String::new(),
            |board| {
                metrics::render(&board.metrics, &instruments, |id| {
                    board.live.get(&id).copied().unwrap_or(false)
                })
            },
        )
    })?;
    println!("serving metrics at http://{}/metrics", server.addr());
    Ok(Some(server))
}

/// Flushes and closes the journal, printing its statistics. `false` if it failed.
fn finish_journal(handle: JournalHandle) -> bool {
    println!("journal health: {:?}", handle.health());
    match handle.finish() {
        Ok(stats) => {
            println!(
                "journal: {} records, {} bytes, {} segment(s), {} dropped",
                stats.records, stats.bytes, stats.segments, stats.dropped
            );
            true
        }
        Err(e) => {
            eprintln!("journal failed: {e}");
            false
        }
    }
}

/// TLS roots and the instruments' rules from the venue. Prints what went wrong on failure.
fn connect_setup(args: &Args) -> Option<(TlsConfig, InstrumentTable)> {
    let tls = match TlsConfig::from_platform_roots() {
        Ok(tls) => tls,
        Err(e) => {
            eprintln!("TLS setup failed: {e}");
            return None;
        }
    };
    let instruments =
        match load_instruments(&args.rest, &args.symbols, &tls, Duration::from_secs(10)) {
            Ok(instruments) => instruments,
            Err(e) => {
                eprintln!("could not load instruments: {e}");
                return None;
            }
        };
    for i in instruments.iter() {
        println!(
            "{}: tick {} lot {} min notional {}",
            i.symbol, i.tick, i.lot, i.min_notional
        );
    }

    Some((tls, instruments))
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    if let Some(dir) = &args.replay {
        return replay_journal(dir);
    }
    let Some((tls, instruments)) = connect_setup(&args) else {
        return ExitCode::FAILURE;
    };

    let board = Arc::new(Mutex::new(Board::default()));
    if let Ok(mut board) = board.lock() {
        board.metrics.started = SystemClock::new().wall();
    }
    let _metrics = match start_metrics(&args, &board, &instruments) {
        Ok(server) => server,
        Err(e) => {
            eprintln!("could not serve metrics: {e}");
            return ExitCode::FAILURE;
        }
    };
    let recorder = Recorder {
        board: Arc::clone(&board),
        symbols: args.symbols.clone(),
        reported_messages: 0,
    };
    let mut config = MdConfig::new(&args.stream, &args.rest);
    // A shared machine: sleep briefly when idle instead of spinning a core.
    config.idle_sleep = Some(Duration::from_micros(100));
    let mut session = match MdSession::new(config, instruments.clone(), tls, recorder) {
        Ok(session) => session,
        Err(e) => {
            eprintln!("could not start session: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut journal = None;
    if let Some(dir) = &args.journal {
        match bowst_journal::start(bowst_journal::JournalConfig::new(dir)) {
            Ok((producer, handle)) => {
                session = session.with_journal(producer);
                journal = Some(handle);
                println!("journaling to {}", dir.display());
            }
            Err(e) => {
                eprintln!("could not start journal: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let md = thread::Builder::new().name("md".into()).spawn(move || {
        session.run(&flag);
        (session.stats(), session.latency())
    });
    let Ok(md) = md else {
        eprintln!("could not start the market-data thread");
        return ExitCode::FAILURE;
    };

    let ever_live = watch(
        &board,
        &instruments,
        Duration::from_secs(args.seconds),
        journal.as_ref(),
    );
    stop.store(true, Ordering::Relaxed);
    let Ok((stats, latency)) = md.join() else {
        eprintln!("market-data thread panicked");
        return ExitCode::FAILURE;
    };
    if let Some(handle) = journal
        && !finish_journal(handle)
    {
        return ExitCode::FAILURE;
    }
    print_summary(&stats, &latency);
    if stats.verification_mismatches > 0 {
        eprintln!(
            "a book differed from a fresh venue snapshot: investigate before trusting this build"
        );
        return ExitCode::FAILURE;
    }
    if ever_live.len() == instruments.iter().len() {
        ExitCode::SUCCESS
    } else {
        eprintln!("some instruments never went live");
        ExitCode::FAILURE
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use bowst_book::SyncConfig;
    use bowst_core::{Dec, Increment, MonoTime, VenueId};
    use bowst_journal::format::RecordHeader;
    use bowst_venue::binance::replay::{BookSizing, describe};

    fn instrument(id: u32, symbol: &str) -> Instrument {
        Instrument {
            id: InstrumentId::new(id),
            venue: VenueId::Binance,
            symbol: Symbol::new(symbol).unwrap(),
            tick: Increment::parse("0.01").unwrap(),
            lot: Increment::parse("0.00001").unwrap(),
            min_notional: Dec::parse("5").unwrap(),
        }
    }

    fn table(symbols: &[&str]) -> InstrumentTable {
        InstrumentTable::new(
            symbols
                .iter()
                .enumerate()
                .map(|(i, s)| instrument(u32::try_from(i).unwrap(), s))
                .collect(),
        )
        .unwrap()
    }

    /// Writes one session-start record per table into a fresh journal directory.
    fn journal(name: &str, sessions: &[InstrumentTable]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("bowst-md-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let sizing = BookSizing {
            sync: SyncConfig {
                levels_per_side: 10,
                buffered_messages: 10,
                buffered_updates: 10,
            },
            max_levels_per_message: 10,
            snapshot_limit: 10,
        };
        for session in sessions {
            let (mut producer, handle) =
                bowst_journal::start(bowst_journal::JournalConfig::new(&dir)).unwrap();
            let header = RecordHeader {
                kind: Kind::SESSION_START,
                source: 1,
                mono: MonoTime::from_nanos(1),
                wall: WallTime::from_nanos(1),
            };
            assert!(producer.append(header, &[describe(&sizing, session).as_bytes()]));
            drop(producer);
            handle.finish().unwrap();
        }
        dir
    }

    #[test]
    fn board_line_shows_decimal_prices_and_quantities() {
        let top = Top {
            bid: Some((Price::new(8_475_800), Qty::new(438_837))),
            ask: Some((Price::new(8_475_801), Qty::new(13_276))),
            depth: (1_185, 943),
            updates: 159,
            event_time: WallTime::from_nanos(0),
        };
        let line = board_line(&instrument(0, "BTCUSDT"), &top, true, "159 updates");
        assert!(
            line.starts_with("BTCUSDT    LIVE  bid 4.38837 @ 84758.00"),
            "{line}"
        );
        assert!(line.contains("ask 0.13276 @ 84758.01"), "{line}");
        assert!(
            line.contains("spread    1 ticks  depth 1185/943  159 updates"),
            "{line}"
        );
    }

    #[test]
    fn latency_is_shown_in_microseconds_without_floats() {
        assert_eq!(micros(0), "0.00 µs");
        assert_eq!(micros(1_234), "1.23 µs");
        assert_eq!(micros(12_005), "12.00 µs");
        assert_eq!(latency_text(&LatencySummary::default()), "no samples");
        let summary = LatencySummary {
            count: 3,
            sum: 76_900,
            min: 900,
            p50: 1_000,
            p90: 2_000,
            p99: 4_990,
            p999: 5_000,
            max: 70_000,
        };
        assert_eq!(
            latency_text(&summary),
            "p50 1.00 µs p99 4.99 µs p99.9 5.00 µs max 70.00 µs"
        );
    }

    #[test]
    fn replay_labels_come_from_the_journal() {
        let dir = journal(
            "labels",
            &[
                table(&["BTCUSDT", "ETHUSDT"]),
                table(&["BTCUSDT", "ETHUSDT"]),
            ],
        );
        let found = journal_instruments(&dir).unwrap().unwrap();
        let symbols: Vec<_> = found.iter().map(|i| i.symbol.as_str().to_owned()).collect();
        assert_eq!(symbols, ["BTCUSDT", "ETHUSDT"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_journal_mixing_instrument_sets_is_rejected() {
        let dir = journal("mixed", &[table(&["BTCUSDT"]), table(&["SOLUSDT"])]);
        assert!(journal_instruments(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_empty_journal_has_no_instruments() {
        let dir = journal("empty", &[]);
        std::fs::create_dir_all(&dir).unwrap();
        assert!(journal_instruments(&dir).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
