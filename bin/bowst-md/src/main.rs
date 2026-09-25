//! `bowst-md`: streams live Binance Spot order books and prints their health once a second.
//!
//! Used to validate market data end to end (Phase 1 soak test) and to watch a venue during
//! operations. It runs the same session code as the engine.
//!
//! ```text
//! bowst-md [--symbols BTCUSDT,ETHUSDT] [--seconds 60] [--stream URL] [--rest URL]
//! ```
//!
//! Defaults point at Binance's public market-data endpoints (`data-stream.binance.vision`,
//! `data-api.binance.vision`), which serve public data only. The exit code is non-zero if any
//! instrument never went live.

// Operator CLI: printing to the terminal is its purpose.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bowst_book::Book;
use bowst_core::{InstrumentId, InstrumentTable, Price, Qty, Symbol, WallTime};
use bowst_venue::binance::md::{MdConfig, MdHandler, MdSession, MdStatus};
use bowst_venue::binance::rest::load_instruments;
use bowst_venue::net::TlsConfig;

const DEFAULT_STREAM: &str = "wss://data-stream.binance.vision";
const DEFAULT_REST: &str = "https://data-api.binance.vision";

struct Args {
    symbols: Vec<Symbol>,
    seconds: u64,
    stream: String,
    rest: String,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        symbols: vec![Symbol::new("BTCUSDT").ok_or("bad default")?],
        seconds: 30,
        stream: DEFAULT_STREAM.into(),
        rest: DEFAULT_REST.into(),
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
            "-h" | "--help" => {
                return Err(
                    "usage: bowst-md [--symbols A,B] [--seconds N] [--stream URL] [--rest URL]"
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
                }
                println!("[status] disconnected: {reason}");
            }
            MdStatus::Connecting => println!("[status] connecting"),
            MdStatus::Connected => println!("[status] connected"),
        }
    }
}

fn print_board(board: &Board, instruments: &InstrumentTable, previous: &mut BTreeMap<u32, u64>) {
    for instrument in instruments.iter() {
        let id = instrument.id.get();
        let top = board.tops.get(&id).copied().unwrap_or_default();
        let rate = top
            .updates
            .saturating_sub(previous.insert(id, top.updates).unwrap_or(0));
        let live = board.live.get(&id).copied().unwrap_or(false);
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
        println!(
            "{:<10} {:<5} bid {:<28} ask {:<28} spread {spread:>4} ticks  depth {}/{}  {rate:>3} upd/s",
            instrument.symbol.as_str(),
            if live { "LIVE" } else { "down" },
            side(top.bid),
            side(top.ask),
            top.depth.0,
            top.depth.1,
        );
    }
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    let tls = match TlsConfig::from_platform_roots() {
        Ok(tls) => tls,
        Err(e) => {
            eprintln!("TLS setup failed: {e}");
            return ExitCode::FAILURE;
        }
    };
    let instruments =
        match load_instruments(&args.rest, &args.symbols, &tls, Duration::from_secs(10)) {
            Ok(instruments) => instruments,
            Err(e) => {
                eprintln!("could not load instruments: {e}");
                return ExitCode::FAILURE;
            }
        };
    for i in instruments.iter() {
        println!(
            "{}: tick {} lot {} min notional {}",
            i.symbol, i.tick, i.lot, i.min_notional
        );
    }

    let board = Arc::new(Mutex::new(Board::default()));
    let recorder = Recorder {
        board: Arc::clone(&board),
        symbols: args.symbols.clone(),
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
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let md = thread::Builder::new().name("md".into()).spawn(move || {
        session.run(&flag);
        session.stats()
    });
    let Ok(md) = md else {
        eprintln!("could not start the market-data thread");
        return ExitCode::FAILURE;
    };

    let started = Instant::now();
    let mut previous = BTreeMap::new();
    let mut ever_live = BTreeMap::new();
    while started.elapsed() < Duration::from_secs(args.seconds) {
        thread::sleep(Duration::from_secs(1));
        if let Ok(board) = board.lock() {
            println!("--- {:>4}s", started.elapsed().as_secs());
            print_board(&board, &instruments, &mut previous);
            for (id, live) in &board.live {
                if *live {
                    ever_live.insert(*id, true);
                }
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    let Ok(stats) = md.join() else {
        eprintln!("market-data thread panicked");
        return ExitCode::FAILURE;
    };
    println!(
        "summary: {} messages, {} deltas applied, {} book invalidations, {} connections, {}/{} snapshots applied/requested",
        stats.messages,
        stats.deltas_applied,
        stats.book_invalidations,
        stats.connections,
        stats.snapshots_applied,
        stats.snapshots_requested
    );
    if ever_live.len() == instruments.iter().len() {
        ExitCode::SUCCESS
    } else {
        eprintln!("some instruments never went live");
        ExitCode::FAILURE
    }
}
