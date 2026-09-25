//! End-to-end tests of the Binance market-data session against a mock venue built from the
//! recorded fixtures (no network).
//!
//! The mock venue holds the true order book for each symbol. Its WebSocket server streams the
//! recorded diff-depth messages, applying each one to the true book as it is sent. Its REST
//! server answers depth requests with the true book *at the moment of the request*, exactly like
//! the real venue. Faults can be injected: a dropped message, a disconnect, or silence.
//!
//! Every scenario must end with the session's books identical to the venue's true books.

// Test helpers fail fast on broken setup.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bowst_book::Book;
use bowst_core::{Dec, Instrument, InstrumentId, InstrumentTable, Side, Symbol, VenueId, WallTime};
use bowst_venue::binance::exchange_info::decode_exchange_info;
use bowst_venue::binance::md::{MdConfig, MdHandler, MdSession, MdStatus};
use bowst_venue::json::Reader;
use bowst_venue::net::TlsConfig;
use bowst_venue::ws::handshake::accept_for_key;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/binance");
const SYMBOLS: [&str; 2] = ["BTCUSDT", "DOGEUSDT"];
const DEADLINE: Duration = Duration::from_secs(30);
/// Levels per side compared between session and venue (the session's snapshot is 1,000 deep).
const COMPARE_DEPTH: usize = 200;

// ---------------------------------------------------------------------------------------------
// Fixture parsing (with the crate's own JSON reader)
// ---------------------------------------------------------------------------------------------

type Levels = Vec<(String, String)>;

struct Frame {
    raw: Vec<u8>,
    symbol: String,
    last: u64,
    bids: Levels,
    asks: Levels,
}

fn read_levels(r: &mut Reader<'_>) -> Levels {
    let mut out = Vec::new();
    r.begin_array().unwrap();
    while r.next_element().unwrap() {
        r.begin_array().unwrap();
        assert!(r.next_element().unwrap());
        let price = r.str().unwrap().to_owned();
        assert!(r.next_element().unwrap());
        let qty = r.str().unwrap().to_owned();
        assert!(!r.next_element().unwrap());
        out.push((price, qty));
    }
    out
}

fn parse_frame(raw: &[u8]) -> Frame {
    let mut r = Reader::new(raw);
    let mut frame = Frame {
        raw: raw.to_vec(),
        symbol: String::new(),
        last: 0,
        bids: vec![],
        asks: vec![],
    };
    r.begin_object().unwrap();
    while let Some(key) = r.next_key().unwrap() {
        if key != "data" {
            r.skip().unwrap();
            continue;
        }
        r.begin_object().unwrap();
        while let Some(key) = r.next_key().unwrap() {
            match key {
                "s" => r.str().unwrap().clone_into(&mut frame.symbol),
                "u" => frame.last = r.u64().unwrap(),
                "b" => frame.bids = read_levels(&mut r),
                "a" => frame.asks = read_levels(&mut r),
                _ => r.skip().unwrap(),
            }
        }
    }
    frame
}

fn frames() -> Vec<Frame> {
    std::fs::read(format!("{FIXTURES}/depth_stream.jsonl"))
        .unwrap()
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .map(parse_frame)
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Mock venue
// ---------------------------------------------------------------------------------------------

/// The venue's true book for one symbol.
#[derive(Default)]
struct TrueBook {
    last_update_id: u64,
    bids: BTreeMap<Dec, (String, String)>,
    asks: BTreeMap<Dec, (String, String)>,
}

impl TrueBook {
    fn load(symbol: &str) -> Self {
        let raw = std::fs::read(format!(
            "{FIXTURES}/depth_snapshot_{}.json",
            symbol.to_lowercase()
        ))
        .unwrap();
        let mut book = Self::default();
        let mut r = Reader::new(&raw);
        r.begin_object().unwrap();
        while let Some(key) = r.next_key().unwrap() {
            match key {
                "lastUpdateId" => book.last_update_id = r.u64().unwrap(),
                "bids" => book.apply(Side::Buy, &read_levels(&mut r)),
                "asks" => book.apply(Side::Sell, &read_levels(&mut r)),
                _ => r.skip().unwrap(),
            }
        }
        book
    }

    fn apply(&mut self, side: Side, levels: &Levels) {
        let map = if side == Side::Buy {
            &mut self.bids
        } else {
            &mut self.asks
        };
        for (price, qty) in levels {
            let key = Dec::parse(price).unwrap();
            if Dec::parse(qty).unwrap().is_zero() {
                map.remove(&key);
            } else {
                map.insert(key, (price.clone(), qty.clone()));
            }
        }
    }

    /// Best-first levels, as (price, qty) decimals.
    fn best(&self, side: Side, depth: usize) -> Vec<(Dec, Dec)> {
        let pair = |(p, q): &(String, String)| (Dec::parse(p).unwrap(), Dec::parse(q).unwrap());
        match side {
            Side::Buy => self.bids.values().rev().take(depth).map(pair).collect(),
            Side::Sell => self.asks.values().take(depth).map(pair).collect(),
        }
    }

    fn snapshot_json(&self, limit: usize) -> String {
        let side = |levels: Vec<&(String, String)>| {
            levels
                .iter()
                .map(|(p, q)| format!(r#"["{p}","{q}"]"#))
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            r#"{{"lastUpdateId":{},"bids":[{}],"asks":[{}]}}"#,
            self.last_update_id,
            side(self.bids.values().rev().take(limit).collect()),
            side(self.asks.values().take(limit).collect()),
        )
    }
}

#[derive(Clone, Copy, Default)]
struct Faults {
    /// Index of a frame that is applied to the true book but never sent (lost in transit).
    drop_frame: Option<usize>,
    /// Close the connection after sending this frame index (the next connection resumes).
    disconnect_after: Option<usize>,
    /// Go silent (no data, no pings) after this frame index, on the first connection only.
    silence_after: Option<usize>,
}

struct Venue {
    books: Mutex<BTreeMap<String, TrueBook>>,
    frames: Vec<Frame>,
    cursor: AtomicUsize,
    connections: AtomicUsize,
    snapshots_served: AtomicUsize,
    faults: Faults,
    shutdown: AtomicBool,
}

impl Venue {
    fn start(faults: Faults) -> (Arc<Self>, String, String) {
        let books = SYMBOLS
            .iter()
            .map(|s| ((*s).to_owned(), TrueBook::load(s)))
            .collect();
        let venue = Arc::new(Self {
            books: Mutex::new(books),
            frames: frames(),
            cursor: AtomicUsize::new(0),
            connections: AtomicUsize::new(0),
            snapshots_served: AtomicUsize::new(0),
            faults,
            shutdown: AtomicBool::new(false),
        });
        let ws = TcpListener::bind("127.0.0.1:0").unwrap();
        let rest = TcpListener::bind("127.0.0.1:0").unwrap();
        let urls = (
            format!("ws://127.0.0.1:{}", ws.local_addr().unwrap().port()),
            format!("http://127.0.0.1:{}", rest.local_addr().unwrap().port()),
        );
        let v = Arc::clone(&venue);
        thread::spawn(move || v.serve(&ws, Self::stream));
        let v = Arc::clone(&venue);
        thread::spawn(move || v.serve(&rest, Self::rest));
        (venue, urls.0, urls.1)
    }

    fn serve(self: Arc<Self>, listener: &TcpListener, handle: fn(&Self, TcpStream)) {
        listener.set_nonblocking(true).unwrap();
        while !self.shutdown.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let venue = Arc::clone(&self);
                    thread::spawn(move || handle(&venue, stream));
                }
                Err(_) => thread::sleep(Duration::from_millis(2)),
            }
        }
    }

    /// Applies a frame to the true book (frames the snapshot already contains are no-ops).
    fn advance(&self, frame: &Frame) {
        let mut books = self.books.lock().unwrap();
        let book = books.get_mut(&frame.symbol).unwrap();
        if frame.last > book.last_update_id {
            book.apply(Side::Buy, &frame.bids);
            book.apply(Side::Sell, &frame.asks);
            book.last_update_id = frame.last;
        }
    }

    fn stream(&self, mut stream: TcpStream) {
        let connection = self.connections.fetch_add(1, Ordering::SeqCst);
        let request = read_head(&mut stream);
        for symbol in SYMBOLS {
            assert!(
                request.contains(&format!("{}@depth@100ms", symbol.to_lowercase())),
                "{request}"
            );
        }
        let key = request
            .lines()
            .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
            .unwrap()
            .trim();
        let accept = accept_for_key(key.as_bytes());
        let head = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Accept: {}\r\n\r\n",
            std::str::from_utf8(&accept).unwrap()
        );
        if stream.write_all(head.as_bytes()).is_err() {
            return;
        }
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                return;
            }
            let index = self.cursor.load(Ordering::SeqCst);
            let Some(frame) = self.frames.get(index) else {
                // Caught up: keep the connection alive like Binance does, with pings.
                if stream.write_all(&server_frame(0x9, b"keepalive")).is_err() {
                    return;
                }
                thread::sleep(Duration::from_millis(50));
                continue;
            };
            if self.faults.silence_after.is_some_and(|at| index > at) && connection == 0 {
                thread::sleep(Duration::from_millis(10));
                continue; // Hold the connection open, sending nothing.
            }
            self.advance(frame);
            self.cursor.fetch_add(1, Ordering::SeqCst);
            if self.faults.drop_frame != Some(index)
                && stream.write_all(&server_frame(0x1, &frame.raw)).is_err()
            {
                return;
            }
            if self.faults.disconnect_after == Some(index) && connection == 0 {
                return; // Dropping the stream closes the TCP connection.
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn rest(&self, mut stream: TcpStream) {
        let request = read_head(&mut stream);
        let path = request.split(' ').nth(1).unwrap();
        let query = path
            .strip_prefix("/api/v3/depth?")
            .expect("only depth requests expected");
        let param = |name: &str| {
            query
                .split('&')
                .find_map(|kv| kv.strip_prefix(&format!("{name}=")))
                .unwrap()
                .to_owned()
        };
        let body = {
            let books = self.books.lock().unwrap();
            books[&param("symbol")].snapshot_json(param("limit").parse().unwrap())
        };
        self.snapshots_served.fetch_add(1, Ordering::SeqCst);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             X-MBX-USED-WEIGHT-1M: 50\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
    }

    fn true_best(&self, symbol: &str, side: Side) -> Vec<(Dec, Dec)> {
        self.books.lock().unwrap()[symbol].best(side, COMPARE_DEPTH)
    }

    fn finished(&self) -> bool {
        self.cursor.load(Ordering::SeqCst) >= self.frames.len()
    }
}

fn read_head(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read_exact(&mut byte).is_err() {
            break;
        }
        head.push(byte[0]);
    }
    String::from_utf8(head).unwrap()
}

fn server_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0x80 | opcode];
    match payload.len() {
        n if n < 126 => out.push(u8::try_from(n).unwrap()),
        n if n <= 0xFFFF => {
            out.push(126);
            out.extend_from_slice(&u16::try_from(n).unwrap().to_be_bytes());
        }
        n => {
            out.push(127);
            out.extend_from_slice(&u64::try_from(n).unwrap().to_be_bytes());
        }
    }
    out.extend_from_slice(payload);
    out
}

// ---------------------------------------------------------------------------------------------
// Session harness
// ---------------------------------------------------------------------------------------------

type BestLevels = [Vec<(Dec, Dec)>; 2];

#[derive(Default)]
struct Observed {
    statuses: Vec<MdStatus>,
    books: BTreeMap<u32, BestLevels>,
    /// Every book update and instrument status change, in order, for replay comparison.
    events: Vec<String>,
}

struct Recorder {
    observed: Arc<Mutex<Observed>>,
    instruments: InstrumentTable,
}

impl MdHandler for Recorder {
    fn on_book(&mut self, instrument: InstrumentId, book: &Book, event_time: WallTime) {
        // Test-only: copies the top of the book on every update.
        let meta = self.instruments.get(instrument).unwrap();
        let levels = |side| -> Vec<(Dec, Dec)> {
            book.side(side)
                .iter()
                .take(COMPARE_DEPTH)
                .map(|l| (l.price.to_dec(meta.tick), l.qty.to_dec(meta.lot)))
                .collect()
        };
        let top = [levels(Side::Buy), levels(Side::Sell)];
        let event = format!(
            "book {} {:?} {:?} {}/{} at {}",
            instrument.get(),
            top[0].first(),
            top[1].first(),
            book.side(Side::Buy).len(),
            book.side(Side::Sell).len(),
            event_time.as_nanos()
        );
        let mut observed = self.observed.lock().unwrap();
        observed.books.insert(instrument.get(), top);
        observed.events.push(event);
    }

    fn on_status(&mut self, status: MdStatus) {
        let mut observed = self.observed.lock().unwrap();
        // Instrument-level changes come from the shared book core, so replay reproduces them;
        // connection-level ones are properties of the live session only.
        if matches!(
            status,
            MdStatus::InstrumentLive(_) | MdStatus::InstrumentDown { .. }
        ) {
            observed.events.push(format!("{status:?}"));
        }
        observed.statuses.push(status);
    }
}

fn instruments() -> InstrumentTable {
    let rules =
        decode_exchange_info(&std::fs::read(format!("{FIXTURES}/exchange_info.json")).unwrap())
            .unwrap();
    let instruments = SYMBOLS
        .iter()
        .enumerate()
        .map(|(i, symbol)| {
            let rule = rules.iter().find(|r| r.symbol.as_str() == *symbol).unwrap();
            Instrument {
                id: InstrumentId::new(u32::try_from(i).unwrap()),
                venue: VenueId::Binance,
                symbol: Symbol::new(symbol).unwrap(),
                tick: rule.tick,
                lot: rule.lot,
                min_notional: rule.min_notional,
            }
        })
        .collect();
    InstrumentTable::new(instruments).unwrap()
}

struct Run {
    venue: Arc<Venue>,
    observed: Arc<Mutex<Observed>>,
    stop: Arc<AtomicBool>,
    session: thread::JoinHandle<bowst_venue::binance::md::MdStats>,
}

fn start(faults: Faults, tweak: impl FnOnce(&mut MdConfig)) -> Run {
    start_journaled(faults, tweak, None)
}

fn start_journaled(
    faults: Faults,
    tweak: impl FnOnce(&mut MdConfig),
    journal: Option<bowst_journal::JournalProducer>,
) -> Run {
    let (venue, ws_url, rest_url) = Venue::start(faults);
    let mut config = MdConfig::new(&ws_url, &rest_url);
    config.idle_sleep = Some(Duration::from_micros(200));
    config.connect_timeout = Duration::from_secs(5);
    config.reconnect_initial = Duration::from_millis(20);
    config.reconnect_max = Duration::from_millis(200);
    config.snapshot_retry = Duration::from_millis(50);
    config.rest_weight_per_minute = 6_000;
    tweak(&mut config);
    let observed = Arc::new(Mutex::new(Observed::default()));
    let recorder = Recorder {
        observed: Arc::clone(&observed),
        instruments: instruments(),
    };
    let tls = TlsConfig::from_platform_roots().unwrap();
    let mut session = MdSession::new(config, instruments(), tls, recorder).unwrap();
    if let Some(journal) = journal {
        session = session.with_journal(journal);
    }
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let session = thread::spawn(move || {
        session.run(&flag);
        session.stats()
    });
    Run {
        venue,
        observed,
        stop,
        session,
    }
}

impl Run {
    /// Waits until the venue has sent everything and both books match the venue's true books.
    fn converge(&self) {
        let started = Instant::now();
        loop {
            if self.venue.finished() && self.books_match() {
                return;
            }
            assert!(
                started.elapsed() < DEADLINE,
                "books did not converge; statuses: {:?}",
                self.statuses()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn books_match(&self) -> bool {
        let observed = self.observed.lock().unwrap();
        SYMBOLS.iter().enumerate().all(|(i, symbol)| {
            observed
                .books
                .get(&u32::try_from(i).unwrap())
                .is_some_and(|[bids, asks]| {
                    *bids == self.venue.true_best(symbol, Side::Buy)
                        && *asks == self.venue.true_best(symbol, Side::Sell)
                })
        })
    }

    fn statuses(&self) -> Vec<MdStatus> {
        self.observed.lock().unwrap().statuses.clone()
    }

    fn finish(self) -> (Vec<MdStatus>, bowst_venue::binance::md::MdStats, Arc<Venue>) {
        self.stop.store(true, Ordering::Relaxed);
        let stats = self.session.join().unwrap();
        self.venue.shutdown.store(true, Ordering::Relaxed);
        let statuses = self.observed.lock().unwrap().statuses.clone();
        (statuses, stats, self.venue)
    }
}

fn count(statuses: &[MdStatus], matches: impl Fn(&MdStatus) -> bool) -> usize {
    statuses.iter().filter(|s| matches(s)).count()
}

// ---------------------------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------------------------

#[test]
fn clean_session_matches_the_venue_exactly() {
    let run = start(Faults::default(), |_| {});
    run.converge();
    let (statuses, stats, venue) = run.finish();
    assert_eq!(
        count(&statuses, |s| matches!(s, MdStatus::InstrumentLive(_))),
        2,
        "{statuses:?}"
    );
    assert_eq!(
        count(&statuses, |s| matches!(s, MdStatus::InstrumentDown { .. })),
        0,
        "{statuses:?}"
    );
    assert_eq!(stats.connections, 1);
    assert_eq!(stats.book_invalidations, 0);
    assert_eq!(stats.messages, u64::try_from(venue.frames.len()).unwrap());
    assert!(stats.snapshots_applied >= 2);
}

#[test]
fn lost_message_takes_only_that_book_down_and_resyncs() {
    // Drop a BTCUSDT message in the middle of the stream.
    let frames = frames();
    let victim = frames
        .iter()
        .enumerate()
        .filter(|(_, f)| f.symbol == "BTCUSDT")
        .nth(150)
        .unwrap()
        .0;
    let run = start(
        Faults {
            drop_frame: Some(victim),
            ..Faults::default()
        },
        |_| {},
    );
    run.converge();
    let (statuses, stats, _) = run.finish();
    let down: Vec<_> = statuses
        .iter()
        .filter(|s| matches!(s, MdStatus::InstrumentDown { .. }))
        .collect();
    assert_eq!(down.len(), 1, "{statuses:?}");
    assert!(
        matches!(down[0], MdStatus::InstrumentDown { instrument, reason } if instrument.get() == 0 && reason.contains("gap")),
        "{down:?}"
    );
    assert_eq!(stats.connections, 1, "a gap must not drop the connection");
    assert_eq!(stats.book_invalidations, 1);
    assert_eq!(
        count(
            &statuses,
            |s| matches!(s, MdStatus::InstrumentLive(id) if id.get() == 0)
        ),
        2
    );
}

#[test]
fn disconnect_resets_every_book_and_reconnects() {
    let run = start(
        Faults {
            disconnect_after: Some(200),
            ..Faults::default()
        },
        |_| {},
    );
    run.converge();
    let (statuses, stats, venue) = run.finish();
    assert_eq!(stats.connections, 2, "{statuses:?}");
    assert!(venue.connections.load(Ordering::SeqCst) >= 2);
    let disconnected = statuses
        .iter()
        .position(|s| matches!(s, MdStatus::Disconnected { .. }))
        .expect("disconnect reported");
    let live_after = count(&statuses[disconnected..], |s| {
        matches!(s, MdStatus::InstrumentLive(_))
    });
    assert_eq!(
        live_after, 2,
        "both books resync after reconnecting: {statuses:?}"
    );
}

#[test]
fn silent_connection_is_treated_as_dead() {
    let run = start(
        Faults {
            silence_after: Some(100),
            ..Faults::default()
        },
        |config| {
            config.stale_after = Duration::from_millis(300);
        },
    );
    run.converge();
    let (statuses, stats, _) = run.finish();
    assert_eq!(stats.connections, 2, "{statuses:?}");
    assert!(
        statuses
            .iter()
            .any(|s| matches!(s, MdStatus::Disconnected { reason } if reason.contains("no data"))),
        "{statuses:?}"
    );
}

#[test]
fn unreachable_venue_is_retried_with_backoff() {
    // Nothing listens on this port.
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = format!("ws://127.0.0.1:{port}");
    let mut config = MdConfig::new(&url, &format!("http://127.0.0.1:{port}"));
    config.reconnect_initial = Duration::from_millis(10);
    config.reconnect_max = Duration::from_millis(40);
    config.connect_timeout = Duration::from_millis(200);
    let observed = Arc::new(Mutex::new(Observed::default()));
    let recorder = Recorder {
        observed: Arc::clone(&observed),
        instruments: instruments(),
    };
    let mut session = MdSession::new(
        config,
        instruments(),
        TlsConfig::from_platform_roots().unwrap(),
        recorder,
    )
    .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        session.run(&flag);
        session.stats()
    });
    let started = Instant::now();
    while count(&observed.lock().unwrap().statuses, |s| {
        matches!(s, MdStatus::Disconnected { .. })
    }) < 3
    {
        assert!(started.elapsed() < DEADLINE);
        thread::sleep(Duration::from_millis(10));
    }
    stop.store(true, Ordering::Relaxed);
    let stats = handle.join().unwrap();
    assert_eq!(stats.connections, 0);
    assert!(
        observed.lock().unwrap().books.is_empty(),
        "no book is ever reported without a connection"
    );
}

#[test]
fn journal_replays_the_live_session_exactly() {
    let dir = std::env::temp_dir().join(format!("bowst-md-journal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (producer, journal) =
        bowst_journal::start(bowst_journal::JournalConfig::new(&dir)).unwrap();

    // A lost message and a mid-stream disconnect, so resyncs and resets are replayed too.
    let frames = frames();
    let victim = frames
        .iter()
        .enumerate()
        .filter(|(_, f)| f.symbol == "BTCUSDT")
        .nth(100)
        .unwrap()
        .0;
    let faults = Faults {
        drop_frame: Some(victim),
        disconnect_after: Some(300),
        ..Faults::default()
    };
    let run = start_journaled(faults, |_| {}, Some(producer));
    run.converge();
    let live = Arc::clone(&run.observed);
    let (statuses, _, _) = run.finish();
    assert!(
        statuses
            .iter()
            .any(|s| matches!(s, MdStatus::InstrumentDown { .. })),
        "gap exercised"
    );
    let journal_stats = journal.finish().unwrap();
    assert_eq!(journal_stats.dropped, 0);

    let replayed = Arc::new(Mutex::new(Observed::default()));
    let mut recorder = Recorder {
        observed: Arc::clone(&replayed),
        instruments: instruments(),
    };
    let mut reader = bowst_journal::JournalReader::open(&dir).unwrap();
    let report = bowst_venue::binance::replay::replay(&mut reader, &mut recorder).unwrap();
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(report.sessions, 1);
    assert_eq!(report.dropped, 0);
    assert!(!report.torn_tail);
    assert!(report.resets >= 2, "disconnect and final stop: {report:?}");
    let (live, replayed) = (live.lock().unwrap(), replayed.lock().unwrap());
    assert!(live.events.len() > 300, "{}", live.events.len());
    assert_eq!(live.events.len(), replayed.events.len());
    for (i, (a, b)) in live.events.iter().zip(&replayed.events).enumerate() {
        assert_eq!(a, b, "first difference at event {i}");
    }
    assert_eq!(live.books, replayed.books);
}
