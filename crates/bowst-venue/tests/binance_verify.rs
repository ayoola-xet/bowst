//! Book verification in the deterministic market-data core ([`MdBooks`]), driven by recorded
//! Binance traffic (public, from the official market-data mirror). Verification snapshots are
//! built from a second, independent copy of the books, so they are exactly what the venue
//! would have served at that update.

// Test helpers fail fast on broken fixtures.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use bowst_book::{Book, SyncConfig};
use bowst_core::{Instrument, InstrumentId, InstrumentTable, Side, Symbol, VenueId, WallTime};
use bowst_venue::binance::books::{Applied, DownReason, MdBooks, Verification};
use bowst_venue::binance::exchange_info::decode_exchange_info;
use bowst_venue::binance::md::{MdHandler, MdStatus};
use bowst_venue::json::Reader;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/binance");
const SYMBOLS: [&str; 2] = ["BTCUSDT", "DOGEUSDT"];
const BTC: InstrumentId = InstrumentId::new(0);

fn read(name: &str) -> Vec<u8> {
    std::fs::read(format!("{FIXTURES}/{name}")).unwrap()
}

fn instruments() -> InstrumentTable {
    let rules = decode_exchange_info(&read("exchange_info.json")).unwrap();
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

/// Every recorded frame, with the instrument and last update ID it carries.
fn frames() -> Vec<(InstrumentId, u64, Vec<u8>)> {
    read("depth_stream.jsonl")
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .map(|raw| {
            let text = std::str::from_utf8(raw).unwrap();
            let id = u32::from(!text.contains("\"BTCUSDT\""));
            let last = text
                .split("\"u\":")
                .nth(1)
                .unwrap()
                .split(|c: char| !c.is_ascii_digit())
                .next()
                .unwrap()
                .parse()
                .unwrap();
            (InstrumentId::new(id), last, raw.to_vec())
        })
        .collect()
}

fn snapshot_update_id(body: &[u8]) -> u64 {
    let mut r = Reader::new(body);
    r.begin_object().unwrap();
    while let Some(key) = r.next_key().unwrap() {
        if key == "lastUpdateId" {
            return r.u64().unwrap();
        }
        r.skip().unwrap();
    }
    panic!("no lastUpdateId");
}

#[derive(Default)]
struct Collector {
    statuses: Vec<MdStatus>,
}

impl MdHandler for Collector {
    fn on_book(&mut self, _: InstrumentId, _: &Book, _: WallTime) {}
    fn on_status(&mut self, status: MdStatus) {
        self.statuses.push(status);
    }
}

/// The recorded session, fed into a fresh `MdBooks` one frame at a time, applying each
/// fixture snapshot as soon as the stream reaches it (Binance's documented procedure).
struct Feed {
    books: MdBooks,
    frames: Vec<(InstrumentId, u64, Vec<u8>)>,
    snapshots: Vec<(Vec<u8>, u64, bool)>,
    next: usize,
    handler: Collector,
}

impl Feed {
    fn new() -> Self {
        let sync = SyncConfig {
            levels_per_side: 5_000,
            buffered_messages: 4_096,
            buffered_updates: 1 << 18,
        };
        let snapshots = SYMBOLS
            .iter()
            .map(|s| {
                let body = read(&format!("depth_snapshot_{}.json", s.to_lowercase()));
                let id = snapshot_update_id(&body);
                (body, id, false)
            })
            .collect();
        Self {
            books: MdBooks::new(instruments(), sync, 20_000, 5_000).unwrap(),
            frames: frames(),
            snapshots,
            next: 0,
            handler: Collector::default(),
        }
    }

    /// Feeds one frame, returning what applying it did.
    fn step(&mut self) -> Applied {
        let (id, last, raw) = self.frames[self.next].clone();
        self.next += 1;
        let applied = self.books.on_message(&raw, &mut self.handler).unwrap();
        let (body, snapshot_id, done) = &mut self.snapshots[usize::try_from(id.get()).unwrap()];
        if !*done && last >= *snapshot_id {
            self.books
                .on_raw_snapshot(id, body, WallTime::from_nanos(0), &mut self.handler);
            *done = true;
        }
        applied
    }

    /// Feeds frames until `count` have been fed in total.
    fn run_to(&mut self, count: usize) {
        while self.next < count {
            self.step();
        }
    }

    /// Skips the next BTCUSDT frame, as if lost in transit.
    fn lose_next_btc_frame(&mut self) {
        let offset = self.frames[self.next..]
            .iter()
            .position(|(id, _, _)| *id == BTC)
            .unwrap();
        self.frames.remove(self.next + offset);
    }

    /// A REST-style snapshot body of the current BTCUSDT book: what the venue would serve now.
    fn btc_snapshot(&self) -> Vec<u8> {
        let meta = *self.books.instruments().get(BTC).unwrap();
        let book = self.books.book(BTC).expect("BTCUSDT is live");
        let side = |side| {
            book.side(side)
                .iter()
                .map(|l| {
                    format!(
                        r#"["{}","{}"]"#,
                        l.price.to_dec(meta.tick),
                        l.qty.to_dec(meta.lot)
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            r#"{{"lastUpdateId":{},"bids":[{}],"asks":[{}]}}"#,
            self.books.last_update_id(BTC).unwrap(),
            side(Side::Buy),
            side(Side::Sell)
        )
        .into_bytes()
    }

    fn verify_with(&mut self, snapshot: &[u8]) -> Verification {
        self.books
            .on_raw_verification_snapshot(BTC, snapshot, &mut self.handler)
    }

    fn downs(&self) -> Vec<&MdStatus> {
        self.handler
            .statuses
            .iter()
            .filter(|s| matches!(s, MdStatus::InstrumentDown { .. }))
            .collect()
    }
}

/// Frames fed before the scenarios start: both books are live well before this.
const WARM: usize = 200;

#[test]
fn a_correct_book_passes_verification() {
    let mut feed = Feed::new();
    feed.run_to(WARM);
    assert!(feed.books.is_live(BTC));
    assert!(feed.books.start_verification(BTC));
    feed.run_to(WARM + 20);
    let snapshot = feed.btc_snapshot();
    // The snapshot arrives after more deltas: the verification book catches up from its
    // buffer, then the two are compared.
    feed.run_to(WARM + 40);
    assert_eq!(feed.verify_with(&snapshot), Verification::Passed);
    assert_eq!(feed.books.verifying(), None);
    assert_eq!(feed.books.stats().verifications_passed, 1);
    assert!(feed.books.is_live(BTC));
    assert!(feed.downs().is_empty());
}

#[test]
fn a_snapshot_ahead_of_the_stream_is_compared_when_the_book_catches_up() {
    let mut ahead = Feed::new();
    ahead.run_to(WARM + 60);
    let snapshot = ahead.btc_snapshot();

    let mut feed = Feed::new();
    feed.run_to(WARM);
    assert!(feed.books.start_verification(BTC));
    feed.run_to(WARM + 10);
    assert_eq!(feed.verify_with(&snapshot), Verification::Pending);
    assert_eq!(feed.books.verifying(), Some(BTC));
    feed.run_to(WARM + 60);
    assert_eq!(
        feed.books.verifying(),
        None,
        "compared once the book caught up"
    );
    assert_eq!(feed.books.stats().verifications_passed, 1);
    assert!(feed.downs().is_empty());
}

#[test]
fn a_book_that_differs_from_the_venue_is_taken_down() {
    let mut feed = Feed::new();
    feed.run_to(WARM);
    assert!(feed.books.start_verification(BTC));
    feed.run_to(WARM + 20);
    // The venue's book lacks the fifth-best bid that ours has.
    let snapshot = String::from_utf8(feed.btc_snapshot()).unwrap();
    let bids_start = snapshot.find(r#""bids":["#).unwrap() + r#""bids":["#.len();
    let fifth = snapshot[bids_start..]
        .split(']')
        .nth(4)
        .unwrap()
        .trim_start_matches(',');
    let tampered = snapshot.replacen(&format!(",{fifth}]"), "", 1);
    assert_ne!(tampered, snapshot);

    let verdict = feed.verify_with(tampered.as_bytes());
    let Verification::Failed(mismatch) = verdict else {
        panic!("expected a mismatch, got {verdict:?}");
    };
    assert_eq!((mismatch.side, mismatch.level), (Side::Buy, 4));
    assert!(!feed.books.is_live(BTC), "fail closed");
    assert_eq!(feed.books.stats().verification_mismatches, 1);
    let downs = feed.downs();
    assert_eq!(downs.len(), 1);
    assert!(
        matches!(downs[0], MdStatus::InstrumentDown { instrument, reason }
            if *instrument == BTC && reason.contains("differs from a fresh snapshot")),
        "{downs:?}"
    );
}

#[test]
fn a_mismatch_found_on_a_later_delta_is_reported_by_apply() {
    let mut ahead = Feed::new();
    ahead.run_to(WARM + 60);
    let snapshot = String::from_utf8(ahead.btc_snapshot()).unwrap();
    // Change the best ask quantity in the venue's (future) snapshot.
    let asks = snapshot.find(r#""asks":[[""#).unwrap() + r#""asks":[[""#.len();
    let qty_start = asks + snapshot[asks..].find(r#"",""#).unwrap() + 3;
    let qty_end = qty_start + snapshot[qty_start..].find('"').unwrap();
    let mut tampered = snapshot.clone();
    tampered.replace_range(qty_start..qty_end, "123");

    let mut feed = Feed::new();
    feed.run_to(WARM);
    assert!(feed.books.start_verification(BTC));
    assert_eq!(feed.verify_with(tampered.as_bytes()), Verification::Pending);
    let mut verdict = None;
    while feed.next < WARM + 60 {
        if let Applied::Down { instrument, reason } = feed.step() {
            verdict = Some((instrument, reason));
        }
    }
    let (instrument, reason) = verdict.expect("mismatch reported when the book caught up");
    assert_eq!(instrument, BTC);
    assert!(matches!(reason, DownReason::Mismatch(m) if m.side == Side::Sell && m.level == 0));
    assert_eq!(feed.books.stats().verification_mismatches, 1);
}

#[test]
fn a_snapshot_older_than_the_buffer_gives_no_verdict() {
    let mut behind = Feed::new();
    behind.run_to(WARM - 50);
    let old = behind.btc_snapshot();

    let mut feed = Feed::new();
    feed.run_to(WARM);
    assert!(feed.books.start_verification(BTC));
    feed.run_to(WARM + 20);
    assert_eq!(feed.verify_with(&old), Verification::Unusable);
    assert_eq!(feed.books.verifying(), None);
    let stats = feed.books.stats();
    assert_eq!(
        (stats.verifications_passed, stats.verification_mismatches),
        (0, 0)
    );
    assert!(feed.books.is_live(BTC), "no verdict, no action");
}

#[test]
fn a_gap_cancels_the_verification() {
    let mut feed = Feed::new();
    feed.run_to(WARM);
    assert!(feed.books.start_verification(BTC));
    feed.lose_next_btc_frame();
    feed.run_to(WARM + 20);
    assert_eq!(feed.books.verifying(), None);
    assert!(!feed.books.is_live(BTC));
    let snapshot_from_elsewhere = {
        let mut other = Feed::new();
        other.run_to(WARM + 20);
        other.btc_snapshot()
    };
    assert_eq!(
        feed.verify_with(&snapshot_from_elsewhere),
        Verification::NotRunning
    );
    assert_eq!(feed.books.stats().verification_mismatches, 0);
}

#[test]
fn only_live_books_can_be_verified() {
    let mut feed = Feed::new();
    assert!(!feed.books.start_verification(BTC));
    feed.run_to(WARM);
    assert!(feed.books.start_verification(BTC));
    feed.books.reset_all();
    assert_eq!(feed.books.verifying(), None, "a reset ends verification");
}
