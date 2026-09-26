//! Decode-and-apply cost per message through `MdBooks`, on recorded Binance traffic: the
//! quantity behind the Phase 1 latency target (README §15). Warm caches, one thread.
//!
//! Besides criterion's mean, it prints the per-message distribution (the target is a p99),
//! measured with the same histogram the live session uses.

// Benchmarks are test code: failing fast on a broken setup is the right behavior.
#![allow(missing_docs, clippy::unwrap_used, clippy::print_stderr)]

use std::hint::black_box;
use std::time::Instant;

use bowst_book::{Book, SyncConfig};
use bowst_core::{Instrument, InstrumentId, InstrumentTable, VenueId, WallTime};
use bowst_telemetry::LatencyHistogram;
use bowst_venue::binance::books::MdBooks;
use bowst_venue::binance::exchange_info::decode_exchange_info;
use bowst_venue::binance::md::{MdHandler, MdStatus};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/binance");
const SYMBOLS: [&str; 2] = ["BTCUSDT", "DOGEUSDT"];

fn read(name: &str) -> Vec<u8> {
    std::fs::read(format!("{FIXTURES}/{name}")).unwrap()
}

fn table() -> InstrumentTable {
    let rules = decode_exchange_info(&read("exchange_info.json")).unwrap();
    let instruments = SYMBOLS
        .iter()
        .enumerate()
        .map(|(i, symbol)| {
            let rule = rules.iter().find(|r| r.symbol.as_str() == *symbol).unwrap();
            Instrument {
                id: InstrumentId::new(u32::try_from(i).unwrap()),
                venue: VenueId::Binance,
                symbol: rule.symbol,
                tick: rule.tick,
                lot: rule.lot,
                min_notional: rule.min_notional,
            }
        })
        .collect();
    InstrumentTable::new(instruments).unwrap()
}

struct Quiet;

impl MdHandler for Quiet {
    fn on_book(&mut self, _: InstrumentId, _: &Book, _: WallTime) {}
    fn on_status(&mut self, _: MdStatus) {}
}

/// The recorded session: frames, plus each instrument's snapshot and the frame after which
/// it is applied.
struct Session {
    frames: Vec<Vec<u8>>,
    snapshots: Vec<(usize, InstrumentId, Vec<u8>)>,
}

fn session() -> Session {
    let frames: Vec<Vec<u8>> = read("depth_stream.jsonl")
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .map(<[u8]>::to_vec)
        .collect();
    let snapshots = SYMBOLS
        .iter()
        .enumerate()
        .map(|(i, symbol)| {
            let body = read(&format!("depth_snapshot_{}.json", symbol.to_lowercase()));
            let text = std::str::from_utf8(&body).unwrap();
            let id: u64 = text
                .split("\"lastUpdateId\":")
                .nth(1)
                .unwrap()
                .split(|c: char| !c.is_ascii_digit())
                .next()
                .unwrap()
                .parse()
                .unwrap();
            let after = frames
                .iter()
                .position(|f| {
                    let t = std::str::from_utf8(f).unwrap();
                    t.contains(&format!("\"{symbol}\""))
                        && t.split("\"u\":")
                            .nth(1)
                            .unwrap()
                            .split(|c: char| !c.is_ascii_digit())
                            .next()
                            .unwrap()
                            .parse::<u64>()
                            .unwrap()
                            >= id
                })
                .unwrap();
            (after, InstrumentId::new(u32::try_from(i).unwrap()), body)
        })
        .collect();
    Session { frames, snapshots }
}

/// Runs the whole session once, calling `each` around every message's `apply`.
fn run(books: &mut MdBooks, session: &Session, mut each: impl FnMut(&mut MdBooks, &[u8])) {
    books.reset_all();
    for (index, frame) in session.frames.iter().enumerate() {
        each(books, frame);
        for (after, id, body) in &session.snapshots {
            if *after == index {
                books.on_raw_snapshot(*id, body, WallTime::from_nanos(0), &mut Quiet);
            }
        }
    }
}

fn md_books(c: &mut Criterion) {
    let session = session();
    let sync = SyncConfig {
        levels_per_side: 5_000,
        buffered_messages: 4_096,
        buffered_updates: 1 << 18,
    };
    let mut books = MdBooks::new(table(), sync, 20_000, 5_000).unwrap();

    // Per-message distribution over many warm passes.
    let mut histogram = LatencyHistogram::new();
    for _ in 0..3 {
        run(&mut books, &session, |b, f| {
            black_box(b.apply(f).unwrap());
        });
    }
    for _ in 0..200 {
        run(&mut books, &session, |b, f| {
            let started = Instant::now();
            black_box(b.apply(f).unwrap());
            histogram.record(u64::try_from(started.elapsed().as_nanos()).unwrap());
        });
    }
    let s = histogram.summary();
    eprintln!(
        "md_books apply per message (warm, {} samples): p50 {} ns, p90 {} ns, p99 {} ns, p99.9 {} ns, max {} ns",
        s.count, s.p50, s.p90, s.p99, s.p999, s.max
    );

    let mut group = c.benchmark_group("md_books");
    group.throughput(Throughput::Elements(
        u64::try_from(session.frames.len()).unwrap(),
    ));
    group.bench_function("apply_all_recorded_frames", |b| {
        b.iter(|| {
            run(&mut books, &session, |b, f| {
                black_box(b.apply(f).unwrap());
            });
        });
    });
    group.finish();
}

criterion_group!(benches, md_books);
criterion_main!(benches);
