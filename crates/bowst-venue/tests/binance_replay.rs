//! Replays recorded Binance Spot market data (public, from the official market-data mirror)
//! through the decoders and `BookSync`, following Binance's documented sync procedure.
//!
//! Fixtures: `tests/fixtures/binance/` holds ~20 s of `@depth@100ms` updates for BTCUSDT and
//! DOGEUSDT, a 1,000-level snapshot of each taken 2 s into the recording, and their
//! `exchangeInfo` rules.

// Test helpers fail fast on broken fixtures.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;

use bowst_book::{BookSync, Level, SyncConfig, SyncError};
use bowst_core::alloc_counter::{CountingAllocator, count_allocations};
use bowst_core::{Instrument, InstrumentId, InstrumentTable, Side, VenueId};
use bowst_venue::binance::depth::{DepthSnapshotDecoder, DepthUpdateDecoder};
use bowst_venue::binance::exchange_info::decode_exchange_info;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/binance");
const SYMBOLS: [&str; 2] = ["BTCUSDT", "DOGEUSDT"];
const CONFIG: SyncConfig = SyncConfig {
    levels_per_side: 5_000,
    buffered_messages: 1_024,
    buffered_updates: 65_536,
};

fn read(name: &str) -> Vec<u8> {
    std::fs::read(format!("{FIXTURES}/{name}")).unwrap()
}

fn instruments() -> InstrumentTable {
    let rules = decode_exchange_info(&read("exchange_info.json")).unwrap();
    let instruments = SYMBOLS
        .iter()
        .enumerate()
        .map(|(index, symbol)| {
            let rule = rules.iter().find(|r| r.symbol.as_str() == *symbol).unwrap();
            assert!(rule.trading);
            Instrument {
                id: InstrumentId::new(u32::try_from(index).unwrap()),
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

struct Snapshot {
    last_update_id: u64,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

fn snapshots(table: &InstrumentTable) -> Vec<Snapshot> {
    let mut decoder = DepthSnapshotDecoder::new(5_000);
    table
        .iter()
        .map(|instrument| {
            let raw = read(&format!(
                "depth_snapshot_{}.json",
                instrument.symbol.as_str().to_lowercase()
            ));
            let snapshot = decoder.decode(&raw, instrument).unwrap();
            Snapshot {
                last_update_id: snapshot.last_update_id,
                bids: snapshot.bids.to_vec(),
                asks: snapshot.asks.to_vec(),
            }
        })
        .collect()
}

fn frames() -> Vec<Vec<u8>> {
    read("depth_stream.jsonl")
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(<[u8]>::to_vec)
        .collect()
}

/// Runs the documented procedure: buffer deltas, apply the snapshot once a delta reaches
/// its `lastUpdateId`, then stay live. Returns the syncs and the last `u` seen per instrument.
fn replay(
    frames: &[Vec<u8>],
    skip_frame: Option<usize>,
) -> (Vec<BookSync>, Vec<u64>, Vec<SyncError>) {
    let table = instruments();
    let snapshots = snapshots(&table);
    let mut syncs: Vec<_> = table
        .iter()
        .map(|_| BookSync::new(CONFIG).unwrap())
        .collect();
    let mut snapshot_applied = vec![false; syncs.len()];
    let mut last_u = vec![0; syncs.len()];
    let mut errors = Vec::new();
    let mut decoder = DepthUpdateDecoder::new(5_000);

    for (index, frame) in frames.iter().enumerate() {
        if Some(index) == skip_frame {
            continue;
        }
        let update = decoder.decode(frame, &table).unwrap();
        let i = usize::try_from(update.instrument.get()).unwrap();
        last_u[i] = update.range.last();
        if let Err(err) = syncs[i].on_delta(update.range, update.updates.iter().copied()) {
            errors.push(err);
        }
        let snapshot = &snapshots[i];
        if !snapshot_applied[i] && update.range.last() >= snapshot.last_update_id {
            syncs[i]
                .on_snapshot(
                    snapshot.last_update_id,
                    snapshot.bids.iter().copied(),
                    snapshot.asks.iter().copied(),
                )
                .unwrap();
            snapshot_applied[i] = true;
        }
    }
    (syncs, last_u, errors)
}

#[test]
fn recorded_session_stays_live_and_consistent() {
    let frames = frames();
    let (syncs, last_u, errors) = replay(&frames, None);
    assert!(errors.is_empty(), "{errors:?}");
    for (sync, last) in syncs.iter().zip(last_u) {
        assert!(sync.is_live());
        assert_eq!(sync.last_seq(), Some(last));
        let book = sync.book().unwrap();
        let (bid, ask) = (book.best_bid().unwrap(), book.best_ask().unwrap());
        assert!(bid.price < ask.price);
        assert_eq!(sync.stats().invalidations, 0);
        assert!(book.side(Side::Buy).len() >= 900 && book.side(Side::Sell).len() >= 900);
    }
}

/// Rebuilds each book independently (snapshot plus every delta after it, applied to a plain
/// map) and requires the synced book to match exactly.
#[test]
fn recorded_session_matches_reference_rebuild() {
    let table = instruments();
    let snapshots = snapshots(&table);
    let mut reference: Vec<[BTreeMap<i64, i64>; 2]> = snapshots
        .iter()
        .map(|s| {
            let map = |levels: &[Level]| {
                levels
                    .iter()
                    .map(|l| (l.price.get(), l.qty.get()))
                    .collect()
            };
            [map(&s.bids), map(&s.asks)]
        })
        .collect();
    let mut decoder = DepthUpdateDecoder::new(5_000);
    for frame in frames() {
        let update = decoder.decode(&frame, &table).unwrap();
        let i = usize::try_from(update.instrument.get()).unwrap();
        if update.range.last() <= snapshots[i].last_update_id {
            continue;
        }
        for u in update.updates {
            let side = &mut reference[i][usize::from(u.side == Side::Sell)];
            if u.qty.is_zero() {
                side.remove(&u.price.get());
            } else {
                side.insert(u.price.get(), u.qty.get());
            }
        }
    }

    let (syncs, _, _) = replay(&frames(), None);
    for (sync, [bids, asks]) in syncs.iter().zip(reference) {
        let book = sync.book().unwrap();
        let held = |side| -> BTreeMap<i64, i64> {
            book.side(side)
                .iter()
                .map(|l| (l.price.get(), l.qty.get()))
                .collect()
        };
        assert_eq!(held(Side::Buy), bids);
        assert_eq!(held(Side::Sell), asks);
    }
}

#[test]
fn dropped_message_is_detected_as_a_gap() {
    let frames = frames();
    // Drop a BTCUSDT frame well after the snapshot.
    let table = instruments();
    let victim = frames
        .iter()
        .enumerate()
        .rev()
        .find(|(_, f)| {
            std::str::from_utf8(f)
                .unwrap()
                .contains("\"s\":\"BTCUSDT\"")
        })
        .map(|(i, _)| i)
        .unwrap()
        .saturating_sub(40);
    let mut decoder = DepthUpdateDecoder::new(5_000);
    assert_eq!(
        decoder.decode(&frames[victim], &table).unwrap().instrument,
        InstrumentId::new(0)
    );

    let (syncs, _, errors) = replay(&frames, Some(victim));
    assert!(
        matches!(errors.as_slice(), [SyncError::Gap { .. }]),
        "{errors:?}"
    );
    assert!(
        !syncs[0].is_live(),
        "book must stay unavailable until a new snapshot"
    );
    assert!(syncs[1].is_live(), "other instruments are unaffected");
}

#[test]
fn live_decoding_does_not_allocate() {
    let frames = frames();
    let table = instruments();
    let mut decoder = DepthUpdateDecoder::new(5_000);
    let mut syncs: Vec<_> = table
        .iter()
        .map(|_| BookSync::new(CONFIG).unwrap())
        .collect();
    let ((), allocations) = count_allocations(|| {
        for frame in &frames {
            let update = decoder.decode(frame, &table).unwrap();
            let i = usize::try_from(update.instrument.get()).unwrap();
            // Buffering path (no snapshot applied here), which is allocation-free too.
            let _ = syncs[i].on_delta(update.range, update.updates.iter().copied());
        }
    });
    assert_eq!(allocations, 0);
}
