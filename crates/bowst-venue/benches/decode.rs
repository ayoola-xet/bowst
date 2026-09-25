//! Decoding benchmarks on recorded Binance messages.

// Benchmarks are test code: failing fast on a broken setup is the right behavior.
#![allow(missing_docs, clippy::unwrap_used)]

use std::hint::black_box;

use bowst_core::{Instrument, InstrumentId, InstrumentTable, VenueId};
use bowst_venue::binance::depth::{DepthSnapshotDecoder, DepthUpdateDecoder};
use bowst_venue::binance::exchange_info::decode_exchange_info;
use bowst_venue::json::Reader;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/binance");

fn read(name: &str) -> Vec<u8> {
    std::fs::read(format!("{FIXTURES}/{name}")).unwrap()
}

fn table() -> InstrumentTable {
    let rules = decode_exchange_info(&read("exchange_info.json")).unwrap();
    let instruments = ["BTCUSDT", "DOGEUSDT"]
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

fn decode(c: &mut Criterion) {
    let table = table();
    let stream = read("depth_stream.jsonl");
    let mut frames: Vec<&[u8]> = stream
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    frames.sort_by_key(|f| f.len());
    let median = frames[frames.len() / 2];
    let total: usize = frames.iter().map(|f| f.len()).sum();

    let mut decoder = DepthUpdateDecoder::new(5_000);
    let mut group = c.benchmark_group("binance");
    group.throughput(Throughput::Bytes(u64::try_from(median.len()).unwrap()));
    group.bench_function("depth_update_median_frame", |b| {
        b.iter(|| {
            decoder
                .decode(black_box(median), &table)
                .map(|u| u.updates.len())
        });
    });
    // JSON structure only, no number conversion: separates reader cost from decode cost.
    group.bench_function("json_walk_median_frame", |b| {
        b.iter(|| Reader::new(black_box(median)).skip());
    });
    group.throughput(Throughput::Bytes(u64::try_from(total).unwrap()));
    group.bench_function("depth_update_all_recorded_frames", |b| {
        b.iter(|| {
            for frame in &frames {
                black_box(
                    decoder
                        .decode(frame, &table)
                        .map(|u| u.updates.len())
                        .unwrap(),
                );
            }
        });
    });
    let snapshot = read("depth_snapshot_btcusdt.json");
    let instrument = *table.get(InstrumentId::new(0)).unwrap();
    let mut snapshots = DepthSnapshotDecoder::new(5_000);
    group.throughput(Throughput::Bytes(u64::try_from(snapshot.len()).unwrap()));
    group.bench_function("depth_snapshot_1000_levels", |b| {
        b.iter(|| {
            snapshots
                .decode(black_box(&snapshot), &instrument)
                .map(|s| s.bids.len())
        });
    });
    group.finish();
}

criterion_group!(benches, decode);
criterion_main!(benches);
