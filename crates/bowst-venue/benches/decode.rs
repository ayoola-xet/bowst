//! Decoding benchmarks on recorded Binance messages.

// Benchmarks are test code: failing fast on a broken setup is the right behavior.
#![allow(missing_docs, clippy::unwrap_used)]

use std::hint::black_box;

use bowst_core::{Instrument, InstrumentId, InstrumentTable, VenueId};
use bowst_venue::binance::depth::{DepthSnapshotDecoder, DepthUpdateDecoder};
use bowst_venue::binance::exchange_info::decode_exchange_info;
use bowst_venue::json::Reader;
use bowst_venue::ws::reader::{ReaderConfig, WsEvent, WsReader};
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

/// All recorded messages as one WebSocket byte stream, read through `WsReader` in 16 KiB
/// socket-sized chunks: the framing cost alone, before JSON decoding.
fn websocket(c: &mut Criterion) {
    let stream = read("depth_stream.jsonl");
    let mut wire = Vec::new();
    let mut messages = 0_u64;
    for line in stream.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
        wire.push(0x81);
        match u16::try_from(line.len()) {
            Ok(len) if len < 126 => wire.push(u8::try_from(len).unwrap()),
            Ok(len) => {
                wire.push(126);
                wire.extend_from_slice(&len.to_be_bytes());
            }
            Err(_) => {
                wire.push(127);
                wire.extend_from_slice(&u64::try_from(line.len()).unwrap().to_be_bytes());
            }
        }
        wire.extend_from_slice(line);
        messages += 1;
    }
    let config = ReaderConfig {
        buffer: 256 * 1024,
        max_frame: 128 * 1024,
        max_message: 128 * 1024,
    };
    let mut reader = WsReader::new(config).unwrap();
    let mut group = c.benchmark_group("ws");
    group.throughput(Throughput::Elements(messages));
    group.bench_function("read_all_recorded_frames", |b| {
        b.iter(|| {
            reader.reset();
            let mut bytes = 0;
            for chunk in wire.chunks(16 * 1024) {
                let spare = reader.spare();
                spare[..chunk.len()].copy_from_slice(chunk);
                reader.commit(chunk.len());
                while let Some(WsEvent::Text(payload)) = reader.next_event().unwrap() {
                    bytes += payload.len();
                }
            }
            black_box(bytes)
        });
    });
    group.finish();
}

criterion_group!(benches, decode, websocket);
criterion_main!(benches);
