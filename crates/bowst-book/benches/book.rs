//! Book update benchmarks for the market-data path.

// Benchmarks are test code: failing fast on a broken setup is the right behavior.
#![allow(missing_docs, clippy::unwrap_used)]

use std::hint::black_box;

use bowst_book::{Book, BookSync, Level, LevelUpdate, SeqRange, SyncConfig};
use bowst_core::{Price, Qty, Side};
use criterion::{Criterion, criterion_group, criterion_main};

const DEPTH: i64 = 1_000;
const MID: i64 = 1_000_000;

fn levels(side: Side) -> Vec<Level> {
    (1..=DEPTH)
        .map(|i| Level {
            price: Price::new(match side {
                Side::Buy => MID - i,
                Side::Sell => MID + i,
            }),
            qty: Qty::new(10),
        })
        .collect()
}

fn book_updates(c: &mut Criterion) {
    let mut book = Book::new(5_000).unwrap();
    book.load(levels(Side::Buy), levels(Side::Sell)).unwrap();

    c.bench_function("book/update_existing_top_level", |b| {
        let mut qty = 1;
        b.iter(|| {
            qty = qty % 50 + 1;
            book.apply(black_box(LevelUpdate {
                side: Side::Buy,
                price: Price::new(MID - 1),
                qty: Qty::new(qty),
            }))
        });
    });

    c.bench_function("book/insert_and_remove_near_top", |b| {
        b.iter(|| {
            // A new best bid inside the spread, then its removal: the typical churn.
            let price = Price::new(MID);
            book.apply(black_box(LevelUpdate {
                side: Side::Buy,
                price,
                qty: Qty::new(3),
            }))
            .unwrap();
            book.apply(black_box(LevelUpdate {
                side: Side::Buy,
                price,
                qty: Qty::ZERO,
            }))
        });
    });

    c.bench_function("book/update_deep_level", |b| {
        let mut qty = 1;
        b.iter(|| {
            qty = qty % 50 + 1;
            book.apply(black_box(LevelUpdate {
                side: Side::Sell,
                price: Price::new(MID + DEPTH / 2),
                qty: Qty::new(qty),
            }))
        });
    });

    c.bench_function("book/update_level_at_depth_50", |b| {
        let mut qty = 1;
        b.iter(|| {
            qty = qty % 50 + 1;
            book.apply(black_box(LevelUpdate {
                side: Side::Sell,
                price: Price::new(MID + 50),
                qty: Qty::new(qty),
            }))
        });
    });

    c.bench_function("book/insert_and_remove_at_depth_200", |b| {
        b.iter(|| {
            // Removing a level 200 deep, then adding it back: the deep churn that dominates
            // recorded Binance traffic.
            let price = Price::new(MID + 200);
            for qty in [Qty::ZERO, Qty::new(10)] {
                book.apply(black_box(LevelUpdate {
                    side: Side::Sell,
                    price,
                    qty,
                }))
                .unwrap();
            }
        });
    });

    c.bench_function("book/best_bid_ask", |b| {
        b.iter(|| (black_box(&book).best_bid(), black_box(&book).best_ask()));
    });
}

fn sync_delta(c: &mut Criterion) {
    let config = SyncConfig {
        levels_per_side: 5_000,
        buffered_messages: 1_024,
        buffered_updates: 65_536,
    };
    let mut sync = BookSync::new(config).unwrap();
    sync.on_snapshot(0, levels(Side::Buy), levels(Side::Sell))
        .unwrap();
    // A delta message the size of a typical 100 ms depth update.
    let message: Vec<_> = (0..20)
        .map(|i| LevelUpdate {
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            price: Price::new(if i % 2 == 0 { MID - 1 - i } else { MID + 1 + i }),
            qty: Qty::new(i % 3 * 5),
        })
        .collect();
    let mut seq = 0;
    c.bench_function("sync/apply_20_level_delta", |b| {
        b.iter(|| {
            seq += 1;
            sync.on_delta(SeqRange::single(seq), black_box(&message).iter().copied())
        });
    });
}

criterion_group!(benches, book_updates, sync_delta);
criterion_main!(benches);
