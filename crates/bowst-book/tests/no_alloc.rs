//! Proves applying market data never allocates once the book is built.

use bowst_book::{BookSync, Level, LevelUpdate, SeqRange, SyncConfig};
use bowst_core::alloc_counter::{CountingAllocator, count_allocations};
use bowst_core::{Price, Qty, Side};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

const ROUNDS: u64 = if cfg!(miri) { 50 } else { 20_000 };

fn update(side: Side, price: i64, qty: i64) -> LevelUpdate {
    LevelUpdate {
        side,
        price: Price::new(price),
        qty: Qty::new(qty),
    }
}

#[test]
fn deltas_snapshots_and_resyncs_do_not_allocate() {
    let config = SyncConfig {
        levels_per_side: 64,
        buffered_messages: 16,
        buffered_updates: 64,
    };
    let mut sync = BookSync::new(config).unwrap();
    let bids: Vec<_> = (1..=100)
        .map(|p| Level {
            price: Price::new(p),
            qty: Qty::new(1),
        })
        .collect();
    let asks: Vec<_> = (101..=200)
        .map(|p| Level {
            price: Price::new(p),
            qty: Qty::new(1),
        })
        .collect();

    let ((), allocations) = count_allocations(|| {
        // Snapshot larger than capacity exercises truncation.
        sync.on_snapshot(0, bids.iter().copied(), asks.iter().copied())
            .unwrap();
        for seq in 1..=ROUNDS {
            let offset = i64::try_from(seq % 8).unwrap();
            let updates = [
                update(Side::Buy, 90 + offset, i64::try_from(seq % 3).unwrap()),
                update(Side::Sell, 105 + offset, 2),
            ];
            sync.on_delta(SeqRange::single(seq), updates).unwrap();
        }
        // A gap, buffering, and a fresh snapshot.
        assert!(sync.on_delta(SeqRange::single(ROUNDS + 5), []).is_err());
        sync.on_delta(SeqRange::single(ROUNDS + 6), [update(Side::Buy, 50, 1)])
            .unwrap();
        sync.on_snapshot(ROUNDS + 5, bids.iter().copied(), asks.iter().copied())
            .unwrap();
        assert!(sync.is_live());
    });
    assert_eq!(allocations, 0);
}
