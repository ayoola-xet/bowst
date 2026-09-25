//! Property tests: the ladder and book sync against a simple reference model.

use std::collections::BTreeMap;

use bowst_book::{BookSync, Ladder, Level, LevelUpdate, SeqRange, SyncConfig};
use bowst_core::{Price, Qty, Side};
use proptest::prelude::*;

/// Reference model: every level ever reported, best-first order computed on demand.
#[derive(Default)]
struct Model {
    levels: BTreeMap<i64, i64>,
}

impl Model {
    fn apply(&mut self, price: i64, qty: i64) {
        if qty == 0 {
            self.levels.remove(&price);
        } else {
            self.levels.insert(price, qty);
        }
    }

    fn best_first(&self, side: Side) -> Vec<(i64, i64)> {
        let mut levels: Vec<_> = self.levels.iter().map(|(p, q)| (*p, *q)).collect();
        if side == Side::Buy {
            levels.reverse();
        }
        levels
    }
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

/// Updates concentrated on a narrow price range so levels are hit repeatedly.
fn updates() -> impl Strategy<Value = Vec<(i64, i64)>> {
    proptest::collection::vec((1_i64..40, prop_oneof![Just(0_i64), 1_i64..100]), 0..300)
}

fn held(ladder: &Ladder) -> Vec<(i64, i64)> {
    ladder
        .iter()
        .map(|l| (l.price.get(), l.qty.get()))
        .collect()
}

fn is_strictly_best_first(side: Side, levels: &[(i64, i64)]) -> bool {
    levels.windows(2).all(|w| match side {
        Side::Buy => w[0].0 > w[1].0,
        Side::Sell => w[0].0 < w[1].0,
    })
}

proptest! {
    #[test]
    fn ladder_with_room_matches_model(side in side(), updates in updates()) {
        let mut ladder = Ladder::new(side, 64).unwrap();
        let mut model = Model::default();
        for (price, qty) in updates {
            ladder.apply(Price::new(price), Qty::new(qty)).unwrap();
            model.apply(price, qty);
        }
        prop_assert_eq!(held(&ladder), model.best_first(side));
        prop_assert_eq!(ladder.horizon(), None);
    }

    #[test]
    fn truncated_ladder_is_exact_up_to_its_horizon(
        side in side(),
        capacity in 1_usize..8,
        updates in updates(),
    ) {
        let mut ladder = Ladder::new(side, capacity).unwrap();
        let mut model = Model::default();
        for (price, qty) in updates {
            ladder.apply(Price::new(price), Qty::new(qty)).unwrap();
            model.apply(price, qty);

            let levels = held(&ladder);
            prop_assert!(levels.len() <= capacity);
            prop_assert!(is_strictly_best_first(side, &levels));
            prop_assert!(levels.iter().all(|&(_, q)| q > 0));

            // Everything the model has at or better than the horizon must be held exactly.
            let expected: Vec<_> = model
                .best_first(side)
                .into_iter()
                .filter(|&(p, _)| match ladder.horizon() {
                    None => true,
                    Some(h) => !side.is_more_aggressive(h, Price::new(p)),
                })
                .collect();
            prop_assert_eq!(levels, expected);
        }
    }

    #[test]
    fn load_matches_model_regardless_of_input_order(
        side in side(),
        capacity in 1_usize..12,
        prices in proptest::collection::btree_set(1_i64..1_000, 0..20),
        shuffle_seed in any::<u64>(),
    ) {
        let mut input: Vec<_> = prices.iter().map(|&p| (p, p % 7 + 1)).collect();
        // Deterministic shuffle without extra dependencies.
        let mut state = shuffle_seed | 1;
        for i in (1..input.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let j = usize::try_from(state % (u64::try_from(i).unwrap() + 1)).unwrap();
            input.swap(i, j);
        }
        let mut ladder = Ladder::new(side, capacity).unwrap();
        ladder
            .load(input.iter().map(|&(p, q)| Level { price: Price::new(p), qty: Qty::new(q) }))
            .unwrap();

        let mut model = Model::default();
        for &(p, q) in &input {
            model.apply(p, q);
        }
        let expected: Vec<_> = model.best_first(side).into_iter().take(capacity).collect();
        prop_assert_eq!(held(&ladder), expected);
    }

    /// Splitting one stream of updates into deltas, losing none, and taking the snapshot at
    /// any point must always reproduce the book the full stream produces.
    #[test]
    fn sync_reproduces_the_full_stream(
        updates in proptest::collection::vec((1_i64..20, 1_i64..50), 1..120),
        chunk in 1_usize..6,
        snapshot_after in 0_usize..40,
    ) {
        // Bids below 20 and asks above 20 can never cross.
        let as_update = |i: usize, (p, q): (i64, i64)| LevelUpdate {
            side: if i.is_multiple_of(2) { Side::Buy } else { Side::Sell },
            price: Price::new(if i.is_multiple_of(2) { p } else { p + 20 }),
            qty: Qty::new(q % 3 * 10), // Zero a third of the time.
        };
        let all: Vec<_> = updates.iter().enumerate().map(|(i, &u)| as_update(i, u)).collect();
        let messages: Vec<_> = all.chunks(chunk).collect();

        let config = SyncConfig { levels_per_side: 64, buffered_messages: 256, buffered_updates: 1024 };
        let mut sync = BookSync::new(config).unwrap();
        let mut snapshot_model: (Model, Model) = Default::default();
        let snapshot_at = snapshot_after.min(messages.len());
        let mut seq = 0_u64;
        for (index, message) in messages.iter().enumerate() {
            let first = seq + 1;
            seq += u64::try_from(message.len()).unwrap();
            sync.on_delta(SeqRange::new(first, seq).unwrap(), message.iter().copied()).unwrap();
            if index < snapshot_at {
                for u in *message {
                    let model = if u.side == Side::Buy { &mut snapshot_model.0 } else { &mut snapshot_model.1 };
                    model.apply(u.price.get(), u.qty.get());
                }
            }
        }
        let snapshot_seq: u64 = messages.iter().take(snapshot_at).map(|m| u64::try_from(m.len()).unwrap()).sum();
        let to_levels = |m: &Model| -> Vec<Level> {
            m.levels.iter().map(|(&p, &q)| Level { price: Price::new(p), qty: Qty::new(q) }).collect()
        };
        sync.on_snapshot(snapshot_seq, to_levels(&snapshot_model.0), to_levels(&snapshot_model.1)).unwrap();

        let mut full = (Model::default(), Model::default());
        for u in &all {
            let model = if u.side == Side::Buy { &mut full.0 } else { &mut full.1 };
            model.apply(u.price.get(), u.qty.get());
        }
        let book = sync.book().unwrap();
        prop_assert_eq!(held(book.side(Side::Buy)), full.0.best_first(Side::Buy));
        prop_assert_eq!(held(book.side(Side::Sell)), full.1.best_first(Side::Sell));
        prop_assert_eq!(sync.last_seq(), Some(seq));
    }
}
