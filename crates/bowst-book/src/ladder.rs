//! One side of a level-2 book: price levels sorted with the best level last.

use bowst_core::{Price, Qty, Side};
use core::cmp::Ordering;

use crate::BookError;

/// Levels at the top of the book checked linearly before binary search.
const TOP_SCAN: usize = 16;

/// Aggregate resting quantity at one price.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Level {
    /// Price in ticks.
    pub price: Price,
    /// Total quantity in lots. Always positive inside a book.
    pub qty: Qty,
}

/// What [`Ladder::apply`] did with an update.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// A new level was added.
    Inserted,
    /// An existing level's quantity changed.
    Updated,
    /// An existing level was removed.
    Removed,
    /// Nothing changed: a removal of a price that had no level.
    Unchanged,
    /// The price lies beyond the [horizon](Ladder::horizon) and was ignored.
    BeyondHorizon,
}

/// One side of the book.
///
/// Levels are kept in a pre-allocated array sorted from worst to best, so the best level is
/// last. Nearly all venue updates touch the top of the book, where inserting or removing
/// moves only the few levels above the change. The array never grows past its capacity, so
/// the ladder never allocates after construction.
///
/// When the ladder is full and an update would add a level, the worst level is dropped and
/// the ladder records a *horizon*: the worst price it still knows exactly. Updates beyond the
/// horizon are ignored from then on, because the levels between the horizon and such an
/// update are unknown. Every level at or better than the horizon stays exact.
#[derive(Clone, Debug)]
pub struct Ladder {
    side: Side,
    levels: Vec<Level>,
    capacity: usize,
    horizon: Option<Price>,
}

impl Ladder {
    /// Creates an empty ladder that holds at most `capacity` levels.
    ///
    /// # Errors
    /// [`BookError::ZeroCapacity`] if `capacity` is zero.
    pub fn new(side: Side, capacity: usize) -> Result<Self, BookError> {
        if capacity == 0 {
            return Err(BookError::ZeroCapacity);
        }
        Ok(Self {
            side,
            levels: Vec::with_capacity(capacity),
            capacity,
            horizon: None,
        })
    }

    /// Which side of the book this is.
    #[must_use]
    pub fn side(&self) -> Side {
        self.side
    }

    /// Number of levels held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.levels.len()
    }

    /// Whether the ladder holds no levels.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// Maximum number of levels held.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The worst price known exactly, once the ladder has had to drop levels. `None` means
    /// every level the venue reported is held.
    #[must_use]
    pub fn horizon(&self) -> Option<Price> {
        self.horizon
    }

    /// The best level.
    #[must_use]
    pub fn best(&self) -> Option<Level> {
        self.levels.last().copied()
    }

    /// The `n`-th best level, counting from 0.
    #[must_use]
    pub fn nth_best(&self, n: usize) -> Option<Level> {
        let index = self.levels.len().checked_sub(n)?.checked_sub(1)?;
        self.levels.get(index).copied()
    }

    /// Levels from best to worst.
    #[must_use = "iterators are lazy"]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Level> + DoubleEndedIterator + '_ {
        self.levels.iter().rev().copied()
    }

    /// Orders a level's price relative to `price`: `Less` means worse, `Greater` means better.
    fn rank(&self, level: Price, price: Price) -> Ordering {
        match self.side {
            Side::Buy => level.cmp(&price),
            Side::Sell => price.cmp(&level),
        }
    }

    /// Finds `price`: `Ok(index)` if held, `Err(index)` where it would be inserted.
    ///
    /// Venue updates cluster at the top of the book, so the best few levels are scanned
    /// directly before falling back to binary search over the rest.
    fn search(&self, price: Price) -> Result<usize, usize> {
        let len = self.levels.len();
        let scanned = len.min(TOP_SCAN);
        for (offset, level) in self.levels.iter().rev().take(scanned).enumerate() {
            // `offset < len`, so these cannot underflow.
            let index = len.saturating_sub(offset).saturating_sub(1);
            match self.rank(level.price, price) {
                Ordering::Equal => return Ok(index),
                Ordering::Less => return Err(index.saturating_add(1)),
                Ordering::Greater => {}
            }
        }
        let rest = len.saturating_sub(scanned);
        self.levels.get(..rest).map_or(Err(0), |deeper| {
            deeper.binary_search_by(|level| self.rank(level.price, price))
        })
    }

    /// Whether `price` lies strictly beyond the horizon.
    fn beyond_horizon(&self, price: Price) -> bool {
        self.horizon
            .is_some_and(|horizon| self.side.is_more_aggressive(horizon, price))
    }

    /// Sets the quantity at `price`. A quantity of zero removes the level.
    ///
    /// # Errors
    /// [`BookError::InvalidPrice`] if `price` is not positive, or [`BookError::InvalidQty`] if
    /// `qty` is negative. The ladder is unchanged on error.
    pub fn apply(&mut self, price: Price, qty: Qty) -> Result<Applied, BookError> {
        validate(self.side, price, qty)?;
        if self.beyond_horizon(price) {
            return Ok(Applied::BeyondHorizon);
        }
        match (self.search(price), qty.is_zero()) {
            (Ok(index), true) => {
                self.levels.remove(index);
                Ok(Applied::Removed)
            }
            (Ok(index), false) => {
                if let Some(level) = self.levels.get_mut(index) {
                    level.qty = qty;
                }
                Ok(Applied::Updated)
            }
            (Err(_), true) => Ok(Applied::Unchanged),
            (Err(index), false) => Ok(self.insert(index, Level { price, qty })),
        }
    }

    /// Inserts a new level at its sorted `index`, dropping the worst level if full.
    fn insert(&mut self, index: usize, level: Level) -> Applied {
        if self.levels.len() < self.capacity {
            self.levels.insert(index, level);
            return Applied::Inserted;
        }
        if index == 0 {
            // Worse than every level held: it becomes unknown territory.
            self.horizon = self.levels.first().map(|worst| worst.price);
            return Applied::BeyondHorizon;
        }
        // Full: drop the worst level (rare, and the only non-top-of-book memmove).
        self.levels.remove(0);
        self.levels.insert(index.saturating_sub(1), level);
        self.horizon = self.levels.first().map(|worst| worst.price);
        Applied::Inserted
    }

    /// Removes every level and forgets the horizon.
    pub fn clear(&mut self) {
        self.levels.clear();
        self.horizon = None;
    }

    /// Replaces the contents with `levels`, given in any order.
    ///
    /// # Errors
    /// [`BookError::InvalidPrice`], [`BookError::InvalidQty`] (including a zero quantity,
    /// which a snapshot must not contain) or [`BookError::DuplicateLevel`]. On error the ladder
    /// is left empty.
    pub fn load(&mut self, levels: impl IntoIterator<Item = Level>) -> Result<(), BookError> {
        self.clear();
        let result = self.load_inner(levels);
        if result.is_err() {
            self.clear();
        }
        result
    }

    fn load_inner(&mut self, levels: impl IntoIterator<Item = Level>) -> Result<(), BookError> {
        let side = self.side;
        let mut sorted = false;
        for level in levels {
            validate(side, level.price, level.qty)?;
            if level.qty.is_zero() {
                return Err(BookError::InvalidQty {
                    side,
                    price: level.price,
                    qty: level.qty,
                });
            }
            if self.levels.len() < self.capacity {
                self.levels.push(level);
                continue;
            }
            // More levels than capacity: sort what we have once, then insert the rest in order
            // so the ladder keeps the best `capacity` levels and records its horizon.
            if !sorted {
                self.sort_and_check()?;
                sorted = true;
            }
            match self.search(level.price) {
                Ok(_) => {
                    return Err(BookError::DuplicateLevel {
                        side,
                        price: level.price,
                    });
                }
                Err(index) => {
                    self.insert(index, level);
                }
            }
        }
        if !sorted {
            self.sort_and_check()?;
        }
        Ok(())
    }

    fn sort_and_check(&mut self) -> Result<(), BookError> {
        let side = self.side;
        self.levels.sort_unstable_by(|a, b| match side {
            Side::Buy => a.price.cmp(&b.price),
            Side::Sell => b.price.cmp(&a.price),
        });
        match self.levels.windows(2).find_map(|pair| match pair {
            [a, b] if a.price == b.price => Some(a.price),
            _ => None,
        }) {
            Some(price) => Err(BookError::DuplicateLevel { side, price }),
            None => Ok(()),
        }
    }
}

fn validate(side: Side, price: Price, qty: Qty) -> Result<(), BookError> {
    if price.get() <= 0 {
        return Err(BookError::InvalidPrice { side, price });
    }
    if qty.get() < 0 {
        return Err(BookError::InvalidQty { side, price, qty });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(price: i64, qty: i64) -> Level {
        Level {
            price: Price::new(price),
            qty: Qty::new(qty),
        }
    }

    fn prices(ladder: &Ladder) -> Vec<i64> {
        ladder.iter().map(|l| l.price.get()).collect()
    }

    #[test]
    fn bids_are_best_highest_and_asks_best_lowest() {
        let mut bids = Ladder::new(Side::Buy, 8).unwrap();
        let mut asks = Ladder::new(Side::Sell, 8).unwrap();
        for p in [100, 102, 101] {
            bids.apply(Price::new(p), Qty::new(1)).unwrap();
            asks.apply(Price::new(p), Qty::new(1)).unwrap();
        }
        assert_eq!(prices(&bids), [102, 101, 100]);
        assert_eq!(prices(&asks), [100, 101, 102]);
        assert_eq!(bids.nth_best(1), Some(lvl(101, 1)));
        assert_eq!(asks.nth_best(3), None);
    }

    #[test]
    fn updates_and_removes_levels() {
        let mut bids = Ladder::new(Side::Buy, 8).unwrap();
        assert_eq!(
            bids.apply(Price::new(10), Qty::new(5)),
            Ok(Applied::Inserted)
        );
        assert_eq!(
            bids.apply(Price::new(10), Qty::new(7)),
            Ok(Applied::Updated)
        );
        assert_eq!(bids.best(), Some(lvl(10, 7)));
        assert_eq!(bids.apply(Price::new(10), Qty::ZERO), Ok(Applied::Removed));
        assert_eq!(
            bids.apply(Price::new(10), Qty::ZERO),
            Ok(Applied::Unchanged)
        );
        assert!(bids.is_empty());
    }

    #[test]
    fn rejects_invalid_input_without_changing_state() {
        let mut asks = Ladder::new(Side::Sell, 4).unwrap();
        asks.apply(Price::new(5), Qty::new(1)).unwrap();
        assert!(matches!(
            asks.apply(Price::new(0), Qty::new(1)),
            Err(BookError::InvalidPrice { .. })
        ));
        assert!(matches!(
            asks.apply(Price::new(6), Qty::new(-1)),
            Err(BookError::InvalidQty { .. })
        ));
        assert_eq!(prices(&asks), [5]);
        assert_eq!(
            Ladder::new(Side::Buy, 0).unwrap_err(),
            BookError::ZeroCapacity
        );
    }

    #[test]
    fn full_ladder_drops_worst_and_sets_horizon() {
        let mut bids = Ladder::new(Side::Buy, 3).unwrap();
        for p in [100, 101, 102] {
            bids.apply(Price::new(p), Qty::new(1)).unwrap();
        }
        // Better than everything: 100 is dropped, 101 becomes the horizon.
        assert_eq!(
            bids.apply(Price::new(103), Qty::new(1)),
            Ok(Applied::Inserted)
        );
        assert_eq!(prices(&bids), [103, 102, 101]);
        assert_eq!(bids.horizon(), Some(Price::new(101)));
        // Beyond the horizon: ignored, even after room frees up.
        assert_eq!(
            bids.apply(Price::new(100), Qty::new(9)),
            Ok(Applied::BeyondHorizon)
        );
        bids.apply(Price::new(103), Qty::ZERO).unwrap();
        assert_eq!(
            bids.apply(Price::new(99), Qty::new(9)),
            Ok(Applied::BeyondHorizon)
        );
        // At the horizon: still exact, so applied.
        assert_eq!(
            bids.apply(Price::new(101), Qty::new(4)),
            Ok(Applied::Updated)
        );
        assert_eq!(prices(&bids), [102, 101]);
    }

    #[test]
    fn full_ladder_ignores_new_worst_level() {
        let mut asks = Ladder::new(Side::Sell, 2).unwrap();
        asks.apply(Price::new(10), Qty::new(1)).unwrap();
        asks.apply(Price::new(11), Qty::new(1)).unwrap();
        assert_eq!(
            asks.apply(Price::new(12), Qty::new(1)),
            Ok(Applied::BeyondHorizon)
        );
        assert_eq!(asks.horizon(), Some(Price::new(11)));
        assert_eq!(prices(&asks), [10, 11]);
    }

    #[test]
    fn load_sorts_and_truncates_to_capacity() {
        let mut bids = Ladder::new(Side::Buy, 3).unwrap();
        bids.load([lvl(5, 1), lvl(9, 1), lvl(7, 1), lvl(8, 1), lvl(6, 1)])
            .unwrap();
        assert_eq!(prices(&bids), [9, 8, 7]);
        assert_eq!(bids.horizon(), Some(Price::new(7)));

        bids.load([lvl(1, 1), lvl(2, 1)]).unwrap();
        assert_eq!(prices(&bids), [2, 1]);
        assert_eq!(bids.horizon(), None);
    }

    #[test]
    fn load_rejects_bad_snapshots_and_leaves_ladder_empty() {
        let mut asks = Ladder::new(Side::Sell, 8).unwrap();
        assert_eq!(
            asks.load([lvl(5, 1), lvl(5, 2)]),
            Err(BookError::DuplicateLevel {
                side: Side::Sell,
                price: Price::new(5)
            })
        );
        assert!(asks.is_empty());
        assert!(matches!(
            asks.load([lvl(5, 0)]),
            Err(BookError::InvalidQty { .. })
        ));
        let mut small = Ladder::new(Side::Sell, 1).unwrap();
        assert!(matches!(
            small.load([lvl(5, 1), lvl(6, 1), lvl(5, 1)]),
            Err(BookError::DuplicateLevel { .. })
        ));
        assert!(small.is_empty());
    }

    proptest::proptest! {
        /// The search agrees with a plain binary search over the whole ladder, on both sides,
        /// for prices held, between levels, and beyond either end.
        #[test]
        fn search_matches_a_reference_binary_search(
            prices in proptest::collection::btree_set(1_i64..10_000, 0..300),
            probe in 0_i64..10_100,
            buy in proptest::bool::ANY,
        ) {
            let side = if buy { Side::Buy } else { Side::Sell };
            let mut ladder = Ladder::new(side, 1_000).unwrap();
            ladder.load(prices.iter().map(|&p| lvl(p, 1))).unwrap();
            let probe = Price::new(probe);
            let reference = ladder
                .levels
                .binary_search_by(|level| ladder.rank(level.price, probe));
            proptest::prop_assert_eq!(ladder.search(probe), reference);
        }
    }
}
