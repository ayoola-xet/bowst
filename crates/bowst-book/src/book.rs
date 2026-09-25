//! A two-sided level-2 order book.

use bowst_core::{Price, Qty, Side};

use crate::BookError;
use crate::ladder::{Applied, Ladder, Level};

/// A change to one price level. A quantity of zero removes the level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LevelUpdate {
    /// Book side.
    pub side: Side,
    /// Price in ticks.
    pub price: Price,
    /// New total quantity in lots; zero removes the level.
    pub qty: Qty,
}

/// Bids and asks for one instrument. Pure data: no sequencing, I/O or time. See
/// [`crate::BookSync`] for keeping it in step with a venue feed.
#[derive(Clone, Debug)]
pub struct Book {
    bids: Ladder,
    asks: Ladder,
}

impl Book {
    /// Creates an empty book holding at most `levels_per_side` levels on each side.
    ///
    /// # Errors
    /// [`BookError::ZeroCapacity`] if `levels_per_side` is zero.
    pub fn new(levels_per_side: usize) -> Result<Self, BookError> {
        Ok(Self {
            bids: Ladder::new(Side::Buy, levels_per_side)?,
            asks: Ladder::new(Side::Sell, levels_per_side)?,
        })
    }

    /// One side of the book.
    #[must_use]
    pub fn side(&self, side: Side) -> &Ladder {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut Ladder {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Best bid.
    #[must_use]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.best()
    }

    /// Best ask.
    #[must_use]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks.best()
    }

    /// Whether the best bid is at or above the best ask. A crossed book cannot come from a
    /// correct feed, so it means lost or misapplied updates.
    #[must_use]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid.price >= ask.price,
            _ => false,
        }
    }

    /// Applies one level update.
    ///
    /// # Errors
    /// See [`Ladder::apply`].
    #[inline]
    pub fn apply(&mut self, update: LevelUpdate) -> Result<Applied, BookError> {
        self.side_mut(update.side).apply(update.price, update.qty)
    }

    /// Replaces both sides with a snapshot.
    ///
    /// # Errors
    /// See [`Ladder::load`]. On error the whole book is left empty.
    pub fn load(
        &mut self,
        bids: impl IntoIterator<Item = Level>,
        asks: impl IntoIterator<Item = Level>,
    ) -> Result<(), BookError> {
        let result = self.bids.load(bids).and_then(|()| self.asks.load(asks));
        if result.is_err() {
            self.clear();
        }
        result
    }

    /// Empties both sides.
    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }
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

    #[test]
    fn detects_crossed_and_locked_books() {
        let mut book = Book::new(4).unwrap();
        book.load([lvl(99, 1)], [lvl(101, 1)]).unwrap();
        assert!(!book.is_crossed());
        book.apply(LevelUpdate {
            side: Side::Buy,
            price: Price::new(101),
            qty: Qty::new(1),
        })
        .unwrap();
        assert!(book.is_crossed());
    }

    #[test]
    fn failed_load_empties_both_sides() {
        let mut book = Book::new(4).unwrap();
        book.load([lvl(99, 1)], [lvl(101, 1)]).unwrap();
        assert!(book.load([lvl(98, 1)], [lvl(0, 1)]).is_err());
        assert!(book.side(Side::Buy).is_empty());
        assert!(book.side(Side::Sell).is_empty());
    }
}
