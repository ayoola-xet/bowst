//! Keeping a [`Book`] in step with a venue's snapshot-plus-delta feed.
//!
//! Venues publish a full snapshot (with a sequence number) and a stream of deltas, each
//! covering a contiguous range of sequence numbers. Levels carry absolute quantities, so
//! re-applying an already-applied update in order is harmless. The rules, shared by every
//! venue adapter:
//!
//! 1. Until a snapshot arrives, deltas are buffered (in pre-allocated storage). A break in the
//!    buffered chain discards the older part, since no snapshot can bridge it.
//! 2. On a snapshot, buffered deltas that end at or before it are dropped. The next one must
//!    satisfy `first <= snapshot + 1 <= last`; otherwise the snapshot is too old and another is
//!    needed. The rest are applied in order.
//! 3. While live, a delta ending at or before the last applied sequence is stale and ignored.
//!    One that starts after `last + 1` is a gap.
//! 4. A gap, invalid data or a crossed book invalidates the book: it is cleared and the sync
//!    returns to buffering until a new snapshot arrives.
//!
//! The book is only readable through [`BookSync::book`] while live, so a stale or
//! half-synchronized book can never be quoted from.

use bowst_core::Price;

use crate::BookError;
use crate::book::{Book, LevelUpdate};
use crate::ladder::Level;

/// An inclusive range of venue sequence numbers covered by one delta message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SeqRange {
    first: u64,
    last: u64,
}

impl SeqRange {
    /// Builds `first..=last`. `None` if `first > last`.
    #[must_use]
    pub const fn new(first: u64, last: u64) -> Option<Self> {
        if first > last {
            return None;
        }
        Some(Self { first, last })
    }

    /// A range covering one sequence number.
    #[must_use]
    pub const fn single(seq: u64) -> Self {
        Self {
            first: seq,
            last: seq,
        }
    }

    /// First sequence number.
    #[must_use]
    pub const fn first(self) -> u64 {
        self.first
    }

    /// Last sequence number.
    #[must_use]
    pub const fn last(self) -> u64 {
        self.last
    }

    /// Whether this range continues directly from `prev`, allowing overlap.
    fn continues(self, prev: u64) -> bool {
        self.first <= prev.saturating_add(1)
    }
}

/// Sizes for a [`BookSync`]. All storage is allocated up front.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncConfig {
    /// Levels held per side of the book.
    pub levels_per_side: usize,
    /// Delta messages that can be buffered while waiting for a snapshot.
    pub buffered_messages: usize,
    /// Level updates, across all buffered messages, that can be buffered.
    pub buffered_updates: usize,
}

/// Why the book was invalidated or a snapshot was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SyncError {
    /// A delta did not continue from the last applied sequence number. The book was
    /// invalidated and the delta buffered for the next snapshot.
    #[error("sequence gap: expected {expected}, got {}..={}", got.first, got.last)]
    Gap {
        /// Sequence number that should have come next.
        expected: u64,
        /// Range actually received.
        got: SeqRange,
    },
    /// The snapshot is older than the buffered deltas, so a newer one is needed. The book
    /// stays unavailable and the buffered deltas are kept.
    #[error("snapshot {snapshot} is older than buffered delta starting at {first_buffered}")]
    SnapshotTooOld {
        /// Snapshot sequence number.
        snapshot: u64,
        /// First sequence number of the oldest usable buffered delta.
        first_buffered: u64,
    },
    /// The book became crossed. It was invalidated.
    #[error("crossed book: bid {bid:?} >= ask {ask:?}")]
    Crossed {
        /// Best bid price.
        bid: Price,
        /// Best ask price.
        ask: Price,
    },
    /// Invalid venue data. The book was invalidated.
    #[error(transparent)]
    Book(#[from] BookError),
}

/// What happened to a delta that did not cause an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeltaOutcome {
    /// Applied to the live book.
    Applied,
    /// Stored until a snapshot arrives.
    Buffered,
    /// Already covered by the book or buffer; ignored.
    Stale,
}

/// Counters for monitoring.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Times the book was invalidated.
    pub invalidations: u64,
    /// Times the buffer overflowed (or broke) and was restarted.
    pub buffer_resets: u64,
    /// Stale deltas ignored.
    pub stale_deltas: u64,
}

#[derive(Clone, Copy, Debug)]
struct Pending {
    range: SeqRange,
    start: usize,
    len: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    AwaitingSnapshot,
    Live { last: u64 },
}

/// A [`Book`] plus the sequencing state that keeps it correct. See the module docs.
#[derive(Clone, Debug)]
pub struct BookSync {
    book: Book,
    state: State,
    pending: Vec<Pending>,
    pending_updates: Vec<LevelUpdate>,
    config: SyncConfig,
    stats: SyncStats,
}

impl BookSync {
    /// Creates a sync waiting for its first snapshot.
    ///
    /// # Errors
    /// [`BookError::ZeroCapacity`] if any configured size is zero.
    pub fn new(config: SyncConfig) -> Result<Self, BookError> {
        if config.buffered_messages == 0 || config.buffered_updates == 0 {
            return Err(BookError::ZeroCapacity);
        }
        Ok(Self {
            book: Book::new(config.levels_per_side)?,
            state: State::AwaitingSnapshot,
            pending: Vec::with_capacity(config.buffered_messages),
            pending_updates: Vec::with_capacity(config.buffered_updates),
            config,
            stats: SyncStats::default(),
        })
    }

    /// The book, only while it is live and consistent with the feed.
    #[must_use]
    pub fn book(&self) -> Option<&Book> {
        match self.state {
            State::Live { .. } => Some(&self.book),
            State::AwaitingSnapshot => None,
        }
    }

    /// Whether the book is live.
    #[must_use]
    pub fn is_live(&self) -> bool {
        matches!(self.state, State::Live { .. })
    }

    /// Last sequence number applied, while live.
    #[must_use]
    pub fn last_seq(&self) -> Option<u64> {
        match self.state {
            State::Live { last } => Some(last),
            State::AwaitingSnapshot => None,
        }
    }

    /// Monitoring counters.
    #[must_use]
    pub fn stats(&self) -> SyncStats {
        self.stats
    }

    /// Clears the book and waits for a new snapshot, for example after a reconnect.
    pub fn reset(&mut self) {
        self.invalidate();
    }

    /// Handles one delta message.
    ///
    /// # Errors
    /// [`SyncError::Gap`], [`SyncError::Crossed`] or [`SyncError::Book`]; each invalidates the
    /// book, and the caller must stop quoting the instrument and request a snapshot.
    pub fn on_delta(
        &mut self,
        range: SeqRange,
        updates: impl IntoIterator<Item = LevelUpdate>,
    ) -> Result<DeltaOutcome, SyncError> {
        let State::Live { last } = self.state else {
            return Ok(self.buffer(range, updates));
        };
        if range.last <= last {
            self.stats.stale_deltas = self.stats.stale_deltas.saturating_add(1);
            return Ok(DeltaOutcome::Stale);
        }
        if !range.continues(last) {
            self.invalidate();
            self.buffer(range, updates);
            return Err(SyncError::Gap {
                expected: last.saturating_add(1),
                got: range,
            });
        }
        self.apply_all(updates).inspect_err(|_| self.invalidate())?;
        self.state = State::Live { last: range.last };
        self.check_crossed()?;
        Ok(DeltaOutcome::Applied)
    }

    /// Loads a snapshot taken at sequence number `seq` and replays buffered deltas after it.
    ///
    /// While live, a snapshot older than the last applied delta is stale and ignored (returns
    /// `Ok(false)`); the live book is newer. Returns `Ok(true)` when the snapshot was used.
    ///
    /// # Errors
    /// [`SyncError::SnapshotTooOld`] (buffer kept, fetch a newer snapshot), or
    /// [`SyncError::Book`] / [`SyncError::Crossed`] for bad data (book invalidated).
    pub fn on_snapshot(
        &mut self,
        seq: u64,
        bids: impl IntoIterator<Item = Level>,
        asks: impl IntoIterator<Item = Level>,
    ) -> Result<bool, SyncError> {
        if let State::Live { last } = self.state
            && seq < last
        {
            return Ok(false);
        }
        if let Err(err) = self.book.load(bids, asks) {
            self.invalidate();
            return Err(err.into());
        }
        let mut cursor = seq;
        for index in 0..self.pending.len() {
            let Some(message) = self.pending.get(index).copied() else {
                break;
            };
            if message.range.last <= cursor {
                continue;
            }
            if !message.range.continues(cursor) {
                // Buffered deltas are contiguous, so only the first one kept can fail here.
                self.book.clear();
                self.state = State::AwaitingSnapshot;
                return Err(SyncError::SnapshotTooOld {
                    snapshot: seq,
                    first_buffered: message.range.first,
                });
            }
            let end = message.start.saturating_add(message.len);
            for position in message.start..end {
                let Some(update) = self.pending_updates.get(position).copied() else {
                    break;
                };
                if let Err(err) = self.book.apply(update) {
                    self.invalidate();
                    return Err(err.into());
                }
            }
            cursor = message.range.last;
        }
        self.pending.clear();
        self.pending_updates.clear();
        self.state = State::Live { last: cursor };
        self.check_crossed()?;
        Ok(true)
    }

    fn apply_all(
        &mut self,
        updates: impl IntoIterator<Item = LevelUpdate>,
    ) -> Result<(), BookError> {
        for update in updates {
            self.book.apply(update)?;
        }
        Ok(())
    }

    fn check_crossed(&mut self) -> Result<(), SyncError> {
        if let (Some(bid), Some(ask)) = (self.book.best_bid(), self.book.best_ask())
            && bid.price >= ask.price
        {
            self.invalidate();
            return Err(SyncError::Crossed {
                bid: bid.price,
                ask: ask.price,
            });
        }
        Ok(())
    }

    fn invalidate(&mut self) {
        self.book.clear();
        self.state = State::AwaitingSnapshot;
        self.pending.clear();
        self.pending_updates.clear();
        self.stats.invalidations = self.stats.invalidations.saturating_add(1);
    }

    fn restart_buffer(&mut self) {
        self.pending.clear();
        self.pending_updates.clear();
        self.stats.buffer_resets = self.stats.buffer_resets.saturating_add(1);
    }

    fn buffer(
        &mut self,
        range: SeqRange,
        updates: impl IntoIterator<Item = LevelUpdate>,
    ) -> DeltaOutcome {
        if let Some(prev) = self.pending.last() {
            if range.last <= prev.range.last {
                self.stats.stale_deltas = self.stats.stale_deltas.saturating_add(1);
                return DeltaOutcome::Stale;
            }
            if !range.continues(prev.range.last) {
                // No snapshot can bridge the break, so the older part is useless.
                self.restart_buffer();
            }
        }
        if self.pending.len() >= self.config.buffered_messages {
            self.restart_buffer();
        }
        let start = self.pending_updates.len();
        for update in updates {
            if self.pending_updates.len() >= self.config.buffered_updates {
                // Out of room: keep nothing, so the next snapshot starts a clean chain.
                self.restart_buffer();
                return DeltaOutcome::Buffered;
            }
            self.pending_updates.push(update);
        }
        let len = self.pending_updates.len().saturating_sub(start);
        self.pending.push(Pending { range, start, len });
        DeltaOutcome::Buffered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bowst_core::{Qty, Side};

    const CONFIG: SyncConfig = SyncConfig {
        levels_per_side: 16,
        buffered_messages: 8,
        buffered_updates: 32,
    };

    fn up(side: Side, price: i64, qty: i64) -> LevelUpdate {
        LevelUpdate {
            side,
            price: Price::new(price),
            qty: Qty::new(qty),
        }
    }

    fn lvl(price: i64, qty: i64) -> Level {
        Level {
            price: Price::new(price),
            qty: Qty::new(qty),
        }
    }

    fn range(first: u64, last: u64) -> SeqRange {
        SeqRange::new(first, last).unwrap()
    }

    fn live_at(seq: u64) -> BookSync {
        let mut sync = BookSync::new(CONFIG).unwrap();
        sync.on_snapshot(seq, [lvl(99, 5)], [lvl(101, 5)]).unwrap();
        sync
    }

    fn best(sync: &BookSync) -> (i64, i64, i64, i64) {
        let book = sync.book().unwrap();
        let (bid, ask) = (book.best_bid().unwrap(), book.best_ask().unwrap());
        (
            bid.price.get(),
            bid.qty.get(),
            ask.price.get(),
            ask.qty.get(),
        )
    }

    #[test]
    fn rejects_inverted_range() {
        assert_eq!(SeqRange::new(5, 4), None);
    }

    #[test]
    fn book_hidden_until_snapshot() {
        let mut sync = BookSync::new(CONFIG).unwrap();
        assert!(sync.book().is_none());
        assert_eq!(
            sync.on_delta(range(1, 2), [up(Side::Buy, 99, 1)]),
            Ok(DeltaOutcome::Buffered)
        );
        assert!(sync.book().is_none());
    }

    #[test]
    fn binance_style_bridging_replays_buffer() {
        let mut sync = BookSync::new(CONFIG).unwrap();
        // Buffered before the snapshot: 10..=12 is covered by it, 13..=15 bridges it.
        sync.on_delta(range(10, 12), [up(Side::Buy, 99, 1)])
            .unwrap();
        sync.on_delta(range(13, 15), [up(Side::Buy, 99, 7)])
            .unwrap();
        sync.on_delta(range(16, 16), [up(Side::Sell, 101, 3)])
            .unwrap();
        sync.on_snapshot(14, [lvl(99, 5)], [lvl(101, 5)]).unwrap();
        assert_eq!(best(&sync), (99, 7, 101, 3));
        assert_eq!(sync.last_seq(), Some(16));
        assert_eq!(
            sync.on_delta(range(17, 18), [up(Side::Sell, 100, 2)]),
            Ok(DeltaOutcome::Applied)
        );
        assert_eq!(best(&sync), (99, 7, 100, 2));
    }

    #[test]
    fn snapshot_newer_than_buffer_goes_live() {
        let mut sync = BookSync::new(CONFIG).unwrap();
        sync.on_delta(range(1, 3), [up(Side::Buy, 50, 1)]).unwrap();
        sync.on_snapshot(10, [lvl(99, 5)], [lvl(101, 5)]).unwrap();
        assert_eq!(sync.last_seq(), Some(10));
        assert_eq!(best(&sync), (99, 5, 101, 5));
        // The next live delta must bridge 11.
        assert_eq!(
            sync.on_delta(range(9, 12), [up(Side::Buy, 99, 6)]),
            Ok(DeltaOutcome::Applied)
        );
    }

    #[test]
    fn too_old_snapshot_keeps_buffer_for_the_next_one() {
        let mut sync = BookSync::new(CONFIG).unwrap();
        sync.on_delta(range(20, 22), [up(Side::Buy, 99, 1)])
            .unwrap();
        assert_eq!(
            sync.on_snapshot(10, [lvl(99, 5)], [lvl(101, 5)]),
            Err(SyncError::SnapshotTooOld {
                snapshot: 10,
                first_buffered: 20
            })
        );
        assert!(sync.book().is_none());
        sync.on_snapshot(21, [lvl(99, 5)], [lvl(101, 5)]).unwrap();
        assert_eq!(best(&sync), (99, 1, 101, 5));
    }

    #[test]
    fn gap_invalidates_and_buffers_the_new_chain() {
        let mut sync = live_at(10);
        assert_eq!(
            sync.on_delta(range(12, 13), [up(Side::Buy, 98, 1)]),
            Err(SyncError::Gap {
                expected: 11,
                got: range(12, 13)
            })
        );
        assert!(sync.book().is_none());
        assert_eq!(sync.stats().invalidations, 1);
        // The delta that revealed the gap is kept, so a snapshot at 12 bridges it.
        sync.on_snapshot(12, [lvl(99, 5)], [lvl(101, 5)]).unwrap();
        assert_eq!(sync.last_seq(), Some(13));
        assert_eq!(sync.book().unwrap().side(Side::Buy).len(), 2);
    }

    #[test]
    fn stale_deltas_are_ignored() {
        let mut sync = live_at(10);
        assert_eq!(
            sync.on_delta(range(5, 10), [up(Side::Buy, 1, 1)]),
            Ok(DeltaOutcome::Stale)
        );
        assert_eq!(best(&sync), (99, 5, 101, 5));
        assert_eq!(sync.stats().stale_deltas, 1);
    }

    #[test]
    fn crossed_book_invalidates() {
        let mut sync = live_at(10);
        assert_eq!(
            sync.on_delta(range(11, 11), [up(Side::Buy, 101, 1)]),
            Err(SyncError::Crossed {
                bid: Price::new(101),
                ask: Price::new(101)
            })
        );
        assert!(!sync.is_live());
        let mut fresh = BookSync::new(CONFIG).unwrap();
        assert!(matches!(
            fresh.on_snapshot(1, [lvl(102, 1)], [lvl(101, 1)]),
            Err(SyncError::Crossed { .. })
        ));
        assert!(!fresh.is_live());
    }

    #[test]
    fn invalid_data_invalidates() {
        let mut sync = live_at(10);
        assert!(matches!(
            sync.on_delta(range(11, 11), [up(Side::Buy, 98, -1)]),
            Err(SyncError::Book(BookError::InvalidQty { .. }))
        ));
        assert!(!sync.is_live());
        assert!(matches!(
            sync.on_snapshot(1, [lvl(99, 1), lvl(99, 2)], []),
            Err(SyncError::Book(BookError::DuplicateLevel { .. }))
        ));
        assert!(!sync.is_live());
    }

    #[test]
    fn broken_buffer_chain_keeps_only_the_newest_part() {
        let mut sync = BookSync::new(CONFIG).unwrap();
        sync.on_delta(range(1, 2), [up(Side::Buy, 1, 1)]).unwrap();
        sync.on_delta(range(5, 6), [up(Side::Buy, 99, 2)]).unwrap();
        assert_eq!(sync.stats().buffer_resets, 1);
        sync.on_snapshot(5, [lvl(99, 5)], [lvl(101, 5)]).unwrap();
        assert_eq!(best(&sync), (99, 2, 101, 5));
    }

    #[test]
    fn buffer_overflow_restarts_the_chain() {
        let mut sync = BookSync::new(SyncConfig {
            buffered_messages: 2,
            ..CONFIG
        })
        .unwrap();
        for seq in 1..=3 {
            sync.on_delta(SeqRange::single(seq), [up(Side::Buy, 99, 1)])
                .unwrap();
        }
        assert_eq!(sync.stats().buffer_resets, 1);
        // Only message 3 is kept, so a snapshot at 1 is too old.
        assert!(matches!(
            sync.on_snapshot(1, [lvl(99, 5)], [lvl(101, 5)]),
            Err(SyncError::SnapshotTooOld {
                first_buffered: 3,
                ..
            })
        ));
        let too_many = (0..40).map(|i| up(Side::Buy, 10 + i, 1));
        assert_eq!(
            sync.on_delta(SeqRange::single(4), too_many),
            Ok(DeltaOutcome::Buffered)
        );
        assert_eq!(sync.stats().buffer_resets, 2);
    }

    #[test]
    fn older_snapshot_while_live_is_ignored() {
        let mut sync = live_at(10);
        sync.on_delta(range(11, 12), [up(Side::Buy, 99, 8)])
            .unwrap();
        assert_eq!(sync.on_snapshot(11, [lvl(90, 1)], [lvl(110, 1)]), Ok(false));
        assert_eq!(best(&sync), (99, 8, 101, 5));
        assert_eq!(sync.last_seq(), Some(12));
        // Same or newer replaces the book.
        assert_eq!(sync.on_snapshot(12, [lvl(90, 1)], [lvl(110, 1)]), Ok(true));
        assert_eq!(best(&sync), (90, 1, 110, 1));
    }

    #[test]
    fn reset_waits_for_new_snapshot() {
        let mut sync = live_at(10);
        sync.reset();
        assert!(sync.book().is_none());
        assert_eq!(sync.last_seq(), None);
    }
}
