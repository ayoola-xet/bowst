//! The deterministic core of Binance market data: decoding, book maintenance and book
//! verification, with no I/O, clocks or threads. The live session ([`super::md`]) and journal
//! replay ([`super::replay`]) both drive this same code, so a replay reproduces a live run
//! exactly.
//!
//! **Verification.** A live book is only as good as the chain of deltas since its snapshot.
//! To prove the chain is right, the session periodically rebuilds one instrument's book
//! independently: [`start_verification`](MdBooks::start_verification) starts a second
//! ("shadow") sync that buffers the instrument's deltas, a fresh snapshot is loaded into it
//! with [`on_verification_snapshot`](MdBooks::on_verification_snapshot), and as soon as both
//! books have applied the same last update, their top [`VERIFY_LEVELS`] levels per side are
//! compared. A difference takes the live book down (fail closed) and it resynchronizes.

use core::fmt;

use bowst_book::{Book, BookSync, DeltaOutcome, Level, SyncConfig, SyncError};
use bowst_core::{InstrumentId, InstrumentTable, Side, WallTime};

use super::depth::{DepthSnapshotDecoder, DepthUpdateDecoder};
use super::md::{MdHandler, MdStats, MdStatus};

/// Levels per side compared when verifying a book against a fresh snapshot. Well inside the
/// snapshot depth, so levels near the snapshot's edge (which the live book may know more
/// about) are never compared.
pub const VERIFY_LEVELS: usize = 100;

/// What applying a snapshot did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotOutcome {
    /// The book is now live.
    Live,
    /// Nothing changed: the snapshot is older than the already-live book.
    Unchanged,
    /// The stream has moved past this snapshot; a newer one is needed now.
    TooOld,
    /// The snapshot was invalid or produced a crossed book; the book is down.
    Invalid,
}

/// The result of applying one stream message, before the handler hears about it. See
/// [`MdBooks::apply`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// Nothing for the handler (buffered while awaiting a snapshot, or stale).
    Quiet,
    /// The instrument's live book changed.
    Book {
        /// Which instrument.
        instrument: InstrumentId,
        /// Venue event time of the message.
        event_time: WallTime,
    },
    /// The instrument's book went down and needs a new snapshot.
    Down {
        /// Which instrument.
        instrument: InstrumentId,
        /// Why.
        reason: DownReason,
    },
}

/// Why a book went down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownReason {
    /// A gap, invalid data or a crossed book.
    Sync(SyncError),
    /// Verification found the live book differs from one rebuilt from a fresh snapshot.
    Mismatch(Mismatch),
}

impl fmt::Display for DownReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sync(err) => err.fmt(f),
            Self::Mismatch(m) => m.fmt(f),
        }
    }
}

/// Where a verified book first differed from the book rebuilt from a fresh snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mismatch {
    /// Book side.
    pub side: Side,
    /// Level (0 is the best) where they first differ.
    pub level: usize,
    /// Update ID both books had reached.
    pub update_id: u64,
    /// The live book's level there, if any.
    pub live: Option<Level>,
    /// The rebuilt book's level there, if any.
    pub rebuilt: Option<Level>,
}

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "book differs from a fresh snapshot at update {}: {:?} level {} is {:?}, expected {:?}",
            self.update_id, self.side, self.level, self.live, self.rebuilt
        )
    }
}

/// The result of loading a verification snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verification {
    /// No verification of this instrument is in progress (it was cancelled, for example by a
    /// gap or a reconnect); nothing was done.
    NotRunning,
    /// Loaded; the comparison happens once the live book reaches the snapshot's update.
    Pending,
    /// Compared: the books match.
    Passed,
    /// Compared: the books differ. The live book was taken down.
    Failed(Mismatch),
    /// The snapshot could not be used (too old for the buffered deltas, or invalid). No
    /// verdict; verification ended and should be retried later.
    Unusable,
}

/// An in-progress verification of one instrument.
#[derive(Debug)]
struct Shadow {
    instrument: Option<InstrumentId>,
    sync: BookSync,
    /// A snapshot was loaded; compare as soon as both books are at the same update.
    loaded: bool,
}

/// Every configured instrument's book, plus the decoders that feed them.
#[derive(Debug)]
pub struct MdBooks {
    instruments: InstrumentTable,
    syncs: Vec<BookSync>,
    shadow: Shadow,
    decoder: DepthUpdateDecoder,
    snapshot_decoder: DepthSnapshotDecoder,
    stats: MdStats,
}

impl MdBooks {
    /// Allocates every book and decoder buffer, including the verification book.
    ///
    /// # Errors
    /// `Err(())` if a configured size is zero.
    #[allow(clippy::result_unit_err)] // Callers map this to their own configuration error.
    pub fn new(
        instruments: InstrumentTable,
        sync: SyncConfig,
        max_levels_per_message: usize,
        snapshot_levels: usize,
    ) -> Result<Self, ()> {
        let syncs = instruments
            .iter()
            .map(|_| BookSync::new(sync))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ())?;
        Ok(Self {
            instruments,
            syncs,
            shadow: Shadow {
                instrument: None,
                sync: BookSync::new(sync).map_err(|_| ())?,
                loaded: false,
            },
            decoder: DepthUpdateDecoder::new(max_levels_per_message),
            snapshot_decoder: DepthSnapshotDecoder::new(snapshot_levels),
            stats: MdStats::default(),
        })
    }

    /// The configured instruments.
    #[must_use]
    pub fn instruments(&self) -> &InstrumentTable {
        &self.instruments
    }

    /// Counters so far.
    #[must_use]
    pub fn stats(&self) -> MdStats {
        self.stats
    }

    pub(crate) fn stats_mut(&mut self) -> &mut MdStats {
        &mut self.stats
    }

    /// Whether an instrument's book is live.
    #[must_use]
    pub fn is_live(&self, instrument: InstrumentId) -> bool {
        self.syncs
            .get(index_of(instrument))
            .is_some_and(BookSync::is_live)
    }

    /// An instrument's book, while live.
    #[must_use]
    pub fn book(&self, instrument: InstrumentId) -> Option<&Book> {
        self.syncs
            .get(index_of(instrument))
            .and_then(BookSync::book)
    }

    /// The last update ID applied to an instrument's book, while live.
    #[must_use]
    pub fn last_update_id(&self, instrument: InstrumentId) -> Option<u64> {
        self.syncs
            .get(index_of(instrument))
            .and_then(BookSync::last_seq)
    }

    /// The instrument being verified, if any.
    #[must_use]
    pub fn verifying(&self) -> Option<InstrumentId> {
        self.shadow.instrument
    }

    /// Decodes and applies one stream message, then tells the handler. Equivalent to
    /// [`apply`](Self::apply) followed by [`report`](Self::report).
    ///
    /// # Errors
    /// As [`apply`](Self::apply).
    pub fn on_message(
        &mut self,
        message: &[u8],
        handler: &mut impl MdHandler,
    ) -> Result<Applied, String> {
        let applied = self.apply(message)?;
        self.report(applied, handler);
        Ok(applied)
    }

    /// Decodes and applies one stream message without calling the handler, so decode and
    /// book-update time can be measured on its own. Hot path: no allocation on success.
    ///
    /// # Errors
    /// A description, when the message cannot be attributed to an instrument. The connection
    /// must then be dropped, since the state of every book is unknown.
    pub fn apply(&mut self, message: &[u8]) -> Result<Applied, String> {
        self.stats.messages = self.stats.messages.saturating_add(1);
        let update = match self.decoder.decode(message, &self.instruments) {
            Ok(update) => update,
            Err(err) => return Err(format!("undecodable message: {err}")),
        };
        let (instrument, event_time) = (update.instrument, update.event_time);
        let Some(sync) = self.syncs.get_mut(index_of(instrument)) else {
            return Err("message for an unconfigured instrument".into());
        };
        let outcome = sync.on_delta(update.range, update.updates.iter().copied());
        let verifying = self.shadow.instrument == Some(instrument);
        match outcome {
            Ok(DeltaOutcome::Applied) => {
                self.stats.deltas_applied = self.stats.deltas_applied.saturating_add(1);
                if verifying {
                    // The shadow's own gaps and staleness are irrelevant: it only matters
                    // whether it ends up at the same update as the live book.
                    let _ = self
                        .shadow
                        .sync
                        .on_delta(update.range, update.updates.iter().copied());
                    if let Some(mismatch) = self.settle_verification() {
                        return Ok(Applied::Down {
                            instrument,
                            reason: DownReason::Mismatch(mismatch),
                        });
                    }
                }
                Ok(Applied::Book {
                    instrument,
                    event_time,
                })
            }
            Ok(DeltaOutcome::Buffered | DeltaOutcome::Stale) => {
                if verifying {
                    let _ = self
                        .shadow
                        .sync
                        .on_delta(update.range, update.updates.iter().copied());
                }
                Ok(Applied::Quiet)
            }
            Err(err) => {
                if verifying {
                    self.cancel_verification();
                }
                self.stats.book_invalidations = self.stats.book_invalidations.saturating_add(1);
                Ok(Applied::Down {
                    instrument,
                    reason: DownReason::Sync(err),
                })
            }
        }
    }

    /// Tells the handler what [`apply`](Self::apply) did.
    pub fn report(&self, applied: Applied, handler: &mut impl MdHandler) {
        match applied {
            Applied::Quiet => {}
            Applied::Book {
                instrument,
                event_time,
            } => {
                if let Some(book) = self.book(instrument) {
                    handler.on_book(instrument, book, event_time);
                }
            }
            Applied::Down { instrument, reason } => handler.on_status(MdStatus::InstrumentDown {
                instrument,
                reason: reason.to_string(),
            }),
        }
    }

    /// Applies a decoded snapshot.
    pub fn on_snapshot(
        &mut self,
        instrument: InstrumentId,
        last_update_id: u64,
        bids: &[Level],
        asks: &[Level],
        fetched_at: WallTime,
        handler: &mut impl MdHandler,
    ) -> SnapshotOutcome {
        let Some(sync) = self.syncs.get_mut(index_of(instrument)) else {
            return SnapshotOutcome::Invalid;
        };
        let result = sync.on_snapshot(last_update_id, bids.iter().copied(), asks.iter().copied());
        settle(
            &mut self.stats,
            sync,
            instrument,
            result,
            fetched_at,
            handler,
        )
    }

    /// Decodes a raw REST snapshot body (as journaled) and applies it.
    pub fn on_raw_snapshot(
        &mut self,
        instrument: InstrumentId,
        body: &[u8],
        fetched_at: WallTime,
        handler: &mut impl MdHandler,
    ) -> SnapshotOutcome {
        let Some(meta) = self.instruments.get(instrument).copied() else {
            return SnapshotOutcome::Invalid;
        };
        let Some(sync) = self.syncs.get_mut(index_of(instrument)) else {
            return SnapshotOutcome::Invalid;
        };
        let result = match self.snapshot_decoder.decode(body, &meta) {
            Ok(snapshot) => sync.on_snapshot(
                snapshot.last_update_id,
                snapshot.bids.iter().copied(),
                snapshot.asks.iter().copied(),
            ),
            Err(err) => {
                handler.on_status(MdStatus::InstrumentDown {
                    instrument,
                    reason: format!("snapshot decode: {err}"),
                });
                return SnapshotOutcome::Invalid;
            }
        };
        settle(
            &mut self.stats,
            sync,
            instrument,
            result,
            fetched_at,
            handler,
        )
    }

    /// Starts verifying `instrument`: from now on its deltas also feed the verification book,
    /// until [`on_verification_snapshot`](Self::on_verification_snapshot) loads a snapshot
    /// into it and the two books are compared. Replaces any verification in progress.
    ///
    /// Returns `false`, and starts nothing, if the instrument's book is not live.
    pub fn start_verification(&mut self, instrument: InstrumentId) -> bool {
        if !self.is_live(instrument) {
            return false;
        }
        self.shadow.sync.reset();
        self.shadow.instrument = Some(instrument);
        self.shadow.loaded = false;
        true
    }

    /// Ends any verification in progress without a verdict.
    pub fn cancel_verification(&mut self) {
        if self.shadow.instrument.take().is_some() {
            self.shadow.sync.reset();
            self.shadow.loaded = false;
        }
    }

    /// Loads a decoded snapshot into the verification book, comparing right away if the live
    /// book is at the same update. A mismatch takes the live book down and reports it.
    pub fn on_verification_snapshot(
        &mut self,
        instrument: InstrumentId,
        last_update_id: u64,
        bids: &[Level],
        asks: &[Level],
        handler: &mut impl MdHandler,
    ) -> Verification {
        if self.shadow.instrument != Some(instrument) || self.shadow.loaded {
            return Verification::NotRunning;
        }
        let result = self.shadow.sync.on_snapshot(
            last_update_id,
            bids.iter().copied(),
            asks.iter().copied(),
        );
        self.after_verification_snapshot(instrument, result.is_ok(), handler)
    }

    /// Decodes a raw REST snapshot body (as journaled) and loads it into the verification
    /// book. See [`on_verification_snapshot`](Self::on_verification_snapshot).
    pub fn on_raw_verification_snapshot(
        &mut self,
        instrument: InstrumentId,
        body: &[u8],
        handler: &mut impl MdHandler,
    ) -> Verification {
        if self.shadow.instrument != Some(instrument) || self.shadow.loaded {
            return Verification::NotRunning;
        }
        let Some(meta) = self.instruments.get(instrument).copied() else {
            return Verification::NotRunning;
        };
        let loaded = match self.snapshot_decoder.decode(body, &meta) {
            Ok(snapshot) => self
                .shadow
                .sync
                .on_snapshot(
                    snapshot.last_update_id,
                    snapshot.bids.iter().copied(),
                    snapshot.asks.iter().copied(),
                )
                .is_ok(),
            Err(_) => false,
        };
        self.after_verification_snapshot(instrument, loaded, handler)
    }

    fn after_verification_snapshot(
        &mut self,
        instrument: InstrumentId,
        loaded: bool,
        handler: &mut impl MdHandler,
    ) -> Verification {
        if !loaded {
            self.cancel_verification();
            return Verification::Unusable;
        }
        self.shadow.loaded = true;
        match self.settle_verification() {
            Some(mismatch) => {
                self.report(
                    Applied::Down {
                        instrument,
                        reason: DownReason::Mismatch(mismatch),
                    },
                    handler,
                );
                Verification::Failed(mismatch)
            }
            None if self.shadow.instrument.is_none() => Verification::Passed,
            None => Verification::Pending,
        }
    }

    /// Compares the books if a snapshot is loaded and both are at the same update. Ends the
    /// verification when compared; on a mismatch, also takes the live book down.
    fn settle_verification(&mut self) -> Option<Mismatch> {
        let instrument = self.shadow.instrument?;
        if !self.shadow.loaded {
            return None;
        }
        let live = self.syncs.get(index_of(instrument))?;
        let (Some(update_id), Some(shadow_seq)) = (live.last_seq(), self.shadow.sync.last_seq())
        else {
            return None;
        };
        if update_id != shadow_seq {
            return None;
        }
        let verdict = match (live.book(), self.shadow.sync.book()) {
            (Some(live), Some(rebuilt)) => compare(live, rebuilt, update_id),
            _ => None,
        };
        self.cancel_verification();
        match verdict {
            None => {
                self.stats.verifications_passed = self.stats.verifications_passed.saturating_add(1);
                None
            }
            Some(mismatch) => {
                self.stats.verification_mismatches =
                    self.stats.verification_mismatches.saturating_add(1);
                self.stats.book_invalidations = self.stats.book_invalidations.saturating_add(1);
                if let Some(sync) = self.syncs.get_mut(index_of(instrument)) {
                    sync.reset();
                }
                Some(mismatch)
            }
        }
    }

    /// Takes every book down (the connection ended). Ends any verification.
    pub fn reset_all(&mut self) {
        for sync in &mut self.syncs {
            sync.reset();
        }
        self.cancel_verification();
    }
}

/// The first difference in the top [`VERIFY_LEVELS`] levels of either side, if any.
fn compare(live: &Book, rebuilt: &Book, update_id: u64) -> Option<Mismatch> {
    for side in [Side::Buy, Side::Sell] {
        let (a, b) = (live.side(side), rebuilt.side(side));
        for level in 0..VERIFY_LEVELS {
            let (x, y) = (a.nth_best(level), b.nth_best(level));
            if x != y {
                return Some(Mismatch {
                    side,
                    level,
                    update_id,
                    live: x,
                    rebuilt: y,
                });
            }
            if x.is_none() {
                break;
            }
        }
    }
    None
}

/// Turns a snapshot result into an outcome, reporting status changes. Shared by the decoded
/// (live) and raw (replay) snapshot paths so both behave identically.
fn settle(
    stats: &mut MdStats,
    sync: &BookSync,
    instrument: InstrumentId,
    result: Result<bool, SyncError>,
    fetched_at: WallTime,
    handler: &mut impl MdHandler,
) -> SnapshotOutcome {
    match result {
        Ok(true) => {
            if let Some(book) = sync.book() {
                stats.snapshots_applied = stats.snapshots_applied.saturating_add(1);
                handler.on_status(MdStatus::InstrumentLive(instrument));
                handler.on_book(instrument, book, fetched_at);
            }
            SnapshotOutcome::Live
        }
        Ok(false) => SnapshotOutcome::Unchanged,
        Err(SyncError::SnapshotTooOld { .. }) => SnapshotOutcome::TooOld,
        Err(err) => {
            stats.book_invalidations = stats.book_invalidations.saturating_add(1);
            handler.on_status(MdStatus::InstrumentDown {
                instrument,
                reason: err.to_string(),
            });
            SnapshotOutcome::Invalid
        }
    }
}

pub(crate) fn index_of(id: InstrumentId) -> usize {
    usize::try_from(id.get()).unwrap_or(usize::MAX)
}
