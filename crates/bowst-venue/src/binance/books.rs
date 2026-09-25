//! The deterministic core of Binance market data: decoding and book maintenance, with no I/O,
//! clocks or threads. The live session ([`super::md`]) and journal replay ([`super::replay`])
//! both drive this same code, so a replay reproduces a live run exactly.

use bowst_book::{BookSync, DeltaOutcome, Level, SyncConfig, SyncError};
use bowst_core::{InstrumentId, InstrumentTable, WallTime};

use super::depth::{DepthSnapshotDecoder, DepthUpdateDecoder};
use super::md::{MdHandler, MdStats, MdStatus};

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

/// Every configured instrument's book, plus the decoders that feed them.
#[derive(Debug)]
pub struct MdBooks {
    instruments: InstrumentTable,
    syncs: Vec<BookSync>,
    decoder: DepthUpdateDecoder,
    snapshot_decoder: DepthSnapshotDecoder,
    stats: MdStats,
}

impl MdBooks {
    /// Allocates every book and decoder buffer.
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

    /// Decodes and applies one stream message. Hot path: no allocation on success.
    ///
    /// Returns `Ok(Some(instrument))` if that instrument's book went down (it needs a new
    /// snapshot).
    ///
    /// # Errors
    /// A description, when the message cannot be attributed to an instrument. The connection
    /// must then be dropped, since the state of every book is unknown.
    pub fn on_message(
        &mut self,
        message: &[u8],
        handler: &mut impl MdHandler,
    ) -> Result<Option<InstrumentId>, String> {
        self.stats.messages = self.stats.messages.saturating_add(1);
        let update = match self.decoder.decode(message, &self.instruments) {
            Ok(update) => update,
            Err(err) => return Err(format!("undecodable message: {err}")),
        };
        let Some(sync) = self.syncs.get_mut(index_of(update.instrument)) else {
            return Err("message for an unconfigured instrument".into());
        };
        match sync.on_delta(update.range, update.updates.iter().copied()) {
            Ok(DeltaOutcome::Applied) => {
                self.stats.deltas_applied = self.stats.deltas_applied.saturating_add(1);
                if let Some(book) = sync.book() {
                    handler.on_book(update.instrument, book, update.event_time);
                }
                Ok(None)
            }
            Ok(DeltaOutcome::Buffered | DeltaOutcome::Stale) => Ok(None),
            Err(err) => {
                self.stats.book_invalidations = self.stats.book_invalidations.saturating_add(1);
                handler.on_status(MdStatus::InstrumentDown {
                    instrument: update.instrument,
                    reason: err.to_string(),
                });
                Ok(Some(update.instrument))
            }
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

    /// Takes every book down (the connection ended).
    pub fn reset_all(&mut self) {
        for sync in &mut self.syncs {
            sync.reset();
        }
    }
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
