//! Replaying a journaled Binance market-data session through the same book logic as the live
//! session, for post-mortems and regression tests.
//!
//! A journal is self-describing: each session starts with a [`Kind::SESSION_START`] record
//! holding the instrument rules and sizing that affect book behavior (see [`describe`]). Replay
//! then feeds every journaled message, snapshot and reset to [`MdBooks`] in order, so the
//! handler sees exactly the books and status changes the live session produced.

use core::fmt::Write as _;

use bowst_book::SyncConfig;
use bowst_core::{Dec, Increment, Instrument, InstrumentId, InstrumentTable, Symbol, VenueId};
use bowst_journal::format::Kind;
use bowst_journal::{JournalReader, ReadError};

use super::books::MdBooks;
use super::md::MdHandler;

/// First line of a session description, with its format version.
const HEADER_LINE: &str = "bowst binance-md session v1";

/// Sizing that changes how books behave, recorded so replay matches the live run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BookSizing {
    /// Book and delta-buffer sizes.
    pub sync: SyncConfig,
    /// Largest diff-depth message accepted.
    pub max_levels_per_message: usize,
    /// Levels per side requested in snapshots.
    pub snapshot_limit: u32,
}

/// Text written as the payload of the session-start record.
#[must_use]
pub fn describe(sizing: &BookSizing, instruments: &InstrumentTable) -> String {
    let mut out = String::new();
    // Writing to a String cannot fail.
    let _ = writeln!(out, "{HEADER_LINE}");
    let _ = writeln!(
        out,
        "sizing {} {} {} {} {}",
        sizing.sync.levels_per_side,
        sizing.sync.buffered_messages,
        sizing.sync.buffered_updates,
        sizing.max_levels_per_message,
        sizing.snapshot_limit
    );
    for i in instruments.iter() {
        let _ = writeln!(
            out,
            "instrument {} {} {} {} {}",
            i.id.get(),
            i.symbol,
            i.tick,
            i.lot,
            i.min_notional
        );
    }
    out
}

/// Parses a session description written by [`describe`].
///
/// # Errors
/// [`ReplayError::BadSession`] naming the first line that could not be read.
pub fn parse_description(text: &str) -> Result<(BookSizing, InstrumentTable), ReplayError> {
    let bad = |line: &str| ReplayError::BadSession(line.chars().take(80).collect());
    let mut lines = text.lines();
    if lines.next() != Some(HEADER_LINE) {
        return Err(bad(text.lines().next().unwrap_or_default()));
    }
    let mut sizing = None;
    let mut instruments = Vec::new();
    for line in lines {
        let fields: Vec<&str> = line.split(' ').collect();
        match fields.as_slice() {
            ["sizing", levels, messages, updates, per_message, limit] => {
                let number = |s: &str| s.parse::<usize>().map_err(|_| bad(line));
                sizing = Some(BookSizing {
                    sync: SyncConfig {
                        levels_per_side: number(levels)?,
                        buffered_messages: number(messages)?,
                        buffered_updates: number(updates)?,
                    },
                    max_levels_per_message: number(per_message)?,
                    snapshot_limit: limit.parse().map_err(|_| bad(line))?,
                });
            }
            ["instrument", id, symbol, tick, lot, min_notional] => {
                instruments.push(Instrument {
                    id: InstrumentId::new(id.parse().map_err(|_| bad(line))?),
                    venue: VenueId::Binance,
                    symbol: Symbol::new(symbol).ok_or_else(|| bad(line))?,
                    tick: Increment::parse(tick).map_err(|_| bad(line))?,
                    lot: Increment::parse(lot).map_err(|_| bad(line))?,
                    min_notional: Dec::parse(min_notional).map_err(|_| bad(line))?,
                });
            }
            [""] => {}
            _ => return Err(bad(line)),
        }
    }
    let sizing = sizing.ok_or_else(|| bad("missing sizing line"))?;
    let table = InstrumentTable::new(instruments).map_err(|_| bad("invalid instrument list"))?;
    Ok((sizing, table))
}

/// Why a journal could not be replayed.
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// The journal could not be read.
    #[error(transparent)]
    Read(#[from] ReadError),
    /// A market-data record appeared before any session-start record.
    #[error("market-data record before the session start")]
    NoSession,
    /// A session-start record could not be parsed.
    #[error("unreadable session description: {0:?}")]
    BadSession(String),
    /// A snapshot record is shorter than its instrument ID.
    #[error("malformed snapshot record")]
    BadSnapshot,
    /// The books could not be allocated with the recorded sizing.
    #[error("recorded sizing is invalid")]
    InvalidSizing,
}

/// What a replay went through.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplayReport {
    /// Sessions in the journal.
    pub sessions: u64,
    /// Market-data messages replayed.
    pub messages: u64,
    /// Snapshots replayed.
    pub snapshots: u64,
    /// Connection resets replayed.
    pub resets: u64,
    /// Records the live session failed to journal (from gap markers). Non-zero means the
    /// replay is not exact from that point on.
    pub dropped: u64,
    /// Whether the journal ended with a torn record (the live process did not shut down
    /// cleanly).
    pub torn_tail: bool,
}

/// Replays every session in a journal into `handler`.
///
/// # Errors
/// [`ReplayError`] if the journal is unreadable or inconsistent.
pub fn replay(
    reader: &mut JournalReader,
    handler: &mut impl MdHandler,
) -> Result<ReplayReport, ReplayError> {
    let mut report = ReplayReport::default();
    let mut books: Option<MdBooks> = None;
    while let Some(record) = reader.next_record()? {
        let header = record.header;
        match header.kind {
            Kind::SESSION_START => {
                let text = core::str::from_utf8(record.payload)
                    .map_err(|_| ReplayError::BadSession("not UTF-8".into()))?;
                let (sizing, instruments) = parse_description(text)?;
                let snapshot_levels = usize::try_from(sizing.snapshot_limit).unwrap_or(usize::MAX);
                books = Some(
                    MdBooks::new(
                        instruments,
                        sizing.sync,
                        sizing.max_levels_per_message,
                        snapshot_levels,
                    )
                    .map_err(|()| ReplayError::InvalidSizing)?,
                );
                report.sessions = report.sessions.saturating_add(1);
            }
            Kind::MD_MESSAGE => {
                let books = books.as_mut().ok_or(ReplayError::NoSession)?;
                // An undecodable message made the live session drop the connection; the
                // journal's following reset record replays that.
                let _ = books.on_message(record.payload, handler);
                report.messages = report.messages.saturating_add(1);
            }
            Kind::MD_SNAPSHOT => {
                let books = books.as_mut().ok_or(ReplayError::NoSession)?;
                let (id, body) = record
                    .payload
                    .split_first_chunk::<4>()
                    .ok_or(ReplayError::BadSnapshot)?;
                let instrument = InstrumentId::new(u32::from_le_bytes(*id));
                books.on_raw_snapshot(instrument, body, header.wall, handler);
                report.snapshots = report.snapshots.saturating_add(1);
            }
            Kind::MD_RESET => {
                books.as_mut().ok_or(ReplayError::NoSession)?.reset_all();
                report.resets = report.resets.saturating_add(1);
            }
            Kind::GAP => {
                let count = record
                    .payload
                    .first_chunk::<8>()
                    .map_or(0, |bytes| u64::from_le_bytes(*bytes));
                report.dropped = report.dropped.saturating_add(count);
            }
            // Status text and kinds from other components are for humans and other tools.
            _ => {}
        }
    }
    report.torn_tail = reader.torn_tail();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> InstrumentTable {
        InstrumentTable::new(vec![Instrument {
            id: InstrumentId::new(0),
            venue: VenueId::Binance,
            symbol: Symbol::new("BTCUSDT").unwrap(),
            tick: Increment::parse("0.01").unwrap(),
            lot: Increment::parse("0.00001").unwrap(),
            min_notional: Dec::parse("5").unwrap(),
        }])
        .unwrap()
    }

    const SIZING: BookSizing = BookSizing {
        sync: SyncConfig {
            levels_per_side: 5_000,
            buffered_messages: 4_096,
            buffered_updates: 262_144,
        },
        max_levels_per_message: 20_000,
        snapshot_limit: 1_000,
    };

    #[test]
    fn description_round_trips() {
        let text = describe(&SIZING, &table());
        assert!(
            text.starts_with("bowst binance-md session v1\nsizing 5000 4096 262144 20000 1000\n")
        );
        assert!(text.contains("instrument 0 BTCUSDT 0.01 0.00001 5"));
        let (sizing, instruments) = parse_description(&text).unwrap();
        assert_eq!(sizing, SIZING);
        assert_eq!(
            instruments.get(InstrumentId::new(0)),
            table().get(InstrumentId::new(0))
        );
    }

    #[test]
    fn rejects_unknown_or_malformed_descriptions() {
        let good = describe(&SIZING, &table());
        for bad in [
            good.replace("v1", "v2"),
            good.replace("sizing 5000", "sizing x"),
            good.replace("instrument 0", "instrument -1"),
            good.replace("0.01 ", "0 "),
            good.replace("sizing 5000 4096 262144 20000 1000\n", ""),
            format!("{good}surprise line\n"),
        ] {
            assert!(parse_description(&bad).is_err(), "accepted {bad:?}");
        }
    }
}
