//! Binance Spot diff-depth updates and depth snapshots.
//!
//! Stream messages (`<symbol>@depth@100ms`) carry the range of book update IDs they cover
//! (`U`..=`u`) and the changed levels. REST snapshots (`GET /api/v3/depth`) carry the
//! `lastUpdateId` they were taken at. Both map directly onto [`BookSync`]:
//! `on_delta(SeqRange(U, u), updates)` and `on_snapshot(lastUpdateId, bids, asks)`.
//!
//! [`BookSync`]: bowst_book::BookSync

use bowst_book::{Level, LevelUpdate, SeqRange};
use bowst_core::instrument::Instrument;
use bowst_core::{InstrumentId, InstrumentTable, Side, Symbol, VenueId, WallTime};

use crate::DecodeError;
use crate::json::Reader;
use crate::levels::{read_levels_into, read_updates_into};

/// One decoded diff-depth update. `updates` borrows the decoder's buffer.
#[derive(Debug, PartialEq, Eq)]
pub struct DepthUpdate<'d> {
    /// Instrument the update is for.
    pub instrument: InstrumentId,
    /// Venue event time.
    pub event_time: WallTime,
    /// Book update IDs covered (`U`..=`u`).
    pub range: SeqRange,
    /// Changed levels, bids first. A zero quantity removes the level.
    pub updates: &'d [LevelUpdate],
}

/// Decodes diff-depth messages into a buffer allocated once.
#[derive(Debug)]
pub struct DepthUpdateDecoder {
    updates: Vec<LevelUpdate>,
    limit: usize,
}

/// Fields collected while walking a message, in whatever order they appear.
#[derive(Default)]
struct Fields<'a> {
    event_seen: bool,
    event_time: Option<u64>,
    instrument: Option<Instrument>,
    first: Option<u64>,
    last: Option<u64>,
    /// Side arrays seen before the symbol, kept as the rest of the message to re-read.
    deferred: [Option<&'a [u8]>; 2],
    sides_read: [bool; 2],
}

const SIDES: [(Side, &str); 2] = [(Side::Buy, "b"), (Side::Sell, "a")];

impl DepthUpdateDecoder {
    /// Creates a decoder that accepts up to `max_levels` changed levels per message.
    #[must_use]
    pub fn new(max_levels: usize) -> Self {
        Self {
            updates: Vec::with_capacity(max_levels),
            limit: max_levels,
        }
    }

    /// Decodes one message, either a combined-stream envelope (`{"stream":..,"data":{..}}`)
    /// or a raw payload.
    ///
    /// # Errors
    /// Any [`DecodeError`]. The message must then be treated as lost.
    pub fn decode<'d>(
        &'d mut self,
        message: &[u8],
        instruments: &InstrumentTable,
    ) -> Result<DepthUpdate<'d>, DecodeError> {
        self.updates.clear();
        let mut fields = Fields::default();
        let mut r = Reader::new(message);
        self.read_object(&mut r, message, instruments, &mut fields, true)?;
        r.finish()?;

        let instrument = fields.instrument.ok_or(DecodeError::MissingField("s"))?;
        for ((side, name), (deferred, read)) in SIDES
            .into_iter()
            .zip(fields.deferred.into_iter().zip(fields.sides_read))
        {
            match (deferred, read) {
                (Some(rest), _) => self.read_side(&mut Reader::new(rest), side, &instrument)?,
                (None, true) => {}
                (None, false) => return Err(DecodeError::MissingField(name)),
            }
        }
        if !fields.event_seen {
            return Err(DecodeError::MissingField("e"));
        }
        let first = fields.first.ok_or(DecodeError::MissingField("U"))?;
        let last = fields.last.ok_or(DecodeError::MissingField("u"))?;
        let range =
            SeqRange::new(first, last).ok_or(DecodeError::InvalidSequence { first, last })?;
        let event_time = fields
            .event_time
            .and_then(WallTime::from_unix_millis)
            .ok_or(DecodeError::MissingField("E"))?;
        Ok(DepthUpdate {
            instrument: instrument.id,
            event_time,
            range,
            updates: &self.updates,
        })
    }

    fn read_object<'a>(
        &mut self,
        r: &mut Reader<'a>,
        message: &'a [u8],
        instruments: &InstrumentTable,
        fields: &mut Fields<'a>,
        envelope_allowed: bool,
    ) -> Result<(), DecodeError> {
        r.begin_object()?;
        while let Some(key) = r.next_key()? {
            match key {
                "data" if envelope_allowed => {
                    self.read_object(r, message, instruments, fields, false)?;
                }
                "e" => {
                    if r.str()? != "depthUpdate" {
                        return Err(DecodeError::UnexpectedValue("e"));
                    }
                    fields.event_seen = true;
                }
                "E" => fields.event_time = Some(r.u64()?),
                "s" => {
                    let symbol = r.str()?;
                    let instrument = instruments
                        .find(VenueId::Binance, symbol)
                        .ok_or(DecodeError::UnknownSymbol(Symbol::new(symbol)))?;
                    fields.instrument = Some(*instrument);
                }
                "U" => fields.first = Some(r.u64()?),
                "u" => fields.last = Some(r.u64()?),
                "b" | "a" => {
                    let index = usize::from(key == "a");
                    let already = fields.sides_read.get(index).copied().unwrap_or(true)
                        || fields.deferred.get(index).is_some_and(Option::is_some);
                    if already {
                        // A repeated side would apply its levels twice.
                        return Err(DecodeError::UnexpectedValue(if index == 0 {
                            "b"
                        } else {
                            "a"
                        }));
                    }
                    if let Some(instrument) = fields.instrument {
                        let side = SIDES.get(index).map_or(Side::Buy, |(side, _)| *side);
                        self.read_side(r, side, &instrument)?;
                        if let Some(read) = fields.sides_read.get_mut(index) {
                            *read = true;
                        }
                    } else {
                        // Symbol not seen yet: remember where the array starts, read it later.
                        let rest = message.get(r.position()..).unwrap_or_default();
                        if let Some(slot) = fields.deferred.get_mut(index) {
                            *slot = Some(rest);
                        }
                        r.skip()?;
                    }
                }
                _ => r.skip()?,
            }
        }
        Ok(())
    }

    fn read_side(
        &mut self,
        r: &mut Reader<'_>,
        side: Side,
        instrument: &Instrument,
    ) -> Result<(), DecodeError> {
        read_updates_into(
            r,
            side,
            instrument.tick,
            instrument.lot,
            self.limit,
            &mut self.updates,
        )
    }
}

/// One decoded depth snapshot. Levels borrow the decoder's buffers.
#[derive(Debug, PartialEq, Eq)]
pub struct DepthSnapshot<'d> {
    /// Book update ID the snapshot was taken at.
    pub last_update_id: u64,
    /// Bid levels as sent (best first).
    pub bids: &'d [Level],
    /// Ask levels as sent (best first).
    pub asks: &'d [Level],
}

/// Decodes `GET /api/v3/depth` responses into buffers allocated once.
#[derive(Debug)]
pub struct DepthSnapshotDecoder {
    bids: Vec<Level>,
    asks: Vec<Level>,
    limit: usize,
}

impl DepthSnapshotDecoder {
    /// Creates a decoder that accepts up to `max_levels` levels per side (Binance serves up
    /// to 5,000).
    #[must_use]
    pub fn new(max_levels: usize) -> Self {
        Self {
            bids: Vec::with_capacity(max_levels),
            asks: Vec::with_capacity(max_levels),
            limit: max_levels,
        }
    }

    /// Decodes a snapshot for `instrument` (the symbol the request was made for; the response
    /// does not repeat it).
    ///
    /// # Errors
    /// Any [`DecodeError`].
    pub fn decode<'d>(
        &'d mut self,
        message: &[u8],
        instrument: &Instrument,
    ) -> Result<DepthSnapshot<'d>, DecodeError> {
        self.bids.clear();
        self.asks.clear();
        let (mut last_update_id, mut seen_bids, mut seen_asks) = (None, false, false);
        let mut r = Reader::new(message);
        r.begin_object()?;
        while let Some(key) = r.next_key()? {
            match key {
                "lastUpdateId" => last_update_id = Some(r.u64()?),
                "bids" => {
                    read_levels_into(
                        &mut r,
                        instrument.tick,
                        instrument.lot,
                        self.limit,
                        &mut self.bids,
                    )?;
                    seen_bids = true;
                }
                "asks" => {
                    read_levels_into(
                        &mut r,
                        instrument.tick,
                        instrument.lot,
                        self.limit,
                        &mut self.asks,
                    )?;
                    seen_asks = true;
                }
                _ => r.skip()?,
            }
        }
        r.finish()?;
        if !seen_bids {
            return Err(DecodeError::MissingField("bids"));
        }
        if !seen_asks {
            return Err(DecodeError::MissingField("asks"));
        }
        Ok(DepthSnapshot {
            last_update_id: last_update_id.ok_or(DecodeError::MissingField("lastUpdateId"))?,
            bids: &self.bids,
            asks: &self.asks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bowst_core::{Dec, Increment, Price, Qty};

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

    const PAYLOAD: &str = r#"{"e":"depthUpdate","E":1790299002614,"s":"BTCUSDT","U":10,"u":12,"b":[["84000.01","0.5"],["83999.99","0"]],"a":[["84000.02","1.25"]]}"#;

    fn up(side: Side, price: i64, qty: i64) -> LevelUpdate {
        LevelUpdate {
            side,
            price: Price::new(price),
            qty: Qty::new(qty),
        }
    }

    #[test]
    fn decodes_raw_and_combined_messages_identically() {
        let table = table();
        let mut decoder = DepthUpdateDecoder::new(8);
        let expected_updates = [
            up(Side::Buy, 8_400_001, 50_000),
            up(Side::Buy, 8_399_999, 0),
            up(Side::Sell, 8_400_002, 125_000),
        ];
        for message in [
            PAYLOAD.to_owned(),
            format!(r#"{{"stream":"btcusdt@depth@100ms","data":{PAYLOAD}}}"#),
        ] {
            let update = decoder.decode(message.as_bytes(), &table).unwrap();
            assert_eq!(update.instrument, InstrumentId::new(0));
            assert_eq!(update.range, SeqRange::new(10, 12).unwrap());
            assert_eq!(
                update.event_time,
                WallTime::from_unix_millis(1_790_299_002_614).unwrap()
            );
            assert_eq!(update.updates, expected_updates);
        }
    }

    #[test]
    fn handles_levels_before_symbol() {
        let message =
            r#"{"b":[["1.00","1"]],"a":[],"U":1,"u":1,"E":1,"e":"depthUpdate","s":"BTCUSDT"}"#;
        let table = table();
        let mut decoder = DepthUpdateDecoder::new(8);
        let update = decoder.decode(message.as_bytes(), &table).unwrap();
        assert_eq!(update.updates, [up(Side::Buy, 100, 100_000)]);
    }

    #[test]
    fn rejects_bad_messages() {
        let table = table();
        let mut decoder = DepthUpdateDecoder::new(3);
        let cases: [(&str, DecodeError); 8] = [
            (
                &PAYLOAD.replace("depthUpdate", "trade"),
                DecodeError::UnexpectedValue("e"),
            ),
            (
                &PAYLOAD.replace("BTCUSDT", "ETHUSDT"),
                DecodeError::UnknownSymbol(Symbol::new("ETHUSDT")),
            ),
            (
                &PAYLOAD.replace(r#""U":10"#, r#""U":13"#),
                DecodeError::InvalidSequence {
                    first: 13,
                    last: 12,
                },
            ),
            (
                &PAYLOAD.replace(r#","u":12"#, ""),
                DecodeError::MissingField("u"),
            ),
            (
                &PAYLOAD.replace(r#","a":[["84000.02","1.25"]]"#, ""),
                DecodeError::MissingField("a"),
            ),
            (
                &PAYLOAD.replace(r#""e":"depthUpdate","#, ""),
                DecodeError::MissingField("e"),
            ),
            (
                &PAYLOAD.replace("0.5", "0.000001"),
                DecodeError::InvalidNumber {
                    field: "qty",
                    error: bowst_core::FixedError::NotMultiple,
                },
            ),
            (
                &PAYLOAD.replace(r#"]],"a""#, r#"],["1","1"]],"a""#),
                DecodeError::TooManyLevels { limit: 3 },
            ),
        ];
        for (message, expected) in cases {
            assert_eq!(
                decoder.decode(message.as_bytes(), &table),
                Err(expected),
                "{message}"
            );
        }
        assert!(
            decoder
                .decode(format!("{PAYLOAD}x").as_bytes(), &table)
                .is_err()
        );
        assert!(decoder.decode(br#"{"data":{"data":{}}}"#, &table).is_err());
        let repeated = PAYLOAD.replace(r#","a":"#, r#","b":[],"a":"#);
        assert_eq!(
            decoder.decode(repeated.as_bytes(), &table),
            Err(DecodeError::UnexpectedValue("b"))
        );
    }

    #[test]
    fn decodes_snapshots() {
        let table = table();
        let instrument = table.get(InstrumentId::new(0)).unwrap();
        let mut decoder = DepthSnapshotDecoder::new(4);
        let snapshot = decoder
            .decode(
                br#"{"lastUpdateId":99,"bids":[["84000.00","1"]],"asks":[["84000.01","2"],["84000.02","3"]]}"#,
                instrument,
            )
            .unwrap();
        assert_eq!(snapshot.last_update_id, 99);
        assert_eq!(snapshot.bids.len(), 1);
        assert_eq!(snapshot.asks[1].price, Price::new(8_400_002));

        for (message, expected) in [
            (
                r#"{"bids":[],"asks":[]}"#,
                DecodeError::MissingField("lastUpdateId"),
            ),
            (
                r#"{"lastUpdateId":1,"asks":[]}"#,
                DecodeError::MissingField("bids"),
            ),
            (
                r#"{"lastUpdateId":1,"bids":[]}"#,
                DecodeError::MissingField("asks"),
            ),
        ] {
            assert_eq!(
                decoder.decode(message.as_bytes(), instrument),
                Err(expected)
            );
        }
    }
}
