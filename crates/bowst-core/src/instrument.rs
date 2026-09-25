//! Tradable instruments and their venue rules.

use core::fmt;

use crate::fixed::{Dec, Increment};
use crate::ids::{InstrumentId, VenueId};

/// Longest venue symbol a [`Symbol`] can hold.
pub const MAX_SYMBOL_LEN: usize = 24;

/// A venue's symbol for an instrument (for example `BTCUSDT`), stored inline so it is `Copy`
/// and never allocates.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Symbol {
    bytes: [u8; MAX_SYMBOL_LEN],
    len: u8,
}

impl Symbol {
    /// Wraps `text`. `None` if it is empty, longer than [`MAX_SYMBOL_LEN`] or not printable
    /// ASCII.
    #[must_use]
    pub fn new(text: &str) -> Option<Self> {
        if text.is_empty()
            || text.len() > MAX_SYMBOL_LEN
            || !text.bytes().all(|b| b.is_ascii_graphic())
        {
            return None;
        }
        let mut bytes = [0_u8; MAX_SYMBOL_LEN];
        bytes
            .get_mut(..text.len())?
            .copy_from_slice(text.as_bytes());
        Some(Self {
            bytes,
            len: u8::try_from(text.len()).ok()?,
        })
    }

    /// The symbol text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // Only ASCII is ever stored.
        self.bytes
            .get(..usize::from(self.len))
            .and_then(|b| core::str::from_utf8(b).ok())
            .unwrap_or("")
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Symbol({})", self.as_str())
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One instrument on one venue, with the rules its orders must satisfy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Instrument {
    /// Process-local ID.
    pub id: InstrumentId,
    /// Venue it trades on.
    pub venue: VenueId,
    /// Venue symbol.
    pub symbol: Symbol,
    /// Price increment.
    pub tick: Increment,
    /// Quantity increment.
    pub lot: Increment,
    /// Smallest order value (price × quantity) the venue accepts, in the quote asset.
    pub min_notional: Dec,
}

/// All configured instruments, looked up without allocating.
#[derive(Clone, Debug, Default)]
pub struct InstrumentTable {
    instruments: Vec<Instrument>,
}

impl InstrumentTable {
    /// Builds a table. IDs must be `0..n` in order, so an ID is also an index.
    ///
    /// # Errors
    /// [`InstrumentTableError`] if IDs are not sequential or a venue symbol repeats.
    pub fn new(instruments: Vec<Instrument>) -> Result<Self, InstrumentTableError> {
        for (index, instrument) in instruments.iter().enumerate() {
            if usize::try_from(instrument.id.get()).ok() != Some(index) {
                return Err(InstrumentTableError::NonSequentialId(instrument.id));
            }
            let duplicate = instruments
                .iter()
                .take(index)
                .any(|other| other.venue == instrument.venue && other.symbol == instrument.symbol);
            if duplicate {
                return Err(InstrumentTableError::DuplicateSymbol(
                    instrument.venue,
                    instrument.symbol,
                ));
            }
        }
        Ok(Self { instruments })
    }

    /// Instrument by ID.
    #[must_use]
    pub fn get(&self, id: InstrumentId) -> Option<&Instrument> {
        self.instruments.get(usize::try_from(id.get()).ok()?)
    }

    /// Instrument by venue symbol. A linear scan: tables hold a handful of instruments.
    #[must_use]
    pub fn find(&self, venue: VenueId, symbol: &str) -> Option<&Instrument> {
        self.instruments
            .iter()
            .find(|i| i.venue == venue && i.symbol.as_str() == symbol)
    }

    /// All instruments in ID order.
    #[must_use = "iterators are lazy"]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &Instrument> {
        self.instruments.iter()
    }
}

/// Invalid instrument configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InstrumentTableError {
    /// IDs must equal their position in the table.
    #[error("instrument id {0:?} is out of sequence")]
    NonSequentialId(InstrumentId),
    /// A venue symbol appears twice.
    #[error("duplicate symbol {1} on {0}")]
    DuplicateSymbol(VenueId, Symbol),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument(id: u32, symbol: &str) -> Instrument {
        Instrument {
            id: InstrumentId::new(id),
            venue: VenueId::Binance,
            symbol: Symbol::new(symbol).unwrap(),
            tick: Increment::parse("0.01").unwrap(),
            lot: Increment::parse("0.00001").unwrap(),
            min_notional: Dec::parse("5").unwrap(),
        }
    }

    #[test]
    fn symbols_are_validated() {
        assert_eq!(Symbol::new("BTCUSDT").unwrap().as_str(), "BTCUSDT");
        assert!(Symbol::new("").is_none());
        assert!(Symbol::new("BTC USDT").is_none());
        assert!(Symbol::new(&"X".repeat(MAX_SYMBOL_LEN + 1)).is_none());
        assert!(Symbol::new(&"X".repeat(MAX_SYMBOL_LEN)).is_some());
    }

    #[test]
    fn table_looks_up_by_id_and_symbol() {
        let table = InstrumentTable::new(vec![instrument(0, "BTCUSDT"), instrument(1, "DOGEUSDT")])
            .unwrap();
        assert_eq!(
            table.find(VenueId::Binance, "DOGEUSDT").map(|i| i.id),
            Some(InstrumentId::new(1))
        );
        assert!(table.find(VenueId::Bybit, "DOGEUSDT").is_none());
        assert_eq!(
            table.get(InstrumentId::new(0)).map(|i| i.symbol.as_str()),
            Some("BTCUSDT")
        );
        assert!(table.get(InstrumentId::new(2)).is_none());
    }

    #[test]
    fn table_rejects_bad_configuration() {
        assert_eq!(
            InstrumentTable::new(vec![instrument(1, "BTCUSDT")]).unwrap_err(),
            InstrumentTableError::NonSequentialId(InstrumentId::new(1))
        );
        assert!(matches!(
            InstrumentTable::new(vec![instrument(0, "BTCUSDT"), instrument(1, "BTCUSDT")]),
            Err(InstrumentTableError::DuplicateSymbol(..))
        ));
    }
}
