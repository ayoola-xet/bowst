//! Shared setup for the Binance fuzz targets.

use bowst_core::{Dec, Increment, Instrument, InstrumentId, InstrumentTable, Symbol, VenueId};

/// The two recorded instruments, with the rules from the fixtures.
pub fn instruments() -> InstrumentTable {
    let make = |id, symbol, tick, lot| Instrument {
        id: InstrumentId::new(id),
        venue: VenueId::Binance,
        symbol: Symbol::new(symbol).unwrap(),
        tick: Increment::parse(tick).unwrap(),
        lot: Increment::parse(lot).unwrap(),
        min_notional: Dec::parse("1").unwrap(),
    };
    InstrumentTable::new(vec![
        make(0, "BTCUSDT", "0.01", "0.00001"),
        make(1, "DOGEUSDT", "0.00001", "1"),
    ])
    .unwrap()
}
