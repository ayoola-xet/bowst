//! Binance Spot `GET /api/v3/exchangeInfo`: the order rules for each symbol.
//!
//! Read once at startup (and on refresh), so this decoder allocates its result.

use bowst_core::{Dec, Increment, Symbol};

use crate::DecodeError;
use crate::json::Reader;

/// Order rules for one symbol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SymbolRules {
    /// Venue symbol.
    pub symbol: Symbol,
    /// Whether the symbol's status is `TRADING`. Anything else must not be quoted.
    pub trading: bool,
    /// `PRICE_FILTER.tickSize`.
    pub tick: Increment,
    /// `LOT_SIZE.stepSize`.
    pub lot: Increment,
    /// `NOTIONAL.minNotional` (or the older `MIN_NOTIONAL` filter).
    pub min_notional: Dec,
}

fn number(text: &str, field: &'static str) -> Result<Dec, DecodeError> {
    Dec::parse(text).map_err(|error| DecodeError::InvalidNumber { field, error })
}

fn increment(text: &str, field: &'static str) -> Result<Increment, DecodeError> {
    Increment::parse(text).map_err(|error| DecodeError::InvalidNumber { field, error })
}

/// Decodes an `exchangeInfo` response into rules for every symbol it lists.
///
/// # Errors
/// Any [`DecodeError`], including a symbol missing one of the three filters.
pub fn decode_exchange_info(message: &[u8]) -> Result<Vec<SymbolRules>, DecodeError> {
    let mut rules = Vec::new();
    let mut r = Reader::new(message);
    r.begin_object()?;
    while let Some(key) = r.next_key()? {
        if key == "symbols" {
            r.begin_array()?;
            while r.next_element()? {
                rules.push(read_symbol(&mut r)?);
            }
        } else {
            r.skip()?;
        }
    }
    r.finish()?;
    Ok(rules)
}

fn read_symbol(r: &mut Reader<'_>) -> Result<SymbolRules, DecodeError> {
    let (mut symbol, mut status) = (None, None);
    let (mut tick, mut lot, mut min_notional) = (None, None, None);
    r.begin_object()?;
    while let Some(key) = r.next_key()? {
        match key {
            "symbol" => {
                let text = r.str()?;
                symbol = Some(Symbol::new(text).ok_or(DecodeError::UnexpectedValue("symbol"))?);
            }
            "status" => status = Some(r.str()? == "TRADING"),
            "filters" => {
                r.begin_array()?;
                while r.next_element()? {
                    read_filter(r, &mut tick, &mut lot, &mut min_notional)?;
                }
            }
            _ => r.skip()?,
        }
    }
    Ok(SymbolRules {
        symbol: symbol.ok_or(DecodeError::MissingField("symbol"))?,
        trading: status.ok_or(DecodeError::MissingField("status"))?,
        tick: tick.ok_or(DecodeError::MissingField("tickSize"))?,
        lot: lot.ok_or(DecodeError::MissingField("stepSize"))?,
        min_notional: min_notional.ok_or(DecodeError::MissingField("minNotional"))?,
    })
}

/// Reads one filter object. Its `filterType` may come after the values, so values are
/// collected first and assigned once the type is known.
fn read_filter(
    r: &mut Reader<'_>,
    tick: &mut Option<Increment>,
    lot: &mut Option<Increment>,
    min_notional: &mut Option<Dec>,
) -> Result<(), DecodeError> {
    let (mut kind, mut tick_size, mut step_size, mut notional) = (None, None, None, None);
    r.begin_object()?;
    while let Some(key) = r.next_key()? {
        match key {
            "filterType" => kind = Some(r.str()?),
            "tickSize" => tick_size = Some(r.str()?),
            "stepSize" => step_size = Some(r.str()?),
            "minNotional" => notional = Some(r.str()?),
            _ => r.skip()?,
        }
    }
    match kind {
        Some("PRICE_FILTER") => {
            *tick = Some(increment(
                tick_size.ok_or(DecodeError::MissingField("tickSize"))?,
                "tickSize",
            )?);
        }
        Some("LOT_SIZE") => {
            *lot = Some(increment(
                step_size.ok_or(DecodeError::MissingField("stepSize"))?,
                "stepSize",
            )?);
        }
        Some("NOTIONAL" | "MIN_NOTIONAL") => {
            *min_notional = Some(number(
                notional.ok_or(DecodeError::MissingField("minNotional"))?,
                "minNotional",
            )?);
        }
        Some(_) => {}
        None => return Err(DecodeError::MissingField("filterType")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_rules_with_filters_in_any_field_order() {
        let message =
            br#"{"timezone":"UTC","symbols":[{"symbol":"BTCUSDT","status":"TRADING","filters":[
            {"tickSize":"0.01000000","filterType":"PRICE_FILTER","minPrice":"0.01"},
            {"filterType":"LOT_SIZE","minQty":"0.00001","stepSize":"0.00001000"},
            {"filterType":"ICEBERG_PARTS","limit":10},
            {"filterType":"NOTIONAL","minNotional":"5.00000000"}]},
            {"symbol":"OLDUSDT","status":"BREAK","filters":[
            {"filterType":"PRICE_FILTER","tickSize":"1"},{"filterType":"LOT_SIZE","stepSize":"1"},
            {"filterType":"MIN_NOTIONAL","minNotional":"10"}]}]}"#;
        let rules = decode_exchange_info(message).unwrap();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].symbol.as_str(), "BTCUSDT");
        assert!(rules[0].trading);
        assert_eq!(rules[0].tick, Increment::parse("0.01").unwrap());
        assert_eq!(rules[0].lot, Increment::parse("0.00001").unwrap());
        assert_eq!(rules[0].min_notional, Dec::parse("5").unwrap());
        assert!(!rules[1].trading);
    }

    #[test]
    fn rejects_symbols_missing_rules() {
        let missing_lot = br#"{"symbols":[{"symbol":"X","status":"TRADING","filters":[
            {"filterType":"PRICE_FILTER","tickSize":"1"},{"filterType":"NOTIONAL","minNotional":"1"}]}]}"#;
        assert_eq!(
            decode_exchange_info(missing_lot),
            Err(DecodeError::MissingField("stepSize"))
        );
        let zero_tick = br#"{"symbols":[{"symbol":"X","status":"TRADING","filters":[
            {"filterType":"PRICE_FILTER","tickSize":"0.000"}]}]}"#;
        assert!(matches!(
            decode_exchange_info(zero_tick),
            Err(DecodeError::InvalidNumber {
                field: "tickSize",
                ..
            })
        ));
    }
}
