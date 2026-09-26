//! Decoding `[["price", "qty"], ...]` level arrays, the format Binance and Bybit share.

use bowst_book::{Level, LevelUpdate};
use bowst_core::{Increment, Price, Qty, Rounding, Side};

use crate::DecodeError;
use crate::json::Reader;

#[inline]
fn exact(text: &[u8], increment: Increment, field: &'static str) -> Result<i64, DecodeError> {
    increment
        .parse_units_bytes(text, Rounding::Exact)
        .map_err(|error| DecodeError::InvalidNumber { field, error })
}

/// Reads one `["price", "qty"]` pair, converted exactly to ticks and lots.
///
/// # Errors
/// On malformed JSON, a pair that is not exactly two strings, or a value that is not a whole
/// number of the instrument's increments.
#[inline]
pub fn read_level(
    r: &mut Reader<'_>,
    tick: Increment,
    lot: Increment,
) -> Result<Level, DecodeError> {
    r.begin_array()?;
    if !r.next_element()? {
        return Err(DecodeError::UnexpectedValue("level"));
    }
    let price = Price::new(exact(r.str_bytes()?, tick, "price")?);
    if !r.next_element()? {
        return Err(DecodeError::UnexpectedValue("level"));
    }
    let qty = Qty::new(exact(r.str_bytes()?, lot, "qty")?);
    if r.next_element()? {
        return Err(DecodeError::UnexpectedValue("level"));
    }
    Ok(Level { price, qty })
}

/// Reads a whole level array, appending to `out` without growing it past `limit`.
///
/// # Errors
/// See [`read_level`], plus [`DecodeError::TooManyLevels`].
pub fn read_levels_into(
    r: &mut Reader<'_>,
    tick: Increment,
    lot: Increment,
    limit: usize,
    out: &mut Vec<Level>,
) -> Result<(), DecodeError> {
    r.begin_array()?;
    while r.next_element()? {
        if out.len() >= limit {
            return Err(DecodeError::TooManyLevels { limit });
        }
        out.push(read_level(r, tick, lot)?);
    }
    Ok(())
}

/// Reads a whole level array as updates for `side`, appending to `out` without growing it
/// past `limit`.
///
/// # Errors
/// See [`read_levels_into`].
pub fn read_updates_into(
    r: &mut Reader<'_>,
    side: Side,
    tick: Increment,
    lot: Increment,
    limit: usize,
    out: &mut Vec<LevelUpdate>,
) -> Result<(), DecodeError> {
    r.begin_array()?;
    while r.next_element()? {
        if out.len() >= limit {
            return Err(DecodeError::TooManyLevels { limit });
        }
        let level = read_level(r, tick, lot)?;
        out.push(LevelUpdate {
            side,
            price: level.price,
            qty: level.qty,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inc(s: &str) -> Increment {
        Increment::parse(s).unwrap()
    }

    #[test]
    fn reads_exact_levels() {
        let mut r = Reader::new(br#"[["84735.39000000","5.01777000"],["0.01","0"]]"#);
        let mut out = Vec::with_capacity(4);
        read_levels_into(&mut r, inc("0.01"), inc("0.00001"), 4, &mut out).unwrap();
        assert_eq!(
            out,
            [
                Level {
                    price: Price::new(8_473_539),
                    qty: Qty::new(501_777)
                },
                Level {
                    price: Price::new(1),
                    qty: Qty::ZERO
                },
            ]
        );
    }

    #[test]
    fn rejects_inexact_or_malformed_levels() {
        for input in [
            r#"[["1.005","1"]]"#,
            r#"[["1.00","0.000001"]]"#,
            r#"[["1.00"]]"#,
            r#"[["1.00","1","x"]]"#,
            r#"[[1.00,"1"]]"#,
            r#"[["abc","1"]]"#,
        ] {
            let mut r = Reader::new(input.as_bytes());
            let mut out = Vec::with_capacity(4);
            assert!(
                read_levels_into(&mut r, inc("0.01"), inc("0.00001"), 4, &mut out).is_err(),
                "accepted {input}"
            );
        }
    }

    #[test]
    fn stops_at_the_limit_without_growing() {
        let mut r = Reader::new(br#"[["1","1"],["2","1"],["3","1"]]"#);
        let mut out = Vec::with_capacity(2);
        assert_eq!(
            read_levels_into(&mut r, inc("1"), inc("1"), 2, &mut out),
            Err(DecodeError::TooManyLevels { limit: 2 })
        );
        assert_eq!(out.capacity(), 2);
    }
}
