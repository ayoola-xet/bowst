//! Exact decimal numbers and conversion to integer ticks and lots.
//!
//! Venues send prices and quantities as decimal strings (`"43251.37000000"`). They are parsed
//! into [`Dec`] without ever touching floating point, then converted to integer units of an
//! instrument's [`Increment`] (tick size or lot step) with an explicit [`Rounding`] direction.
//! All hot-path money math then runs on those integers (see [`crate::units`]).

use core::cmp::Ordering;
use core::fmt;
use core::str::FromStr;

/// Largest scale (number of decimal places) a [`Dec`] may carry.
///
/// Products of two parsed values need up to `2 * MAX_PARSE_SCALE` places.
pub const MAX_SCALE: u8 = 36;

/// Largest number of decimal places accepted when parsing text or building an [`Increment`].
pub const MAX_PARSE_SCALE: u8 = 18;

/// Longest text [`Dec::write_to`] can produce: sign, 39 digits, decimal point, leading zero.
pub const MAX_TEXT_LEN: usize = 42;

/// `10^n` for `n` in `0..=38`, the full range that fits in an `i128`.
// Indexing runs only during const evaluation: an out-of-bounds index fails the build, it
// cannot panic at runtime.
#[allow(clippy::indexing_slicing)]
const POW10: [i128; 39] = {
    let mut table = [1_i128; 39];
    let mut i = 1;
    while i < table.len() {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

fn pow10(n: u8) -> Result<i128, FixedError> {
    POW10
        .get(usize::from(n))
        .copied()
        .ok_or(FixedError::ScaleTooLarge)
}

/// Errors from parsing, converting or formatting fixed-point numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FixedError {
    /// The input text was empty or had no digits.
    #[error("empty number")]
    Empty,
    /// The input text contained a character that is not part of a plain decimal number.
    #[error("invalid character at byte {0}")]
    InvalidChar(usize),
    /// The input text had more than [`MAX_PARSE_SCALE`] decimal places.
    #[error("more than {MAX_PARSE_SCALE} decimal places")]
    TooManyDecimals,
    /// A scale above [`MAX_SCALE`] was requested or produced.
    #[error("scale above {MAX_SCALE}")]
    ScaleTooLarge,
    /// The value does not fit in the target integer type.
    #[error("numeric overflow")]
    Overflow,
    /// An increment must be strictly positive.
    #[error("increment must be positive")]
    NonPositiveIncrement,
    /// [`Rounding::Exact`] was requested and the value is not a whole number of increments.
    #[error("value is not a multiple of the increment")]
    NotMultiple,
    /// The output buffer is too small for the formatted number.
    #[error("output buffer too small")]
    BufferTooSmall,
}

/// Rounding direction when converting a decimal value into whole increments.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Rounding {
    /// Toward negative infinity. Used for bid prices so a quote never crosses fair value.
    Down,
    /// Toward positive infinity. Used for ask prices.
    Up,
    /// Fail with [`FixedError::NotMultiple`] unless the value is already exact.
    Exact,
}

/// An exact decimal number: `mantissa * 10^-scale`.
///
/// Equality and ordering compare numeric value, so `1.0 == 1.00`. `Dec` is `Copy` and
/// never allocates.
#[derive(Clone, Copy, Debug)]
pub struct Dec {
    mantissa: i128,
    scale: u8,
}

impl Dec {
    /// Zero.
    pub const ZERO: Self = Self {
        mantissa: 0,
        scale: 0,
    };

    /// Builds `mantissa * 10^-scale`.
    ///
    /// # Errors
    /// [`FixedError::ScaleTooLarge`] if `scale > MAX_SCALE`.
    pub const fn new(mantissa: i128, scale: u8) -> Result<Self, FixedError> {
        if scale > MAX_SCALE {
            return Err(FixedError::ScaleTooLarge);
        }
        Ok(Self { mantissa, scale })
    }

    /// The integer mantissa.
    #[must_use]
    pub const fn mantissa(self) -> i128 {
        self.mantissa
    }

    /// The number of decimal places.
    #[must_use]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    /// Whether the value is zero.
    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.mantissa == 0
    }

    /// Whether the value is strictly negative.
    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.mantissa < 0
    }

    /// Parses a plain decimal number such as `"42"`, `"-0.00100"` or `"43251.37"`.
    ///
    /// Accepted form: optional `-`, one or more digits, then optionally `.` followed by one or
    /// more digits. Exponents, `+`, whitespace and separators are rejected.
    ///
    /// # Errors
    /// [`FixedError::Empty`], [`FixedError::InvalidChar`], [`FixedError::TooManyDecimals`] or
    /// [`FixedError::Overflow`].
    pub fn parse(text: &str) -> Result<Self, FixedError> {
        let bytes = text.as_bytes();
        let (negative, digits_start) = match bytes.first() {
            None => return Err(FixedError::Empty),
            Some(b'-') => (true, 1),
            Some(_) => (false, 0),
        };

        let mut mantissa: i128 = 0;
        let mut scale: u8 = 0;
        let mut seen_point = false;
        let mut int_digits = 0_usize;

        for (pos, &byte) in bytes.iter().enumerate().skip(digits_start) {
            match byte {
                b'0'..=b'9' => {
                    mantissa = mantissa
                        .checked_mul(10)
                        .and_then(|m| m.checked_add(i128::from(byte.wrapping_sub(b'0'))))
                        .ok_or(FixedError::Overflow)?;
                    if seen_point {
                        scale = scale.saturating_add(1);
                        if scale > MAX_PARSE_SCALE {
                            return Err(FixedError::TooManyDecimals);
                        }
                    } else {
                        int_digits = int_digits.saturating_add(1);
                    }
                }
                b'.' if !seen_point && int_digits > 0 => seen_point = true,
                _ => return Err(FixedError::InvalidChar(pos)),
            }
        }

        if int_digits == 0 {
            return Err(FixedError::Empty);
        }
        if seen_point && scale == 0 {
            // Trailing point with no fractional digits, e.g. "12."
            return Err(FixedError::InvalidChar(bytes.len().saturating_sub(1)));
        }
        if negative {
            mantissa = mantissa.checked_neg().ok_or(FixedError::Overflow)?;
        }
        Ok(Self { mantissa, scale })
    }

    /// Exact product of two decimals.
    ///
    /// # Errors
    /// [`FixedError::Overflow`] or [`FixedError::ScaleTooLarge`].
    pub fn checked_mul(self, rhs: Self) -> Result<Self, FixedError> {
        let mantissa = self
            .mantissa
            .checked_mul(rhs.mantissa)
            .ok_or(FixedError::Overflow)?;
        let scale = self
            .scale
            .checked_add(rhs.scale)
            .ok_or(FixedError::ScaleTooLarge)?;
        Self::new(mantissa, scale)
    }

    /// Mantissa expressed at a larger `scale`.
    fn mantissa_at(self, scale: u8) -> Result<i128, FixedError> {
        let shift = scale
            .checked_sub(self.scale)
            .ok_or(FixedError::ScaleTooLarge)?;
        self.mantissa
            .checked_mul(pow10(shift)?)
            .ok_or(FixedError::Overflow)
    }

    /// Writes the value as plain decimal text into `buf` and returns the number of bytes
    /// written. Keeps every decimal place of the scale (`1.50` stays `"1.50"`). Never allocates.
    ///
    /// # Errors
    /// [`FixedError::BufferTooSmall`] if `buf` is shorter than the output. A buffer of
    /// [`MAX_TEXT_LEN`] bytes always suffices.
    pub fn write_to(self, buf: &mut [u8]) -> Result<usize, FixedError> {
        // Digits are produced least significant first into `rev`.
        let mut rev = [b'0'; MAX_TEXT_LEN];
        let mut len = 0_usize;
        let mut rest = self.mantissa.unsigned_abs();
        let min_digits = usize::from(self.scale).saturating_add(1);
        while rest > 0 || len < min_digits {
            let digit = u8::try_from(rest % 10).map_err(|_| FixedError::Overflow)?;
            *rev.get_mut(len).ok_or(FixedError::Overflow)? = b'0'.saturating_add(digit);
            rest /= 10;
            len = len.saturating_add(1);
        }

        let mut out = Writer { buf, pos: 0 };
        if self.mantissa < 0 {
            out.push(b'-')?;
        }
        let point_at = len.saturating_sub(usize::from(self.scale));
        for (i, digit) in rev.iter().take(len).rev().enumerate() {
            if i == point_at && self.scale > 0 {
                out.push(b'.')?;
            }
            out.push(*digit)?;
        }
        Ok(out.pos)
    }

    /// Integer part (floored) and non-negative fractional mantissa at this scale.
    fn split(self) -> (i128, i128) {
        // Scale is at most MAX_SCALE, so the table lookup cannot fail.
        let unit = POW10.get(usize::from(self.scale)).copied().unwrap_or(1);
        let int = self.mantissa.div_euclid(unit);
        let frac = self.mantissa.rem_euclid(unit);
        (int, frac)
    }
}

impl Ord for Dec {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.scale == other.scale {
            return self.mantissa.cmp(&other.mantissa);
        }
        let (a_int, a_frac) = self.split();
        let (b_int, b_frac) = other.split();
        a_int.cmp(&b_int).then_with(|| {
            // Both fractions are below 10^scale <= 10^36, so rescaling to the larger scale
            // stays below 10^36 and cannot overflow an i128.
            let common = self.scale.max(other.scale);
            let rescale = |frac: i128, scale: u8| {
                let shift = common.saturating_sub(scale);
                frac.saturating_mul(POW10.get(usize::from(shift)).copied().unwrap_or(1))
            };
            rescale(a_frac, self.scale).cmp(&rescale(b_frac, other.scale))
        })
    }
}

impl PartialOrd for Dec {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Dec {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Dec {}

impl FromStr for Dec {
    type Err = FixedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Display for Dec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = [0_u8; MAX_TEXT_LEN];
        let len = self.write_to(&mut buf).map_err(|_| fmt::Error)?;
        let text = buf.get(..len).ok_or(fmt::Error)?;
        f.write_str(core::str::from_utf8(text).map_err(|_| fmt::Error)?)
    }
}

/// Bounds-checked byte writer used by [`Dec::write_to`].
struct Writer<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl Writer<'_> {
    fn push(&mut self, byte: u8) -> Result<(), FixedError> {
        *self
            .buf
            .get_mut(self.pos)
            .ok_or(FixedError::BufferTooSmall)? = byte;
        self.pos = self.pos.saturating_add(1);
        Ok(())
    }
}

/// A strictly positive step size: an instrument's tick size or lot step.
///
/// Stored normalized (no trailing zeros), so `"0.01000000"` and `"0.01"` are the same
/// increment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Increment {
    mantissa: i64,
    scale: u8,
}

impl Increment {
    /// Builds `mantissa * 10^-scale`.
    ///
    /// # Errors
    /// [`FixedError::NonPositiveIncrement`] or [`FixedError::TooManyDecimals`].
    pub fn new(mantissa: i64, scale: u8) -> Result<Self, FixedError> {
        if mantissa <= 0 {
            return Err(FixedError::NonPositiveIncrement);
        }
        if scale > MAX_PARSE_SCALE {
            return Err(FixedError::TooManyDecimals);
        }
        let (mut mantissa, mut scale) = (mantissa, scale);
        while scale > 0 && mantissa % 10 == 0 {
            mantissa /= 10;
            scale = scale.saturating_sub(1);
        }
        Ok(Self { mantissa, scale })
    }

    /// Parses an increment from venue text such as `"0.01000000"`.
    ///
    /// # Errors
    /// Any [`Dec::parse`] error, [`FixedError::NonPositiveIncrement`] or
    /// [`FixedError::Overflow`] if the mantissa does not fit in an `i64`.
    pub fn parse(text: &str) -> Result<Self, FixedError> {
        let dec = Dec::parse(text)?;
        let mantissa = i64::try_from(dec.mantissa).map_err(|_| FixedError::Overflow)?;
        Self::new(mantissa, dec.scale)
    }

    /// The increment as a decimal.
    #[must_use]
    pub fn as_dec(self) -> Dec {
        Dec {
            mantissa: i128::from(self.mantissa),
            scale: self.scale,
        }
    }

    /// Converts a decimal value into a whole number of increments.
    ///
    /// # Errors
    /// [`FixedError::NotMultiple`] with [`Rounding::Exact`] when the value is not exact, or
    /// [`FixedError::Overflow`] if the result does not fit in an `i64`.
    pub fn to_units(self, value: Dec, rounding: Rounding) -> Result<i64, FixedError> {
        let common = value.scale.max(self.scale);
        let numerator = value.mantissa_at(common)?;
        let denominator = self.as_dec().mantissa_at(common)?;
        // `denominator` is strictly positive, so Euclidean division is floor division.
        let floor = numerator
            .checked_div_euclid(denominator)
            .ok_or(FixedError::Overflow)?;
        let remainder = numerator
            .checked_rem_euclid(denominator)
            .ok_or(FixedError::Overflow)?;
        let units = match (rounding, remainder == 0) {
            (_, true) | (Rounding::Down, false) => floor,
            (Rounding::Up, false) => floor.checked_add(1).ok_or(FixedError::Overflow)?,
            (Rounding::Exact, false) => return Err(FixedError::NotMultiple),
        };
        i64::try_from(units).map_err(|_| FixedError::Overflow)
    }

    /// The decimal value of `units` whole increments. Always exact.
    #[must_use]
    pub fn from_units(self, units: i64) -> Dec {
        // |i64 * i64| < 2^126, so the product always fits in an i128.
        let mantissa = i128::from(units).saturating_mul(i128::from(self.mantissa));
        Dec {
            mantissa,
            scale: self.scale,
        }
    }
}

impl fmt::Display for Increment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_dec().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn dec(s: &str) -> Dec {
        Dec::parse(s).unwrap()
    }

    #[test]
    fn parses_plain_decimals() {
        assert_eq!(dec("42"), Dec::new(42, 0).unwrap());
        assert_eq!(dec("-0.00100").mantissa(), -100);
        assert_eq!(dec("-0.00100").scale(), 5);
        assert_eq!(dec("43251.37000000").scale(), 8);
        assert_eq!(dec("0"), Dec::ZERO);
    }

    #[test]
    fn rejects_malformed_text() {
        for (text, err) in [
            ("", FixedError::Empty),
            ("-", FixedError::Empty),
            (".5", FixedError::InvalidChar(0)),
            ("1.2.3", FixedError::InvalidChar(3)),
            ("12.", FixedError::InvalidChar(2)),
            ("1e-8", FixedError::InvalidChar(1)),
            ("+1", FixedError::InvalidChar(0)),
            (" 1", FixedError::InvalidChar(0)),
            ("1,000", FixedError::InvalidChar(1)),
            ("0.0000000000000000001", FixedError::TooManyDecimals),
            (
                "999999999999999999999999999999999999999999",
                FixedError::Overflow,
            ),
        ] {
            assert_eq!(Dec::parse(text), Err(err), "input {text:?}");
        }
    }

    #[test]
    fn compares_by_value_across_scales() {
        assert_eq!(dec("1.0"), dec("1.00"));
        assert!(dec("1.01") > dec("1.009"));
        assert!(dec("-1.5") < dec("-1.25"));
        assert!(dec("-0.1") < dec("0"));
    }

    #[test]
    fn formats_keeping_scale() {
        for text in ["0", "42", "-0.00100", "43251.37000000", "0.5", "-12.340"] {
            assert_eq!(dec(text).to_string(), text);
        }
    }

    #[test]
    fn write_to_reports_small_buffer() {
        let mut buf = [0_u8; 3];
        assert_eq!(
            dec("123.4").write_to(&mut buf),
            Err(FixedError::BufferTooSmall)
        );
    }

    #[test]
    fn increment_normalizes_trailing_zeros() {
        assert_eq!(
            Increment::parse("0.01000000").unwrap(),
            Increment::parse("0.01").unwrap()
        );
        assert_eq!(Increment::parse("10").unwrap().to_string(), "10");
        assert_eq!(Increment::parse("0"), Err(FixedError::NonPositiveIncrement));
        assert_eq!(
            Increment::parse("-0.1"),
            Err(FixedError::NonPositiveIncrement)
        );
    }

    #[test]
    fn converts_to_units_with_rounding() {
        let tick = Increment::parse("0.05").unwrap();
        assert_eq!(tick.to_units(dec("100.05"), Rounding::Exact), Ok(2001));
        assert_eq!(tick.to_units(dec("100.07"), Rounding::Down), Ok(2001));
        assert_eq!(tick.to_units(dec("100.07"), Rounding::Up), Ok(2002));
        assert_eq!(
            tick.to_units(dec("100.07"), Rounding::Exact),
            Err(FixedError::NotMultiple)
        );
        assert_eq!(tick.to_units(dec("-0.07"), Rounding::Down), Ok(-2));
        assert_eq!(tick.to_units(dec("-0.07"), Rounding::Up), Ok(-1));
        assert_eq!(tick.from_units(2001).to_string(), "100.05");
    }

    #[test]
    fn converts_with_whole_number_increments() {
        let lot = Increment::parse("5").unwrap();
        assert_eq!(lot.to_units(dec("12"), Rounding::Down), Ok(2));
        assert_eq!(lot.to_units(dec("12"), Rounding::Up), Ok(3));
    }

    #[test]
    fn reports_unit_overflow() {
        let tick = Increment::parse("0.000000000000000001").unwrap();
        assert_eq!(
            tick.to_units(dec("100000"), Rounding::Exact),
            Err(FixedError::Overflow)
        );
    }

    fn arb_dec() -> impl Strategy<Value = Dec> {
        (-10_i128.pow(20)..10_i128.pow(20), 0..=MAX_PARSE_SCALE)
            .prop_map(|(m, s)| Dec::new(m, s).unwrap())
    }

    fn arb_increment() -> impl Strategy<Value = Increment> {
        (1_i64..1_000_000, 0..=12_u8).prop_map(|(m, s)| Increment::new(m, s).unwrap())
    }

    proptest! {
        #[test]
        fn text_round_trips(value in arb_dec()) {
            let parsed = Dec::parse(&value.to_string()).unwrap();
            prop_assert_eq!(parsed.mantissa(), value.mantissa());
            prop_assert_eq!(parsed.scale(), value.scale());
        }

        #[test]
        fn rounding_brackets_the_exact_value(value in arb_dec(), inc in arb_increment()) {
            let (Ok(down), Ok(up)) = (
                inc.to_units(value, Rounding::Down),
                inc.to_units(value, Rounding::Up),
            ) else {
                return Ok(()); // out of i64 range, reported as overflow
            };
            prop_assert!(inc.from_units(down) <= value);
            prop_assert!(inc.from_units(up) >= value);
            prop_assert!(up - down <= 1);
            prop_assert_eq!(up == down, inc.to_units(value, Rounding::Exact).is_ok());
        }

        #[test]
        fn ordering_matches_integer_ordering(a in -1_000_000_i64..1_000_000, b in -1_000_000_i64..1_000_000, sa in 0..=8_u8, sb in 0..=8_u8) {
            // Compare a/10^sa and b/10^sb by cross-multiplying exactly in i128.
            let lhs = i128::from(a) * 10_i128.pow(u32::from(sb));
            let rhs = i128::from(b) * 10_i128.pow(u32::from(sa));
            let expected = lhs.cmp(&rhs);
            prop_assert_eq!(Dec::new(a.into(), sa).unwrap().cmp(&Dec::new(b.into(), sb).unwrap()), expected);
        }
    }
}
