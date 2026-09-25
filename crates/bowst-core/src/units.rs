//! Integer price and quantity types used for all hot-path money math.
//!
//! A [`Price`] is a whole number of an instrument's tick size and a [`Qty`] is a whole number
//! of its lot step. Converting to and from decimals always goes through an
//! [`Increment`] with an explicit [`Rounding`], so rounding decisions are visible in code.

use crate::fixed::{Dec, FixedError, Increment, Rounding};

/// Defines an `i64` newtype counted in whole increments, with the shared conversions.
macro_rules! increment_units {
    ($(#[$meta:meta])* $name:ident, $unit:literal) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name(i64);

        impl $name {
            #[doc = concat!("Zero ", $unit, ".")]
            pub const ZERO: Self = Self(0);

            #[doc = concat!("Wraps a whole number of ", $unit, ".")]
            #[must_use]
            pub const fn new(units: i64) -> Self {
                Self(units)
            }

            #[doc = concat!("The whole number of ", $unit, ".")]
            #[must_use]
            pub const fn get(self) -> i64 {
                self.0
            }

            #[doc = concat!("Whether this is zero ", $unit, ".")]
            #[must_use]
            pub const fn is_zero(self) -> bool {
                self.0 == 0
            }

            #[doc = concat!("Converts a decimal into ", $unit, " of `increment`.")]
            ///
            /// # Errors
            /// See [`Increment::to_units`].
            pub fn from_dec(
                value: Dec,
                increment: Increment,
                rounding: Rounding,
            ) -> Result<Self, FixedError> {
                increment.to_units(value, rounding).map(Self)
            }

            /// The exact decimal value, given the instrument's increment.
            #[must_use]
            pub fn to_dec(self, increment: Increment) -> Dec {
                increment.from_units(self.0)
            }

            /// Checked addition. `None` on overflow.
            #[must_use]
            pub const fn checked_add(self, rhs: Self) -> Option<Self> {
                match self.0.checked_add(rhs.0) {
                    Some(v) => Some(Self(v)),
                    None => None,
                }
            }

            /// Checked subtraction. `None` on overflow.
            #[must_use]
            pub const fn checked_sub(self, rhs: Self) -> Option<Self> {
                match self.0.checked_sub(rhs.0) {
                    Some(v) => Some(Self(v)),
                    None => None,
                }
            }
        }
    };
}

increment_units!(
    /// A price as a whole number of the instrument's tick size.
    Price,
    "ticks"
);

increment_units!(
    /// A quantity as a whole number of the instrument's lot step.
    Qty,
    "lots"
);

/// Order or book side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Side {
    /// Bid side: buying the base asset.
    Buy,
    /// Ask side: selling the base asset.
    Sell,
}

impl Side {
    /// The other side.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    /// `+1` for buys, `-1` for sells: the sign of the base-asset change when filled.
    #[must_use]
    pub const fn sign(self) -> i64 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }

    /// Rounding that keeps a quote on the passive side of a price: bids round down,
    /// asks round up.
    #[must_use]
    pub const fn passive_rounding(self) -> Rounding {
        match self {
            Self::Buy => Rounding::Down,
            Self::Sell => Rounding::Up,
        }
    }

    /// Whether price `a` is strictly more aggressive than `b` on this side: higher for
    /// buys, lower for sells.
    #[must_use]
    pub fn is_more_aggressive(self, a: Price, b: Price) -> bool {
        match self {
            Self::Buy => a > b,
            Self::Sell => a < b,
        }
    }
}

/// Exact quote-currency value of `qty` at `price`.
///
/// # Errors
/// [`FixedError::Overflow`] if the product does not fit.
pub fn notional(
    price: Price,
    tick: Increment,
    qty: Qty,
    lot: Increment,
) -> Result<Dec, FixedError> {
    price.to_dec(tick).checked_mul(qty.to_dec(lot))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inc(s: &str) -> Increment {
        Increment::parse(s).unwrap()
    }

    #[test]
    fn passive_rounding_keeps_quotes_off_fair_value() {
        let tick = inc("0.01");
        let fair = Dec::parse("100.005").unwrap();
        let bid = Price::from_dec(fair, tick, Side::Buy.passive_rounding()).unwrap();
        let ask = Price::from_dec(fair, tick, Side::Sell.passive_rounding()).unwrap();
        assert_eq!(bid, Price::new(10_000));
        assert_eq!(ask, Price::new(10_001));
        assert!(bid.to_dec(tick) <= fair && ask.to_dec(tick) >= fair);
    }

    #[test]
    fn aggressiveness_depends_on_side() {
        let (lo, hi) = (Price::new(1), Price::new(2));
        assert!(Side::Buy.is_more_aggressive(hi, lo));
        assert!(Side::Sell.is_more_aggressive(lo, hi));
        assert!(!Side::Buy.is_more_aggressive(lo, lo));
    }

    #[test]
    fn notional_is_exact() {
        let value = notional(
            Price::new(4_325_137),
            inc("0.01"),
            Qty::new(150),
            inc("0.0001"),
        )
        .unwrap();
        assert_eq!(value, Dec::parse("648.77055").unwrap());
    }

    #[test]
    fn checked_arithmetic_reports_overflow() {
        assert_eq!(Qty::new(i64::MAX).checked_add(Qty::new(1)), None);
        assert_eq!(
            Price::new(5).checked_sub(Price::new(7)),
            Some(Price::new(-2))
        );
    }
}
