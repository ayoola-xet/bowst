//! Binance Spot REST calls used by market data: depth snapshots and exchange info.

use std::time::Duration;

use bowst_core::{Instrument, InstrumentId, InstrumentTable, Symbol, VenueId};

use super::exchange_info::decode_exchange_info;
use crate::DecodeError;
use crate::net::http::{self, Response};
use crate::net::{NetError, TlsConfig, Url};

/// Binance's documented request weight for `GET /api/v3/depth` at a given `limit`.
#[must_use]
pub fn depth_weight(limit: u32) -> u64 {
    match limit {
        0..=100 => 5,
        101..=500 => 25,
        501..=1000 => 50,
        _ => 250,
    }
}

/// Largest depth snapshot Binance serves.
pub const MAX_DEPTH_LIMIT: u32 = 5_000;

/// URL for a depth snapshot of `symbol` with `limit` levels per side.
///
/// # Errors
/// [`NetError::InvalidUrl`] if `rest_base` is not a valid base URL.
pub fn depth_url(rest_base: &str, symbol: Symbol, limit: u32) -> Result<Url, NetError> {
    Url::parse(&format!(
        "{}/api/v3/depth?symbol={}&limit={}",
        rest_base.trim_end_matches('/'),
        symbol,
        limit.min(MAX_DEPTH_LIMIT)
    ))
}

/// A REST call that failed.
#[derive(Debug, thiserror::Error)]
pub enum RestError {
    /// Network, TLS or HTTP framing failure.
    #[error(transparent)]
    Net(#[from] NetError),
    /// A non-200 status. 429 and 418 mean the rate limit was hit and carry `Retry-After`.
    #[error("HTTP status {status}")]
    Status {
        /// Status code.
        status: u16,
        /// Seconds to wait before retrying, if given.
        retry_after_secs: Option<u32>,
    },
    /// The body could not be decoded.
    #[error("decode: {0}")]
    Decode(#[from] DecodeError),
    /// A configured symbol is not listed by the venue.
    #[error("symbol {0} is not listed")]
    UnknownSymbol(Symbol),
    /// A configured symbol is listed but not in `TRADING` status.
    #[error("symbol {0} is not trading")]
    NotTrading(Symbol),
    /// The configured instrument set is invalid (duplicates, too many).
    #[error("invalid instrument configuration")]
    InvalidConfiguration,
}

/// Fails unless the response status is 200.
///
/// # Errors
/// [`RestError::Status`].
pub fn require_ok(response: Response) -> Result<Response, RestError> {
    if response.status == 200 {
        Ok(response)
    } else {
        Err(RestError::Status {
            status: response.status,
            retry_after_secs: response.retry_after_secs,
        })
    }
}

/// Fetches trading rules for `symbols` and builds the instrument table, with IDs in the
/// order given. Fails closed if any symbol is unknown or not trading.
///
/// # Errors
/// Any [`RestError`].
pub fn load_instruments(
    rest_base: &str,
    symbols: &[Symbol],
    tls: &TlsConfig,
    timeout: Duration,
) -> Result<InstrumentTable, RestError> {
    let list = symbols
        .iter()
        .map(|s| format!("%22{s}%22"))
        .collect::<Vec<_>>()
        .join(",");
    let url = Url::parse(&format!(
        "{}/api/v3/exchangeInfo?symbols=%5B{list}%5D",
        rest_base.trim_end_matches('/')
    ))?;
    let (mut raw, mut body) = (Vec::new(), Vec::new());
    require_ok(http::get(&url, tls, timeout, 4 << 20, &mut raw, &mut body)?)?;
    let rules = decode_exchange_info(&body)?;
    let mut instruments = Vec::with_capacity(symbols.len());
    for (index, symbol) in symbols.iter().enumerate() {
        let rule = rules
            .iter()
            .find(|r| r.symbol == *symbol)
            .ok_or(RestError::UnknownSymbol(*symbol))?;
        if !rule.trading {
            return Err(RestError::NotTrading(*symbol));
        }
        instruments.push(Instrument {
            id: InstrumentId::new(
                u32::try_from(index).map_err(|_| RestError::InvalidConfiguration)?,
            ),
            venue: VenueId::Binance,
            symbol: rule.symbol,
            tick: rule.tick,
            lot: rule.lot,
            min_notional: rule.min_notional,
        });
    }
    InstrumentTable::new(instruments).map_err(|_| RestError::InvalidConfiguration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_weights_follow_binance_tiers() {
        assert_eq!(
            [1, 100, 101, 500, 501, 1000, 1001, 5000].map(depth_weight),
            [5, 5, 25, 25, 50, 50, 250, 250]
        );
    }

    #[test]
    fn builds_depth_urls() {
        let url = depth_url(
            "https://api.example/",
            Symbol::new("BTCUSDT").unwrap(),
            9_999,
        )
        .unwrap();
        assert_eq!(url.path, "/api/v3/depth?symbol=BTCUSDT&limit=5000");
        assert_eq!(url.host, "api.example");
    }
}
