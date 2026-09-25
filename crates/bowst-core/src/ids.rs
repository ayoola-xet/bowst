//! Venue, instrument and client-order identifiers.

use core::fmt;

/// A supported trading venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum VenueId {
    /// Binance Spot.
    Binance,
    /// Bybit Spot.
    Bybit,
}

impl VenueId {
    /// Every supported venue.
    pub const ALL: [Self; 2] = [Self::Binance, Self::Bybit];

    /// Stable lowercase name used in config, logs and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
        }
    }

    /// Stable numeric code, used as the source field of journal records. Never reuse a code.
    #[must_use]
    pub const fn code(self) -> u16 {
        match self {
            Self::Binance => 1,
            Self::Bybit => 2,
        }
    }

    /// Looks a venue up by its [`as_str`](Self::as_str) name.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|v| v.as_str() == name)
    }
}

impl fmt::Display for VenueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Dense process-local index of an instrument (one venue + one symbol), assigned at startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct InstrumentId(u32);

impl InstrumentId {
    /// Wraps an index.
    #[must_use]
    pub const fn new(index: u32) -> Self {
        Self(index)
    }

    /// The index.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Prefix on every client order ID Bowst sends, so our orders are recognizable on the venue.
const PREFIX: &[u8; 2] = b"bw";
const HEX: &[u8; 16] = b"0123456789abcdef";
const HEX_DIGITS: usize = 16;

/// Length of an encoded client order ID: prefix plus 16 hex digits. Fits every supported
/// venue's limit (36 characters on both Binance and Bybit).
pub const CLIENT_ORDER_ID_LEN: usize = PREFIX.len() + HEX_DIGITS;

/// Our identifier for an order: a 32-bit session number and a 32-bit sequence within it.
///
/// The session is chosen at startup (normally the start time in Unix seconds), so IDs stay
/// unique across restarts as long as one engine owns a venue account at a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientOrderId {
    session: u32,
    sequence: u32,
}

impl ClientOrderId {
    /// Builds an ID from its parts.
    #[must_use]
    pub const fn new(session: u32, sequence: u32) -> Self {
        Self { session, sequence }
    }

    /// The session part.
    #[must_use]
    pub const fn session(self) -> u32 {
        self.session
    }

    /// The sequence part.
    #[must_use]
    pub const fn sequence(self) -> u32 {
        self.sequence
    }

    /// Encodes as venue text, for example `bw65f1a2c000000001`. Never allocates.
    #[must_use]
    pub fn encode(self) -> ClientOrderIdText {
        let mut bytes = [0_u8; CLIENT_ORDER_ID_LEN];
        let (prefix, digits) = bytes.split_at_mut(PREFIX.len());
        prefix.copy_from_slice(PREFIX);
        let (session, sequence) = digits.split_at_mut(HEX_DIGITS / 2);
        write_hex(self.session, session);
        write_hex(self.sequence, sequence);
        ClientOrderIdText(bytes)
    }

    /// Decodes venue text produced by [`encode`](Self::encode). Returns `None` for IDs we did
    /// not generate, which lets reconciliation spot foreign orders on the account.
    #[must_use]
    pub fn decode(text: &str) -> Option<Self> {
        let digits = text.strip_prefix(core::str::from_utf8(PREFIX).ok()?)?;
        if digits.len() != HEX_DIGITS || !digits.bytes().all(|b| HEX.contains(&b)) {
            return None;
        }
        let (session, sequence) = digits.split_at(HEX_DIGITS / 2);
        Some(Self {
            session: u32::from_str_radix(session, 16).ok()?,
            sequence: u32::from_str_radix(sequence, 16).ok()?,
        })
    }
}

/// Writes `value` as lowercase hex into `out`, most significant nibble first.
fn write_hex(value: u32, out: &mut [u8]) {
    let mut rest = value;
    for slot in out.iter_mut().rev() {
        *slot = HEX
            .get(usize::from(rest.to_le_bytes()[0] & 0xf))
            .copied()
            .unwrap_or(b'0');
        rest = rest.wrapping_shr(4);
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.encode().as_str())
    }
}

/// Stack-allocated text form of a [`ClientOrderId`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientOrderIdText([u8; CLIENT_ORDER_ID_LEN]);

impl ClientOrderIdText {
    /// The encoded text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        // Only ASCII from `PREFIX` and `HEX` is ever written.
        core::str::from_utf8(&self.0).unwrap_or("")
    }
}

/// Hands out sequential [`ClientOrderId`]s for one session.
#[derive(Debug)]
pub struct ClientOrderIdGen {
    session: u32,
    next: Option<u32>,
}

impl ClientOrderIdGen {
    /// Starts a session at sequence 0.
    #[must_use]
    pub const fn new(session: u32) -> Self {
        Self {
            session,
            next: Some(0),
        }
    }

    /// The next ID, or `None` once the 2^32 sequence numbers of this session are used up.
    /// IDs are never reused: an exhausted generator stays exhausted and the caller must stop
    /// sending orders.
    #[allow(clippy::should_implement_trait)] // Not an iterator: exhaustion is an error state.
    pub fn next(&mut self) -> Option<ClientOrderId> {
        let sequence = self.next?;
        self.next = sequence.checked_add(1);
        Some(ClientOrderId::new(self.session, sequence))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn venue_names_round_trip() {
        for venue in VenueId::ALL {
            assert_eq!(VenueId::from_name(venue.as_str()), Some(venue));
        }
        assert_eq!(VenueId::from_name("unknown"), None);
    }

    #[test]
    fn encodes_known_value() {
        let id = ClientOrderId::new(0x65f1_a2c0, 1);
        assert_eq!(id.encode().as_str(), "bw65f1a2c000000001");
        assert_eq!(id.session(), 0x65f1_a2c0);
        assert_eq!(id.sequence(), 1);
    }

    #[test]
    fn rejects_foreign_ids() {
        for text in [
            "",
            "bw",
            "web_abc",
            "bw65F1A2C000000001",
            "bw65f1a2c0000000011",
            "xx65f1a2c000000001",
        ] {
            assert_eq!(ClientOrderId::decode(text), None, "input {text:?}");
        }
    }

    #[test]
    fn generator_stops_instead_of_wrapping() {
        let mut ids = ClientOrderIdGen {
            session: 7,
            next: Some(u32::MAX),
        };
        assert_eq!(ids.next(), Some(ClientOrderId::new(7, u32::MAX)));
        assert_eq!(ids.next(), None);
        assert_eq!(ids.next(), None);
    }

    proptest! {
        #[test]
        fn ids_round_trip(session: u32, sequence: u32) {
            let id = ClientOrderId::new(session, sequence);
            prop_assert_eq!(ClientOrderId::decode(id.encode().as_str()), Some(id));
        }
    }
}
