//! Frame headers: parsing server frames and encoding client frames.

use super::WsError;

/// Frame opcodes defined by RFC 6455.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Opcode {
    /// Continues a fragmented message.
    Continuation,
    /// UTF-8 text data.
    Text,
    /// Binary data.
    Binary,
    /// Connection close.
    Close,
    /// Ping; must be answered with a pong carrying the same payload.
    Ping,
    /// Pong.
    Pong,
}

impl Opcode {
    fn from_bits(bits: u8) -> Result<Self, WsError> {
        match bits {
            0x0 => Ok(Self::Continuation),
            0x1 => Ok(Self::Text),
            0x2 => Ok(Self::Binary),
            0x8 => Ok(Self::Close),
            0x9 => Ok(Self::Ping),
            0xA => Ok(Self::Pong),
            other => Err(WsError::UnknownOpcode(other)),
        }
    }

    const fn bits(self) -> u8 {
        match self {
            Self::Continuation => 0x0,
            Self::Text => 0x1,
            Self::Binary => 0x2,
            Self::Close => 0x8,
            Self::Ping => 0x9,
            Self::Pong => 0xA,
        }
    }

    /// Whether this is a control frame (close, ping, pong).
    #[must_use]
    pub const fn is_control(self) -> bool {
        matches!(self, Self::Close | Self::Ping | Self::Pong)
    }
}

/// A parsed server frame header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// Whether this is the final fragment of its message.
    pub fin: bool,
    /// Frame type.
    pub opcode: Opcode,
    /// Length of the header in bytes (2, 4 or 10; servers never mask).
    pub header_len: usize,
    /// Length of the payload in bytes.
    pub payload_len: usize,
}

/// Longest payload a control frame may carry.
pub const MAX_CONTROL_PAYLOAD: usize = 125;

/// Longest possible client frame header: 2 bytes, 8-byte extended length, 4-byte mask.
pub const MAX_CLIENT_HEADER: usize = 14;

/// Parses the header of a frame sent by a server. `Ok(None)` means more bytes are needed.
///
/// # Errors
/// Any protocol violation, or a payload longer than `max_payload`.
pub fn parse_server_header(buf: &[u8], max_payload: usize) -> Result<Option<FrameHeader>, WsError> {
    let (Some(&b0), Some(&b1)) = (buf.first(), buf.get(1)) else {
        return Ok(None);
    };
    if b0 & 0x70 != 0 {
        return Err(WsError::ReservedBits);
    }
    let fin = b0 & 0x80 != 0;
    let opcode = Opcode::from_bits(b0 & 0x0F)?;
    if b1 & 0x80 != 0 {
        return Err(WsError::MaskedServerFrame);
    }
    let (len, header_len) = match b1 & 0x7F {
        LEN_16 => {
            let Some(bytes) = buf.get(2..4).and_then(|b| <[u8; 2]>::try_from(b).ok()) else {
                return Ok(None);
            };
            let len = u64::from(u16::from_be_bytes(bytes));
            if len < u64::from(LEN_16) {
                return Err(WsError::InvalidLength);
            }
            (len, 4)
        }
        LEN_64 => {
            let Some(bytes) = buf.get(2..10).and_then(|b| <[u8; 8]>::try_from(b).ok()) else {
                return Ok(None);
            };
            let len = u64::from_be_bytes(bytes);
            // Must need 64 bits (else non-minimal) and have the top bit clear (RFC 6455 §5.2).
            if u16::try_from(len).is_ok() || len >> 63 != 0 {
                return Err(WsError::InvalidLength);
            }
            (len, 10)
        }
        short => (u64::from(short), 2),
    };
    let control_limit = u64::try_from(MAX_CONTROL_PAYLOAD).unwrap_or(u64::MAX);
    if opcode.is_control() && (!fin || len > control_limit) {
        return Err(WsError::InvalidControlFrame);
    }
    let payload_len = usize::try_from(len)
        .ok()
        .filter(|&l| l <= max_payload)
        .ok_or(WsError::FrameTooLarge {
            len,
            limit: max_payload,
        })?;
    Ok(Some(FrameHeader {
        fin,
        opcode,
        header_len,
        payload_len,
    }))
}

/// Encodes one complete client frame (`fin` set) into `out`, masking the payload with
/// `mask` as clients must. Returns the number of bytes written.
///
/// # Errors
/// [`WsError::BufferTooSmall`] if `out` cannot hold the frame, or
/// [`WsError::InvalidControlFrame`] for a control frame over 125 bytes.
pub fn encode_client_frame(
    opcode: Opcode,
    payload: &[u8],
    mask: [u8; 4],
    out: &mut [u8],
) -> Result<usize, WsError> {
    if opcode.is_control() && payload.len() > MAX_CONTROL_PAYLOAD {
        return Err(WsError::InvalidControlFrame);
    }
    let mut w = Writer { out, pos: 0 };
    w.put(&[FIN | opcode.bits()])?;
    let len = payload.len();
    if let Some(short) = u8::try_from(len).ok().filter(|&l| l < LEN_16) {
        w.put(&[MASKED | short])?;
    } else if let Ok(medium) = u16::try_from(len) {
        w.put(&[MASKED | LEN_16])?;
        w.put(&medium.to_be_bytes())?;
    } else {
        let long = u64::try_from(len).map_err(|_| WsError::InvalidLength)?;
        w.put(&[MASKED | LEN_64])?;
        w.put(&long.to_be_bytes())?;
    }
    w.put(&mask)?;
    let body = w.reserve(len)?;
    for ((dst, src), key) in body.iter_mut().zip(payload).zip(mask.iter().cycle()) {
        *dst = src ^ key;
    }
    Ok(w.pos)
}

const FIN: u8 = 0x80;
const MASKED: u8 = 0x80;
/// Length marker for a 16-bit extended payload length.
const LEN_16: u8 = 0x7E;
/// Length marker for a 64-bit extended payload length.
const LEN_64: u8 = 0x7F;

/// Bounds-checked sequential writer.
struct Writer<'a> {
    out: &'a mut [u8],
    pos: usize,
}

impl Writer<'_> {
    fn reserve(&mut self, len: usize) -> Result<&mut [u8], WsError> {
        let end = self.pos.checked_add(len).ok_or(WsError::BufferTooSmall)?;
        let slot = self
            .out
            .get_mut(self.pos..end)
            .ok_or(WsError::BufferTooSmall)?;
        self.pos = end;
        Ok(slot)
    }

    fn put(&mut self, bytes: &[u8]) -> Result<(), WsError> {
        self.reserve(bytes.len())?.copy_from_slice(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unmasks a client frame the way a server would, returning (opcode bits, payload).
    fn server_view(frame: &[u8]) -> (u8, Vec<u8>) {
        assert_eq!(frame[0] & 0x80, 0x80, "fin");
        assert_eq!(frame[1] & 0x80, 0x80, "client frames must be masked");
        let (len, mut at) = match frame[1] & 0x7F {
            126 => (usize::from(u16::from_be_bytes([frame[2], frame[3]])), 4),
            127 => (
                usize::try_from(u64::from_be_bytes(frame[2..10].try_into().unwrap())).unwrap(),
                10,
            ),
            n => (usize::from(n), 2),
        };
        let mask = [frame[at], frame[at + 1], frame[at + 2], frame[at + 3]];
        at += 4;
        assert_eq!(frame.len(), at + len);
        let payload = frame[at..]
            .iter()
            .zip(mask.iter().cycle())
            .map(|(b, k)| b ^ k)
            .collect();
        (frame[0] & 0x0F, payload)
    }

    #[test]
    fn encodes_masked_frames_of_every_length_class() {
        let mut out = vec![0_u8; 70_000];
        for len in [0_usize, 125, 126, 65_535, 65_536] {
            let payload: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
            let n = encode_client_frame(Opcode::Text, &payload, [1, 2, 3, 4], &mut out).unwrap();
            assert_eq!(server_view(&out[..n]), (0x1, payload), "length {len}");
        }
    }

    #[test]
    fn rejects_oversized_control_frames_and_small_buffers() {
        let mut out = [0_u8; 256];
        assert_eq!(
            encode_client_frame(Opcode::Pong, &[0; 126], [0; 4], &mut out),
            Err(WsError::InvalidControlFrame)
        );
        assert_eq!(
            encode_client_frame(Opcode::Text, &[0; 10], [0; 4], &mut out[..15]),
            Err(WsError::BufferTooSmall)
        );
        assert_eq!(
            encode_client_frame(Opcode::Pong, b"hb", [9; 4], &mut out),
            Ok(8)
        );
    }
}
