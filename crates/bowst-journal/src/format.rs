//! On-disk format. All integers are little-endian.
//!
//! A journal is a directory of segment files, read in file-name order. Each segment is:
//!
//! ```text
//! file header (32 bytes)
//!   magic      8   b"BOWSTJNL"
//!   version    4   1
//!   segment    4   index of this segment, from 0
//!   created    8   wall-clock nanoseconds when the segment was opened
//!   reserved   8   zero
//! records, back to back:
//!   kind       2   what the payload is (see `Kind`)
//!   source     2   which component or instrument produced it
//!   length     4   payload bytes
//!   mono       8   monotonic receive time, nanoseconds
//!   wall       8   wall-clock receive time, nanoseconds since the Unix epoch
//!   crc        4   CRC-32 of the 24 header bytes before it and the payload
//!   header_crc 4   CRC-32 of the 28 header bytes before it
//!   payload    length bytes
//! ```
//!
//! The header has its own checksum, so a damaged length is detected before it is trusted: a
//! record is only "incomplete" when its header is intact and the file really ends early.
//!
//! A crash can leave the last record of the last segment incomplete. Readers accept that one
//! torn record and nothing else: a bad checksum or a short record anywhere else is corruption.

use bowst_core::{MonoTime, WallTime};

/// File magic.
pub const MAGIC: &[u8; 8] = b"BOWSTJNL";
/// Current format version.
pub const VERSION: u32 = 1;
/// File header length.
pub const FILE_HEADER_LEN: usize = 32;
/// Record header length.
pub const RECORD_HEADER_LEN: usize = 32;
/// Largest payload a record may carry (16 MiB). Bounds memory when reading hostile files.
pub const MAX_PAYLOAD: usize = 16 << 20;

/// What a record's payload is. A single registry, so kinds never collide across crates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Kind(pub u16);

impl Kind {
    /// Journal-internal: records were dropped before this point. Payload: `u64` count.
    pub const GAP: Self = Self(0x0001);
    /// A process or session started. Payload: UTF-8 description (version, configuration).
    pub const SESSION_START: Self = Self(0x0002);
    /// A raw market-data message exactly as received. Source: venue ID.
    pub const MD_MESSAGE: Self = Self(0x0100);
    /// A raw REST depth snapshot at the moment it was applied. Payload: instrument ID (`u32`)
    /// then the raw response body. Source: venue ID.
    pub const MD_SNAPSHOT: Self = Self(0x0101);
    /// A market-data status change. Payload: UTF-8 text. Source: venue ID.
    pub const MD_STATUS: Self = Self(0x0102);
    /// The market-data connection ended and every book was reset. No payload. Source: venue ID.
    pub const MD_RESET: Self = Self(0x0103);
    /// Verification of one instrument's book started: its deltas now also feed a verification
    /// book. Payload: instrument ID (`u32`). Source: venue ID.
    pub const MD_VERIFY_START: Self = Self(0x0104);
    /// A raw REST depth snapshot loaded into the verification book. Payload: instrument ID
    /// (`u32`) then the raw response body. Source: venue ID.
    pub const MD_VERIFY_SNAPSHOT: Self = Self(0x0105);
}

/// A record's header fields (the checksum is handled by the writer and reader).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordHeader {
    /// Payload type.
    pub kind: Kind,
    /// Producing component or venue.
    pub source: u16,
    /// Monotonic receive time.
    pub mono: MonoTime,
    /// Wall-clock receive time.
    pub wall: WallTime,
}

/// Malformed journal data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FormatError {
    /// Not a journal file, or an unsupported version.
    #[error("bad file header")]
    BadFileHeader,
    /// A record's checksum does not match its contents.
    #[error("checksum mismatch in record at byte {offset}")]
    Checksum {
        /// Offset of the record in its segment.
        offset: u64,
    },
    /// A record declares a payload larger than [`MAX_PAYLOAD`].
    #[error("record at byte {offset} declares {len} bytes")]
    TooLarge {
        /// Offset of the record in its segment.
        offset: u64,
        /// Declared length.
        len: u64,
    },
    /// A record header's own checksum does not match: the header (including its length)
    /// cannot be trusted.
    #[error("header checksum mismatch in record at byte {offset}")]
    HeaderChecksum {
        /// Offset of the record in its segment.
        offset: u64,
    },
}

fn u16_at(bytes: &[u8], at: usize) -> u16 {
    bytes
        .get(at..at.saturating_add(2))
        .and_then(|b| <[u8; 2]>::try_from(b).ok())
        .map_or(0, u16::from_le_bytes)
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    bytes
        .get(at..at.saturating_add(4))
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map_or(0, u32::from_le_bytes)
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    bytes
        .get(at..at.saturating_add(8))
        .and_then(|b| <[u8; 8]>::try_from(b).ok())
        .map_or(0, u64::from_le_bytes)
}

/// Encodes a segment file header.
#[must_use]
pub fn encode_file_header(segment: u32, created: WallTime) -> [u8; FILE_HEADER_LEN] {
    let mut out = [0_u8; FILE_HEADER_LEN];
    let fields: [&[u8]; 4] = [
        MAGIC,
        &VERSION.to_le_bytes(),
        &segment.to_le_bytes(),
        &created.as_nanos().to_le_bytes(),
    ];
    let mut at = 0_usize;
    for field in fields {
        let end = at.saturating_add(field.len());
        if let Some(dst) = out.get_mut(at..end) {
            dst.copy_from_slice(field);
        }
        at = end;
    }
    out
}

/// Decodes a segment file header, returning its segment index.
///
/// # Errors
/// [`FormatError::BadFileHeader`].
pub fn decode_file_header(bytes: &[u8]) -> Result<u32, FormatError> {
    if bytes.len() < FILE_HEADER_LEN
        || bytes.get(..8) != Some(MAGIC.as_slice())
        || u32_at(bytes, 8) != VERSION
    {
        return Err(FormatError::BadFileHeader);
    }
    Ok(u32_at(bytes, 12))
}

/// Writes a record header with zero checksums into `out` (at least
/// [`RECORD_HEADER_LEN`] bytes). The writer fills in the checksums with [`seal`].
pub fn encode_record_header(header: RecordHeader, payload_len: u32, out: &mut [u8]) {
    let fields: [&[u8]; 7] = [
        &header.kind.0.to_le_bytes(),
        &header.source.to_le_bytes(),
        &payload_len.to_le_bytes(),
        &header.mono.as_nanos().to_le_bytes(),
        &header.wall.as_nanos().to_le_bytes(),
        &0_u32.to_le_bytes(),
        &0_u32.to_le_bytes(),
    ];
    let mut at = 0_usize;
    for field in fields {
        let end = at.saturating_add(field.len());
        if let Some(dst) = out.get_mut(at..end) {
            dst.copy_from_slice(field);
        }
        at = end;
    }
}

/// Byte offset of the record checksum inside a record header.
pub const CRC_AT: usize = 24;
/// Byte offset of the header checksum inside a record header.
pub const HEADER_CRC_AT: usize = 28;

/// CRC-32 of a complete record (header plus payload), excluding the checksum field itself.
#[must_use]
pub fn record_checksum(record: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(record.get(..CRC_AT).unwrap_or_default());
    hasher.update(record.get(RECORD_HEADER_LEN..).unwrap_or_default());
    hasher.finalize()
}

/// CRC-32 of the first 28 header bytes (everything before the header checksum), given the
/// first 24 bytes and the record checksum separately so a writer need not copy the header.
#[must_use]
pub fn header_checksum(first_24: &[u8], record_crc: u32) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(first_24.get(..CRC_AT).unwrap_or_default());
    hasher.update(&record_crc.to_le_bytes());
    hasher.finalize()
}

/// Computes and stores both checksums of a complete record (header plus payload).
pub fn seal(record: &mut [u8]) {
    let crc = record_checksum(record);
    let header_crc = header_checksum(record, crc);
    if let Some(dst) = record.get_mut(CRC_AT..HEADER_CRC_AT) {
        dst.copy_from_slice(&crc.to_le_bytes());
    }
    if let Some(dst) = record.get_mut(HEADER_CRC_AT..RECORD_HEADER_LEN) {
        dst.copy_from_slice(&header_crc.to_le_bytes());
    }
}

/// Result of decoding the record at the start of a buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decoded {
    /// A complete, valid record: its header and the length of header plus payload.
    Record {
        /// Header fields.
        header: RecordHeader,
        /// Total bytes the record occupies (header plus payload).
        len: usize,
    },
    /// The buffer ends before the record does (a torn tail if at the end of the journal).
    Incomplete,
}

/// Decodes and verifies the record at the start of `bytes`. `offset` is used for errors only.
///
/// # Errors
/// [`FormatError`] for a bad header or record checksum, or an oversized length.
pub fn decode_record(bytes: &[u8], offset: u64) -> Result<Decoded, FormatError> {
    if bytes.len() < RECORD_HEADER_LEN {
        return Ok(Decoded::Incomplete);
    }
    if header_checksum(bytes, u32_at(bytes, CRC_AT)) != u32_at(bytes, HEADER_CRC_AT) {
        return Err(FormatError::HeaderChecksum { offset });
    }
    let payload_len = u32_at(bytes, 4);
    let len = usize::try_from(payload_len).unwrap_or(usize::MAX);
    if len > MAX_PAYLOAD {
        return Err(FormatError::TooLarge {
            offset,
            len: u64::from(payload_len),
        });
    }
    let total = RECORD_HEADER_LEN.saturating_add(len);
    let Some(record) = bytes.get(..total) else {
        return Ok(Decoded::Incomplete);
    };
    if record_checksum(record) != u32_at(record, CRC_AT) {
        return Err(FormatError::Checksum { offset });
    }
    Ok(Decoded::Record {
        header: RecordHeader {
            kind: Kind(u16_at(record, 0)),
            source: u16_at(record, 2),
            mono: MonoTime::from_nanos(u64_at(record, 8)),
            wall: WallTime::from_nanos(u64_at(record, 16)),
        },
        len: total,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(kind: u16, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0_u8; RECORD_HEADER_LEN + payload.len()];
        let header = RecordHeader {
            kind: Kind(kind),
            source: 7,
            mono: MonoTime::from_nanos(11),
            wall: WallTime::from_nanos(22),
        };
        encode_record_header(header, u32::try_from(payload.len()).unwrap(), &mut out);
        out[RECORD_HEADER_LEN..].copy_from_slice(payload);
        seal(&mut out);
        out
    }

    #[test]
    fn file_header_round_trips() {
        let header = encode_file_header(42, WallTime::from_nanos(9));
        assert_eq!(decode_file_header(&header), Ok(42));
        let mut bad = header;
        bad[0] = b'X';
        assert_eq!(decode_file_header(&bad), Err(FormatError::BadFileHeader));
        let mut future = header;
        future[8] = 2;
        assert_eq!(decode_file_header(&future), Err(FormatError::BadFileHeader));
        assert_eq!(
            decode_file_header(&header[..31]),
            Err(FormatError::BadFileHeader)
        );
    }

    #[test]
    fn records_round_trip_and_detect_corruption() {
        let bytes = record(0x0100, b"payload");
        let Ok(Decoded::Record { header, len }) = decode_record(&bytes, 0) else {
            panic!("expected a record");
        };
        assert_eq!(
            (header.kind, header.source, len),
            (Kind(0x0100), 7, bytes.len())
        );
        assert_eq!((header.mono.as_nanos(), header.wall.as_nanos()), (11, 22));

        // Any single-bit flip anywhere is detected; a flipped length is caught by the header
        // checksum rather than mistaken for a truncated record.
        for flip in 0..bytes.len() {
            for bit in 0..8 {
                let mut corrupt = bytes.clone();
                corrupt[flip] ^= 1 << bit;
                assert!(
                    decode_record(&corrupt, 5).is_err(),
                    "flip at byte {flip} bit {bit}"
                );
            }
        }
    }

    #[test]
    fn short_buffers_are_incomplete_not_errors() {
        let bytes = record(1, b"abc");
        for cut in [0, 10, RECORD_HEADER_LEN, bytes.len() - 1] {
            assert_eq!(
                decode_record(&bytes[..cut], 0),
                Ok(Decoded::Incomplete),
                "cut {cut}"
            );
        }
    }

    #[test]
    fn rejects_oversized_lengths_before_reading_them() {
        let mut bytes = record(1, b"");
        bytes[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        // Re-seal so both checksums are valid: the length itself must be rejected.
        seal(&mut bytes);
        assert!(matches!(
            decode_record(&bytes, 0),
            Err(FormatError::TooLarge { .. })
        ));
    }
}
