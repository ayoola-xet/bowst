//! Client-side WebSocket protocol (RFC 6455), without I/O and without per-message allocation.
//!
//! General-purpose WebSocket libraries allocate a buffer for every message received. On the
//! market-data thread that breaks the no-allocation rule (CLAUDE.md §1.6), so this module
//! implements the protocol directly over a pre-allocated receive buffer (ADR 0006):
//!
//! - [`handshake`]: the HTTP upgrade request and strict validation of the server's reply.
//! - [`frame`]: frame header parsing and client frame encoding (client frames are masked).
//! - [`reader`]: turns received bytes into events, borrowing message payloads from its buffer.
//!
//! The transport (TCP + TLS) lives elsewhere and only moves bytes in and out. Everything here
//! is pure and fuzzed.
//!
//! Extensions and subprotocols are never negotiated, so compressed or reserved-bit frames are
//! protocol errors.

pub mod frame;
pub mod handshake;
pub mod reader;

/// A WebSocket protocol violation or a limit exceeded. The connection must be dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WsError {
    /// The server's upgrade response was not a valid `101 Switching Protocols` for our key.
    #[error("invalid handshake response: {0}")]
    Handshake(&'static str),
    /// The upgrade response headers exceeded the size limit.
    #[error("handshake response headers too large")]
    HandshakeTooLarge,
    /// A reserved header bit was set; no extension that defines it was negotiated.
    #[error("reserved bits set in frame header")]
    ReservedBits,
    /// An opcode not defined by RFC 6455.
    #[error("unknown opcode {0:#x}")]
    UnknownOpcode(u8),
    /// Servers must not mask frames.
    #[error("server sent a masked frame")]
    MaskedServerFrame,
    /// A payload length that was not encoded in the fewest possible bytes, or had its top bit set.
    #[error("non-minimal or invalid payload length")]
    InvalidLength,
    /// A control frame that was fragmented or longer than 125 bytes.
    #[error("invalid control frame")]
    InvalidControlFrame,
    /// A frame larger than the configured limit.
    #[error("frame of {len} bytes exceeds limit of {limit}")]
    FrameTooLarge {
        /// Declared payload length.
        len: u64,
        /// Configured limit.
        limit: usize,
    },
    /// A fragmented message larger than the configured limit.
    #[error("message exceeds limit of {limit} bytes")]
    MessageTooLarge {
        /// Configured limit.
        limit: usize,
    },
    /// Fragments out of order: a continuation with nothing to continue, or a new data frame
    /// before the previous message finished.
    #[error("unexpected fragment")]
    UnexpectedFragment,
    /// A close frame with a one-byte payload, which cannot hold a status code.
    #[error("malformed close frame")]
    MalformedClose,
    /// An output buffer too small for the frame being encoded.
    #[error("output buffer too small")]
    BufferTooSmall,
}
