//! Turning received bytes into WebSocket events without allocating.
//!
//! The transport reads socket bytes straight into [`WsReader::spare`] and reports how many
//! arrived with [`WsReader::commit`]. [`WsReader::next_event`] then yields events whose
//! payloads borrow the receive buffer. Unfragmented messages (the normal case for venue feeds)
//! are never copied; fragmented ones are reassembled into a second pre-allocated buffer.

use super::WsError;
use super::frame::{FrameHeader, Opcode, parse_server_header};

/// One event from the server. Payloads borrow the reader's buffers until the next call.
#[derive(Debug, PartialEq, Eq)]
pub enum WsEvent<'a> {
    /// A complete text message.
    Text(&'a [u8]),
    /// A complete binary message.
    Binary(&'a [u8]),
    /// A ping. The caller must reply with a pong carrying the same payload.
    Ping(&'a [u8]),
    /// A pong.
    Pong(&'a [u8]),
    /// The server is closing the connection.
    Close {
        /// Status code, if the frame carried one.
        code: Option<u16>,
        /// Close reason bytes (UTF-8 by protocol, not validated here).
        reason: &'a [u8],
    },
}

/// Sizes for a [`WsReader`], allocated once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReaderConfig {
    /// Receive buffer size. Must exceed `max_frame` plus the largest header (10 bytes).
    pub buffer: usize,
    /// Largest single frame payload accepted.
    pub max_frame: usize,
    /// Largest reassembled (fragmented) message accepted.
    pub max_message: usize,
}

/// Incremental, allocation-free WebSocket event reader. See the module docs.
#[derive(Debug)]
pub struct WsReader {
    buf: Vec<u8>,
    /// Start of unconsumed bytes.
    start: usize,
    /// End of received bytes.
    end: usize,
    /// Reassembly buffer for fragmented messages.
    message: Vec<u8>,
    /// Opcode of the fragmented message in progress, if any.
    fragment: Option<Opcode>,
    config: ReaderConfig,
}

/// Largest server frame header (2 bytes plus an 8-byte extended length).
const MAX_SERVER_HEADER: usize = 10;

impl WsReader {
    /// Allocates the reader's buffers.
    ///
    /// # Errors
    /// [`WsError::BufferTooSmall`] if the receive buffer cannot hold a maximum-size frame.
    pub fn new(config: ReaderConfig) -> Result<Self, WsError> {
        if config.max_frame.saturating_add(MAX_SERVER_HEADER) > config.buffer {
            return Err(WsError::BufferTooSmall);
        }
        Ok(Self {
            buf: vec![0; config.buffer],
            start: 0,
            end: 0,
            message: Vec::with_capacity(config.max_message),
            fragment: None,
            config,
        })
    }

    /// Clears all state, for reuse on a new connection.
    pub fn reset(&mut self) {
        self.start = 0;
        self.end = 0;
        self.message.clear();
        self.fragment = None;
    }

    /// Free space to read socket bytes into. Compacts the buffer first when needed, so a
    /// complete maximum-size frame always fits once the previous events are consumed.
    pub fn spare(&mut self) -> &mut [u8] {
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
        } else if self.start > 0 && self.buf.len().saturating_sub(self.end) < self.config.max_frame
        {
            self.buf.copy_within(self.start..self.end, 0);
            self.end = self.end.saturating_sub(self.start);
            self.start = 0;
        }
        self.buf.get_mut(self.end..).unwrap_or_default()
    }

    /// Records that `n` bytes were written into the slice last returned by
    /// [`spare`](Self::spare).
    pub fn commit(&mut self, n: usize) {
        self.end = self.end.saturating_add(n).min(self.buf.len());
    }

    /// Bytes received but not yet consumed as events.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    /// The next complete event, or `Ok(None)` if more bytes are needed.
    ///
    /// # Errors
    /// Any [`WsError`]; the connection must then be dropped and the reader reset.
    pub fn next_event(&mut self) -> Result<Option<WsEvent<'_>>, WsError> {
        let ready = loop {
            let available = self.buf.get(self.start..self.end).unwrap_or_default();
            let Some(header) = parse_server_header(available, self.config.max_frame)? else {
                return Ok(None);
            };
            let frame_len = header.header_len.saturating_add(header.payload_len);
            if available.len() < frame_len {
                return Ok(None);
            }
            let payload = Span::Buf {
                start: self.start.saturating_add(header.header_len),
                end: self.start.saturating_add(frame_len),
            };
            self.start = self.start.saturating_add(frame_len);
            // Data fragments with more to come produce no event: keep reading.
            if let Some(ready) = self.on_frame(header, payload)? {
                break ready;
            }
        };
        Ok(Some(self.event(ready)))
    }

    /// Updates fragment state for one frame and decides which event, if any, it completes.
    fn on_frame(&mut self, header: FrameHeader, payload: Span) -> Result<Option<Ready>, WsError> {
        match header.opcode {
            Opcode::Ping => Ok(Some(Ready::Ping(payload))),
            Opcode::Pong => Ok(Some(Ready::Pong(payload))),
            Opcode::Close => self.close(payload).map(Some),
            Opcode::Text | Opcode::Binary => {
                if self.fragment.is_some() {
                    return Err(WsError::UnexpectedFragment);
                }
                if header.fin {
                    return Ok(Some(Ready::Data(header.opcode, payload)));
                }
                self.fragment = Some(header.opcode);
                self.message.clear();
                self.append(payload)?;
                Ok(None)
            }
            Opcode::Continuation => {
                let opcode = self.fragment.ok_or(WsError::UnexpectedFragment)?;
                self.append(payload)?;
                if !header.fin {
                    return Ok(None);
                }
                self.fragment = None;
                Ok(Some(Ready::Data(opcode, Span::Message)))
            }
        }
    }

    fn slice(&self, span: Span) -> &[u8] {
        match span {
            Span::Buf { start, end } => self.buf.get(start..end).unwrap_or_default(),
            Span::Message => &self.message,
        }
    }

    fn event(&self, ready: Ready) -> WsEvent<'_> {
        match ready {
            Ready::Data(Opcode::Binary, span) => WsEvent::Binary(self.slice(span)),
            Ready::Data(_, span) => WsEvent::Text(self.slice(span)),
            Ready::Ping(span) => WsEvent::Ping(self.slice(span)),
            Ready::Pong(span) => WsEvent::Pong(self.slice(span)),
            Ready::Close(code, span) => WsEvent::Close {
                code,
                reason: self.slice(span),
            },
        }
    }

    fn close(&self, payload: Span) -> Result<Ready, WsError> {
        let Span::Buf { start, end } = payload else {
            return Err(WsError::MalformedClose);
        };
        match self.slice(payload) {
            [] => Ok(Ready::Close(None, payload)),
            [_] => Err(WsError::MalformedClose),
            [hi, lo, ..] => Ok(Ready::Close(
                Some(u16::from_be_bytes([*hi, *lo])),
                Span::Buf {
                    start: start.saturating_add(2),
                    end,
                },
            )),
        }
    }

    /// Appends a fragment without ever growing past the preallocated limit.
    fn append(&mut self, payload: Span) -> Result<(), WsError> {
        let Span::Buf { start, end } = payload else {
            return Err(WsError::UnexpectedFragment);
        };
        let limit = self.config.max_message;
        if self.message.len().saturating_add(end.saturating_sub(start)) > limit {
            return Err(WsError::MessageTooLarge { limit });
        }
        let bytes = self.buf.get(start..end).unwrap_or_default();
        self.message.extend_from_slice(bytes);
        Ok(())
    }
}

/// Where an event's payload lives: in the receive buffer or the reassembly buffer.
#[derive(Clone, Copy, Debug)]
enum Span {
    Buf { start: usize, end: usize },
    Message,
}

/// An event decided but not yet borrowed from the buffers.
#[derive(Clone, Copy, Debug)]
enum Ready {
    Data(Opcode, Span),
    Ping(Span),
    Pong(Span),
    Close(Option<u16>, Span),
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const CONFIG: ReaderConfig = ReaderConfig {
        buffer: 1024,
        max_frame: 512,
        max_message: 2048,
    };

    /// Encodes a server frame (unmasked), as a server would.
    fn server_frame(fin: bool, opcode: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![if fin { 0x80 } else { 0 } | opcode];
        match payload.len() {
            n if n < 126 => out.push(u8::try_from(n).unwrap()),
            n if n <= 0xFFFF => {
                out.push(126);
                out.extend_from_slice(&u16::try_from(n).unwrap().to_be_bytes());
            }
            n => {
                out.push(127);
                out.extend_from_slice(&u64::try_from(n).unwrap().to_be_bytes());
            }
        }
        out.extend_from_slice(payload);
        out
    }

    fn feed(reader: &mut WsReader, bytes: &[u8]) {
        let spare = reader.spare();
        spare[..bytes.len()].copy_from_slice(bytes);
        reader.commit(bytes.len());
    }

    /// Owned copy of an event, so tests can collect several.
    #[derive(Debug, PartialEq, Eq, Clone)]
    enum Owned {
        Text(Vec<u8>),
        Binary(Vec<u8>),
        Ping(Vec<u8>),
        Pong(Vec<u8>),
        Close(Option<u16>, Vec<u8>),
    }

    fn drain(reader: &mut WsReader) -> Result<Vec<Owned>, WsError> {
        let mut events = Vec::new();
        while let Some(event) = reader.next_event()? {
            events.push(match event {
                WsEvent::Text(p) => Owned::Text(p.to_vec()),
                WsEvent::Binary(p) => Owned::Binary(p.to_vec()),
                WsEvent::Ping(p) => Owned::Ping(p.to_vec()),
                WsEvent::Pong(p) => Owned::Pong(p.to_vec()),
                WsEvent::Close { code, reason } => Owned::Close(code, reason.to_vec()),
            });
        }
        Ok(events)
    }

    #[test]
    fn reads_unfragmented_messages_of_every_length_encoding() {
        let mut reader = WsReader::new(ReaderConfig {
            buffer: 70_000,
            max_frame: 66_000,
            max_message: 16,
        })
        .unwrap();
        for len in [0, 125, 126, 65_535, 65_536] {
            let payload = vec![b'x'; len];
            feed(&mut reader, &server_frame(true, 0x1, &payload));
            assert_eq!(
                drain(&mut reader).unwrap(),
                [Owned::Text(payload)],
                "length {len}"
            );
        }
    }

    #[test]
    fn waits_for_partial_frames() {
        let mut reader = WsReader::new(CONFIG).unwrap();
        let frame = server_frame(true, 0x1, b"hello");
        feed(&mut reader, &frame[..3]);
        assert_eq!(reader.next_event(), Ok(None));
        feed(&mut reader, &frame[3..]);
        assert_eq!(reader.next_event(), Ok(Some(WsEvent::Text(b"hello"))));
        assert_eq!(reader.buffered(), 0);
    }

    #[test]
    fn reassembles_fragments_around_interleaved_control_frames() {
        let mut reader = WsReader::new(CONFIG).unwrap();
        let mut bytes = server_frame(false, 0x2, b"ab");
        bytes.extend(server_frame(true, 0x9, b"ping"));
        bytes.extend(server_frame(false, 0x0, b"cd"));
        bytes.extend(server_frame(true, 0x0, b"ef"));
        feed(&mut reader, &bytes);
        assert_eq!(
            drain(&mut reader).unwrap(),
            [
                Owned::Ping(b"ping".to_vec()),
                Owned::Binary(b"abcdef".to_vec())
            ]
        );
    }

    #[test]
    fn decodes_close_frames() {
        let mut reader = WsReader::new(CONFIG).unwrap();
        let mut bytes = server_frame(true, 0x8, &[0x03, 0xE8, b'b', b'y', b'e']);
        bytes.extend(server_frame(true, 0x8, &[]));
        bytes.extend(server_frame(true, 0xA, b"p"));
        feed(&mut reader, &bytes);
        assert_eq!(
            drain(&mut reader).unwrap(),
            [
                Owned::Close(Some(1000), b"bye".to_vec()),
                Owned::Close(None, vec![]),
                Owned::Pong(b"p".to_vec())
            ]
        );
    }

    #[test]
    fn rejects_protocol_violations() {
        let cases: [(Vec<u8>, WsError); 9] = [
            (vec![0xC1, 0x00], WsError::ReservedBits),
            (vec![0x83, 0x00], WsError::UnknownOpcode(3)),
            (vec![0x81, 0x80, 0, 0, 0, 0], WsError::MaskedServerFrame),
            (vec![0x81, 126, 0x00, 0x05], WsError::InvalidLength),
            (
                vec![0x81, 127, 0, 0, 0, 0, 0, 0, 0x01, 0x00],
                WsError::InvalidLength,
            ),
            (vec![0x09, 0x00], WsError::InvalidControlFrame),
            (
                server_frame(true, 0x9, &[0; 126]),
                WsError::InvalidControlFrame,
            ),
            (server_frame(true, 0x0, b"x"), WsError::UnexpectedFragment),
            (server_frame(true, 0x8, b"x"), WsError::MalformedClose),
        ];
        for (bytes, expected) in cases {
            let mut reader = WsReader::new(CONFIG).unwrap();
            feed(&mut reader, &bytes);
            assert_eq!(drain(&mut reader), Err(expected), "{bytes:?}");
        }
    }

    #[test]
    fn enforces_size_limits() {
        let mut reader = WsReader::new(CONFIG).unwrap();
        feed(&mut reader, &server_frame(true, 0x1, &[0; 513])[..4]);
        assert!(matches!(
            drain(&mut reader),
            Err(WsError::FrameTooLarge { len: 513, .. })
        ));

        let mut reader = WsReader::new(ReaderConfig {
            max_message: 4,
            ..CONFIG
        })
        .unwrap();
        let mut bytes = server_frame(false, 0x1, b"abc");
        bytes.extend(server_frame(true, 0x0, b"de"));
        feed(&mut reader, &bytes);
        assert_eq!(
            drain(&mut reader),
            Err(WsError::MessageTooLarge { limit: 4 })
        );

        let mut reader = WsReader::new(CONFIG).unwrap();
        let mut bytes = server_frame(false, 0x1, b"a");
        bytes.extend(server_frame(true, 0x1, b"b"));
        feed(&mut reader, &bytes);
        assert_eq!(drain(&mut reader), Err(WsError::UnexpectedFragment));

        assert_eq!(
            WsReader::new(ReaderConfig {
                buffer: 100,
                max_frame: 100,
                max_message: 1
            })
            .unwrap_err(),
            WsError::BufferTooSmall
        );
    }

    #[test]
    fn compaction_keeps_max_size_frames_fitting() {
        let mut reader = WsReader::new(CONFIG).unwrap();
        let big = server_frame(true, 0x2, &[7; 512]);
        for _ in 0..20 {
            // A small frame then a partial big one leaves the unconsumed tail mid-buffer.
            let mut bytes = server_frame(true, 0x1, b"s");
            bytes.extend_from_slice(&big[..300]);
            feed(&mut reader, &bytes);
            assert_eq!(drain(&mut reader).unwrap(), [Owned::Text(b"s".to_vec())]);
            feed(&mut reader, &big[300..]);
            assert_eq!(drain(&mut reader).unwrap(), [Owned::Binary(vec![7; 512])]);
        }
    }

    proptest! {
        /// Any sequence of valid frames, delivered in arbitrary chunks, yields the same
        /// events as delivering it at once.
        #[test]
        fn chunking_never_changes_the_events(
            messages in proptest::collection::vec(
                (proptest::collection::vec(any::<u8>(), 0..300), 1_usize..4, any::<bool>()),
                1..12,
            ),
            chunk in 1_usize..64,
        ) {
            let mut stream = Vec::new();
            let mut expected = Vec::new();
            for (payload, parts, with_ping) in &messages {
                let pieces: Vec<&[u8]> = payload.chunks((payload.len() / parts).max(1)).collect();
                let pieces = if pieces.is_empty() { vec![&[][..]] } else { pieces };
                for (i, piece) in pieces.iter().enumerate() {
                    let last = i + 1 == pieces.len();
                    let opcode = if i == 0 { 0x2 } else { 0x0 };
                    stream.extend(server_frame(last, opcode, piece));
                    if *with_ping && i == 0 && !last {
                        stream.extend(server_frame(true, 0x9, b"hb"));
                        expected.push(Owned::Ping(b"hb".to_vec()));
                    }
                }
                expected.push(Owned::Binary(payload.clone()));
            }
            let mut reader = WsReader::new(CONFIG).unwrap();
            let mut events = Vec::new();
            for piece in stream.chunks(chunk) {
                feed(&mut reader, piece);
                events.extend(drain(&mut reader).unwrap());
            }
            prop_assert_eq!(events, expected);
            prop_assert_eq!(reader.buffered(), 0);
        }
    }
}
