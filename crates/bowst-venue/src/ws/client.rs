//! A WebSocket client over a [`Stream`]: connection setup plus non-blocking polling.
//!
//! Usage on the market-data thread:
//!
//! ```no_run
//! # use std::time::Duration;
//! # use bowst_venue::net::{NetError, TlsConfig, Url};
//! # use bowst_venue::ws::client::WsClient;
//! # use bowst_venue::ws::frame::Opcode;
//! # use bowst_venue::ws::reader::{ReaderConfig, WsEvent};
//! # fn main() -> Result<(), NetError> {
//! # let url = Url::parse("wss://stream.example/ws")?;
//! # let tls = TlsConfig::from_platform_roots()?;
//! # let config = ReaderConfig { buffer: 1 << 16, max_frame: 1 << 15, max_message: 1 << 16 };
//! let mut client = WsClient::connect(&url, &tls, config, Duration::from_secs(5))?;
//! let mut ping = [0_u8; 125];
//! loop {
//!     // Drain everything already received before reading the socket again.
//!     while let Some(event) = client.next_event()? {
//!         match event {
//!             WsEvent::Text(message) => { /* decode and apply */ }
//!             WsEvent::Ping(payload) => {
//!                 // The payload borrows the client, so copy it before replying.
//!                 let n = payload.len();
//!                 ping[..n].copy_from_slice(payload);
//!                 client.send(Opcode::Pong, &ping[..n])?;
//!             }
//!             WsEvent::Close { .. } => return Err(NetError::Closed),
//!             WsEvent::Binary(_) | WsEvent::Pong(_) => {}
//!         }
//!     }
//!     client.fill()?; // one non-blocking read
//! }
//! # }
//! ```
//!
//! Reading and event parsing are separate calls so the caller controls exactly when the
//! socket is touched. Nothing here allocates after `connect`.

use std::io::{ErrorKind, Read, Write};
use std::time::{Duration, Instant};

use super::frame::{MAX_CLIENT_HEADER, MAX_CONTROL_PAYLOAD, Opcode, encode_client_frame};
use super::handshake::Handshake;
use super::reader::{ReaderConfig, WsEvent, WsReader};
use crate::net::{NetError, Stream, TlsConfig, Url};

/// Largest client frame [`WsClient::send`] accepts (subscriptions and control frames).
pub const MAX_SEND_PAYLOAD: usize = 4096;

/// How long a blocked write of one small frame may take before the connection is dropped.
const SEND_DEADLINE: Duration = Duration::from_secs(1);

/// A connected WebSocket. See the module docs.
#[derive(Debug)]
pub struct WsClient {
    stream: Stream,
    reader: WsReader,
    tls: TlsConfig,
    out: Vec<u8>,
}

impl WsClient {
    /// Connects, performs the upgrade handshake (blocking, bounded by `timeout`), then
    /// switches the socket to non-blocking mode.
    ///
    /// # Errors
    /// Connection, TLS, handshake or I/O failures.
    pub fn connect(
        url: &Url,
        tls: &TlsConfig,
        config: ReaderConfig,
        timeout: Duration,
    ) -> Result<Self, NetError> {
        let mut stream = Stream::connect(url, tls, timeout)?;
        let mut key = [0_u8; 16];
        tls.fill_random(&mut key)?;
        let handshake = Handshake::new(key);
        let mut request = String::new();
        handshake.write_request(&url.host_header(), &url.path, &mut request);
        stream
            .write_all(request.as_bytes())
            .map_err(NetError::io("send upgrade request"))?;

        // Read the response headers; anything after them is already WebSocket frames.
        let mut response = Vec::with_capacity(1024);
        let mut chunk = [0_u8; 1024];
        let header_len = loop {
            let n = match stream.read(&mut chunk) {
                Ok(0) => return Err(NetError::Closed),
                Ok(n) => n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                    return Err(NetError::Timeout("WebSocket handshake"));
                }
                Err(e) => {
                    return Err(NetError::Io {
                        op: "read upgrade response",
                        source: e,
                    });
                }
            };
            response.extend_from_slice(chunk.get(..n).unwrap_or_default());
            if let Some(len) = handshake.parse_response(&response)? {
                break len;
            }
        };

        let mut reader = WsReader::new(config)?;
        let leftover = response.get(header_len..).unwrap_or_default();
        let spare = reader.spare();
        let n = leftover.len().min(spare.len());
        spare
            .get_mut(..n)
            .unwrap_or_default()
            .copy_from_slice(leftover.get(..n).unwrap_or_default());
        reader.commit(n);

        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            reader,
            tls: tls.clone(),
            out: vec![0; MAX_SEND_PAYLOAD.saturating_add(MAX_CLIENT_HEADER)],
        })
    }

    /// Performs one non-blocking read into the receive buffer. Returns the number of bytes
    /// read (0 if none were available).
    ///
    /// Drain [`next_event`](Self::next_event) before calling this again: if the peer has
    /// closed, this returns [`NetError::Closed`], and events already received but not yet
    /// consumed would otherwise be skipped.
    ///
    /// # Errors
    /// [`NetError::Closed`] when the peer closed the connection, or [`NetError::Io`].
    pub fn fill(&mut self) -> Result<usize, NetError> {
        let spare = self.reader.spare();
        if spare.is_empty() {
            // Unconsumed events fill the buffer; the caller must drain them first.
            return Ok(0);
        }
        match self.stream.read(spare) {
            Ok(0) => Err(NetError::Closed),
            Ok(n) => {
                self.reader.commit(n);
                Ok(n)
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => Ok(0),
            Err(e) => Err(NetError::Io {
                op: "read WebSocket",
                source: e,
            }),
        }
    }

    /// The next complete event from bytes already read, if any.
    ///
    /// # Errors
    /// [`NetError::WebSocket`] on a protocol violation; the connection must be dropped.
    pub fn next_event(&mut self) -> Result<Option<WsEvent<'_>>, NetError> {
        Ok(self.reader.next_event()?)
    }

    /// Sends one frame (masked, with a fresh random mask). Waits at most one second for a
    /// full socket buffer to drain; only small, rare frames are sent on this path.
    ///
    /// # Errors
    /// Frame too large, or I/O failure / timeout (drop the connection).
    pub fn send(&mut self, opcode: Opcode, payload: &[u8]) -> Result<(), NetError> {
        if payload.len() > MAX_SEND_PAYLOAD
            || (opcode.is_control() && payload.len() > MAX_CONTROL_PAYLOAD)
        {
            return Err(crate::ws::WsError::BufferTooSmall.into());
        }
        let mut mask = [0_u8; 4];
        self.tls.fill_random(&mut mask)?;
        let len = encode_client_frame(opcode, payload, mask, &mut self.out)?;
        let mut pending = self.out.get(..len).unwrap_or_default();
        let started = Instant::now();
        while !pending.is_empty() {
            match self.stream.write(pending) {
                Ok(0) => return Err(NetError::Closed),
                Ok(n) => pending = pending.get(n..).unwrap_or_default(),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
                    if started.elapsed() >= SEND_DEADLINE {
                        return Err(NetError::Timeout("sending WebSocket frame"));
                    }
                    std::hint::spin_loop();
                }
                Err(e) => {
                    return Err(NetError::Io {
                        op: "write WebSocket",
                        source: e,
                    });
                }
            }
        }
        loop {
            match self.stream.flush() {
                Ok(()) => return Ok(()),
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted) => {
                    if started.elapsed() >= SEND_DEADLINE {
                        return Err(NetError::Timeout("flushing WebSocket frame"));
                    }
                    std::hint::spin_loop();
                }
                Err(e) => {
                    return Err(NetError::Io {
                        op: "flush WebSocket",
                        source: e,
                    });
                }
            }
        }
    }

    /// Sends a close frame (best effort) before the client is dropped.
    pub fn close(&mut self) {
        // 1000 = normal closure.
        let _ = self.send(Opcode::Close, &1000_u16.to_be_bytes());
    }
}
