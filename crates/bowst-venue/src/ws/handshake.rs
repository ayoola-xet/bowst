//! The HTTP/1.1 upgrade that opens a WebSocket connection.
//!
//! The server's reply is validated strictly: status 101, `Upgrade: websocket`,
//! `Connection: Upgrade`, and a `Sec-WebSocket-Accept` value derived from our key. Any
//! extension or subprotocol in the reply is rejected because none was requested.

use core::fmt::Write as _;

use super::WsError;

/// Magic value from RFC 6455 §1.3 used to derive `Sec-WebSocket-Accept`.
const ACCEPT_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Largest upgrade response header block accepted.
pub const MAX_RESPONSE_HEADERS: usize = 8 * 1024;

/// Length of a base64-encoded 16-byte key.
const KEY_LEN: usize = 24;

/// Length of a base64-encoded SHA-1 digest.
const ACCEPT_LEN: usize = 28;

/// One upgrade attempt: the random key and the accept value the server must echo.
#[derive(Clone, Copy, Debug)]
pub struct Handshake {
    key: [u8; KEY_LEN],
    accept: [u8; ACCEPT_LEN],
}

impl Handshake {
    /// Starts a handshake with 16 random bytes (from a cryptographic RNG) as the key.
    #[must_use]
    pub fn new(random: [u8; 16]) -> Self {
        let mut key = [0_u8; KEY_LEN];
        base64_encode(&random, &mut key);
        let mut sha = sha1_smol::Sha1::new();
        sha.update(&key);
        sha.update(ACCEPT_GUID);
        let mut accept = [0_u8; ACCEPT_LEN];
        base64_encode(&sha.digest().bytes(), &mut accept);
        Self { key, accept }
    }

    /// The `Sec-WebSocket-Key` value.
    #[must_use]
    pub fn key(&self) -> &str {
        core::str::from_utf8(&self.key).unwrap_or_default()
    }

    /// The `Sec-WebSocket-Accept` value the server must return.
    #[must_use]
    pub fn expected_accept(&self) -> &str {
        core::str::from_utf8(&self.accept).unwrap_or_default()
    }

    /// Writes the upgrade request for `path` on `host` into `out` (cleared first). Connection
    /// setup is a cold path, so this may allocate.
    pub fn write_request(&self, host: &str, path: &str, out: &mut String) {
        out.clear();
        // Writing to a String cannot fail.
        let _ = write!(
            out,
            "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\n\r\n",
            self.key()
        );
    }

    /// Validates the server's response. Returns `Ok(None)` while the header block is
    /// incomplete, and `Ok(Some(n))` once valid, where `n` is the header length: any bytes
    /// after it are already WebSocket frames.
    ///
    /// # Errors
    /// [`WsError::Handshake`] for an invalid response, or [`WsError::HandshakeTooLarge`].
    pub fn parse_response(&self, buf: &[u8]) -> Result<Option<usize>, WsError> {
        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            if buf.len() > MAX_RESPONSE_HEADERS {
                return Err(WsError::HandshakeTooLarge);
            }
            return Ok(None);
        };
        let header_len = end.saturating_add(4);
        if header_len > MAX_RESPONSE_HEADERS {
            return Err(WsError::HandshakeTooLarge);
        }
        let head = buf
            .get(..end)
            .and_then(|b| core::str::from_utf8(b).ok())
            .ok_or(WsError::Handshake("headers are not UTF-8"))?;
        let mut lines = head.split("\r\n");
        let status = lines.next().unwrap_or_default();
        let mut parts = status.split(' ');
        if parts.next() != Some("HTTP/1.1") || parts.next() != Some("101") {
            return Err(WsError::Handshake("status is not 101"));
        }
        let (mut upgrade, mut connection, mut accept) = (false, false, false);
        for line in lines {
            let (name, value) = line
                .split_once(':')
                .ok_or(WsError::Handshake("malformed header line"))?;
            let value = value.trim();
            if name.eq_ignore_ascii_case("upgrade") {
                upgrade = value.eq_ignore_ascii_case("websocket");
            } else if name.eq_ignore_ascii_case("connection") {
                connection = value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));
            } else if name.eq_ignore_ascii_case("sec-websocket-accept") {
                accept = value == self.expected_accept();
                if !accept {
                    return Err(WsError::Handshake(
                        "Sec-WebSocket-Accept does not match key",
                    ));
                }
            } else if name.eq_ignore_ascii_case("sec-websocket-extensions")
                || name.eq_ignore_ascii_case("sec-websocket-protocol")
            {
                return Err(WsError::Handshake("unrequested extension or subprotocol"));
            }
        }
        match (upgrade, connection, accept) {
            (false, _, _) => Err(WsError::Handshake("missing Upgrade: websocket")),
            (_, false, _) => Err(WsError::Handshake("missing Connection: Upgrade")),
            (_, _, false) => Err(WsError::Handshake("missing Sec-WebSocket-Accept")),
            (true, true, true) => Ok(Some(header_len)),
        }
    }
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding. `out` must be exactly `4 * ceil(input.len() / 3)` bytes.
fn base64_encode(input: &[u8], out: &mut [u8]) {
    let symbol = |index: u32| {
        BASE64
            .get(usize::try_from(index & 0x3F).unwrap_or(0))
            .copied()
            .unwrap_or(b'=')
    };
    for (chunk, dst) in input.chunks(3).zip(out.chunks_mut(4)) {
        let bytes = [
            chunk.first().copied().unwrap_or(0),
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]);
        let encoded = [
            symbol(n >> 18),
            symbol(n >> 12),
            if chunk.len() > 1 {
                symbol(n >> 6)
            } else {
                b'='
            },
            if chunk.len() > 2 { symbol(n) } else { b'=' },
        ];
        for (d, e) in dst.iter_mut().zip(encoded) {
            *d = e;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 6455 §1.3: key "dGhlIHNhbXBsZSBub25jZQ==" (the bytes "the sample nonce").
    fn rfc_handshake() -> Handshake {
        Handshake::new(*b"the sample nonce")
    }

    fn response(accept: &str, extra: &str) -> String {
        format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\n{extra}\r\n"
        )
    }

    #[test]
    fn matches_rfc_example() {
        let hs = rfc_handshake();
        assert_eq!(hs.key(), "dGhlIHNhbXBsZSBub25jZQ==");
        assert_eq!(hs.expected_accept(), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn base64_matches_known_vectors() {
        for (input, expected) in [
            (&b"f"[..], "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
        ] {
            let mut out = vec![0_u8; expected.len()];
            base64_encode(input, &mut out);
            assert_eq!(std::str::from_utf8(&out).unwrap(), expected);
        }
    }

    #[test]
    fn writes_request() {
        let mut out = String::new();
        rfc_handshake().write_request("stream.example", "/stream?streams=a", &mut out);
        assert!(out.starts_with("GET /stream?streams=a HTTP/1.1\r\nHost: stream.example\r\n"));
        assert!(out.contains("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"));
        assert!(out.ends_with("\r\n\r\n"));
    }

    #[test]
    fn accepts_valid_response_and_reports_header_length() {
        let hs = rfc_handshake();
        let reply = response("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", "Date: x\r\n");
        let mut bytes = reply.clone().into_bytes();
        bytes.extend_from_slice(&[0x81, 0x00]); // A frame already follows the headers.
        assert_eq!(hs.parse_response(&bytes), Ok(Some(reply.len())));
        // Case-insensitive names and values, token list in Connection.
        let loose = "HTTP/1.1 101 OK\r\nupgrade: WebSocket\r\nconnection: keep-alive, Upgrade\r\n\
                     sec-websocket-accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
        assert!(hs.parse_response(loose.as_bytes()).unwrap().is_some());
    }

    #[test]
    fn waits_for_complete_headers() {
        let reply = response("s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", "");
        assert_eq!(
            rfc_handshake().parse_response(&reply.as_bytes()[..reply.len() - 1]),
            Ok(None)
        );
    }

    #[test]
    fn rejects_invalid_responses() {
        let hs = rfc_handshake();
        let ok = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";
        for bad in [
            response(ok, "").replace("101 Switching Protocols", "200 OK"),
            response("wrong", ""),
            response(ok, "").replace("Upgrade: websocket\r\n", ""),
            response(ok, "").replace("Connection: Upgrade", "Connection: close"),
            response(ok, "").replace(&format!("Sec-WebSocket-Accept: {ok}\r\n"), ""),
            response(ok, "Sec-WebSocket-Extensions: permessage-deflate\r\n"),
            response(ok, "garbage line\r\n"),
        ] {
            assert!(
                matches!(
                    hs.parse_response(bad.as_bytes()),
                    Err(WsError::Handshake(_))
                ),
                "accepted {bad:?}"
            );
        }
        let huge = vec![b'a'; MAX_RESPONSE_HEADERS + 1];
        assert_eq!(hs.parse_response(&huge), Err(WsError::HandshakeTooLarge));
    }
}
