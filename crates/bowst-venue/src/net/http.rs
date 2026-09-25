//! A minimal HTTP/1.1 client for venue REST calls (snapshots, exchange info, reconciliation).
//!
//! REST is never on the hot path, so this favors simplicity: one request per connection
//! (`Connection: close`), the whole response read into a bounded buffer, then parsed by the
//! pure [`parse_response`] (fuzzed). Supports `Content-Length`, chunked and read-to-close
//! bodies. Compressed responses are rejected because compression is never requested.
//!
//! Venue rate-limit headers are surfaced so callers can respect limits: Binance's
//! `X-MBX-USED-WEIGHT-1M` and the standard `Retry-After`.

use std::io::{Read, Write};
use std::time::Duration;

use super::{NetError, Stream, TlsConfig, Url};

/// Malformed or unacceptable HTTP response.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HttpError {
    /// The response ended before the headers were complete.
    #[error("incomplete response headers")]
    IncompleteHeaders,
    /// The status line or a header line is malformed.
    #[error("malformed {0}")]
    Malformed(&'static str),
    /// The response uses a feature this client does not support.
    #[error("unsupported: {0}")]
    Unsupported(&'static str),
    /// The body is shorter than `Content-Length` or a chunk declared.
    #[error("truncated body")]
    TruncatedBody,
    /// The response exceeds the size limit.
    #[error("response larger than {limit} bytes")]
    TooLarge {
        /// Configured limit.
        limit: usize,
    },
}

/// Status and venue rate-limit information from a response. The body is written to the
/// caller's buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Response {
    /// HTTP status code.
    pub status: u16,
    /// Binance `X-MBX-USED-WEIGHT-1M`: request weight used in the current minute.
    pub used_weight_1m: Option<u32>,
    /// `Retry-After` in seconds (sent with 418 and 429 responses).
    pub retry_after_secs: Option<u32>,
}

/// Largest header block accepted.
const MAX_HEADERS: usize = 16 * 1024;

/// Parses a complete HTTP/1.1 response (everything read until the server closed the
/// connection) and writes its decoded body to `body` (cleared first).
///
/// # Errors
/// [`HttpError`] for malformed, truncated, compressed or oversized responses.
pub fn parse_response(
    raw: &[u8],
    body: &mut Vec<u8>,
    max_body: usize,
) -> Result<Response, HttpError> {
    body.clear();
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or(HttpError::IncompleteHeaders)?;
    if head_end > MAX_HEADERS {
        return Err(HttpError::TooLarge { limit: MAX_HEADERS });
    }
    let head = raw
        .get(..head_end)
        .and_then(|h| core::str::from_utf8(h).ok())
        .ok_or(HttpError::Malformed("headers"))?;
    let rest = raw.get(head_end.saturating_add(4)..).unwrap_or_default();

    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let mut parts = status_line.splitn(3, ' ');
    if !matches!(parts.next(), Some("HTTP/1.1" | "HTTP/1.0")) {
        return Err(HttpError::Malformed("status line"));
    }
    let status = parts
        .next()
        .filter(|code| code.len() == 3)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or(HttpError::Malformed("status code"))?;

    let mut response = Response {
        status,
        used_weight_1m: None,
        retry_after_secs: None,
    };
    let (mut content_length, mut chunked) = (None, false);
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or(HttpError::Malformed("header line"))?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let length = value
                .parse::<usize>()
                .map_err(|_| HttpError::Malformed("Content-Length"))?;
            if content_length.is_some_and(|previous| previous != length) {
                return Err(HttpError::Malformed("conflicting Content-Length"));
            }
            content_length = Some(length);
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if !value.eq_ignore_ascii_case("chunked") {
                return Err(HttpError::Unsupported("transfer encoding"));
            }
            chunked = true;
        } else if name.eq_ignore_ascii_case("content-encoding") {
            if !value.eq_ignore_ascii_case("identity") {
                return Err(HttpError::Unsupported("content encoding"));
            }
        } else if name.eq_ignore_ascii_case("x-mbx-used-weight-1m") {
            response.used_weight_1m = value.parse().ok();
        } else if name.eq_ignore_ascii_case("retry-after") {
            response.retry_after_secs = value.parse().ok();
        }
    }

    if chunked {
        if content_length.is_some() {
            return Err(HttpError::Malformed("both chunked and Content-Length"));
        }
        decode_chunked(rest, body, max_body)?;
    } else if let Some(length) = content_length {
        if length > max_body {
            return Err(HttpError::TooLarge { limit: max_body });
        }
        body.extend_from_slice(rest.get(..length).ok_or(HttpError::TruncatedBody)?);
    } else {
        // No framing headers: the body runs to the end of the connection.
        if rest.len() > max_body {
            return Err(HttpError::TooLarge { limit: max_body });
        }
        body.extend_from_slice(rest);
    }
    Ok(response)
}

fn decode_chunked(mut rest: &[u8], body: &mut Vec<u8>, max_body: usize) -> Result<(), HttpError> {
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or(HttpError::TruncatedBody)?;
        let size_line = rest
            .get(..line_end)
            .and_then(|l| core::str::from_utf8(l).ok())
            .ok_or(HttpError::Malformed("chunk size"))?;
        let size_text = size_line.split(';').next().unwrap_or_default().trim();
        let size =
            usize::from_str_radix(size_text, 16).map_err(|_| HttpError::Malformed("chunk size"))?;
        rest = rest.get(line_end.saturating_add(2)..).unwrap_or_default();
        if size == 0 {
            // Trailers (if any) are ignored.
            return Ok(());
        }
        if body.len().saturating_add(size) > max_body {
            return Err(HttpError::TooLarge { limit: max_body });
        }
        let data = rest.get(..size).ok_or(HttpError::TruncatedBody)?;
        body.extend_from_slice(data);
        let after = rest.get(size..).ok_or(HttpError::TruncatedBody)?;
        rest = after
            .strip_prefix(b"\r\n")
            .ok_or(HttpError::Malformed("chunk terminator"))?;
    }
}

/// Performs `GET url` and writes the decoded body to `body`. Opens a fresh connection and
/// sends `Connection: close`. `raw` is scratch space for the response; both buffers are
/// reused across calls.
///
/// # Errors
/// Connection, TLS, I/O, timeout or [`HttpError`] failures.
pub fn get(
    url: &Url,
    tls: &TlsConfig,
    timeout: Duration,
    max_body: usize,
    raw: &mut Vec<u8>,
    body: &mut Vec<u8>,
) -> Result<Response, NetError> {
    let mut stream = Stream::connect(url, tls, timeout)?;
    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: bowst\r\nAccept: application/json\r\n\
         Connection: close\r\n\r\n",
        url.path,
        url.host_header()
    );
    stream
        .write_all(request.as_bytes())
        .map_err(NetError::io("send HTTP request"))?;
    stream.flush().map_err(NetError::io("send HTTP request"))?;

    raw.clear();
    let limit = max_body.saturating_add(MAX_HEADERS);
    let mut chunk = [0_u8; 16 * 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                if raw.len().saturating_add(n) > limit {
                    return Err(HttpError::TooLarge { limit: max_body }.into());
                }
                raw.extend_from_slice(chunk.get(..n).unwrap_or_default());
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Err(NetError::Timeout("reading HTTP response"));
            }
            // Some servers close TLS without a close_notify after `Connection: close`; the
            // framing checks in `parse_response` still catch any truncation.
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                return Err(NetError::Io {
                    op: "read HTTP response",
                    source: e,
                });
            }
        }
    }
    Ok(parse_response(raw, body, max_body)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> Result<(Response, String), HttpError> {
        let mut body = Vec::new();
        let response = parse_response(raw.as_bytes(), &mut body, 64)?;
        Ok((response, String::from_utf8(body).unwrap()))
    }

    #[test]
    fn parses_content_length_bodies_and_rate_limit_headers() {
        let (response, body) =
            parse("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nx-mbx-used-weight-1m: 57\r\n\r\nhello")
                .unwrap();
        assert_eq!(
            response,
            Response {
                status: 200,
                used_weight_1m: Some(57),
                retry_after_secs: None
            }
        );
        assert_eq!(body, "hello");
        let (response, _) =
            parse("HTTP/1.1 429 Too Many\r\nRetry-After: 30\r\nContent-Length: 0\r\n\r\n").unwrap();
        assert_eq!(
            (response.status, response.retry_after_secs),
            (429, Some(30))
        );
    }

    #[test]
    fn decodes_chunked_and_read_to_close_bodies() {
        let (_, body) = parse(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4;ext=1\r\nWiki\r\n5\r\npedia\r\n0\r\nX-Trailer: 1\r\n\r\n",
        )
        .unwrap();
        assert_eq!(body, "Wikipedia");
        let (_, body) = parse("HTTP/1.0 200 OK\r\n\r\nuntil close").unwrap();
        assert_eq!(body, "until close");
    }

    #[test]
    fn rejects_bad_responses() {
        for (raw, expected) in [
            (
                "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n",
                HttpError::IncompleteHeaders,
            ),
            ("HTTP/2 200 OK\r\n\r\n", HttpError::Malformed("status line")),
            (
                "HTTP/1.1 2000 OK\r\n\r\n",
                HttpError::Malformed("status code"),
            ),
            (
                "HTTP/1.1 200 OK\r\nbad header\r\n\r\n",
                HttpError::Malformed("header line"),
            ),
            (
                "HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort",
                HttpError::TruncatedBody,
            ),
            (
                "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab",
                HttpError::Malformed("conflicting Content-Length"),
            ),
            (
                "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n",
                HttpError::Unsupported("content encoding"),
            ),
            (
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n",
                HttpError::Unsupported("transfer encoding"),
            ),
            (
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\n",
                HttpError::Malformed("both chunked and Content-Length"),
            ),
            (
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
                HttpError::Malformed("chunk size"),
            ),
            (
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab",
                HttpError::TruncatedBody,
            ),
            (
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nabX\r\n0\r\n\r\n",
                HttpError::Malformed("chunk terminator"),
            ),
            (
                "HTTP/1.1 200 OK\r\nContent-Length: 65\r\n\r\n",
                HttpError::TooLarge { limit: 64 },
            ),
        ] {
            assert_eq!(parse(raw).map(|_| ()), Err(expected), "{raw:?}");
        }
    }
}
