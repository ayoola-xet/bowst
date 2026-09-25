//! Network transport: TCP and TLS connections, and a minimal HTTP/1.1 client.
//!
//! This layer only moves bytes. Protocol handling lives in [`crate::ws`] (pure) and the venue
//! modules. Connection setup (DNS, TCP connect, TLS handshake) is blocking with timeouts and
//! happens off the hot path; after setup, WebSocket streams are switched to non-blocking so
//! the market-data thread can busy-poll them.
//!
//! TLS certificates are verified against the operating system's trust store, which honors
//! the standard `SSL_CERT_FILE` / `SSL_CERT_DIR` overrides. Verification is never disabled.

pub mod http;

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

/// Network, TLS or HTTP failure. Always carries what was being attempted.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    /// A URL that is malformed or uses an unsupported feature.
    #[error("invalid URL: {0}")]
    InvalidUrl(&'static str),
    /// No usable root certificates were found.
    #[error("no trusted root certificates could be loaded")]
    NoRootCertificates,
    /// An I/O failure during `op`.
    #[error("{op}: {source}")]
    Io {
        /// What was being attempted.
        op: &'static str,
        /// Underlying error.
        #[source]
        source: io::Error,
    },
    /// The TLS layer rejected the connection (for example, certificate verification failed).
    #[error("TLS: {0}")]
    Tls(#[from] rustls::Error),
    /// The peer closed the connection.
    #[error("connection closed by peer")]
    Closed,
    /// An operation did not finish within its deadline.
    #[error("timed out: {0}")]
    Timeout(&'static str),
    /// A malformed or unacceptable HTTP response.
    #[error("HTTP: {0}")]
    Http(#[from] http::HttpError),
    /// A WebSocket protocol violation.
    #[error("WebSocket: {0}")]
    WebSocket(#[from] crate::ws::WsError),
}

impl NetError {
    pub(crate) fn io(op: &'static str) -> impl FnOnce(io::Error) -> Self {
        move |source| Self::Io { op, source }
    }
}

/// URL schemes this transport speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
    /// WebSocket over plain TCP (tests and local tools only).
    Ws,
    /// WebSocket over TLS.
    Wss,
    /// HTTP over plain TCP (tests and local tools only).
    Http,
    /// HTTP over TLS.
    Https,
}

impl Scheme {
    /// Whether the connection uses TLS.
    #[must_use]
    pub fn is_tls(self) -> bool {
        matches!(self, Self::Wss | Self::Https)
    }

    fn default_port(self) -> u16 {
        if self.is_tls() { 443 } else { 80 }
    }
}

/// A parsed `scheme://host[:port][/path][?query]` URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Url {
    /// Scheme.
    pub scheme: Scheme,
    /// Host name or IPv4 address.
    pub host: String,
    /// Port (the scheme's default if not given).
    pub port: u16,
    /// Path and query, starting with `/`.
    pub path: String,
}

impl Url {
    /// Parses a URL. User info, fragments and IPv6 literals are rejected.
    ///
    /// # Errors
    /// [`NetError::InvalidUrl`].
    pub fn parse(text: &str) -> Result<Self, NetError> {
        let (scheme, rest) = text
            .split_once("://")
            .ok_or(NetError::InvalidUrl("missing scheme"))?;
        let scheme = match scheme {
            "ws" => Scheme::Ws,
            "wss" => Scheme::Wss,
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            _ => return Err(NetError::InvalidUrl("unsupported scheme")),
        };
        if rest.contains(['#', ' ']) || !rest.is_ascii() {
            return Err(NetError::InvalidUrl("fragment, space or non-ASCII"));
        }
        let (authority, path) = match rest.find(['/', '?']) {
            Some(at) => rest.split_at(at),
            None => (rest, ""),
        };
        // '@' is fine in a path or query (Binance stream names use it), not in the authority.
        if authority.contains(['@', '[']) {
            return Err(NetError::InvalidUrl("user info or IPv6 literal"));
        }
        let (host, port) = match authority.split_once(':') {
            Some((host, port)) => (
                host,
                port.parse::<u16>()
                    .ok()
                    .filter(|&p| p != 0)
                    .ok_or(NetError::InvalidUrl("invalid port"))?,
            ),
            None => (authority, scheme.default_port()),
        };
        if host.is_empty() {
            return Err(NetError::InvalidUrl("missing host"));
        }
        let path = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
        Ok(Self {
            scheme,
            host: host.to_owned(),
            port,
            path,
        })
    }
}

impl Url {
    /// Value for the HTTP `Host` header: the port is included only if it is not the
    /// scheme's default.
    #[must_use]
    pub fn host_header(&self) -> String {
        if self.port == self.scheme.default_port() {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// Shared TLS client settings.
#[derive(Clone, Debug)]
pub struct TlsConfig(Arc<ClientConfig>);

impl TlsConfig {
    /// Trusts the operating system's root certificates (honoring `SSL_CERT_FILE` and
    /// `SSL_CERT_DIR`), with the `ring` crypto provider and safe default protocol versions.
    ///
    /// # Errors
    /// [`NetError::NoRootCertificates`] if none could be loaded, or [`NetError::Tls`].
    pub fn from_platform_roots() -> Result<Self, NetError> {
        let mut roots = RootCertStore::empty();
        let loaded = rustls_native_certs::load_native_certs();
        let (added, _ignored) = roots.add_parsable_certificates(loaded.certs);
        if added == 0 {
            return Err(NetError::NoRootCertificates);
        }
        let config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()?
                .with_root_certificates(roots)
                .with_no_client_auth();
        Ok(Self(Arc::new(config)))
    }

    /// Fills `out` from the TLS crypto provider's secure random source.
    ///
    /// # Errors
    /// [`NetError::Tls`] if the source fails.
    pub fn fill_random(&self, out: &mut [u8]) -> Result<(), NetError> {
        self.0
            .crypto_provider()
            .secure_random
            .fill(out)
            .map_err(|_| NetError::Tls(rustls::Error::FailedToGetRandomBytes))
    }
}

/// A connected byte stream, plain or TLS.
#[derive(Debug)]
pub enum Stream {
    /// Plain TCP.
    Plain(TcpStream),
    /// TLS over TCP.
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Stream {
    /// Connects to the URL's host and port, and completes the TLS handshake if the scheme
    /// uses TLS. Blocking, bounded by `timeout` per step. Nagle's algorithm is disabled.
    ///
    /// # Errors
    /// Resolution, connection or TLS failures.
    pub fn connect(url: &Url, tls: &TlsConfig, timeout: Duration) -> Result<Self, NetError> {
        let addrs = (url.host.as_str(), url.port)
            .to_socket_addrs()
            .map_err(NetError::io("resolve host"))?;
        let mut last_error = None;
        let mut socket = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, timeout) {
                Ok(s) => {
                    socket = Some(s);
                    break;
                }
                Err(e) => last_error = Some(e),
            }
        }
        let socket = socket.ok_or_else(|| NetError::Io {
            op: "connect",
            source: last_error
                .unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no addresses")),
        })?;
        socket
            .set_nodelay(true)
            .map_err(NetError::io("set TCP_NODELAY"))?;
        socket
            .set_read_timeout(Some(timeout))
            .map_err(NetError::io("set read timeout"))?;
        socket
            .set_write_timeout(Some(timeout))
            .map_err(NetError::io("set write timeout"))?;
        if !url.scheme.is_tls() {
            return Ok(Self::Plain(socket));
        }
        let name = ServerName::try_from(url.host.clone())
            .map_err(|_| NetError::InvalidUrl("host is not a valid TLS server name"))?;
        let mut conn = ClientConnection::new(Arc::clone(&tls.0), name)?;
        let mut socket = socket;
        while conn.is_handshaking() {
            conn.complete_io(&mut socket)
                .map_err(NetError::io("TLS handshake"))?;
        }
        Ok(Self::Tls(Box::new(StreamOwned::new(conn, socket))))
    }

    fn socket(&self) -> &TcpStream {
        match self {
            Self::Plain(s) => s,
            Self::Tls(s) => s.get_ref(),
        }
    }

    /// Switches the socket between blocking and non-blocking mode.
    ///
    /// # Errors
    /// [`NetError::Io`].
    pub fn set_nonblocking(&self, nonblocking: bool) -> Result<(), NetError> {
        self.socket()
            .set_nonblocking(nonblocking)
            .map_err(NetError::io("set non-blocking"))
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_urls() {
        let url = Url::parse("wss://data-stream.binance.vision/stream?streams=btcusdt@depth@100ms")
            .unwrap();
        assert_eq!(url.host, "data-stream.binance.vision");
        assert_eq!(url.path, "/stream?streams=btcusdt@depth@100ms");
        let url = Url::parse("wss://stream.example:9443/stream?streams=a/b").unwrap();
        assert_eq!(
            url,
            Url {
                scheme: Scheme::Wss,
                host: "stream.example".into(),
                port: 9443,
                path: "/stream?streams=a/b".into()
            }
        );
        let url = Url::parse("https://api.example").unwrap();
        assert_eq!((url.port, url.path.as_str()), (443, "/"));
        assert_eq!(url.host_header(), "api.example");
        assert_eq!(Url::parse("ws://h:8080/").unwrap().host_header(), "h:8080");
        let url = Url::parse("http://127.0.0.1:8080?x=1").unwrap();
        assert_eq!(
            (url.host.as_str(), url.port, url.path.as_str()),
            ("127.0.0.1", 8080, "/?x=1")
        );
    }

    #[test]
    fn rejects_bad_urls() {
        for bad in [
            "stream.example",
            "ftp://x",
            "wss://",
            "wss://:443/",
            "wss://host:0/",
            "wss://host:99999/",
            "wss://user@host/",
            "wss://host/#frag",
            "wss://[::1]/",
            "wss://h\u{e9}st/",
        ] {
            assert!(Url::parse(bad).is_err(), "accepted {bad:?}");
        }
    }
}
