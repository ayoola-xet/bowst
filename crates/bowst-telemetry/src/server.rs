//! A minimal HTTP endpoint that serves `GET /metrics` for Prometheus to scrape.
//!
//! It runs on its own thread, handles one request at a time, and closes every connection
//! after responding. It is read-only: the only thing it does is call the render function.
//!
//! Security: it has no authentication, so it binds to a loopback address unless the caller
//! explicitly allows otherwise (see [`MetricsConfig::allow_remote`]). Reach it from another
//! machine through an SSH tunnel or a VPN, never by exposing it to the internet (README
//! §11). Requests are bounded: headers over [`MAX_REQUEST`] bytes, or slower than
//! `io_timeout`, are refused.

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::exposition::CONTENT_TYPE;

/// Largest request head accepted, in bytes.
pub const MAX_REQUEST: usize = 8 * 1024;

/// Endpoint settings.
#[derive(Clone, Copy, Debug)]
pub struct MetricsConfig {
    /// Address to listen on.
    pub addr: SocketAddr,
    /// Permit a non-loopback address. Off by default: the endpoint has no authentication.
    pub allow_remote: bool,
    /// Read and write timeout for each connection.
    pub io_timeout: Duration,
}

impl MetricsConfig {
    /// Loopback-only settings for `addr`, with a 2-second I/O timeout.
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            allow_remote: false,
            io_timeout: Duration::from_secs(2),
        }
    }
}

/// Why the endpoint could not start.
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    /// A non-loopback address was requested without `allow_remote`.
    #[error("refusing to serve metrics on non-loopback address {0} without allow_remote")]
    NotLoopback(SocketAddr),
    /// The address could not be bound.
    #[error("bind {addr}: {source}")]
    Bind {
        /// Requested address.
        addr: SocketAddr,
        /// Underlying error.
        #[source]
        source: io::Error,
    },
    /// The server thread could not be started.
    #[error("could not start the metrics thread")]
    Thread,
}

/// A running endpoint. Stops when [`stop`](Self::stop) is called or it is dropped.
#[derive(Debug)]
pub struct MetricsServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl MetricsServer {
    /// Binds and starts serving. `render` is called on the server thread for every scrape.
    ///
    /// # Errors
    /// [`MetricsError`] if the address is not allowed, cannot be bound, or the thread cannot
    /// start.
    pub fn start(
        config: MetricsConfig,
        render: impl Fn() -> String + Send + 'static,
    ) -> Result<Self, MetricsError> {
        if !config.allow_remote && !config.addr.ip().is_loopback() {
            return Err(MetricsError::NotLoopback(config.addr));
        }
        let bind_error = |source| MetricsError::Bind {
            addr: config.addr,
            source,
        };
        let listener = TcpListener::bind(config.addr).map_err(bind_error)?;
        listener.set_nonblocking(true).map_err(bind_error)?;
        let addr = listener.local_addr().map_err(bind_error)?;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("metrics".into())
            .spawn(move || serve(&listener, &flag, config.io_timeout, &render))
            .map_err(|_| MetricsError::Thread)?;
        Ok(Self {
            addr,
            stop,
            thread: Some(thread),
        })
    }

    /// The address actually bound (useful when port 0 was requested).
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stops serving and waits for the thread to finish.
    pub fn stop(mut self) {
        self.shutdown();
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn serve(
    listener: &TcpListener,
    stop: &AtomicBool,
    io_timeout: Duration,
    render: &impl Fn() -> String,
) {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                // A failed exchange only affects that one client.
                let _ = handle(stream, io_timeout, render);
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => thread::sleep(Duration::from_millis(100)),
        }
    }
}

fn handle(
    mut stream: TcpStream,
    io_timeout: Duration,
    render: &impl Fn() -> String,
) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(io_timeout))?;
    stream.set_write_timeout(Some(io_timeout))?;
    let response = match read_head(&mut stream)? {
        Head::Complete(head) => respond(&head, render),
        Head::TooLarge => status(431, "Request Header Fields Too Large"),
    };
    stream.write_all(&response)?;
    stream.flush()?;
    // Closing with unread input makes the kernel send a reset, which can destroy the
    // response before the client reads it (for example after refusing an oversized
    // request). Signal the end of the response, then discard what the client still sends,
    // bounded by the read timeout and `DRAIN_LIMIT`.
    stream.shutdown(Shutdown::Write)?;
    let mut sink = [0_u8; 4096];
    let mut drained = 0_usize;
    while drained < DRAIN_LIMIT {
        match stream.read(&mut sink) {
            Ok(0) | Err(_) => break,
            Ok(n) => drained = drained.saturating_add(n),
        }
    }
    Ok(())
}

/// Most bytes discarded from a client after responding.
const DRAIN_LIMIT: usize = 64 * 1024;

enum Head {
    Complete(Vec<u8>),
    TooLarge,
}

/// Reads up to the blank line ending the request head.
fn read_head(stream: &mut TcpStream) -> io::Result<Head> {
    let mut head = Vec::with_capacity(512);
    let mut chunk = [0_u8; 512];
    loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(Head::Complete(head));
        }
        head.extend_from_slice(chunk.get(..n).unwrap_or_default());
        if head.len() > MAX_REQUEST {
            return Ok(Head::TooLarge);
        }
        if head.windows(4).any(|w| w == b"\r\n\r\n") {
            return Ok(Head::Complete(head));
        }
    }
}

fn respond(head: &[u8], render: &impl Fn() -> String) -> Vec<u8> {
    let line = head
        .split(|&b| b == b'\n')
        .next()
        .unwrap_or_default()
        .strip_suffix(b"\r")
        .unwrap_or_default();
    let mut parts = line.split(|&b| b == b' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return status(400, "Bad Request");
    };
    if !version.starts_with(b"HTTP/1.") {
        return status(400, "Bad Request");
    }
    let path = target.split(|&b| b == b'?').next().unwrap_or_default();
    match (method, path) {
        (b"GET", b"/metrics") => {
            let body = render();
            let mut out = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {CONTENT_TYPE}\r\nContent-Length: {}\r\n\
                 Cache-Control: no-store\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .into_bytes();
            out.extend_from_slice(body.as_bytes());
            out
        }
        (b"GET", _) => status(404, "Not Found"),
        _ => status(405, "Method Not Allowed"),
    }
}

fn status(code: u16, reason: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n{}\r\n",
        if code == 405 { "Allow: GET\r\n" } else { "" }
    )
    .into_bytes()
}
