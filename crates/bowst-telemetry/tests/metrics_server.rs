//! The metrics endpoint over real loopback sockets.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bowst_telemetry::exposition::CONTENT_TYPE;
use bowst_telemetry::server::{MAX_REQUEST, MetricsConfig, MetricsError, MetricsServer};

fn start() -> (MetricsServer, Arc<AtomicU64>) {
    let scrapes = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&scrapes);
    let mut config = MetricsConfig::new("127.0.0.1:0".parse().unwrap());
    config.io_timeout = Duration::from_millis(300);
    let server = MetricsServer::start(config, move || {
        let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
        format!("# TYPE scrapes counter\nscrapes {n}\n")
    })
    .unwrap();
    (server, scrapes)
}

/// Sends `request` and returns the whole response.
fn exchange(addr: SocketAddr, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(request).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn serves_metrics_to_a_scrape() {
    let (server, scrapes) = start();
    let response = exchange(
        server.addr(),
        b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nAccept: text/plain\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.contains(&format!("Content-Type: {CONTENT_TYPE}\r\n")));
    assert!(response.contains("Content-Length: 33\r\n"), "{response}");
    assert!(response.ends_with("\r\n\r\n# TYPE scrapes counter\nscrapes 1\n"));
    // Rendered fresh on every scrape, and query strings are ignored.
    let again = exchange(server.addr(), b"GET /metrics?x=1 HTTP/1.0\r\n\r\n");
    assert!(again.ends_with("scrapes 2\n"), "{again}");
    assert_eq!(scrapes.load(Ordering::SeqCst), 2);
    server.stop();
}

#[test]
fn only_get_metrics_is_served() {
    let (server, scrapes) = start();
    let addr = server.addr();
    let cases: [(&[u8], &str); 5] = [
        (b"GET / HTTP/1.1\r\n\r\n", "404"),
        (b"GET /metricsx HTTP/1.1\r\n\r\n", "404"),
        (b"POST /metrics HTTP/1.1\r\n\r\n", "405"),
        (b"GET /metrics\r\n\r\n", "400"),
        (b"GET /metrics SPDY/3\r\n\r\n", "400"),
    ];
    for (request, code) in cases {
        let response = exchange(addr, request);
        assert!(
            response.starts_with(&format!("HTTP/1.1 {code} ")),
            "{request:?} -> {response}"
        );
    }
    assert_eq!(scrapes.load(Ordering::SeqCst), 0, "nothing rendered");
    server.stop();
}

#[test]
fn oversized_and_stalled_requests_do_not_block_scrapes() {
    let (server, _) = start();
    let addr = server.addr();
    let huge = format!(
        "GET /metrics HTTP/1.1\r\nX: {}\r\n\r\n",
        "a".repeat(MAX_REQUEST)
    );
    assert!(exchange(addr, huge.as_bytes()).starts_with("HTTP/1.1 431 "));
    // A client that connects and sends nothing is dropped after the I/O timeout.
    let idle = TcpStream::connect(addr).unwrap();
    let response = exchange(addr, b"GET /metrics HTTP/1.1\r\n\r\n");
    assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
    drop(idle);
    server.stop();
}

#[test]
fn refuses_non_loopback_addresses_unless_allowed() {
    let config = MetricsConfig::new("0.0.0.0:0".parse().unwrap());
    assert!(matches!(
        MetricsServer::start(config, String::new),
        Err(MetricsError::NotLoopback(_))
    ));
    let mut allowed = config;
    allowed.allow_remote = true;
    MetricsServer::start(allowed, String::new).unwrap().stop();
}

#[test]
fn stopping_releases_the_port() {
    let (server, _) = start();
    let addr = server.addr();
    server.stop();
    assert!(TcpStream::connect(addr).is_err());
}
