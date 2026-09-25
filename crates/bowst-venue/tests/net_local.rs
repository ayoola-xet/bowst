//! Transport tests against local mock servers over plain TCP (no network access).
//!
//! Live tests against Binance's public market-data mirror are `#[ignore]`d so CI stays
//! deterministic; run them with `cargo test -p bowst-venue --test net_local -- --ignored`.

// Test helpers fail fast on broken setup.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use bowst_venue::net::http::{self, HttpError};
use bowst_venue::net::{NetError, TlsConfig, Url};
use bowst_venue::ws::WsError;
use bowst_venue::ws::client::WsClient;
use bowst_venue::ws::frame::Opcode;
use bowst_venue::ws::handshake::accept_for_key;
use bowst_venue::ws::reader::{ReaderConfig, WsEvent};

const TIMEOUT: Duration = Duration::from_secs(5);
const READER: ReaderConfig = ReaderConfig {
    buffer: 64 * 1024,
    max_frame: 32 * 1024,
    max_message: 64 * 1024,
};

fn tls() -> TlsConfig {
    TlsConfig::from_platform_roots().expect("platform root certificates")
}

/// Starts a one-connection server on a free local port running `serve`.
fn serve(serve: impl FnOnce(TcpStream) + Send + 'static) -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(TIMEOUT)).unwrap();
        serve(stream);
    });
    (port, handle)
}

fn read_headers(stream: &mut TcpStream) -> String {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut byte).unwrap();
        head.push(byte[0]);
    }
    String::from_utf8(head).unwrap()
}

fn server_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    assert!(payload.len() < 126);
    let mut frame = vec![0x80 | opcode, u8::try_from(payload.len()).unwrap()];
    frame.extend_from_slice(payload);
    frame
}

/// Reads one small masked client frame and returns (opcode, unmasked payload).
fn read_client_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut head = [0_u8; 2];
    stream.read_exact(&mut head).unwrap();
    assert_eq!(head[1] & 0x80, 0x80, "client frames must be masked");
    let len = usize::from(head[1] & 0x7F);
    assert!(len < 126);
    let mut mask = [0_u8; 4];
    stream.read_exact(&mut mask).unwrap();
    let mut payload = vec![0_u8; len];
    stream.read_exact(&mut payload).unwrap();
    for (byte, key) in payload.iter_mut().zip(mask.iter().cycle()) {
        *byte ^= key;
    }
    (head[0] & 0x0F, payload)
}

fn upgrade_response(request: &str, accept_override: Option<&str>) -> String {
    let key = request
        .lines()
        .find_map(|l| l.strip_prefix("Sec-WebSocket-Key: "))
        .unwrap()
        .trim();
    let accept = accept_for_key(key.as_bytes());
    let accept = accept_override.unwrap_or_else(|| std::str::from_utf8(&accept).unwrap());
    format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    )
}

/// Polls until an event arrives, converting it to an owned form. Buffered events are
/// drained before reading again, as the engine loop must.
fn next_owned(client: &mut WsClient) -> Result<(String, Vec<u8>), NetError> {
    let started = Instant::now();
    loop {
        if let Some(event) = client.next_event()? {
            return Ok(match event {
                WsEvent::Text(p) => ("text".into(), p.to_vec()),
                WsEvent::Binary(p) => ("binary".into(), p.to_vec()),
                WsEvent::Ping(p) => ("ping".into(), p.to_vec()),
                WsEvent::Pong(p) => ("pong".into(), p.to_vec()),
                WsEvent::Close { code, .. } => {
                    ("close".into(), code.unwrap_or(0).to_be_bytes().to_vec())
                }
            });
        }
        client.fill()?;
        assert!(started.elapsed() < TIMEOUT, "no event within timeout");
        std::hint::spin_loop();
    }
}

#[test]
fn websocket_session_against_local_server() {
    let (port, server) = serve(|mut stream| {
        let request = read_headers(&mut stream);
        assert!(request.starts_with("GET /stream?streams=btcusdt@depth@100ms HTTP/1.1\r\n"));
        assert!(request.contains(&format!(
            "Host: 127.0.0.1:{}",
            stream.local_addr().unwrap().port()
        )));
        // Headers and the first frame in one write: the client must keep the leftover bytes.
        let mut reply = upgrade_response(&request, None).into_bytes();
        reply.extend(server_frame(0x1, br#"{"hello":1}"#));
        stream.write_all(&reply).unwrap();
        stream.write_all(&server_frame(0x9, b"hb-1")).unwrap();
        assert_eq!(
            read_client_frame(&mut stream),
            (0xA, b"hb-1".to_vec()),
            "pong echoes ping"
        );
        assert_eq!(
            read_client_frame(&mut stream),
            (0x1, br#"{"method":"SUBSCRIBE"}"#.to_vec())
        );
        stream
            .write_all(&server_frame(0x8, &1000_u16.to_be_bytes()))
            .unwrap();
        assert_eq!(
            read_client_frame(&mut stream).0,
            0x8,
            "client answers close"
        );
    });

    let url = Url::parse(&format!(
        "ws://127.0.0.1:{port}/stream?streams=btcusdt@depth@100ms"
    ))
    .unwrap();
    let mut client = WsClient::connect(&url, &tls(), READER, TIMEOUT).unwrap();
    assert_eq!(
        next_owned(&mut client).unwrap(),
        ("text".into(), br#"{"hello":1}"#.to_vec())
    );
    let (kind, payload) = next_owned(&mut client).unwrap();
    assert_eq!(kind, "ping");
    client.send(Opcode::Pong, &payload).unwrap();
    client
        .send(Opcode::Text, br#"{"method":"SUBSCRIBE"}"#)
        .unwrap();
    assert_eq!(
        next_owned(&mut client).unwrap(),
        ("close".into(), 1000_u16.to_be_bytes().to_vec())
    );
    client.close();
    server.join().unwrap();
    assert!(matches!(next_owned(&mut client), Err(NetError::Closed)));
}

#[test]
fn websocket_rejects_wrong_accept() {
    let (port, server) = serve(|mut stream| {
        let request = read_headers(&mut stream);
        let reply = upgrade_response(&request, Some("AAAAAAAAAAAAAAAAAAAAAAAAAAA="));
        stream.write_all(reply.as_bytes()).unwrap();
    });
    let url = Url::parse(&format!("ws://127.0.0.1:{port}/")).unwrap();
    let err = WsClient::connect(&url, &tls(), READER, TIMEOUT).unwrap_err();
    assert!(
        matches!(err, NetError::WebSocket(WsError::Handshake(_))),
        "{err}"
    );
    server.join().unwrap();
}

#[test]
fn websocket_protocol_violation_is_reported() {
    let (port, server) = serve(|mut stream| {
        let request = read_headers(&mut stream);
        let mut reply = upgrade_response(&request, None).into_bytes();
        reply.extend_from_slice(&[0x81, 0x80, 0, 0, 0, 0]); // Masked server frame.
        stream.write_all(&reply).unwrap();
    });
    let url = Url::parse(&format!("ws://127.0.0.1:{port}/")).unwrap();
    let mut client = WsClient::connect(&url, &tls(), READER, TIMEOUT).unwrap();
    assert!(matches!(
        next_owned(&mut client),
        Err(NetError::WebSocket(WsError::MaskedServerFrame))
    ));
    server.join().unwrap();
}

fn http_server(response: &'static [u8]) -> (Url, thread::JoinHandle<()>) {
    let (port, handle) = serve(move |mut stream| {
        let request = read_headers(&mut stream);
        assert!(request.starts_with("GET /api/v3/depth?symbol=BTCUSDT&limit=5 HTTP/1.1\r\n"));
        assert!(request.contains("Connection: close\r\n"));
        stream.write_all(response).unwrap();
    });
    let url = Url::parse(&format!(
        "http://127.0.0.1:{port}/api/v3/depth?symbol=BTCUSDT&limit=5"
    ))
    .unwrap();
    (url, handle)
}

#[test]
fn http_get_reads_length_and_chunked_bodies() {
    let (mut raw, mut body) = (Vec::new(), Vec::new());
    for response in [
        &b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nX-MBX-USED-WEIGHT-1M: 7\r\n\r\n{}"[..],
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nX-MBX-USED-WEIGHT-1M: 7\r\n\r\n1\r\n{\r\n1\r\n}\r\n0\r\n\r\n",
    ] {
        let (url, server) = http_server(response);
        let reply = http::get(&url, &tls(), TIMEOUT, 1024, &mut raw, &mut body).unwrap();
        assert_eq!((reply.status, reply.used_weight_1m), (200, Some(7)));
        assert_eq!(body, b"{}");
        server.join().unwrap();
    }
}

#[test]
fn http_get_reports_truncation_and_timeouts() {
    let (mut raw, mut body) = (Vec::new(), Vec::new());
    let (url, server) = http_server(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort");
    let err = http::get(&url, &tls(), TIMEOUT, 1024, &mut raw, &mut body).unwrap_err();
    assert!(
        matches!(err, NetError::Http(HttpError::TruncatedBody)),
        "{err}"
    );
    server.join().unwrap();

    // A server that accepts but never answers.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = Url::parse(&format!(
        "http://127.0.0.1:{}/",
        listener.local_addr().unwrap().port()
    ))
    .unwrap();
    let (release, released) = std::sync::mpsc::channel::<()>();
    let hold = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        // Hold the connection open, silent, until the client has given up.
        let _ = released.recv_timeout(TIMEOUT);
        drop(stream);
    });
    let started = Instant::now();
    let err = http::get(
        &url,
        &tls(),
        Duration::from_millis(200),
        1024,
        &mut raw,
        &mut body,
    )
    .unwrap_err();
    assert!(matches!(err, NetError::Timeout(_)), "{err}");
    assert!(started.elapsed() < Duration::from_secs(2));
    release.send(()).unwrap();
    hold.join().unwrap();
}

#[test]
#[ignore = "network: Binance public market-data mirror"]
fn live_rest_and_stream_from_binance_mirror() {
    let tls = tls();
    let (mut raw, mut body) = (Vec::new(), Vec::new());
    let url =
        Url::parse("https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=5").unwrap();
    let reply = http::get(&url, &tls, TIMEOUT, 1 << 20, &mut raw, &mut body).unwrap();
    assert_eq!(reply.status, 200);
    assert!(body.starts_with(br#"{"lastUpdateId":"#));

    let url =
        Url::parse("wss://data-stream.binance.vision/stream?streams=btcusdt@depth@100ms").unwrap();
    let mut client = WsClient::connect(&url, &tls, READER, TIMEOUT).unwrap();
    let (kind, payload) = next_owned(&mut client).unwrap();
    assert_eq!(kind, "text");
    assert!(payload.starts_with(br#"{"stream":"btcusdt@depth@100ms""#));
    client.close();
}
