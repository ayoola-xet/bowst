# 0007. TLS via rustls with platform trust roots; minimal HTTP client for REST

- Status: Accepted
- Date: 2026-09-25

## Context
Venue connections need TLS. Certificate trust must be verified, never disabled, and must work both on production hosts and in environments that re-terminate TLS through an egress gateway with its own CA (as the development environment does). REST calls (snapshots, exchange info, reconciliation) are rare and off the hot path, but still carry untrusted bytes, including venue rate-limit headers the engine must honor.

## Decision
- TLS uses `rustls` with the `ring` crypto provider (pure Rust plus assembly, no C build tooling) and safe default protocol versions.
- Trust roots come from the operating system store via `rustls-native-certs`, which honors `SSL_CERT_FILE` / `SSL_CERT_DIR`. Production hosts keep their normal OS trust store up to date; environments with a TLS-inspecting gateway point those variables at the gateway's bundle. There is no option to skip verification.
- `bowst_venue::net::Stream` is plain TCP or TLS over TCP with `TCP_NODELAY`. Setup is blocking with timeouts; WebSocket streams then switch to non-blocking for busy-polling.
- REST uses a minimal in-house HTTP/1.1 client (`net::http`): one request per connection with `Connection: close`, bounded response size, `Content-Length` / chunked / read-to-close bodies, compression rejected, and Binance's `X-MBX-USED-WEIGHT-1M` plus `Retry-After` surfaced. The response parser is pure and fuzzed.

## Consequences
- One TLS configuration is shared by WebSocket and REST, with no second HTTP stack and a small dependency tree (`rustls`, `ring`, `rustls-webpki`, `rustls-native-certs`; licenses Apache-2.0 / ISC / MIT, ISC added to the allowlist).
- Opening a new TLS connection per REST call adds a handshake (tens of milliseconds) to each snapshot. Snapshots are only fetched at startup and after a resync, so this is acceptable; connection reuse can be added if reconciliation frequency requires it.
- Verified live against Binance's public market-data mirror over TLS (REST and WebSocket) with an `#[ignore]`d test, run manually because CI tests must not depend on the network.
