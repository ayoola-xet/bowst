# 0006. In-house client WebSocket protocol over a pre-allocated buffer

- Status: Accepted
- Date: 2026-09-25

## Context
Binance and Bybit stream market data and order updates over WebSocket. The market-data thread must not allocate after warm-up (CLAUDE.md §1.6), and every inbound venue byte must be handled by code that is fuzzed and cannot panic.

The common Rust WebSocket libraries (`tungstenite` and the async stacks built on it) return each message as an owned, heap-allocated buffer. That is one allocation per message on the hottest path, with unpredictable latency under allocator contention. They also bundle features we do not want on this path: compression extensions, async runtimes and automatic protocol replies that hide behavior we need to control, such as how pings are answered.

## Decision
- `bowst_venue::ws` implements the client side of RFC 6455 directly, with no I/O:
  - `handshake`: builds the upgrade request and strictly validates the reply (status 101, `Upgrade`, `Connection`, and `Sec-WebSocket-Accept` computed with SHA-1 from the `sha1_smol` crate). Any unrequested extension or subprotocol is rejected.
  - `frame`: parses server frame headers with every RFC check (reserved bits, unknown opcodes, masked server frames, non-minimal lengths, invalid control frames, size limits) and encodes masked client frames into a caller-supplied buffer.
  - `reader`: the transport reads socket bytes straight into a fixed receive buffer; events borrow their payloads from it. Unfragmented messages, the normal case, are never copied. Fragmented messages are reassembled into a second pre-allocated buffer.
- The transport (TCP, TLS, socket polling) is a separate layer that only moves bytes. Replies to pings are the caller's explicit decision, not hidden behavior.

## Consequences
- Zero allocations per message, verified by test.
- Framing is covered by unit tests of every RFC rule, a property test that arbitrary chunking never changes the decoded events, and two fuzz targets (reader and handshake) in CI.
- New dependency: `sha1_smol` (no dependencies of its own, BSD-3-Clause, added to the license allowlist). SHA-1 is used only to check the handshake reply as RFC 6455 requires; connection security comes from TLS.
- We own a small protocol implementation that must be kept correct. It is limited to the client subset we use, and the tests and fuzzers above guard it.
