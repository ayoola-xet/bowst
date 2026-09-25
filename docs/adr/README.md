# Architecture Decision Records

Each significant architecture decision gets one short record here, numbered in order. Records are never deleted. A reversed decision gets a new record that supersedes the old one, and the old one's status is updated.

| # | Title | Status |
|---|---|---|
| [0001](0001-rust-for-the-engine.md) | Rust for the trading engine | Accepted |
| [0002](0002-fixed-point-money.md) | Fixed-point integers for prices and quantities | Accepted |
| [0003](0003-spsc-rings-between-threads.md) | Lock-free SPSC rings as the only hot-path channel | Accepted |
| [0004](0004-order-book-representation.md) | Order book as a sorted array with the best level last | Accepted |
| [0005](0005-json-decoding.md) | In-house allocation-free JSON reader for venue messages | Accepted |
| [0006](0006-websocket-protocol.md) | In-house client WebSocket protocol over a pre-allocated buffer | Accepted |
| [0007](0007-tls-and-transport.md) | TLS via rustls with platform trust roots; minimal HTTP client for REST | Accepted |

## Template

```markdown
# NNNN. Title

- Status: Proposed | Accepted | Superseded by NNNN
- Date: YYYY-MM-DD

## Context
What forces are at play and what problem needs a decision.

## Decision
What we will do.

## Consequences
What becomes easier, what becomes harder, and what we must watch.
```
