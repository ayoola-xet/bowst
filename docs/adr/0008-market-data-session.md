# 0008. Market-data session: one stream, a separate snapshot thread, fail closed per instrument

- Status: Accepted
- Date: 2026-09-25

## Context
Binance publishes order-book changes as a diff-depth stream and serves full snapshots over REST, which is rate-limited by request weight per IP. A local book is only correct once a snapshot has been bridged to the stream, and it stops being correct the moment a message is lost. Connections fail, go silent, or are closed by the venue after 24 hours.

## Decision
- One WebSocket per venue connection carries every instrument's stream (combined-stream URL); no subscription messages are needed.
- The market-data thread owns the socket, the decoder and all books. It busy-polls in production (`idle_sleep: None`).
- REST snapshots are fetched on a separate thread and handed back over a channel, so a slow or rate-limited snapshot never stalls updates for other instruments. The snapshot thread spends from a token bucket (default 1,200 weight per minute, well under Binance's 6,000 per IP), corrects the bucket from `X-MBX-USED-WEIGHT-1M`, and pauses all requests on 429 or 418 for `Retry-After`.
- Failure scope is as narrow as it can safely be:
  - A gap, invalid data or a crossed book takes down **one instrument**, which resyncs on its own.
  - Undecodable data, protocol errors, silence, or reaching the maximum connection age drop **the connection**. Every book goes down, and the session reconnects with jittered exponential backoff.
  - Snapshot results carry a connection generation, and results from an earlier connection are discarded.
- Books are exposed only through `MdHandler::on_book` while live. Status changes go to `MdHandler::on_status` for logging, metrics and, later, the risk layer.

## Consequences
- Verified end to end against a mock venue that replays recorded Binance traffic and serves snapshots of its true book at request time. With a lost message, a mid-stream disconnect or a silent connection, the session's books converge to exactly the venue's true books.
- Verified live against Binance's public market-data endpoints with the `bowst-md` tool.
- The planned reconnect at 23 hours causes a brief resync once a day. A seamless handover (open the replacement connection and sync it before closing the old one) is a follow-up before capital is at risk.
- Snapshot handoff allocates (owned level vectors). This happens only on the rare resync path, never per message.
