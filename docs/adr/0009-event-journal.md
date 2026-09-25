# 0009. Event journal: hot-path byte ring, background writer, checksummed segments

- Status: Accepted
- Date: 2026-09-25

## Context
Every inbound and outbound event must be recorded so any session can be replayed exactly for post-mortems and regression tests (README §8), and so there is an audit trail for every order. Recording must not slow the hot path, must never silently lose data, and must survive crashes.

## Decision
- **Hot path:** `JournalProducer::append` builds the record header and copies the payload parts into a lock-free byte ring (`bowst_core::bytes_ring`, ADR 0003's single-producer design extended to variable-length records). No syscall, lock or allocation.
- **Writer thread:** drains the ring, computes checksums, and writes through a 1 MiB buffer to segment files. It flushes every 100 ms, `fsync`s every second and at segment ends, and rotates at 128 MiB. The README's original plan was an mmap'd log; plain buffered writes are simpler and equivalent here because the hot thread never touches the file.
- **Format:** a 32-byte file header, then records with a 32-byte header (kind, source, length, monotonic and wall-clock times, a CRC-32 of header and payload, and a separate CRC-32 of the header). The header checksum means a damaged length is detected before it is trusted, so corruption is never mistaken for a crash-truncated record. A single `Kind` registry prevents collisions between components.
- **Never silently incomplete:** if the ring is full or a record is too large, the record is dropped and counted, and the next record that gets through is preceded by a `GAP` record carrying the count. `JournalHealth` reports `Ok`, `Degraded { dropped }` or `Failed` (a disk error). The risk layer will refuse new orders unless the journal is `Ok`: no trading without an audit trail.
- **Reading:** `JournalReader` verifies every checksum. One incomplete record at the very end of the last segment is tolerated as a crash leftover and reported; any other truncation, a bad checksum or a missing segment is an error. Existing segments are never overwritten: a new run continues the numbering.

## Consequences
- Journaling a typical 459-byte message costs about 20 ns on the hot path (the byte-ring hand-off).
- Tested: a 2,000-record round trip, rotation across files, a torn tail, truncation in the middle, every single-bit flip in a record, a missing segment, drop accounting with the gap marker, no overwriting, zero allocations on append, and a segment written by an independent implementation of the documented format. The record decoder is fuzzed in CI.
- New dependency: `crc32fast` (MIT OR Apache-2.0).
- With `fsync` once a second, up to one second of records can be lost in a power failure (not in a process crash, which only loses what is still in the ring). Order events may need a stricter policy when order entry is built.
