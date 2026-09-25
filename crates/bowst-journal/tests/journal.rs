//! Journal write/read behavior, including crash and corruption cases.

// Test helpers fail fast on broken setup.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bowst_core::alloc_counter::{CountingAllocator, count_allocations};
use bowst_core::{MonoTime, WallTime};
use bowst_journal::format::{Kind, RecordHeader};
use bowst_journal::{JournalConfig, JournalHealth, JournalReader, ReadError, start};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

/// A fresh, unique directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let unique = format!(
            "bowst-journal-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let path = std::env::temp_dir().join(unique);
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn header(kind: Kind, i: u64) -> RecordHeader {
    RecordHeader {
        kind,
        source: 3,
        mono: MonoTime::from_nanos(1_000 + i),
        wall: WallTime::from_nanos(2_000 + i),
    }
}

fn payload(i: u64) -> Vec<u8> {
    let len = usize::try_from(i * 37 % 1_500).unwrap();
    let start = usize::try_from(i).unwrap();
    (0..len)
        .map(|j| u8::try_from((start + j) % 251).unwrap())
        .collect()
}

/// Reads every record as (kind, source, mono, wall, payload).
/// (kind, source, mono, wall, payload) of one record.
type Row = (u16, u16, u64, u64, Vec<u8>);

fn read_all(dir: &Path) -> Result<(Vec<Row>, bool), ReadError> {
    let mut reader = JournalReader::open(dir)?;
    let mut out = Vec::new();
    while let Some(record) = reader.next_record()? {
        let h = record.header;
        out.push((
            h.kind.0,
            h.source,
            h.mono.as_nanos(),
            h.wall.as_nanos(),
            record.payload.to_vec(),
        ));
    }
    Ok((out, reader.torn_tail()))
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    files
}

fn write_records(config: JournalConfig, count: u64) {
    let (mut producer, handle) = start(config).unwrap();
    for i in 0..count {
        let body = payload(i);
        // Two parts: exercises building one record from pieces.
        let (a, b) = body.split_at(body.len() / 2);
        assert!(producer.append(header(Kind::MD_MESSAGE, i), &[a, b]));
    }
    assert_eq!(handle.health(), JournalHealth::Ok);
    drop(producer);
    let stats = handle.finish().unwrap();
    assert_eq!(stats.records, count);
    assert_eq!(stats.dropped, 0);
}

#[test]
fn round_trips_every_record_in_order() {
    let dir = TempDir::new("roundtrip");
    write_records(JournalConfig::new(&dir.0), 2_000);
    let (records, torn) = read_all(&dir.0).unwrap();
    assert!(!torn);
    assert_eq!(records.len(), 2_000);
    for (i, (kind, source, mono, wall, body)) in records.into_iter().enumerate() {
        let i = u64::try_from(i).unwrap();
        assert_eq!(
            (kind, source, mono, wall),
            (Kind::MD_MESSAGE.0, 3, 1_000 + i, 2_000 + i)
        );
        assert_eq!(body, payload(i));
    }
}

#[test]
fn rotates_segments_and_reads_across_them() {
    let dir = TempDir::new("rotate");
    let mut config = JournalConfig::new(&dir.0);
    config.segment_bytes = 64 * 1024;
    write_records(config, 3_000);
    assert!(segments(&dir.0).len() > 5, "{:?}", segments(&dir.0));
    let (records, torn) = read_all(&dir.0).unwrap();
    assert!(!torn);
    assert_eq!(records.len(), 3_000);
    assert_eq!(records[2_999].4, payload(2_999));
}

#[test]
fn a_torn_final_record_is_tolerated_and_reported() {
    let dir = TempDir::new("torn");
    write_records(JournalConfig::new(&dir.0), 100);
    let last = segments(&dir.0).pop().unwrap();
    let len = fs::metadata(&last).unwrap().len();
    // Simulate a crash mid-write of the final record.
    OpenOptions::new()
        .write(true)
        .open(&last)
        .unwrap()
        .set_len(len - 10)
        .unwrap();
    let (records, torn) = read_all(&dir.0).unwrap();
    assert!(torn);
    assert_eq!(records.len(), 99);
}

#[test]
fn truncation_before_the_end_is_an_error() {
    let dir = TempDir::new("truncated-middle");
    let mut config = JournalConfig::new(&dir.0);
    config.segment_bytes = 32 * 1024;
    write_records(config, 1_000);
    let first = segments(&dir.0).remove(0);
    let len = fs::metadata(&first).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&first)
        .unwrap()
        .set_len(len - 10)
        .unwrap();
    assert!(matches!(read_all(&dir.0), Err(ReadError::Truncated { .. })));
}

#[test]
fn flipped_bits_are_reported_as_corruption() {
    let dir = TempDir::new("corrupt");
    write_records(JournalConfig::new(&dir.0), 100);
    let file = segments(&dir.0).remove(0);
    let mut bytes = fs::read(&file).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x40;
    fs::write(&file, &bytes).unwrap();
    assert!(matches!(read_all(&dir.0), Err(ReadError::Corrupt { .. })));
}

#[test]
fn a_missing_segment_is_reported() {
    let dir = TempDir::new("missing");
    let mut config = JournalConfig::new(&dir.0);
    config.segment_bytes = 32 * 1024;
    write_records(config, 1_000);
    let files = segments(&dir.0);
    assert!(files.len() >= 3);
    fs::remove_file(&files[1]).unwrap();
    assert!(matches!(
        read_all(&dir.0),
        Err(ReadError::MissingSegment {
            expected: 1,
            found: 2
        })
    ));
}

#[test]
fn dropped_records_are_counted_and_marked_with_a_gap() {
    let dir = TempDir::new("gap");
    let mut config = JournalConfig::new(&dir.0);
    config.ring_bytes = 4_096; // Largest record: 2,040 bytes including its header.
    let (mut producer, handle) = start(config).unwrap();
    assert!(producer.append(header(Kind::MD_MESSAGE, 0), &[b"before"]));
    assert!(
        !producer.append(header(Kind::MD_MESSAGE, 1), &[&[0; 3_000]]),
        "too large"
    );
    assert!(
        !producer.append(header(Kind::MD_MESSAGE, 2), &[&[0; 3_000]]),
        "too large"
    );
    assert_eq!(handle.health(), JournalHealth::Degraded { dropped: 2 });
    assert!(producer.append(header(Kind::MD_MESSAGE, 3), &[b"after"]));
    drop(producer);
    let stats = handle.finish().unwrap();
    assert_eq!((stats.records, stats.dropped), (3, 2));

    let (records, _) = read_all(&dir.0).unwrap();
    let kinds: Vec<_> = records.iter().map(|r| r.0).collect();
    assert_eq!(kinds, [Kind::MD_MESSAGE.0, Kind::GAP.0, Kind::MD_MESSAGE.0]);
    assert_eq!(
        records[1].4,
        2_u64.to_le_bytes(),
        "gap carries the dropped count"
    );
    assert_eq!(records[2].4, b"after");
}

#[test]
fn a_second_run_never_overwrites_the_first() {
    let dir = TempDir::new("append");
    write_records(JournalConfig::new(&dir.0), 10);
    write_records(JournalConfig::new(&dir.0), 10);
    assert_eq!(segments(&dir.0).len(), 2);
    let (records, _) = read_all(&dir.0).unwrap();
    assert_eq!(records.len(), 20);
}

#[test]
fn appending_does_not_allocate() {
    let dir = TempDir::new("no-alloc");
    let (mut producer, handle) = start(JournalConfig::new(&dir.0)).unwrap();
    let message = [9_u8; 459];
    let id = 7_u32.to_le_bytes();
    let ((), allocations) = count_allocations(|| {
        for i in 0..10_000 {
            assert!(producer.append(header(Kind::MD_SNAPSHOT, i), &[&id, &message]));
        }
    });
    assert_eq!(allocations, 0);
    drop(producer);
    assert_eq!(handle.finish().unwrap().records, 10_000);
}

/// A segment written by an independent implementation of the documented format (the Python
/// script that produced the fuzz seed) must read back correctly: the docs and code agree.
#[test]
fn documented_format_matches_the_implementation() {
    let dir = TempDir::new("spec");
    fs::create_dir_all(&dir.0).unwrap();
    let seed = include_bytes!("../../../fuzz/seeds/journal_records/two_records");
    fs::write(dir.0.join("journal-00000000.bjl"), seed).unwrap();
    let (records, torn) = read_all(&dir.0).unwrap();
    assert!(!torn);
    assert_eq!(records.len(), 2);
    assert_eq!(
        records[0],
        (0x0100, 3, 1_000, 2_000, br#"{"e":"depthUpdate"}"#.to_vec())
    );
    assert_eq!(
        records[1],
        (Kind::GAP.0, 3, 1_000, 2_000, 2_u64.to_le_bytes().to_vec())
    );
}
