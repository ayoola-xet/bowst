//! Writing the journal: a hot-path producer and a background writer thread.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bowst_core::bytes_ring::{self, ByteConsumer, ByteProducer};
use bowst_core::{Clock, SystemClock};

use crate::format::{
    CRC_AT, FILE_HEADER_LEN, Kind, MAX_PAYLOAD, RECORD_HEADER_LEN, RecordHeader,
    encode_file_header, encode_record_header, header_checksum, record_checksum,
};

/// Journal settings.
#[derive(Clone, Debug)]
pub struct JournalConfig {
    /// Directory holding the segment files. Created if missing. Existing segments are never
    /// overwritten: numbering continues after the highest one present.
    pub dir: PathBuf,
    /// Size of the in-memory ring between producer and writer. Bounds how much a slow disk can
    /// fall behind before records are dropped, and (at half) the largest record.
    pub ring_bytes: usize,
    /// Start a new segment once the current one reaches this size.
    pub segment_bytes: u64,
    /// Flush buffered writes to the OS at least this often.
    pub flush_every: Duration,
    /// Ask the OS to persist to disk (`fsync`) at least this often, and at segment ends.
    pub sync_every: Duration,
}

impl JournalConfig {
    /// Defaults: 64 MiB ring, 128 MiB segments, flush every 100 ms, fsync every second.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ring_bytes: 64 << 20,
            segment_bytes: 128 << 20,
            flush_every: Duration::from_millis(100),
            sync_every: Duration::from_secs(1),
        }
    }
}

/// Journal setup or write failure.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// A file-system operation failed.
    #[error("{op} {path}: {source}")]
    Io {
        /// What was being attempted.
        op: &'static str,
        /// File or directory involved.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: io::Error,
    },
    /// The ring size is invalid.
    #[error("invalid journal ring size")]
    InvalidRing,
    /// The writer thread could not be started or panicked.
    #[error("journal writer thread failed")]
    Thread,
}

/// Whether the journal is complete. The risk layer refuses new orders unless it is `Ok`, so
/// trading never continues without an audit trail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalHealth {
    /// Every record has been accepted.
    Ok,
    /// Records were dropped because the writer fell behind. Gaps are marked in the journal.
    Degraded {
        /// Records dropped so far.
        dropped: u64,
    },
    /// Writing to disk failed. Records are no longer persisted.
    Failed,
}

/// Counters from a finished journal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JournalStats {
    /// Records written to disk.
    pub records: u64,
    /// Bytes written to disk, including headers.
    pub bytes: u64,
    /// Segment files written.
    pub segments: u32,
    /// Records dropped by producers (ring full or record too large).
    pub dropped: u64,
    /// Records lost after a write error.
    pub lost_after_failure: u64,
}

#[derive(Debug, Default)]
struct Shared {
    dropped: AtomicU64,
    failed: AtomicBool,
    stop: AtomicBool,
}

/// Starts the writer thread and returns the producer for the hot path.
///
/// # Errors
/// [`JournalError`] if the directory or first segment cannot be created.
pub fn start(config: JournalConfig) -> Result<(JournalProducer, JournalHandle), JournalError> {
    let (producer, consumer) =
        bytes_ring::channel(config.ring_bytes).map_err(|_| JournalError::InvalidRing)?;
    fs::create_dir_all(&config.dir).map_err(|source| JournalError::Io {
        op: "create directory",
        path: config.dir.clone(),
        source,
    })?;
    let first = next_segment_index(&config.dir)?;
    let shared = Arc::new(Shared::default());
    let segment = Segment::create(&config.dir, first)?;
    let writer_shared = Arc::clone(&shared);
    let thread = thread::Builder::new()
        .name("journal".into())
        .spawn(move || Writer::new(config, consumer, writer_shared, segment).run())
        .map_err(|_| JournalError::Thread)?;
    Ok((
        JournalProducer {
            ring: producer,
            pending_gap: 0,
            shared: Arc::clone(&shared),
        },
        JournalHandle { shared, thread },
    ))
}

/// Hot-path side of the journal: copies records into the ring and never blocks.
#[derive(Debug)]
pub struct JournalProducer {
    ring: ByteProducer,
    /// Records dropped since the last one that got through; reported as a gap record next.
    pending_gap: u64,
    shared: Arc<Shared>,
}

impl JournalProducer {
    /// Appends a record whose payload is the concatenation of `parts`. Never blocks or
    /// allocates. Returns `false` if the record was dropped (ring full or record too large);
    /// the drop is counted, reflected in [`JournalHealth`], and marked in the journal by a
    /// [`Kind::GAP`] record as soon as there is room.
    #[inline]
    pub fn append(&mut self, header: RecordHeader, parts: &[&[u8]]) -> bool {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        let total = RECORD_HEADER_LEN.saturating_add(len);
        // Records that can never fit are dropped before anything else is written, so a run of
        // them produces one gap marker with the full count.
        let Some(payload_len) = u32::try_from(len)
            .ok()
            .filter(|_| len <= MAX_PAYLOAD && total <= self.ring.max_record())
        else {
            return self.drop_record();
        };
        if self.pending_gap > 0 && !self.write_gap(header) {
            return self.drop_record();
        }
        let written = self
            .ring
            .write_with(total, |dst| {
                let (head, mut body) = dst.split_at_mut(RECORD_HEADER_LEN);
                encode_record_header(header, payload_len, head);
                for part in parts {
                    let (now, rest) = body.split_at_mut(part.len());
                    now.copy_from_slice(part);
                    body = rest;
                }
            })
            .is_ok();
        if !written {
            return self.drop_record();
        }
        true
    }

    fn write_gap(&mut self, at: RecordHeader) -> bool {
        let header = RecordHeader {
            kind: Kind::GAP,
            ..at
        };
        let count = self.pending_gap.to_le_bytes();
        let written = self
            .ring
            .write_with(RECORD_HEADER_LEN.saturating_add(count.len()), |dst| {
                let (head, body) = dst.split_at_mut(RECORD_HEADER_LEN);
                encode_record_header(header, 8, head);
                body.copy_from_slice(&count);
            })
            .is_ok();
        if written {
            self.pending_gap = 0;
        }
        written
    }

    fn drop_record(&mut self) -> bool {
        self.pending_gap = self.pending_gap.saturating_add(1);
        self.shared.dropped.fetch_add(1, Ordering::Relaxed);
        false
    }
}

/// Control side of the journal.
#[derive(Debug)]
pub struct JournalHandle {
    shared: Arc<Shared>,
    thread: JoinHandle<Result<JournalStats, JournalError>>,
}

impl JournalHandle {
    /// Current health. Cheap: reads two atomics.
    #[must_use]
    pub fn health(&self) -> JournalHealth {
        if self.shared.failed.load(Ordering::Relaxed) {
            return JournalHealth::Failed;
        }
        match self.shared.dropped.load(Ordering::Relaxed) {
            0 => JournalHealth::Ok,
            dropped => JournalHealth::Degraded { dropped },
        }
    }

    /// Stops the writer after it has written everything already in the ring, then flushes and
    /// `fsync`s. Stop the producers first: records appended after this call may be lost.
    ///
    /// # Errors
    /// The first write error, or [`JournalError::Thread`] if the writer panicked.
    pub fn finish(self) -> Result<JournalStats, JournalError> {
        self.shared.stop.store(true, Ordering::Release);
        self.thread.join().map_err(|_| JournalError::Thread)?
    }
}

/// Name of segment `index`.
fn segment_name(index: u32) -> String {
    format!("journal-{index:08}.bjl")
}

/// Parses a segment file name back into its index.
pub(crate) fn segment_index(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_str()?;
    name.strip_prefix("journal-")?
        .strip_suffix(".bjl")?
        .parse()
        .ok()
}

fn next_segment_index(dir: &Path) -> Result<u32, JournalError> {
    let entries = fs::read_dir(dir).map_err(|source| JournalError::Io {
        op: "list",
        path: dir.to_path_buf(),
        source,
    })?;
    let highest = entries
        .filter_map(Result::ok)
        .filter_map(|entry| segment_index(&entry.path()))
        .max();
    Ok(highest.map_or(0, |h| h.saturating_add(1)))
}

struct Segment {
    path: PathBuf,
    out: BufWriter<File>,
    bytes: u64,
}

impl Segment {
    fn create(dir: &Path, index: u32) -> Result<Self, JournalError> {
        let path = dir.join(segment_name(index));
        let io_error = |op| {
            let path = path.clone();
            move |source| JournalError::Io { op, path, source }
        };
        // `create_new`: never overwrite an existing journal.
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(io_error("create"))?;
        let mut out = BufWriter::with_capacity(1 << 20, file);
        let created = SystemClock::new().wall();
        out.write_all(&encode_file_header(index, created))
            .map_err(io_error("write header"))?;
        Ok(Self {
            path,
            out,
            bytes: u64::try_from(FILE_HEADER_LEN).unwrap_or(0),
        })
    }

    fn sync(&mut self) -> io::Result<()> {
        self.out.flush()?;
        self.out.get_ref().sync_data()
    }
}

/// Everything the writer thread owns except the ring, so records can be borrowed from the
/// ring while being written here.
struct Sink {
    config: JournalConfig,
    shared: Arc<Shared>,
    segment: Segment,
    index: u32,
    stats: JournalStats,
    error: Option<JournalError>,
}

struct Writer {
    ring: ByteConsumer,
    sink: Sink,
}

impl Writer {
    fn new(
        config: JournalConfig,
        ring: ByteConsumer,
        shared: Arc<Shared>,
        segment: Segment,
    ) -> Self {
        let index = segment_index(&segment.path).unwrap_or(0);
        Self {
            ring,
            sink: Sink {
                config,
                shared,
                segment,
                index,
                stats: JournalStats {
                    segments: 1,
                    ..JournalStats::default()
                },
                error: None,
            },
        }
    }

    fn run(self) -> Result<JournalStats, JournalError> {
        let Self { mut ring, mut sink } = self;
        let mut last_flush = Instant::now();
        let mut last_sync = Instant::now();
        loop {
            let stopping = sink.shared.stop.load(Ordering::Acquire);
            let mut wrote_any = false;
            while ring.read_with(|record| sink.write(record)).is_some() {
                wrote_any = true;
            }
            if sink.error.is_none() && sink.segment.bytes >= sink.config.segment_bytes {
                sink.rotate();
            }
            if last_flush.elapsed() >= sink.config.flush_every {
                sink.guard("flush", |s| s.segment.out.flush());
                last_flush = Instant::now();
            }
            if last_sync.elapsed() >= sink.config.sync_every {
                sink.guard("sync", |s| s.segment.sync());
                last_sync = Instant::now();
            }
            if stopping && !wrote_any {
                break;
            }
            if !wrote_any {
                // Off the hot path: an idle writer sleeps instead of spinning a core.
                thread::sleep(Duration::from_millis(1));
            }
        }
        sink.guard("sync", |s| s.segment.sync());
        sink.stats.dropped = sink.shared.dropped.load(Ordering::Relaxed);
        match sink.error.take() {
            Some(error) => Err(error),
            None => Ok(sink.stats),
        }
    }
}

impl Sink {
    /// Writes one record from the ring, computing its checksum on the way.
    fn write(&mut self, record: &[u8]) {
        if self.error.is_some() {
            self.stats.lost_after_failure = self.stats.lost_after_failure.saturating_add(1);
            return;
        }
        let crc = record_checksum(record);
        let header_crc = header_checksum(record, crc);
        let before = record.get(..CRC_AT).unwrap_or_default();
        let payload = record.get(RECORD_HEADER_LEN..).unwrap_or_default();
        let result = self
            .segment
            .out
            .write_all(before)
            .and_then(|()| self.segment.out.write_all(&crc.to_le_bytes()))
            .and_then(|()| self.segment.out.write_all(&header_crc.to_le_bytes()))
            .and_then(|()| self.segment.out.write_all(payload));
        match result {
            Ok(()) => {
                let len = u64::try_from(record.len()).unwrap_or(0);
                self.segment.bytes = self.segment.bytes.saturating_add(len);
                self.stats.bytes = self.stats.bytes.saturating_add(len);
                self.stats.records = self.stats.records.saturating_add(1);
            }
            Err(source) => self.fail("write", source),
        }
    }

    fn rotate(&mut self) {
        self.guard("sync", |s| s.segment.sync());
        if self.error.is_some() {
            return;
        }
        let next = self.index.saturating_add(1);
        match Segment::create(&self.config.dir, next) {
            Ok(segment) => {
                self.segment = segment;
                self.index = next;
                self.stats.segments = self.stats.segments.saturating_add(1);
            }
            Err(error) => {
                self.shared.failed.store(true, Ordering::Relaxed);
                self.error.get_or_insert(error);
            }
        }
    }

    fn guard(&mut self, op: &'static str, action: impl FnOnce(&mut Self) -> io::Result<()>) {
        if self.error.is_some() {
            return;
        }
        if let Err(source) = action(self) {
            self.fail(op, source);
        }
    }

    fn fail(&mut self, op: &'static str, source: io::Error) {
        self.shared.failed.store(true, Ordering::Relaxed);
        self.error.get_or_insert(JournalError::Io {
            op,
            path: self.segment.path.clone(),
            source,
        });
    }
}
