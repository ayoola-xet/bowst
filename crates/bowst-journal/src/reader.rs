//! Reading a journal back, for replay and post-mortems.

use std::fs;
use std::path::{Path, PathBuf};

use crate::format::{
    Decoded, FILE_HEADER_LEN, FormatError, RecordHeader, decode_file_header, decode_record,
};
use crate::journal::segment_index;

/// One record read from a journal. The payload borrows the reader's buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record<'a> {
    /// Header fields.
    pub header: RecordHeader,
    /// Payload bytes.
    pub payload: &'a [u8],
}

/// A journal that cannot be read.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    /// A file could not be listed or read.
    #[error("read {path}: {source}")]
    Io {
        /// File or directory.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: std::io::Error,
    },
    /// Corruption: a bad header or checksum, or a truncated record that is not the final one.
    #[error("{path}: {error}")]
    Corrupt {
        /// Segment file.
        path: PathBuf,
        /// What is wrong.
        error: FormatError,
    },
    /// A record is cut short somewhere other than the very end of the journal.
    #[error("{path}: truncated record at byte {offset}")]
    Truncated {
        /// Segment file.
        path: PathBuf,
        /// Offset of the truncated record.
        offset: u64,
    },
    /// Segment numbers are missing: a file was lost.
    #[error("segment {expected} is missing (found {found})")]
    MissingSegment {
        /// The next index expected.
        expected: u32,
        /// The index found instead.
        found: u32,
    },
}

/// Reads every record of a journal directory in order, verifying each checksum.
///
/// A crash can leave the final record of the final segment incomplete; that one torn record is
/// skipped and reported by [`torn_tail`](Self::torn_tail). Any other damage is an error.
#[derive(Debug)]
pub struct JournalReader {
    files: Vec<PathBuf>,
    next_file: usize,
    data: Vec<u8>,
    pos: usize,
    torn_tail: bool,
}

impl JournalReader {
    /// Lists the journal's segments.
    ///
    /// # Errors
    /// [`ReadError::Io`] or [`ReadError::MissingSegment`].
    pub fn open(dir: &Path) -> Result<Self, ReadError> {
        let io = |source| ReadError::Io {
            path: dir.to_path_buf(),
            source,
        };
        let mut files: Vec<(u32, PathBuf)> = fs::read_dir(dir)
            .map_err(io)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter_map(|path| segment_index(&path).map(|index| (index, path)))
            .collect();
        files.sort_unstable_by_key(|(index, _)| *index);
        for pair in files.windows(2) {
            if let [(a, _), (b, _)] = pair
                && a.saturating_add(1) != *b
            {
                return Err(ReadError::MissingSegment {
                    expected: a.saturating_add(1),
                    found: *b,
                });
            }
        }
        Ok(Self {
            files: files.into_iter().map(|(_, path)| path).collect(),
            next_file: 0,
            data: Vec::new(),
            pos: 0,
            torn_tail: false,
        })
    }

    /// Whether the journal ended with one incomplete record (an unclean shutdown).
    #[must_use]
    pub fn torn_tail(&self) -> bool {
        self.torn_tail
    }

    fn current_path(&self) -> PathBuf {
        self.files
            .get(self.next_file.saturating_sub(1))
            .cloned()
            .unwrap_or_default()
    }

    /// Loads the next segment. `false` when there are no more.
    fn load_next(&mut self) -> Result<bool, ReadError> {
        let Some(path) = self.files.get(self.next_file).cloned() else {
            return Ok(false);
        };
        self.next_file = self.next_file.saturating_add(1);
        self.data = fs::read(&path).map_err(|source| ReadError::Io {
            path: path.clone(),
            source,
        })?;
        decode_file_header(&self.data).map_err(|error| ReadError::Corrupt { path, error })?;
        self.pos = FILE_HEADER_LEN;
        Ok(true)
    }

    /// The next record, or `None` at the end of the journal.
    ///
    /// # Errors
    /// [`ReadError`] for corruption, truncation before the end, or I/O failure.
    pub fn next_record(&mut self) -> Result<Option<Record<'_>>, ReadError> {
        // Skip exhausted and empty (header-only) segments.
        while self.pos >= self.data.len() {
            if !self.load_next()? {
                return Ok(None);
            }
        }
        let rest = self.data.get(self.pos..).unwrap_or_default();
        let offset = u64::try_from(self.pos).unwrap_or(u64::MAX);
        let (header, len) = match decode_record(rest, offset) {
            Ok(Decoded::Record { header, len }) => (header, len),
            Ok(Decoded::Incomplete) => {
                if self.next_file < self.files.len() {
                    return Err(ReadError::Truncated {
                        path: self.current_path(),
                        offset,
                    });
                }
                self.torn_tail = true;
                self.pos = self.data.len();
                return Ok(None);
            }
            Err(error) => {
                return Err(ReadError::Corrupt {
                    path: self.current_path(),
                    error,
                });
            }
        };
        let start = self.pos;
        let end = start.saturating_add(len);
        self.pos = end;
        let payload_start = start.saturating_add(crate::format::RECORD_HEADER_LEN);
        Ok(Some(Record {
            header,
            payload: self.data.get(payload_start..end).unwrap_or_default(),
        }))
    }
}
