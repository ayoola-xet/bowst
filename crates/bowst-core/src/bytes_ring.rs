//! Bounded, lock-free, single-producer/single-consumer ring of variable-length byte records.
//!
//! The fixed-size [`ring`](crate::ring) carries hot-path events between threads. This ring
//! carries byte records of any size up to half its capacity: raw venue messages on their way
//! to the journal, for example. Properties:
//!
//! - Allocates once in [`channel`]. Writing and reading never allocate, lock or make syscalls.
//! - Each record is one contiguous slice, so readers see it without reassembly. A record that
//!   would run past the end of the buffer is written at the start instead, after a wrap marker.
//! - Records are 8-byte aligned: an 8-byte header (length) then the payload, padded to 8.
//! - The producer writes in place through a closure, so a record can be built from several
//!   parts (for example a journal header and a message) with a single copy.
//! - A full ring returns an error instead of blocking or overwriting. What to do about it is
//!   the caller's decision.
//! - Each side caches the other side's position and only reads the shared one when its cache
//!   says the ring looks full (producer) or empty (consumer).
#![allow(unsafe_code)] // Reviewed: see the SAFETY comments below. Tested under Miri in CI.

use core::cell::UnsafeCell;
use core::fmt;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::ring::CachePadded;

/// Bytes before each record's payload: a little-endian `u32` length and 4 reserved bytes.
const HEADER: usize = 8;
/// Record alignment.
const ALIGN: usize = 8;
/// Length value marking "the rest of the buffer is padding; continue at the start".
const WRAP: u32 = u32::MAX;
/// Smallest allowed capacity.
pub const MIN_CAPACITY: usize = 64;

/// Error from [`channel`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BytesRingError {
    /// Capacity must be a power of two, at least [`MIN_CAPACITY`], and addressable by a `u32`.
    #[error("capacity must be a power of two between {MIN_CAPACITY} and 2^31, got {0}")]
    InvalidCapacity(usize),
}

/// Why a record was not written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WriteError {
    /// The consumer has not freed enough space yet.
    #[error("ring is full")]
    Full,
    /// The record can never fit: it is larger than [`ByteProducer::max_record`].
    #[error("record of {len} bytes exceeds the maximum of {max}")]
    TooLarge {
        /// Requested length.
        len: usize,
        /// Largest record this ring accepts.
        max: usize,
    },
}

struct Shared {
    /// Next position the consumer reads. Written only by the consumer.
    head: CachePadded<AtomicUsize>,
    /// Next position the producer writes. Written only by the producer.
    tail: CachePadded<AtomicUsize>,
    buf: Box<[UnsafeCell<u8>]>,
    mask: usize,
}

// SAFETY: Byte regions are handed between exactly one producer and one consumer. The producer
// only writes regions in `[tail, head + capacity)`, which the consumer never reads, and makes
// them readable with a Release store of `tail`. The consumer only reads regions in
// `[head, tail)` after an Acquire load of `tail`, and gives them back with a Release store of
// `head`, which the producer Acquire-loads before reusing them.
unsafe impl Sync for Shared {}

impl Shared {
    fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Mutable view of `len` bytes at physical index `start`.
    ///
    /// # Safety
    /// The caller must own the region exclusively (see the `Sync` impl) for the lifetime of
    /// the returned slice.
    #[allow(clippy::mut_from_ref)] // Interior mutability through `UnsafeCell`; see above.
    unsafe fn region_mut(&self, start: usize, len: usize) -> &mut [u8] {
        let end = start.saturating_add(len);
        // Bounds-checked: an out-of-range region is a bug, and stops the program rather than
        // corrupting memory.
        let Some(cells) = self.buf.get(start..end) else {
            unreachable!("ring region out of bounds: invariant violated");
        };
        let ptr = UnsafeCell::raw_get(cells.as_ptr());
        // SAFETY: `ptr` points to `len` initialized bytes inside `buf` (bounds checked above);
        // `UnsafeCell<u8>` has the same layout as `u8`; the caller guarantees exclusivity.
        unsafe { core::slice::from_raw_parts_mut(ptr, len) }
    }

    /// Shared view of `len` bytes at physical index `start`.
    ///
    /// # Safety
    /// The region must not be written while the returned slice lives (see the `Sync` impl).
    unsafe fn region(&self, start: usize, len: usize) -> &[u8] {
        let end = start.saturating_add(len);
        let Some(cells) = self.buf.get(start..end) else {
            unreachable!("ring region out of bounds: invariant violated");
        };
        let ptr = UnsafeCell::raw_get(cells.as_ptr());
        // SAFETY: As for `region_mut`, with no concurrent writer by the caller's guarantee.
        unsafe { core::slice::from_raw_parts(ptr.cast_const(), len) }
    }
}

fn padded(len: usize) -> usize {
    len.saturating_add(ALIGN - 1) & !(ALIGN - 1)
}

/// Creates a ring of `capacity` bytes.
///
/// # Errors
/// [`BytesRingError::InvalidCapacity`] unless `capacity` is a power of two between
/// [`MIN_CAPACITY`] and 2^31.
pub fn channel(capacity: usize) -> Result<(ByteProducer, ByteConsumer), BytesRingError> {
    if !capacity.is_power_of_two() || !(MIN_CAPACITY..=(1 << 31)).contains(&capacity) {
        return Err(BytesRingError::InvalidCapacity(capacity));
    }
    let buf = (0..capacity).map(|_| UnsafeCell::new(0)).collect();
    let shared = Arc::new(Shared {
        head: CachePadded::default(),
        tail: CachePadded::default(),
        buf,
        mask: capacity.wrapping_sub(1),
    });
    Ok((
        ByteProducer {
            shared: Arc::clone(&shared),
            tail: 0,
            cached_head: 0,
        },
        ByteConsumer {
            shared,
            head: 0,
            cached_tail: 0,
        },
    ))
}

/// Writing half. Exactly one exists per ring.
pub struct ByteProducer {
    shared: Arc<Shared>,
    tail: usize,
    cached_head: usize,
}

impl ByteProducer {
    /// Largest record this ring accepts: half its capacity minus the header, so any record
    /// fits once the consumer catches up, wherever the write position is.
    #[must_use]
    pub fn max_record(&self) -> usize {
        (self.shared.capacity() / 2).saturating_sub(HEADER)
    }

    /// Writes a `len`-byte record, letting `fill` write it in place. `fill` must fill the
    /// whole slice it is given; nothing is visible to the consumer until it returns.
    ///
    /// # Errors
    /// [`WriteError::Full`] (try later or drop), or [`WriteError::TooLarge`] (never fits).
    #[inline]
    pub fn write_with(
        &mut self,
        len: usize,
        fill: impl FnOnce(&mut [u8]),
    ) -> Result<(), WriteError> {
        let max = self.max_record();
        if len > max {
            return Err(WriteError::TooLarge { len, max });
        }
        let capacity = self.shared.capacity();
        let size = HEADER.saturating_add(padded(len));
        let index = self.tail & self.shared.mask;
        let to_end = capacity.saturating_sub(index);
        // Records never straddle the end: pad to the end and start over at index 0.
        let (skip, start) = if size > to_end {
            (to_end, 0)
        } else {
            (0, index)
        };
        let needed = skip.saturating_add(size);
        if capacity.saturating_sub(self.tail.wrapping_sub(self.cached_head)) < needed {
            self.cached_head = self.shared.head.0.load(Ordering::Acquire);
            if capacity.saturating_sub(self.tail.wrapping_sub(self.cached_head)) < needed {
                return Err(WriteError::Full);
            }
        }
        // SAFETY (all three blocks): `[tail, tail + needed)` is free (checked against `head`
        // above), so the consumer does not read it until the Release store below, and no
        // other producer exists.
        if skip > 0 {
            // SAFETY: see above.
            let marker = unsafe { self.shared.region_mut(index, 4) };
            marker.copy_from_slice(&WRAP.to_le_bytes());
        }
        // SAFETY: see above.
        fill(unsafe { self.shared.region_mut(start.saturating_add(HEADER), len) });
        let header = u32::try_from(len).unwrap_or(WRAP - 1).to_le_bytes();
        // SAFETY: see above.
        let slot = unsafe { self.shared.region_mut(start, 4) };
        slot.copy_from_slice(&header);
        self.tail = self.tail.wrapping_add(needed);
        self.shared.tail.0.store(self.tail, Ordering::Release);
        Ok(())
    }

    /// Writes `bytes` as one record.
    ///
    /// # Errors
    /// As [`write_with`](Self::write_with).
    #[inline]
    pub fn push(&mut self, bytes: &[u8]) -> Result<(), WriteError> {
        self.write_with(bytes.len(), |dst| dst.copy_from_slice(bytes))
    }

    /// Whether the consumer has been dropped.
    #[must_use]
    pub fn is_abandoned(&self) -> bool {
        Arc::strong_count(&self.shared) == 1
    }
}

impl fmt::Debug for ByteProducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteProducer")
            .field("tail", &self.tail)
            .finish_non_exhaustive()
    }
}

/// Reading half. Exactly one exists per ring.
pub struct ByteConsumer {
    shared: Arc<Shared>,
    head: usize,
    cached_tail: usize,
}

impl ByteConsumer {
    /// Passes the oldest record to `read` and removes it. `None` if the ring is empty.
    #[inline]
    pub fn read_with<R>(&mut self, read: impl FnOnce(&[u8]) -> R) -> Option<R> {
        loop {
            if self.head == self.cached_tail {
                self.cached_tail = self.shared.tail.0.load(Ordering::Acquire);
                if self.head == self.cached_tail {
                    return None;
                }
            }
            let capacity = self.shared.capacity();
            let index = self.head & self.shared.mask;
            // SAFETY: `[head, tail)` was published by the producer's Release store of `tail`,
            // observed by the Acquire load above, and is not rewritten until `head` moves on.
            let header = unsafe { self.shared.region(index, 4) };
            let len = u32::from_le_bytes(<[u8; 4]>::try_from(header).unwrap_or([0; 4]));
            if len == WRAP {
                self.head = self.head.wrapping_add(capacity.saturating_sub(index));
                self.shared.head.0.store(self.head, Ordering::Release);
                continue;
            }
            let len = usize::try_from(len).unwrap_or(0);
            // SAFETY: As above; the payload lies inside the published record.
            let payload = unsafe { self.shared.region(index.saturating_add(HEADER), len) };
            let result = read(payload);
            self.head = self.head.wrapping_add(HEADER.saturating_add(padded(len)));
            self.shared.head.0.store(self.head, Ordering::Release);
            return Some(result);
        }
    }

    /// Whether the producer has been dropped. Records already written can still be read.
    #[must_use]
    pub fn is_abandoned(&self) -> bool {
        Arc::strong_count(&self.shared) == 1
    }
}

impl fmt::Debug for ByteConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteConsumer")
            .field("head", &self.head)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::VecDeque;

    fn pop(rx: &mut ByteConsumer) -> Option<Vec<u8>> {
        rx.read_with(<[u8]>::to_vec)
    }

    #[test]
    fn rejects_invalid_capacity() {
        for capacity in [0, 32, 100, 3 << 20] {
            assert!(channel(capacity).is_err(), "{capacity}");
        }
    }

    #[test]
    fn round_trips_records_of_any_length() {
        let (mut tx, mut rx) = channel(256).unwrap();
        assert_eq!(tx.max_record(), 120);
        for len in [0, 1, 7, 8, 9, 120] {
            let record: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
            tx.push(&record).unwrap();
            assert_eq!(pop(&mut rx), Some(record));
        }
        assert_eq!(pop(&mut rx), None);
        assert_eq!(
            tx.push(&[0; 121]),
            Err(WriteError::TooLarge { len: 121, max: 120 })
        );
    }

    #[test]
    fn wraps_records_to_the_start_instead_of_splitting_them() {
        let (mut tx, mut rx) = channel(64).unwrap();
        // 24-byte records (8 header + 16) at 0, 24 and 48: the third leaves a 16-byte tail
        // that cannot hold the next one, so it goes to the start after a wrap marker.
        for round in 0..20_u8 {
            tx.push(&[round; 16]).unwrap();
            assert_eq!(pop(&mut rx), Some(vec![round; 16]));
        }
    }

    #[test]
    fn reports_full_and_recovers() {
        let (mut tx, mut rx) = channel(64).unwrap();
        tx.push(&[1; 24]).unwrap();
        tx.push(&[2; 24]).unwrap();
        assert_eq!(tx.push(&[3; 8]), Err(WriteError::Full));
        assert_eq!(pop(&mut rx), Some(vec![1; 24]));
        tx.push(&[3; 8]).unwrap();
        assert_eq!(pop(&mut rx), Some(vec![2; 24]));
        assert_eq!(pop(&mut rx), Some(vec![3; 8]));
    }

    #[test]
    fn builds_records_from_parts_in_place() {
        let (mut tx, mut rx) = channel(128).unwrap();
        tx.write_with(11, |dst| {
            dst[..5].copy_from_slice(b"head:");
            dst[5..].copy_from_slice(b"body!!");
        })
        .unwrap();
        assert_eq!(pop(&mut rx), Some(b"head:body!!".to_vec()));
    }

    #[test]
    fn keeps_order_across_threads() {
        let count: u32 = if cfg!(miri) { 300 } else { 200_000 };
        let (mut tx, mut rx) = channel(4096).unwrap();
        let producer = std::thread::spawn(move || {
            for i in 0..count {
                let len = usize::try_from(i % 97).unwrap() + 4;
                loop {
                    let written = tx.write_with(len, |dst| {
                        dst[..4].copy_from_slice(&i.to_le_bytes());
                        dst[4..].fill(u8::try_from(i % 256).unwrap());
                    });
                    match written {
                        Ok(()) => break,
                        Err(WriteError::Full) => std::hint::spin_loop(),
                        Err(e) => panic!("{e}"),
                    }
                }
            }
        });
        let mut expected = 0_u32;
        while expected < count {
            let got = rx.read_with(|record| {
                let i = u32::from_le_bytes(record[..4].try_into().unwrap());
                assert_eq!(record.len(), usize::try_from(i % 97).unwrap() + 4);
                assert!(record[4..].iter().all(|&b| u32::from(b) == i % 256));
                i
            });
            match got {
                Some(i) => {
                    assert_eq!(i, expected);
                    expected += 1;
                }
                None => std::hint::spin_loop(),
            }
        }
        producer.join().unwrap();
        assert_eq!(pop(&mut rx), None);
    }

    #[derive(Clone, Debug)]
    enum Op {
        Push(Vec<u8>),
        Pop,
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(if cfg!(miri) { 4 } else { 512 }))]

        /// Against a model: records come out in order, intact, and `Full` only happens when
        /// the ring truly lacks contiguous room.
        #[test]
        fn behaves_like_a_bounded_record_queue(
            capacity_log2 in 6_u32..10,
            ops in proptest::collection::vec(
                prop_oneof![
                    proptest::collection::vec(any::<u8>(), 0..200).prop_map(Op::Push),
                    Just(Op::Pop),
                ],
                0..300,
            ),
        ) {
            let capacity = 1_usize << capacity_log2;
            let (mut tx, mut rx) = channel(capacity).unwrap();
            let max = tx.max_record();
            let mut model: VecDeque<Vec<u8>> = VecDeque::new();
            for op in ops {
                match op {
                    Op::Push(record) => match tx.push(&record) {
                        Ok(()) => model.push_back(record),
                        Err(WriteError::TooLarge { .. }) => prop_assert!(record.len() > max),
                        Err(WriteError::Full) => {
                            // Full is legitimate only if the model holds a lot of data.
                            let used: usize = model.iter().map(|r| 8 + r.len().div_ceil(8) * 8).sum();
                            prop_assert!(used + 8 + record.len().div_ceil(8) * 8 > capacity / 2);
                        }
                    },
                    Op::Pop => prop_assert_eq!(pop(&mut rx), model.pop_front()),
                }
            }
            while let Some(expected) = model.pop_front() {
                prop_assert_eq!(pop(&mut rx), Some(expected));
            }
            prop_assert_eq!(pop(&mut rx), None);
        }
    }
}
