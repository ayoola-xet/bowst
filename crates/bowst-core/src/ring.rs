//! Bounded, lock-free, single-producer/single-consumer ring buffer.
//!
//! This is the only way threads exchange data on the hot path (README §3). Properties:
//!
//! - Allocates once in [`channel`]. `push` and `pop` never allocate, lock or make syscalls.
//! - Elements are `Copy`, so slots never need dropping.
//! - Each slot carries a sequence number on the same cache line as its value, and slots are
//!   cache-line aligned. The consumer learns a value is ready from the slot itself, so handing
//!   over one message moves one cache line between cores (two if the value is larger than
//!   [`SINGLE_LINE_PAYLOAD`] bytes). Keep hot-path messages within that size.
//! - Only the producer writes slots. The consumer publishes its read position on its own cache
//!   line after every element, and the producer reads it only when its cached copy says the
//!   ring looks full. While the ring has room (its normal state on the hot path) freeing
//!   slots costs no cross-core traffic, and "full" is always exact.
//! - Measured on the same host, one-way handoff matches two bare atomic signals between the
//!   same threads (`benches/ring.rs`), so the ring adds no measurable latency of its own.
//! - A full ring hands the value back to the producer instead of blocking or overwriting.
//!   What to do about back-pressure is the caller's decision.
//!
//! Protocol, for position `p` (a wrapping counter) stored in slot `p % capacity`: the producer
//! writes the value, then stores `seq = p + 1`; the consumer at position `p` takes the value
//! once it sees `seq == p + 1`, then stores `head = p + 1`. The producer reuses a slot only
//! after observing that `head` has passed the slot's previous position.
#![allow(unsafe_code)] // Reviewed: see the SAFETY comments below. Tested under Miri in CI.

use core::cell::UnsafeCell;
use core::fmt;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Cache line size assumed for slot alignment.
const CACHE_LINE: usize = 64;

/// Largest value size that still fits a slot in one cache line alongside its sequence number.
pub const SINGLE_LINE_PAYLOAD: usize = CACHE_LINE - size_of::<usize>();

/// One ring slot: sequence number and value on the same cache line.
#[repr(C, align(64))]
struct Slot<T> {
    /// `position + 1` of the value last published here. Written only by the producer.
    seq: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// Aligns a value to 128 bytes so it never shares a cache line, or an adjacent-line prefetch
/// pair, with anything else.
#[derive(Debug, Default)]
#[repr(align(128))]
struct CachePadded<T>(T);

/// Error from [`channel`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RingError {
    /// Capacity must be a power of two and at least 2.
    #[error("ring capacity must be a power of two >= 2, got {0}")]
    InvalidCapacity(usize),
}

/// The ring was full. Carries back the value that was not sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("ring is full")]
pub struct Full<T>(pub T);

struct Shared<T> {
    /// Next position the consumer reads. Written only by the consumer.
    head: CachePadded<AtomicUsize>,
    slots: Box<[Slot<T>]>,
    mask: usize,
}

// SAFETY: Each slot's value is accessed by exactly one side at a time. The producer writes the
// value at position `p` only after an Acquire load of `head` shows the consumer has moved past
// position `p - capacity` (the slot's previous use), then publishes it with a Release store of
// `seq = p + 1`. The consumer reads it only after an Acquire load of `seq` returns `p + 1`,
// then hands it back with a Release store of `head = p + 1`. Sending `T` across threads
// therefore only requires `T: Send`.
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> Shared<T> {
    #[inline]
    fn slot(&self, position: usize) -> &Slot<T> {
        // `mask` is `len - 1` with `len` a power of two, so `position & mask < len`.
        match self.slots.get(position & self.mask) {
            Some(slot) => slot,
            None => unreachable!("position masked into range"),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.slots.len()
    }
}

/// Creates a ring with room for `capacity` elements.
///
/// # Errors
/// [`RingError::InvalidCapacity`] unless `capacity` is a power of two and at least 2.
pub fn channel<T: Copy + Send>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), RingError> {
    if capacity < 2 || !capacity.is_power_of_two() {
        return Err(RingError::InvalidCapacity(capacity));
    }
    let slots = (0..capacity)
        .map(|position: usize| Slot {
            // Never equal to `position + 1`, so no slot starts out looking published.
            seq: AtomicUsize::new(position.wrapping_sub(1)),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect();
    let shared = Arc::new(Shared {
        head: CachePadded::default(),
        slots,
        mask: capacity.wrapping_sub(1),
    });
    let producer = Producer {
        shared: Arc::clone(&shared),
        position: 0,
        cached_head: 0,
    };
    let consumer = Consumer {
        shared,
        position: 0,
    };
    Ok((producer, consumer))
}

/// Sending half of a ring. Exactly one exists per ring.
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
    /// Next position to write.
    position: usize,
    /// Last observed consumer `head`. May lag the real value, which only makes the ring look
    /// fuller than it is.
    cached_head: usize,
}

impl<T: Copy> Producer<T> {
    /// Sends `value`, or returns it in [`Full`] if the ring has no free slot.
    ///
    /// # Errors
    /// [`Full`] when the consumer has not yet made room.
    #[inline]
    pub fn push(&mut self, value: T) -> Result<(), Full<T>> {
        let capacity = self.shared.capacity();
        if self.position.wrapping_sub(self.cached_head) >= capacity {
            self.cached_head = self.shared.head.0.load(Ordering::Acquire);
            if self.position.wrapping_sub(self.cached_head) >= capacity {
                return Err(Full(value));
            }
        }
        let slot = self.shared.slot(self.position);
        // SAFETY: `position - head < capacity`, so the consumer has finished with this slot's
        // previous value (its Release store of `head` is ordered before our Acquire load) and
        // will not read this slot until our Release store of `seq` below. No other producer
        // exists.
        unsafe { slot.value.get().write(MaybeUninit::new(value)) };
        let next = self.position.wrapping_add(1);
        slot.seq.store(next, Ordering::Release);
        self.position = next;
        Ok(())
    }

    /// Number of elements the ring can hold.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.shared.capacity()
    }

    /// Whether the consumer has been dropped (for example, its thread exited). Callers should
    /// check this on their slow path and fail closed.
    #[must_use]
    pub fn is_abandoned(&self) -> bool {
        Arc::strong_count(&self.shared) == 1
    }
}

impl<T> fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producer")
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

/// Receiving half of a ring. Exactly one exists per ring.
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    /// Next position to read.
    position: usize,
}

impl<T: Copy> Consumer<T> {
    /// Takes the oldest element, or `None` if the ring is empty.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let slot = self.shared.slot(self.position);
        let next = self.position.wrapping_add(1);
        if slot.seq.load(Ordering::Acquire) != next {
            return None;
        }
        // SAFETY: `seq == position + 1`, so the producer wrote this slot and published it with
        // the Release store our Acquire load observed. It will not write the slot again until
        // our Release store of `head` below frees it.
        let value = unsafe { slot.value.get().read() };
        // SAFETY: A published slot always holds an initialized value (see above).
        let value = unsafe { value.assume_init() };
        self.shared.head.0.store(next, Ordering::Release);
        self.position = next;
        Some(value)
    }

    /// Whether no element is waiting right now. The producer may add one concurrently.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.shared.slot(self.position).seq.load(Ordering::Acquire) != self.position.wrapping_add(1)
    }

    /// Whether the producer has been dropped. Elements already sent can still be popped.
    #[must_use]
    pub fn is_abandoned(&self) -> bool {
        Arc::strong_count(&self.shared) == 1
    }
}

impl<T> fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Consumer")
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::VecDeque;

    #[test]
    fn rejects_invalid_capacity() {
        for capacity in [0, 1, 3, 100] {
            assert_eq!(
                channel::<u8>(capacity).unwrap_err(),
                RingError::InvalidCapacity(capacity)
            );
        }
    }

    #[test]
    fn fills_then_drains_in_order() {
        let (mut tx, mut rx) = channel::<u32>(4).unwrap();
        for i in 0..4 {
            tx.push(i).unwrap();
        }
        assert_eq!(tx.push(99), Err(Full(99)));
        assert!(!rx.is_empty());
        for i in 0..4 {
            assert_eq!(rx.pop(), Some(i));
        }
        assert_eq!(rx.pop(), None);
        assert!(rx.is_empty());
    }

    #[test]
    fn wraps_around_many_times() {
        let (mut tx, mut rx) = channel::<u64>(2).unwrap();
        for i in 0..1_000 {
            tx.push(i).unwrap();
            assert_eq!(rx.pop(), Some(i));
        }
    }

    #[test]
    fn reports_abandonment() {
        let (tx, rx) = channel::<u8>(2).unwrap();
        assert!(!tx.is_abandoned());
        drop(rx);
        assert!(tx.is_abandoned());
    }

    #[test]
    fn keeps_order_across_threads() {
        let count: u64 = if cfg!(miri) { 2_000 } else { 1_000_000 };
        let (mut tx, mut rx) = channel::<u64>(64).unwrap();
        let producer = std::thread::spawn(move || {
            for i in 0..count {
                let mut value = i;
                while let Err(Full(back)) = tx.push(value) {
                    value = back;
                    std::hint::spin_loop();
                }
            }
        });
        let mut expected = 0;
        while expected < count {
            if let Some(value) = rx.pop() {
                assert_eq!(value, expected);
                expected += 1;
            } else {
                std::hint::spin_loop();
            }
        }
        producer.join().unwrap();
        assert_eq!(rx.pop(), None);
    }

    #[derive(Clone, Debug)]
    enum Op {
        Push(u16),
        Pop,
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(if cfg!(miri) { 4 } else { 256 }))]

        #[test]
        fn behaves_like_a_bounded_queue(
            capacity_log2 in 1_u32..6,
            ops in proptest::collection::vec(
                prop_oneof![any::<u16>().prop_map(Op::Push), Just(Op::Pop)],
                0..200,
            ),
        ) {
            let capacity = 1_usize << capacity_log2;
            let (mut tx, mut rx) = channel::<u16>(capacity).unwrap();
            let mut model = VecDeque::new();
            for op in ops {
                match op {
                    Op::Push(v) => {
                        let result = tx.push(v);
                        if model.len() < capacity {
                            prop_assert_eq!(result, Ok(()));
                            model.push_back(v);
                        } else {
                            prop_assert_eq!(result, Err(Full(v)));
                        }
                    }
                    Op::Pop => prop_assert_eq!(rx.pop(), model.pop_front()),
                }
                prop_assert_eq!(rx.is_empty(), model.is_empty());
            }
        }
    }
}
