//! Global allocator wrapper that counts heap allocations per thread.
//!
//! Used by tests and benchmarks to prove hot paths do not allocate (CLAUDE.md §1.6):
//!
//! ```
//! use bowst_core::alloc_counter::{CountingAllocator, count_allocations};
//!
//! #[global_allocator]
//! static ALLOC: CountingAllocator = CountingAllocator;
//!
//! fn main() {
//!     let (_, allocations) = count_allocations(|| std::hint::black_box(2_u64) + 2);
//!     assert_eq!(allocations, 0);
//!     let (_, allocations) = count_allocations(|| vec![0_u8; 64]);
//!     assert_eq!(allocations, 1);
//! }
//! ```
//!
//! Counting is per thread, so allocations made concurrently by other test threads are not
//! attributed to the code under measurement.
#![allow(unsafe_code)] // Reviewed: thin delegation to `System`, see SAFETY comments.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    // `const` initialization: accessing this never allocates, which matters inside `alloc`.
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn record() {
    // `try_with` fails only during thread teardown; missing those counts is harmless.
    let _ = ALLOCATIONS.try_with(|count| count.set(count.get().saturating_add(1)));
}

/// Delegates to the system allocator and counts allocations on the calling thread.
#[derive(Clone, Copy, Debug, Default)]
pub struct CountingAllocator;

// SAFETY: Every method delegates to `System` with the caller's arguments unchanged, so this
// allocator upholds exactly the guarantees `System` does. Counting touches only a
// const-initialized thread-local `Cell`, which never allocates or re-enters the allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        // SAFETY: Forwarded unchanged; the caller upholds `GlobalAlloc::alloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record();
        // SAFETY: Forwarded unchanged; the caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record();
        // SAFETY: Forwarded unchanged; the caller upholds `GlobalAlloc::realloc`'s contract.
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Forwarded unchanged; the caller upholds `GlobalAlloc::dealloc`'s contract.
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Runs `f` and returns its result with the number of heap allocations (including
/// reallocations) it made on the current thread. Only meaningful when [`CountingAllocator`]
/// is installed as the global allocator; otherwise the count is always zero.
pub fn count_allocations<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = ALLOCATIONS.with(Cell::get);
    let result = f();
    let after = ALLOCATIONS.with(Cell::get);
    (result, after.saturating_sub(before))
}
