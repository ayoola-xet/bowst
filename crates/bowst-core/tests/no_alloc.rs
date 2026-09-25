//! Proves the hot-path operations in `bowst-core` never touch the heap after construction.

use bowst_core::alloc_counter::{CountingAllocator, count_allocations};
use bowst_core::fixed::MAX_TEXT_LEN;
use bowst_core::{ClientOrderIdGen, Dec, Increment, Price, Rounding, Side, bytes_ring, ring};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

/// Loop iterations per test; kept small under Miri, which is orders of magnitude slower.
const ROUNDS: u64 = if cfg!(miri) { 64 } else { 10_000 };

#[test]
fn counter_detects_allocations() {
    let (vec, allocations) = count_allocations(|| vec![1_u8; 16]);
    assert_eq!(vec.len(), 16);
    assert!(allocations >= 1);
}

#[test]
fn ring_push_and_pop_do_not_allocate() {
    let (mut tx, mut rx) = ring::channel::<[u64; 4]>(1024).unwrap();
    let ((), allocations) = count_allocations(|| {
        for round in 0..ROUNDS {
            tx.push([round; 4]).unwrap();
            assert_eq!(rx.pop(), Some([round; 4]));
        }
        assert_eq!(rx.pop(), None);
    });
    assert_eq!(allocations, 0);
}

#[test]
fn decimal_parse_convert_and_format_do_not_allocate() {
    let tick = Increment::parse("0.01").unwrap();
    let ((), allocations) = count_allocations(|| {
        let fair = Dec::parse("43251.37500000").unwrap();
        let bid = Price::from_dec(fair, tick, Side::Buy.passive_rounding()).unwrap();
        let ask = Price::from_dec(fair, tick, Rounding::Up).unwrap();
        assert!(ask > bid);
        let mut buf = [0_u8; MAX_TEXT_LEN];
        let len = bid.to_dec(tick).write_to(&mut buf).unwrap();
        assert_eq!(&buf[..len], b"43251.37");
    });
    assert_eq!(allocations, 0);
}

#[test]
fn client_order_ids_do_not_allocate() {
    let mut ids = ClientOrderIdGen::new(1);
    let ((), allocations) = count_allocations(|| {
        for _ in 0..ROUNDS {
            let id = ids.next().unwrap();
            assert_eq!(id.encode().as_str().len(), 18);
        }
    });
    assert_eq!(allocations, 0);
}

#[test]
fn byte_ring_write_and_read_do_not_allocate() {
    let (mut tx, mut rx) = bytes_ring::channel(4096).unwrap();
    let record = [5_u8; 459];
    let ((), allocations) = count_allocations(|| {
        for _ in 0..ROUNDS {
            tx.write_with(record.len() + 8, |dst| {
                dst[..8].copy_from_slice(&42_u64.to_le_bytes());
                dst[8..].copy_from_slice(&record);
            })
            .unwrap();
            assert_eq!(rx.read_with(<[u8]>::len), Some(467));
        }
    });
    assert_eq!(allocations, 0);
}
