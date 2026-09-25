//! Proves the WebSocket reader and frame encoder never allocate once built.

// Test helpers fail fast on broken setup.
#![allow(clippy::unwrap_used)]

use bowst_core::alloc_counter::{CountingAllocator, count_allocations};
use bowst_venue::ws::frame::{Opcode, encode_client_frame};
use bowst_venue::ws::reader::{ReaderConfig, WsEvent, WsReader};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

const ROUNDS: usize = if cfg!(miri) { 20 } else { 10_000 };

#[test]
fn reading_and_encoding_do_not_allocate() {
    let config = ReaderConfig {
        buffer: 64 * 1024,
        max_frame: 32 * 1024,
        max_message: 64 * 1024,
    };
    let mut reader = WsReader::new(config).unwrap();
    let text = br#"{"e":"depthUpdate","s":"BTCUSDT"}"#;
    let mut unfragmented = vec![0x81, u8::try_from(text.len()).unwrap()];
    unfragmented.extend_from_slice(text);
    // A fragmented message with a ping in the middle exercises reassembly.
    let mut fragmented = vec![0x01, 0x02, b'a', b'b', 0x89, 0x01, b'p', 0x80, 0x01, b'c'];
    fragmented.extend_from_slice(&unfragmented);
    let mut out = [0_u8; 256];

    let ((), allocations) = count_allocations(|| {
        for _ in 0..ROUNDS {
            for chunk in fragmented.chunks(7) {
                let spare = reader.spare();
                spare[..chunk.len()].copy_from_slice(chunk);
                reader.commit(chunk.len());
                while let Some(event) = reader.next_event().unwrap() {
                    if let WsEvent::Ping(payload) = event {
                        encode_client_frame(Opcode::Pong, payload, [1, 2, 3, 4], &mut out).unwrap();
                    }
                }
            }
        }
    });
    assert_eq!(allocations, 0);
}
