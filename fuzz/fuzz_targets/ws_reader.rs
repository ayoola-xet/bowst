//! Arbitrary bytes through the WebSocket reader, delivered in chunks whose sizes come from
//! the input itself. It must never panic, and every returned payload must lie within limits.
#![no_main]

use bowst_venue::ws::reader::{ReaderConfig, WsEvent, WsReader};
use libfuzzer_sys::fuzz_target;

const CONFIG: ReaderConfig = ReaderConfig { buffer: 4096, max_frame: 2048, max_message: 3000 };

fuzz_target!(|data: &[u8]| {
    let Some((&chunk_seed, stream)) = data.split_first() else {
        return;
    };
    let chunk = usize::from(chunk_seed % 64) + 1;
    let mut reader = WsReader::new(CONFIG).unwrap();
    for piece in stream.chunks(chunk) {
        let spare = reader.spare();
        let n = piece.len().min(spare.len());
        spare[..n].copy_from_slice(&piece[..n]);
        reader.commit(n);
        loop {
            match reader.next_event() {
                Ok(Some(WsEvent::Text(p) | WsEvent::Binary(p))) => assert!(p.len() <= CONFIG.max_message),
                Ok(Some(WsEvent::Ping(p) | WsEvent::Pong(p))) => assert!(p.len() <= 125),
                Ok(Some(WsEvent::Close { reason, .. })) => assert!(reason.len() <= 123),
                Ok(None) => break,
                Err(_) => return,
            }
        }
    }
});
