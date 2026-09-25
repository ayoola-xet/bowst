//! Arbitrary bytes as a WebSocket upgrade response: validation must return, never panic.
#![no_main]

use bowst_venue::ws::handshake::Handshake;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let handshake = Handshake::new(*b"the sample nonce");
    if let Ok(Some(len)) = handshake.parse_response(data) {
        assert!(len <= data.len());
    }
});
