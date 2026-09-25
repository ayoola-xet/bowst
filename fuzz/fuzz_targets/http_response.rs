//! Arbitrary bytes as an HTTP response: parsing must return, never panic, and any decoded
//! body must respect the size limit.
#![no_main]

use bowst_venue::net::http::parse_response;
use libfuzzer_sys::fuzz_target;

const MAX_BODY: usize = 4096;

fuzz_target!(|data: &[u8]| {
    let mut body = Vec::new();
    if parse_response(data, &mut body, MAX_BODY).is_ok() {
        assert!(body.len() <= MAX_BODY);
    }
});
