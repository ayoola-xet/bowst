//! Arbitrary bytes through the JSON reader: it must return, never panic or recurse unbounded.
#![no_main]

use bowst_venue::json::Reader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut reader = Reader::new(data);
    if reader.skip().is_ok() {
        let _ = reader.finish();
    }
});
