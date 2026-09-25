//! Arbitrary bytes as a journal segment: decoding must return, never panic, and every decoded
//! record must lie within the input.
#![no_main]

use bowst_journal::format::{Decoded, FILE_HEADER_LEN, decode_file_header, decode_record};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_file_header(data);
    let mut pos = FILE_HEADER_LEN.min(data.len());
    while let Ok(Decoded::Record { len, .. }) = decode_record(&data[pos..], pos as u64) {
        assert!(len >= 32 && pos + len <= data.len());
        pos += len;
    }
});
