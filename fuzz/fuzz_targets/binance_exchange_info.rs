//! Arbitrary bytes through the exchangeInfo decoder: it must return, never panic.
#![no_main]

use bowst_venue::binance::exchange_info::decode_exchange_info;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = decode_exchange_info(data);
});
