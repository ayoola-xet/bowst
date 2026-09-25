//! Arbitrary bytes through the depth snapshot decoder: it must return, never panic.
#![no_main]

mod common;

use bowst_core::InstrumentId;
use bowst_venue::binance::depth::DepthSnapshotDecoder;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let table = common::instruments();
    let instrument = table.get(InstrumentId::new(0)).unwrap();
    let mut decoder = DepthSnapshotDecoder::new(64);
    let _ = decoder.decode(data, instrument);
});
