//! Arbitrary bytes through the diff-depth decoder, and anything it accepts into a book sync.
//! Neither may panic; the sync must stay internally consistent.
#![no_main]

mod common;

use bowst_book::{BookSync, SyncConfig};
use bowst_venue::binance::depth::DepthUpdateDecoder;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let table = common::instruments();
    let mut decoder = DepthUpdateDecoder::new(64);
    let Ok(update) = decoder.decode(data, &table) else {
        return;
    };
    let config = SyncConfig { levels_per_side: 16, buffered_messages: 4, buffered_updates: 64 };
    let mut sync = BookSync::new(config).unwrap();
    // Go live on an empty snapshot just before the update, then apply it.
    let seq = update.range.first().saturating_sub(1);
    if sync.on_snapshot(seq, [], []).is_ok()
        && sync.on_delta(update.range, update.updates.iter().copied()).is_ok()
    {
        let book = sync.book().unwrap();
        assert!(!book.is_crossed());
        assert_eq!(sync.last_seq(), Some(update.range.last()));
    }
});
