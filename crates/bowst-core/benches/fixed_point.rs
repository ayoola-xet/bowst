//! Decimal parsing and conversion benchmarks for the market-data decode path.

// Benchmarks are test code: failing fast on a broken setup is the right behavior.
#![allow(missing_docs, clippy::unwrap_used)]

use std::hint::black_box;

use bowst_core::fixed::MAX_TEXT_LEN;
use bowst_core::{Dec, Increment, Rounding};
use criterion::{Criterion, criterion_group, criterion_main};

fn fixed_point(c: &mut Criterion) {
    let tick = Increment::parse("0.01").unwrap();
    let price = Dec::parse("43251.37000000").unwrap();

    c.bench_function("fixed/parse_price_text", |b| {
        b.iter(|| Dec::parse(black_box("43251.37000000")));
    });
    c.bench_function("fixed/dec_to_ticks", |b| {
        b.iter(|| black_box(tick).to_units(black_box(price), Rounding::Exact));
    });
    c.bench_function("fixed/parse_and_convert", |b| {
        b.iter(|| {
            let dec = Dec::parse(black_box("43251.37000000")).unwrap();
            tick.to_units(dec, Rounding::Exact)
        });
    });
    let lot = Increment::parse("0.00001").unwrap();
    c.bench_function("fixed/parse_units_price", |b| {
        b.iter(|| black_box(tick).parse_units(black_box("43251.37000000"), Rounding::Exact));
    });
    c.bench_function("fixed/parse_units_qty", |b| {
        b.iter(|| black_box(lot).parse_units(black_box("5.01777000"), Rounding::Exact));
    });
    c.bench_function("fixed/format_price", |b| {
        let mut buf = [0_u8; MAX_TEXT_LEN];
        b.iter(|| black_box(price).write_to(&mut buf));
    });
}

criterion_group!(benches, fixed_point);
criterion_main!(benches);
