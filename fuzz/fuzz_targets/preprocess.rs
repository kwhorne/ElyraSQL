#![no_main]
//! Fuzz the SQL string-preprocessing + parse pipeline.
//!
//! Feeds arbitrary bytes (interpreted as UTF-8 where valid) through
//! `elyra_engine::fuzz_preprocess_parse`, which runs every string rewriter
//! (INSERT..SET, comma UPDATE, CREATE-options / DML-LIMIT stripping,
//! top-level splitting) and both parser dialects. The invariant is simply that
//! it must never panic. Run with:
//!
//!   cargo +nightly fuzz run preprocess

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        elyra_engine::fuzz_preprocess_parse(s);
    }
});
