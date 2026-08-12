#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    elyra_wire::fuzz_parse_protocol(data);
});
