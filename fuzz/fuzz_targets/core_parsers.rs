#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(input) = std::str::from_utf8(data) else {
        return;
    };

    if let Some(json) = elyra_core::json::parse(input) {
        let rendered = json.to_json_string();
        assert!(elyra_core::json::parse(&rendered).is_some());
        let _ = json.extract(input);
    }
    let _ = elyra_core::datetime::parse_date(input);
    let _ = elyra_core::datetime::parse_datetime(input);
    let _ = elyra_core::datetime::parse_time(input);
    for scale in [0, 1, 6, 18, u8::MAX] {
        let _ = elyra_core::value::parse_decimal(input, scale);
    }
});
