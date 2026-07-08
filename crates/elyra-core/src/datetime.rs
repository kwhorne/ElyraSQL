//! Minimal, dependency-free date/time helpers for ElyraSQL.
//!
//! `DATE` is stored as days since 1970-01-01; `DATETIME`/`TIMESTAMP` as
//! microseconds since the Unix epoch. Conversions use Howard Hinnant's
//! proleptic-Gregorian algorithms (valid across the full range).

/// Days from civil date (proleptic Gregorian). 1970-01-01 -> 0.
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Civil date from days since epoch. Returns `(year, month, day)`.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Parse `YYYY-MM-DD` into days since epoch.
pub fn parse_date(s: &str) -> Option<i32> {
    let s = s.trim();
    let mut it = s.splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d) as i32)
}

/// Parse `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` into microseconds since epoch.
pub fn parse_datetime(s: &str) -> Option<i64> {
    let s = s.trim();
    let (date_part, time_part) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (s, None),
    };
    let days = parse_date(date_part)? as i64;
    let mut micros = days * 86_400 * 1_000_000;
    if let Some(t) = time_part {
        let (hms, frac) = match t.split_once('.') {
            Some((a, b)) => (a, Some(b)),
            None => (t, None),
        };
        let mut it = hms.splitn(3, ':');
        let h: i64 = it.next()?.parse().ok()?;
        let mi: i64 = it.next().unwrap_or("0").parse().ok()?;
        let se: i64 = it.next().unwrap_or("0").parse().ok()?;
        micros += (h * 3600 + mi * 60 + se) * 1_000_000;
        if let Some(f) = frac {
            let f = format!("{:0<6}", &f[..f.len().min(6)]);
            micros += f.parse::<i64>().ok()?;
        }
    }
    Some(micros)
}

/// Parse `HH:MM:SS[.ffffff]` into microseconds since midnight.
pub fn parse_time(s: &str) -> Option<i64> {
    let s = s.trim();
    let (hms, frac) = match s.split_once('.') {
        Some((a, b)) => (a, Some(b)),
        None => (s, None),
    };
    let mut it = hms.splitn(3, ':');
    let h: i64 = it.next()?.parse().ok()?;
    let mi: i64 = it.next()?.parse().ok()?;
    let se: i64 = it.next().unwrap_or("0").parse().ok()?;
    let mut micros = (h * 3600 + mi * 60 + se) * 1_000_000;
    if let Some(f) = frac {
        let f = format!("{:0<6}", &f[..f.len().min(6)]);
        micros += f.parse::<i64>().ok()?;
    }
    Some(micros)
}

/// Format microseconds-since-midnight as `HH:MM:SS[.ffffff]`.
pub fn format_time(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let (h, mi, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if frac == 0 {
        format!("{h:02}:{mi:02}:{s:02}")
    } else {
        format!("{h:02}:{mi:02}:{s:02}.{frac:06}")
    }
}

/// Format days-since-epoch as `YYYY-MM-DD`.
pub fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Format micros-since-epoch as `YYYY-MM-DD HH:MM:SS[.ffffff]`.
pub fn format_datetime(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    if frac == 0 {
        format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}")
    } else {
        format!("{y:04}-{m:02}-{d:02} {h:02}:{mi:02}:{s:02}.{frac:06}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_roundtrip() {
        for s in ["1970-01-01", "2000-02-29", "2024-12-31", "1999-07-08"] {
            let d = parse_date(s).unwrap();
            assert_eq!(format_date(d), s);
        }
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(parse_date("1970-01-02"), Some(1));
    }

    #[test]
    fn datetime_roundtrip() {
        let s = "2024-06-15 13:45:30";
        let t = parse_datetime(s).unwrap();
        assert_eq!(format_datetime(t), s);
    }
}
