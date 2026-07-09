//! Prepared-statement support.
//!
//! ElyraSQL implements `COM_STMT_PREPARE`/`EXECUTE` by counting `?`
//! placeholders at prepare time and, at execute time, rendering the bound
//! parameters as SQL literals and substituting them into the statement. This
//! reuses the full query engine without a separate parameterised code path,
//! while quoting/escaping values so it stays injection-safe.

use opensrv_mysql::ValueInner;

/// A token produced by [`scan`].
enum Tok {
    Placeholder,
    Char(char),
}

/// Count `?` placeholders that are outside string/identifier quotes.
pub fn count_placeholders(sql: &str) -> usize {
    let mut n = 0;
    scan(sql, |t| {
        if matches!(t, Tok::Placeholder) {
            n += 1;
        }
    });
    n
}

/// Substitute `literals` for the `?` placeholders in `sql`, in order.
pub fn bind(sql: &str, literals: &[String]) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len() + literals.len() * 4);
    let mut idx = 0usize;
    let mut overflow = false;
    scan(sql, |t| match t {
        Tok::Placeholder => {
            if let Some(lit) = literals.get(idx) {
                out.push_str(lit);
            } else {
                overflow = true;
            }
            idx += 1;
        }
        Tok::Char(ch) => out.push(ch),
    });
    if idx != literals.len() || overflow {
        return Err(format!(
            "parameter count mismatch: statement has {idx} placeholders, {} bound",
            literals.len()
        ));
    }
    Ok(out)
}

/// Render a bound parameter value as a SQL literal.
pub fn value_to_literal(v: ValueInner<'_>) -> String {
    match v {
        ValueInner::NULL => "NULL".to_string(),
        ValueInner::Int(i) => i.to_string(),
        ValueInner::UInt(u) => u.to_string(),
        ValueInner::Double(d) => d.to_string(),
        ValueInner::Bytes(b) => quote(b),
        // Temporal parameters arrive as MySQL binary encodings; decode them to
        // string literals the engine can coerce.
        ValueInner::Date(b) | ValueInner::Datetime(b) => datetime_literal(b),
        ValueInner::Time(b) => time_literal(b),
    }
}

/// MySQL binary DATE/DATETIME encoding -> `'YYYY-MM-DD[ HH:MM:SS[.ffffff]]'`.
fn datetime_literal(b: &[u8]) -> String {
    if b.len() < 4 {
        return "'0000-00-00'".to_string();
    }
    let y = u16::from_le_bytes([b[0], b[1]]);
    let (mo, d) = (b[2], b[3]);
    let (h, mi, s) = if b.len() >= 7 {
        (b[4], b[5], b[6])
    } else {
        (0, 0, 0)
    };
    let us = if b.len() >= 11 {
        u32::from_le_bytes([b[7], b[8], b[9], b[10]])
    } else {
        0
    };
    if b.len() < 7 {
        format!("'{y:04}-{mo:02}-{d:02}'")
    } else if us > 0 {
        format!("'{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}'")
    } else {
        format!("'{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}'")
    }
}

/// MySQL binary TIME encoding -> `'[-]HH:MM:SS[.ffffff]'`.
fn time_literal(b: &[u8]) -> String {
    if b.len() < 8 {
        return "'00:00:00'".to_string();
    }
    let neg = b[0] != 0;
    let days = u32::from_le_bytes([b[1], b[2], b[3], b[4]]);
    let hours = b[5] as u32 + days * 24;
    let (mins, secs) = (b[6], b[7]);
    let us = if b.len() >= 12 {
        u32::from_le_bytes([b[8], b[9], b[10], b[11]])
    } else {
        0
    };
    let sign = if neg { "-" } else { "" };
    if us > 0 {
        format!("'{sign}{hours:02}:{mins:02}:{secs:02}.{us:06}'")
    } else {
        format!("'{sign}{hours:02}:{mins:02}:{secs:02}'")
    }
}

/// Quote and escape a byte string as a SQL single-quoted literal.
fn quote(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            _ => out.push(c),
        }
    }
    out.push('\'');
    out
}

/// Walk `sql`, invoking `on_placeholder` for each unquoted `?` and `on_char`
/// for every other character. Tracks single/double quotes and backticks, with
/// backslash escapes and doubled-quote escapes inside strings.
fn scan(sql: &str, mut emit: impl FnMut(Tok)) {
    #[derive(PartialEq)]
    enum Q {
        None,
        Single,
        Double,
        Back,
    }
    let mut state = Q::None;
    let mut escaped = false;
    let mut i = 0;
    let chars: Vec<char> = sql.chars().collect();
    while i < chars.len() {
        let c = chars[i];
        match state {
            Q::None => match c {
                '\'' => {
                    state = Q::Single;
                    emit(Tok::Char(c));
                }
                '"' => {
                    state = Q::Double;
                    emit(Tok::Char(c));
                }
                '`' => {
                    state = Q::Back;
                    emit(Tok::Char(c));
                }
                '?' => emit(Tok::Placeholder),
                _ => emit(Tok::Char(c)),
            },
            Q::Single | Q::Double | Q::Back => {
                emit(Tok::Char(c));
                if escaped {
                    escaped = false;
                } else if c == '\\' && state != Q::Back {
                    escaped = true;
                } else {
                    let closes = matches!(
                        (&state, c),
                        (Q::Single, '\'') | (Q::Double, '"') | (Q::Back, '`')
                    );
                    if closes {
                        state = Q::None;
                    }
                }
            }
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_placeholders_ignoring_quotes() {
        assert_eq!(
            count_placeholders("SELECT * FROM t WHERE a = ? AND b = ?"),
            2
        );
        assert_eq!(count_placeholders("SELECT '?' , a FROM t WHERE a = ?"), 1);
        assert_eq!(
            count_placeholders("SELECT `we?rd`, a FROM t WHERE a = ?"),
            1
        );
    }

    #[test]
    fn binds_in_order_and_escapes() {
        let sql = "INSERT INTO t (a,b) VALUES (?, ?)";
        let out = bind(sql, &["1".into(), "'o''brien'".into()]).unwrap();
        assert_eq!(out, "INSERT INTO t (a,b) VALUES (1, 'o''brien')");
    }

    #[test]
    fn placeholder_inside_string_not_bound() {
        let out = bind("SELECT '100%?' , ? FROM t", &["5".into()]).unwrap();
        assert_eq!(out, "SELECT '100%?' , 5 FROM t");
    }

    #[test]
    fn count_mismatch_errors() {
        assert!(bind("SELECT ?", &[]).is_err());
        assert!(bind("SELECT ?", &["1".into(), "2".into()]).is_err());
    }

    #[test]
    fn decodes_temporal_params() {
        // DATE 2024-03-15 -> [0xE8,0x07, 3, 15]
        assert_eq!(datetime_literal(&[0xE8, 0x07, 3, 15]), "'2024-03-15'");
        // DATETIME 2024-03-15 13:45:30
        assert_eq!(
            datetime_literal(&[0xE8, 0x07, 3, 15, 13, 45, 30]),
            "'2024-03-15 13:45:30'"
        );
        // DATETIME with microseconds
        assert_eq!(
            datetime_literal(&[0xE8, 0x07, 3, 15, 13, 45, 30, 0xE8, 0x03, 0x00, 0x00]),
            "'2024-03-15 13:45:30.001000'"
        );
        // TIME 02:30:00 (no days)
        assert_eq!(time_literal(&[0, 0, 0, 0, 0, 2, 30, 0]), "'02:30:00'");
        // TIME with a day rolls into hours: 1 day + 02:00:00 -> 26:00:00
        assert_eq!(time_literal(&[0, 1, 0, 0, 0, 2, 0, 0]), "'26:00:00'");
        // Empty encodings.
        assert_eq!(datetime_literal(&[]), "'0000-00-00'");
        assert_eq!(time_literal(&[]), "'00:00:00'");
    }
}
