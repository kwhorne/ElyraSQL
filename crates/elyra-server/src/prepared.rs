//! Prepared-statement support.
//!
//! ElyraSQL implements `COM_STMT_PREPARE`/`EXECUTE` by counting `?`
//! placeholders at prepare time and, at execute time, rendering the bound
//! parameters as SQL literals and substituting them into the statement. This
//! reuses the full query engine without a separate parameterised code path,
//! while quoting/escaping values so it stays injection-safe.

use elyra_wire::ValueInner;

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

/// Render a byte-string parameter as a SQL literal.
///
/// Text becomes a single-quoted literal. Anything that is not valid UTF-8 becomes
/// a hex literal (`x'..'`), which the engine decodes back to the exact bytes.
///
/// The binary protocol sends strings and BLOBs the same way -- as bytes -- so a
/// driver binding an image, a hash or ciphertext arrives here too. Rendering
/// those through a `String` (as `from_utf8_lossy` did) replaced every non-UTF-8
/// byte with U+FFFD, silently: seven bytes went in, fifteen came back, and no
/// error was raised anywhere. Hex is what the engine itself uses to render
/// `Value::Bytes` as SQL, so the two paths agree.
///
/// Valid UTF-8 keeps the quoted form on purpose: `x'..'` is a *binary* string
/// in MySQL, and turning every text parameter into one would change how
/// `WHERE name = ?` compares under a case-insensitive collation.
fn quote(bytes: &[u8]) -> String {
    let Ok(s) = std::str::from_utf8(bytes) else {
        return hex_literal(bytes);
    };
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

/// `x'0A1B..'` -- the SQL spelling of exact bytes.
fn hex_literal(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2 + 3);
    out.push_str("x'");
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out.push('\'');
    out
}

/// Walk `sql`, invoking `on_placeholder` for each executable `?` and `on_char`
/// for every other character. Strings, quoted identifiers, and MySQL comments
/// are copied verbatim and never interpreted as parameter markers.
fn scan(sql: &str, mut emit: impl FnMut(Tok)) {
    #[derive(PartialEq)]
    enum Q {
        None,
        Single,
        Double,
        Back,
        LineComment,
        BlockComment,
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
                '#' => {
                    state = Q::LineComment;
                    emit(Tok::Char(c));
                }
                '-' if chars.get(i + 1) == Some(&'-')
                    && chars.get(i + 2).is_none_or(|next| next.is_whitespace()) =>
                {
                    state = Q::LineComment;
                    emit(Tok::Char(c));
                }
                '/' if chars.get(i + 1) == Some(&'*') => {
                    state = Q::BlockComment;
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
            Q::LineComment => {
                emit(Tok::Char(c));
                if c == '\n' || c == '\r' {
                    state = Q::None;
                }
            }
            Q::BlockComment => {
                emit(Tok::Char(c));
                if c == '/' && i > 0 && chars[i - 1] == '*' {
                    state = Q::None;
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
    /// Bytes that are not UTF-8 must survive exactly. Through a `String` they
    /// did not: each invalid byte became U+FFFD.
    fn non_utf8_bytes_become_a_hex_literal() {
        assert_eq!(quote(&[0x00, 0x80, 0xff, 0xfe, 0x27]), "x'0080fffe27'");
        // Valid UTF-8 keeps the text form, with its escaping.
        assert_eq!(quote(b"it's"), "'it''s'");
        assert_eq!(quote(b"a\\b"), "'a\\\\b'");
        assert_eq!(quote("æøå".as_bytes()), "'æøå'");
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
    fn placeholders_inside_comments_are_not_bound() {
        for sql in [
            "SELECT 1 -- ?\n, ?",
            "SELECT 1 # ?\n, ?",
            "SELECT /* ? */ ?",
        ] {
            assert_eq!(count_placeholders(sql), 1, "{sql}");
            let bound = bind(sql, &["7".into()]).expect("bind executable placeholder");
            assert!(bound.ends_with("7"), "{bound}");
        }
    }

    #[test]
    fn comment_placeholder_cannot_activate_bound_sql() {
        let sql = "SELECT 1 -- ?";
        assert_eq!(count_placeholders(sql), 0);
        assert!(bind(sql, &["'\\nDROP TABLE secrets'".into()]).is_err());
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
