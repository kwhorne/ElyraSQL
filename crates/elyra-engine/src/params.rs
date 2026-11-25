//! `?` parameter markers in a statement's text, shared by the binary
//! protocol's prepared statements and SQL-level `PREPARE`.

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

/// `sql` with its `?` placeholders numbered `?1`, `?2`, ... in order, for the
/// engine's prepared plan: each parameter goes to its own position in the
/// parsed statement, whatever a text pass did to the order of the text.
pub fn number_placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut n = 0usize;
    scan(sql, |t| match t {
        Tok::Placeholder => {
            n += 1;
            out.push('?');
            out.push_str(&n.to_string());
        }
        Tok::Char(ch) => out.push(ch),
    });
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
