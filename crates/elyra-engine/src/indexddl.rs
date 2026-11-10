//! MySQL index definitions in the forms sqlparser 0.53 does not parse.
//!
//! mysqldump, Navicat, Laravel and MySQL's own `SHOW CREATE TABLE` write index
//! definitions sqlparser rejects with 1064:
//!
//! - an index type after the key parts, or before them on a primary key:
//!   `KEY k (a) USING BTREE`, `PRIMARY KEY USING BTREE (id)`;
//! - a direction on a key part: `KEY k (a DESC, b ASC)`;
//! - index options: `COMMENT '...'`, `VISIBLE`, `INVISIBLE`,
//!   `KEY_BLOCK_SIZE = 8`, `WITH PARSER p`, `ENGINE_ATTRIBUTE = '...'`;
//! - `ALGORITHM = ...` / `LOCK = ...` on `CREATE INDEX` and `ALTER TABLE`.
//!
//! None of them changes what ElyraSQL stores or answers: its indexes are
//! B-trees whichever type is named, read in either direction, and these
//! options are hints. They are removed before parsing, from `CREATE TABLE`,
//! `CREATE INDEX` and `ALTER TABLE ... ADD` index definitions only. An option
//! this does not know is left where it is, so the statement still fails to
//! parse rather than silently losing something.

use crate::sqllex::split_top_level;

/// `sql` with its index definitions normalised, or `None` if nothing changed.
pub(crate) fn normalize(sql: &str) -> Option<String> {
    let words = leading_words(sql, 4);
    let word = |i: usize| words.get(i).map(String::as_str);
    match (word(0), word(1), word(2)) {
        (Some("create"), Some("table"), _) | (Some("create"), Some("temporary"), Some("table")) => {
            create_table(sql)
        }
        (Some("create"), Some("index"), _)
        | (Some("create"), Some("unique" | "fulltext" | "spatial"), Some("index")) => {
            create_index(sql)
        }
        (Some("alter"), Some("table"), _) => alter_table(sql),
        _ => None,
    }
}

/// The first `n` words of `sql`, lowercased.
fn leading_words(sql: &str, n: usize) -> Vec<String> {
    sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .take(n)
        .map(str::to_ascii_lowercase)
        .collect()
}

fn create_table(sql: &str) -> Option<String> {
    let open = first_code_byte(sql, 0, b'(')?;
    let close = crate::sqllex::matching_paren_end(sql, open)? - 1;
    let mut changed = false;
    let items = split_top_level(&sql[open + 1..close], ',')
        .into_iter()
        .map(|item| match index_item(&item) {
            Some(normal) => {
                changed = true;
                normal
            }
            None => item,
        })
        .collect::<Vec<_>>();
    changed.then(|| format!("{}{}{}", &sql[..=open], items.join(","), &sql[close..]))
}

/// `CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX name [USING t] ON tbl (parts)
/// [USING t] [options] [ALGORITHM ...] [LOCK ...]`.
fn create_index(sql: &str) -> Option<String> {
    let on = keyword_position(sql, "on")?;
    let open = first_code_byte(sql, on, b'(')?;
    let close = crate::sqllex::matching_paren_end(sql, open)? - 1;
    let head = strip_index_type(&sql[..open]);
    let parts = key_parts(&sql[open + 1..close]);
    let (tail, tail_changed) = strip_options(&sql[close + 1..]);
    let changed = head != sql[..open] || parts != sql[open + 1..close] || tail_changed;
    changed.then(|| format!("{head}({parts}){tail}"))
}

/// `ALTER TABLE t spec, spec, ...`: each `ADD` index definition normalised,
/// and `ALGORITHM` / `LOCK` specifications dropped.
fn alter_table(sql: &str) -> Option<String> {
    // The table name ends the prefix; the specifications follow it.
    let table = keyword_position(sql, "table")? + "table".len();
    let rest = &sql[table..];
    let name_start = table + (rest.len() - rest.trim_start().len());
    let name_end = name_end(sql, name_start)?;
    let mut changed = false;
    let mut specs = Vec::new();
    for spec in split_top_level(&sql[name_end..], ',') {
        let words = leading_words(&spec, 1);
        match words.first().map(String::as_str) {
            Some("algorithm" | "lock") if is_assignment_option(&spec) => {
                changed = true;
            }
            Some("add") => {
                let trimmed = spec.trim_start();
                let body = &trimmed[3..];
                match index_item(body) {
                    Some(normal) => {
                        changed = true;
                        specs.push(format!(" ADD {}", normal.trim_start()));
                    }
                    None => specs.push(spec),
                }
            }
            _ => specs.push(spec),
        }
    }
    if !changed {
        return None;
    }
    if specs.iter().all(|s| s.trim().is_empty()) {
        return None;
    }
    let terminator = if sql.trim_end().ends_with(';') && !specs.join(",").trim_end().ends_with(';')
    {
        ";"
    } else {
        ""
    };
    Some(format!(
        "{}{}{terminator}",
        &sql[..name_end],
        specs.join(",")
    ))
}

/// `ALGORITHM [=] word` or `LOCK [=] word`, and nothing else.
fn is_assignment_option(spec: &str) -> bool {
    let tokens = tokens(spec.trim_end().trim_end_matches(';'));
    matches!(
        tokens.as_slice(),
        [Tok::Word(_), Tok::Word(_)] | [Tok::Word(_), Tok::Eq, Tok::Word(_)]
    )
}

/// Where the identifier starting at `start` ends (backtick-quoted or bare,
/// possibly `db.table`).
fn name_end(sql: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut i = start;
    loop {
        match bytes.get(i)? {
            b'`' => {
                i += 1;
                while bytes.get(i)? != &b'`' {
                    i += 1;
                }
                i += 1;
            }
            _ => {
                while bytes
                    .get(i)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$')
                {
                    i += 1;
                }
            }
        }
        if bytes.get(i) == Some(&b'.') {
            i += 1;
        } else {
            return Some(i);
        }
    }
}

/// One table-level index definition, normalised; `None` if `item` is not an
/// index definition or needed no change.
fn index_item(item: &str) -> Option<String> {
    let words = leading_words(item, 4);
    let mut at = 0;
    if words.first().map(String::as_str) == Some("constraint") {
        // `CONSTRAINT [name] PRIMARY KEY | UNIQUE ...`.
        at = if matches!(words.get(1).map(String::as_str), Some("primary" | "unique")) {
            1
        } else {
            2
        };
    }
    let is_index = match words.get(at).map(String::as_str) {
        Some("primary") => words.get(at + 1).map(String::as_str) == Some("key"),
        Some("unique" | "key" | "index" | "fulltext" | "spatial") => true,
        _ => false,
    };
    if !is_index {
        return None;
    }
    let open = first_code_byte(item, 0, b'(')?;
    let close = crate::sqllex::matching_paren_end(item, open)? - 1;
    let head = strip_index_type(&item[..open]);
    let parts = key_parts(&item[open + 1..close]);
    let (tail, tail_changed) = strip_options(&item[close + 1..]);
    let changed = head != item[..open] || parts != item[open + 1..close] || tail_changed;
    changed.then(|| format!("{head}({parts}){tail}"))
}

/// `head` without a `USING <type>` index type.
fn strip_index_type(head: &str) -> String {
    let tokens = tokens(head);
    let mut out = head.to_string();
    for window in tokens.windows(2).rev() {
        if let [Tok::Word(w), Tok::Word(_)] = window {
            if w.text.eq_ignore_ascii_case("using") {
                let end = window[1].end();
                out.replace_range(w.start..end, " ");
            }
        }
    }
    out
}

/// Key parts with any `ASC` / `DESC` direction and `(length)` prefix length
/// removed: ElyraSQL indexes whole values, read in either direction.
fn key_parts(parts: &str) -> String {
    split_top_level(parts, ',')
        .into_iter()
        .map(|part| {
            let trimmed = part.trim_end();
            let words = tokens(trimmed);
            let part = match words.last() {
                Some(Tok::Word(w))
                    if words.len() > 1
                        && (w.text.eq_ignore_ascii_case("asc")
                            || w.text.eq_ignore_ascii_case("desc")) =>
                {
                    trimmed[..w.start].trim_end().to_string()
                }
                _ => part,
            };
            // `column(length)`.
            let pieces = tokens(&part);
            let column_end = match pieces.as_slice() {
                [Tok::Word(column), Tok::Other(open, _), Tok::Word(length), Tok::Other(close, _)]
                    if part.as_bytes()[*open] == b'('
                        && part.as_bytes()[*close] == b')'
                        && length.text.bytes().all(|b| b.is_ascii_digit()) =>
                {
                    Some(column.start + column.text.len())
                }
                _ => None,
            };
            match column_end {
                Some(end) => part[..end].to_string(),
                None => part,
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// The text after the key parts with the index options it knows removed, and
/// whether anything was. Unknown text keeps the whole tail as it was.
fn strip_options(tail: &str) -> (String, bool) {
    // A trailing `;` or the rest of an outer list (inside CREATE TABLE the
    // tail is only this item's) stays.
    let body = tail.trim_end();
    let (body, end) = match body.strip_suffix(';') {
        Some(b) => (b, ";"),
        None => (body, ""),
    };
    let tokens = tokens(body);
    if tokens.is_empty() {
        return (tail.to_string(), false);
    }
    let mut i = 0;
    while i < tokens.len() {
        let Tok::Word(w) = &tokens[i] else {
            return (tail.to_string(), false);
        };
        let key = w.text.to_ascii_lowercase();
        let value = |i: usize| -> Option<usize> {
            // `[=] value`
            let j = if matches!(tokens.get(i + 1), Some(Tok::Eq)) {
                i + 2
            } else {
                i + 1
            };
            tokens.get(j).map(|_| j + 1)
        };
        i = match key.as_str() {
            "visible" | "invisible" => i + 1,
            "using"
            | "comment"
            | "key_block_size"
            | "engine_attribute"
            | "secondary_engine_attribute"
            | "algorithm"
            | "lock" => match value(i) {
                Some(next) => next,
                None => return (tail.to_string(), false),
            },
            "with" => match (tokens.get(i + 1), tokens.get(i + 2)) {
                (Some(Tok::Word(p)), Some(Tok::Word(_)))
                    if p.text.eq_ignore_ascii_case("parser") =>
                {
                    i + 3
                }
                _ => return (tail.to_string(), false),
            },
            _ => return (tail.to_string(), false),
        };
    }
    (end.to_string(), true)
}

/// Byte offset of the first code (unquoted, uncommented) `byte` at or after
/// `from`.
fn first_code_byte(sql: &str, from: usize, byte: u8) -> Option<usize> {
    crate::sqllex::code_bytes(sql)
        .find(|c| c.index >= from && c.byte == byte)
        .map(|c| c.index)
}

/// Byte offset of the first code occurrence of keyword `kw`.
fn keyword_position(sql: &str, kw: &str) -> Option<usize> {
    crate::sqllex::code_bytes(sql)
        .find(|c| crate::sqllex::keyword_at(sql, c.index, kw))
        .map(|c| c.index)
}

struct Word {
    start: usize,
    text: String,
}

enum Tok {
    Word(Word),
    Eq,
    Other(usize, usize),
}

impl Tok {
    fn end(&self) -> usize {
        match self {
            Tok::Word(w) => w.start + w.text.len(),
            Tok::Eq => 0,
            Tok::Other(_, end) => *end,
        }
    }
}

/// Words (bare or backtick-quoted, and numbers), `=`, and anything else
/// (a quoted string, punctuation) as an opaque token.
fn tokens(text: &str) -> Vec<Tok> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
        } else if b == b'=' {
            out.push(Tok::Eq);
            i += 1;
        } else if b.is_ascii_alphanumeric() || b == b'_' || b == b'$' {
            let start = i;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$')
            {
                i += 1;
            }
            out.push(Tok::Word(Word {
                start,
                text: text[start..i].to_string(),
            }));
        } else if matches!(b, b'\'' | b'"' | b'`') {
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != b {
                if bytes[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            i = (i + 1).min(bytes.len());
            if b == b'`' {
                out.push(Tok::Word(Word {
                    start,
                    text: text[start..i].to_string(),
                }));
            } else {
                out.push(Tok::Other(start, i));
            }
        } else {
            out.push(Tok::Other(i, i + 1));
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::normalize;

    #[test]
    fn index_types_directions_and_options_are_removed() {
        for (sql, want) in [
            (
                "CREATE TABLE t (`id` int NOT NULL, `a` int, PRIMARY KEY (`id`) USING BTREE, KEY `k` (`a`) USING BTREE) ENGINE=InnoDB",
                "CREATE TABLE t (`id` int NOT NULL, `a` int, PRIMARY KEY (`id`), KEY `k` (`a`)) ENGINE=InnoDB",
            ),
            (
                "CREATE TABLE t (id int, PRIMARY KEY USING BTREE (id))",
                "CREATE TABLE t (id int, PRIMARY KEY   (id))",
            ),
            (
                "CREATE TABLE t (a int, b int, KEY k (a DESC, b ASC) COMMENT 'x' INVISIBLE)",
                "CREATE TABLE t (a int, b int, KEY k (a, b))",
            ),
            (
                "CREATE TABLE t (a int, CONSTRAINT u UNIQUE (a) USING HASH KEY_BLOCK_SIZE = 8)",
                "CREATE TABLE t (a int, CONSTRAINT u UNIQUE (a))",
            ),
            (
                "CREATE UNIQUE INDEX i ON t (c ASC) USING BTREE ALGORITHM=INPLACE LOCK=NONE;",
                "CREATE UNIQUE INDEX i ON t (c);",
            ),
            ("CREATE INDEX i USING BTREE ON t (a)", "CREATE INDEX i   ON t (a)"),
            ("CREATE INDEX i ON t (`b`(10) DESC, c)", "CREATE INDEX i ON t (`b`, c)"),
            (
                "ALTER TABLE t ADD INDEX i (a DESC) USING BTREE, ALGORITHM=INPLACE, LOCK=NONE",
                "ALTER TABLE t ADD INDEX i (a)",
            ),
            (
                "ALTER TABLE `db`.`t` ADD UNIQUE KEY u (b) USING HASH, ADD COLUMN c INT",
                "ALTER TABLE `db`.`t` ADD UNIQUE KEY u (b), ADD COLUMN c INT",
            ),
        ] {
            assert_eq!(normalize(sql).as_deref(), Some(want), "{sql}");
        }
    }

    #[test]
    fn everything_else_is_left_alone() {
        for sql in [
            "CREATE TABLE t (id int PRIMARY KEY, a int COMMENT 'USING BTREE desc')",
            "CREATE TABLE t (a int, KEY k (a))",
            "CREATE TABLE t (a int, CONSTRAINT f FOREIGN KEY (a) REFERENCES p (id) ON DELETE CASCADE)",
            "SELECT a FROM t ORDER BY a DESC",
            "ALTER TABLE t ADD COLUMN c INT",
            // An option it does not know stays, so the statement still fails.
            "CREATE TABLE t (a int, KEY k (a) FROBNICATE)",
        ] {
            assert_eq!(normalize(sql), None, "{sql}");
        }
    }
}
