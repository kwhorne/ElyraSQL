//! MySQL table options: `ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4
//! COMMENT='...'`, after a `CREATE TABLE`'s column list or as `ALTER TABLE`
//! specifications.
//!
//! sqlparser 0.53 rejects them in `ALTER TABLE` -- Laravel writes `alter table
//! t comment = '...'` for `$table->comment()` and `auto_increment = 100` for
//! `->from(100)`, phpMyAdmin dumps `ALTER TABLE t MODIFY ... AUTO_INCREMENT,
//! AUTO_INCREMENT=5` -- and `CREATE TABLE` drops them before parsing (see
//! `strip_create_table_options`). Two of them carry meaning here and are kept:
//! `AUTO_INCREMENT`, the next value the counter hands out, and `COMMENT`. The
//! rest name a storage engine, a character set, a row format or an InnoDB
//! tuning knob: there is one engine, text is utf8mb4, and the table's own
//! collation does not reach its columns here, so they change nothing.
//!
//! An option this does not know makes the whole text "not table options", so
//! the statement goes on to the parser and fails there rather than losing it.

use crate::indexddl::{tokens, Tok};
use crate::sqllex::split_top_level;

/// The table options that change anything here.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct TableOptions {
    /// `AUTO_INCREMENT = n`: the next value the counter hands out.
    pub auto_increment: Option<u64>,
    /// `COMMENT = '...'`, unquoted; an empty comment removes it, as in MySQL.
    pub comment: Option<String>,
}

impl TableOptions {
    /// Whether any option needs applying.
    pub fn any(&self) -> bool {
        self.auto_increment.is_some() || self.comment.is_some()
    }
}

/// `text` read as a run of table options -- separated by spaces or commas --
/// or `None` if any part of it is not one.
pub(crate) fn parse(text: &str) -> Option<TableOptions> {
    let text = text.trim().trim_end_matches(';');
    let toks = tokens(text);
    let mut out = TableOptions::default();
    let mut i = 0;
    let word = |i: usize| match toks.get(i) {
        Some(Tok::Word(w)) => Some(w.text.to_ascii_lowercase()),
        _ => None,
    };
    // The token after an optional `=`, and the index after it.
    let value = |i: usize| -> Option<(usize, usize)> {
        let at = if matches!(toks.get(i), Some(Tok::Eq)) {
            i + 1
        } else {
            i
        };
        match toks.get(at) {
            Some(Tok::Word(_)) => Some((at, at + 1)),
            Some(Tok::Other(start, end)) if is_string(&text[*start..*end]) => Some((at, at + 1)),
            _ => None,
        }
    };
    while i < toks.len() {
        if let Tok::Other(start, end) = &toks[i] {
            if &text[*start..*end] == "," {
                i += 1;
                continue;
            }
            return None;
        }
        let key = word(i)?;
        i = match key.as_str() {
            // `DEFAULT CHARSET=...`, `DEFAULT COLLATE=...`.
            "default" => match word(i + 1).as_deref() {
                Some("charset" | "character" | "collate") => i + 1,
                _ => return None,
            },
            "character" => match word(i + 1).as_deref() {
                Some("set") => value(i + 2)?.1,
                _ => return None,
            },
            "auto_increment" => {
                let (at, next) = value(i + 1)?;
                out.auto_increment = Some(word(at)?.parse().ok()?);
                next
            }
            "comment" => {
                let (at, next) = value(i + 1)?;
                let Tok::Other(start, end) = &toks[at] else {
                    return None;
                };
                out.comment = Some(unquote(&text[*start..*end])?);
                next
            }
            "charset" | "collate" | "engine" | "row_format" | "insert_method"
            | "stats_auto_recalc" | "stats_persistent" | "stats_sample_pages" | "pack_keys"
            | "checksum" | "table_checksum" | "delay_key_write" | "key_block_size"
            | "avg_row_length" | "max_rows" | "min_rows" | "compression" | "encryption"
            | "connection" | "engine_attribute" | "secondary_engine_attribute" | "password"
            | "autoextend_size" | "tablespace"
            // `ALTER TABLE` specifications that are hints or no-ops here:
            // the copy/in-place choice and the lock level.
            | "algorithm" | "lock" => value(i + 1)?.1,
            // `DATA DIRECTORY = '...'`, `INDEX DIRECTORY = '...'`.
            "data" | "index" => match word(i + 1).as_deref() {
                Some("directory") => value(i + 2)?.1,
                _ => return None,
            },
            // `ALTER TABLE t FORCE` rebuilds the table in MySQL.
            "force" => i + 1,
            _ => return None,
        };
    }
    Some(out)
}

/// An `ALTER TABLE` whose specifications include table options.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AlterOptions {
    /// The table as the statement names it (quoting and database kept).
    pub table: String,
    pub options: TableOptions,
    /// The statement with only its other specifications, if it has any.
    pub rest: Option<String>,
}

/// An `ALTER TABLE` statement's table options, split from its other
/// specifications; `None` if it has none.
pub(crate) fn alter_table(sql: &str) -> Option<AlterOptions> {
    let toks = tokens(sql);
    let is = |i: usize, kw: &str| matches!(toks.get(i), Some(Tok::Word(w)) if w.text.eq_ignore_ascii_case(kw));
    if !(is(0, "alter") && is(1, "table")) {
        return None;
    }
    // The table name: a word, possibly `db.table`.
    let Some(Tok::Word(first)) = toks.get(2) else {
        return None;
    };
    let mut name_end = first.start + first.text.len();
    if sql[name_end..].starts_with('.') {
        let Some(Tok::Word(second)) = toks.get(4) else {
            return None;
        };
        name_end = second.start + second.text.len();
    }
    let table = sql[first.start..name_end].to_string();
    let mut options = TableOptions::default();
    let mut any_option = false;
    let mut rest = Vec::new();
    for spec in split_top_level(&sql[name_end..], ',') {
        match parse(&spec) {
            Some(found) if !spec.trim().trim_end_matches(';').is_empty() => {
                // `ALGORITHM` / `LOCK` alone leave the statement to the usual
                // path, which drops them (see `indexddl`).
                let hint = matches!(
                    tokens(&spec).first(),
                    Some(Tok::Word(w)) if w.text.eq_ignore_ascii_case("algorithm")
                        || w.text.eq_ignore_ascii_case("lock")
                );
                any_option |= !hint;
                if found.auto_increment.is_some() {
                    options.auto_increment = found.auto_increment;
                }
                if found.comment.is_some() {
                    options.comment = found.comment;
                }
            }
            _ => rest.push(spec),
        }
    }
    if !any_option {
        return None;
    }
    let rest = (!rest.is_empty()).then(|| format!("{}{}", &sql[..name_end], rest.join(",")));
    Some(AlterOptions {
        table,
        options,
        rest,
    })
}

/// A `CREATE TABLE`'s table name as written and the table options after its
/// column list, from the statement and the same statement without them.
pub(crate) fn create_table(sql: &str, stripped: &str) -> Option<(String, bool, TableOptions)> {
    let options = parse(sql.get(stripped.len()..)?)?;
    if !options.any() {
        return None;
    }
    let toks = tokens(stripped);
    let is = |i: usize, kw: &str| matches!(toks.get(i), Some(Tok::Word(w)) if w.text.eq_ignore_ascii_case(kw));
    let mut i = 1;
    if is(i, "temporary") {
        i += 1;
    }
    if !(is(0, "create") && is(i, "table")) {
        return None;
    }
    i += 1;
    let if_not_exists = is(i, "if") && is(i + 1, "not") && is(i + 2, "exists");
    if if_not_exists {
        i += 3;
    }
    let Some(Tok::Word(first)) = toks.get(i) else {
        return None;
    };
    let mut name_end = first.start + first.text.len();
    if stripped[name_end..].starts_with('.') {
        let Some(Tok::Word(second)) = toks.get(i + 2) else {
            return None;
        };
        name_end = second.start + second.text.len();
    }
    Some((
        stripped[first.start..name_end].to_string(),
        if_not_exists,
        options,
    ))
}

fn is_string(token: &str) -> bool {
    token.len() >= 2 && (token.starts_with('\'') || token.starts_with('"'))
}

/// A MySQL string literal's value: `''` and backslash escapes resolved.
fn unquote(token: &str) -> Option<String> {
    let quote = token.chars().next()?;
    let body = token.strip_prefix(quote)?.strip_suffix(quote)?;
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next()? {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                '0' => out.push('\0'),
                'Z' => out.push('\u{1a}'),
                other => out.push(other),
            },
            c if c == quote && chars.peek() == Some(&quote) => {
                chars.next();
                out.push(quote);
            }
            c => out.push(c),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_options_are_read_and_others_refused() {
        let opts = parse(
            " ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='it''s \\'x\\''",
        )
        .unwrap();
        assert_eq!(opts.auto_increment, Some(10));
        assert_eq!(opts.comment.as_deref(), Some("it's 'x'"));
        let opts = parse("CHARACTER SET = utf8mb4, ROW_FORMAT DYNAMIC, COMMENT 'c'").unwrap();
        assert_eq!(opts.comment.as_deref(), Some("c"));
        assert_eq!(opts.auto_increment, None);
        assert_eq!(parse("FORCE"), Some(TableOptions::default()));
        assert_eq!(parse("ADD COLUMN x INT"), None);
        assert_eq!(parse("AUTO_INCREMENT = abc"), None);
        assert_eq!(parse("COMMENT = 5"), None);
        assert_eq!(parse("DEFAULT 1"), None);
    }

    #[test]
    fn alter_table_options_are_split_from_the_other_specifications() {
        let alter = alter_table("ALTER TABLE `t` COMMENT = 'hi'").unwrap();
        assert_eq!(alter.table, "`t`");
        assert_eq!(alter.options.comment.as_deref(), Some("hi"));
        assert_eq!(alter.rest, None);

        let alter = alter_table(
            "ALTER TABLE db.t MODIFY id INT NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=5;",
        )
        .unwrap();
        assert_eq!(alter.table, "db.t");
        assert_eq!(alter.options.auto_increment, Some(5));
        assert_eq!(
            alter.rest.as_deref(),
            Some("ALTER TABLE db.t MODIFY id INT NOT NULL AUTO_INCREMENT")
        );

        let alter = alter_table("alter table t ENGINE=InnoDB DEFAULT CHARSET=utf8mb4").unwrap();
        assert_eq!(alter.options, TableOptions::default());
        assert_eq!(alter.rest, None);

        assert_eq!(alter_table("ALTER TABLE t ADD COLUMN c INT"), None);
        assert_eq!(
            alter_table("ALTER TABLE t ALGORITHM=INPLACE, ADD INDEX i (a)"),
            None
        );
        let alter =
            alter_table("ALTER TABLE t ENGINE=InnoDB, ALGORITHM=COPY, ADD INDEX i (a)").unwrap();
        assert_eq!(alter.rest.as_deref(), Some("ALTER TABLE t ADD INDEX i (a)"));
    }

    #[test]
    fn create_table_options_name_the_table() {
        let sql = "CREATE TABLE IF NOT EXISTS `db`.`t` (id INT) ENGINE=InnoDB AUTO_INCREMENT=7 COMMENT='c'";
        let stripped = "CREATE TABLE IF NOT EXISTS `db`.`t` (id INT)";
        let (name, if_not_exists, opts) = create_table(sql, stripped).unwrap();
        assert_eq!(name, "`db`.`t`");
        assert!(if_not_exists);
        assert_eq!(opts.auto_increment, Some(7));
        assert_eq!(opts.comment.as_deref(), Some("c"));
        // Nothing to apply: the usual path.
        assert_eq!(
            create_table(
                "CREATE TABLE t (id INT) ENGINE=InnoDB",
                "CREATE TABLE t (id INT)"
            ),
            None
        );
    }
}
