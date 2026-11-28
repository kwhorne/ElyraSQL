//! MySQL table options: `ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4
//! COMMENT='...'`, after a `CREATE TABLE`'s column list or as `ALTER TABLE`
//! specifications.
//!
//! sqlparser 0.53 rejects them in `ALTER TABLE` -- Laravel writes `alter table
//! t comment = '...'` for `$table->comment()` and `auto_increment = 100` for
//! `->from(100)`, phpMyAdmin dumps `ALTER TABLE t MODIFY ... AUTO_INCREMENT,
//! AUTO_INCREMENT=5` -- and `CREATE TABLE` drops them before parsing (see
//! `strip_create_table_options`). Three of them carry meaning here and are
//! kept: `AUTO_INCREMENT`, the next value the counter hands out; `COMMENT`;
//! and the table's collation (`COLLATE`, or a `CHARSET`'s default one), which
//! its text columns take unless they name their own. The rest name a storage
//! engine, a row format or an InnoDB tuning knob, and change nothing: there is
//! one engine. `ALTER TABLE ... CONVERT TO CHARACTER SET` is read here too.
//!
//! A `CREATE TABLE`'s collation is not applied afterwards like the others:
//! `strip_create_table_options` keeps it in a spelling sqlparser reads, so the
//! columns take it as they are made.
//!
//! An option this does not know makes the whole text "not table options", so
//! the statement goes on to the parser and fails there rather than losing it.

use crate::indexddl::{tokens, Tok};
use crate::sqllex::split_top_level;
use elyra_core::Collation;

/// The table options that change anything here.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct TableOptions {
    /// `AUTO_INCREMENT = n`: the next value the counter hands out.
    pub auto_increment: Option<u64>,
    /// `COMMENT = '...'`, unquoted; an empty comment removes it, as in MySQL.
    pub comment: Option<String>,
    /// The table's collation: `COLLATE = c`, or else the default collation of
    /// `[DEFAULT] CHARSET = cs`. Its text columns that name neither take it.
    pub collation: Option<Collation>,
    /// `CONVERT TO CHARACTER SET cs [COLLATE c]`: every text column, and the
    /// table, to this collation.
    pub convert: Option<Collation>,
}

impl TableOptions {
    /// Whether a CREATE TABLE has options to apply once the table exists (its
    /// collation is applied as the columns are made; see `create_table`).
    pub fn any(&self) -> bool {
        self.auto_increment.is_some() || self.comment.is_some()
    }

    /// Fold in the options of another specification of the same statement.
    fn merge(&mut self, other: TableOptions) {
        self.auto_increment = other.auto_increment.or(self.auto_increment);
        self.comment = other.comment.or(self.comment.take());
        self.collation = other.collation.or(self.collation);
        self.convert = other.convert.or(self.convert);
    }
}

/// The collation `name` -- a collation, or a character set standing for its
/// default one -- comes to: `binary`, `_bin` and `_cs` compare exactly, the
/// rest as `utf8mb4_0900_ai_ci` (see `Collation::from_name`).
fn collation_named(name: &str) -> Collation {
    Collation::from_name(name)
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
    // A value's text: a word without backticks, a string without its quotes.
    let text_at = |at: usize| -> Option<String> {
        match toks.get(at)? {
            Tok::Word(w) => Some(w.text.trim_matches('`').to_string()),
            Tok::Other(start, end) => unquote(&text[*start..*end]),
            Tok::Eq => None,
        }
    };
    let mut charset: Option<String> = None;
    let mut collate: Option<String> = None;
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
                Some("set") => {
                    let (at, next) = value(i + 2)?;
                    charset = Some(text_at(at)?);
                    next
                }
                _ => return None,
            },
            "charset" => {
                let (at, next) = value(i + 1)?;
                charset = Some(text_at(at)?);
                next
            }
            "collate" => {
                let (at, next) = value(i + 1)?;
                collate = Some(text_at(at)?);
                next
            }
            // `CONVERT TO {CHARACTER SET | CHARSET} cs [COLLATE c]`.
            "convert" => {
                let at = match (word(i + 1).as_deref(), word(i + 2).as_deref()) {
                    (Some("to"), Some("charset")) => i + 3,
                    (Some("to"), Some("character")) if word(i + 3).as_deref() == Some("set") => {
                        i + 4
                    }
                    _ => return None,
                };
                let (cs, mut next) = value(at)?;
                let mut target = collation_named(&text_at(cs)?);
                if word(next).as_deref() == Some("collate") {
                    let (c, after) = value(next + 1)?;
                    target = collation_named(&text_at(c)?);
                    next = after;
                }
                out.convert = Some(target);
                next
            }
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
            "engine" | "row_format" | "insert_method"
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
    out.collation = collate.or(charset).map(|name| collation_named(&name));
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
                options.merge(found);
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
        assert_eq!(alter.options.collation, Some(Collation::Ci));
        assert_eq!(alter.rest, None);

        // The table's collation: COLLATE over a CHARSET's default one.
        let alter =
            alter_table("ALTER TABLE t DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin, ADD c INT")
                .unwrap();
        assert_eq!(alter.options.collation, Some(Collation::Bin));
        assert_eq!(alter.rest.as_deref(), Some("ALTER TABLE t ADD c INT"));
        let alter = alter_table("ALTER TABLE t CHARACTER SET binary").unwrap();
        assert_eq!(alter.options.collation, Some(Collation::Bin));

        // CONVERT TO, with and without its own COLLATE.
        let alter =
            alter_table("ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4 COLLATE 'utf8mb4_bin'")
                .unwrap();
        assert_eq!(alter.options.convert, Some(Collation::Bin));
        assert_eq!(alter.options.collation, None);
        let alter = alter_table("ALTER TABLE t CONVERT TO CHARSET utf8mb4").unwrap();
        assert_eq!(alter.options.convert, Some(Collation::Ci));
        assert_eq!(parse("CONVERT utf8mb4"), None);

        // Text that is not ASCII is no option, and no panic either.
        assert_eq!(parse("COMMENT = 'ø' \u{202e}"), None);
        assert_eq!(parse("\u{80}"), None);

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
