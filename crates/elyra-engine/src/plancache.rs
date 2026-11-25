//! The plan cache for text queries.
//!
//! A client that does not prepare -- PHP's PDO with its default emulated
//! prepares, so Laravel; anything that builds SQL text -- sends each query
//! with its values written in: `select * from users where id = 42 limit 1`.
//! Every one went through the text passes and a full parse, about a third of
//! a primary-key lookup's time in the engine. [`normalize`] takes the values
//! out (`... id = ?1 limit ?2`), and the statement is planned once per shape
//! with [`Engine::prepare_plan`](crate::Engine::prepare_plan), as a prepared
//! statement is; each query puts its own values into the parsed tree.
//!
//! The cache is the server's, not a connection's: PHP-FPM opens a connection
//! per request, which would rarely see a shape twice. A shape the engine
//! does not plan is remembered as such, so it is not parsed twice each time.

use crate::PreparedPlan;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex};

/// A shape longer than this, or with more values, is not cached: a bulk
/// INSERT or a long `IN` list is rarely sent twice with the same count.
const MAX_KEY_BYTES: usize = 2048;
const MAX_LITERALS: usize = 100;

const SHARDS: usize = 16;
const PER_SHARD: usize = 64;

/// Words after which a string is part of the syntax, not a value (`DATE
/// '2020-01-01'`, `AS 'name'`, `COLLATE 'utf8mb4_bin'`, ...): it stays in the
/// shape.
const STRING_KEEPERS: &[&str] = &[
    "date",
    "time",
    "timestamp",
    "datetime",
    "interval",
    "collate",
    "escape",
    "separator",
    "as",
    "using",
    "set",
    "charset",
    "character",
    "binary",
];

fn is_word(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// The end of the quoted run starting at `start` (one past its closing
/// quote), as the SQL tokenizer reads it: a doubled quote is one quote, and
/// in strings a backslash escapes the next character. `None` if it is not
/// closed.
fn quoted_end(b: &[u8], start: usize, quote: u8, backslash: bool) -> Option<usize> {
    let mut i = start + 1;
    while i < b.len() {
        let c = b[i];
        if backslash && c == b'\\' {
            i += 2;
            continue;
        }
        if c == quote {
            if b.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

/// `sql`'s shape -- its text with each value (a plain number, a string)
/// replaced by `?1`, `?2`, ... -- and those values as they are written, in
/// order. `None` when the query is not one to cache: not a SELECT, WITH,
/// INSERT, REPLACE, UPDATE or DELETE; a comment, `@` variable or `?` in it
/// (comments are where tokenizers disagree, and a variable belongs to the
/// session); too long; too many values; or an unclosed quote.
///
/// What is a value is decided conservatively, and anything else stays in the
/// shape: a string right after a word (`x'41'`, `N'a'`, `_utf8mb4'a'`) or one
/// of [`STRING_KEEPERS`]; a number that is not just digits with an optional
/// fraction (`1e3`, `0x1F`, `.5`, `1abc`); double-quoted strings, which are
/// identifiers under ANSI_QUOTES. Taking out too little only makes more
/// shapes. Taking out too much is caught by `prepare_plan`, which plans a
/// shape only when every `?k` is a value in the parsed statement: `CAST(x AS
/// DECIMAL(?1, ?2))` does not parse and stays on the text path.
pub(crate) fn normalize(sql: &str) -> Option<(String, Vec<&str>)> {
    let lead = sql.trim_start();
    let first = lead
        .split(|c: char| !c.is_ascii_alphabetic())
        .next()
        .unwrap_or("");
    if !["select", "with", "insert", "replace", "update", "delete"]
        .iter()
        .any(|w| first.eq_ignore_ascii_case(w))
    {
        return None;
    }
    if sql.len() > MAX_KEY_BYTES * 8 {
        return None;
    }
    let b = sql.as_bytes();
    let mut key = String::with_capacity(sql.len());
    let mut literals: Vec<&str> = Vec::new();
    // The previous token, when it was a word: a string after a keeper stays.
    let mut prev_word: Option<(usize, usize)> = None;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        match c {
            b'\'' => {
                let end = quoted_end(b, i, b'\'', true)?;
                let attached = i > 0 && is_word(b[i - 1]);
                let kept = prev_word.is_some_and(|(s, e)| {
                    STRING_KEEPERS
                        .iter()
                        .any(|k| sql[s..e].eq_ignore_ascii_case(k))
                });
                if attached || kept {
                    key.push_str(&sql[i..end]);
                } else {
                    if literals.len() == MAX_LITERALS {
                        return None;
                    }
                    literals.push(&sql[i..end]);
                    let _ = write!(key, "?{}", literals.len());
                }
                i = end;
                prev_word = None;
            }
            b'"' | b'`' => {
                let end = quoted_end(b, i, c, c == b'"')?;
                key.push_str(&sql[i..end]);
                i = end;
                prev_word = None;
            }
            b'?' | b'@' | b'#' => return None,
            b'-' if b.get(i + 1) == Some(&b'-') => return None,
            b'/' if b.get(i + 1) == Some(&b'*') => return None,
            b'0'..=b'9' => {
                let mut j = i;
                while j < b.len() && b[j].is_ascii_digit() {
                    j += 1;
                }
                if j + 1 < b.len() && b[j] == b'.' && b[j + 1].is_ascii_digit() {
                    j += 1;
                    while j < b.len() && b[j].is_ascii_digit() {
                        j += 1;
                    }
                }
                let after_dot = i > 0 && b[i - 1] == b'.';
                let ends = j == b.len() || !(is_word(b[j]) || b[j] == b'.');
                if !after_dot && ends {
                    if literals.len() == MAX_LITERALS {
                        return None;
                    }
                    literals.push(&sql[i..j]);
                    let _ = write!(key, "?{}", literals.len());
                } else {
                    // The whole run stays, an exponent's sign included
                    // (`1.5e-3`): split there, `-3` would be read as a value.
                    j = i;
                    loop {
                        while j < b.len() && (is_word(b[j]) || b[j] == b'.') {
                            j += 1;
                        }
                        let exponent = matches!(b[j - 1], b'e' | b'E')
                            && matches!(b.get(j), Some(b'+' | b'-'))
                            && b.get(j + 1).is_some_and(u8::is_ascii_digit);
                        if !exponent {
                            break;
                        }
                        j += 1;
                    }
                    key.push_str(&sql[i..j]);
                }
                i = j;
                prev_word = None;
            }
            c if is_word(c) => {
                let start = i;
                while i < b.len() && is_word(b[i]) {
                    i += 1;
                }
                key.push_str(&sql[start..i]);
                prev_word = Some((start, i));
            }
            c if c.is_ascii_whitespace() => {
                key.push(c as char);
                i += 1;
            }
            _ => {
                key.push(c as char);
                i += 1;
                prev_word = None;
            }
        }
    }
    if key.len() > MAX_KEY_BYTES {
        return None;
    }
    Some((key, literals))
}

/// A cached shape: its plan, or `None` for one the engine does not plan.
struct Slot {
    plan: Option<Arc<PreparedPlan>>,
    used: bool,
}

/// The shapes seen, keyed by `(ANSI_QUOTES, shape)` -- the one session
/// setting the text passes read -- and bounded: a full shard drops the
/// shapes not used since it was last full (and all of them if every one
/// was).
pub(crate) struct PlanCache {
    hasher: std::collections::hash_map::RandomState,
    shards: Vec<Mutex<HashMap<(bool, String), Slot>>>,
}

pub(crate) type Key = (bool, String);

impl PlanCache {
    pub(crate) fn new() -> Self {
        Self {
            hasher: Default::default(),
            shards: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }

    fn shard(&self, key: &Key) -> &Mutex<HashMap<Key, Slot>> {
        &self.shards[self.hasher.hash_one(key) as usize % SHARDS]
    }

    /// `None`: not seen. `Some(None)`: seen, and not planned.
    pub(crate) fn lookup(&self, key: &Key) -> Option<Option<Arc<PreparedPlan>>> {
        let mut shard = self.shard(key).lock().unwrap_or_else(|e| e.into_inner());
        let slot = shard.get_mut(key)?;
        slot.used = true;
        Some(slot.plan.clone())
    }

    pub(crate) fn insert(&self, key: Key, plan: Option<Arc<PreparedPlan>>) {
        let mut shard = self.shard(&key).lock().unwrap_or_else(|e| e.into_inner());
        if shard.len() >= PER_SHARD && !shard.contains_key(&key) {
            shard.retain(|_, slot| std::mem::replace(&mut slot.used, false));
            if shard.len() >= PER_SHARD {
                shard.clear();
            }
        }
        shard.insert(key, Slot { plan, used: false });
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(sql: &str) -> Option<(String, Vec<String>)> {
        normalize(sql).map(|(k, l)| (k, l.into_iter().map(str::to_string).collect()))
    }

    #[track_caller]
    fn is(sql: &str, key: &str, literals: &[&str]) {
        assert_eq!(
            shape(sql),
            Some((
                key.to_string(),
                literals.iter().map(|s| s.to_string()).collect()
            ))
        );
    }

    #[test]
    fn values_come_out_of_a_laravel_query() {
        is(
            "select * from `users` where `users`.`id` = 42 limit 1",
            "select * from `users` where `users`.`id` = ?1 limit ?2",
            &["42", "1"],
        );
        is(
            "insert into `posts` (`user_id`, `title`) values (7, 'It''s \\'ok\\'')",
            "insert into `posts` (`user_id`, `title`) values (?1, ?2)",
            &["7", "'It''s \\'ok\\''"],
        );
        is(
            "update `t` set `a` = -1.50, `b` = '' where `id` in (1,2)",
            "update `t` set `a` = -?1, `b` = ?2 where `id` in (?3,?4)",
            &["1.50", "''", "1", "2"],
        );
    }

    #[test]
    fn syntax_stays_in_the_shape() {
        is(
            "SELECT x'41', N'a', _utf8mb4'b', 0x1F, 1e3, .5, t1.c2, 1abc FROM t",
            "SELECT x'41', N'a', _utf8mb4'b', 0x1F, 1e3, .5, t1.c2, 1abc FROM t",
            &[],
        );
        is(
            "SELECT DATE '2020-01-01', name AS 'n', \"dq\" FROM t WHERE c COLLATE 'x' = 'y'",
            "SELECT DATE '2020-01-01', name AS 'n', \"dq\" FROM t WHERE c COLLATE 'x' = ?1",
            &["'y'"],
        );
        is(
            "SELECT `a?b`, '?' FROM `t1`",
            "SELECT `a?b`, ?1 FROM `t1`",
            &["'?'"],
        );
        is(
            "SELECT 1.5e-3, 2E+10, .5e-1, x - 3 FROM t",
            "SELECT 1.5e-3, 2E+10, .5e-1, x - ?1 FROM t",
            &["3"],
        );
        is(
            "SELECT 'æ', 'ø' + 2",
            "SELECT ?1, ?2 + ?3",
            &["'æ'", "'ø'", "2"],
        );
    }

    #[test]
    fn queries_not_to_cache() {
        for sql in [
            "SET @a = 1",
            "BEGIN",
            "SHOW TABLES",
            "CREATE TABLE t (a INT)",
            "SELECT @a",
            "SELECT @@version",
            "SELECT ?",
            "SELECT 1 -- c",
            "SELECT 3--2",
            "SELECT 1 # c",
            "SELECT /* 5 */ 1",
            "SELECT 'unclosed",
            "SELECT `unclosed",
        ] {
            assert_eq!(shape(sql), None, "{sql}");
        }
        let long_in = format!(
            "SELECT * FROM t WHERE id IN ({})",
            (0..=MAX_LITERALS)
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        assert_eq!(shape(&long_in), None);
    }

    #[test]
    fn a_full_shard_keeps_the_shapes_in_use() {
        let cache = PlanCache::new();
        let hot: Key = (false, "SELECT hot".into());
        cache.insert(hot.clone(), None);
        for i in 0..SHARDS * PER_SHARD * 4 {
            assert!(
                cache.lookup(&hot).is_some(),
                "the shape in use was dropped at {i}"
            );
            cache.insert((false, format!("SELECT {i}")), None);
            assert!(cache.len() <= SHARDS * PER_SHARD);
        }
    }
}
