//! MySQL's definer clauses on stored objects:
//!
//! ```sql
//! CREATE [OR REPLACE] [ALGORITHM = {UNDEFINED|MERGE|TEMPTABLE}]
//!     [DEFINER = account] [SQL SECURITY {DEFINER|INVOKER}] VIEW ...
//! CREATE [DEFINER = account] {PROCEDURE|FUNCTION|TRIGGER|EVENT} ...
//! ```
//!
//! `mysqldump` writes them on every view and routine, and the parser accepts
//! none of them, so each such statement was refused. They are taken out of the
//! statement text before it is parsed. A view's definer and security type are
//! recorded beside it (`viewsec::<name>`) and decide whose privileges it runs
//! with and what `CURRENT_USER()` is inside it. For routines and triggers the
//! clause is accepted and not recorded: they run as `root@%`, the definer
//! `information_schema` reports for them.

use serde::{Deserialize, Serialize};

/// What the clauses of one `CREATE` said.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Clauses {
    /// `DEFINER = account`, as `user@host`; `None` when absent or
    /// `CURRENT_USER` (the creator, resolved when the view is stored).
    pub definer: Option<String>,
    /// `SQL SECURITY INVOKER`.
    pub invoker: bool,
}

/// A view's recorded definer and security type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewSecurity {
    /// The account the view runs as, `user@host`.
    pub definer: String,
    /// `SQL SECURITY INVOKER`: it runs with the privileges of whoever queries
    /// it, who must be able to read the tables it reads.
    pub invoker: bool,
}

impl ViewSecurity {
    /// A view created before definers were recorded: it has always run as an
    /// administrator, the `root@%` that `information_schema` reported.
    pub fn legacy() -> Self {
        ViewSecurity {
            definer: crate::sessfn::DEFINER.into(),
            invoker: false,
        }
    }
}

/// `viewsec::<name>`.
pub fn view_security_key(name: &str) -> Vec<u8> {
    format!("viewsec::{name}").into_bytes()
}

pub fn encode(security: &ViewSecurity) -> Vec<u8> {
    bincode::serialize(security).unwrap_or_default()
}

pub fn decode(bytes: &[u8]) -> ViewSecurity {
    bincode::deserialize(bytes).unwrap_or_else(|_| ViewSecurity::legacy())
}

/// Take the definer clauses out of a `CREATE` statement, or `None` when it has
/// none (or is not a `CREATE` of a view, routine, trigger or event).
///
/// Returns the statement without them, and what they said. A clause the object
/// cannot carry (`SQL SECURITY` or `ALGORITHM` before `PROCEDURE`) is left in
/// place, so the parser refuses it as before.
pub fn strip(sql: &str) -> Option<(String, Clauses)> {
    let mut s = Scanner::new(sql);
    s.skip_space();
    if !s.word("create") {
        return None;
    }
    let mut kept = String::from("CREATE");
    s.skip_space();
    let after_create = s.pos;
    if s.word("or") {
        s.skip_space();
        if !s.word("replace") {
            return None;
        }
        kept.push_str(" OR REPLACE");
        s.skip_space();
    } else {
        s.pos = after_create;
    }
    let mut clauses = Clauses::default();
    let mut view_only = false;
    let mut any = false;
    loop {
        s.skip_space();
        let at = s.pos;
        if s.word("algorithm") {
            s.skip_space();
            if !s.symbol('=') {
                return None;
            }
            s.skip_space();
            let alg = s.ident()?.to_ascii_lowercase();
            if !matches!(alg.as_str(), "undefined" | "merge" | "temptable") {
                return None;
            }
            view_only = true;
            any = true;
            continue;
        }
        s.pos = at;
        if s.word("definer") {
            s.skip_space();
            if !s.symbol('=') {
                return None;
            }
            s.skip_space();
            clauses.definer = s.account()?;
            any = true;
            continue;
        }
        s.pos = at;
        if s.word("sql") {
            s.skip_space();
            if !s.word("security") {
                return None;
            }
            s.skip_space();
            let kind = s.ident()?.to_ascii_lowercase();
            clauses.invoker = match kind.as_str() {
                "definer" => false,
                "invoker" => true,
                _ => return None,
            };
            view_only = true;
            any = true;
            continue;
        }
        s.pos = at;
        break;
    }
    if !any {
        return None;
    }
    let object_at = s.pos;
    let object = s.ident()?.to_ascii_lowercase();
    let fits = match object.as_str() {
        "view" => true,
        "procedure" | "function" | "trigger" | "event" => !view_only,
        _ => false,
    };
    if !fits {
        return None;
    }
    Some((format!("{kept} {}", &sql[object_at..]), clauses))
}

struct Scanner<'a> {
    sql: &'a str,
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(sql: &'a str) -> Self {
        Scanner { sql, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.sql[self.pos..]
    }

    fn skip_space(&mut self) {
        let trimmed = self.rest().trim_start();
        self.pos = self.sql.len() - trimmed.len();
    }

    /// The keyword `kw`, case-insensitively, as a whole word.
    fn word(&mut self, kw: &str) -> bool {
        let rest = self.rest();
        let Some(head) = rest.get(..kw.len()) else {
            return false;
        };
        let boundary = rest[kw.len()..]
            .chars()
            .next()
            .is_none_or(|c| !(c.is_alphanumeric() || c == '_' || c == '$'));
        if head.eq_ignore_ascii_case(kw) && boundary {
            self.pos += kw.len();
            true
        } else {
            false
        }
    }

    fn symbol(&mut self, c: char) -> bool {
        if self.rest().starts_with(c) {
            self.pos += c.len_utf8();
            true
        } else {
            false
        }
    }

    /// An unquoted identifier.
    fn ident(&mut self) -> Option<&'a str> {
        let rest = self.rest();
        let len = rest
            .char_indices()
            .find(|&(_, c)| !(c.is_alphanumeric() || c == '_' || c == '$'))
            .map_or(rest.len(), |(i, _)| i);
        if len == 0 {
            return None;
        }
        self.pos += len;
        Some(&rest[..len])
    }

    /// A name, unquoted or in `` ` ``, `'` or `"` quotes (a doubled quote is
    /// one quote character).
    fn name(&mut self) -> Option<String> {
        let rest = self.rest();
        let Some(q) = rest
            .chars()
            .next()
            .filter(|c| matches!(c, '`' | '\'' | '"'))
        else {
            return self.ident().map(str::to_string);
        };
        let mut out = String::new();
        let mut chars = rest[1..].char_indices();
        while let Some((i, c)) = chars.next() {
            if c == q {
                if rest[1 + i + 1..].starts_with(q) {
                    out.push(q);
                    chars.next();
                    continue;
                }
                self.pos += 1 + i + 1;
                return Some(out);
            }
            out.push(c);
        }
        None
    }

    /// `user[@host]` in any quoting, or `CURRENT_USER[()]`, which is `None`:
    /// the creator, known only when the statement runs.
    fn account(&mut self) -> Option<Option<String>> {
        let at = self.pos;
        if self.word("current_user") {
            let after = self.pos;
            self.skip_space();
            if !(self.symbol('(') && {
                self.skip_space();
                self.symbol(')')
            }) {
                self.pos = after;
            }
            return Some(None);
        }
        self.pos = at;
        let user = self.name()?;
        let host = if self.symbol('@') {
            self.name()?
        } else {
            "%".into()
        };
        Some(Some(format!("{user}@{host}")))
    }
}

#[cfg(test)]
mod tests {
    use super::{strip, Clauses};

    fn clauses(definer: Option<&str>, invoker: bool) -> Clauses {
        Clauses {
            definer: definer.map(str::to_string),
            invoker,
        }
    }

    /// The forms `mysqldump` writes, and the ones people type.
    #[test]
    fn view_clauses_are_taken_out() {
        for (sql, want, rest) in [
            (
                "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT 1",
                clauses(Some("root@%"), false),
                "CREATE VIEW `v` AS SELECT 1",
            ),
            (
                "create or replace sql security invoker view v as select 1",
                clauses(None, true),
                "CREATE OR REPLACE view v as select 1",
            ),
            (
                "CREATE DEFINER = 'app'@'10.0.0.%' VIEW v AS SELECT 1",
                clauses(Some("app@10.0.0.%"), false),
                "CREATE VIEW v AS SELECT 1",
            ),
            (
                "CREATE DEFINER=CURRENT_USER() SQL SECURITY INVOKER VIEW v AS SELECT 1",
                clauses(None, true),
                "CREATE VIEW v AS SELECT 1",
            ),
            (
                "CREATE DEFINER=app VIEW v AS SELECT 1",
                clauses(Some("app@%"), false),
                "CREATE VIEW v AS SELECT 1",
            ),
        ] {
            let (out, got) = strip(sql).unwrap_or_else(|| panic!("{sql}"));
            assert_eq!(got, want, "{sql}");
            assert_eq!(out, rest, "{sql}");
        }
    }

    #[test]
    fn a_routine_takes_only_a_definer() {
        let (out, got) =
            strip("CREATE DEFINER=`root`@`%` PROCEDURE p() BEGIN SELECT 1; END").unwrap();
        assert_eq!(got, clauses(Some("root@%"), false));
        assert_eq!(out, "CREATE PROCEDURE p() BEGIN SELECT 1; END");
        assert!(strip("CREATE SQL SECURITY INVOKER PROCEDURE p() BEGIN END").is_none());
        assert!(strip(
            "CREATE ALGORITHM=MERGE TRIGGER t BEFORE INSERT ON x FOR EACH ROW SET NEW.a = 1"
        )
        .is_none());
    }

    #[test]
    fn statements_without_clauses_are_left_alone() {
        for sql in [
            "CREATE VIEW v AS SELECT 1",
            "CREATE TABLE definer (id INT)",
            "SELECT 'DEFINER=x'",
            "CREATE ALGORITHM=FAST VIEW v AS SELECT 1",
        ] {
            assert!(strip(sql).is_none(), "{sql}");
        }
    }
}
