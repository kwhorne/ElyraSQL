//! SQL-level prepared statements -- `PREPARE name FROM 'text' | @var`,
//! `EXECUTE name [USING @a, ...]`, `{DEALLOCATE | DROP} PREPARE name` -- and
//! the budget they share with the binary protocol's prepared statements.
//!
//! A statement prepared this way is the one `COM_STMT_PREPARE` makes: its `?`
//! markers counted, planned once when the engine can (`Engine::prepare_plan`),
//! and run with its parameters -- here the values of user variables -- in its
//! tree, or substituted into its text when it has no plan.

use crate::{params, Engine, Error, PreparedPlan, Privilege, QueryResult, Result, Session};
use elyra_core::Value;
use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Server-wide limit on live prepared statements, both kinds together, as
/// MySQL's `max_prepared_stmt_count` (same default, 16382). Global rather
/// than per connection for the same reason MySQL's is: a per-connection cap
/// does not bound memory when connections are themselves unbounded.
/// Configurable via `ELYRASQL_MAX_PREPARED_STMTS`.
pub fn max_prepared_stmts() -> usize {
    use std::sync::OnceLock;
    static CACHE: OnceLock<usize> = OnceLock::new();
    *CACHE.get_or_init(|| {
        std::env::var("ELYRASQL_MAX_PREPARED_STMTS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(16_382)
    })
}

/// Number of prepared statements currently live across all connections.
static PREPARED_LIVE: AtomicUsize = AtomicUsize::new(0);

/// One prepared statement's slot in the budget, released on drop.
pub struct StmtPermit;

impl StmtPermit {
    /// Reserve one slot, or `None` when the server-wide limit is reached.
    pub fn acquire() -> Option<Self> {
        let limit = max_prepared_stmts();
        let mut cur = PREPARED_LIVE.load(Ordering::Relaxed);
        loop {
            if cur >= limit {
                return None;
            }
            match PREPARED_LIVE.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(StmtPermit),
                Err(observed) => cur = observed,
            }
        }
    }

    /// Live count across all connections.
    pub fn live() -> usize {
        PREPARED_LIVE.load(Ordering::Relaxed)
    }
}

impl Drop for StmtPermit {
    fn drop(&mut self) {
        PREPARED_LIVE.fetch_sub(1, Ordering::AcqRel);
    }
}

/// `ELYRASQL_PLAN_CACHE=off` runs every query and prepared statement through
/// the text path, as before plans existed: a way back should a plan ever
/// answer differently. On by default.
pub fn plans_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("ELYRASQL_PLAN_CACHE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "off" | "0" | "false" | "no"
        )
    })
}

/// A statement made by `PREPARE`, held by its session under its name.
pub(crate) struct SqlPrepared {
    sql: String,
    params: usize,
    plan: Option<PreparedPlan>,
    _permit: StmtPermit,
}

enum Command {
    Prepare { name: String, source: Source },
    Execute { name: String, using: Vec<String> },
    Deallocate { name: String },
}

enum Source {
    Text(String),
    Var(String),
}

/// Whether `head` (a statement's first characters, lowercased) is one of
/// these commands.
pub(crate) fn is_command(head: &str) -> bool {
    let word = |w: &str| {
        head.strip_prefix(w).is_some_and(|rest| {
            rest.is_empty()
                || !rest.as_bytes()[0].is_ascii_alphanumeric() && rest.as_bytes()[0] != b'_'
        })
    };
    word("prepare")
        || word("execute")
        || word("deallocate")
        || head.strip_prefix("drop").is_some_and(|rest| {
            rest.trim_start().starts_with("prepare") && word_after(rest.trim_start(), "prepare")
        })
}

fn word_after(s: &str, w: &str) -> bool {
    s.strip_prefix(w).is_some_and(|rest| {
        rest.is_empty() || !rest.as_bytes()[0].is_ascii_alphanumeric() && rest.as_bytes()[0] != b'_'
    })
}

fn syntax(sql: &str) -> Error {
    Error::Parse(format!(
        "You have an error in your SQL syntax; check the manual for the right syntax near '{}'",
        sql.trim()
    ))
}

/// The command in `sql`, read with the SQL tokenizer so quotes, escapes and
/// comments are read as everywhere else.
fn parse(sql: &str) -> Result<Command> {
    // `\_` and `\%` keep their backslash inside the statement's text, as in
    // the text path (see `keep_like_escapes`).
    let escaped = crate::sqllex::keep_like_escapes(sql);
    let tokens = Tokenizer::new(&MySqlDialect {}, escaped.as_deref().unwrap_or(sql))
        .tokenize()
        .map_err(|e| Error::Parse(e.to_string()))?;
    let mut t: Vec<Token> = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    while matches!(t.last(), Some(Token::SemiColon)) {
        t.pop();
    }
    let kw = |i: usize, k: &str| matches!(t.get(i), Some(Token::Word(w)) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(k));
    let name = |i: usize| match t.get(i) {
        Some(Token::Word(w)) if !w.value.starts_with('@') => Some(w.value.to_ascii_lowercase()),
        _ => None,
    };
    // `@name`: one word to the MySQL tokenizer, or `@` and a word.
    let var = |i: usize| -> Option<(String, usize)> {
        match (t.get(i), t.get(i + 1)) {
            (Some(Token::Word(w)), _)
                if w.value.len() > 1 && w.value.starts_with('@') && !w.value.starts_with("@@") =>
            {
                Some((w.value[1..].to_ascii_lowercase(), i + 1))
            }
            (Some(Token::AtSign), Some(Token::Word(w))) => {
                Some((w.value.to_ascii_lowercase(), i + 2))
            }
            _ => None,
        }
    };
    if kw(0, "prepare") {
        let (Some(name), true) = (name(1), kw(2, "from")) else {
            return Err(syntax(sql));
        };
        let source = match t.get(3) {
            Some(Token::SingleQuotedString(s) | Token::DoubleQuotedString(s)) if t.len() == 4 => {
                Source::Text(s.clone())
            }
            _ => match var(3) {
                Some((v, end)) if end == t.len() => Source::Var(v),
                _ => return Err(syntax(sql)),
            },
        };
        return Ok(Command::Prepare { name, source });
    }
    if kw(0, "execute") {
        let Some(name) = name(1) else {
            return Err(syntax(sql));
        };
        let mut using = Vec::new();
        if t.len() > 2 {
            if !kw(2, "using") {
                return Err(syntax(sql));
            }
            let mut i = 3;
            loop {
                let Some((v, end)) = var(i) else {
                    return Err(syntax(sql));
                };
                using.push(v);
                i = end;
                match t.get(i) {
                    None => break,
                    Some(Token::Comma) => i += 1,
                    Some(_) => return Err(syntax(sql)),
                }
            }
        }
        return Ok(Command::Execute { name, using });
    }
    if (kw(0, "deallocate") || kw(0, "drop")) && kw(1, "prepare") && t.len() == 3 {
        if let Some(name) = name(2) {
            return Ok(Command::Deallocate { name });
        }
    }
    Err(syntax(sql))
}

/// The words a statement can start with. PREPARE refuses any other at once,
/// as MySQL's parser does (`SELEC 1`); a statement starting with one of these
/// that is not a SELECT, INSERT, REPLACE, UPDATE or DELETE is read when it
/// runs.
const STATEMENT_STARTS: &[&str] = &[
    "select",
    "with",
    "insert",
    "replace",
    "update",
    "delete",
    "values",
    "table",
    "set",
    "show",
    "describe",
    "desc",
    "explain",
    "create",
    "drop",
    "alter",
    "rename",
    "truncate",
    "call",
    "do",
    "begin",
    "start",
    "commit",
    "rollback",
    "savepoint",
    "release",
    "lock",
    "unlock",
    "grant",
    "revoke",
    "flush",
    "analyze",
    "optimize",
    "check",
    "checksum",
    "repair",
    "use",
    "load",
    "backup",
    "refresh",
    "purge",
    "kill",
    "help",
    "xa",
    "install",
    "uninstall",
    "reset",
    "cache",
    "change",
    "handler",
];

/// A user variable's value as the text of a statement to prepare.
fn statement_text(value: Value) -> Result<String> {
    match value {
        Value::Null => Err(syntax("NULL")),
        Value::Text(s) => Ok(s),
        other => other.to_wire_string().ok_or_else(|| syntax("NULL")),
    }
}

impl Engine {
    /// Run a PREPARE, EXECUTE or DEALLOCATE PREPARE (see [`is_command`]).
    pub(crate) async fn run_sql_prepare(
        &self,
        sql: &str,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<Vec<QueryResult>> {
        match parse(sql)? {
            Command::Prepare { name, source } => {
                let text = match source {
                    Source::Text(text) => text,
                    Source::Var(var) => statement_text(sess.user_var(&var))?,
                };
                // As in MySQL, a statement of the same name goes first, even
                // when the new one then fails to prepare.
                sess.drop_sql_prepared(&name);
                let stmt = self.prepare_sql_text(text, sess)?;
                sess.set_sql_prepared(name, std::sync::Arc::new(stmt));
                Ok(vec![QueryResult::Affected(0)])
            }
            Command::Execute { name, using } => {
                let stmt = sess
                    .sql_prepared(&name)
                    .ok_or(Error::UnknownStmtHandler(name, "EXECUTE"))?;
                if using.len() != stmt.params {
                    return Err(Error::WrongArguments("EXECUTE"));
                }
                let literals: Vec<String> = using
                    .iter()
                    .map(|v| crate::exec::value_sql_literal(&sess.user_var(v)))
                    .collect();
                if let Some(plan) = &stmt.plan {
                    if let Some(res) = self
                        .execute_plan(plan, &literals, privilege, user, sess)
                        .await
                    {
                        return res;
                    }
                }
                let bound = params::bind(&stmt.sql, &literals).map_err(Error::Query)?;
                Box::pin(self.execute_as(&bound, privilege, user, sess)).await
            }
            Command::Deallocate { name } => {
                if sess.drop_sql_prepared(&name) {
                    Ok(vec![QueryResult::Affected(0)])
                } else {
                    Err(Error::UnknownStmtHandler(name, "DEALLOCATE PREPARE"))
                }
            }
        }
    }

    /// Prepare `text` as `COM_STMT_PREPARE` does, for `PREPARE`.
    fn prepare_sql_text(&self, text: String, sess: &Session) -> Result<SqlPrepared> {
        let body = text.trim().trim_end_matches(';').trim();
        if body.is_empty() {
            return Err(Error::EmptyQuery);
        }
        let head: String = body
            .chars()
            .take(24)
            .collect::<String>()
            .to_ascii_lowercase();
        if is_command(&head) {
            return Err(Error::UnsupportedPs);
        }
        let count = params::count_placeholders(&text);
        // MySQL parses at PREPARE; so does this for the statements the parser
        // reads, with the markers as values (a number, else a string, for the
        // places that take only one). Anything else is read when it runs.
        let first = if head.starts_with('(') {
            "select"
        } else {
            head.split(|c: char| !c.is_ascii_alphabetic())
                .next()
                .unwrap_or("")
        };
        if !STATEMENT_STARTS.contains(&first) {
            return Err(syntax(body));
        }
        if matches!(
            first,
            "select" | "with" | "insert" | "replace" | "update" | "delete"
        ) {
            let check = |value: &str| {
                let sql =
                    params::bind(&text, &vec![value.to_string(); count]).map_err(Error::Query)?;
                self.parse_frontend(&sql, sess).map(|f| f.statements.len())
            };
            match check("0") {
                Ok(1) => {}
                Ok(_) => return Err(syntax(body)),
                Err(Error::Parse(error)) => match check("'0'") {
                    Ok(1) => {}
                    Ok(_) => return Err(syntax(body)),
                    Err(Error::Parse(_)) => return Err(Error::Parse(error)),
                    Err(_) => {}
                },
                Err(_) => {}
            }
        }
        let permit =
            StmtPermit::acquire().ok_or_else(|| Error::MaxPreparedStmts(max_prepared_stmts()))?;
        let plan = self.prepare_plan(&params::number_placeholders(&text), count, sess);
        Ok(SqlPrepared {
            sql: text,
            params: count,
            plan,
            _permit: permit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::StmtPermit;
    use crate::{Engine, Privilege, QueryResult, Session};
    use elyra_core::Value;

    /// The budget is the process's: these tests take turns, so that one can
    /// count the slots the others hold.
    static TURN: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn engine() -> (Engine, Session) {
        let e = Engine::new(elyra_storage::Db::in_memory().unwrap());
        let s = e.session();
        for sql in [
            "CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(32))",
            "INSERT INTO t (name) VALUES ('one'), ('two'), ('three')",
        ] {
            e.execute(sql, Privilege::Admin, &s).await.unwrap();
        }
        (e, s)
    }

    /// The rows of the first result set (a CALL's comes before its OK), or
    /// the error's MySQL code.
    async fn run(e: &Engine, s: &Session, sql: &str) -> Result<Vec<Vec<Value>>, u16> {
        let results = e
            .execute(sql, Privilege::Admin, s)
            .await
            .map_err(|e| e.mysql_code())?;
        match results
            .into_iter()
            .find(|r| matches!(r, QueryResult::Rows(_)))
        {
            Some(QueryResult::Rows(mut rows)) => {
                let mut all = Vec::new();
                loop {
                    let batch = rows.next_batch(64).await.unwrap();
                    if batch.is_empty() {
                        break;
                    }
                    all.extend(batch);
                }
                Ok(all)
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn one(e: &Engine, s: &Session, sql: &str) -> Value {
        run(e, s, sql)
            .await
            .unwrap_or_else(|c| panic!("{sql}: {c}"))[0][0]
            .clone()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prepare_execute_and_deallocate() {
        let _turn = TURN.lock().await;
        let (e, s) = engine().await;
        run(&e, &s, "PREPARE s FROM 'SELECT ?-?'").await.unwrap();
        run(&e, &s, "SET @a = 3, @b = -2").await.unwrap();
        assert_eq!(one(&e, &s, "EXECUTE s USING @a, @b").await, Value::Int(5));
        assert_eq!(one(&e, &s, "EXECUTE S USING @b, @a").await, Value::Int(-5));

        // From a variable; executed again with a new value.
        run(
            &e,
            &s,
            "SET @sql = 'SELECT name FROM t WHERE id = ?', @id = 2",
        )
        .await
        .unwrap();
        run(&e, &s, "PREPARE q FROM @sql").await.unwrap();
        assert_eq!(
            one(&e, &s, "EXECUTE q USING @id").await,
            Value::Text("two".into())
        );
        run(&e, &s, "SET @id = '3'").await.unwrap();
        assert_eq!(
            one(&e, &s, "EXECUTE q USING @id").await,
            Value::Text("three".into())
        );
        // An unset variable is NULL.
        assert!(run(&e, &s, "EXECUTE q USING @never_set")
            .await
            .unwrap()
            .is_empty());

        // Writes, with a string that needs quoting.
        run(&e, &s, "PREPARE ins FROM 'INSERT INTO t (name) VALUES (?)'")
            .await
            .unwrap();
        run(&e, &s, "SET @n = 'O''Br\\\\ien'").await.unwrap();
        run(&e, &s, "EXECUTE ins USING @n").await.unwrap();
        assert_eq!(one(&e, &s, "SELECT LAST_INSERT_ID()").await, Value::Int(4));
        assert_eq!(
            one(&e, &s, "SELECT name FROM t WHERE id = 4").await,
            Value::Text("O'Br\\ien".into())
        );

        run(&e, &s, "DEALLOCATE PREPARE s").await.unwrap();
        assert_eq!(run(&e, &s, "EXECUTE s USING @a, @b").await, Err(1243));
        run(&e, &s, "DROP PREPARE q;").await.unwrap();
        assert_eq!(run(&e, &s, "DEALLOCATE PREPARE q").await, Err(1243));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn errors_are_mysqls() {
        let _turn = TURN.lock().await;
        let (e, s) = engine().await;
        assert_eq!(run(&e, &s, "EXECUTE nothing").await, Err(1243));
        assert_eq!(run(&e, &s, "DEALLOCATE PREPARE nothing").await, Err(1243));
        run(&e, &s, "PREPARE s FROM 'SELECT ?'").await.unwrap();
        assert_eq!(run(&e, &s, "EXECUTE s").await, Err(1210));
        run(&e, &s, "SET @a = 1").await.unwrap();
        assert_eq!(run(&e, &s, "EXECUTE s USING @a, @a").await, Err(1210));
        assert_eq!(
            run(&e, &s, "PREPARE p FROM 'PREPARE x FROM ''SELECT 1'''").await,
            Err(1295)
        );
        assert_eq!(run(&e, &s, "PREPARE p FROM 'EXECUTE s'").await, Err(1295));
        assert_eq!(run(&e, &s, "PREPARE p FROM ''").await, Err(1065));
        assert_eq!(
            run(&e, &s, "PREPARE p FROM 'SELECT 1; SELECT 2'").await,
            Err(1064)
        );
        assert_eq!(run(&e, &s, "PREPARE p FROM @not_set").await, Err(1064));
        assert_eq!(run(&e, &s, "PREPARE p 'SELECT 1'").await, Err(1064));
        assert_eq!(run(&e, &s, "EXECUTE s USING 1").await, Err(1064));
        // A statement that fails to prepare takes the old one of its name with it.
        run(&e, &s, "PREPARE s FROM 'SELECT 1'").await.unwrap();
        assert_eq!(run(&e, &s, "PREPARE s FROM 'SELEC 1'").await, Err(1064));
        assert_eq!(
            run(&e, &s, "PREPARE s FROM 'SELECT * FROM'").await,
            Err(1064)
        );
        assert_eq!(run(&e, &s, "EXECUTE s").await, Err(1243));
        run(&e, &s, "PREPARE u FROM '(SELECT ?) UNION (SELECT 2)'")
            .await
            .unwrap();
        assert_eq!(run(&e, &s, "EXECUTE u USING @a").await.unwrap().len(), 2);
        // Markers where only a number or only a string goes still prepare.
        run(
            &e,
            &s,
            "PREPARE l FROM 'SELECT id FROM t ORDER BY id LIMIT ?'",
        )
        .await
        .unwrap();
        run(&e, &s, "PREPARE x FROM 'SELECT ''a_b'' LIKE ? ESCAPE ?'")
            .await
            .unwrap();
        run(&e, &s, "SET @two = 2, @p = 'a|_b', @esc = '|'")
            .await
            .unwrap();
        assert_eq!(run(&e, &s, "EXECUTE l USING @two").await.unwrap().len(), 2);
        assert_eq!(
            one(&e, &s, "EXECUTE x USING @p, @esc").await,
            Value::Bool(true)
        );
    }

    /// Statements the plan does not take run through the text path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn any_statement_prepares() {
        let _turn = TURN.lock().await;
        let (e, s) = engine().await;
        run(&e, &s, "PREPARE st FROM 'SHOW TABLES'").await.unwrap();
        assert_eq!(run(&e, &s, "EXECUTE st").await.unwrap().len(), 1);
        run(&e, &s, "PREPARE sv FROM 'SET @x = ? * 2'")
            .await
            .unwrap();
        run(&e, &s, "SET @a = 21").await.unwrap();
        run(&e, &s, "EXECUTE sv USING @a").await.unwrap();
        assert_eq!(one(&e, &s, "SELECT @x").await, Value::Int(42));
        run(&e, &s, "PREPARE ddl FROM 'CREATE TABLE u (a INT)'")
            .await
            .unwrap();
        run(&e, &s, "EXECUTE ddl").await.unwrap();
        assert_eq!(one(&e, &s, "SELECT COUNT(*) FROM u").await, Value::Int(0));
        // `\_` in the statement's text keeps its backslash, as in a query.
        run(&e, &s, "PREPARE lk FROM 'SELECT ? LIKE ''a\\_b'''")
            .await
            .unwrap();
        run(&e, &s, "SET @u = 'a_b', @x = 'axb'").await.unwrap();
        assert_eq!(one(&e, &s, "EXECUTE lk USING @u").await, Value::Bool(true));
        assert_eq!(one(&e, &s, "EXECUTE lk USING @x").await, Value::Bool(false));
    }

    /// PREPARE and DEALLOCATE PREPARE do not commit, as in MySQL, and DROP
    /// PREPARE is no write in a read-only transaction.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prepare_does_not_commit() {
        let _turn = TURN.lock().await;
        let (e, s) = engine().await;
        run(&e, &s, "START TRANSACTION").await.unwrap();
        run(&e, &s, "INSERT INTO t (name) VALUES ('four')")
            .await
            .unwrap();
        run(&e, &s, "PREPARE c FROM 'SELECT COUNT(*) FROM t'")
            .await
            .unwrap();
        assert_eq!(one(&e, &s, "EXECUTE c").await, Value::Int(4));
        run(&e, &s, "DROP PREPARE c").await.unwrap();
        run(&e, &s, "ROLLBACK").await.unwrap();
        assert_eq!(one(&e, &s, "SELECT COUNT(*) FROM t").await, Value::Int(3));
        run(&e, &s, "START TRANSACTION READ ONLY").await.unwrap();
        run(&e, &s, "PREPARE c FROM 'SELECT 1'").await.unwrap();
        run(&e, &s, "DROP PREPARE c").await.unwrap();
        run(&e, &s, "PREPARE w FROM 'INSERT INTO t (name) VALUES (?)'")
            .await
            .unwrap();
        run(&e, &s, "SET @n = 'no'").await.unwrap();
        assert_eq!(run(&e, &s, "EXECUTE w USING @n").await, Err(1792));
        run(&e, &s, "COMMIT").await.unwrap();
    }

    /// Each statement holds a slot in the budget the binary protocol's share,
    /// released when it is deallocated, replaced or its session ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn statements_hold_a_slot_in_the_budget() {
        let _turn = TURN.lock().await;
        let before = StmtPermit::live();
        {
            let (e, s) = engine().await;
            run(&e, &s, "PREPARE a FROM 'SELECT 1'").await.unwrap();
            run(&e, &s, "PREPARE b FROM 'SELECT 2'").await.unwrap();
            run(&e, &s, "PREPARE b FROM 'SELECT 3'").await.unwrap();
            assert_eq!(StmtPermit::live(), before + 2);
            run(&e, &s, "DEALLOCATE PREPARE a").await.unwrap();
            assert_eq!(StmtPermit::live(), before + 1);
        }
        assert_eq!(StmtPermit::live(), before);
    }

    /// A negative user variable after a minus: `3-@b` was `3--2`, a comment.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_negative_variable_after_a_minus() {
        let _turn = TURN.lock().await;
        let (e, s) = engine().await;
        run(&e, &s, "SET @b = -2, @f = -0.5").await.unwrap();
        assert_eq!(one(&e, &s, "SELECT 3-@b").await, Value::Int(5));
        assert_eq!(one(&e, &s, "SELECT -@b").await, Value::Int(2));
        assert_eq!(
            one(&e, &s, "SELECT 1-@f").await.to_wire_string().as_deref(),
            Some("1.5")
        );
        // A procedure's local variable and a trigger's NEW value, substituted
        // the same way.
        run(
            &e,
            &s,
            "CREATE PROCEDURE p() BEGIN DECLARE b INT DEFAULT -2; SELECT 3-b; END",
        )
        .await
        .unwrap();
        assert_eq!(one(&e, &s, "CALL p()").await, Value::Int(5));
        run(&e, &s, "CREATE TABLE w (a INT, b INT)").await.unwrap();
        run(
            &e,
            &s,
            "CREATE TRIGGER wb BEFORE INSERT ON w FOR EACH ROW SET NEW.b = 3-NEW.a",
        )
        .await
        .unwrap();
        run(&e, &s, "INSERT INTO w (a) VALUES (-2)").await.unwrap();
        assert_eq!(one(&e, &s, "SELECT b FROM w").await, Value::Int(5));
    }
}
