//! ElyraSQL query engine.
//!
//! Frontend is `sqlparser` with the MySQL dialect. Execution is **async and
//! streaming** end to end, layered on the high-concurrency [`Db`] handle:
//! reads scale across connections, writes group-commit, and result sets are
//! never fully materialised. This is what lets ElyraSQL handle large data
//! under high traffic.

mod aggregate;
mod aggspill;
mod catalog;
mod eval;
mod exec;
mod ft;
mod index;
mod keyenc;
pub mod lockmgr;
mod predicate;
mod proc;
mod session;
mod sort;
mod stream;
mod users;
mod vindex;

pub use session::{Isolation, Session};

use elyra_core::{ColumnType, Error, Privilege, Result, Schema, Value};
use elyra_storage::Db;
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

pub use stream::RowStream;

/// Outcome of a single SQL statement.
#[allow(clippy::large_enum_variant)]
pub enum QueryResult {
    /// A (streaming) result set.
    Rows(RowStream),
    /// A statement that changed state; carries affected row count.
    Affected(u64),
}

impl QueryResult {
    pub fn empty_ok() -> Self {
        QueryResult::Affected(0)
    }

    /// One column, one row — used for `SELECT 1`, `@@version`, etc.
    pub fn scalar(col: &str, ty: ColumnType, value: Value) -> Self {
        let schema = Schema::new(vec![elyra_core::ColumnDef {
            name: col.into(),
            ty,
            nullable: true,
            collation: elyra_core::Collation::Ci,
        }]);
        QueryResult::Rows(RowStream::literal(schema, vec![vec![value]]))
    }
}

/// The ElyraSQL engine. Cheap to clone (shared, concurrent DB handle).
#[derive(Clone)]
pub struct Engine {
    db: Db,
    vindex: vindex::VectorRegistry,
    locks: std::sync::Arc<lockmgr::LockManager>,
}

impl Engine {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            vindex: vindex::VectorRegistry::new(),
            locks: std::sync::Arc::new(lockmgr::LockManager::new()),
        }
    }

    /// Parse and execute one or more `;`-separated statements, enforcing that
    /// each statement is permitted at the caller's `privilege` level.
    /// Create a per-connection session over the shared database.
    pub fn session(&self) -> Session {
        Session::new(self.db.clone(), self.locks.clone())
    }

    /// The underlying database handle (used for replication).
    pub fn db(&self) -> Db {
        self.db.clone()
    }

    /// Interpret a parsed procedure body against a variable environment,
    /// returning any control-flow signal that escaped it.
    async fn run_proc(
        &self,
        stmts: &[proc::ProcStmt],
        env: &mut std::collections::HashMap<String, Value>,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<proc::Flow> {
        use proc::{Flow, ProcStmt};
        const MAX_LOOP: u64 = 10_000_000;
        let cond = |c: &str, env: &std::collections::HashMap<String, Value>| -> Result<bool> {
            Ok(truthy(&exec::eval_scalar(&exec::substitute_vars(c, env))?))
        };
        // How a loop body's escape signal is handled by a loop with `label`.
        enum Act {
            Continue,
            Break,
            Bubble(Flow),
        }
        let act = |f: Flow, label: &Option<String>| -> Act {
            match f {
                Flow::Normal => Act::Continue,
                Flow::Iterate(ref l) if label.as_deref() == Some(l.as_str()) => Act::Continue,
                Flow::Leave(ref l) if label.as_deref() == Some(l.as_str()) => Act::Break,
                other => Act::Bubble(other),
            }
        };
        for stmt in stmts {
            match stmt {
                ProcStmt::Declare { name, default } => {
                    let v = match default {
                        Some(e) => exec::eval_scalar(&exec::substitute_vars(e, env))?,
                        None => Value::Null,
                    };
                    env.insert(name.to_ascii_lowercase(), v);
                }
                ProcStmt::Set { name, expr } => {
                    let v = exec::eval_scalar(&exec::substitute_vars(expr, env))?;
                    env.insert(name.to_ascii_lowercase(), v);
                }
                ProcStmt::Leave(l) => return Ok(Flow::Leave(l.clone())),
                ProcStmt::Iterate(l) => return Ok(Flow::Iterate(l.clone())),
                ProcStmt::If { branches, els } => {
                    let mut ran = false;
                    for (c, body) in branches {
                        if cond(c, env)? {
                            let f =
                                Box::pin(self.run_proc(body, env, privilege, user, sess)).await?;
                            if f != Flow::Normal {
                                return Ok(f);
                            }
                            ran = true;
                            break;
                        }
                    }
                    if !ran {
                        if let Some(body) = els {
                            let f =
                                Box::pin(self.run_proc(body, env, privilege, user, sess)).await?;
                            if f != Flow::Normal {
                                return Ok(f);
                            }
                        }
                    }
                }
                ProcStmt::While {
                    label,
                    cond: c,
                    body,
                } => {
                    let mut n = 0u64;
                    while cond(c, env)? {
                        let f = Box::pin(self.run_proc(body, env, privilege, user, sess)).await?;
                        match act(f, label) {
                            Act::Continue => {}
                            Act::Break => break,
                            Act::Bubble(o) => return Ok(o),
                        }
                        n += 1;
                        if n >= MAX_LOOP {
                            return Err(Error::Query("WHILE exceeded iteration limit".into()));
                        }
                    }
                }
                ProcStmt::Loop { label, body } => {
                    let mut n = 0u64;
                    loop {
                        let f = Box::pin(self.run_proc(body, env, privilege, user, sess)).await?;
                        match act(f, label) {
                            Act::Continue => {}
                            Act::Break => break,
                            Act::Bubble(o) => return Ok(o),
                        }
                        n += 1;
                        if n >= MAX_LOOP {
                            return Err(Error::Query("LOOP exceeded iteration limit".into()));
                        }
                    }
                }
                ProcStmt::Repeat { label, body, until } => {
                    let mut n = 0u64;
                    loop {
                        let f = Box::pin(self.run_proc(body, env, privilege, user, sess)).await?;
                        match act(f, label) {
                            Act::Continue => {}
                            Act::Break => break,
                            Act::Bubble(o) => return Ok(o),
                        }
                        if cond(until, env)? {
                            break;
                        }
                        n += 1;
                        if n >= MAX_LOOP {
                            return Err(Error::Query("REPEAT exceeded iteration limit".into()));
                        }
                    }
                }
                ProcStmt::Sql(s) => {
                    let sql = exec::substitute_vars(s, env);
                    Box::pin(self.execute_as(&sql, privilege, user, sess)).await?;
                }
            }
        }
        Ok(Flow::Normal)
    }

    /// Run any trigger bodies queued by the last DML (with definer/admin rights),
    /// depth-guarded against runaway recursion.
    async fn fire_triggers(&self, sess: &Session) -> Result<()> {
        let pending = sess.take_triggers();
        if pending.is_empty() {
            return Ok(());
        }
        sess.enter_call()?;
        let mut result = Ok(());
        for sql in pending {
            if let Err(e) = Box::pin(self.execute_as(&sql, Privilege::Admin, "", sess)).await {
                result = Err(e);
                break;
            }
        }
        sess.leave_call();
        result
    }

    pub async fn execute(
        &self,
        sql: &str,
        privilege: Privilege,
        sess: &Session,
    ) -> Result<Vec<QueryResult>> {
        self.execute_as(sql, privilege, "", sess).await
    }

    /// Execute with the connection's user name, so per-table (scoped) grants can
    /// raise the effective privilege for the statement's target tables.
    pub async fn execute_as(
        &self,
        sql: &str,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<Vec<QueryResult>> {
        // Cheap keyword dispatch on a short prefix — statements can be huge
        // (bulk INSERT), so never lowercase the whole thing here.
        let trimmed = sql.trim_start();
        let head: String = trimmed
            .chars()
            .take(24)
            .collect::<String>()
            .to_ascii_lowercase();

        // Transaction isolation level (SET [SESSION] TRANSACTION ISOLATION
        // LEVEL ...) — handled by string match since not all forms parse.
        if head.starts_with("set") {
            let lower = trimmed.to_ascii_lowercase();
            if lower.contains("isolation level") {
                let level = if lower.contains("serializable") {
                    Isolation::Serializable
                } else {
                    Isolation::Snapshot
                };
                sess.set_isolation(level);
                return Ok(vec![QueryResult::empty_ok()]);
            }
            // SET @user = expr
            let after = trimmed[3..].trim_start();
            if let Some(rest) = after.strip_prefix('@') {
                if let Some(eq) = rest.find('=') {
                    let name = rest[..eq].trim().to_string();
                    let expr = rest[eq + 1..].trim().trim_end_matches(';');
                    let subst = exec::substitute_uvars(expr, &sess.user_vars_snapshot());
                    let v = exec::eval_scalar(&subst)?;
                    sess.set_user_var(&name, v);
                    return Ok(vec![QueryResult::empty_ok()]);
                }
            }
        }

        // SHOW INDEX / SHOW KEYS is not parsed by the SQL frontend; handle it here.
        if head.starts_with("show index") || head.starts_with("show key") {
            let toks: Vec<&str> = trimmed.split_whitespace().collect();
            let name = toks
                .iter()
                .position(|t| t.eq_ignore_ascii_case("from") || t.eq_ignore_ascii_case("in"))
                .and_then(|i| toks.get(i + 1))
                .map(|s| s.trim_matches(['`', '"', '\'', ';']).to_string())
                .ok_or_else(|| Error::Parse("SHOW INDEX requires FROM <table>".into()))?;
            return Ok(vec![exec::show_index(sess, &name).await?]);
        }

        // BACKUP [DATABASE] TO '<path>' — hot, consistent copy of the whole
        // database to a new file. Not standard SQL, so handled here.
        if head.starts_with("backup") {
            if privilege < Privilege::Admin {
                return Err(Error::Query(
                    "access denied: BACKUP requires ADMIN privilege".into(),
                ));
            }
            let toks: Vec<&str> = trimmed.split_whitespace().collect();
            let path = toks
                .iter()
                .position(|t| t.eq_ignore_ascii_case("to"))
                .and_then(|i| toks.get(i + 1))
                .map(|s| s.trim_matches(['`', '"', '\'', ';']).to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::Parse("usage: BACKUP [DATABASE] TO '<path>'".into()))?;
            let n = sess
                .raw_db()
                .backup_to(std::path::PathBuf::from(path))
                .await?;
            return Ok(vec![QueryResult::Affected(n)]);
        }

        // CREATE FULLTEXT INDEX (not reliably parsed by the frontend).
        if head.starts_with("create fulltext index") {
            if privilege < Privilege::Admin {
                return Err(Error::Query(
                    "access denied: CREATE FULLTEXT INDEX requires ADMIN privilege".into(),
                ));
            }
            let toks: Vec<&str> = trimmed.split_whitespace().collect();
            let name = toks
                .iter()
                .position(|t| t.eq_ignore_ascii_case("index"))
                .and_then(|i| toks.get(i + 1))
                .map(|s| s.trim_matches(['`', '"']).to_string())
                .ok_or_else(|| Error::Parse("CREATE FULLTEXT INDEX requires a name".into()))?;
            let on = trimmed
                .to_ascii_lowercase()
                .find(" on ")
                .ok_or_else(|| Error::Parse("CREATE FULLTEXT INDEX requires ON".into()))?;
            let rest = trimmed[on + 4..].trim();
            let open = rest
                .find('(')
                .ok_or_else(|| Error::Parse("CREATE FULLTEXT INDEX requires (columns)".into()))?;
            let table = rest[..open].trim().trim_matches(['`', '"']).to_string();
            let close = rest
                .rfind(')')
                .ok_or_else(|| Error::Parse("CREATE FULLTEXT INDEX requires (columns)".into()))?;
            let cols: Vec<String> = rest[open + 1..close]
                .split(',')
                .map(|c| c.trim().trim_matches(['`', '"']).to_string())
                .filter(|c| !c.is_empty())
                .collect();
            return Ok(vec![
                exec::create_fulltext_index(sess, &name, &table, &cols).await?,
            ]);
        }

        // Pessimistic table locking (LOCK TABLES / UNLOCK TABLES) — not parsed by
        // the SQL frontend.
        if head.starts_with("lock tables") || head.starts_with("lock table ") {
            let rest = {
                let lower = trimmed.to_ascii_lowercase();
                let pos = lower
                    .find("tables")
                    .or_else(|| lower.find("table"))
                    .unwrap();
                trimmed[pos..]
                    .split_once(char::is_whitespace)
                    .map(|x| x.1)
                    .unwrap_or("")
            };
            for entry in rest.trim_end_matches(';').split(',') {
                let toks: Vec<&str> = entry.split_whitespace().collect();
                if toks.is_empty() {
                    continue;
                }
                let table = toks[0].trim_matches(['`', '"']).to_string();
                let mode = if entry.to_ascii_lowercase().contains("write") {
                    lockmgr::LockMode::Exclusive
                } else {
                    lockmgr::LockMode::Shared
                };
                sess.lock_table(&table, mode).await?;
            }
            return Ok(vec![QueryResult::empty_ok()]);
        }
        if head.starts_with("unlock tables") || head.starts_with("unlock table") {
            sess.unlock_tables();
            return Ok(vec![QueryResult::empty_ok()]);
        }

        // Triggers (MySQL CREATE/DROP TRIGGER, not parsed by the frontend).
        if head.starts_with("create trigger") || head.starts_with("create or replace trigger") {
            if privilege < Privilege::Admin {
                return Err(Error::Query(
                    "access denied: CREATE TRIGGER requires ADMIN privilege".into(),
                ));
            }
            let t = parse_create_trigger(trimmed)?;
            sess.commit_write(
                vec![(
                    catalog::trigger_key(&t.table, &t.name),
                    bincode::serialize(&t).map_err(|e| Error::Storage(e.to_string()))?,
                )],
                vec![],
            )
            .await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }
        if head.starts_with("drop trigger") {
            if privilege < Privilege::Admin {
                return Err(Error::Query(
                    "access denied: DROP TRIGGER requires ADMIN privilege".into(),
                ));
            }
            let toks: Vec<&str> = trimmed.split_whitespace().collect();
            let name = toks
                .iter()
                .position(|t| t.eq_ignore_ascii_case("trigger"))
                .and_then(|i| toks.get(i + 1))
                .map(|s| s.trim_matches(['`', '"', ';']).to_string())
                .filter(|s| !s.eq_ignore_ascii_case("if"))
                .ok_or_else(|| Error::Parse("DROP TRIGGER requires a name".into()))?;
            match catalog::find_trigger(sess, &name).await? {
                Some(t) => {
                    sess.commit_write(vec![], vec![catalog::trigger_key(&t.table, &t.name)])
                        .await?;
                }
                None => {
                    if !trimmed.to_ascii_lowercase().contains("if exists") {
                        return Err(Error::Query(format!("trigger does not exist: {name}")));
                    }
                }
            }
            return Ok(vec![QueryResult::empty_ok()]);
        }

        // Binlog administration (not standard SQL).
        if head.starts_with("show binary logs") || head.starts_with("show master logs") {
            return Ok(vec![exec::show_binary_logs(sess).await?]);
        }
        if head.starts_with("purge binary") || head.starts_with("purge master") {
            if privilege < Privilege::Admin {
                return Err(Error::Query(
                    "access denied: PURGE BINARY LOGS requires ADMIN privilege".into(),
                ));
            }
            let toks: Vec<&str> = trimmed.split_whitespace().collect();
            let to = toks
                .iter()
                .position(|t| t.eq_ignore_ascii_case("to"))
                .and_then(|i| toks.get(i + 1))
                .map(|s| s.trim_matches(['`', '"', '\'', ';']).to_string())
                .ok_or_else(|| Error::Parse("usage: PURGE BINARY LOGS TO '<name>'".into()))?;
            return Ok(vec![exec::purge_binary_logs(sess, &to).await?]);
        }

        // Stored procedures (CREATE/DROP PROCEDURE, CALL): the MySQL BEGIN..END
        // body is not parsed by the SQL frontend, so handle it here.
        if head.starts_with("create procedure") || head.starts_with("create or replace procedure") {
            if privilege < Privilege::Admin {
                return Err(Error::Query(
                    "access denied: CREATE PROCEDURE requires ADMIN privilege".into(),
                ));
            }
            let (name, def) = parse_create_procedure(trimmed)?;
            let enc = bincode::serialize(&def).map_err(|e| Error::Storage(e.to_string()))?;
            sess.commit_write(vec![(catalog::proc_key(&name), enc)], vec![])
                .await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }
        if head.starts_with("drop procedure") {
            if privilege < Privilege::Admin {
                return Err(Error::Query(
                    "access denied: DROP PROCEDURE requires ADMIN privilege".into(),
                ));
            }
            let toks: Vec<&str> = trimmed.split_whitespace().collect();
            let name = toks
                .iter()
                .position(|t| t.eq_ignore_ascii_case("procedure"))
                .and_then(|i| toks.get(i + 1))
                .map(|s| s.trim_matches(['`', '"', ';', '(']).to_string())
                .filter(|s| !s.eq_ignore_ascii_case("if"))
                .ok_or_else(|| Error::Parse("DROP PROCEDURE requires a name".into()))?;
            sess.commit_write(vec![], vec![catalog::proc_key(&name)])
                .await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }
        if head.starts_with("call ") {
            let call = trimmed[4..].trim().trim_end_matches(';');
            let name = call
                .split(['(', ' '])
                .next()
                .unwrap_or("")
                .trim_matches(['`', '"'])
                .to_string();
            let def: proc::ProcDef = match sess.get(catalog::proc_key(&name)).await? {
                Some(b) => bincode::deserialize(&b).map_err(|e| Error::Storage(e.to_string()))?,
                None => return Err(Error::Query(format!("procedure does not exist: {name}"))),
            };
            // Bind arguments to parameters (IN evaluated; OUT/INOUT bound to a
            // @user variable to write back).
            let mut env: std::collections::HashMap<String, Value> =
                std::collections::HashMap::new();
            let mut writeback: Vec<(String, String)> = Vec::new();
            let uvars = sess.user_vars_snapshot();
            if let (Some(open), Some(close)) = (call.find('('), call.rfind(')')) {
                let args_s = &call[open + 1..close];
                let args: Vec<&str> = if args_s.trim().is_empty() {
                    Vec::new()
                } else {
                    args_s.split(',').collect()
                };
                for (i, a) in args.iter().enumerate() {
                    let Some((pname, mode)) = def.params.get(i) else {
                        continue;
                    };
                    let a = a.trim();
                    match mode {
                        proc::ParamMode::In => {
                            env.insert(
                                pname.clone(),
                                exec::eval_scalar(&exec::substitute_uvars(a, &uvars))?,
                            );
                        }
                        proc::ParamMode::Out | proc::ParamMode::Inout => {
                            let var = a.trim_start_matches('@').to_string();
                            if a.starts_with('@') {
                                writeback.push((pname.clone(), var.clone()));
                            }
                            let init = if *mode == proc::ParamMode::Inout {
                                sess.user_var(&var)
                            } else {
                                Value::Null
                            };
                            env.insert(pname.clone(), init);
                        }
                    }
                }
            }
            let stmts = proc::parse(&def.body)?;
            sess.enter_call()?;
            let r = Box::pin(self.run_proc(&stmts, &mut env, privilege, user, sess)).await;
            sess.leave_call();
            r?;
            for (pname, var) in writeback {
                sess.set_user_var(&var, env.get(&pname).cloned().unwrap_or(Value::Null));
            }
            return Ok(vec![QueryResult::empty_ok()]);
        }

        // User management (CREATE USER / GRANT / REVOKE / ...): parsed and
        // executed here, not by the SQL frontend.
        if users::is_user_stmt(trimmed) {
            return Ok(vec![users::execute(sql, sess, privilege).await?]);
        }

        if let Some(r) = self.intercept_session(sql) {
            return Ok(vec![r]); // session/introspection: read-level
        }

        let dialect = MySqlDialect {};
        // Substitute @user variables (leaving @@system vars) before parsing.
        let mut subst_sql = if sql.contains('@') {
            exec::substitute_uvars(sql, &sess.user_vars_snapshot())
        } else {
            sql.to_string()
        };
        // `LOCK IN SHARE MODE` is a synonym for `FOR SHARE` (not parsed by the
        // MySQL dialect on its own).
        if subst_sql
            .to_ascii_lowercase()
            .contains("lock in share mode")
        {
            subst_sql = replace_ci(&subst_sql, "lock in share mode", "for share");
        }
        let statements =
            Parser::parse_sql(&dialect, &subst_sql).map_err(|e| Error::Parse(e.to_string()))?;

        let mut out = Vec::with_capacity(statements.len());
        for stmt in statements {
            let need = required_privilege(&stmt);
            let effective = self
                .effective_privilege(privilege, user, &stmt, sess)
                .await?;
            if effective < need {
                return Err(Error::Query(format!(
                    "access denied: statement requires {need:?} privilege"
                )));
            }
            // Pessimistic locking: while another session holds an explicit
            // LOCK TABLES, acquire a transient lock on this statement's target
            // tables for the statement's duration (skipped entirely otherwise).
            let mut _guards: Vec<lockmgr::LockGuard> = Vec::new();
            if self.locks.explicit_active() {
                let mode = if need >= Privilege::Write {
                    lockmgr::LockMode::Exclusive
                } else {
                    lockmgr::LockMode::Shared
                };
                for t in stmt_targets(&stmt) {
                    if !sess.holds_lock(&t) {
                        _guards.push(lockmgr::transient(&self.locks, &t, mode).await?);
                    }
                }
            }
            out.push(self.execute_stmt(stmt, sess).await?);
        }
        Ok(out)
    }

    /// A user's granted privilege on one table (default `Read`).
    async fn table_grant(&self, user: &str, table: &str, sess: &Session) -> Result<Privilege> {
        if user.is_empty() {
            return Ok(Privilege::Read);
        }
        match sess
            .get(elyra_core::users::table_grant_key(user, table))
            .await?
        {
            Some(b) => Ok(elyra_core::users::decode_privilege(&b).unwrap_or(Privilege::Read)),
            None => Ok(Privilege::Read),
        }
    }

    /// Effective privilege for a statement: the global level, raised by any
    /// per-table grant on the statement's target tables. Reads are always
    /// allowed at the global baseline; when a write/DDL target cannot be
    /// determined, the global level is required (deny-safe).
    async fn effective_privilege(
        &self,
        global: Privilege,
        user: &str,
        stmt: &Statement,
        sess: &Session,
    ) -> Result<Privilege> {
        let need = required_privilege(stmt);
        if need <= Privilege::Read {
            return Ok(global.max(Privilege::Read));
        }
        let targets = stmt_targets(stmt);
        if targets.is_empty() {
            return Ok(global);
        }
        // The statement is allowed only if every target satisfies `need`, so the
        // effective level is the minimum of per-target max(global, grant).
        let mut eff = Privilege::Admin;
        for t in targets {
            let e = global.max(self.table_grant(user, &t, sess).await?);
            if e < eff {
                eff = e;
            }
        }
        Ok(eff)
    }

    async fn execute_stmt(&self, stmt: Statement, sess: &Session) -> Result<QueryResult> {
        match stmt {
            Statement::Query(q) => {
                if query_has_from(&q) {
                    exec::select(sess, &self.vindex, &q).await
                } else {
                    eval::eval_literal_select(&q)
                }
            }
            Statement::CreateTable(ct) => exec::create_table(sess, &self.vindex, ct).await,
            Statement::Truncate { table_names, .. } => {
                let name = table_names
                    .first()
                    .and_then(|t| t.name.0.last())
                    .map(|i| i.value.clone())
                    .ok_or_else(|| Error::Catalog("empty table name".into()))?;
                exec::truncate(sess, &name).await
            }
            Statement::CreateView {
                name,
                columns,
                query,
                or_replace,
                ..
            } => exec::create_view(sess, &name, &columns, &query, or_replace).await,
            Statement::CreateIndex(ci) => exec::create_index(sess, ci).await,
            Statement::AlterTable {
                name, operations, ..
            } => exec::alter_table(sess, &name, &operations).await,
            Statement::Insert(ins) => {
                let r = exec::insert(sess, &self.vindex, ins).await?;
                self.fire_triggers(sess).await?;
                Ok(r)
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let r = exec::update(sess, &self.vindex, &table, &assignments, selection.as_ref())
                    .await?;
                self.fire_triggers(sess).await?;
                Ok(r)
            }
            Statement::Delete(del) => {
                let r = exec::delete(sess, &self.vindex, &del).await?;
                self.fire_triggers(sess).await?;
                Ok(r)
            }
            Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Table,
                names,
                if_exists,
                ..
            } => {
                let name = names
                    .first()
                    .and_then(|n| n.0.last())
                    .map(|i| i.value.clone())
                    .ok_or_else(|| Error::Catalog("empty table name".into()))?;
                exec::drop_table(sess, &name, if_exists).await
            }
            Statement::Drop {
                object_type: sqlparser::ast::ObjectType::View,
                names,
                if_exists,
                ..
            } => {
                let name = names
                    .first()
                    .and_then(|n| n.0.last())
                    .map(|i| i.value.clone())
                    .ok_or_else(|| Error::Catalog("empty view name".into()))?;
                exec::drop_view(sess, &name, if_exists).await
            }
            Statement::StartTransaction { .. } => {
                sess.begin()?;
                Ok(QueryResult::empty_ok())
            }
            Statement::Commit { .. } => {
                sess.commit().await?;
                Ok(QueryResult::empty_ok())
            }
            Statement::Rollback { savepoint, .. } => {
                match savepoint {
                    Some(name) => sess.rollback_to(&name.value)?,
                    None => sess.rollback(),
                }
                Ok(QueryResult::empty_ok())
            }
            Statement::Savepoint { name } => {
                sess.savepoint(&name.value)?;
                Ok(QueryResult::empty_ok())
            }
            Statement::Analyze { table_name, .. } => {
                let name = table_name
                    .0
                    .last()
                    .map(|i| i.value.clone())
                    .ok_or_else(|| Error::Catalog("empty table name".into()))?;
                exec::analyze_table(sess, &name).await
            }
            Statement::ReleaseSavepoint { name } => {
                sess.release_savepoint(&name.value)?;
                Ok(QueryResult::empty_ok())
            }
            Statement::ShowTables { .. } => exec::show_tables(sess).await,
            Statement::ShowCreate {
                obj_type: sqlparser::ast::ShowCreateObject::Table,
                obj_name,
            } => {
                let name = obj_name
                    .0
                    .last()
                    .map(|i| i.value.clone())
                    .ok_or_else(|| Error::Catalog("empty table name".into()))?;
                exec::show_create_table(sess, &name).await
            }
            Statement::ShowColumns { show_options, .. } => {
                let name = show_options
                    .show_in
                    .and_then(|si| si.parent_name)
                    .and_then(|n| n.0.last().map(|i| i.value.clone()))
                    .ok_or_else(|| Error::Catalog("SHOW COLUMNS requires a table".into()))?;
                exec::show_columns(sess, &name).await
            }
            Statement::ExplainTable { table_name, .. } => {
                let name = table_name
                    .0
                    .last()
                    .map(|i| i.value.clone())
                    .ok_or_else(|| Error::Catalog("empty table name".into()))?;
                exec::show_columns(sess, &name).await
            }
            Statement::SetVariable { .. } | Statement::Use { .. } => Ok(QueryResult::empty_ok()),
            other => Err(Error::Unsupported(format!(
                "statement not yet implemented: {other}"
            ))),
        }
    }

    /// Handle the well-known session/introspection queries MySQL drivers send.
    fn intercept_session(&self, sql: &str) -> Option<QueryResult> {
        let t = sql.trim().trim_end_matches(';').trim();
        // Every intercepted query is short; skip large statements cheaply
        // (a long `SET ...` is still swallowed).
        if t.len() > 48 {
            return t
                .get(..4)
                .filter(|h| h.eq_ignore_ascii_case("set "))
                .map(|_| QueryResult::empty_ok());
        }
        let lower = t.to_ascii_lowercase();

        match lower.as_str() {
            "select @@version_comment limit 1" | "select @@version_comment" => {
                Some(QueryResult::scalar(
                    "@@version_comment",
                    ColumnType::Text,
                    Value::Text("ElyraSQL — MIT licensed, robust SQL server".into()),
                ))
            }
            "select @@version" | "select version()" => Some(QueryResult::scalar(
                "version()",
                ColumnType::Text,
                Value::Text(elyra_core::SERVER_VERSION.into()),
            )),
            "select database()" | "select schema()" => Some(QueryResult::scalar(
                "database()",
                ColumnType::Text,
                Value::Null,
            )),
            _ if lower.starts_with("set ") => Some(QueryResult::empty_ok()),
            _ => None,
        }
    }
}

/// Minimum privilege required to run a statement.
/// SQL truthiness for procedure IF/WHILE conditions.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        other => other.as_f64().map(|n| n != 0.0).unwrap_or(true),
    }
}

fn object_name_last(name: &sqlparser::ast::ObjectName) -> Option<String> {
    name.0.last().map(|i| i.value.clone())
}

/// Case-insensitive substring replace (used to normalize `LOCK IN SHARE MODE`).
fn replace_ci(haystack: &str, needle: &str, replacement: &str) -> String {
    let (hl, nl) = (haystack.to_ascii_lowercase(), needle.to_ascii_lowercase());
    let mut out = String::with_capacity(haystack.len());
    let mut i = 0;
    while let Some(pos) = hl[i..].find(&nl) {
        let at = i + pos;
        out.push_str(&haystack[i..at]);
        out.push_str(replacement);
        i = at + needle.len();
    }
    out.push_str(&haystack[i..]);
    out
}

/// A single plain (unjoined) base table in `twj`, if that's what it is.
fn single_base_table(twj: &sqlparser::ast::TableWithJoins) -> Option<String> {
    if !twj.joins.is_empty() {
        return None;
    }
    match &twj.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => object_name_last(name),
        _ => None,
    }
}

/// The base tables a write/DDL statement targets (that must satisfy its
/// privilege). An empty result means "undetermined" — the caller then requires
/// the global privilege (deny-safe).
fn stmt_targets(stmt: &Statement) -> Vec<String> {
    use sqlparser::ast::*;
    match stmt {
        Statement::Insert(ins) => object_name_last(&ins.table_name).into_iter().collect(),
        Statement::Update {
            table, from: None, ..
        } => single_base_table(table).into_iter().collect(),
        Statement::Delete(del) => {
            // Only a simple single-table DELETE has a determinable target.
            if !del.tables.is_empty() {
                return vec![];
            }
            let froms = match &del.from {
                FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
            };
            match froms.as_slice() {
                [one] => single_base_table(one).into_iter().collect(),
                _ => vec![],
            }
        }
        Statement::CreateTable(ct) => object_name_last(&ct.name).into_iter().collect(),
        Statement::AlterTable { name, .. } => object_name_last(name).into_iter().collect(),
        Statement::CreateIndex(ci) => object_name_last(&ci.table_name).into_iter().collect(),
        Statement::Truncate { table_names, .. } => table_names
            .iter()
            .filter_map(|t| object_name_last(&t.name))
            .collect(),
        Statement::Drop {
            object_type: ObjectType::Table,
            names,
            ..
        } => names.iter().filter_map(object_name_last).collect(),
        _ => vec![],
    }
}

/// Parse `CREATE TRIGGER name {BEFORE|AFTER} {INSERT|UPDATE|DELETE} ON table
/// FOR EACH ROW <body>`.
fn parse_create_trigger(sql: &str) -> Result<catalog::TriggerDef> {
    use catalog::TrigEvent;
    let lower = sql.to_ascii_lowercase();
    let after = lower
        .find("trigger")
        .map(|i| i + "trigger".len())
        .ok_or_else(|| Error::Parse("malformed CREATE TRIGGER".into()))?;
    // name is the first token after TRIGGER
    let toks: Vec<&str> = sql[after..].split_whitespace().collect();
    let name = toks
        .first()
        .map(|s| s.trim_matches(['`', '"']).to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Parse("CREATE TRIGGER requires a name".into()))?;
    // The timing/event clause lives before ` ON <table> `; restrict the search
    // to the header so keywords inside the body (e.g. an INSERT statement) are
    // not mistaken for the trigger event.
    let on = lower
        .find(" on ")
        .ok_or_else(|| Error::Parse("CREATE TRIGGER requires ON <table>".into()))?;
    let header = &lower[..on];
    let before = if header.contains(" before ") {
        true
    } else if header.contains(" after ") {
        false
    } else {
        return Err(Error::Parse(
            "CREATE TRIGGER requires BEFORE or AFTER".into(),
        ));
    };
    let event = if header.contains("insert") {
        TrigEvent::Insert
    } else if header.contains("update") {
        TrigEvent::Update
    } else if header.contains("delete") {
        TrigEvent::Delete
    } else {
        return Err(Error::Parse(
            "CREATE TRIGGER requires INSERT, UPDATE or DELETE".into(),
        ));
    };
    let table = sql[on + 4..]
        .split_whitespace()
        .next()
        .map(|s| s.trim_matches(['`', '"']).to_string())
        .ok_or_else(|| Error::Parse("CREATE TRIGGER requires ON <table>".into()))?;
    // body: everything after FOR EACH ROW
    let fer = lower
        .find("for each row")
        .map(|i| i + "for each row".len())
        .ok_or_else(|| Error::Parse("CREATE TRIGGER requires FOR EACH ROW <body>".into()))?;
    let body = sql[fer..].trim().trim_end_matches(';').trim().to_string();
    if body.is_empty() {
        return Err(Error::Parse("CREATE TRIGGER has an empty body".into()));
    }
    Ok(catalog::TriggerDef {
        name,
        table,
        before,
        event,
        body,
    })
}

/// Parse `CREATE [OR REPLACE] PROCEDURE name(params) BEGIN <body> END` into the
/// procedure name and definition (parameter names + body).
fn parse_create_procedure(sql: &str) -> Result<(String, proc::ProcDef)> {
    let lower = sql.to_ascii_lowercase();
    let after_proc = lower
        .find("procedure")
        .map(|i| i + "procedure".len())
        .ok_or_else(|| Error::Parse("malformed CREATE PROCEDURE".into()))?;
    let rest = sql[after_proc..].trim_start();
    let name: String = rest
        .chars()
        .take_while(|c| !c.is_whitespace() && *c != '(')
        .collect();
    let name = name.trim_matches(['`', '"']).to_string();
    if name.is_empty() {
        return Err(Error::Parse("CREATE PROCEDURE requires a name".into()));
    }
    // Parameter list, if any, between the first '(' and its matching ')'.
    let mut params = Vec::new();
    if let Some(open) = sql[after_proc..].find('(') {
        let open = after_proc + open;
        if let Some(close) = sql[open..].find(')') {
            let inner = &sql[open + 1..open + close];
            for p in inner.split(',') {
                // [IN|OUT|INOUT] name type -> take the name token.
                let toks: Vec<&str> = p.split_whitespace().collect();
                let (mode, nm) = match toks.as_slice() {
                    [] => continue,
                    [a] => (proc::ParamMode::In, *a),
                    [a, b, ..] => match a.to_ascii_lowercase().as_str() {
                        "out" => (proc::ParamMode::Out, *b),
                        "inout" => (proc::ParamMode::Inout, *b),
                        "in" => (proc::ParamMode::In, *b),
                        _ => (proc::ParamMode::In, *a),
                    },
                };
                params.push((nm.trim_matches(['`', '"']).to_ascii_lowercase(), mode));
            }
        }
    }
    let begin = lower
        .find("begin")
        .map(|i| i + "begin".len())
        .ok_or_else(|| Error::Parse("CREATE PROCEDURE requires a BEGIN ... END body".into()))?;
    let end = lower
        .rfind("end")
        .filter(|e| *e >= begin)
        .ok_or_else(|| Error::Parse("CREATE PROCEDURE requires a BEGIN ... END body".into()))?;
    let body = sql[begin..end].trim().to_string();
    Ok((name, proc::ProcDef { params, body }))
}

fn required_privilege(stmt: &Statement) -> Privilege {
    match stmt {
        Statement::Query(_) | Statement::SetVariable { .. } | Statement::Use { .. } => {
            Privilege::Read
        }
        Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_) => Privilege::Write,
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowCreate { .. }
        | Statement::ExplainTable { .. } => Privilege::Read,
        _ => Privilege::Admin, // CREATE / DROP / CREATE INDEX and anything else
    }
}

fn query_has_from(q: &sqlparser::ast::Query) -> bool {
    // Route anything the full engine must handle: SELECTs with a FROM, set
    // operations (UNION/INTERSECT/EXCEPT), CTEs, and nested queries. Only bare
    // literal selects (`SELECT 1`) fall through to the lightweight evaluator.
    if q.with.is_some() {
        return true;
    }
    match q.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => !s.from.is_empty(),
        _ => true,
    }
}
