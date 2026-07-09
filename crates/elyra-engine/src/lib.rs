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
mod index;
mod keyenc;
mod predicate;
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
}

impl Engine {
    pub fn new(db: Db) -> Self {
        Self {
            db,
            vindex: vindex::VectorRegistry::new(),
        }
    }

    /// Parse and execute one or more `;`-separated statements, enforcing that
    /// each statement is permitted at the caller's `privilege` level.
    /// Create a per-connection session over the shared database.
    pub fn session(&self) -> Session {
        Session::new(self.db.clone())
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

        // User management (CREATE USER / GRANT / REVOKE / ...): parsed and
        // executed here, not by the SQL frontend.
        if users::is_user_stmt(trimmed) {
            return Ok(vec![users::execute(sql, sess, privilege).await?]);
        }

        if let Some(r) = self.intercept_session(sql) {
            return Ok(vec![r]); // session/introspection: read-level
        }

        let dialect = MySqlDialect {};
        let statements =
            Parser::parse_sql(&dialect, sql).map_err(|e| Error::Parse(e.to_string()))?;

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
            Statement::Insert(ins) => exec::insert(sess, &self.vindex, ins).await,
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => exec::update(sess, &self.vindex, &table, &assignments, selection.as_ref()).await,
            Statement::Delete(del) => exec::delete(sess, &self.vindex, &del).await,
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
fn object_name_last(name: &sqlparser::ast::ObjectName) -> Option<String> {
    name.0.last().map(|i| i.value.clone())
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
