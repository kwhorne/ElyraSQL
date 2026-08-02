//! ElyraSQL query engine.
//!
//! Frontend is `sqlparser` with the MySQL dialect. Execution is **async and
//! streaming** end to end, layered on the high-concurrency [`Db`] handle:
//! reads scale across connections, writes group-commit, and result sets are
//! never fully materialised. This is what lets ElyraSQL handle large data
//! under high traffic.

mod aggregate;
mod aggspill;
mod aiembed;
mod catalog;
mod colcache;
pub mod collmig;
mod cpred;
mod eval;
mod exec;
mod ft;
mod index;
mod keyenc;
pub mod lockmgr;
mod predicate;
mod proc;
mod rowdec;
mod sessfn;
mod session;
mod sort;
mod stream;
mod users;
mod vindex;
mod zonemap;

pub use session::{Isolation, Session};
pub use sort::cleanup_stale_tempfiles;

use elyra_core::{ColumnType, Error, Privilege, Result, Schema, Value};
use elyra_storage::Db;
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::borrow::Cow;

pub use stream::RowStream;

struct UpdateModifiers {
    base_sql: String,
    order_by: Vec<sqlparser::ast::OrderByExpr>,
    limit: Option<usize>,
}

struct DmlLimit {
    base_sql: String,
    limit: usize,
}

/// Outcome of a single SQL statement.
#[allow(clippy::large_enum_variant)]
pub enum QueryResult {
    /// A (streaming) result set.
    Rows(RowStream),
    /// A statement that changed state; carries affected row count.
    Affected(u64),
    /// An INSERT result, including the statement-local ID for the OK packet.
    Insert {
        affected_rows: u64,
        last_insert_id: u64,
    },
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
            qualifier: Vec::new(),
            result_metadata: Default::default(),
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
    /// Bring the database up to the current collation version, re-keying text
    /// primary keys and rebuilding text index entries if the folding changed.
    ///
    /// Must run before the server accepts connections: no query may observe a
    /// half-migrated keyspace.
    pub async fn migrate_collation(&self) -> elyra_core::Result<()> {
        crate::collmig::migrate(&self.session()).await
    }

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
        ctx: &mut proc::ProcCtx,
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
                            let f = Box::pin(self.run_proc(body, env, ctx, privilege, user, sess))
                                .await?;
                            if f != Flow::Normal {
                                return Ok(f);
                            }
                            ran = true;
                            break;
                        }
                    }
                    if !ran {
                        if let Some(body) = els {
                            let f = Box::pin(self.run_proc(body, env, ctx, privilege, user, sess))
                                .await?;
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
                        let f =
                            Box::pin(self.run_proc(body, env, ctx, privilege, user, sess)).await?;
                        match act(f, label) {
                            Act::Continue => {}
                            Act::Break => break,
                            Act::Bubble(o) => return Ok(o),
                        }
                        n += 1;
                        if n.is_multiple_of(1024) {
                            tokio::task::yield_now().await;
                        }
                        if n >= MAX_LOOP {
                            return Err(Error::Query("WHILE exceeded iteration limit".into()));
                        }
                    }
                }
                ProcStmt::Loop { label, body } => {
                    let mut n = 0u64;
                    loop {
                        let f =
                            Box::pin(self.run_proc(body, env, ctx, privilege, user, sess)).await?;
                        match act(f, label) {
                            Act::Continue => {}
                            Act::Break => break,
                            Act::Bubble(o) => return Ok(o),
                        }
                        n += 1;
                        if n.is_multiple_of(1024) {
                            tokio::task::yield_now().await;
                        }
                        if n >= MAX_LOOP {
                            return Err(Error::Query("LOOP exceeded iteration limit".into()));
                        }
                    }
                }
                ProcStmt::Repeat { label, body, until } => {
                    let mut n = 0u64;
                    loop {
                        let f =
                            Box::pin(self.run_proc(body, env, ctx, privilege, user, sess)).await?;
                        match act(f, label) {
                            Act::Continue => {}
                            Act::Break => break,
                            Act::Bubble(o) => return Ok(o),
                        }
                        if cond(until, env)? {
                            break;
                        }
                        n += 1;
                        if n.is_multiple_of(1024) {
                            tokio::task::yield_now().await;
                        }
                        if n >= MAX_LOOP {
                            return Err(Error::Query("REPEAT exceeded iteration limit".into()));
                        }
                    }
                }
                ProcStmt::DeclareHandler(h) => {
                    ctx.handlers.push(h.clone());
                }
                ProcStmt::DeclareCursor { name, query } => {
                    ctx.cursor_defs
                        .insert(name.to_ascii_lowercase(), query.clone());
                }
                ProcStmt::OpenCursor(name) => {
                    let key = name.to_ascii_lowercase();
                    let query =
                        ctx.cursor_defs.get(&key).cloned().ok_or_else(|| {
                            Error::Query(format!("cursor '{name}' is not declared"))
                        })?;
                    let sql = exec::substitute_vars(&query, env);
                    let rows = self.materialize_rows(&sql, privilege, user, sess).await?;
                    ctx.cursors.insert(key, proc::Cursor { rows, pos: 0 });
                }
                ProcStmt::CloseCursor(name) => {
                    ctx.cursors.remove(&name.to_ascii_lowercase());
                }
                ProcStmt::Fetch { cursor, vars } => {
                    let key = cursor.to_ascii_lowercase();
                    let row = {
                        let cur = ctx.cursors.get_mut(&key).ok_or_else(|| {
                            Error::Query(format!("cursor '{cursor}' is not open"))
                        })?;
                        if cur.pos < cur.rows.len() {
                            let r = cur.rows[cur.pos].clone();
                            cur.pos += 1;
                            Some(r)
                        } else {
                            None
                        }
                    };
                    match row {
                        Some(r) => {
                            for (v, val) in vars.iter().zip(r) {
                                env.insert(v.to_ascii_lowercase(), val);
                            }
                        }
                        None => {
                            // NOT FOUND: run a matching handler if one is declared.
                            if let Some(flow) = self
                                .run_handler(ctx, env, true, privilege, user, sess)
                                .await?
                            {
                                if flow == Flow::Exit {
                                    return Ok(Flow::Exit);
                                }
                            }
                        }
                    }
                }
                ProcStmt::Sql(s) => {
                    let sql = exec::substitute_vars(s, env);
                    match Box::pin(self.execute_as(&sql, privilege, user, sess)).await {
                        Ok(_) => {}
                        Err(e) => {
                            match self
                                .run_handler(ctx, env, false, privilege, user, sess)
                                .await?
                            {
                                Some(Flow::Exit) => return Ok(Flow::Exit),
                                Some(_) => {} // CONTINUE handler ran: swallow the error
                                None => return Err(e),
                            }
                        }
                    }
                }
            }
        }
        Ok(Flow::Normal)
    }

    /// Find and run a declared handler matching the current condition. Returns
    /// `Some(Flow)` if a handler ran (its kind decides continue vs exit), or
    /// `None` if no handler matched.
    async fn run_handler(
        &self,
        ctx: &mut proc::ProcCtx,
        env: &mut std::collections::HashMap<String, Value>,
        not_found: bool,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<Option<proc::Flow>> {
        let found = ctx
            .handlers
            .iter()
            .rev()
            .find(|h| h.matches(not_found))
            .map(|h| (h.kind, (*h.action).clone()));
        let Some((kind, action)) = found else {
            return Ok(None);
        };
        Box::pin(self.run_proc(
            std::slice::from_ref(&action),
            env,
            ctx,
            privilege,
            user,
            sess,
        ))
        .await?;
        Ok(Some(match kind {
            proc::HandlerKind::Exit => proc::Flow::Exit,
            proc::HandlerKind::Continue => proc::Flow::Normal,
        }))
    }

    /// Enforce per-column SELECT masking: if `user` has column grants on a table
    /// referenced by a `SELECT`, they may only read those columns. Enforced for
    /// single-base-table selects; a restricted table used in a more complex
    /// query (joins/subqueries) is denied (deny-safe).
    async fn enforce_column_masking(
        &self,
        user: &str,
        stmt: &Statement,
        sess: &Session,
    ) -> Result<()> {
        use sqlparser::ast::{SelectItem, SetExpr};
        let Statement::Query(q) = stmt else {
            return Ok(());
        };
        let SetExpr::Select(select) = q.body.as_ref() else {
            return Ok(());
        };
        // Base tables referenced in FROM.
        let mut tables: Vec<String> = Vec::new();
        for twj in &select.from {
            if let Some(t) = single_base_table(twj) {
                tables.push(t);
            }
            for j in &twj.joins {
                if let sqlparser::ast::TableFactor::Table { name, .. } = &j.relation {
                    if let Some(t) = object_name_last(name) {
                        tables.push(t);
                    }
                }
            }
        }
        let simple = select.from.len() == 1 && select.from[0].joins.is_empty() && tables.len() == 1;
        for t in &tables {
            let Some(granted) = users::column_grants(sess, user, t).await? else {
                continue; // not column-restricted on this table
            };
            if !simple {
                return Err(Error::Query(format!(
                    "access denied: column-restricted table '{t}' cannot be used in this query"
                )));
            }
            // Collect referenced columns; a wildcard means all table columns.
            let mut refs: Vec<String> = Vec::new();
            let mut ok = true;
            let mut all = false;
            for item in &select.projection {
                match item {
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => all = true,
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        ok &= collect_col_refs(e, &mut refs);
                    }
                }
            }
            if let Some(w) = &select.selection {
                ok &= collect_col_refs(w, &mut refs);
            }
            if let Some(ob) = &q.order_by {
                for o in &ob.exprs {
                    ok &= collect_col_refs(&o.expr, &mut refs);
                }
            }
            if all {
                // SELECT * requires every column of the table to be granted.
                let def = catalog::load(sess, t).await?;
                for c in &def.schema.columns {
                    refs.push(c.name.clone());
                }
            }
            if !ok {
                return Err(Error::Query(format!(
                    "access denied: query on column-restricted table '{t}' uses an \
                     expression that cannot be verified"
                )));
            }
            for r in &refs {
                if !granted
                    .iter()
                    .any(|column| predicate::identifier_eq(column, r))
                {
                    return Err(Error::Query(format!(
                        "access denied: no SELECT privilege on column '{t}.{r}'"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Handle CREATE / REFRESH / DROP MATERIALIZED VIEW by driving ordinary
    /// CTAS / DROP / catalog writes.
    async fn materialized_view(
        &self,
        sql: &str,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<Vec<QueryResult>> {
        let lower = sql.to_ascii_lowercase();
        // Position just after "... materialized view".
        let head_end = lower.find("materialized view").unwrap() + "materialized view".len();
        let rest = sql[head_end..].trim_start();
        let verb = lower.trim_start();

        if verb.starts_with("create") {
            let as_pos = rest.to_ascii_lowercase().find(" as ").ok_or_else(|| {
                Error::Parse("CREATE MATERIALIZED VIEW requires AS <query>".into())
            })?;
            let raw_name = rest[..as_pos].trim();
            let query = rest[as_pos + 4..].trim().trim_end_matches(';').to_string();
            if raw_name.is_empty() || query.is_empty() {
                return Err(Error::Parse(
                    "CREATE MATERIALIZED VIEW: name and query required".into(),
                ));
            }
            let name = mysql_table_name_fragment(raw_name)?;
            let name = exec::stored_table_ident(sess, &name)?;
            let ctas = format!("CREATE TABLE `{name}` AS {query}");
            Box::pin(self.execute_as(&ctas, privilege, user, sess)).await?;
            let dep = exec::matview_deps_put(sess, &name, &query).await?;
            sess.commit_write(
                vec![(catalog::matview_key(&name), query.into_bytes()), dep],
                vec![],
            )
            .await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }

        if verb.starts_with("refresh") {
            let raw_name = rest.trim().trim_end_matches(';');
            let name = mysql_table_name_fragment(raw_name)?;
            let name = exec::stored_table_ident(sess, &name)?;
            self.refresh_matview_atomic(&name, privilege, user, sess)
                .await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }

        // DROP [IF EXISTS] <name>
        let mut name = rest.trim().trim_end_matches(';');
        if let Some(stripped) = name.to_ascii_lowercase().strip_prefix("if exists") {
            let cut = name.len() - stripped.len();
            name = name[cut..].trim();
        }
        let name = mysql_table_name_fragment(name)?;
        let name = exec::stored_table_ident(sess, &name)?;
        if sess.get(catalog::matview_key(&name)).await?.is_none() {
            return Err(Error::Catalog(format!("no such materialized view: {name}")));
        }
        Box::pin(self.execute_as(&format!("DROP TABLE `{name}`"), privilege, user, sess)).await?;
        sess.commit_write(
            vec![],
            vec![catalog::matview_key(&name), catalog::matdep_key(&name)],
        )
        .await?;
        Ok(vec![QueryResult::empty_ok()])
    }

    /// Recompute a materialized view (DROP + CTAS) and refresh its dependency
    /// write-counters. Used by explicit REFRESH and by auto-refresh.
    async fn refresh_matview(
        &self,
        name: &str,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<()> {
        let query = match sess.get(catalog::matview_key(name)).await? {
            Some(b) => String::from_utf8_lossy(&b).into_owned(),
            None => return Err(Error::Catalog(format!("no such materialized view: {name}"))),
        };
        Box::pin(self.execute_as(&format!("DROP TABLE `{name}`"), privilege, user, sess)).await?;
        Box::pin(self.execute_as(
            &format!("CREATE TABLE `{name}` AS {query}"),
            privilege,
            user,
            sess,
        ))
        .await?;
        let dep = exec::matview_deps_put(sess, name, &query).await?;
        sess.commit_write(vec![dep], vec![]).await?;
        Ok(())
    }

    /// Run an explicit materialized-view refresh as one recoverable unit. The
    /// rebuild drops the old table before executing its defining query, so a
    /// failure must restore both the table and its indexes.
    async fn refresh_matview_atomic(
        &self,
        name: &str,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<()> {
        let mut refresh_transaction = false;
        let checkpoint = if sess.in_txn() {
            Some(sess.transaction_checkpoint()?)
        } else {
            sess.begin()?;
            refresh_transaction = true;
            None
        };

        match self.refresh_matview(name, privilege, user, sess).await {
            Ok(()) => {
                if refresh_transaction {
                    sess.commit().await?;
                } else if let Some(checkpoint) = checkpoint {
                    sess.release_transaction_checkpoint(checkpoint)?;
                }
                Ok(())
            }
            Err(error) => {
                if refresh_transaction {
                    sess.rollback();
                } else if let Some(checkpoint) = checkpoint {
                    sess.rollback_transaction_checkpoint(checkpoint)?;
                }
                Err(error)
            }
        }
    }

    /// Find every stale materialized view read anywhere in a query, including
    /// through derived tables, set operations, scalar subqueries, and views.
    async fn stale_matviews(&self, stmt: &Statement, sess: &Session) -> Result<Vec<String>> {
        let Statement::Query(q) = stmt else {
            return Ok(Vec::new());
        };
        let mut stale = Vec::new();
        for table in exec::query_materialized_relations(sess, q).await? {
            if sess.get(catalog::matview_key(&table)).await?.is_some()
                && exec::matview_is_stale(sess, &table).await?
            {
                stale.push(table);
            }
        }
        Ok(stale)
    }

    /// Execute a query and materialize all of its rows (for a cursor OPEN).
    async fn materialize_rows(
        &self,
        sql: &str,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<Vec<Vec<Value>>> {
        let mut out = Vec::new();
        for res in Box::pin(self.execute_as(sql, privilege, user, sess)).await? {
            if let QueryResult::Rows(mut rs) = res {
                loop {
                    let batch = rs.next_batch(1024).await?;
                    if batch.is_empty() {
                        break;
                    }
                    out.extend(batch);
                }
            }
        }
        Ok(out)
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
    /// Statically resolve the result columns of a simple `SELECT` for
    /// `COM_STMT_PREPARE` (no execution): `SELECT <cols|*> FROM <one base table>
    /// [WHERE ...]`. Returns `None` for anything else (execute-time still
    /// describes it). Placeholders are fine — only projection + FROM are read.
    pub async fn describe_query(&self, sql: &str, sess: &Session) -> Option<Schema> {
        use sqlparser::ast::{Expr, SelectItem, SetExpr, TableFactor};
        // Never parse a pathologically deep expression (stack-overflow guard).
        guard_sql_complexity(sql).ok()?;
        let stmts = Parser::parse_sql(&MySqlDialect {}, sql).ok()?;
        if stmts.len() != 1 {
            return None;
        }
        let Statement::Query(q) = &stmts[0] else {
            return None;
        };
        let SetExpr::Select(sel) = q.body.as_ref() else {
            return None;
        };
        // USING/NATURAL changes both wildcard width and column order by
        // coalescing its key columns. The generic descriptor below only sees
        // independent physical relations, so returning those columns would
        // advertise a different binary result shape than execution produces.
        // Decline static metadata until describe-time join planning can reuse
        // the executor's logical-schema construction.
        let has_coalescing_join = sel.from.iter().flat_map(|table| &table.joins).any(|join| {
            use sqlparser::ast::{JoinConstraint, JoinOperator};
            matches!(
                &join.join_operator,
                JoinOperator::Inner(JoinConstraint::Using(_) | JoinConstraint::Natural)
                    | JoinOperator::LeftOuter(JoinConstraint::Using(_) | JoinConstraint::Natural)
                    | JoinOperator::RightOuter(JoinConstraint::Using(_) | JoinConstraint::Natural)
                    | JoinOperator::FullOuter(JoinConstraint::Using(_) | JoinConstraint::Natural)
            )
        });
        if has_coalescing_join {
            return None;
        }
        // Resolve every plain or virtual table in FROM (across commas and joins),
        // preserving its structured factor for qualified-wildcard binding.
        // `all_plain` stays true only if every FROM relation is describable --
        // required to know the exact column count for `*`.
        // (Explicit projection items are always described by best-effort type,
        // so what matters for a prepared statement is an exact column count.)
        let factors = sel
            .from
            .iter()
            .flat_map(|table| {
                std::iter::once(&table.relation)
                    .chain(table.joins.iter().map(|join| &join.relation))
            })
            .collect::<Vec<_>>();
        let mut relations: Vec<(&TableFactor, Schema)> = Vec::new();
        let mut all_plain = !sel.from.is_empty();
        for factor in factors {
            match exec::describe_relation_schema(sess, factor).await {
                Ok(schema) => relations.push((factor, schema)),
                Err(_) => all_plain = false,
            }
        }
        let col_by_name = |n: &str| -> Option<ColumnType> {
            let want = n.rsplit('.').next().unwrap_or(n);
            for (_, schema) in &relations {
                if let Some(c) = schema.columns.iter().find(|c| {
                    predicate::identifier_eq(&c.name, n) || predicate::identifier_eq(&c.name, want)
                }) {
                    return Some(c.ty.clone());
                }
            }
            None
        };
        // Best-effort result type of a projection expression.
        fn expr_type(e: &Expr, col: &dyn Fn(&str) -> Option<ColumnType>) -> ColumnType {
            use sqlparser::ast::Value as V;
            match e {
                Expr::Identifier(i) => col(&i.value).unwrap_or(ColumnType::Text),
                Expr::CompoundIdentifier(p) => p
                    .last()
                    .and_then(|x| col(&x.value))
                    .unwrap_or(ColumnType::Text),
                Expr::Value(V::Number(n, _)) => {
                    if n.contains('.') {
                        ColumnType::Float
                    } else {
                        ColumnType::Int
                    }
                }
                Expr::Value(V::SingleQuotedString(_)) | Expr::Value(V::DoubleQuotedString(_)) => {
                    ColumnType::Text
                }
                Expr::Value(V::Boolean(_)) => ColumnType::Bool,
                Expr::Nested(inner) => expr_type(inner, col),
                Expr::Function(f) => {
                    match f
                        .name
                        .0
                        .last()
                        .map(|i| i.value.to_ascii_lowercase())
                        .as_deref()
                    {
                        Some("count") => ColumnType::Int,
                        Some(
                            "sum" | "min" | "max" | "abs" | "round" | "floor" | "ceil" | "ceiling",
                        ) => ColumnType::Int,
                        Some(
                            "avg" | "stddev" | "stddev_pop" | "stddev_samp" | "variance"
                            | "var_pop" | "var_samp",
                        ) => ColumnType::Float,
                        _ => ColumnType::Text,
                    }
                }
                Expr::BinaryOp { .. } => ColumnType::Int,
                _ => ColumnType::Text,
            }
        }
        let mut out = Vec::new();
        for item in &sel.projection {
            match item {
                // `*`: every base table's columns, in FROM order. Requires all
                // FROM relations to be plain, loadable tables (else the count is
                // unknown -> None).
                SelectItem::Wildcard(_) => {
                    if !all_plain || relations.is_empty() {
                        return None;
                    }
                    for (_, schema) in &relations {
                        out.extend(schema.columns.iter().cloned());
                    }
                }
                // Bind the complete wildcard identity to one relation before
                // expanding its columns. This keeps same-final-name joins exact.
                SelectItem::QualifiedWildcard(obj, _) => {
                    let mut matches = relations
                        .iter()
                        .filter(|(factor, _)| exec::wildcard_matches_relation(sess, obj, factor));
                    let (_, schema) = matches.next()?;
                    if matches.next().is_some() {
                        return None;
                    }
                    out.extend(schema.columns.iter().cloned());
                }
                SelectItem::UnnamedExpr(e) => {
                    let name = match e {
                        Expr::Identifier(i) => i.value.clone(),
                        Expr::CompoundIdentifier(parts) => parts.last()?.value.clone(),
                        other => format!("{other}"),
                    };
                    out.push(elyra_core::ColumnDef {
                        name,
                        ty: expr_type(e, &col_by_name),
                        nullable: true,
                        collation: elyra_core::Collation::Ci,
                        qualifier: Vec::new(),
                        result_metadata: Default::default(),
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    out.push(elyra_core::ColumnDef {
                        name: alias.value.clone(),
                        ty: expr_type(expr, &col_by_name),
                        nullable: true,
                        collation: elyra_core::Collation::Ci,
                        qualifier: Vec::new(),
                        result_metadata: Default::default(),
                    });
                }
            }
        }
        Some(Schema::new(out))
    }

    pub async fn execute_as(
        &self,
        sql: &str,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<Vec<QueryResult>> {
        // Apply the query timeout for the duration of this statement. Held as a
        // guard so every exit path clears it, and nesting-aware so a trigger or
        // procedure body inherits the outer deadline instead of resetting it.
        let _cancel = CancelGuard::new(sess);
        // Reject pathologically deep expressions before any parsing/evaluation so
        // a hostile query can't overflow the worker stack and abort the process.
        guard_sql_complexity(sql)?;
        // Cheap keyword dispatch on a short prefix — statements can be huge
        // (bulk INSERT), so never lowercase the whole thing here.
        let trimmed = sql.trim_start();
        let head: String = trimmed
            .chars()
            .take(24)
            .collect::<String>()
            .to_ascii_lowercase();

        // sqlparser does not accept MySQL's `SET SESSION TRANSACTION ...`
        // spelling, so handle that narrow form before generic parsing.
        if head.starts_with("set") {
            if let Some(level) = session_transaction_isolation(trimmed) {
                sess.set_transaction_isolation(&level)?;
                return Ok(vec![QueryResult::empty_ok()]);
            }
            // SET @user = expr
            let after = trimmed[3..].trim_start();
            if let Some(rest) = after
                .strip_prefix('@')
                .filter(|rest| !rest.starts_with('@'))
            {
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
            let name = mysql_show_index_table(trimmed)?;
            let name = exec::stored_table_ident(sess, &name)?;
            return Ok(vec![exec::show_index(sess, &name).await?]);
        }

        // sqlparser flattens SHOW TABLE STATUS into an identifier list and
        // loses the dots/clause boundaries. Parse the optional database from
        // tokens so LIKE/WHERE is not mistaken for another name component.
        if head.starts_with("show table status") {
            let (database, pattern) = mysql_show_table_status_options(trimmed)?;
            if let Some(database) = database {
                exec::selected_database_ident(sess, &database)?;
            }
            return Ok(vec![
                exec::show_table_status(sess, pattern.as_deref()).await?,
            ]);
        }

        // SHOW FUNCTION/PROCEDURE STATUS [WHERE ...] — the WHERE form doesn't
        // parse, so intercept here and return an empty routines listing.
        if head.starts_with("show function status") || head.starts_with("show procedure status") {
            return Ok(vec![exec::show_routine_status()?]);
        }

        // SHOW [FULL] PROCESSLIST — handled in-engine so it works over the
        // prepared-statement path too (SHOW FULL PROCESSLIST also fails to parse).
        if head.starts_with("show processlist") || head.starts_with("show full processlist") {
            return Ok(vec![exec::show_processlist(sess)?]);
        }

        // sqlparser 0.53 does not recognize standalone RENAME TABLE, and parses
        // ALTER TABLE ... RENAME INDEX as a malformed column rename. Handle
        // those narrow MySQL forms before the generic frontend.
        if let Some(rename) = parse_mysql_rename(trimmed) {
            require_privilege(privilege, PrivilegedAction::Rename)?;
            let result = match rename {
                MysqlRename::Table { old, new } => {
                    let old = exec::stored_table_ident(sess, &old)?;
                    let new = exec::stored_table_ident(sess, &new)?;
                    exec::rename_table(sess, &old, &new).await?
                }
                MysqlRename::Index { table, old, new } => {
                    let table = exec::stored_table_ident(sess, &table)?;
                    exec::rename_index(sess, &table, &old, &new).await?
                }
            };
            return Ok(vec![result]);
        }

        // sqlparser 0.53 does not recognize MySQL's DROP INDEX / DROP FOREIGN
        // KEY forms. Parse these narrow DDL statements before the generic
        // frontend so schema tools can remove indexes and keys.
        if let Some(drop) = parse_mysql_drop(trimmed) {
            require_privilege(privilege, PrivilegedAction::AlterTable)?;
            let table = exec::stored_table_ident(sess, &drop.table)?;
            let result = match drop.kind {
                MysqlDropKind::Index => exec::drop_index(sess, &table, &drop.name).await?,
                MysqlDropKind::ForeignKey => {
                    exec::drop_foreign_key(sess, &table, &drop.name).await?
                }
            };
            return Ok(vec![result]);
        }

        // BACKUP [DATABASE] TO '<path>' — hot, consistent copy of the whole
        // database to a new file. Not standard SQL, so handled here.
        if head.starts_with("backup") {
            require_privilege(privilege, PrivilegedAction::Backup)?;
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
            require_privilege(privilege, PrivilegedAction::CreateFulltextIndex)?;
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
            let table = mysql_table_name_fragment(rest[..open].trim())?;
            let table = exec::stored_table_ident(sess, &table)?;
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

        // CREATE TABLE ... PARTITION BY ... — sqlparser doesn't parse MySQL
        // partitioning, so strip the clause, create the base table, and store the
        // partition scheme as managed primary-key ranges (metadata + cheap
        // DROP/TRUNCATE PARTITION; scan pruning comes free from PK range scans).
        if head.starts_with("create table") {
            let lower = trimmed.to_ascii_lowercase();
            if let Some(pb) = lower.find(" partition by ") {
                let base = trimmed[..pb].trim_end().trim_end_matches(';').to_string();
                let clause = trimmed[pb + " partition by ".len()..]
                    .trim()
                    .trim_end_matches(';');
                let spec = exec::parse_partition_clause(clause)?;
                let statements = Parser::parse_sql(&MySqlDialect {}, &base)
                    .map_err(|error| Error::Parse(error.to_string()))?;
                let table = match statements.as_slice() {
                    [Statement::CreateTable(table)] => table.name.clone(),
                    _ => return Err(Error::Parse("expected one CREATE TABLE statement".into())),
                };
                let table = exec::stored_table_ident(sess, &table)?;
                Box::pin(self.execute_as(&base, privilege, user, sess)).await?;
                let enc = bincode::serialize(&spec).map_err(|e| Error::Storage(e.to_string()))?;
                sess.commit_write(vec![(catalog::partmeta_key(&table), enc)], vec![])
                    .await?;
                return Ok(vec![QueryResult::empty_ok()]);
            }
        }
        // ALTER TABLE t DROP|TRUNCATE PARTITION p
        if head.starts_with("alter table") {
            let lower = trimmed.to_ascii_lowercase();
            let op = if lower.contains("drop partition") {
                Some(("drop partition", true))
            } else if lower.contains("truncate partition") {
                Some(("truncate partition", false))
            } else {
                None
            };
            if let Some((kw, drop_meta)) = op {
                require_privilege(privilege, PrivilegedAction::AlterPartition)?;
                let (table, pname) = mysql_alter_partition_target(trimmed, kw)?;
                let table = exec::stored_table_ident(sess, &table)?;
                let spec = catalog::load_partspec(sess, &table)
                    .await?
                    .ok_or_else(|| Error::Catalog(format!("table '{table}' is not partitioned")))?;
                let where_ = exec::partition_where(&spec, &pname).ok_or_else(|| {
                    Error::Query(format!("cannot drop partition '{pname}' (unknown or HASH)"))
                })?;
                let del = format!("DELETE FROM `{table}` WHERE {where_}");
                let r = Box::pin(self.execute_as(&del, privilege, user, sess)).await?;
                if drop_meta {
                    let mut spec2 = spec;
                    spec2.parts.retain(|p| !p.name.eq_ignore_ascii_case(&pname));
                    let enc =
                        bincode::serialize(&spec2).map_err(|e| Error::Storage(e.to_string()))?;
                    sess.commit_write(vec![(catalog::partmeta_key(&table), enc)], vec![])
                        .await?;
                }
                return Ok(r);
            }
        }

        // Materialized views: CREATE / REFRESH / DROP MATERIALIZED VIEW. The data
        // lives in a normal table of the same name (built via CREATE TABLE AS
        // SELECT); matview:: stores the defining query for REFRESH.
        if head.starts_with("create materialized")
            || head.starts_with("refresh materialized")
            || head.starts_with("drop materialized")
        {
            require_privilege(privilege, PrivilegedAction::MaterializedViews)?;
            return self.materialized_view(trimmed, privilege, user, sess).await;
        }

        // LOAD DATA INFILE '<server-side path>' INTO TABLE t ... — reads a file on
        // the server and bulk-inserts it (requires ADMIN, like MySQL's FILE priv).
        if head.starts_with("load data") {
            require_privilege(privilege, PrivilegedAction::LoadDataInfile)?;
            let mut spec = exec::parse_load_data(trimmed)?;
            let table = mysql_load_data_table(trimmed)?;
            spec.table = exec::stored_table_ident(sess, &table)?;
            let content = tokio::fs::read_to_string(&spec.path).await.map_err(|e| {
                Error::Query(format!("LOAD DATA: cannot read '{}': {e}", spec.path))
            })?;
            let stmts = exec::build_load_inserts(&spec, &content, 1000);
            let mut total = 0u64;
            for stmt in stmts {
                for r in Box::pin(self.execute_as(&stmt, privilege, user, sess)).await? {
                    match r {
                        QueryResult::Affected(n)
                        | QueryResult::Insert {
                            affected_rows: n, ..
                        } => total += n,
                        QueryResult::Rows(_) => {}
                    }
                }
            }
            return Ok(vec![QueryResult::Affected(total)]);
        }

        // Pessimistic table locking (LOCK TABLES / UNLOCK TABLES) — not parsed by
        // the SQL frontend.
        if head.starts_with("lock tables") || head.starts_with("lock table ") {
            require_privilege(privilege, PrivilegedAction::LockTables)?;
            // Resolve every qualifier before taking the first lock. Otherwise a
            // later invalid database leaves the earlier locks held even though
            // the statement itself was rejected.
            let locks = mysql_lock_tables(trimmed)?
                .into_iter()
                .map(|(table, mode)| Ok((exec::stored_table_ident(sess, &table)?, mode)))
                .collect::<Result<Vec<_>>>()?;
            for (table, mode) in locks {
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
            require_privilege(privilege, PrivilegedAction::CreateTrigger)?;
            let mut t = parse_create_trigger(trimmed)?;
            let trigger = mysql_named_object(trimmed, "trigger")?;
            t.name = exec::stored_table_ident(sess, &trigger)?;
            let table = mysql_trigger_table(trimmed)?;
            t.table = exec::stored_table_ident(sess, &table)?;
            sess.commit_write(
                vec![
                    (
                        catalog::trigger_key(&t.table, &t.name),
                        bincode::serialize(&t).map_err(|e| Error::Storage(e.to_string()))?,
                    ),
                    // Name->table index for O(1) DROP TRIGGER.
                    (catalog::trigname_key(&t.name), t.table.clone().into_bytes()),
                ],
                vec![],
            )
            .await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }
        if head.starts_with("drop trigger") {
            require_privilege(privilege, PrivilegedAction::DropTrigger)?;
            let trigger = mysql_named_object(trimmed, "trigger")?;
            let name = exec::stored_table_ident(sess, &trigger)?;
            match catalog::find_trigger(sess, &name).await? {
                Some(t) => {
                    sess.commit_write(
                        vec![],
                        vec![
                            catalog::trigger_key(&t.table, &t.name),
                            catalog::trigname_key(&t.name),
                        ],
                    )
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
            require_privilege(privilege, PrivilegedAction::PurgeBinaryLogs)?;
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
            require_privilege(privilege, PrivilegedAction::CreateProcedure)?;
            let (_, def) = parse_create_procedure(trimmed)?;
            let procedure = mysql_named_object(trimmed, "procedure")?;
            let name = exec::stored_table_ident(sess, &procedure)?;
            let enc = bincode::serialize(&def).map_err(|e| Error::Storage(e.to_string()))?;
            sess.commit_write(vec![(catalog::proc_key(&name), enc)], vec![])
                .await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }
        if head.starts_with("drop procedure") {
            require_privilege(privilege, PrivilegedAction::DropProcedure)?;
            let procedure = mysql_named_object(trimmed, "procedure")?;
            let name = exec::stored_table_ident(sess, &procedure)?;
            sess.commit_write(vec![], vec![catalog::proc_key(&name)])
                .await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }
        if head.starts_with("call ") {
            let call = trimmed[4..].trim().trim_end_matches(';');
            let procedure = mysql_named_object(trimmed, "call")?;
            let name = exec::stored_table_ident(sess, &procedure)?;
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
            let mut ctx = proc::ProcCtx::default();
            sess.enter_call()?;
            let r =
                Box::pin(self.run_proc(&stmts, &mut env, &mut ctx, privilege, user, sess)).await;
            sess.leave_call();
            r?; // Flow::Exit from an EXIT handler is normal completion
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

        if let Some(r) = self.intercept_session(sql, sess) {
            return Ok(vec![r]); // session/introspection: read-level
        }

        let dialect = MySqlDialect {};
        // Substitute @user variables (leaving @@system vars) before parsing.
        let mut subst_sql: Cow<'_, str> = if exec::contains_uvar_reference(sql) {
            Cow::Owned(exec::substitute_uvars(sql, &sess.user_vars_snapshot()))
        } else {
            Cow::Borrowed(sql)
        };
        if let Some(rewritten) = rewrite_atat_session_set(&subst_sql) {
            subst_sql = Cow::Owned(rewritten);
        }
        if subst_sql.contains("@@") {
            subst_sql = Cow::Owned(exec::substitute_system_vars(&subst_sql, |name| {
                sess.system_var(name)
            }));
        }
        if sess.ansi_quotes() {
            subst_sql = Cow::Owned(rewrite_ansi_quoted_identifiers(&subst_sql));
        }
        if let Some(rewritten) = rewrite_multi_set(&subst_sql) {
            subst_sql = Cow::Owned(rewritten);
        }
        // `LOCK IN SHARE MODE` is a synonym for `FOR SHARE` (not parsed by the
        // MySQL dialect on its own).
        if contains_ci(&subst_sql, "lock in share mode") {
            subst_sql = Cow::Owned(replace_ci(&subst_sql, "lock in share mode", "for share"));
        }
        // MySQL permits an odd digit count in 0x-prefixed literals and treats
        // the leading nibble as zero. sqlparser erases the distinction between
        // that form and X'..', which requires pairs, so normalize 0x first.
        if let Some(rewritten) = rewrite_odd_0x_literals(&subst_sql) {
            subst_sql = Cow::Owned(rewritten);
        }
        // Strip trailing MySQL table options (ENGINE=, DEFAULT CHARSET/CHARACTER
        // SET, COLLATE, AUTO_INCREMENT, ROW_FORMAT, COMMENT, ...) from CREATE
        // TABLE, which the parser does not accept in all their spellings. They
        // are no-ops here (single storage engine, utf8mb4). This makes schema
        // dumps and ORM-emitted DDL parse.
        if let Some(stripped) = strip_create_table_options(&subst_sql) {
            subst_sql = Cow::Owned(stripped);
        }
        // sqlparser 0.53 represents inline key parts as bare identifiers and
        // rejects MySQL's optional `(length)` suffix. ElyraSQL's B-tree index
        // representation has no prefix-length field, so accept the MySQL DDL
        // spelling while retaining the existing full-column index behaviour.
        if let Some(stripped) = strip_index_prefix_lengths(&subst_sql) {
            subst_sql = Cow::Owned(stripped);
        }
        // CHANGE/MODIFY parse column options manually in sqlparser 0.53 and
        // omit COLLATE. Rewrite only top-level ALTER TABLE collation clauses to
        // the equivalent CHARACTER SET option, which that parser path retains.
        if let Some(rewritten) = rewrite_alter_column_collations(&subst_sql) {
            subst_sql = Cow::Owned(rewritten);
        }
        let mut update_modifiers = None;
        if let Some(parsed) = parse_update_modifiers(&subst_sql)? {
            subst_sql = Cow::Owned(parsed.base_sql.clone());
            update_modifiers = Some(parsed);
        }
        let mut dml_limit = None;
        // sqlparser does not accept a trailing LIMIT on every MySQL UPDATE and
        // DELETE shape. Parse it separately and pass the row bound to execution.
        if update_modifiers.is_none() {
            if let Some(parsed) = parse_dml_limit(&subst_sql) {
                subst_sql = Cow::Owned(parsed.base_sql);
                dml_limit = Some(parsed.limit);
            }
        }
        // Rewrite MySQL's `INSERT ... SET col = val, ...` shorthand into the
        // standard `INSERT ... (cols) VALUES (...)` the parser accepts.
        if let Some(rewritten) = rewrite_insert_set(&subst_sql) {
            subst_sql = Cow::Owned(rewritten);
        }
        // Rewrite comma-style multi-table `UPDATE t1, t2 SET ... WHERE ...` into
        // `UPDATE t1 CROSS JOIN t2 SET ... WHERE ...` (the WHERE supplies the
        // join condition, as in the comma form).
        if let Some(rewritten) = rewrite_comma_update(&subst_sql) {
            subst_sql = Cow::Owned(rewritten);
        }
        // Rewrite unary bitwise-NOT `~x` into `(x ^ 18446744073709551615)` (the
        // parser has no `~` prefix); the result is BIGINT UNSIGNED.
        if let Some(rewritten) = rewrite_tilde(&subst_sql) {
            subst_sql = Cow::Owned(rewritten);
        }
        // Rewrite the `!` logical-NOT prefix into `(NOT (...))` (after `~` so a
        // mixed `!~x` is already parenthesised).
        if let Some(rewritten) = rewrite_bang(&subst_sql) {
            subst_sql = Cow::Owned(rewritten);
        }
        let statements = match Parser::parse_sql(&dialect, subst_sql.as_ref()) {
            Ok(s) => s,
            Err(e) => {
                // The MySQL dialect rejects `GROUP BY ... WITH ROLLUP` and the
                // `<<` / `>>` shift operators; the generic dialect parses both
                // (ROLLUP into a group-by modifier, shifts into
                // PGBitwiseShiftLeft/Right, which the evaluator handles). Only
                // retry for those so all other syntax stays on the MySQL dialect.
                if contains_ci(&subst_sql, "rollup")
                    || subst_sql.contains("<<")
                    || subst_sql.contains(">>")
                {
                    Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, subst_sql.as_ref())
                        .map_err(|_| Error::Parse(e.to_string()))?
                } else {
                    return Err(Error::Parse(e.to_string()));
                }
            }
        };

        let mut out = Vec::with_capacity(statements.len());
        for stmt in statements {
            // Resolve ai_embed('...') calls (embed once, substitute a vector
            // literal) before anything inspects the statement.
            let mut stmt = stmt;
            aiembed::resolve_stmt(&mut stmt).await?;
            // Resolve session-backed functions before execution because the
            // stateless evaluator cannot read connection state.
            let database = sess.database();
            sessfn::rewrite(
                &mut stmt,
                sess.last_insert_id(),
                sess.row_count(),
                &database,
            );
            let need = required_privilege(&stmt);
            let effective = self
                .effective_privilege(privilege, user, &stmt, sess)
                .await?;
            require_privilege(effective, PrivilegedAction::Statement(need))?;
            // Fine-grained write enforcement: within the write tier, require the
            // *specific* privilege (INSERT/UPDATE/DELETE) on each target table,
            // not merely "some write". Skipped for Admin/open-auth connections
            // (full access) and for reads/DDL (handled by the tier/Admin gates).
            let need_bits = required_privset(&stmt);
            if need_bits != 0 && privilege < Privilege::Admin && !user.is_empty() {
                for t in stmt_targets(&stmt) {
                    let have = users::effective_table_privset(sess, user, &t).await?;
                    if have & need_bits != need_bits {
                        let missing = elyra_core::users::privset_to_names(need_bits & !have);
                        return Err(Error::Query(format!(
                            "access denied: {missing} command denied to user '{user}' for table '{t}'"
                        )));
                    }
                }
            }
            if statement_starts_implicit_transaction(&stmt) {
                sess.begin_implicit_transaction()?;
            }
            // Refreshes and the query that consumes them are one atomic unit.
            // Autocommit uses a temporary transaction; an explicit transaction
            // uses a private checkpoint outside the client's savepoint namespace.
            let stale_matviews = if catalog::matviews_exist(sess).await {
                self.stale_matviews(&stmt, sess).await?
            } else {
                Vec::new()
            };
            let mut refresh_transaction = false;
            let refresh_checkpoint = if stale_matviews.is_empty() {
                None
            } else if sess.in_txn() {
                Some(sess.transaction_checkpoint()?)
            } else {
                sess.begin()?;
                refresh_transaction = true;
                None
            };

            let statement_result: Result<QueryResult> = async {
                for table in &stale_matviews {
                    self.refresh_matview(table, privilege, user, sess).await?;
                }

                // Per-column masking: a column-restricted user may only read the
                // columns granted to them on a table. Skipped when no column grants
                // exist anywhere.
                if !user.is_empty() && catalog::colgrants_exist(sess).await {
                    self.enforce_column_masking(user, &stmt, sess).await?;
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
                self.execute_stmt(stmt, sess, update_modifiers.as_ref(), dml_limit)
                    .await
            }
            .await;

            let r = match statement_result {
                Ok(result) => {
                    if refresh_transaction {
                        sess.commit().await?;
                    } else if let Some(checkpoint) = refresh_checkpoint {
                        sess.release_transaction_checkpoint(checkpoint)?;
                    }
                    result
                }
                Err(error) => {
                    if refresh_transaction {
                        sess.rollback();
                    } else if let Some(checkpoint) = refresh_checkpoint {
                        sess.rollback_transaction_checkpoint(checkpoint)?;
                    }
                    return Err(error);
                }
            };
            // Track ROW_COUNT(): rows changed by DML, or -1 after a result set
            // (matches MySQL).
            match &r {
                QueryResult::Affected(n) => sess.set_row_count(*n as i64),
                QueryResult::Insert { affected_rows, .. } => {
                    sess.set_row_count(*affected_rows as i64)
                }
                QueryResult::Rows(_) => sess.set_row_count(-1),
            }
            out.push(r);
        }
        Ok(out)
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
        // Fast path: the connection's own privilege already satisfies the
        // statement. Roles and per-table grants only ever *add* privileges, so
        // no grant lookup (a storage read on every statement) is needed here.
        if global >= need {
            return Ok(global);
        }
        // Raise the connection's baseline by any roles granted to the user.
        let global = if user.is_empty() {
            global
        } else {
            global.max(users::effective_global(sess, user).await?)
        };
        if need <= Privilege::Read {
            return Ok(global.max(Privilege::Read));
        }
        let targets = stmt_targets(stmt);
        if targets.is_empty() {
            return Ok(global);
        }
        // The statement is allowed only if every target satisfies `need`, so the
        // effective level is the minimum of per-target max(global, grant). Grants
        // include those inherited from the user's roles.
        let mut eff = Privilege::Admin;
        for t in targets {
            let e = global.max(users::effective_table_grant(sess, user, &t).await?);
            if e < eff {
                eff = e;
            }
        }
        Ok(eff)
    }

    async fn execute_stmt(
        &self,
        stmt: Statement,
        sess: &Session,
        update_modifiers: Option<&UpdateModifiers>,
        dml_limit: Option<usize>,
    ) -> Result<QueryResult> {
        match stmt {
            Statement::Query(q) => {
                if query_has_from(&q) {
                    // Resolve columns against the same structured schemas used
                    // by CREATE VIEW and EXPLAIN before execution. The row
                    // evaluator intentionally accepts qualified references on
                    // an unqualified single-table scan, so this preflight is
                    // what prevents an unknown qualifier from degrading into a
                    // unique bare-column match.
                    exec::validate_query_columns(sess, &q).await?;
                    exec::select(sess, &self.vindex, &q).await
                } else {
                    eval::eval_literal_select(&q)
                }
            }
            Statement::CreateTable(ct) => exec::create_table(sess, &self.vindex, ct).await,
            Statement::Truncate { table_names, .. } => {
                let name = table_names
                    .first()
                    .map(|t| exec::stored_table_ident(sess, &t.name))
                    .transpose()?
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
                let order_by = update_modifiers
                    .map(|modifiers| modifiers.order_by.as_slice())
                    .unwrap_or_default();
                let limit = update_modifiers
                    .and_then(|modifiers| modifiers.limit)
                    .or(dml_limit);
                let r = exec::update(
                    sess,
                    &self.vindex,
                    &table,
                    &assignments,
                    selection.as_ref(),
                    order_by,
                    limit,
                )
                .await?;
                self.fire_triggers(sess).await?;
                Ok(r)
            }
            Statement::Delete(del) => {
                let r = exec::delete(sess, &self.vindex, &del, dml_limit).await?;
                self.fire_triggers(sess).await?;
                Ok(r)
            }
            Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Table,
                names,
                if_exists,
                ..
            } => {
                let table_names = names
                    .iter()
                    .map(|name| exec::stored_table_ident(sess, name))
                    .collect::<Result<Vec<_>>>()?;
                if !if_exists {
                    for name in &table_names {
                        if !catalog::exists(sess, name).await? {
                            return Err(Error::Catalog(format!("no such table: {name}")));
                        }
                    }
                }
                for name in table_names {
                    exec::drop_table(sess, &name, true).await?;
                }
                Ok(QueryResult::Affected(0))
            }
            Statement::Drop {
                object_type: sqlparser::ast::ObjectType::View,
                names,
                if_exists,
                ..
            } => {
                let view_names = names
                    .iter()
                    .map(|name| exec::stored_table_ident(sess, name))
                    .collect::<Result<Vec<_>>>()?;
                if !if_exists {
                    for name in &view_names {
                        if catalog::load_view(sess, name).await?.is_none() {
                            return Err(Error::Catalog(format!("no such view: {name}")));
                        }
                    }
                }
                for name in view_names {
                    exec::drop_view(sess, &name, true).await?;
                }
                Ok(QueryResult::Affected(0))
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
                let name = exec::stored_table_ident(sess, &table_name)?;
                exec::analyze_table(sess, &name).await
            }
            Statement::ReleaseSavepoint { name } => {
                sess.release_savepoint(&name.value)?;
                Ok(QueryResult::empty_ok())
            }
            Statement::ShowTables { show_options, .. } => {
                if let Some(database) = show_options.show_in.and_then(|show| show.parent_name) {
                    exec::selected_database_ident(sess, &database)?;
                }
                exec::show_tables(sess).await
            }
            Statement::ShowCreate {
                obj_type: sqlparser::ast::ShowCreateObject::Table,
                obj_name,
            } => {
                let name = exec::stored_table_ident(sess, &obj_name)?;
                exec::show_create_table(sess, &name).await
            }
            Statement::ShowColumns { show_options, .. } => {
                let name = show_options
                    .show_in
                    .and_then(|si| si.parent_name)
                    .ok_or_else(|| Error::Catalog("SHOW COLUMNS requires a table".into()))?;
                let name = exec::stored_table_ident(sess, &name)?;
                exec::show_columns(sess, &name).await
            }
            Statement::ExplainTable { table_name, .. } => {
                let name = exec::stored_table_ident(sess, &table_name)?;
                exec::show_columns(sess, &name).await
            }
            Statement::SetVariable {
                variables, value, ..
            } => {
                self.apply_session_variables(sess, variables, value).await?;
                Ok(QueryResult::empty_ok())
            }
            // ElyraSQL always uses utf8mb4, so changing its connection encoding
            // to that same fixed charset is an honest no-op. This form commonly
            // shares a comma-separated SET statement with real session settings.
            Statement::SetNames { .. } | Statement::SetNamesDefault {} => {
                Ok(QueryResult::empty_ok())
            }
            Statement::Use(use_expr) => {
                use sqlparser::ast::Use;
                let database = match use_expr {
                    Use::Database(name) | Use::Schema(name) | Use::Object(name) => {
                        object_name_last(&name)
                            .ok_or_else(|| Error::Catalog("empty database name".into()))?
                    }
                    Use::Default => "elyra".into(),
                    other => {
                        return Err(Error::Unsupported(format!(
                            "USE target is not supported: {other}"
                        )))
                    }
                };
                sess.set_database(&database);
                Ok(QueryResult::empty_ok())
            }
            // ElyraSQL is a single logical schema backed by one file. Reporting
            // success for an unconditional `CREATE DATABASE other` would make the
            // caller believe it got an isolated database when every connection
            // still shares `elyra`, so that form is refused. The conditional forms
            // are how tooling asks "make sure this exists" rather than "give me a
            // fresh one" -- Laravel's MigrateCommand, container entrypoints and our
            // own benches all issue `IF NOT EXISTS` -- so they succeed as no-ops,
            // as does naming the database that already exists.
            Statement::CreateDatabase { if_not_exists, .. } => {
                create_database_result(if_not_exists)
            }
            Statement::CreateSchema { if_not_exists, .. } => create_database_result(if_not_exists),
            Statement::Explain { statement, .. } => {
                if let Statement::Query(query) = statement.as_ref() {
                    exec::validate_query_relations(sess, query).await?;
                    exec::validate_query_columns(sess, query).await?;
                }
                exec::explain(sess, &statement).await
            }
            Statement::Drop {
                object_type:
                    sqlparser::ast::ObjectType::Database | sqlparser::ast::ObjectType::Schema,
                if_exists,
                names,
                ..
            } => {
                let target = names.first().and_then(object_name_last);
                drop_database_result(&sess.database(), if_exists, target.as_deref())
            }
            // Session/introspection queries GUI tools and ORMs fire on connect.
            Statement::ShowVariables { filter, .. } => exec::show_variables(sess, filter.as_ref()),
            Statement::ShowStatus { filter, .. } => exec::show_status(filter.as_ref()),
            Statement::ShowCollation { filter } => exec::show_collation(filter.as_ref()),
            Statement::ShowDatabases { .. } => exec::show_databases(sess),
            Statement::ShowVariable { variable } => {
                let kw = variable
                    .iter()
                    .map(|i| i.value.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" ");
                match kw.as_str() {
                    "warnings" | "errors" => exec::show_warnings(),
                    _ if kw.starts_with("table status") => {
                        exec::show_table_status(sess, None).await
                    }
                    _ => Err(Error::Unsupported(format!(
                        "statement not yet implemented: SHOW {kw}"
                    ))),
                }
            }
            other => Err(Error::Unsupported(format!(
                "statement not yet implemented: {other}"
            ))),
        }
    }

    async fn apply_session_variables(
        &self,
        sess: &Session,
        variables: sqlparser::ast::OneOrManyWithParens<sqlparser::ast::ObjectName>,
        values: Vec<sqlparser::ast::Expr>,
    ) -> Result<()> {
        let variables: Vec<_> = variables.into_iter().collect();
        if variables.len() != values.len() {
            return Err(Error::Parse(
                "SET variable and value counts do not match".into(),
            ));
        }
        for (variable, expr) in variables.into_iter().zip(values) {
            let name = session_variable_name(&variable);
            let value = set_expression_value(&expr)?;
            match name.as_str() {
                "autocommit" => {
                    sess.set_autocommit(session_bool(&value, "autocommit")?)
                        .await?
                }
                "sql_mode" => sess.set_sql_mode(session_text(value, "sql_mode")?),
                "foreign_key_checks" => {
                    sess.set_foreign_key_checks(session_bool(&value, "foreign_key_checks")?)
                }
                "group_concat_max_len" => {
                    sess.set_group_concat_max_len(session_usize(&value, "group_concat_max_len")?)
                }
                "transaction_isolation" | "tx_isolation" => {
                    sess.set_transaction_isolation(&session_text(value, "transaction_isolation")?)?
                }
                unsupported => {
                    return Err(Error::Unsupported(format!(
                        "session variable is not supported: {unsupported}"
                    )))
                }
            }
        }
        Ok(())
    }

    /// Handle the well-known session/introspection queries MySQL drivers send.
    fn intercept_session(&self, sql: &str, sess: &Session) -> Option<QueryResult> {
        let t = sql.trim().trim_end_matches(';').trim();
        if t.len() > 48 {
            return None;
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
                Value::Text(sess.database()),
            )),
            _ => None,
        }
    }
}

fn session_variable_name(variable: &sqlparser::ast::ObjectName) -> String {
    let mut parts = variable
        .0
        .iter()
        .map(|part| part.value.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if parts
        .first()
        .is_some_and(|part| matches!(part.as_str(), "session" | "local"))
    {
        parts.remove(0);
    }
    parts.join(".")
}

fn set_expression_value(expr: &sqlparser::ast::Expr) -> Result<Value> {
    if let sqlparser::ast::Expr::Identifier(identifier) = expr {
        let value = identifier.value.to_ascii_uppercase();
        if matches!(value.as_str(), "ON" | "OFF" | "TRUE" | "FALSE") {
            return Ok(Value::Text(value));
        }
    }
    eval::eval_expr(expr)
}

fn session_bool(value: &Value, variable: &str) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::Int(0) | Value::UInt(0) => Ok(false),
        Value::Int(1) | Value::UInt(1) => Ok(true),
        Value::Text(value) | Value::Json(value)
            if matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "0" | "off" | "false" | "no"
            ) =>
        {
            Ok(false)
        }
        Value::Text(value) | Value::Json(value)
            if matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "on" | "true" | "yes"
            ) =>
        {
            Ok(true)
        }
        _ => Err(Error::Query(format!(
            "{variable} expects ON/OFF or 0/1, got {value:?}"
        ))),
    }
}

fn session_text(value: Value, variable: &str) -> Result<String> {
    match value {
        Value::Null => Ok(String::new()),
        value @ (Value::Text(_) | Value::Json(_)) => Ok(value.to_wire_string().unwrap_or_default()),
        other => Err(Error::Query(format!(
            "{variable} expects a string, got {other:?}"
        ))),
    }
}

fn session_usize(value: &Value, variable: &str) -> Result<usize> {
    let value = match value {
        Value::Int(value) if *value >= 0 => *value as u64,
        Value::UInt(value) => *value,
        Value::Text(value) | Value::Json(value) => value.trim().parse::<u64>().map_err(|_| {
            Error::Query(format!(
                "{variable} expects a non-negative integer, got {value:?}"
            ))
        })?,
        _ => {
            return Err(Error::Query(format!(
                "{variable} expects a non-negative integer, got {value:?}"
            )))
        }
    };
    usize::try_from(value)
        .map_err(|_| Error::OutOfRange(format!("{variable} is too large: {value}")))
}

fn session_transaction_isolation(sql: &str) -> Option<String> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let mut words = sql.split_ascii_whitespace();
    if !words.next()?.eq_ignore_ascii_case("set") {
        return None;
    }
    let mut word = words.next()?;
    if word.eq_ignore_ascii_case("session") || word.eq_ignore_ascii_case("local") {
        word = words.next()?;
    }
    if !word.eq_ignore_ascii_case("transaction")
        || !words.next()?.eq_ignore_ascii_case("isolation")
        || !words.next()?.eq_ignore_ascii_case("level")
    {
        return None;
    }
    let level = words.collect::<Vec<_>>().join(" ");
    (!level.is_empty()).then_some(level)
}

fn statement_starts_implicit_transaction(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Update { .. }
            | Statement::Delete(_)
    )
}

/// sqlparser accepts one SET assignment at a time, while MySQL accepts a
/// comma-separated list (including `SET NAMES ..., SESSION sql_mode = ...`).
/// Split only top-level commas so commas inside mode strings, function calls,
/// and parenthesized tuple assignments remain intact.
fn rewrite_multi_set(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("set"))
    {
        return None;
    }
    let rest = trimmed.get(3..)?;
    if rest
        .chars()
        .next()
        .is_some_and(|character| !character.is_ascii_whitespace())
    {
        return None;
    }
    let rest = rest.trim_start();
    let assignments = split_top_level(rest, ',');
    (assignments.len() > 1).then(|| {
        assignments
            .into_iter()
            .map(|assignment| format!("SET {}", assignment.trim()))
            .collect::<Vec<_>>()
            .join("; ")
    })
}

/// Normalize MySQL's `SET @@session.variable = value` spelling to the
/// `SET SESSION variable = value` form sqlparser accepts. Do this before
/// system-variable expression substitution so the assignment target remains a
/// variable rather than becoming its current literal value.
fn rewrite_atat_session_set(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !trimmed
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("set"))
    {
        return None;
    }
    let rest = trimmed.get(3..)?.trim_start();
    let after_atat = rest.strip_prefix("@@")?;
    let variable = if after_atat
        .get(..8)
        .is_some_and(|scope| scope.eq_ignore_ascii_case("session."))
    {
        after_atat.get(8..)?
    } else if after_atat
        .get(..6)
        .is_some_and(|scope| scope.eq_ignore_ascii_case("local."))
    {
        after_atat.get(6..)?
    } else {
        after_atat
    };
    Some(format!("SET {variable}"))
}

/// In ANSI_QUOTES mode, MySQL treats double quotes as identifier delimiters.
/// sqlparser's MySQL dialect does not expose that mode, so normalize those
/// delimiters to its always-supported backtick form before parsing.
fn rewrite_ansi_quoted_identifiers(sql: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut index = 0;
    while index < chars.len() {
        match chars[index] {
            '\'' | '`' => index = exec::copy_quoted_segment(&chars, index, &mut out),
            '"' => {
                out.push('`');
                index += 1;
                while index < chars.len() {
                    let character = chars[index];
                    if character == '"' {
                        if chars.get(index + 1) == Some(&'"') {
                            out.push_str("``");
                            index += 2;
                            continue;
                        }
                        out.push('`');
                        index += 1;
                        break;
                    }
                    out.push(character);
                    index += 1;
                }
            }
            character => {
                out.push(character);
                index += 1;
            }
        }
    }
    out
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

/// `CREATE DATABASE`/`SCHEMA` against a server that has exactly one database.
///
/// `IF NOT EXISTS` asks for the database to *be there*, which it already is, so
/// it is an honest no-op — and it is the form tooling uses (Laravel's
/// `MigrateCommand`, container entrypoints, our own benches). An unconditional
/// `CREATE DATABASE` asks for a *new, empty* database, which this server cannot
/// give, so refusing beats silently sharing `elyra`.
fn create_database_result(if_not_exists: bool) -> Result<QueryResult> {
    if if_not_exists {
        return Ok(QueryResult::empty_ok());
    }
    Err(Error::Unsupported(
        "CREATE DATABASE is not supported; ElyraSQL uses the single `elyra` database \
         (use CREATE DATABASE IF NOT EXISTS to make this a no-op)"
            .into(),
    ))
}

/// `DROP DATABASE`/`SCHEMA` under the same single-database model.
///
/// `IF EXISTS` on a database that does not exist here is a no-op, which is what
/// the caller asked for. Dropping the database the session is actually using is
/// refused rather than silently ignored: the caller expects its data to be gone.
fn drop_database_result(
    current: &str,
    if_exists: bool,
    target: Option<&str>,
) -> Result<QueryResult> {
    let names_current = target.is_some_and(|t| t.eq_ignore_ascii_case(current));
    if if_exists && !names_current {
        return Ok(QueryResult::empty_ok());
    }
    Err(Error::Unsupported(
        "DROP DATABASE is not supported; ElyraSQL uses the single `elyra` database".into(),
    ))
}

#[cfg(test)]
mod database_ddl_tests {
    use super::{create_database_result, drop_database_result};

    #[test]
    fn conditional_create_is_a_no_op() {
        assert!(create_database_result(true).is_ok());
        assert!(create_database_result(false).is_err());
    }

    #[test]
    fn conditional_drop_spares_everything_but_the_live_database() {
        assert!(drop_database_result("elyra", true, Some("scratch")).is_ok());
        assert!(drop_database_result("elyra", true, Some("ELYRA")).is_err());
        assert!(drop_database_result("elyra", false, Some("scratch")).is_err());
    }
}

#[cfg(test)]
mod describe_wildcard_tests {
    use super::{Engine, Privilege};

    #[tokio::test]
    async fn qualified_wildcard_metadata_uses_the_bound_relation() {
        let engine = Engine::new(elyra_storage::Db::in_memory().unwrap());
        let session = engine.session();
        engine
            .execute(
                "CREATE TABLE TABLES (stored_id INT PRIMARY KEY, stored_label VARCHAR(16))",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();

        let schema = engine
            .describe_query(
                "SELECT information_schema.TABLES.*
                 FROM information_schema.TABLES
                 JOIN TABLES ON TABLES.stored_id = 1
                 WHERE information_schema.TABLES.TABLE_NAME = ?",
                &session,
            )
            .await
            .unwrap();
        let names = schema
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "TABLE_TYPE",
                "ENGINE",
                "TABLE_ROWS",
                "DATA_LENGTH",
                "INDEX_LENGTH",
                "TABLE_COMMENT",
                "TABLE_COLLATION",
                "AUTO_INCREMENT",
                "CREATE_OPTIONS",
            ]
        );

        let schema = engine
            .describe_query("SELECT elyra.t.* FROM elyra.TABLES AS t", &session)
            .await
            .unwrap();
        assert_eq!(
            schema
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["stored_id", "stored_label"]
        );

        assert!(engine
            .describe_query(
                "SELECT def.information_schema.TABLES.*
                     FROM def.information_schema.TABLES",
                &session,
            )
            .await
            .is_none());
    }

    #[tokio::test]
    async fn logical_join_shapes_decline_physical_static_metadata() {
        let engine = Engine::new(elyra_storage::Db::in_memory().unwrap());
        let session = engine.session();
        for sql in [
            "CREATE TABLE describe_l (id INT PRIMARY KEY, lval VARCHAR(8))",
            "CREATE TABLE describe_r (id INT PRIMARY KEY, rval VARCHAR(8))",
        ] {
            engine
                .execute(sql, Privilege::Admin, &session)
                .await
                .unwrap();
        }

        for sql in [
            "SELECT * FROM describe_l JOIN describe_r USING(id)",
            "SELECT * FROM describe_l NATURAL JOIN describe_r",
            "SELECT * FROM describe_l RIGHT JOIN describe_r USING(id)",
        ] {
            assert!(
                engine.describe_query(sql, &session).await.is_none(),
                "{sql} must not advertise its uncoalesced physical columns"
            );
        }
    }
}

/// Collect the column names referenced by an expression. Returns `false` if it
/// hits a node it doesn't understand (so the caller can be deny-safe).
fn collect_col_refs(e: &sqlparser::ast::Expr, out: &mut Vec<String>) -> bool {
    use sqlparser::ast::Expr::*;
    match e {
        Identifier(i) => {
            out.push(i.value.clone());
            true
        }
        CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                out.push(last.value.clone());
            }
            true
        }
        Value(_) => true,
        Nested(inner)
        | UnaryOp { expr: inner, .. }
        | Cast { expr: inner, .. }
        | IsNull(inner)
        | IsNotNull(inner) => collect_col_refs(inner, out),
        BinaryOp { left, right, .. } => collect_col_refs(left, out) && collect_col_refs(right, out),
        Between {
            expr, low, high, ..
        } => {
            collect_col_refs(expr, out) && collect_col_refs(low, out) && collect_col_refs(high, out)
        }
        InList { expr, list, .. } => {
            collect_col_refs(expr, out) && list.iter().all(|x| collect_col_refs(x, out))
        }
        Like { expr, pattern, .. } | ILike { expr, pattern, .. } => {
            collect_col_refs(expr, out) && collect_col_refs(pattern, out)
        }
        Function(f) => {
            use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
            if let FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(x),
                            ..
                        } => {
                            if !collect_col_refs(x, out) {
                                return false;
                            }
                        }
                        // A `*` argument (COUNT(*)) references no specific column.
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
                        _ => return false,
                    }
                }
                true
            } else {
                false
            }
        }
        Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(o) = operand {
                if !collect_col_refs(o, out) {
                    return false;
                }
            }
            for c in conditions {
                if !collect_col_refs(c, out) {
                    return false;
                }
            }
            for r in results {
                if !collect_col_refs(r, out) {
                    return false;
                }
            }
            if let Some(er) = else_result {
                return collect_col_refs(er, out);
            }
            true
        }
        _ => false, // unknown node: be conservative
    }
}

/// Allocation-free ASCII case-insensitive substring search.
fn contains_ci(haystack: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    !needle.is_empty()
        && haystack
            .as_bytes()
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
}

/// Case-insensitive substring replace (used to normalize `LOCK IN SHARE MODE`).
fn replace_ci(haystack: &str, needle: &str, replacement: &str) -> String {
    let needle_bytes = needle.as_bytes();
    if needle_bytes.is_empty() {
        return haystack.to_owned();
    }
    let mut out = String::with_capacity(haystack.len());
    let mut i = 0;
    while let Some(pos) = haystack.as_bytes()[i..]
        .windows(needle_bytes.len())
        .position(|window| window.eq_ignore_ascii_case(needle_bytes))
    {
        let at = i + pos;
        out.push_str(&haystack[i..at]);
        out.push_str(replacement);
        i = at + needle.len();
    }
    out.push_str(&haystack[i..]);
    out
}

fn rewrite_odd_0x_literals(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut insertions = Vec::new();
    let mut quote = None;
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if let Some(delimiter) = quote {
            if byte == b'\\' && delimiter != b'`' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if byte == delimiter {
                if bytes.get(i + 1) == Some(&delimiter) {
                    i += 2;
                    continue;
                }
                quote = None;
            }
            i += 1;
            continue;
        }
        if matches!(byte, b'\'' | b'"' | b'`') {
            quote = Some(byte);
            i += 1;
            continue;
        }

        let has_prefix = byte == b'0'
            && bytes
                .get(i + 1)
                .is_some_and(|next| matches!(next, b'x' | b'X'))
            && i.checked_sub(1)
                .and_then(|previous| bytes.get(previous))
                .is_none_or(|previous| !previous.is_ascii_alphanumeric() && *previous != b'_');
        if has_prefix {
            let digits = i + 2;
            let mut end = digits;
            while bytes.get(end).is_some_and(u8::is_ascii_hexdigit) {
                end += 1;
            }
            let has_token_boundary = bytes
                .get(end)
                .is_none_or(|next| !next.is_ascii_alphanumeric() && *next != b'_');
            if end > digits && (end - digits) % 2 == 1 && has_token_boundary {
                insertions.push(digits);
            }
            i = end.max(i + 1);
            continue;
        }
        i += 1;
    }

    if insertions.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(sql.len() + insertions.len());
    let mut copied = 0;
    for insertion in insertions {
        out.push_str(&sql[copied..insertion]);
        out.push('0');
        copied = insertion;
    }
    out.push_str(&sql[copied..]);
    Some(out)
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

#[derive(Clone, Copy)]
enum PrivilegedAction {
    Rename,
    AlterTable,
    AlterPartition,
    Backup,
    CreateFulltextIndex,
    MaterializedViews,
    LoadDataInfile,
    LockTables,
    CreateTrigger,
    DropTrigger,
    PurgeBinaryLogs,
    CreateProcedure,
    DropProcedure,
    Statement(Privilege),
}

impl PrivilegedAction {
    fn required(self) -> Privilege {
        match self {
            Self::AlterPartition | Self::MaterializedViews | Self::LockTables => Privilege::Write,
            Self::Statement(required) => required,
            Self::Rename
            | Self::AlterTable
            | Self::Backup
            | Self::CreateFulltextIndex
            | Self::LoadDataInfile
            | Self::CreateTrigger
            | Self::DropTrigger
            | Self::PurgeBinaryLogs
            | Self::CreateProcedure
            | Self::DropProcedure => Privilege::Admin,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Rename => "RENAME",
            Self::AlterTable | Self::AlterPartition => "ALTER TABLE",
            Self::Backup => "BACKUP",
            Self::CreateFulltextIndex => "CREATE FULLTEXT INDEX",
            Self::MaterializedViews => "materialized views",
            Self::LoadDataInfile => "LOAD DATA INFILE",
            Self::LockTables => "LOCK TABLES",
            Self::CreateTrigger => "CREATE TRIGGER",
            Self::DropTrigger => "DROP TRIGGER",
            Self::PurgeBinaryLogs => "PURGE BINARY LOGS",
            Self::CreateProcedure => "CREATE PROCEDURE",
            Self::DropProcedure => "DROP PROCEDURE",
            Self::Statement(_) => "statement",
        }
    }
}

fn require_privilege(granted: Privilege, action: PrivilegedAction) -> Result<()> {
    let required = action.required();
    if granted >= required {
        return Ok(());
    }
    let required = match required {
        Privilege::Read => "READ",
        Privilege::Write => "WRITE",
        Privilege::Admin => "ADMIN",
    };
    Err(Error::Query(format!(
        "access denied: {} requires {required} privilege",
        action.label()
    )))
}

fn required_privilege(stmt: &Statement) -> Privilege {
    match stmt {
        Statement::Query(_) | Statement::SetVariable { .. } | Statement::Use(_) => Privilege::Read,
        Statement::Insert(_) | Statement::Update { .. } | Statement::Delete(_) => Privilege::Write,
        Statement::StartTransaction { .. }
        | Statement::Commit { .. }
        | Statement::Rollback { .. }
        | Statement::Savepoint { .. }
        | Statement::ReleaseSavepoint { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowCreate { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowCollation { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowVariable { .. }
        | Statement::Explain { .. }
        | Statement::ExplainTable { .. } => Privilege::Read,
        _ => Privilege::Admin, // CREATE / DROP / CREATE INDEX and anything else
    }
}

/// The *specific* privilege bits a DML statement needs, for fine-grained
/// enforcement within the write tier (so INSERT no longer implies UPDATE/DELETE).
/// Returns 0 for statements handled by the coarse tier / Admin gates (reads stay
/// allowed at the baseline; DDL remains Admin-gated).
fn required_privset(stmt: &Statement) -> u32 {
    use elyra_core::users::priv_bits::*;
    match stmt {
        Statement::Insert(_) => INSERT,
        Statement::Update { .. } => UPDATE,
        Statement::Delete(_) => DELETE,
        _ => 0,
    }
}

/// Default cap on expression nesting/chain depth accepted from a client, in
/// operator-token units (a `a OP a OP a ...` chain of length N counts ~N). A flat
/// chain builds a left-deep AST of depth O(N); the recursive-descent parser is
/// bounded by its own recursion limit, but flat infix/prefix chains bypass that
/// and would recurse O(N) deep in the evaluator -- and, critically, in the AST's
/// own `Drop` -- overflowing the worker stack and aborting the entire process.
/// We reject such input *before* parsing so the pathological AST is never built.
const DEFAULT_MAX_EXPR_DEPTH: usize = 2000;

/// Effective expression-depth limit: `ELYRASQL_MAX_EXPR_DEPTH` if set, clamped to
/// a safe range (never high enough to reintroduce the stack-overflow), else the
/// default. Cached after first read.
fn max_expr_depth() -> usize {
    use std::sync::OnceLock;
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("ELYRASQL_MAX_EXPR_DEPTH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .map(|v| v.clamp(64, 5000))
            .unwrap_or(DEFAULT_MAX_EXPR_DEPTH)
    })
}

/// Is this a keyword that acts as an infix/prefix operator (and so extends an
/// expression chain / AST depth)?
fn is_operator_keyword(k: Keyword) -> bool {
    matches!(
        k,
        Keyword::AND
            | Keyword::OR
            | Keyword::XOR
            | Keyword::NOT
            | Keyword::LIKE
            | Keyword::ILIKE
            | Keyword::RLIKE
            | Keyword::REGEXP
            | Keyword::IN
            | Keyword::IS
            | Keyword::BETWEEN
            | Keyword::DIV
            | Keyword::MOD
    )
}

/// Reject pathologically deep expressions **before** parsing.
///
/// A single query such as `SELECT 1 + 1 + 1 ... (tens of thousands of terms)` or
/// `... WHERE id=1 OR id=1 OR ...` builds a left-deep AST whose depth is O(N).
/// Evaluating it (and even dropping it) recurses O(N) frames deep and overflows
/// the worker thread's stack, which in Rust triggers `abort()` -- killing the
/// whole server process, not just the connection. This runs on the flat token
/// stream (no recursion, safe to drop) and estimates the maximum AST depth with a
/// small bracket-aware state machine, rejecting anything over [`max_expr_depth`]
/// with a normal SQL error so the connection survives and other clients are
/// unaffected. If tokenizing fails we return `Ok(())` and let the parser produce
/// the real syntax error.
/// Arms a session's query deadline for one statement and clears it on drop, so
/// every exit path — including `?` and an unwind — leaves the session with no
/// stale deadline. Nesting-aware: only the outermost statement owns the token, so
/// a trigger or procedure body inherits the outer deadline.
struct CancelGuard<'a> {
    sess: &'a Session,
    armed_here: bool,
}

impl<'a> CancelGuard<'a> {
    fn new(sess: &'a Session) -> Self {
        let armed_here = sess.arm_cancel_if_idle();
        Self { sess, armed_here }
    }
}

impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        if self.armed_here {
            self.sess.disarm_cancel();
        }
    }
}

pub fn guard_sql_complexity(sql: &str) -> Result<()> {
    let dialect = MySqlDialect {};
    let tokens = match Tokenizer::new(&dialect, sql).tokenize() {
        Ok(t) => t,
        Err(_) => return Ok(()),
    };
    let limit = max_expr_depth();
    // Per bracket level: (base_depth at which this level is rooted, chain length
    // accumulated so far at this level). Depth at a point = base + chain.
    let mut stack: Vec<(usize, usize)> = vec![(0, 0)];
    let mut max_depth = 0usize;
    macro_rules! bump {
        ($d:expr) => {{
            let d = $d;
            if d > max_depth {
                max_depth = d;
                if max_depth > limit {
                    return Err(Error::Parse(format!(
                        "expression too deeply nested (depth limit {limit}); simplify the query"
                    )));
                }
            }
        }};
    }
    for tok in &tokens {
        if is_deepening_token(tok) {
            let top = stack.last_mut().unwrap();
            top.1 += 1;
            bump!(top.0 + top.1);
            continue;
        }
        match tok {
            // Opening a group/subscript/call roots a sub-expression one level
            // deeper (catches `((((...))))`, `f(f(f(...)))`, deep subqueries).
            Token::LParen | Token::LBracket => {
                let top = *stack.last().unwrap();
                let base = top.0 + top.1 + 1;
                bump!(base);
                stack.push((base, 0));
            }
            // Closing returns to the parent, and the group/subscript/call becomes
            // one more operand in the parent's chain. Incrementing here is what
            // catches token-balanced *postfix* chains that never accumulate an open
            // bracket depth, e.g. `x[0][0][0]...` or `f(a)(b)...`.
            Token::RParen | Token::RBracket => {
                if stack.len() > 1 {
                    stack.pop();
                }
                let top = stack.last_mut().unwrap();
                top.1 += 1;
                bump!(top.0 + top.1);
            }
            // A comma starts a fresh element (list item / argument) at this level,
            // so a long-but-shallow list (`IN (...)`, multi-row `VALUES`) doesn't
            // accumulate depth.
            Token::Comma => {
                if let Some(top) = stack.last_mut() {
                    top.1 = 0;
                }
            }
            // A statement separator resets to a fresh context, so a multi-statement
            // batch of shallow statements isn't summed into a false rejection.
            Token::SemiColon => {
                stack.clear();
                stack.push((0, 0));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Does this token deepen an expression chain (an operator that nests its
/// operands in the AST)? Covers every symbolic operator the tokenizer can emit
/// plus the keyword operators, so no infix/prefix/postfix operator is missed
/// (unknown-but-operator-shaped tokens are treated as deepening = conservative).
/// `,`, `.`, `;`, literals, identifiers and brackets are handled separately.
fn is_deepening_token(tok: &Token) -> bool {
    use Token::*;
    match tok {
        DoubleEq
        | Eq
        | Neq
        | Lt
        | Gt
        | LtEq
        | GtEq
        | Spaceship
        | Plus
        | Minus
        | Mul
        | Div
        | DuckIntDiv
        | Mod
        | StringConcat
        | Assignment
        | Ampersand
        | Pipe
        | Caret
        | Tilde
        | TildeAsterisk
        | ExclamationMarkTilde
        | ExclamationMarkTildeAsterisk
        | DoubleTilde
        | DoubleTildeAsterisk
        | ExclamationMarkDoubleTilde
        | ExclamationMarkDoubleTildeAsterisk
        | ShiftLeft
        | ShiftRight
        | Overlap
        | ExclamationMark
        | DoubleExclamationMark
        | AtSign
        | CaretAt
        | PGSquareRoot
        | PGCubeRoot
        | Arrow
        | LongArrow
        | HashArrow
        | HashLongArrow
        | AtArrow
        | ArrowAt
        | HashMinus
        | AtQuestion
        | AtAt
        | Question
        | QuestionAnd
        | QuestionPipe
        | CustomBinaryOperator(_) => true,
        Word(w) => is_operator_keyword(w.keyword),
        _ => false,
    }
}

fn mysql_ddl_tokens(sql: &str) -> Option<Vec<Token>> {
    let dialect = MySqlDialect {};
    Some(
        Tokenizer::new(&dialect, sql)
            .tokenize()
            .ok()?
            .into_iter()
            .filter(|token| !matches!(token, Token::Whitespace(_) | Token::SemiColon))
            .collect(),
    )
}

fn mysql_word(tokens: &[Token], position: usize) -> Option<&str> {
    match tokens.get(position) {
        Some(Token::Word(word)) => Some(word.value.as_str()),
        _ => None,
    }
}

fn mysql_keyword(tokens: &[Token], position: usize, expected: &str) -> bool {
    matches!(
        tokens.get(position),
        Some(Token::Word(word))
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected)
    )
}

fn mysql_table_name(
    tokens: &[Token],
    start: usize,
    end: usize,
) -> Option<sqlparser::ast::ObjectName> {
    match tokens.get(start..end)? {
        [Token::Word(table)] => Some(sqlparser::ast::ObjectName(vec![
            sqlparser::ast::Ident::new(table.value.clone()),
        ])),
        [Token::Word(schema), Token::Period, Token::Word(table)] => {
            Some(sqlparser::ast::ObjectName(vec![
                sqlparser::ast::Ident::new(schema.value.clone()),
                sqlparser::ast::Ident::new(table.value.clone()),
            ]))
        }
        _ => None,
    }
}

fn mysql_named_object(sql: &str, keyword: &str) -> Result<sqlparser::ast::ObjectName> {
    let tokens = mysql_ddl_tokens(sql)
        .ok_or_else(|| Error::Parse(format!("invalid {keyword} object name")))?;
    let keyword_position = (0..tokens.len())
        .find(|&position| mysql_keyword(&tokens, position, keyword))
        .ok_or_else(|| Error::Parse(format!("missing {keyword} object name")))?;
    let mut start = keyword_position + 1;
    if mysql_keyword(&tokens, start, "if") && mysql_keyword(&tokens, start + 1, "exists") {
        start += 2;
    }
    let end = if matches!(tokens.get(start + 1), Some(Token::Period)) {
        start + 3
    } else {
        start + 1
    };
    if matches!(tokens.get(end), Some(Token::Period)) {
        return Err(Error::Parse(format!(
            "invalid qualified {keyword} object name"
        )));
    }
    mysql_table_name(&tokens, start, end)
        .ok_or_else(|| Error::Parse(format!("invalid qualified {keyword} object name")))
}

pub(crate) fn mysql_grant_scope(
    sql: &str,
    db: &Session,
    separator: &str,
) -> Result<Option<String>> {
    let tokens = mysql_ddl_tokens(sql)
        .ok_or_else(|| Error::Parse("invalid GRANT/REVOKE object scope".into()))?;
    let end = (0..tokens.len())
        .rfind(|&position| mysql_keyword(&tokens, position, separator))
        .ok_or_else(|| {
            Error::Parse(format!(
                "GRANT/REVOKE requires {} <principal>",
                separator.to_ascii_uppercase()
            ))
        })?;
    // Search backwards from TO/FROM so a privilege column named `on` cannot be
    // mistaken for the object-clause delimiter.
    let on = (0..end)
        .rfind(|&position| mysql_keyword(&tokens, position, "on"))
        .ok_or_else(|| Error::Parse("GRANT/REVOKE requires ON <object>".into()))?;
    match tokens.get(on + 1..end).unwrap_or_default() {
        [Token::Mul] | [Token::Mul, Token::Period, Token::Mul] => Ok(None),
        [Token::Word(database), Token::Period, Token::Mul] => {
            let database = sqlparser::ast::ObjectName(vec![sqlparser::ast::Ident::new(
                database.value.clone(),
            )]);
            exec::selected_database_ident(db, &database)?;
            Ok(None)
        }
        _ => {
            let table = mysql_table_name(&tokens, on + 1, end)
                .ok_or_else(|| Error::Parse("invalid GRANT/REVOKE object scope".into()))?;
            exec::stored_table_ident(db, &table).map(Some)
        }
    }
}

fn mysql_table_name_fragment(raw_name: &str) -> Result<sqlparser::ast::ObjectName> {
    let tokens = mysql_ddl_tokens(raw_name)
        .ok_or_else(|| Error::Parse(format!("invalid qualified table name: {raw_name}")))?;
    mysql_table_name(&tokens, 0, tokens.len())
        .ok_or_else(|| Error::Parse(format!("invalid qualified table name: {raw_name}")))
}

fn mysql_alter_partition_target(
    sql: &str,
    operation: &str,
) -> Result<(sqlparser::ast::ObjectName, String)> {
    let tokens = mysql_ddl_tokens(sql)
        .ok_or_else(|| Error::Parse("invalid ALTER TABLE partition statement".into()))?;
    let verb = operation
        .split_whitespace()
        .next()
        .ok_or_else(|| Error::Parse("missing partition operation".into()))?;
    let operation = (2..tokens.len())
        .find(|&position| {
            mysql_keyword(&tokens, position, verb)
                && mysql_keyword(&tokens, position + 1, "partition")
        })
        .ok_or_else(|| Error::Parse("ALTER TABLE requires a partition operation".into()))?;
    let table = mysql_table_name(&tokens, 2, operation)
        .ok_or_else(|| Error::Parse("ALTER TABLE has an invalid table name".into()))?;
    let partition = match tokens.get(operation + 2) {
        Some(Token::Word(name)) if operation + 3 == tokens.len() => name.value.clone(),
        _ => {
            return Err(Error::Parse(
                "ALTER TABLE partition operation requires one partition name".into(),
            ))
        }
    };
    Ok((table, partition))
}

fn mysql_load_data_table(sql: &str) -> Result<sqlparser::ast::ObjectName> {
    let tokens =
        mysql_ddl_tokens(sql).ok_or_else(|| Error::Parse("invalid LOAD DATA statement".into()))?;
    let into = (0..tokens.len())
        .find(|&position| {
            mysql_keyword(&tokens, position, "into")
                && mysql_keyword(&tokens, position + 1, "table")
        })
        .ok_or_else(|| Error::Parse("LOAD DATA requires INTO TABLE <table>".into()))?;
    let start = into + 2;
    let end = (start..tokens.len())
        .find(|&position| {
            matches!(tokens[position], Token::LParen)
                || ["fields", "columns", "lines", "ignore", "character", "set"]
                    .iter()
                    .any(|keyword| mysql_keyword(&tokens, position, keyword))
        })
        .unwrap_or(tokens.len());
    mysql_table_name(&tokens, start, end)
        .ok_or_else(|| Error::Parse("LOAD DATA has an invalid table name".into()))
}

fn mysql_lock_tables(sql: &str) -> Result<Vec<(sqlparser::ast::ObjectName, lockmgr::LockMode)>> {
    let tokens = mysql_ddl_tokens(sql)
        .ok_or_else(|| Error::Parse("invalid LOCK TABLES statement".into()))?;
    if !mysql_keyword(&tokens, 0, "lock")
        || !(mysql_keyword(&tokens, 1, "table") || mysql_keyword(&tokens, 1, "tables"))
    {
        return Err(Error::Parse("invalid LOCK TABLES statement".into()));
    }
    let mut entries = Vec::new();
    let mut start = 2;
    while start < tokens.len() {
        let end = tokens[start..]
            .iter()
            .position(|token| matches!(token, Token::Comma))
            .map(|offset| start + offset)
            .unwrap_or(tokens.len());
        let name_end = match tokens.get(start..end) {
            Some([Token::Word(_), Token::Period, Token::Word(_), ..]) => start + 3,
            Some([Token::Word(_), ..]) => start + 1,
            _ => return Err(Error::Parse("LOCK TABLES has an invalid table name".into())),
        };
        let name = mysql_table_name(&tokens, start, name_end)
            .ok_or_else(|| Error::Parse("LOCK TABLES has an invalid table name".into()))?;
        let modifiers = &tokens[name_end..end];
        let write = modifiers.iter().any(
            |token| matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case("write")),
        );
        let read = modifiers.iter().any(
            |token| matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case("read")),
        );
        if !write && !read {
            return Err(Error::Parse(
                "LOCK TABLES requires READ or WRITE for each table".into(),
            ));
        }
        entries.push((
            name,
            if write {
                lockmgr::LockMode::Exclusive
            } else {
                lockmgr::LockMode::Shared
            },
        ));
        start = end + usize::from(end < tokens.len());
    }
    Ok(entries)
}

fn mysql_trigger_table(sql: &str) -> Result<sqlparser::ast::ObjectName> {
    let tokens = mysql_ddl_tokens(sql)
        .ok_or_else(|| Error::Parse("invalid CREATE TRIGGER statement".into()))?;
    let on = (0..tokens.len())
        .find(|&position| mysql_keyword(&tokens, position, "on"))
        .ok_or_else(|| Error::Parse("CREATE TRIGGER requires ON <table>".into()))?;
    let for_each_row = (on + 1..tokens.len())
        .find(|&position| {
            mysql_keyword(&tokens, position, "for")
                && mysql_keyword(&tokens, position + 1, "each")
                && mysql_keyword(&tokens, position + 2, "row")
        })
        .ok_or_else(|| Error::Parse("CREATE TRIGGER requires FOR EACH ROW".into()))?;
    mysql_table_name(&tokens, on + 1, for_each_row)
        .ok_or_else(|| Error::Parse("CREATE TRIGGER has an invalid table name".into()))
}

fn mysql_show_table_status_options(
    sql: &str,
) -> Result<(Option<sqlparser::ast::ObjectName>, Option<String>)> {
    let tokens = mysql_ddl_tokens(sql)
        .ok_or_else(|| Error::Parse("invalid SHOW TABLE STATUS statement".into()))?;
    if !(mysql_keyword(&tokens, 0, "show")
        && mysql_keyword(&tokens, 1, "table")
        && mysql_keyword(&tokens, 2, "status"))
    {
        return Err(Error::Parse("invalid SHOW TABLE STATUS statement".into()));
    }
    let mut position = 3;
    let database = if mysql_keyword(&tokens, position, "from")
        || mysql_keyword(&tokens, position, "in")
    {
        let database = mysql_table_name(&tokens, position + 1, position + 2)
            .ok_or_else(|| Error::Parse("SHOW TABLE STATUS has an invalid database name".into()))?;
        position += 2;
        Some(database)
    } else {
        None
    };
    if mysql_keyword(&tokens, position, "where") {
        return Err(Error::Unsupported(
            "SHOW TABLE STATUS WHERE is not supported".into(),
        ));
    }
    let pattern = if mysql_keyword(&tokens, position, "like") {
        let pattern = match tokens.get(position + 1) {
            Some(Token::SingleQuotedString(pattern) | Token::DoubleQuotedString(pattern)) => {
                pattern.clone()
            }
            _ => {
                return Err(Error::Parse(
                    "SHOW TABLE STATUS LIKE requires a string pattern".into(),
                ))
            }
        };
        position += 2;
        Some(pattern)
    } else {
        None
    };
    if position != tokens.len() {
        return Err(Error::Parse(
            "SHOW TABLE STATUS has unexpected trailing tokens".into(),
        ));
    }
    Ok((database, pattern))
}

fn mysql_show_index_table(sql: &str) -> Result<sqlparser::ast::ObjectName> {
    let tokens = mysql_ddl_tokens(sql)
        .ok_or_else(|| Error::Parse("SHOW INDEX requires FROM <table>".into()))?;
    let from = tokens
        .iter()
        .enumerate()
        .skip(2)
        .find_map(|(position, _)| {
            (mysql_keyword(&tokens, position, "from") || mysql_keyword(&tokens, position, "in"))
                .then_some(position)
        })
        .ok_or_else(|| Error::Parse("SHOW INDEX requires FROM <table>".into()))?;
    let table_start = from + 1;
    let suffix = (table_start..tokens.len()).find(|&position| {
        mysql_keyword(&tokens, position, "from")
            || mysql_keyword(&tokens, position, "in")
            || mysql_keyword(&tokens, position, "where")
    });
    let table_end = suffix.unwrap_or(tokens.len());
    let table = mysql_table_name(&tokens, table_start, table_end)
        .ok_or_else(|| Error::Parse("SHOW INDEX requires FROM <table>".into()))?;

    let Some(schema_marker) = suffix.filter(|&position| {
        mysql_keyword(&tokens, position, "from") || mysql_keyword(&tokens, position, "in")
    }) else {
        return Ok(table);
    };
    let schema_start = schema_marker + 1;
    let schema_end = (schema_start..tokens.len())
        .find(|&position| mysql_keyword(&tokens, position, "where"))
        .unwrap_or(tokens.len());
    let schema = mysql_table_name(&tokens, schema_start, schema_end)
        .ok_or_else(|| Error::Parse("SHOW INDEX has an invalid database qualifier".into()))?;
    let mut parts = schema.0;
    parts.extend(table.0);
    Ok(sqlparser::ast::ObjectName(parts))
}

enum MysqlRename {
    Table {
        old: sqlparser::ast::ObjectName,
        new: sqlparser::ast::ObjectName,
    },
    Index {
        table: sqlparser::ast::ObjectName,
        old: String,
        new: String,
    },
}

/// Parse MySQL rename forms absent from sqlparser 0.53. The commonly emitted
/// single-pair `RENAME TABLE` form accepts quoted and schema-qualified names.
fn parse_mysql_rename(sql: &str) -> Option<MysqlRename> {
    let tokens = mysql_ddl_tokens(sql)?;

    if mysql_keyword(&tokens, 0, "rename") && mysql_keyword(&tokens, 1, "table") {
        let to_position = (2..tokens.len()).find(|&i| mysql_keyword(&tokens, i, "to"))?;
        return Some(MysqlRename::Table {
            old: mysql_table_name(&tokens, 2, to_position)?,
            new: mysql_table_name(&tokens, to_position + 1, tokens.len())?,
        });
    }

    if tokens.len() >= 7 && mysql_keyword(&tokens, 0, "alter") && mysql_keyword(&tokens, 1, "table")
    {
        let rename_position = tokens.iter().position(
            |token| matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case("rename")),
        )?;
        if rename_position + 5 == tokens.len()
            && (mysql_keyword(&tokens, rename_position + 1, "index")
                || mysql_keyword(&tokens, rename_position + 1, "key"))
            && mysql_keyword(&tokens, rename_position + 3, "to")
        {
            return Some(MysqlRename::Index {
                table: mysql_table_name(&tokens, 2, rename_position)?,
                old: mysql_word(&tokens, rename_position + 2)?.to_string(),
                new: mysql_word(&tokens, rename_position + 4)?.to_string(),
            });
        }
    }

    None
}

#[derive(Clone, Copy)]
enum MysqlDropKind {
    Index,
    ForeignKey,
}

struct MysqlDrop {
    table: sqlparser::ast::ObjectName,
    name: String,
    kind: MysqlDropKind,
}

/// Parse the MySQL-only index/key removal forms rejected by sqlparser 0.53:
/// `ALTER TABLE t DROP INDEX i`, `ALTER TABLE t DROP FOREIGN KEY fk`, and
/// `DROP INDEX i ON t`. Quoted and schema-qualified table names are accepted.
fn parse_mysql_drop(sql: &str) -> Option<MysqlDrop> {
    let tokens = mysql_ddl_tokens(sql)?;

    if tokens.len() >= 6 && mysql_keyword(&tokens, 0, "alter") && mysql_keyword(&tokens, 1, "table")
    {
        let drop_position = tokens.iter().position(
            |token| matches!(token, Token::Word(word) if word.value.eq_ignore_ascii_case("drop")),
        )?;
        let table = mysql_table_name(&tokens, 2, drop_position)?;
        if drop_position + 3 == tokens.len()
            && (mysql_keyword(&tokens, drop_position + 1, "index")
                || mysql_keyword(&tokens, drop_position + 1, "key"))
        {
            return Some(MysqlDrop {
                table,
                name: mysql_word(&tokens, drop_position + 2)?.to_string(),
                kind: MysqlDropKind::Index,
            });
        }
        if drop_position + 4 == tokens.len()
            && mysql_keyword(&tokens, drop_position + 1, "foreign")
            && mysql_keyword(&tokens, drop_position + 2, "key")
        {
            return Some(MysqlDrop {
                table,
                name: mysql_word(&tokens, drop_position + 3)?.to_string(),
                kind: MysqlDropKind::ForeignKey,
            });
        }
    }

    if tokens.len() >= 5
        && mysql_keyword(&tokens, 0, "drop")
        && mysql_keyword(&tokens, 1, "index")
        && mysql_keyword(&tokens, 3, "on")
    {
        return Some(MysqlDrop {
            table: mysql_table_name(&tokens, 4, tokens.len())?,
            name: mysql_word(&tokens, 2)?.to_string(),
            kind: MysqlDropKind::Index,
        });
    }

    None
}

/// Make ALTER TABLE CHANGE/MODIFY collation clauses visible to sqlparser 0.53.
/// Its column-option parser retains `CHARACTER SET <name>` but, unlike the
/// CREATE/ADD column path, does not consume `COLLATE <name>`. The executor maps
/// both spellings onto ElyraSQL's stored text collation.
fn rewrite_alter_column_collations(sql: &str) -> Option<String> {
    let statement = sql.trim_start();
    if !keyword_at(statement.as_bytes(), 0, b"alter") {
        return None;
    }
    let table_position = find_top_level_keyword(statement, b"table")?;
    if !statement[..table_position]
        .trim()
        .eq_ignore_ascii_case("alter")
    {
        return None;
    }
    if find_top_level_keyword(statement, b"change").is_none()
        && find_top_level_keyword(statement, b"modify").is_none()
    {
        return None;
    }

    let mut positions = Vec::new();
    let mut search_from = 0;
    while let Some(relative) = find_top_level_keyword(&sql[search_from..], b"collate") {
        let position = search_from + relative;
        positions.push(position);
        search_from = position + b"collate".len();
    }
    if positions.is_empty() {
        return None;
    }

    let mut rewritten = String::with_capacity(sql.len() + positions.len() * 6);
    let mut copied_through = 0;
    for position in positions {
        rewritten.push_str(&sql[copied_through..position]);
        rewritten.push_str("CHARACTER SET");
        copied_through = position + b"collate".len();
    }
    rewritten.push_str(&sql[copied_through..]);
    Some(rewritten)
}

/// Extract MySQL's single-table `UPDATE ... ORDER BY ... [LIMIT ...]` tail.
/// The UPDATE itself remains on sqlparser 0.53; the tail is parsed by wrapping
/// it in a SELECT, whose ORDER BY / LIMIT AST is already supported.
fn parse_update_modifiers(sql: &str) -> Result<Option<UpdateModifiers>> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    if !keyword_at(trimmed.as_bytes(), 0, b"update") {
        return Ok(None);
    }
    let Some((order_start, order_expr_start)) = find_top_level_order_by(trimmed) else {
        return Ok(None);
    };

    let tail = &trimmed[order_expr_start..];
    let limit_start = find_top_level_keyword(tail, b"limit");
    let order_sql = match limit_start {
        Some(position) => tail[..position].trim(),
        None => tail.trim(),
    };
    if order_sql.is_empty() {
        return Ok(None);
    }
    let limit_sql = limit_start.map(|position| tail[position..].trim());
    let wrapper = match limit_sql {
        Some(limit) => {
            format!("SELECT * FROM __elyra_update_order ORDER BY {order_sql} {limit}")
        }
        None => format!("SELECT * FROM __elyra_update_order ORDER BY {order_sql}"),
    };
    let mut statements = Parser::parse_sql(&MySqlDialect {}, &wrapper)
        .map_err(|error| Error::Parse(error.to_string()))?;
    let Statement::Query(mut query) = statements
        .pop()
        .filter(|_| statements.is_empty())
        .ok_or_else(|| Error::Parse("invalid UPDATE ORDER BY clause".into()))?
    else {
        return Err(Error::Parse("invalid UPDATE ORDER BY clause".into()));
    };
    let order_by = query
        .order_by
        .take()
        .map(|order| order.exprs)
        .unwrap_or_default();
    if query.offset.is_some() {
        return Err(Error::Parse(
            "UPDATE LIMIT does not support an offset".into(),
        ));
    }

    Ok(Some(UpdateModifiers {
        base_sql: trimmed[..order_start].trim_end().to_string(),
        order_by,
        limit: query
            .limit
            .take()
            .map(|limit| exec::eval_usize(&limit))
            .transpose()?,
    }))
}

fn find_top_level_order_by(sql: &str) -> Option<(usize, usize)> {
    let bytes = sql.as_bytes();
    let mut search_from = 0;
    while let Some(order_start) = find_top_level_keyword(&sql[search_from..], b"order") {
        let order_start = search_from + order_start;
        let mut by_start = order_start + b"order".len();
        while bytes.get(by_start).is_some_and(u8::is_ascii_whitespace) {
            by_start += 1;
        }
        if keyword_at(bytes, by_start, b"by") {
            return Some((order_start, by_start + b"by".len()));
        }
        search_from = order_start + b"order".len();
    }
    None
}

/// Byte offset of an ASCII keyword outside quotes and parentheses.
fn find_top_level_keyword(sql: &str, keyword: &[u8]) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut quote = None;
    let mut depth = 0usize;
    let mut position = 0usize;
    while position < bytes.len() {
        let byte = bytes[position];
        if let Some(delimiter) = quote {
            if byte == b'\\' {
                position = position.saturating_add(2);
                continue;
            }
            if byte == delimiter {
                if bytes.get(position + 1) == Some(&delimiter) {
                    position += 2;
                    continue;
                }
                quote = None;
            }
        } else {
            match byte {
                b'\'' | b'"' | b'`' => quote = Some(byte),
                b'#' => {
                    position += 1;
                    while position < bytes.len() && bytes[position] != b'\n' {
                        position += 1;
                    }
                    continue;
                }
                b'-' if bytes.get(position + 1) == Some(&b'-') => {
                    position += 2;
                    while position < bytes.len() && bytes[position] != b'\n' {
                        position += 1;
                    }
                    continue;
                }
                b'/' if bytes.get(position + 1) == Some(&b'*') => {
                    position += 2;
                    while position + 1 < bytes.len()
                        && (bytes[position] != b'*' || bytes[position + 1] != b'/')
                    {
                        position += 1;
                    }
                    position = (position + 2).min(bytes.len());
                    continue;
                }
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                _ if depth == 0 && keyword_at(bytes, position, keyword) => return Some(position),
                _ => {}
            }
        }
        position += 1;
    }
    None
}

fn keyword_at(sql: &[u8], position: usize, keyword: &[u8]) -> bool {
    let Some(candidate) = sql.get(position..position.saturating_add(keyword.len())) else {
        return false;
    };
    if !candidate.eq_ignore_ascii_case(keyword) {
        return false;
    }
    let is_identifier = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$';
    !position
        .checked_sub(1)
        .and_then(|previous| sql.get(previous))
        .is_some_and(|byte| is_identifier(*byte))
        && !sql
            .get(position + keyword.len())
            .is_some_and(|byte| is_identifier(*byte))
}

/// Remove trailing table options from a `CREATE TABLE (...) <options>` statement
/// that the SQL parser cannot accept in every MySQL spelling (`ENGINE=`,
/// `DEFAULT CHARSET`/`CHARACTER SET`, `COLLATE '...'`, `AUTO_INCREMENT=`,
/// `ROW_FORMAT=`, `COMMENT='...'`, ...). Returns `Some(new_sql)` only when it
/// safely truncated options after the column-definition list. Leaves anything
/// with `PARTITION`/`AS SELECT`/`LIKE` after the columns untouched.
fn strip_create_table_options(sql: &str) -> Option<String> {
    let head = sql.trim_start();
    let mut up = head
        .chars()
        .take(40)
        .collect::<String>()
        .to_ascii_uppercase();
    up.retain(|c| !c.is_whitespace() || c == ' ');
    if !up.starts_with("CREATE") || !up.contains("TABLE") {
        return None;
    }
    // Find the column-list opening paren, tracking string/backtick literals.
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let open = loop {
        if i >= bytes.len() {
            return None;
        }
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                let q = bytes[i];
                i += 1;
                while i < bytes.len() && bytes[i] != q {
                    i += 1;
                }
            }
            b'(' => break i,
            _ => {}
        }
        i += 1;
    };
    // The first paren only *starts a column list* when nothing before it turned the
    // statement into `CREATE TABLE ... AS <query>` or `... LIKE t`. In a CTAS the
    // first paren usually belongs to the query instead -- `COUNT(*)`, a derived
    // table, a function call -- and treating it as the column list truncated the
    // statement at that paren's partner, silently dropping the rest of the query
    // (`... AS SELECT g, COUNT(*) AS c FROM u GROUP BY g` became
    // `... AS SELECT g, COUNT(*)`, which then failed on an unresolvable column).
    let before_paren = &sql[..open];
    if [b"AS".as_slice(), b"SELECT".as_slice(), b"LIKE".as_slice()]
        .iter()
        .any(|kw| (0..before_paren.len()).any(|p| keyword_at(before_paren.as_bytes(), p, kw)))
    {
        return None;
    }
    // Match the closing paren of the column list.
    let mut depth = 0i32;
    let mut j = open;
    let close = loop {
        if j >= bytes.len() {
            return None;
        }
        match bytes[j] {
            b'\'' | b'"' | b'`' => {
                let q = bytes[j];
                j += 1;
                while j < bytes.len() && bytes[j] != q {
                    j += 1;
                }
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    break j;
                }
            }
            _ => {}
        }
        j += 1;
    };
    let tail = sql[close + 1..].trim().trim_end_matches(';').trim();
    if tail.is_empty() {
        return None;
    }
    let tail_up = tail.to_ascii_uppercase();
    // Preserve clauses the parser genuinely handles / that carry semantics.
    if tail_up.contains("PARTITION") || tail_up.contains("SELECT") || tail_up.starts_with("LIKE") {
        return None;
    }
    // Everything after the column list is table options -> drop it.
    Some(sql[..=close].to_string())
}

/// Strip MySQL key-part prefix lengths from inline `CREATE TABLE` indexes.
///
/// `sqlparser` 0.53 parses an inline key column as an [`Ident`] and therefore
/// rejects `KEY key_name (column_name(191))`. ElyraSQL currently stores only
/// full-column index definitions, so this accepts the MySQL syntax without
/// changing the existing catalog or index-maintenance representation. The
/// rewrite is deliberately limited to table-level PRIMARY, UNIQUE, KEY, and
/// INDEX constraints; type widths and expressions elsewhere are left intact.
fn strip_index_prefix_lengths(sql: &str) -> Option<String> {
    let statement = sql.trim_start();
    if !keyword_at(statement.as_bytes(), 0, b"create") {
        return None;
    }

    let open = first_unquoted_lparen(sql)?;
    let before = &sql[..open];
    if find_top_level_keyword(before, b"table").is_none()
        || find_top_level_keyword(before, b"as").is_some()
        || find_top_level_keyword(before, b"select").is_some()
        || find_top_level_keyword(before, b"like").is_some()
    {
        return None;
    }
    let close = matching_paren_end(sql, open)? - 1;

    let body = &sql[open + 1..close];
    let mut changed = false;
    let body = split_top_level(body, ',')
        .into_iter()
        .map(|constraint| {
            let rewritten = strip_index_prefix_lengths_from_constraint(&constraint);
            changed |= rewritten.is_some();
            rewritten.unwrap_or(constraint)
        })
        .collect::<Vec<_>>()
        .join(",");
    changed.then(|| format!("{}{}{}", &sql[..=open], body, &sql[close..]))
}

/// The first opening parenthesis outside quoted SQL text/identifiers.
fn first_unquoted_lparen(sql: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut position = 0;
    while position < bytes.len() {
        match bytes[position] {
            b'\'' | b'"' | b'`' => position = quoted_end(bytes, position)?,
            b'(' => return Some(position),
            _ => position += 1,
        }
    }
    None
}

/// Remove `(digits)` directly following top-level key-column identifiers.
fn strip_index_prefix_lengths_from_constraint(constraint: &str) -> Option<String> {
    if !is_inline_index_constraint(constraint) {
        return None;
    }
    let open = first_unquoted_lparen(constraint)?;
    let close = matching_paren_end(constraint, open)? - 1;
    let columns = &constraint[open + 1..close];
    let bytes = columns.as_bytes();
    let mut depth = 0usize;
    let mut position = 0;
    let mut copied_through = 0;
    let mut rewritten = String::with_capacity(constraint.len());

    while position < bytes.len() {
        match bytes[position] {
            b'\'' | b'"' => position = quoted_end(bytes, position)?,
            b'`' if depth == 0 => {
                let ident_end = quoted_end(bytes, position)?;
                let prefix_end = index_prefix_length_end(bytes, ident_end);
                if prefix_end > ident_end {
                    rewritten.push_str(&columns[copied_through..ident_end]);
                    copied_through = prefix_end;
                    position = prefix_end;
                } else {
                    position = ident_end;
                }
            }
            b'`' => position = quoted_end(bytes, position)?,
            b'(' => {
                depth += 1;
                position += 1;
            }
            b')' => {
                depth = depth.saturating_sub(1);
                position += 1;
            }
            byte if depth == 0 && is_identifier_start(byte) => {
                let ident_end = bare_identifier_end(bytes, position);
                let prefix_end = index_prefix_length_end(bytes, ident_end);
                if prefix_end > ident_end {
                    rewritten.push_str(&columns[copied_through..ident_end]);
                    copied_through = prefix_end;
                    position = prefix_end;
                } else {
                    position = ident_end;
                }
            }
            _ => position += 1,
        }
    }

    if copied_through == 0 {
        return None;
    }
    rewritten.push_str(&columns[copied_through..]);
    Some(format!(
        "{}{}{}",
        &constraint[..=open],
        rewritten,
        &constraint[close..]
    ))
}

fn is_inline_index_constraint(constraint: &str) -> bool {
    let Some(tokens) = mysql_ddl_tokens(constraint) else {
        return false;
    };
    let mut position = 0;
    if mysql_keyword(&tokens, position, "constraint") {
        position += 1;
        if !matches!(tokens.get(position), Some(Token::Word(_))) {
            return false;
        }
        position += 1;
    }
    if mysql_keyword(&tokens, position, "primary") {
        return mysql_keyword(&tokens, position + 1, "key");
    }
    mysql_keyword(&tokens, position, "unique")
        || mysql_keyword(&tokens, position, "key")
        || mysql_keyword(&tokens, position, "index")
}

fn is_identifier_start(byte: u8) -> bool {
    !byte.is_ascii() || byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$')
}

fn bare_identifier_end(bytes: &[u8], mut position: usize) -> usize {
    while bytes.get(position).is_some_and(|byte| {
        !byte.is_ascii() || byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'$')
    }) {
        position += 1;
    }
    position
}

/// Return the byte after a well-formed key-part suffix, or `ident_end` when
/// the following text is not a `(digits)` prefix length.
fn index_prefix_length_end(bytes: &[u8], ident_end: usize) -> usize {
    let mut position = ident_end;
    while bytes
        .get(position)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        position += 1;
    }
    if bytes.get(position) != Some(&b'(') {
        return ident_end;
    }
    position += 1;
    while bytes
        .get(position)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        position += 1;
    }
    let digits_start = position;
    while bytes.get(position).is_some_and(u8::is_ascii_digit) {
        position += 1;
    }
    if position == digits_start {
        return ident_end;
    }
    while bytes
        .get(position)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
        position += 1;
    }
    if bytes.get(position) == Some(&b')') {
        position + 1
    } else {
        ident_end
    }
}

/// Return the byte after a quoted SQL literal or identifier.
fn quoted_end(bytes: &[u8], start: usize) -> Option<usize> {
    let quote = *bytes.get(start)?;
    let mut position = start + 1;
    while position < bytes.len() {
        if bytes[position] == b'\\' {
            position = (position + 2).min(bytes.len());
            continue;
        }
        if bytes[position] == quote {
            if bytes.get(position + 1) == Some(&quote) {
                position += 2;
                continue;
            }
            return Some(position + 1);
        }
        position += 1;
    }
    None
}

/// Extract a trailing `LIMIT <n>` from an `UPDATE`/`DELETE` statement when the
/// parser does not accept that MySQL form.
fn parse_dml_limit(sql: &str) -> Option<DmlLimit> {
    let head = sql.trim_start();
    let up = head.get(..7).unwrap_or(head).to_ascii_uppercase();
    if !(up.starts_with("UPDATE ") || up.starts_with("DELETE ")) {
        return None;
    }
    let trimmed = sql.trim_end().trim_end_matches(';').trim_end();
    // Match a trailing `LIMIT <digits>` (case-insensitive).
    let bytes = trimmed.as_bytes();
    let mut i = bytes.len();
    while i > 0 && bytes[i - 1].is_ascii_digit() {
        i -= 1;
    }
    if i == bytes.len() {
        return None; // no trailing number
    }
    let before_num = trimmed[..i].trim_end();
    // Compare the last five *bytes* (LIMIT is ASCII); byte-indexing avoids a
    // panic when `len - 5` would fall inside a multi-byte UTF-8 char. When they
    // do match, `len - 5` is an ASCII (char) boundary, so the split below is safe.
    let bb = before_num.as_bytes();
    if bb.len() >= 5 && bb[bb.len() - 5..].eq_ignore_ascii_case(b"limit") {
        let kept = before_num[..before_num.len() - 5].trim_end();
        return Some(DmlLimit {
            base_sql: kept.to_string(),
            limit: trimmed[i..].parse().ok()?,
        });
    }
    None
}

/// Rewrite MySQL's unary bitwise-NOT `~<operand>` into `(<operand> ^
/// 18446744073709551615)` (XOR with all-ones = 64-bit NOT), which the parser
/// accepts and the evaluator computes as `BIGINT UNSIGNED` (see `Value::UInt`).
/// The MySQL/generic dialects have no prefix parser for `~`, so this bridges it.
/// Quote-aware; processes right-to-left so nested `~~x` works. Returns None if
/// there is no top-level `~`, or if any `~`'s operand isn't a shape we can bound
/// safely (then the original is left to fail parsing rather than be mis-scoped).
/// Rewrite MySQL's high-precedence logical-NOT prefix `!x` into `(NOT (x))` (no
/// SQL dialect parses a bare `!` prefix). Wrapping the tightly-bound operand in
/// parentheses preserves `!`'s precedence: `!a = b` becomes `(NOT (a)) = b`, i.e.
/// `(!a) = b`, not `NOT (a = b)`. Skips the `!=` operator and string/quoted
/// contexts. Runs after `rewrite_tilde`, so `!~a` is already `!(...)`.
fn rewrite_bang(sql: &str) -> Option<String> {
    if !sql.contains('!') {
        return None;
    }
    let mut s = sql.to_string();
    // Rewrite the right-most `!` each pass, so nested `!!x` resolves inside-out.
    while let Some(p) = last_top_level_bang(&s) {
        let b = s.as_bytes();
        let mut j = p + 1;
        while j < b.len() && (b[j] == b' ' || b[j] == b'\t') {
            j += 1;
        }
        let Some(end) = tilde_operand_end(&s, j) else {
            return None; // un-boundable operand -> abandon the rewrite
        };
        let operand = &s[j..end];
        let replaced = format!("(NOT ({operand}))");
        s = format!("{}{}{}", &s[..p], replaced, &s[end..]);
    }
    Some(s)
}

/// Right-most top-level `!` that is a prefix operator (not part of `!=`), outside
/// string/quoted contexts.
fn last_top_level_bang(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let (mut in_s, mut in_d, mut in_b) = (false, false, false);
    let mut last = None;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if in_s {
            if c == b'\'' {
                in_s = false;
            }
        } else if in_d {
            if c == b'"' {
                in_d = false;
            }
        } else if in_b {
            if c == b'`' {
                in_b = false;
            }
        } else {
            match c {
                b'\'' => in_s = true,
                b'"' => in_d = true,
                b'`' => in_b = true,
                // A `!` not immediately followed by `=` is the prefix operator.
                b'!' if b.get(i + 1) != Some(&b'=') => last = Some(i),
                _ => {}
            }
        }
        i += 1;
    }
    last
}

fn rewrite_tilde(sql: &str) -> Option<String> {
    if !sql.contains('~') {
        return None;
    }
    let mut s = sql.to_string();
    // Rewrite the right-most `~` each pass until none remain (nested `~~x` works
    // because the inner one becomes `(...)` before the outer is processed).
    while let Some(p) = last_top_level_tilde(&s) {
        // Operand starts at the first non-space after `~`.
        let b = s.as_bytes();
        let mut j = p + 1;
        while j < b.len() && (b[j] == b' ' || b[j] == b'\t') {
            j += 1;
        }
        let Some(end) = tilde_operand_end(&s, j) else {
            return None; // un-boundable operand -> abandon the rewrite
        };
        let operand = &s[j..end];
        let replaced = format!("({operand} ^ 18446744073709551615)");
        s = format!("{}{}{}", &s[..p], replaced, &s[end..]);
    }
    Some(s)
}

/// Byte offset of the right-most `~` that is outside any string literal.
fn last_top_level_tilde(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let (mut in_s, mut in_d, mut in_b) = (false, false, false);
    let mut last = None;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if in_s {
            if c == b'\'' {
                in_s = false;
            }
        } else if in_d {
            if c == b'"' {
                in_d = false;
            }
        } else if in_b {
            if c == b'`' {
                in_b = false;
            }
        } else {
            match c {
                b'\'' => in_s = true,
                b'"' => in_d = true,
                b'`' => in_b = true,
                b'~' => last = Some(i),
                _ => {}
            }
        }
        i += 1;
    }
    last
}

/// End (exclusive) of the unary operand starting at byte `start`, for the safe
/// shapes: parenthesised expr, number, backtick/plain identifier chain
/// (optionally a function call). Returns None for anything else.
fn tilde_operand_end(s: &str, start: usize) -> Option<usize> {
    let b = s.as_bytes();
    if start >= b.len() {
        return None;
    }
    match b[start] {
        b'(' => matching_paren_end(s, start),
        b'0'..=b'9' => {
            let mut i = start;
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
                i += 1;
            }
            Some(i)
        }
        b'`' => {
            // backtick identifier, then optional `.col` / call
            let mut i = start + 1;
            while i < b.len() && b[i] != b'`' {
                i += 1;
            }
            if i >= b.len() {
                return None;
            }
            i += 1; // closing backtick
            ident_tail_end(s, i)
        }
        c if c.is_ascii_alphabetic() || c == b'_' || c == b'@' => {
            let mut i = start;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'@') {
                i += 1;
            }
            ident_tail_end(s, i)
        }
        _ => None,
    }
}

/// Extend an identifier operand across `.member` and a trailing `(...)` call.
fn ident_tail_end(s: &str, mut i: usize) -> Option<usize> {
    let b = s.as_bytes();
    loop {
        if i < b.len() && b[i] == b'.' {
            i += 1;
            if i < b.len() && b[i] == b'`' {
                i += 1;
                while i < b.len() && b[i] != b'`' {
                    i += 1;
                }
                if i >= b.len() {
                    return None;
                }
                i += 1;
            } else {
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'@')
                {
                    i += 1;
                }
            }
        } else if i < b.len() && b[i] == b'(' {
            return matching_paren_end(s, i);
        } else {
            return Some(i);
        }
    }
}

/// End (exclusive) of a balanced parenthesised group starting at `start` (`(`),
/// quote-aware. None if unbalanced.
fn matching_paren_end(s: &str, start: usize) -> Option<usize> {
    let b = s.as_bytes();
    let (mut in_s, mut in_d, mut in_b) = (false, false, false);
    let mut depth = 0i32;
    let mut i = start;
    while i < b.len() {
        let c = b[i];
        if in_s {
            if c == b'\'' {
                in_s = false;
            }
        } else if in_d {
            if c == b'"' {
                in_d = false;
            }
        } else if in_b {
            if c == b'`' {
                in_b = false;
            }
        } else {
            match c {
                b'\'' => in_s = true,
                b'"' => in_d = true,
                b'`' => in_b = true,
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i + 1);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Fuzz / property entry point: run the full SQL string-preprocessing chain and
/// the parser over arbitrary input. It must **never panic** (only produce
/// `Some`/`None`/`Ok`/`Err`), regardless of how malformed, non-UTF-8-boundary,
/// or adversarial the input is. Driven by both the `cargo-fuzz` target
/// (`fuzz/fuzz_targets/preprocess.rs`) and a stable proptest, so the invariant is
/// checked in normal CI too.
pub fn fuzz_preprocess_parse(sql: &str) {
    // Mirror the real pipeline: reject over-deep expressions before parsing, so
    // neither the parser, the evaluator, nor the AST's Drop can recurse
    // unboundedly on adversarial flat chains.
    if guard_sql_complexity(sql).is_err() {
        return;
    }
    let mut s = sql.to_string();
    if contains_ci(&s, "lock in share mode") {
        s = replace_ci(&s, "lock in share mode", "for share");
    }
    if let Some(x) = rewrite_odd_0x_literals(&s) {
        s = x;
    }
    if let Some(x) = strip_create_table_options(&s) {
        s = x;
    }
    if let Some(x) = strip_index_prefix_lengths(&s) {
        s = x;
    }
    let _ = parse_update_modifiers(&s);
    if let Some(x) = parse_dml_limit(&s) {
        s = x.base_sql;
    }
    if let Some(x) = rewrite_insert_set(&s) {
        s = x;
    }
    if let Some(x) = rewrite_comma_update(&s) {
        s = x;
    }
    if let Some(x) = rewrite_tilde(&s) {
        s = x;
    }
    if let Some(x) = rewrite_bang(&s) {
        s = x;
    }
    let _ = split_top_level(&s, ',');
    let _ = split_top_level(&s, '=');
    // Parse both dialects (the generic one is the ROLLUP / shift fallback).
    let _ = Parser::parse_sql(&MySqlDialect {}, &s);
    if s.to_ascii_lowercase().contains("rollup") || s.contains("<<") || s.contains(">>") {
        let _ = Parser::parse_sql(&sqlparser::dialect::GenericDialect {}, &s);
    }
}

/// Return true if the ASCII keyword `kw` sits at byte offset `i` in `bytes`
/// with word boundaries on both sides (case-insensitive).
fn kw_at(bytes: &[u8], i: usize, kw: &str) -> bool {
    let k = kw.as_bytes();
    if i + k.len() > bytes.len() {
        return false;
    }
    if !bytes[i..i + k.len()].eq_ignore_ascii_case(k) {
        return false;
    }
    let boundary = |b: u8| !(b.is_ascii_alphanumeric() || b == b'_');
    let before_ok = i == 0 || boundary(bytes[i - 1]);
    let after_ok = i + k.len() == bytes.len() || boundary(bytes[i + k.len()]);
    before_ok && after_ok
}

/// Split `s` on top-level occurrences of `sep` (paren depth 0, outside
/// single/double-quote and backtick strings). Handles doubled-quote escapes.
fn split_top_level(s: &str, sep: char) -> Vec<String> {
    let b = s.as_bytes();
    let (mut in_s, mut in_d, mut in_b) = (false, false, false);
    let mut depth = 0i32;
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        let c = b[i] as char;
        if in_s {
            if c == '\'' {
                in_s = false;
            }
        } else if in_d {
            if c == '"' {
                in_d = false;
            }
        } else if in_b {
            if c == '`' {
                in_b = false;
            }
        } else {
            match c {
                '\'' => in_s = true,
                '"' => in_d = true,
                '`' => in_b = true,
                '(' => depth += 1,
                ')' => depth -= 1,
                _ if depth == 0 && c == sep => {
                    out.push(s[start..i].to_string());
                    start = i + 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    out.push(s[start..].to_string());
    out
}

/// Rewrite MySQL's `INSERT [options] INTO t SET a = 1, b = 2
/// [ON DUPLICATE KEY UPDATE ...]` into the standard
/// `INSERT [options] INTO t (a, b) VALUES (1, 2) [ON DUPLICATE KEY UPDATE ...]`,
/// which the parser accepts. Returns None if the statement is not an
/// `INSERT ... SET` (e.g. a normal `INSERT ... VALUES`), so callers fall through
/// unchanged. Quote- and paren-aware, so commas/`=` inside string literals or
/// function calls are respected.
fn rewrite_insert_set(sql: &str) -> Option<String> {
    let head = sql.trim_start();
    if !head
        .as_bytes()
        .get(..6)
        .is_some_and(|b| b.eq_ignore_ascii_case(b"INSERT"))
    {
        return None;
    }
    let bytes = sql.as_bytes();
    let (mut in_s, mut in_d, mut in_b) = (false, false, false);
    let mut depth = 0i32;
    let mut i = 0usize;
    let mut set_pos: Option<usize> = None;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_s {
            if c == '\'' {
                in_s = false;
            }
        } else if in_d {
            if c == '"' {
                in_d = false;
            }
        } else if in_b {
            if c == '`' {
                in_b = false;
            }
        } else {
            match c {
                '\'' => in_s = true,
                '"' => in_d = true,
                '`' => in_b = true,
                '(' => depth += 1,
                ')' => depth -= 1,
                _ if depth == 0 => {
                    // A top-level VALUES/SELECT before SET means this is a normal
                    // insert; leave it alone.
                    if kw_at(bytes, i, "VALUES") || kw_at(bytes, i, "SELECT") {
                        return None;
                    }
                    if kw_at(bytes, i, "SET") {
                        set_pos = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    let set_pos = set_pos?;
    let prefix = sql[..set_pos].trim_end();
    let after = &sql[set_pos + 3..];
    let after = after.trim_end().trim_end_matches(';').trim_end();

    // Split off a trailing ON DUPLICATE KEY UPDATE clause (kept verbatim).
    let ab = after.as_bytes();
    let (mut s2, mut d2, mut b2) = (false, false, false);
    let mut dep2 = 0i32;
    let mut odku: Option<usize> = None;
    let mut j = 0usize;
    while j < ab.len() {
        let c = ab[j] as char;
        if s2 {
            if c == '\'' {
                s2 = false;
            }
        } else if d2 {
            if c == '"' {
                d2 = false;
            }
        } else if b2 {
            if c == '`' {
                b2 = false;
            }
        } else {
            match c {
                '\'' => s2 = true,
                '"' => d2 = true,
                '`' => b2 = true,
                '(' => dep2 += 1,
                ')' => dep2 -= 1,
                _ if dep2 == 0 && kw_at(ab, j, "ON") => {
                    // require "ON DUPLICATE" to avoid false positives
                    let rest = after[j..].trim_start();
                    if rest
                        .as_bytes()
                        .get(..12)
                        .is_some_and(|b| b.eq_ignore_ascii_case(b"ON DUPLICATE"))
                    {
                        odku = Some(j);
                        break;
                    }
                }
                _ => {}
            }
        }
        j += 1;
    }
    let (assigns, suffix) = match odku {
        Some(k) => (after[..k].trim_end(), Some(after[k..].trim())),
        None => (after, None),
    };

    let mut cols = Vec::new();
    let mut vals = Vec::new();
    for part in split_top_level(assigns, ',') {
        let eqs = split_top_level(&part, '=');
        if eqs.len() < 2 {
            return None; // not a clean `col = expr`
        }
        let col = eqs[0].trim();
        if col.is_empty() {
            return None;
        }
        let val = part[eqs[0].len() + 1..].trim(); // everything after the first '='
        if val.is_empty() {
            return None;
        }
        cols.push(col.to_string());
        vals.push(val.to_string());
    }
    if cols.is_empty() {
        return None;
    }

    let mut out = format!(
        "{prefix} ({}) VALUES ({})",
        cols.join(", "),
        vals.join(", ")
    );
    if let Some(sfx) = suffix {
        out.push(' ');
        out.push_str(sfx);
    }
    Some(out)
}

/// Rewrite MySQL's comma-style multi-table `UPDATE t1, t2 SET ... WHERE ...`
/// into `UPDATE t1 CROSS JOIN t2 SET ... WHERE ...`, which the parser and the
/// join-UPDATE executor accept (the WHERE supplies the join condition, exactly
/// as in the comma form). Returns None for single-table updates (no top-level
/// comma before SET). Quote/paren/backtick-aware.
fn rewrite_comma_update(sql: &str) -> Option<String> {
    let head = sql.trim_start();
    if !head
        .as_bytes()
        .get(..6)
        .is_some_and(|b| b.eq_ignore_ascii_case(b"UPDATE"))
    {
        return None;
    }
    let bytes = sql.as_bytes();
    let update_end = sql.len() - head.len() + 6; // byte just after "UPDATE"
    let (mut in_s, mut in_d, mut in_b) = (false, false, false);
    let mut depth = 0i32;
    let mut i = update_end;
    let mut set_pos: Option<usize> = None;
    let mut comma_positions: Vec<usize> = Vec::new();
    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_s {
            if c == '\'' {
                in_s = false;
            }
        } else if in_d {
            if c == '"' {
                in_d = false;
            }
        } else if in_b {
            if c == '`' {
                in_b = false;
            }
        } else {
            match c {
                '\'' => in_s = true,
                '"' => in_d = true,
                '`' => in_b = true,
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => comma_positions.push(i),
                _ if depth == 0 && kw_at(bytes, i, "SET") => {
                    set_pos = Some(i);
                    break;
                }
                _ => {}
            }
        }
        i += 1;
    }
    let set_pos = set_pos?;
    // Only commas in the table-list region (before SET) matter.
    let list_commas: Vec<usize> = comma_positions
        .into_iter()
        .filter(|&p| p < set_pos)
        .collect();
    if list_commas.is_empty() {
        return None; // single-table UPDATE
    }
    let mut out = String::with_capacity(sql.len() + list_commas.len() * 11);
    let mut prev = 0usize;
    let sb = sql.as_bytes();
    for p in list_commas {
        out.push_str(sql[prev..p].trim_end());
        out.push_str(" CROSS JOIN ");
        // Skip the comma and any whitespace that followed it.
        prev = p + 1;
        while prev < sb.len() && (sb[prev] == b' ' || sb[prev] == b'\t') {
            prev += 1;
        }
    }
    out.push_str(&sql[prev..]);
    Some(out)
}

fn query_has_from(q: &sqlparser::ast::Query) -> bool {
    // Route anything the full engine must handle: SELECTs with a FROM, set
    // operations (UNION/INTERSECT/EXCEPT), CTEs, and nested queries. Only bare
    // literal selects (`SELECT 1`) fall through to the lightweight evaluator.
    if q.with.is_some() {
        return true;
    }
    match q.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => {
            // A FROM-less SELECT still needs the full engine when its projection
            // or WHERE contains a subquery (scalar / EXISTS / IN), or when it
            // has row-filtering/paging clauses, which the lightweight literal
            // evaluator does not apply.
            !s.from.is_empty()
                || s.selection.is_some()
                || q.limit.is_some()
                || q.offset.is_some()
                || select_has_subquery(s)
        }
        _ => true,
    }
}

/// Whether a SELECT's projection or WHERE contains a subquery expression.
fn select_has_subquery(s: &sqlparser::ast::Select) -> bool {
    use sqlparser::ast::SelectItem;
    let proj = s.projection.iter().any(|it| match it {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            expr_has_subquery(e)
        }
        _ => false,
    });
    proj || s.selection.as_ref().is_some_and(expr_has_subquery)
}

fn expr_has_subquery(e: &sqlparser::ast::Expr) -> bool {
    use sqlparser::ast::Expr;
    match e {
        Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => true,
        Expr::Nested(x)
        | Expr::UnaryOp { expr: x, .. }
        | Expr::Cast { expr: x, .. }
        | Expr::IsNull(x)
        | Expr::IsNotNull(x) => expr_has_subquery(x),
        Expr::BinaryOp { left, right, .. } => expr_has_subquery(left) || expr_has_subquery(right),
        Expr::Between {
            expr, low, high, ..
        } => expr_has_subquery(expr) || expr_has_subquery(low) || expr_has_subquery(high),
        _ => false,
    }
}

#[cfg(test)]
mod insert_set_tests {
    use super::rewrite_insert_set;

    #[test]
    fn basic() {
        assert_eq!(
            rewrite_insert_set("INSERT INTO t SET a = 1, b = 2").unwrap(),
            "INSERT INTO t (a, b) VALUES (1, 2)"
        );
    }

    #[test]
    fn normal_insert_is_left_alone() {
        assert!(rewrite_insert_set("INSERT INTO t (a, b) VALUES (1, 2)").is_none());
        assert!(rewrite_insert_set("INSERT INTO t VALUES (1, 2)").is_none());
        assert!(rewrite_insert_set("INSERT INTO t SELECT * FROM u").is_none());
        assert!(rewrite_insert_set("SELECT 1").is_none());
    }

    #[test]
    fn commas_inside_strings_and_calls() {
        // comma inside a string literal must not split the assignment list
        assert_eq!(
            rewrite_insert_set("INSERT INTO t SET a = 'x,y', b = CONCAT('p','q')").unwrap(),
            "INSERT INTO t (a, b) VALUES ('x,y', CONCAT('p','q'))"
        );
    }

    #[test]
    fn ignore_and_backticks() {
        assert_eq!(
            rewrite_insert_set("INSERT IGNORE INTO `tbl` SET `col` = 5").unwrap(),
            "INSERT IGNORE INTO `tbl` (`col`) VALUES (5)"
        );
    }

    #[test]
    fn on_duplicate_key_update_is_preserved() {
        assert_eq!(
            rewrite_insert_set("INSERT INTO t SET a = 1, b = 2 ON DUPLICATE KEY UPDATE b = b + 1")
                .unwrap(),
            "INSERT INTO t (a, b) VALUES (1, 2) ON DUPLICATE KEY UPDATE b = b + 1"
        );
    }

    #[test]
    fn subquery_value() {
        // a top-level SELECT lives inside parens, so it is not mistaken for
        // `INSERT ... SELECT`, and its inner comma does not split assignments
        assert_eq!(
            rewrite_insert_set("INSERT INTO t SET a = (SELECT MAX(id) FROM u), b = 1").unwrap(),
            "INSERT INTO t (a, b) VALUES ((SELECT MAX(id) FROM u), 1)"
        );
    }
}

#[cfg(test)]
mod complexity_guard_tests {
    use super::guard_sql_complexity;

    #[test]
    fn rejects_deep_flat_chains() {
        // Every shape that builds an O(N)-deep AST must be rejected before parse,
        // whichever way it deepens: infix chains, boolean chains, unary chains,
        // JSON `->`/`->>` chains, token-balanced postfix subscript/call chains,
        // and grouping/function nesting.
        let cases = [
            format!("SELECT 1{}", "+1".repeat(40000)), // arithmetic
            format!("SELECT * FROM t WHERE {}", vec!["id=1"; 40000].join(" OR ")), // OR
            format!("SELECT {}1", "NOT ".repeat(40000)), // unary
            format!("SELECT 1 {}", "| 1 ".repeat(40000)), // bitwise
            format!("SELECT '{{}}' {}", "-> '$' ".repeat(40000)), // JSON arrow
            format!("SELECT '{{}}' {}", "->> '$' ".repeat(40000)), // JSON longarrow
            format!("SELECT x{}", "[0]".repeat(40000)), // subscript chain
            format!("SELECT f{}", "()".repeat(40000)), // call chain
            format!("SELECT {}1{}", "(".repeat(40000), ")".repeat(40000)), // parens
            format!("SELECT {}1{}", "ABS(".repeat(40000), ")".repeat(40000)), // func nest
        ];
        for c in &cases {
            assert!(
                guard_sql_complexity(c).is_err(),
                "expected rejection for a deep chain: {}...",
                &c[..30.min(c.len())]
            );
        }
    }

    #[test]
    fn accepts_legit_wide_but_shallow_queries() {
        // A long IN list is a flat Vec (shallow), not a nested chain.
        let in_list = format!(
            "SELECT * FROM t WHERE id IN ({})",
            (0..6000)
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        guard_sql_complexity(&in_list).unwrap();
        // A big multi-row INSERT with arithmetic in each value stays shallow
        // because commas and parens reset the per-level chain.
        let rows = (0..5000)
            .map(|i| format!("({i},{i}+{i})"))
            .collect::<Vec<_>>()
            .join(",");
        guard_sql_complexity(&format!("INSERT INTO t VALUES {rows}")).unwrap();
        // A moderate chain under the limit is fine.
        guard_sql_complexity(&format!("SELECT 1{}", "+1".repeat(500))).unwrap();
        // A batch of many shallow statements must not be summed into a rejection.
        let batch = "SELECT 1+1; ".repeat(3000);
        guard_sql_complexity(&batch).unwrap();
        // A few JSON arrows / subscripts are fine.
        guard_sql_complexity("SELECT a->'$.x'->'$.y', b[0][1] FROM t").unwrap();
    }
}

#[cfg(test)]
mod comma_update_tests {
    use super::rewrite_comma_update;

    #[test]
    fn two_tables() {
        assert_eq!(
            rewrite_comma_update("UPDATE a, b SET a.v = b.w WHERE a.id = b.id").unwrap(),
            "UPDATE a CROSS JOIN b SET a.v = b.w WHERE a.id = b.id"
        );
    }

    #[test]
    fn single_table_untouched() {
        assert!(rewrite_comma_update("UPDATE t SET v = 1 WHERE id = 2").is_none());
        // comma is in the SET list, not the table list
        assert!(rewrite_comma_update("UPDATE t SET a = 1, b = 2").is_none());
    }

    #[test]
    fn aliases_and_three_tables() {
        assert_eq!(
            rewrite_comma_update("UPDATE a x, b y, c z SET x.v = y.w WHERE x.id = z.id").unwrap(),
            "UPDATE a x CROSS JOIN b y CROSS JOIN c z SET x.v = y.w WHERE x.id = z.id"
        );
    }
}

#[cfg(test)]
mod mysql_ddl_compat_tests {
    use super::{
        mysql_show_index_table, parse_mysql_rename, require_privilege,
        rewrite_alter_column_collations, strip_create_table_options, strip_index_prefix_lengths,
        Engine, MysqlRename, PrivilegedAction,
    };
    use crate::catalog;
    use elyra_core::Privilege;
    use elyra_storage::Db;
    use sqlparser::ast::{Statement, TableConstraint};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn table_option_stripping_never_touches_a_ctas() {
        // The first paren of a CTAS belongs to the query, not to a column list.
        for sql in [
            "CREATE TABLE c AS SELECT g, COUNT(*) AS c FROM u GROUP BY g",
            "CREATE TABLE c AS SELECT COUNT(*) FROM u",
            "CREATE TABLE c AS SELECT * FROM (SELECT 1) x",
            "CREATE TABLE c AS SELECT CONCAT(a, b) FROM u ORDER BY 1",
            "CREATE TABLE c LIKE other",
        ] {
            assert_eq!(strip_create_table_options(sql), None, "{sql}");
        }
    }

    #[test]
    fn table_option_stripping_still_trims_real_options() {
        assert_eq!(
            strip_create_table_options(
                "CREATE TABLE t (id INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
            .as_deref(),
            Some("CREATE TABLE t (id INT)")
        );
        assert_eq!(
            strip_create_table_options("CREATE TABLE `as_of` (id INT) ROW_FORMAT=DYNAMIC")
                .as_deref(),
            Some("CREATE TABLE `as_of` (id INT)")
        );
    }

    #[test]
    fn strips_only_inline_index_prefix_lengths() {
        let sql = "CREATE TABLE `prefixes` (\
            `id` INT,\
            `select` VARCHAR(255),\
            `body` VARCHAR(255),\
            PRIMARY KEY (`id`(8)),\
            UNIQUE KEY `uniq_select` (`select` (191)),\
            KEY `body prefix` (`body`(16), `select`( 12 ))\
        )";
        let rewritten = strip_index_prefix_lengths(sql).unwrap();
        let statements = Parser::parse_sql(&MySqlDialect {}, &rewritten).unwrap();
        let [Statement::CreateTable(table)] = statements.as_slice() else {
            panic!("expected CREATE TABLE");
        };

        assert!(matches!(
            &table.constraints[0],
            TableConstraint::PrimaryKey { columns, .. }
                if columns.iter().map(|column| column.value.as_str()).eq(["id"])
        ));
        assert!(matches!(
            &table.constraints[1],
            TableConstraint::Unique {
                index_name: Some(name),
                columns,
                ..
            } if name.value == "uniq_select"
                && columns.iter().map(|column| column.value.as_str()).eq(["select"])
        ));
        assert!(matches!(
            &table.constraints[2],
            TableConstraint::Index {
                name: Some(name),
                columns,
                ..
            } if name.value == "body prefix"
                && columns.iter().map(|column| column.value.as_str()).eq(["body", "select"])
        ));

        assert!(strip_index_prefix_lengths(
            "CREATE TABLE widths (name VARCHAR(191), CHECK (length(name) > 1))"
        )
        .is_none());
        assert!(
            strip_index_prefix_lengths("CREATE TABLE copy AS SELECT name(191) FROM widths")
                .is_none()
        );
    }

    #[tokio::test]
    async fn creates_catalog_indexes_from_prefixed_key_parts() {
        let engine = Engine::new(Db::in_memory().unwrap());
        let session = engine.session();
        engine
            .execute(
                "CREATE TABLE `prefixes` (\
                    `id` INT,\
                    `select` VARCHAR(255),\
                    `body` VARCHAR(255),\
                    PRIMARY KEY (`id`(8)),\
                    UNIQUE KEY `uniq_select` (`select`(191)),\
                    KEY `body prefix` (`body`(16), `select`(12))\
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();

        let definition = catalog::load(&session, "prefixes").await.unwrap();
        assert_eq!(definition.pk_cols, vec![0]);
        assert_eq!(
            definition
                .indexes
                .iter()
                .map(|index| (index.name.as_str(), index.cols.as_slice(), index.unique))
                .collect::<Vec<_>>(),
            [
                ("uniq_select", &[1][..], true),
                ("body prefix", &[2, 1][..], false),
            ]
        );
    }

    #[test]
    fn rewrites_only_alter_change_and_modify_collations() {
        assert_eq!(
            rewrite_alter_column_collations(
                "ALTER TABLE t CHANGE a b TEXT COLLATE 'utf8mb4_0900_ai_ci'"
            )
            .as_deref(),
            Some("ALTER TABLE t CHANGE a b TEXT CHARACTER SET 'utf8mb4_0900_ai_ci'")
        );
        assert_eq!(
            rewrite_alter_column_collations(
                "ALTER TABLE t MODIFY a TEXT COLLATE utf8mb4_bin, \
                 CHANGE b c TEXT COLLATE utf8mb4_0900_ai_ci"
            )
            .as_deref(),
            Some(
                "ALTER TABLE t MODIFY a TEXT CHARACTER SET utf8mb4_bin, \
                 CHANGE b c TEXT CHARACTER SET utf8mb4_0900_ai_ci"
            )
        );
        assert!(
            rewrite_alter_column_collations("CREATE TABLE t (a TEXT COLLATE utf8mb4_bin)")
                .is_none()
        );
    }

    #[test]
    fn parses_mysql_rename_forms() {
        match parse_mysql_rename("RENAME TABLE `old_table` TO `new_table`") {
            Some(MysqlRename::Table { old, new }) => {
                assert_eq!(old.to_string(), "old_table");
                assert_eq!(new.to_string(), "new_table");
            }
            _ => panic!("expected a table rename"),
        }
        match parse_mysql_rename("ALTER TABLE db.t RENAME INDEX `old_i` TO `new_i`") {
            Some(MysqlRename::Index { table, old, new }) => {
                assert_eq!(table.to_string(), "db.t");
                assert_eq!(old, "old_i");
                assert_eq!(new, "new_i");
            }
            _ => panic!("expected an index rename"),
        }
    }

    #[test]
    fn parses_mysql_show_index_qualifiers() {
        assert_eq!(
            mysql_show_index_table("SHOW INDEX FROM `tenant`.`victim` WHERE Key_name = 'idx'")
                .unwrap()
                .to_string(),
            "tenant.victim"
        );
        assert_eq!(
            mysql_show_index_table("SHOW KEYS FROM `victim` IN `tenant`")
                .unwrap()
                .to_string(),
            "tenant.victim"
        );
        assert_eq!(
            mysql_show_index_table("SHOW INDEX FROM `where`")
                .unwrap()
                .to_string(),
            "where"
        );
    }

    #[test]
    fn shared_privilege_gate_enforces_the_required_tier() {
        assert!(require_privilege(Privilege::Admin, PrivilegedAction::AlterTable).is_ok());
        assert!(require_privilege(Privilege::Write, PrivilegedAction::AlterTable).is_err());
        assert!(require_privilege(Privilege::Read, PrivilegedAction::LockTables).is_err());
        assert!(require_privilege(
            Privilege::Write,
            PrivilegedAction::Statement(Privilege::Write)
        )
        .is_ok());
    }
}

#[cfg(test)]
mod case_insensitive_search_tests {
    use super::{contains_ci, replace_ci, rewrite_odd_0x_literals};

    #[test]
    fn finds_ascii_needles_without_copying_unicode_haystacks() {
        assert!(contains_ci(
            "SELECT 'å🚗' LOCK In ShArE MoDe",
            "lock in share mode"
        ));
        assert!(!contains_ci("SELECT 'lock 🚗 mode'", "lock in share mode"));
    }

    #[test]
    fn replaces_all_ascii_case_variants_without_damaging_unicode() {
        assert_eq!(
            replace_ci(
                "SELECT 'å🚗' LOCK IN SHARE MODE lock in share mode",
                "lock in share mode",
                "for share"
            ),
            "SELECT 'å🚗' for share for share"
        );
    }

    #[test]
    fn pads_only_unquoted_odd_length_0x_literals() {
        assert_eq!(
            rewrite_odd_0x_literals(
                r#"SELECT 0xF, 0Xabc, 0xAB, X'F', '0xF', "0xF", `0xF`, value0xF"#
            )
            .as_deref(),
            Some(r#"SELECT 0x0F, 0X0abc, 0xAB, X'F', '0xF', "0xF", `0xF`, value0xF"#)
        );
    }
}

#[cfg(test)]
mod session_set_tests {
    use super::{Engine, Privilege, QueryResult, Session};
    use elyra_core::Value;

    async fn execute(engine: &Engine, session: &Session, sql: &str) {
        engine
            .execute(sql, Privilege::Admin, session)
            .await
            .unwrap();
    }

    async fn rows(engine: &Engine, session: &Session, sql: &str) -> Vec<Vec<Value>> {
        let mut results = engine
            .execute(sql, Privilege::Admin, session)
            .await
            .unwrap();
        assert_eq!(results.len(), 1, "expected one result for {sql}");
        let QueryResult::Rows(mut stream) = results.remove(0) else {
            panic!("expected rows for {sql}");
        };
        let mut out = Vec::new();
        loop {
            let batch = stream.next_batch(1024).await.unwrap();
            if batch.is_empty() {
                return out;
            }
            out.extend(batch);
        }
    }

    #[tokio::test]
    async fn session_set_applies_and_reports_the_supported_variables() {
        let engine = Engine::new(elyra_storage::Db::in_memory().unwrap());
        let session = engine.session();

        execute(
            &engine,
            &session,
            "SET @@session.sql_mode = 'ANSI_QUOTES,NO_AUTO_VALUE_ON_ZERO'",
        )
        .await;
        assert_eq!(
            rows(&engine, &session, "SELECT @@session.sql_mode, @@sql_mode",).await,
            vec![vec![
                Value::Text("ANSI_QUOTES,NO_AUTO_VALUE_ON_ZERO".into()),
                Value::Text("ANSI_QUOTES,NO_AUTO_VALUE_ON_ZERO".into()),
            ]]
        );

        execute(
            &engine,
            &session,
            "CREATE TABLE session_set_auto (id INT AUTO_INCREMENT PRIMARY KEY)",
        )
        .await;
        execute(&engine, &session, "INSERT INTO session_set_auto VALUES (0)").await;
        assert_eq!(
            rows(&engine, &session, "SELECT \"id\" FROM \"session_set_auto\"",).await,
            vec![vec![Value::Int(0)]]
        );

        execute(
            &engine,
            &session,
            "CREATE TABLE session_set_parent (id INT PRIMARY KEY)",
        )
        .await;
        execute(
            &engine,
            &session,
            "CREATE TABLE session_set_child (id INT PRIMARY KEY, parent_id INT, \
             FOREIGN KEY (parent_id) REFERENCES session_set_parent(id))",
        )
        .await;
        execute(&engine, &session, "SET FOREIGN_KEY_CHECKS = 0").await;
        execute(
            &engine,
            &session,
            "INSERT INTO session_set_child VALUES (1, 99)",
        )
        .await;
        assert_eq!(
            rows(&engine, &session, "SELECT @@foreign_key_checks").await,
            vec![vec![Value::Int(0)]]
        );

        execute(
            &engine,
            &session,
            "CREATE TABLE session_set_concat (id INT PRIMARY KEY, value VARCHAR(4))",
        )
        .await;
        execute(
            &engine,
            &session,
            "INSERT INTO session_set_concat VALUES (1, 'ab'), (2, 'cd'), (3, 'ef')",
        )
        .await;
        execute(&engine, &session, "SET group_concat_max_len = 5").await;
        assert_eq!(
            rows(
                &engine,
                &session,
                "SELECT GROUP_CONCAT(value) FROM session_set_concat",
            )
            .await,
            vec![vec![Value::Text("ab,cd".into())]]
        );
        assert_eq!(
            rows(&engine, &session, "SELECT @@group_concat_max_len").await,
            vec![vec![Value::Int(5)]]
        );

        execute(
            &engine,
            &session,
            "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        )
        .await;
        assert_eq!(
            rows(&engine, &session, "SELECT @@transaction_isolation").await,
            vec![vec![Value::Text("SERIALIZABLE".into())]]
        );
        execute(
            &engine,
            &session,
            "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
        )
        .await;
        assert_eq!(
            rows(&engine, &session, "SELECT @@transaction_isolation").await,
            vec![vec![Value::Text("READ-COMMITTED".into())]]
        );
        assert_eq!(
            rows(&engine, &session, "SHOW VARIABLES LIKE 'sql_mode'").await,
            vec![vec![
                Value::Text("sql_mode".into()),
                Value::Text("ANSI_QUOTES,NO_AUTO_VALUE_ON_ZERO".into()),
            ]]
        );
    }

    #[tokio::test]
    async fn autocommit_zero_buffers_writes_until_commit_or_disconnect() {
        let engine = Engine::new(elyra_storage::Db::in_memory().unwrap());
        let setup = engine.session();
        execute(
            &engine,
            &setup,
            "CREATE TABLE session_set_txn (id INT PRIMARY KEY)",
        )
        .await;

        let writer = engine.session();
        let reader = engine.session();
        execute(&engine, &writer, "SET @@session.autocommit = 0").await;
        assert_eq!(
            rows(&engine, &writer, "SELECT @@autocommit").await,
            vec![vec![Value::Int(0)]]
        );
        assert_eq!(
            rows(&engine, &writer, "SELECT @@global.autocommit").await,
            vec![vec![Value::Int(1)]]
        );
        execute(&engine, &writer, "INSERT INTO session_set_txn VALUES (1)").await;
        assert_eq!(
            rows(&engine, &reader, "SELECT COUNT(*) FROM session_set_txn").await,
            vec![vec![Value::Int(0)]]
        );
        execute(&engine, &writer, "COMMIT").await;
        assert_eq!(
            rows(&engine, &reader, "SELECT COUNT(*) FROM session_set_txn").await,
            vec![vec![Value::Int(1)]]
        );

        execute(&engine, &writer, "INSERT INTO session_set_txn VALUES (2)").await;
        assert_eq!(
            rows(&engine, &reader, "SELECT COUNT(*) FROM session_set_txn").await,
            vec![vec![Value::Int(1)]]
        );
        execute(&engine, &writer, "SET autocommit = 1").await;
        assert_eq!(
            rows(&engine, &reader, "SELECT COUNT(*) FROM session_set_txn").await,
            vec![vec![Value::Int(2)]]
        );

        let abandoned = engine.session();
        execute(&engine, &abandoned, "SET autocommit = 0").await;
        execute(
            &engine,
            &abandoned,
            "INSERT INTO session_set_txn VALUES (3)",
        )
        .await;
        drop(abandoned);
        assert_eq!(
            rows(&engine, &reader, "SELECT COUNT(*) FROM session_set_txn").await,
            vec![vec![Value::Int(2)]]
        );
    }
}

#[cfg(test)]
mod fuzz_props {
    use super::{
        parse_dml_limit, parse_mysql_rename, parse_update_modifiers,
        rewrite_alter_column_collations, rewrite_comma_update, rewrite_insert_set,
        rewrite_odd_0x_literals, split_top_level, strip_create_table_options,
        strip_index_prefix_lengths,
    };
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4000))]

        /// The SQL preprocessing rewriters and splitters must never panic on any
        /// input -- including multi-byte UTF-8, unbalanced quotes/parens, and
        /// arbitrary control characters. (Byte-offset slicing is the classic
        /// hazard here.)
        #[test]
        fn preprocess_never_panics(s in "(?s).{0,120}") {
            let _ = rewrite_insert_set(&s);
            let _ = rewrite_comma_update(&s);
            let _ = strip_create_table_options(&s);
            let _ = strip_index_prefix_lengths(&s);
            let _ = parse_dml_limit(&s);
            let _ = parse_update_modifiers(&s);
            let _ = parse_mysql_rename(&s);
            let _ = rewrite_alter_column_collations(&s);
            let _ = rewrite_odd_0x_literals(&s);
            let _ = split_top_level(&s, ',');
            let _ = split_top_level(&s, '=');
        }

        /// Bias toward the shapes the rewriters actually rewrite so the code
        /// paths past the prefix check are exercised; still must not panic, and
        /// any produced string must be valid (char-boundary safe by construction).
        #[test]
        fn targeted_shapes_never_panic(a in "(?s).{0,60}") {
            let _ = rewrite_insert_set(&format!("INSERT INTO t SET {a}"));
            let _ = rewrite_insert_set(&format!("INSERT IGNORE INTO `t` SET {a}"));
            let _ = rewrite_comma_update(&format!("UPDATE {a} SET x = 1 WHERE y = 2"));
            let _ = strip_create_table_options(&format!("CREATE TABLE t (id INT) {a}"));
            let _ = strip_index_prefix_lengths(&format!("CREATE TABLE t (KEY i (a(191))) {a}"));
            let _ = rewrite_alter_column_collations(&format!(
                "ALTER TABLE t CHANGE a b TEXT {a}"
            ));
        }

        /// split_top_level round-trips: joining the parts with the separator
        /// reproduces the input exactly (no bytes lost or added).
        #[test]
        fn split_top_level_roundtrips(s in "(?s).{0,80}") {
            let parts = split_top_level(&s, ',');
            prop_assert_eq!(parts.join(","), s);
        }

        /// The full preprocessing + parse entry point (also driven by cargo-fuzz)
        /// never panics on arbitrary input.
        #[test]
        fn fuzz_entry_never_panics(s in "(?s).{0,120}") {
            crate::fuzz_preprocess_parse(&s);
        }

        /// Biased toward the shapes the rewriters/fallbacks target.
        #[test]
        fn fuzz_entry_targeted(a in "(?s).{0,60}") {
            crate::fuzz_preprocess_parse(&format!("INSERT INTO t SET {a}"));
            crate::fuzz_preprocess_parse(&format!("UPDATE {a} SET x=1 WHERE y=2"));
            crate::fuzz_preprocess_parse(&format!("SELECT a FROM t GROUP BY a {a} WITH ROLLUP"));
            crate::fuzz_preprocess_parse(&format!("SELECT {a} << {a}"));
        }
    }
}

#[cfg(test)]
mod parse_dml_limit_utf8 {
    use super::parse_dml_limit;
    #[test]
    fn multibyte_before_limit_does_not_panic() {
        // 'é' is 2 bytes; various offsets before a trailing number must not panic.
        for s in [
            "UPDATE t SET x=é 5",
            "DELETE FROM té 9",
            "UPDATE é limité 3",
            "UPDATE tμλ 12",
        ] {
            let _ = parse_dml_limit(s);
        }
        // Sanity: a real LIMIT retains both the base statement and row bound.
        assert_eq!(
            parse_dml_limit("UPDATE t SET x=1 LIMIT 5")
                .map(|parsed| (parsed.base_sql, parsed.limit)),
            Some(("UPDATE t SET x=1".into(), 5))
        );
    }
}
