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
                        if n % 1024 == 0 {
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
                        if n % 1024 == 0 {
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
                        if n % 1024 == 0 {
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
            let granted: std::collections::HashSet<String> = granted
                .into_iter()
                .map(|c| c.to_ascii_lowercase())
                .collect();
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
                    refs.push(c.name.to_ascii_lowercase());
                }
            }
            if !ok {
                return Err(Error::Query(format!(
                    "access denied: query on column-restricted table '{t}' uses an \
                     expression that cannot be verified"
                )));
            }
            for r in &refs {
                if !granted.contains(r) {
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
            let name = rest[..as_pos].trim().trim_matches(['`', '"']).to_string();
            let query = rest[as_pos + 4..].trim().trim_end_matches(';').to_string();
            if name.is_empty() || query.is_empty() {
                return Err(Error::Parse(
                    "CREATE MATERIALIZED VIEW: name and query required".into(),
                ));
            }
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
            let name = rest
                .trim()
                .trim_end_matches(';')
                .trim_matches(['`', '"'])
                .to_string();
            self.refresh_matview(&name, privilege, user, sess).await?;
            return Ok(vec![QueryResult::empty_ok()]);
        }

        // DROP [IF EXISTS] <name>
        let mut name = rest.trim().trim_end_matches(';');
        if let Some(stripped) = name.to_ascii_lowercase().strip_prefix("if exists") {
            let cut = name.len() - stripped.len();
            name = name[cut..].trim();
        }
        let name = name.trim_matches(['`', '"']).to_string();
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

    /// Before reading, auto-refresh any stale materialized view the query reads.
    async fn auto_refresh_matviews(
        &self,
        stmt: &Statement,
        privilege: Privilege,
        user: &str,
        sess: &Session,
    ) -> Result<()> {
        use sqlparser::ast::SetExpr;
        let Statement::Query(q) = stmt else {
            return Ok(());
        };
        let SetExpr::Select(select) = q.body.as_ref() else {
            return Ok(());
        };
        for twj in &select.from {
            for factor in
                std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation))
            {
                if let sqlparser::ast::TableFactor::Table { name, .. } = factor {
                    if let Some(t) = object_name_last(name) {
                        if sess.get(catalog::matview_key(&t)).await?.is_some()
                            && exec::matview_is_stale(sess, &t).await?
                        {
                            self.refresh_matview(&t, privilege, user, sess).await?;
                        }
                    }
                }
            }
        }
        Ok(())
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
        // Resolve the single base table (for `*` expansion and column typing),
        // if the FROM is exactly one plain table. Otherwise `def` is None and we
        // still describe explicit projection items by best-effort type -- what
        // matters for a prepared statement is that the *column count* is exact,
        // so drivers (PDO/mysqlnd) read the result set instead of desyncing.
        let def = if sel.from.len() == 1 && sel.from[0].joins.is_empty() {
            if let TableFactor::Table { name, .. } = &sel.from[0].relation {
                catalog::load(sess, &name.0.last()?.value).await.ok()
            } else {
                None
            }
        } else {
            None
        };
        let col_by_name = |n: &str| -> Option<ColumnType> {
            def.as_ref().and_then(|d| {
                d.schema
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(n))
                    .map(|c| c.ty.clone())
            })
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
                // Wildcards require a resolvable single table to know the count.
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                    let d = def.as_ref()?;
                    out.extend(d.schema.columns.iter().cloned());
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
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    out.push(elyra_core::ColumnDef {
                        name: alias.value.clone(),
                        ty: expr_type(expr, &col_by_name),
                        nullable: true,
                        collation: elyra_core::Collation::Ci,
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

        // SHOW FUNCTION/PROCEDURE STATUS [WHERE ...] — the WHERE form doesn't
        // parse, so intercept here and return an empty routines listing.
        if head.starts_with("show function status") || head.starts_with("show procedure status") {
            return Ok(vec![exec::show_routine_status()?]);
        }

        // SHOW [FULL] PROCESSLIST — handled in-engine so it works over the
        // prepared-statement path too (SHOW FULL PROCESSLIST also fails to parse).
        if head.starts_with("show processlist") || head.starts_with("show full processlist") {
            return Ok(vec![exec::show_processlist()?]);
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
                Box::pin(self.execute_as(&base, privilege, user, sess)).await?;
                // Table name follows CREATE TABLE [IF NOT EXISTS].
                let table = exec::create_table_name(&base)?;
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
                if privilege < Privilege::Write {
                    return Err(Error::Query(
                        "access denied: ALTER TABLE requires WRITE privilege".into(),
                    ));
                }
                let toks: Vec<&str> = trimmed.split_whitespace().collect();
                let table = toks
                    .get(2)
                    .map(|s| s.trim_matches(['`', '"']))
                    .unwrap_or("");
                let pname = lower
                    .find(kw)
                    .and_then(|p| trimmed[p + kw.len()..].split_whitespace().next())
                    .map(|s| s.trim_matches(['`', '"', ';']))
                    .unwrap_or("");
                let spec = catalog::load_partspec(sess, table)
                    .await?
                    .ok_or_else(|| Error::Catalog(format!("table '{table}' is not partitioned")))?;
                let where_ = exec::partition_where(&spec, pname).ok_or_else(|| {
                    Error::Query(format!("cannot drop partition '{pname}' (unknown or HASH)"))
                })?;
                let del = format!("DELETE FROM `{table}` WHERE {where_}");
                let r = Box::pin(self.execute_as(&del, privilege, user, sess)).await?;
                if drop_meta {
                    let mut spec2 = spec;
                    spec2.parts.retain(|p| !p.name.eq_ignore_ascii_case(pname));
                    let enc =
                        bincode::serialize(&spec2).map_err(|e| Error::Storage(e.to_string()))?;
                    sess.commit_write(vec![(catalog::partmeta_key(table), enc)], vec![])
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
            if privilege < Privilege::Write {
                return Err(Error::Query(
                    "access denied: materialized views require WRITE privilege".into(),
                ));
            }
            return self.materialized_view(trimmed, privilege, user, sess).await;
        }

        // LOAD DATA INFILE '<server-side path>' INTO TABLE t ... — reads a file on
        // the server and bulk-inserts it (requires ADMIN, like MySQL's FILE priv).
        if head.starts_with("load data") {
            if privilege < Privilege::Admin {
                return Err(Error::Query(
                    "access denied: LOAD DATA INFILE requires ADMIN privilege".into(),
                ));
            }
            let spec = exec::parse_load_data(trimmed)?;
            let content = tokio::fs::read_to_string(&spec.path).await.map_err(|e| {
                Error::Query(format!("LOAD DATA: cannot read '{}': {e}", spec.path))
            })?;
            let stmts = exec::build_load_inserts(&spec, &content, 1000);
            let mut total = 0u64;
            for stmt in stmts {
                for r in Box::pin(self.execute_as(&stmt, privilege, user, sess)).await? {
                    if let QueryResult::Affected(n) = r {
                        total += n;
                    }
                }
            }
            return Ok(vec![QueryResult::Affected(total)]);
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
        // Strip trailing MySQL table options (ENGINE=, DEFAULT CHARSET/CHARACTER
        // SET, COLLATE, AUTO_INCREMENT, ROW_FORMAT, COMMENT, ...) from CREATE
        // TABLE, which the parser does not accept in all their spellings. They
        // are no-ops here (single storage engine, utf8mb4). This makes Laravel,
        // mysqldump and ORM-emitted DDL parse.
        if let Some(stripped) = strip_create_table_options(&subst_sql) {
            subst_sql = stripped;
        }
        // Strip a trailing `LIMIT n` from UPDATE/DELETE (not parsed; MySQL's
        // UPDATE/DELETE ... LIMIT without ORDER BY is non-deterministic anyway,
        // and drivers like Laravel use it only for a unique-key single row).
        if let Some(stripped) = strip_dml_limit(&subst_sql) {
            subst_sql = stripped;
        }
        // Rewrite MySQL's `INSERT ... SET col = val, ...` shorthand into the
        // standard `INSERT ... (cols) VALUES (...)` the parser accepts.
        if let Some(rewritten) = rewrite_insert_set(&subst_sql) {
            subst_sql = rewritten;
        }
        // Rewrite comma-style multi-table `UPDATE t1, t2 SET ... WHERE ...` into
        // `UPDATE t1 CROSS JOIN t2 SET ... WHERE ...` (the WHERE supplies the
        // join condition, as in the comma form).
        if let Some(rewritten) = rewrite_comma_update(&subst_sql) {
            subst_sql = rewritten;
        }
        let statements =
            Parser::parse_sql(&dialect, &subst_sql).map_err(|e| Error::Parse(e.to_string()))?;

        let mut out = Vec::with_capacity(statements.len());
        for stmt in statements {
            // Resolve ai_embed('...') calls (embed once, substitute a vector
            // literal) before anything inspects the statement.
            let mut stmt = stmt;
            aiembed::resolve_stmt(&mut stmt).await?;
            // Resolve LAST_INSERT_ID()/ROW_COUNT()/FOUND_ROWS() from session
            // state before execution (stateless evaluator can't see it).
            sessfn::rewrite(&mut stmt, sess.last_insert_id(), sess.row_count());
            let need = required_privilege(&stmt);
            let effective = self
                .effective_privilege(privilege, user, &stmt, sess)
                .await?;
            if effective < need {
                return Err(Error::Query(format!(
                    "access denied: statement requires {need:?} privilege"
                )));
            }
            // Auto-refresh any stale materialized view this statement reads.
            // Skipped entirely when the database has no materialized views
            // (avoids a per-query catalog read on the common path).
            if catalog::matviews_exist(sess).await {
                self.auto_refresh_matviews(&stmt, privilege, user, sess)
                    .await?;
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
            let r = self.execute_stmt(stmt, sess).await?;
            // Track ROW_COUNT(): rows changed by DML, or -1 after a result set
            // (matches MySQL).
            match &r {
                QueryResult::Affected(n) => sess.set_row_count(*n as i64),
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
            // ElyraSQL is a single logical schema (one file); CREATE/DROP
            // DATABASE|SCHEMA are accepted as no-ops so tools and migrations that
            // issue them proceed.
            Statement::CreateDatabase { .. } | Statement::CreateSchema { .. } => {
                Ok(QueryResult::empty_ok())
            }
            Statement::Explain { statement, .. } => exec::explain(sess, &statement).await,
            Statement::Drop {
                object_type:
                    sqlparser::ast::ObjectType::Database | sqlparser::ast::ObjectType::Schema,
                ..
            } => Ok(QueryResult::empty_ok()),
            // Session/introspection queries GUI tools and ORMs fire on connect.
            Statement::ShowVariables { filter, .. } => exec::show_variables(filter.as_ref()),
            Statement::ShowStatus { filter, .. } => exec::show_status(filter.as_ref()),
            Statement::ShowCollation { filter } => exec::show_collation(filter.as_ref()),
            Statement::ShowDatabases { .. } => exec::show_databases(),
            Statement::ShowVariable { variable } => {
                let kw = variable
                    .iter()
                    .map(|i| i.value.to_ascii_lowercase())
                    .collect::<Vec<_>>()
                    .join(" ");
                match kw.as_str() {
                    "warnings" | "errors" => exec::show_warnings(),
                    _ if kw.starts_with("table status") => exec::show_table_status(sess).await,
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
                Value::Text("elyra".into()),
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

/// Collect the column names referenced by an expression. Returns `false` if it
/// hits a node it doesn't understand (so the caller can be deny-safe).
fn collect_col_refs(e: &sqlparser::ast::Expr, out: &mut Vec<String>) -> bool {
    use sqlparser::ast::Expr::*;
    match e {
        Identifier(i) => {
            out.push(i.value.to_ascii_lowercase());
            true
        }
        CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                out.push(last.value.to_ascii_lowercase());
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

/// Remove a trailing `LIMIT <n>` from an `UPDATE`/`DELETE` statement, which the
/// parser does not accept. Row-limited UPDATE/DELETE is not enforced (the whole
/// matching set is affected); the WHERE clause is respected as written.
fn strip_dml_limit(sql: &str) -> Option<String> {
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
    if before_num.len() >= 5 && before_num[before_num.len() - 5..].eq_ignore_ascii_case("limit") {
        let kept = before_num[..before_num.len() - 5].trim_end();
        return Some(kept.to_string());
    }
    None
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
    if head.len() < 6 || !head[..6].eq_ignore_ascii_case("INSERT") {
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
                    if rest.len() >= 12 && rest[..12].eq_ignore_ascii_case("ON DUPLICATE") {
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
    if head.len() < 6 || !head[..6].eq_ignore_ascii_case("UPDATE") {
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
            // or WHERE contains a subquery (scalar / EXISTS / IN), which the
            // lightweight literal evaluator cannot resolve.
            !s.from.is_empty() || select_has_subquery(s)
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
