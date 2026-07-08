//! ElyraSQL query engine.
//!
//! Frontend is `sqlparser` with the MySQL dialect. Execution is **async and
//! streaming** end to end, layered on the high-concurrency [`Db`] handle:
//! reads scale across connections, writes group-commit, and result sets are
//! never fully materialised. This is what lets ElyraSQL handle large data
//! under high traffic.

mod catalog;
mod eval;
mod exec;
mod keyenc;
mod predicate;
mod stream;

use elyra_core::{ColumnType, Error, Result, Schema, Value};
use elyra_storage::Db;
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

pub use stream::RowStream;

/// Outcome of a single SQL statement.
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
        }]);
        QueryResult::Rows(RowStream::literal(schema, vec![vec![value]]))
    }
}

/// The ElyraSQL engine. Cheap to clone (shared, concurrent DB handle).
#[derive(Clone)]
pub struct Engine {
    db: Db,
}

impl Engine {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Parse and execute one or more `;`-separated statements.
    pub async fn execute(&self, sql: &str) -> Result<Vec<QueryResult>> {
        if let Some(r) = self.intercept_session(sql) {
            return Ok(vec![r]);
        }

        let dialect = MySqlDialect {};
        let statements =
            Parser::parse_sql(&dialect, sql).map_err(|e| Error::Parse(e.to_string()))?;

        let mut out = Vec::with_capacity(statements.len());
        for stmt in statements {
            out.push(self.execute_stmt(stmt).await?);
        }
        Ok(out)
    }

    async fn execute_stmt(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Query(q) => {
                if query_has_from(&q) {
                    exec::select(&self.db, &q).await
                } else {
                    eval::eval_literal_select(&q)
                }
            }
            Statement::CreateTable(ct) => exec::create_table(&self.db, ct).await,
            Statement::Insert(ins) => exec::insert(&self.db, ins).await,
            Statement::Update { table, assignments, selection, .. } => {
                exec::update(&self.db, &table, &assignments, selection.as_ref()).await
            }
            Statement::Delete(del) => exec::delete(&self.db, &del).await,
            Statement::Drop { object_type, names, if_exists, .. }
                if object_type == sqlparser::ast::ObjectType::Table =>
            {
                let name = names
                    .first()
                    .and_then(|n| n.0.last())
                    .map(|i| i.value.clone())
                    .ok_or_else(|| Error::Catalog("empty table name".into()))?;
                exec::drop_table(&self.db, &name, if_exists).await
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
        let lower = t.to_ascii_lowercase();

        match lower.as_str() {
            "select @@version_comment limit 1" | "select @@version_comment" => Some(
                QueryResult::scalar(
                    "@@version_comment",
                    ColumnType::Text,
                    Value::Text("ElyraSQL — MIT licensed, robust SQL server".into()),
                ),
            ),
            "select @@version" | "select version()" => Some(QueryResult::scalar(
                "version()",
                ColumnType::Text,
                Value::Text(elyra_core::SERVER_VERSION.into()),
            )),
            "select database()" | "select schema()" => {
                Some(QueryResult::scalar("database()", ColumnType::Text, Value::Null))
            }
            _ if lower.starts_with("set ") => Some(QueryResult::empty_ok()),
            _ => None,
        }
    }
}

fn query_has_from(q: &sqlparser::ast::Query) -> bool {
    matches!(q.body.as_ref(), sqlparser::ast::SetExpr::Select(s) if !s.from.is_empty())
}
