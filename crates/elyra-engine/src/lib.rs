//! ElyraSQL query engine.
//!
//! Frontend is `sqlparser` with the MySQL dialect so ElyraSQL speaks the
//! MySQL SQL surface. This scaffold implements a minimal-but-real slice:
//! literal `SELECT`s, arithmetic, and the session/handshake chatter every
//! MySQL client sends on connect (`SET`, `SHOW`, `@@variables`).
//!
//! The heavy engines (transactional executor over [`elyra_storage`],
//! analytics, vector search) plug in behind [`Engine::execute`].

mod eval;

use std::sync::Arc;

use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use elyra_storage::Storage;
use sqlparser::ast::Statement;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

/// Outcome of a single SQL statement.
#[derive(Debug)]
pub enum QueryResult {
    /// A result set with a schema and rows.
    Set { schema: Schema, rows: Vec<Vec<Value>> },
    /// A statement that changed state; carries affected row count.
    Affected(u64),
}

impl QueryResult {
    pub fn empty_ok() -> Self {
        QueryResult::Affected(0)
    }

    /// One column, one row — used for `SELECT 1`, `@@version`, etc.
    pub fn scalar(col: &str, ty: ColumnType, value: Value) -> Self {
        QueryResult::Set {
            schema: Schema::new(vec![ColumnDef { name: col.into(), ty, nullable: true }]),
            rows: vec![vec![value]],
        }
    }
}

/// The ElyraSQL engine. Cheap to clone (shared storage handle).
#[derive(Clone)]
pub struct Engine {
    #[allow(dead_code)]
    storage: Arc<Storage>,
}

impl Engine {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Parse and execute one or more `;`-separated statements.
    pub fn execute(&self, sql: &str) -> Result<Vec<QueryResult>> {
        // Intercept connection-setup commands MySQL clients issue before the
        // full parser gets a chance; keeps handshake robust across clients.
        if let Some(r) = self.intercept_session(sql) {
            return Ok(vec![r]);
        }

        let dialect = MySqlDialect {};
        let statements = Parser::parse_sql(&dialect, sql)
            .map_err(|e| Error::Parse(e.to_string()))?;

        let mut out = Vec::with_capacity(statements.len());
        for stmt in statements {
            out.push(self.execute_stmt(stmt)?);
        }
        Ok(out)
    }

    fn execute_stmt(&self, stmt: Statement) -> Result<QueryResult> {
        match stmt {
            Statement::Query(q) => eval::eval_query(&q),
            Statement::SetVariable { .. } | Statement::Use { .. } => Ok(QueryResult::empty_ok()),
            other => Err(Error::Unsupported(format!(
                "statement not yet implemented in this build: {}",
                stmt_kind(&other)
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
            "select database()" | "select schema()" => Some(QueryResult::scalar(
                "database()",
                ColumnType::Text,
                Value::Null,
            )),
            "select 1" => Some(QueryResult::scalar("1", ColumnType::Int, Value::Int(1))),
            _ if lower.starts_with("set ") => Some(QueryResult::empty_ok()),
            _ => None,
        }
    }
}

fn stmt_kind(s: &Statement) -> &'static str {
    match s {
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::Insert { .. } => "INSERT",
        Statement::Delete { .. } => "DELETE",
        Statement::Update { .. } => "UPDATE",
        Statement::Drop { .. } => "DROP",
        _ => "unknown",
    }
}
