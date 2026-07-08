//! ElyraSQL server: speaks the MySQL wire protocol so every MySQL client,
//! driver and GUI works against ElyraSQL unchanged.
//!
//! One TCP connection = one [`ElyraShim`] over the shared [`Engine`].

use std::collections::HashMap;
use std::sync::Arc;

use elyra_engine::{Engine, QueryResult};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind,
    InitWriter, OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tracing::{error, info};

mod prepared;

// Prepared statements (COM_STMT_PREPARE/EXECUTE) are implemented by counting
// `?` placeholders and binding parameters as SQL literals at execute time.
// Verified working: prepare + (repeated) execute with typed params, escaping,
// and INSERT/SELECT/UPDATE. Known upstream limitation: the opensrv-mysql 0.7
// wire layer can desync across repeated COM_STMT_CLOSE -> COM_STMT_PREPARE
// cycles on one connection; pooled clients that cache prepared statements
// (the common case) and PyMySQL-style client-side binding are unaffected.

/// A parsed prepared statement: the SQL template and its placeholder count.
struct Prepared {
    sql: String,
    params: usize,
}

/// Per-connection protocol handler.
pub struct ElyraShim {
    engine: Engine,
    stmts: HashMap<u32, Prepared>,
    next_id: u32,
}

impl ElyraShim {
    pub fn new(engine: Engine) -> Self {
        Self { engine, stmts: HashMap::new(), next_id: 1 }
    }
}

fn column_type(ty: &elyra_core::ColumnType) -> ColumnType {
    match ty {
        elyra_core::ColumnType::Bool => ColumnType::MYSQL_TYPE_TINY,
        elyra_core::ColumnType::Int => ColumnType::MYSQL_TYPE_LONGLONG,
        elyra_core::ColumnType::Float => ColumnType::MYSQL_TYPE_DOUBLE,
        elyra_core::ColumnType::Text => ColumnType::MYSQL_TYPE_VAR_STRING,
        elyra_core::ColumnType::Bytes => ColumnType::MYSQL_TYPE_BLOB,
        elyra_core::ColumnType::Vector(_) => ColumnType::MYSQL_TYPE_VAR_STRING,
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for ElyraShim {
    type Error = std::io::Error;

    /// Version string reported in the MySQL handshake. Branded ElyraSQL.
    fn version(&self) -> String {
        elyra_core::SERVER_VERSION.to_string()
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let param_count = prepared::count_placeholders(query);
        let id = self.next_id;
        self.next_id += 1;
        self.stmts.insert(id, Prepared { sql: query.to_string(), params: param_count });

        // Generic string parameter descriptors; result columns are described
        // at execute time by the binary resultset.
        let params: Vec<Column> = (0..param_count)
            .map(|_| Column {
                table: String::new(),
                column: "?".into(),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect();
        let columns: Vec<Column> = Vec::new();
        info.reply(id, &params, &columns).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let Some(stmt) = self.stmts.get(&id) else {
            return results
                .error(ErrorKind::ER_UNKNOWN_ERROR, b"unknown prepared statement")
                .await;
        };

        // Render bound parameters as SQL literals and substitute them for the
        // `?` placeholders, producing a concrete statement to execute.
        let mut literals: Vec<String> = Vec::with_capacity(stmt.params);
        for p in params {
            literals.push(prepared::value_to_literal(p.value.into_inner()));
        }
        let sql = match prepared::bind(&stmt.sql, &literals) {
            Ok(s) => s,
            Err(e) => return results.error(ErrorKind::ER_UNKNOWN_ERROR, e.as_bytes()).await,
        };

        match self.engine.execute(&sql).await {
            Ok(outcomes) => write_outcomes(outcomes, results).await,
            Err(e) => results.error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes()).await,
        }
    }

    async fn on_close(&mut self, stmt: u32) {
        self.stmts.remove(&stmt);
    }

    async fn on_init<'a>(
        &'a mut self,
        _schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        // USE <db>: single-catalog for now, always accept.
        writer.ok().await
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        match self.engine.execute(query).await {
            Ok(outcomes) => write_outcomes(outcomes, results).await,
            Err(e) => results.error(ErrorKind::ER_UNKNOWN_ERROR, e.to_string().as_bytes()).await,
        }
    }
}

/// Rows pulled from storage per batch while streaming a result set. Bounds
/// per-connection memory regardless of table size.
const STREAM_BATCH: usize = 1024;

async fn write_outcomes<W: AsyncWrite + Send + Unpin>(
    mut outcomes: Vec<QueryResult>,
    results: QueryResultWriter<'_, W>,
) -> Result<(), std::io::Error> {
    // The text protocol returns a single result per query in this build.
    match outcomes.drain(..).next() {
        Some(QueryResult::Rows(mut stream)) => {
            let cols: Vec<Column> = stream
                .schema
                .columns
                .iter()
                .map(|c| Column {
                    table: String::new(),
                    column: c.name.clone(),
                    coltype: column_type(&c.ty),
                    colflags: ColumnFlags::empty(),
                })
                .collect();

            let mut rw = results.start(&cols).await?;
            // Drain the stream batch-by-batch straight onto the wire.
            loop {
                let batch = match stream.next_batch(STREAM_BATCH).await {
                    Ok(b) => b,
                    Err(e) => {
                        // Mid-stream engine error: surface it and stop.
                        let msg = e.to_string().into_bytes();
                        return rw.finish_error(ErrorKind::ER_UNKNOWN_ERROR, &msg).await;
                    }
                };
                if batch.is_empty() {
                    break;
                }
                for row in batch {
                    // Write each cell with its native type so both the text
                    // (COM_QUERY) and binary (COM_STMT_EXECUTE) encoders emit
                    // correct wire values.
                    for v in &row {
                        write_cell(&mut rw, v)?;
                    }
                    rw.end_row().await?;
                }
            }
            rw.finish().await
        }
        Some(QueryResult::Affected(n)) => {
            results
                .completed(OkResponse { affected_rows: n, ..Default::default() })
                .await
        }
        None => results.completed(OkResponse::default()).await,
    }
}

fn write_cell<W: AsyncWrite + Send + Unpin>(
    rw: &mut opensrv_mysql::RowWriter<'_, W>,
    v: &elyra_core::Value,
) -> std::io::Result<()> {
    use elyra_core::Value;
    match v {
        Value::Null => rw.write_col(None::<i64>),
        Value::Bool(b) => rw.write_col(*b as i8),
        Value::Int(i) => rw.write_col(*i),
        Value::Float(f) => rw.write_col(*f),
        Value::Text(s) => rw.write_col(s),
        Value::Bytes(b) => rw.write_col(b),
        Value::Vector(vec) => {
            let inner = vec.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",");
            rw.write_col(format!("[{inner}]"))
        }
    }
}

/// Bind to `addr` and serve ElyraSQL over the MySQL protocol until cancelled.
pub async fn serve(addr: &str, engine: Engine) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "ElyraSQL accepting MySQL-protocol connections");

    let engine = Arc::new(engine);
    loop {
        let (stream, peer) = listener.accept().await?;
        let engine = (*engine).clone();
        tokio::spawn(async move {
            let shim = ElyraShim::new(engine);
            let (r, w) = stream.into_split();
            if let Err(e) = AsyncMysqlIntermediary::run_on(shim, r, w).await {
                error!(%peer, error = %e, "connection ended with error");
            }
        });
    }
}
