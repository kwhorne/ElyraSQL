//! ElyraSQL server: speaks the MySQL wire protocol so every MySQL client,
//! driver and GUI works against ElyraSQL unchanged.
//!
//! One TCP connection = one [`ElyraShim`] over the shared [`Engine`].

use std::sync::Arc;

use elyra_engine::{Engine, QueryResult};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind,
    InitWriter, OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tracing::{error, info};

/// Per-connection protocol handler.
pub struct ElyraShim {
    engine: Engine,
}

impl ElyraShim {
    pub fn new(engine: Engine) -> Self {
        Self { engine }
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
        _query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        // Prepared statements land in a later milestone.
        info.error(ErrorKind::ER_UNKNOWN_ERROR, b"prepared statements not yet supported")
            .await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _id: u32,
        _params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        results
            .error(ErrorKind::ER_UNKNOWN_ERROR, b"prepared statements not yet supported")
            .await
    }

    async fn on_close(&mut self, _stmt: u32) {}

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
                    let cells: Vec<Option<String>> =
                        row.iter().map(|v| v.to_wire_string()).collect();
                    rw.write_row(cells).await?;
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
