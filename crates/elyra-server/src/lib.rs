//! ElyraSQL server: speaks the MySQL wire protocol so every MySQL client,
//! driver and GUI works against ElyraSQL unchanged.
//!
//! One TCP connection = one [`ElyraShim`] over the shared [`Engine`].

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use elyra_engine::{Engine, QueryResult, Session};
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind,
    InitWriter, OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig as TlsServerConfig;
use tracing::{error, info, warn};

pub mod auth;
mod prepared;

pub use auth::Auth;

/// Runtime configuration for the ElyraSQL server.
pub struct ServerConfig {
    pub listen: String,
    pub auth: Arc<Auth>,
    pub tls: Option<Arc<TlsServerConfig>>,
}

/// Build a rustls server config from PEM certificate and key files.
pub fn load_tls(cert_path: impl AsRef<Path>, key_path: impl AsRef<Path>) -> std::io::Result<TlsServerConfig> {
    use std::fs::File;
    use std::io::{BufReader, Error, ErrorKind};
    use rustls_pki_types::CertificateDer;

    let certs = rustls_pemfile::certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<CertificateDer>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(key_path)?))?
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no private key in key file"))?;
    TlsServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))
}

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
    auth: Arc<Auth>,
    salt: [u8; 20],
    /// Privilege of the authenticated user (set during `authenticate`).
    privilege: std::sync::Mutex<elyra_core::Privilege>,
    /// Per-connection transaction state (BEGIN/COMMIT/ROLLBACK).
    session: Session,
    stmts: HashMap<u32, Prepared>,
    next_id: u32,
}

impl ElyraShim {
    pub fn new(engine: Engine, auth: Arc<Auth>) -> Self {
        let session = engine.session();
        Self {
            engine,
            auth,
            salt: auth::generate_salt(),
            privilege: std::sync::Mutex::new(elyra_core::Privilege::Read),
            session,
            stmts: HashMap::new(),
            next_id: 1,
        }
    }

    fn privilege(&self) -> elyra_core::Privilege {
        *self.privilege.lock().unwrap()
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
        // Date/time/decimal are sent as their canonical string form.
        elyra_core::ColumnType::Date => ColumnType::MYSQL_TYPE_VAR_STRING,
        elyra_core::ColumnType::DateTime => ColumnType::MYSQL_TYPE_VAR_STRING,
        elyra_core::ColumnType::Decimal(_, _) => ColumnType::MYSQL_TYPE_VAR_STRING,
        elyra_core::ColumnType::Time => ColumnType::MYSQL_TYPE_VAR_STRING,
        elyra_core::ColumnType::Json => ColumnType::MYSQL_TYPE_VAR_STRING,
    }
}

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for ElyraShim {
    type Error = std::io::Error;

    /// Version string reported in the MySQL handshake. Branded ElyraSQL.
    fn version(&self) -> String {
        elyra_core::SERVER_VERSION.to_string()
    }

    fn default_auth_plugin(&self) -> &str {
        "mysql_native_password"
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        "mysql_native_password"
    }

    /// Per-connection salt. Must be stable across calls (opensrv reads it for
    /// both the handshake and the verification step).
    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    async fn authenticate(
        &self,
        _auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        let ok = self.auth.verify(username, salt, auth_data);
        if ok {
            *self.privilege.lock().unwrap() = self.auth.privilege(username);
        }
        ok
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

        let privilege = self.privilege();
        match self.engine.execute(&sql, privilege, &self.session).await {
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
        let privilege = self.privilege();
        match self.engine.execute(query, privilege, &self.session).await {
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
        // Date/time/decimal: their canonical string form.
        other => rw.write_col(other.to_wire_string()),
    }
}

/// Bind and serve ElyraSQL over the MySQL protocol until cancelled.
pub async fn serve(config: ServerConfig, engine: Engine) -> std::io::Result<()> {
    let listener = TcpListener::bind(&config.listen).await?;
    let tls_enabled = config.tls.is_some();
    info!(addr = %config.listen, tls = tls_enabled, "ElyraSQL accepting MySQL-protocol connections");
    if config.auth.is_open() {
        warn!("authentication is OPEN (no credentials required) - set --user/--password for production");
    }

    let engine = Arc::new(engine);
    let auth = config.auth;
    let tls = config.tls;
    loop {
        let (stream, peer) = listener.accept().await?;
        let engine = (*engine).clone();
        let auth = auth.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, engine, auth, tls).await {
                error!(%peer, error = %e, "connection ended with error");
            }
        });
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    engine: Engine,
    auth: Arc<Auth>,
    tls: Option<Arc<TlsServerConfig>>,
) -> std::io::Result<()> {
    use opensrv_mysql::{plain_run_with_options, secure_run_with_options, IntermediaryOptions};

    let (mut r, mut w) = stream.into_split();
    let mut shim = ElyraShim::new(engine, auth);
    let opts = IntermediaryOptions::default();

    // Read the handshake first; the client tells us whether it wants TLS.
    let (is_ssl, init) =
        AsyncMysqlIntermediary::init_before_ssl(&mut shim, &mut r, &mut w, &tls).await?;

    if is_ssl {
        let cfg = tls.expect("client negotiated TLS without a server config");
        secure_run_with_options(shim, w, opts, cfg, init).await
    } else {
        plain_run_with_options(shim, w, opts, init).await
    }
}
