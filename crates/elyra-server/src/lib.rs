//! ElyraSQL server: speaks the MySQL wire protocol so every MySQL client,
//! driver and GUI works against ElyraSQL unchanged.
//!
//! One TCP connection = one [`ElyraShim`] over the shared [`Engine`].

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use elyra_engine::{Engine, QueryResult, Session};
use elyra_wire::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter,
    OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter, StatusFlags,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig as TlsServerConfig;
use tracing::{error, info, warn};

pub mod auth;
pub mod cluster;
mod observ;
mod prepared;
pub mod raftlog;
pub mod repl;

pub use auth::Auth;
pub use observ::{AuditLog, Metrics, ProcRegistry};
pub use repl::{run_replica, serve_replication};

/// Runtime configuration for the ElyraSQL server.
pub struct ServerConfig {
    pub listen: String,
    pub auth: Arc<Auth>,
    pub tls: Option<Arc<TlsServerConfig>>,
    /// Queries at or above this many milliseconds are logged as slow. 0 = off.
    pub slow_query_ms: u128,
    /// Optional address for the Prometheus metrics HTTP endpoint.
    pub metrics_listen: Option<String>,
    /// Optional path for the append-only audit log.
    pub audit_log: Option<std::path::PathBuf>,
    /// Optional address for the replication endpoint (makes this a primary).
    pub replication_listen: Option<String>,
    /// When set, reject all writes (used by replicas / non-leaders). Dynamic so
    /// a cluster node can flip it on role change.
    pub read_only: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Build a rustls server config from PEM certificate and key files.
pub fn load_tls(
    cert_path: impl AsRef<Path>,
    key_path: impl AsRef<Path>,
) -> std::io::Result<TlsServerConfig> {
    use rustls_pki_types::CertificateDer;
    use std::fs::File;
    use std::io::{BufReader, Error, ErrorKind};

    let certs = rustls_pemfile::certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<CertificateDer>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(key_path)?))?
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "no private key in key file"))?;
    // rustls 0.23 requires an explicit crypto provider; use ring (pure-Rust
    // friendly, matches the ai_embed HTTPS client and keeps musl builds working
    // -- no aws-lc-rs). Passing it explicitly avoids relying on a process-wide
    // default provider.
    let provider = std::sync::Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    TlsServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| Error::new(ErrorKind::InvalidInput, e))
}

// Prepared statements (COM_STMT_PREPARE/EXECUTE) bind parameters as SQL literals
// at execute time; describe_query reports an exact result-column count at PREPARE
// so native (binary) prepared statements read the result set correctly. Emulated
// (client-side) prepares are the recommended setting for maximum compatibility.
// The MySQL wire layer is our first-party elyra-wire crate (forked from
// opensrv-mysql), which also implements caching_sha2_password auth.

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
    metrics: Arc<Metrics>,
    procs: Arc<ProcRegistry>,
    conn_id: u32,
    /// Authenticated user name (for per-table grant checks).
    user: std::sync::Mutex<String>,
    /// Replica/non-leader mode: cap every connection at read-only (dynamic).
    read_only: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Optional audit log.
    audit: Option<Arc<AuditLog>>,
}

/// The wire auth plugin advertised in the handshake, from `ELYRASQL_AUTH_PLUGIN`
/// (`mysql_native_password` by default, or `caching_sha2_password`). Cached.
fn configured_auth_plugin() -> &'static str {
    use std::sync::OnceLock;
    static P: OnceLock<&'static str> = OnceLock::new();
    P.get_or_init(
        || match std::env::var("ELYRASQL_AUTH_PLUGIN").ok().as_deref() {
            Some("caching_sha2_password") | Some("caching_sha2") => "caching_sha2_password",
            _ => "mysql_native_password",
        },
    )
}

impl ElyraShim {
    /// Record privilege + username on a successful authentication (shared by the
    /// native-password and caching_sha2 paths).
    fn on_auth_success(&self, username: &[u8]) {
        *self.privilege.lock().unwrap() = self.auth.privilege(username);
        let name = String::from_utf8_lossy(username).into_owned();
        *self.user.lock().unwrap() = name.clone();
        self.procs.set_user(self.conn_id, name);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        engine: Engine,
        auth: Arc<Auth>,
        metrics: Arc<Metrics>,
        procs: Arc<ProcRegistry>,
        conn_id: u32,
        read_only: std::sync::Arc<std::sync::atomic::AtomicBool>,
        audit: Option<Arc<AuditLog>>,
    ) -> Self {
        let session = engine.session();
        Self {
            engine,
            auth,
            salt: auth::generate_salt(),
            privilege: std::sync::Mutex::new(elyra_core::Privilege::Read),
            session,
            stmts: HashMap::new(),
            next_id: 1,
            metrics,
            procs,
            conn_id,
            user: std::sync::Mutex::new(String::new()),
            read_only,
            audit,
        }
    }

    fn user(&self) -> String {
        self.user.lock().unwrap().clone()
    }

    fn privilege(&self) -> elyra_core::Privilege {
        if self.read_only.load(std::sync::atomic::Ordering::Relaxed) {
            return elyra_core::Privilege::Read;
        }
        *self.privilege.lock().unwrap()
    }

    /// Intercept the observability queries (`SHOW STATUS` / `SHOW PROCESSLIST`)
    /// and return their column names and rows, if this is one.
    fn observ_result(&self, query: &str) -> Option<(Vec<&'static str>, Vec<Vec<String>>)> {
        let t = query.trim().trim_end_matches(';').trim();
        if t.len() > 96 {
            return None;
        }
        let lower = t.to_ascii_lowercase();
        let l = lower.as_str();
        if l == "show processlist" || l == "show full processlist" {
            return Some((
                vec![
                    "Id", "User", "Host", "db", "Command", "Time", "State", "Info",
                ],
                self.procs.rows(),
            ));
        }
        if l.starts_with("show status")
            || l.starts_with("show global status")
            || l.starts_with("show session status")
        {
            let mut rows = self.metrics.status_rows();
            if let Some(pos) = l.find("like") {
                let pat = t[pos + 4..]
                    .trim()
                    .trim_matches(['\'', '"', ';', ' '])
                    .trim_end_matches('%')
                    .to_ascii_lowercase();
                rows.retain(|(k, _)| k.to_ascii_lowercase().starts_with(&pat));
            }
            let out = rows.into_iter().map(|(k, v)| vec![k, v]).collect();
            return Some((vec!["Variable_name", "Value"], out));
        }
        None
    }
}

/// Map an ElyraSQL error to the MySQL error code the wire should report
/// (e.g. 1064 parse, 1146 no-such-table, 1213 serialization failure).
fn elyra_kind(e: &elyra_core::Error) -> ErrorKind {
    ErrorKind::from(e.mysql_code())
}

/// Whether to describe prepared-statement result columns at PREPARE time
/// (`ELYRASQL_STMT_DESCRIBE=on|1|true`). Off by default: it lets lenient drivers
/// (sqlx) resolve result columns by name, but strict libmysqlclient-based
/// clients mishandle a prepare response that carries result columns.
fn stmt_describe_enabled() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("ELYRASQL_STMT_DESCRIBE")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "on" | "1" | "true" | "yes"
        )
    })
}

/// Column flags for a result column: mark `BIGINT UNSIGNED` with UNSIGNED_FLAG so
/// clients interpret large values (bitwise results, unsigned columns) correctly.
fn column_flags(ty: &elyra_core::ColumnType) -> ColumnFlags {
    match ty {
        elyra_core::ColumnType::UInt => ColumnFlags::UNSIGNED_FLAG,
        _ => ColumnFlags::empty(),
    }
}

fn column_type(ty: &elyra_core::ColumnType) -> ColumnType {
    match ty {
        elyra_core::ColumnType::Bool => ColumnType::MYSQL_TYPE_TINY,
        elyra_core::ColumnType::Int => ColumnType::MYSQL_TYPE_LONGLONG,
        elyra_core::ColumnType::UInt => ColumnType::MYSQL_TYPE_LONGLONG,
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
        configured_auth_plugin()
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        configured_auth_plugin()
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
            self.on_auth_success(username);
        }
        ok
    }

    async fn caching_sha2_requires_password(&self, username: &[u8]) -> bool {
        self.auth.requires_password(username)
    }

    async fn caching_sha2_verify(&self, username: &[u8], password: &[u8]) -> bool {
        let ok = self.auth.verify_cleartext(username, password);
        if ok {
            self.on_auth_success(username);
        }
        ok
    }

    fn caching_sha2_public_key(&self) -> Vec<u8> {
        self.auth.caching_sha2_public_key_pem().into_bytes()
    }

    fn caching_sha2_decrypt(&self, ciphertext: &[u8]) -> Option<Vec<u8>> {
        self.auth.caching_sha2_decrypt(ciphertext)
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let param_count = prepared::count_placeholders(query);
        let id = self.next_id;
        self.next_id += 1;
        self.stmts.insert(
            id,
            Prepared {
                sql: query.to_string(),
                params: param_count,
            },
        );

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
        // Optionally describe result columns statically (no execution) so drivers
        // can build a by-name column map from the prepare response. Off by
        // default: it enables by-name resolution for lenient drivers (e.g. sqlx),
        // but strict libmysqlclient-based clients (mysql-connector) mishandle a
        // prepare response that carries result columns. See ELYRASQL_STMT_DESCRIBE.
        let columns: Vec<Column> = if stmt_describe_enabled() {
            match self.engine.describe_query(query, &self.session).await {
                Some(schema) => schema
                    .columns
                    .iter()
                    .map(|c| Column {
                        table: String::new(),
                        column: c.name.clone(),
                        coltype: column_type(&c.ty),
                        colflags: column_flags(&c.ty),
                    })
                    .collect(),
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
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
            Err(e) => {
                return results
                    .error(ErrorKind::ER_UNKNOWN_ERROR, e.as_bytes())
                    .await
            }
        };

        let privilege = self.privilege();
        let user = self.user();
        self.procs.begin_query(self.conn_id, &sql);
        let start = std::time::Instant::now();
        let res = with_query_timeout(
            self.engine
                .execute_as(&sql, privilege, &user, &self.session),
        )
        .await;
        self.metrics.record(&sql, res.is_ok(), start.elapsed());
        self.procs.end_query(self.conn_id);
        if let Some(a) = &self.audit {
            a.record(self.conn_id, &user, &sql, res.is_ok());
        }
        match res {
            Ok(outcomes) => {
                write_outcomes(
                    outcomes,
                    results,
                    self.session.last_insert_id() as u64,
                    self.session.in_txn(),
                )
                .await
            }
            Err(e) => {
                results
                    .error(elyra_kind(&e), e.to_string().as_bytes())
                    .await
            }
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
        if let Some((cols, rows)) = self.observ_result(query) {
            return write_string_rows(results, &cols, rows).await;
        }
        let privilege = self.privilege();
        let user = self.user();
        self.procs.begin_query(self.conn_id, query);
        let start = std::time::Instant::now();
        let res =
            with_query_timeout(
                self.engine
                    .execute_as(query, privilege, &user, &self.session),
            )
            .await;
        self.metrics.record(query, res.is_ok(), start.elapsed());
        self.procs.end_query(self.conn_id);
        if let Some(a) = &self.audit {
            a.record(self.conn_id, &user, query, res.is_ok());
        }
        match res {
            Ok(outcomes) => {
                write_outcomes(
                    outcomes,
                    results,
                    self.session.last_insert_id() as u64,
                    self.session.in_txn(),
                )
                .await
            }
            Err(e) => {
                results
                    .error(elyra_kind(&e), e.to_string().as_bytes())
                    .await
            }
        }
    }
}

/// Write a simple string-typed result set (used for `SHOW STATUS`/`PROCESSLIST`).
async fn write_string_rows<W: AsyncWrite + Send + Unpin>(
    results: QueryResultWriter<'_, W>,
    col_names: &[&str],
    rows: Vec<Vec<String>>,
) -> Result<(), std::io::Error> {
    let cols: Vec<Column> = col_names
        .iter()
        .map(|n| Column {
            table: String::new(),
            column: (*n).to_string(),
            coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
            colflags: ColumnFlags::empty(),
        })
        .collect();
    let mut rw = results.start(&cols).await?;
    for row in rows {
        for cell in &row {
            rw.write_col(cell)?;
        }
        rw.end_row().await?;
    }
    rw.finish().await
}

/// Rows pulled from storage per batch while streaming a result set. Bounds
/// per-connection memory regardless of table size.
const STREAM_BATCH: usize = 1024;

async fn write_outcomes<W: AsyncWrite + Send + Unpin>(
    mut outcomes: Vec<QueryResult>,
    results: QueryResultWriter<'_, W>,
    last_insert_id: u64,
    in_trans: bool,
) -> Result<(), std::io::Error> {
    // Report an open transaction in the OK status flags so drivers (PDO/mysqlnd)
    // track PDO::inTransaction() correctly.
    let status_flags = if in_trans {
        StatusFlags::SERVER_STATUS_IN_TRANS
    } else {
        StatusFlags::empty()
    };
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
                    colflags: column_flags(&c.ty),
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
                        return rw.finish_error(elyra_kind(&e), &msg).await;
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
                .completed(OkResponse {
                    affected_rows: n,
                    last_insert_id,
                    status_flags,
                    ..Default::default()
                })
                .await
        }
        None => {
            results
                .completed(OkResponse {
                    status_flags,
                    ..Default::default()
                })
                .await
        }
    }
}

fn write_cell<W: AsyncWrite + Send + Unpin>(
    rw: &mut elyra_wire::RowWriter<'_, W>,
    v: &elyra_core::Value,
) -> std::io::Result<()> {
    use elyra_core::Value;
    match v {
        Value::Null => rw.write_col(None::<i64>),
        Value::Bool(b) => rw.write_col(*b as i8),
        Value::Int(i) => rw.write_col(*i),
        Value::UInt(u) => rw.write_col(*u),
        Value::Float(f) => rw.write_col(*f),
        Value::Text(s) => rw.write_col(s.as_str()),
        Value::Bytes(b) => rw.write_col(b),
        Value::Vector(vec) => {
            let inner = vec
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",");
            rw.write_col(format!("[{inner}]"))
        }
        // Date/time/decimal: their canonical string form.
        other => rw.write_col(other.to_wire_string()),
    }
}

/// Whether a bind address accepts connections from outside the local host.
/// Loopback (`127.0.0.0/8`, `::1`, `localhost`) is considered local; the
/// wildcard (`0.0.0.0`, `::`), routable IPs, and unresolved hostnames are treated
/// as exposed (conservatively, so an unknown host does not silently open access).
pub(crate) fn listen_is_exposed(listen: &str) -> bool {
    let host = match listen.rsplit_once(':') {
        Some((h, _)) => h.trim_start_matches('[').trim_end_matches(']'),
        None => listen,
    };
    if host.eq_ignore_ascii_case("localhost") {
        return false;
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => !ip.is_loopback(),
        Err(_) => true,
    }
}

/// Whether `var` is set to a truthy value (`1`/`true`/`yes`/`on`).
pub(crate) fn env_flag(var: &str) -> bool {
    matches!(
        std::env::var(var)
            .ok()
            .as_deref()
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Optional per-query wall-clock timeout (`ELYRASQL_QUERY_TIMEOUT_MS`; `0`/unset
/// = no limit). Read once.
fn query_timeout() -> Option<std::time::Duration> {
    use std::sync::OnceLock;
    static T: OnceLock<Option<std::time::Duration>> = OnceLock::new();
    *T.get_or_init(|| {
        std::env::var("ELYRASQL_QUERY_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .map(std::time::Duration::from_millis)
    })
}

/// Run a query future under the configured timeout, if any. On expiry the client
/// receives a query error immediately; CPU-bound work already handed to a
/// blocking thread may finish in the background, but the connection is unblocked.
async fn with_query_timeout<T, F>(fut: F) -> std::result::Result<T, elyra_core::Error>
where
    F: std::future::Future<Output = std::result::Result<T, elyra_core::Error>>,
{
    match query_timeout() {
        Some(d) => match tokio::time::timeout(d, fut).await {
            Ok(r) => r,
            Err(_) => Err(elyra_core::Error::Query(format!(
                "query exceeded ELYRASQL_QUERY_TIMEOUT_MS ({} ms)",
                d.as_millis()
            ))),
        },
        None => fut.await,
    }
}

/// Bind and serve ElyraSQL over the MySQL protocol until cancelled.
pub async fn serve(config: ServerConfig, engine: Engine) -> std::io::Result<()> {
    // Reclaim spill/aggregation temp files leaked by prior instances that were
    // killed (SIGKILL skips Drop cleanup); only removes files owned by dead PIDs.
    elyra_engine::cleanup_stale_tempfiles();

    // Safe-by-default: refuse to expose an open (no-credentials, everyone-Admin)
    // server to non-loopback clients unless explicitly allowed. Local development
    // (the default 127.0.0.1 bind) is unaffected.
    if config.auth.is_open() {
        if listen_is_exposed(&config.listen) && !env_flag("ELYRASQL_ALLOW_OPEN_AUTH") {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "refusing to start: authentication is OPEN (every client gets Admin) \
                     but the listener {} accepts remote connections. Configure accounts \
                     (--user/--password or --auth USER:PASS:ROLE), bind to localhost, or \
                     set ELYRASQL_ALLOW_OPEN_AUTH=1 to override.",
                    config.listen
                ),
            ));
        }
        warn!(
            "authentication is OPEN (no credentials required, every client gets Admin) - \
             configure --user/--password/--auth for production"
        );
    }

    let listener = TcpListener::bind(&config.listen).await?;
    let tls_enabled = config.tls.is_some();
    info!(addr = %config.listen, tls = tls_enabled, "ElyraSQL accepting MySQL-protocol connections");

    let engine = Arc::new(engine);
    let auth = config.auth;
    let tls = config.tls;
    let metrics = Arc::new(Metrics::new(config.slow_query_ms));
    let procs = Arc::new(ProcRegistry::new());
    let audit = match &config.audit_log {
        Some(p) => match AuditLog::open(p) {
            Ok(a) => {
                info!(path = %p.display(), "audit logging enabled");
                Some(Arc::new(a))
            }
            Err(e) => {
                error!(error = %e, "failed to open audit log; auditing disabled");
                None
            }
        },
        None => None,
    };
    if config.slow_query_ms > 0 {
        info!(
            threshold_ms = config.slow_query_ms as u64,
            "slow-query logging enabled"
        );
    }
    if let Some(maddr) = config.metrics_listen.clone() {
        let m = metrics.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_metrics(maddr, m).await {
                error!(error = %e, "metrics endpoint stopped");
            }
        });
    }
    if let Some(raddr) = config.replication_listen.clone() {
        let rdb = engine.db();
        tokio::spawn(async move {
            if let Err(e) = repl::serve_replication(raddr, rdb).await {
                error!(error = %e, "replication endpoint stopped");
            }
        });
    }
    let read_only = config.read_only;
    static CONN_SEQ: AtomicU32 = AtomicU32::new(0);
    loop {
        let (stream, peer) = listener.accept().await?;
        let engine = (*engine).clone();
        let auth = auth.clone();
        let tls = tls.clone();
        let metrics = metrics.clone();
        let procs = procs.clone();
        let conn_id = CONN_SEQ.fetch_add(1, Ordering::Relaxed) + 1;
        let read_only = read_only.clone();
        let audit = audit.clone();
        tokio::spawn(async move {
            metrics.connect();
            procs.register(conn_id, peer.ip().to_string());
            let res = handle_connection(
                stream,
                engine,
                auth,
                tls,
                metrics.clone(),
                procs.clone(),
                conn_id,
                read_only,
                audit,
            )
            .await;
            procs.deregister(conn_id);
            metrics.disconnect();
            if let Err(e) = res {
                error!(%peer, error = %e, "connection ended with error");
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
/// Serve Prometheus metrics over a minimal HTTP/1.1 endpoint (`GET /metrics`).
async fn serve_metrics(addr: String, metrics: Arc<Metrics>) -> std::io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    info!(%addr, "ElyraSQL Prometheus metrics at /metrics");
    loop {
        let (mut stream, _) = listener.accept().await?;
        let metrics = metrics.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .unwrap_or("/");
            let (status, ctype, body) = if path.starts_with("/metrics") {
                (
                    "200 OK",
                    "text/plain; version=0.0.4",
                    metrics.render_prometheus(),
                )
            } else if path == "/" || path.starts_with("/health") {
                (
                    "200 OK",
                    "text/plain",
                    "ElyraSQL metrics: GET /metrics\n".to_string(),
                )
            } else {
                ("404 Not Found", "text/plain", "not found\n".to_string())
            };
            let resp = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    stream: tokio::net::TcpStream,
    engine: Engine,
    auth: Arc<Auth>,
    tls: Option<Arc<TlsServerConfig>>,
    metrics: Arc<Metrics>,
    procs: Arc<ProcRegistry>,
    conn_id: u32,
    read_only: std::sync::Arc<std::sync::atomic::AtomicBool>,
    audit: Option<Arc<AuditLog>>,
) -> std::io::Result<()> {
    use elyra_wire::{plain_run_with_options, secure_run_with_options, IntermediaryOptions};

    let (mut r, mut w) = stream.into_split();
    let mut shim = ElyraShim::new(engine, auth, metrics, procs, conn_id, read_only, audit);
    let opts = IntermediaryOptions::default();

    // Read the handshake first; the client tells us whether it wants TLS.
    let (is_ssl, init) =
        AsyncMysqlIntermediary::init_before_ssl(&mut shim, &mut r, &mut w, &tls).await?;

    if is_ssl {
        // Defensive: a client should only reach here when the server advertised
        // TLS, but never panic on a malformed/hostile handshake -- drop the one
        // connection with a clean error instead.
        let Some(cfg) = tls else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "client requested TLS but the server has no TLS configuration",
            ));
        };
        secure_run_with_options(shim, w, opts, cfg, init).await
    } else {
        // Buffer the socket writes. opensrv issues one `write_vectored` syscall
        // per row packet and flushes after each command, so an unbuffered
        // TcpStream meant a syscall per result row -- the dominant cost for
        // large result sets. A BufWriter coalesces rows into ~64 KiB writes;
        // opensrv's per-command flush still drains it, so responses are prompt.
        // (TLS is left unbuffered: its handshake needs immediate flushes.)
        let w = tokio::io::BufWriter::with_capacity(64 * 1024, w);
        plain_run_with_options(shim, w, opts, init).await
    }
}

#[cfg(test)]
mod guard_tests {
    use super::listen_is_exposed;

    #[test]
    fn loopback_binds_are_local() {
        assert!(!listen_is_exposed("127.0.0.1:3307"));
        assert!(!listen_is_exposed("127.0.0.5:3307"));
        assert!(!listen_is_exposed("[::1]:3307"));
        assert!(!listen_is_exposed("localhost:3307"));
        assert!(!listen_is_exposed("LOCALHOST:3307"));
    }

    #[test]
    fn wildcard_and_routable_binds_are_exposed() {
        assert!(listen_is_exposed("0.0.0.0:3307"));
        assert!(listen_is_exposed("[::]:3307"));
        assert!(listen_is_exposed("192.168.1.10:3307"));
        assert!(listen_is_exposed("10.0.0.5:3307"));
        // An unresolved hostname is treated as exposed (conservative).
        assert!(listen_is_exposed("db.internal:3307"));
    }
}
