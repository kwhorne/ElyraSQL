use std::collections::BTreeMap;
use std::fs::{self, File};
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Parser as ClapParser, ValueEnum};
use elyra_engine::Engine;
use elyra_server::{serve, Auth, ServerConfig};
use elyra_storage::Db;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, Row, Value};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sql_splitter::diagnostic::Diagnostic;
use sql_splitter::generate::{
    CompileOptions, GenerationEngine, ModelCompiler, RenderOptions, SqlRenderer,
};
use sql_splitter::parser::{Parser, SqlDialect};
use sql_splitter::synthetic::model::OutputMode;
use sql_splitter::synthetic::ConfigLoader;
use sql_splitter::validate::{ValidateOptions, ValidationSummary, Validator};
use tempfile::TempDir;

const DEFAULT_MODEL: &str = "fixtures/models/car_dealership.yaml";
const DEFAULT_ARTIFACTS: &str = "artifacts/latest";

#[derive(Debug, ClapParser)]
#[command(
    about = "Compare generated SQL dumps between ElyraSQL and MySQL",
    long_about = "Developer-only differential stress tool for ElyraSQL. Generates a deterministic MySQL dump from a model, imports it into MySQL and an ephemeral in-process ElyraSQL server, then compares schema and typed row contents."
)]
struct Args {
    /// YAML model used to generate the SQL dump.
    #[arg(long, default_value = DEFAULT_MODEL)]
    model: PathBuf,

    /// Directory for generated SQL, reports, and failure artifacts.
    #[arg(long, default_value = DEFAULT_ARTIFACTS)]
    artifacts: PathBuf,

    /// Deterministic data-generation seed.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Maximum planned rows per table.
    #[arg(long, default_value_t = 3)]
    max_rows: u64,

    /// Generate schema only or schema plus data.
    #[arg(long, value_enum, default_value = "schema-only")]
    mode: DumpMode,

    /// INSERT rows per generated statement; must be greater than zero.
    #[arg(long, default_value = "17")]
    batch_size: NonZeroU32,

    /// Timed query samples per table after one warmup; zero disables profiling.
    #[arg(long, default_value_t = 0)]
    profile_iterations: u16,

    /// MySQL oracle URL; ELYRA_STRESS_MYSQL_URL is used when omitted.
    #[arg(long)]
    mysql_url: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum DumpMode {
    SchemaOnly,
    SchemaAndData,
}

impl DumpMode {
    fn output_mode(self) -> OutputMode {
        match self {
            Self::SchemaOnly => OutputMode::SchemaOnly,
            Self::SchemaAndData => OutputMode::SchemaAndData,
        }
    }
}

#[derive(Debug, Serialize)]
struct RunReport {
    model: String,
    model_sha256: String,
    dump_sha256: String,
    seed: u64,
    max_rows: u64,
    mode: DumpMode,
    batch_size: u32,
    planner_rows_processed: u64,
    diagnostics: Vec<Diagnostic>,
    validation: ValidationSummary,
    mysql_version: String,
    elyra_version: String,
    statements_executed: usize,
    performance: PerformanceReport,
    outcome: Outcome,
}

#[derive(Debug, Serialize)]
struct PerformanceReport {
    import_statements: TimingComparison,
    query_iterations: u16,
    tables: BTreeMap<String, TableProfile>,
}

#[derive(Debug, Serialize)]
struct TableProfile {
    count: TimingComparison,
    ordered_page: TimingComparison,
    point_lookup: Option<TimingComparison>,
}

#[derive(Debug, Serialize)]
struct TimingComparison {
    mysql: TimingStats,
    elyra: TimingStats,
    /// Values above 1.0 mean ElyraSQL was faster for this sample set.
    elyra_speedup: Option<f64>,
}

#[derive(Debug, Serialize)]
struct TimingStats {
    samples: usize,
    total_ns: u64,
    mean_ns: u64,
    median_ns: u64,
    p95_ns: u64,
    max_ns: u64,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Outcome {
    Passed {
        table_count: usize,
        column_count: usize,
        row_counts: BTreeMap<String, u64>,
        content_digests: BTreeMap<String, String>,
    },
    OracleRejected {
        statement: usize,
        error: String,
    },
    ElyraRejected {
        statement: usize,
        error: String,
    },
    MetadataMismatch {
        detail: String,
    },
    DataMismatch {
        table: String,
        mysql_sha256: String,
        elyra_sha256: String,
        mysql_row: Option<String>,
        elyra_row: Option<String>,
    },
    RowCountMismatch {
        mysql: BTreeMap<String, u64>,
        elyra: BTreeMap<String, u64>,
    },
    ProfileFailed {
        detail: String,
    },
}

struct GeneratedDump {
    sql_path: PathBuf,
    dump_sha256: String,
    planner_rows_processed: u64,
    diagnostics: Vec<Diagnostic>,
    validation: ValidationSummary,
}

struct ImportResult {
    statements_executed: usize,
    outcome: Outcome,
    performance: PerformanceReport,
}

struct EmbeddedElyra {
    port: u16,
    handle: tokio::task::JoinHandle<std::io::Result<()>>,
    _temp_dir: TempDir,
}

impl EmbeddedElyra {
    async fn start() -> Result<Self> {
        let temp_dir = tempfile::tempdir().context("create ElyraSQL data directory")?;

        for attempt in 0..8 {
            let probe =
                std::net::TcpListener::bind("127.0.0.1:0").context("reserve an ElyraSQL port")?;
            let port = probe.local_addr()?.port();
            drop(probe);

            let data_path = temp_dir.path().join(format!("stress-{attempt}.edb"));
            let db = Db::open(&data_path).context("open ElyraSQL database")?;
            let auth = Arc::new(Auth::open().with_db(db.clone()));
            let engine = Engine::new(db);
            let config = ServerConfig {
                listen: format!("127.0.0.1:{port}"),
                auth,
                tls: None,
                slow_query_ms: 0,
                metrics_listen: None,
                audit_log: None,
                replication_listen: None,
                read_only: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            let handle = tokio::spawn(async move { serve(config, engine).await });

            for _ in 0..200 {
                if handle.is_finished() {
                    break;
                }
                if tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .is_ok()
                {
                    return Ok(Self {
                        port,
                        handle,
                        _temp_dir: temp_dir,
                    });
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            handle.abort();
        }

        bail!("could not start ElyraSQL after eight ephemeral-port attempts")
    }

    async fn connect(&self) -> Result<Conn> {
        let opts: Opts = OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(self.port)
            .user(Some("root"))
            .db_name(Some("elyra"))
            .prefer_socket(false)
            .into();

        for _ in 0..100 {
            match Conn::new(opts.clone()).await {
                Ok(connection) => return Ok(connection),
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        bail!("ElyraSQL accepted TCP connections but not a MySQL handshake")
    }
}

impl Drop for EmbeddedElyra {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.artifacts)
        .with_context(|| format!("create {}", args.artifacts.display()))?;
    reset_artifacts(&args.artifacts)?;

    let generated = generate_dump(&args)?;
    let model_sha256 = sha256_file(&args.model)?;
    let elyra = EmbeddedElyra::start().await?;
    let mut subject = elyra.connect().await?;
    let mysql_url = args
        .mysql_url
        .clone()
        .or_else(|| std::env::var("ELYRA_STRESS_MYSQL_URL").ok())
        .context("pass --mysql-url or set ELYRA_STRESS_MYSQL_URL")?;
    let mysql_opts = Opts::from_url(&mysql_url).context("parse MySQL oracle URL")?;
    let mut oracle = Conn::new(mysql_opts)
        .await
        .context("connect to the MySQL oracle")?;

    ensure_empty(&mut oracle, "MySQL oracle").await?;
    ensure_empty(&mut subject, "ElyraSQL").await?;

    let mysql_version = version(&mut oracle).await?;
    let elyra_version = version(&mut subject).await?;
    let result = import_and_compare(
        &generated.sql_path,
        &args.artifacts,
        &mut oracle,
        &mut subject,
        args.profile_iterations,
    )
    .await?;

    let passed = matches!(result.outcome, Outcome::Passed { .. });
    let report = RunReport {
        model: args.model.display().to_string(),
        model_sha256,
        dump_sha256: generated.dump_sha256,
        seed: args.seed,
        max_rows: args.max_rows,
        mode: args.mode,
        batch_size: args.batch_size.get(),
        planner_rows_processed: generated.planner_rows_processed,
        diagnostics: generated.diagnostics,
        validation: generated.validation,
        mysql_version,
        elyra_version,
        statements_executed: result.statements_executed,
        performance: result.performance,
        outcome: result.outcome,
    };
    let report_path = args.artifacts.join("report.json");
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)
        .with_context(|| format!("write {}", report_path.display()))?;

    println!("report: {}", report_path.display());
    if !passed {
        bail!(
            "differential import did not pass; see {}",
            report_path.display()
        )
    }
    Ok(())
}

fn generate_dump(args: &Args) -> Result<GeneratedDump> {
    let mut model = ConfigLoader::load(&args.model)
        .map_err(anyhow::Error::msg)?
        .into_model()?;
    model.seed = Some(args.seed);
    model.output.dialect = Some("mysql".to_owned());
    model.output.mode = Some(args.mode.output_mode());
    model.output.batch_size = Some(args.batch_size.get());

    let plan = ModelCompiler::standard()
        .compile(
            model,
            CompileOptions {
                seed: Some(args.seed),
                max_rows: Some(args.max_rows),
                ..CompileOptions::default()
            },
        )
        .map_err(anyhow::Error::msg)?;
    let diagnostics = plan.diagnostics.clone();
    let render = RenderOptions {
        dialect: SqlDialect::MySql,
        source_dialect: plan.input_dialect,
        mode: args.mode.output_mode(),
        batch_size: args.batch_size.get() as usize,
        ..RenderOptions::default()
    };
    let mut renderer = SqlRenderer::new(Vec::new(), render);
    let engine_report = GenerationEngine::new(plan).run(&mut renderer)?;
    let sql = renderer.finish()?;
    let sql_path = args.artifacts.join("generated.sql");
    fs::write(&sql_path, &sql).with_context(|| format!("write {}", sql_path.display()))?;

    let validation = Validator::new(ValidateOptions {
        path: sql_path.clone(),
        dialect: Some(SqlDialect::MySql),
        progress: false,
        strict: true,
        json: false,
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: true,
        max_pk_fk_keys: Some(1_000_000),
    })
    .validate()
    .context("validate generated SQL")?;
    if validation.has_errors() {
        bail!(
            "SQL Splitter validation rejected the generated dump; see {}",
            sql_path.display()
        )
    }

    Ok(GeneratedDump {
        sql_path,
        dump_sha256: hex::encode(Sha256::digest(&sql)),
        planner_rows_processed: engine_report.rows_written,
        diagnostics,
        validation,
    })
}

async fn import_and_compare(
    sql_path: &Path,
    artifacts: &Path,
    oracle: &mut Conn,
    subject: &mut Conn,
    profile_iterations: u16,
) -> Result<ImportResult> {
    let file = File::open(sql_path).with_context(|| format!("open {}", sql_path.display()))?;
    let mut parser = Parser::with_dialect(file, 64 * 1024, SqlDialect::MySql);
    let mut statement_index = 0;
    let mut mysql_import = Vec::new();
    let mut elyra_import = Vec::new();

    while let Some(bytes) = parser.read_statement().context("split generated SQL")? {
        let statement = String::from_utf8(bytes).context("generated SQL is not UTF-8")?;
        if statement.trim().is_empty() {
            continue;
        }
        statement_index += 1;

        let started = Instant::now();
        let mysql_result = oracle.query_drop(&statement).await;
        mysql_import.push(elapsed_ns(started));
        if let Err(error) = mysql_result {
            write_failure(artifacts, &statement)?;
            return Ok(import_result(
                statement_index,
                Outcome::OracleRejected {
                    statement: statement_index,
                    error: error.to_string(),
                },
                &mysql_import,
                &elyra_import,
            ));
        }
        let started = Instant::now();
        let elyra_result = subject.query_drop(&statement).await;
        elyra_import.push(elapsed_ns(started));
        if let Err(error) = elyra_result {
            write_failure(artifacts, &statement)?;
            return Ok(import_result(
                statement_index,
                Outcome::ElyraRejected {
                    statement: statement_index,
                    error: error.to_string(),
                },
                &mysql_import,
                &elyra_import,
            ));
        }
    }

    let oracle_tables = table_names(oracle).await?;
    let subject_tables = table_names(subject).await?;
    if oracle_tables != subject_tables {
        return Ok(import_result(
            statement_index,
            Outcome::MetadataMismatch {
                detail: format!(
                    "table names differ: mysql={oracle_tables:?}, elyra={subject_tables:?}"
                ),
            },
            &mysql_import,
            &elyra_import,
        ));
    }

    let oracle_columns = column_names(oracle).await?;
    let subject_columns = column_names(subject).await?;
    if oracle_columns != subject_columns {
        return Ok(import_result(
            statement_index,
            Outcome::MetadataMismatch {
                detail: first_sequence_difference("columns", &oracle_columns, &subject_columns),
            },
            &mysql_import,
            &elyra_import,
        ));
    }

    let oracle_counts = row_counts(oracle, &oracle_tables).await?;
    let subject_counts = row_counts(subject, &subject_tables).await?;
    if oracle_counts != subject_counts {
        return Ok(import_result(
            statement_index,
            Outcome::RowCountMismatch {
                mysql: oracle_counts,
                elyra: subject_counts,
            },
            &mysql_import,
            &elyra_import,
        ));
    }

    let oracle_digests = content_digests(oracle, &oracle_tables).await?;
    let subject_digests = content_digests(subject, &subject_tables).await?;
    if let Some((table, mysql_sha256)) = oracle_digests
        .iter()
        .find(|(table, digest)| subject_digests.get(*table) != Some(*digest))
    {
        let (mysql_row, elyra_row) = first_row_difference(oracle, subject, table).await?;
        return Ok(import_result(
            statement_index,
            Outcome::DataMismatch {
                table: table.clone(),
                mysql_sha256: mysql_sha256.clone(),
                elyra_sha256: subject_digests
                    .get(table)
                    .cloned()
                    .unwrap_or_else(|| "missing".to_owned()),
                mysql_row,
                elyra_row,
            },
            &mysql_import,
            &elyra_import,
        ));
    }

    let tables = if profile_iterations == 0 {
        BTreeMap::new()
    } else {
        match profile_tables(
            oracle,
            subject,
            &oracle_tables,
            &oracle_columns,
            profile_iterations,
        )
        .await
        {
            Ok(tables) => tables,
            Err(error) => {
                return Ok(import_result(
                    statement_index,
                    Outcome::ProfileFailed {
                        detail: error.to_string(),
                    },
                    &mysql_import,
                    &elyra_import,
                ));
            }
        }
    };
    Ok(ImportResult {
        statements_executed: statement_index,
        outcome: Outcome::Passed {
            table_count: oracle_tables.len(),
            column_count: oracle_columns.len(),
            row_counts: oracle_counts,
            content_digests: oracle_digests,
        },
        performance: PerformanceReport {
            import_statements: TimingComparison::new(&mysql_import, &elyra_import),
            query_iterations: profile_iterations,
            tables,
        },
    })
}

fn import_result(
    statements_executed: usize,
    outcome: Outcome,
    mysql_import: &[u64],
    elyra_import: &[u64],
) -> ImportResult {
    ImportResult {
        statements_executed,
        outcome,
        performance: PerformanceReport {
            import_statements: TimingComparison::new(mysql_import, elyra_import),
            query_iterations: 0,
            tables: BTreeMap::new(),
        },
    }
}

async fn profile_tables(
    oracle: &mut Conn,
    subject: &mut Conn,
    tables: &[String],
    columns: &[(String, String)],
    iterations: u16,
) -> Result<BTreeMap<String, TableProfile>> {
    let mut profiles = BTreeMap::new();
    for table in tables {
        let quoted_table = quote_identifier(table);
        let first_column = columns
            .iter()
            .find_map(|(candidate, column)| (candidate == table).then_some(column))
            .with_context(|| format!("find first column for {table}"))?;
        let quoted_column = quote_identifier(first_column);

        let count = profile_query(
            oracle,
            subject,
            &format!("SELECT COUNT(*) FROM {quoted_table}"),
            iterations,
        )
        .await?;
        let ordered_page_sql =
            format!("SELECT * FROM {quoted_table} ORDER BY {quoted_column} LIMIT 100");
        let ordered_page = profile_query(oracle, subject, &ordered_page_sql, iterations).await?;

        let first_value: Option<Value> = oracle
            .query_first(format!(
                "SELECT {quoted_column} FROM {quoted_table} ORDER BY {quoted_column} LIMIT 1"
            ))
            .await
            .with_context(|| format!("select point-lookup value from {table}"))?;
        let point_lookup = match first_value {
            Some(value) => {
                let sql = format!(
                    "SELECT * FROM {quoted_table} WHERE {quoted_column} = {} LIMIT 1",
                    mysql_value_literal(&value)
                );
                Some(profile_query(oracle, subject, &sql, iterations).await?)
            }
            None => None,
        };

        profiles.insert(
            table.clone(),
            TableProfile {
                count,
                ordered_page,
                point_lookup,
            },
        );
    }
    Ok(profiles)
}

async fn profile_query(
    oracle: &mut Conn,
    subject: &mut Conn,
    sql: &str,
    iterations: u16,
) -> Result<TimingComparison> {
    let (mysql_warmup, _) = timed_rows(oracle, sql).await?;
    let (elyra_warmup, _) = timed_rows(subject, sql).await?;
    ensure_same_query_result(sql, mysql_warmup, elyra_warmup)?;

    let mut mysql_samples = Vec::with_capacity(iterations as usize);
    let mut elyra_samples = Vec::with_capacity(iterations as usize);
    for iteration in 0..iterations {
        let (mysql_rows, mysql_ns, elyra_rows, elyra_ns) = if iteration % 2 == 0 {
            let (mysql_rows, mysql_ns) = timed_rows(oracle, sql).await?;
            let (elyra_rows, elyra_ns) = timed_rows(subject, sql).await?;
            (mysql_rows, mysql_ns, elyra_rows, elyra_ns)
        } else {
            let (elyra_rows, elyra_ns) = timed_rows(subject, sql).await?;
            let (mysql_rows, mysql_ns) = timed_rows(oracle, sql).await?;
            (mysql_rows, mysql_ns, elyra_rows, elyra_ns)
        };
        ensure_same_query_result(sql, mysql_rows, elyra_rows)?;
        mysql_samples.push(mysql_ns);
        elyra_samples.push(elyra_ns);
    }
    Ok(TimingComparison::new(&mysql_samples, &elyra_samples))
}

async fn timed_rows(connection: &mut Conn, sql: &str) -> Result<(Vec<Row>, u64)> {
    let started = Instant::now();
    let rows = connection
        .query(sql)
        .await
        .with_context(|| format!("profile query failed: {sql}"))?;
    Ok((rows, elapsed_ns(started)))
}

fn ensure_same_query_result(sql: &str, mysql_rows: Vec<Row>, elyra_rows: Vec<Row>) -> Result<()> {
    let mysql_digest = digest_rows(mysql_rows.into_iter().map(Row::unwrap));
    let elyra_digest = digest_rows(elyra_rows.into_iter().map(Row::unwrap));
    if mysql_digest != elyra_digest {
        bail!("profile query returned different results: {sql}")
    }
    Ok(())
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn mysql_value_literal(value: &Value) -> String {
    match value {
        Value::NULL => "NULL".to_owned(),
        Value::Bytes(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'")),
            Err(_) => format!("X'{}'", hex::encode(bytes)),
        },
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            format!("'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}'")
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if *negative { "-" } else { "" };
            let total_hours = days * 24 + u32::from(*hours);
            format!("'{sign}{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}'")
        }
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

impl TimingComparison {
    fn new(mysql: &[u64], elyra: &[u64]) -> Self {
        let mysql = TimingStats::new(mysql);
        let elyra = TimingStats::new(elyra);
        let elyra_speedup =
            (elyra.total_ns != 0).then_some(mysql.total_ns as f64 / elyra.total_ns as f64);
        Self {
            mysql,
            elyra,
            elyra_speedup,
        }
    }
}

impl TimingStats {
    fn new(samples: &[u64]) -> Self {
        if samples.is_empty() {
            return Self {
                samples: 0,
                total_ns: 0,
                mean_ns: 0,
                median_ns: 0,
                p95_ns: 0,
                max_ns: 0,
            };
        }

        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        let total_ns = sorted.iter().sum();
        let p95_index = ((sorted.len() * 95).div_ceil(100)).saturating_sub(1);
        Self {
            samples: sorted.len(),
            total_ns,
            mean_ns: total_ns / sorted.len() as u64,
            median_ns: sorted[sorted.len() / 2],
            p95_ns: sorted[p95_index],
            max_ns: *sorted.last().expect("non-empty timing samples"),
        }
    }
}

async fn ensure_empty(connection: &mut Conn, label: &str) -> Result<()> {
    let tables = table_names(connection).await?;
    if !tables.is_empty() {
        bail!("{label} database is not empty: {tables:?}")
    }
    Ok(())
}

async fn version(connection: &mut Conn) -> Result<String> {
    connection
        .query_first("SELECT VERSION()")
        .await?
        .context("VERSION() returned no row")
}

async fn table_names(connection: &mut Conn) -> Result<Vec<String>> {
    connection
        .query(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = SCHEMA() AND TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_NAME",
        )
        .await
        .context("query table metadata")
}

async fn column_names(connection: &mut Conn) -> Result<Vec<(String, String)>> {
    connection
        .query(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = SCHEMA() \
             ORDER BY TABLE_NAME, ORDINAL_POSITION",
        )
        .await
        .context("query column metadata")
}

async fn row_counts(connection: &mut Conn, tables: &[String]) -> Result<BTreeMap<String, u64>> {
    let mut counts = BTreeMap::new();
    for table in tables {
        let quoted = quote_identifier(table);
        let count = connection
            .query_first(format!("SELECT COUNT(*) FROM {quoted}"))
            .await?
            .with_context(|| format!("COUNT(*) returned no row for {table}"))?;
        counts.insert(table.clone(), count);
    }
    Ok(counts)
}

async fn content_digests(
    connection: &mut Conn,
    tables: &[String],
) -> Result<BTreeMap<String, String>> {
    let mut digests = BTreeMap::new();
    for table in tables {
        let quoted = quote_identifier(table);
        let rows: Vec<Row> = connection
            .query(format!("SELECT * FROM {quoted}"))
            .await
            .with_context(|| format!("query rows from {table}"))?;
        digests.insert(
            table.clone(),
            digest_rows(rows.into_iter().map(Row::unwrap)),
        );
    }
    Ok(digests)
}

fn digest_rows(rows: impl IntoIterator<Item = Vec<Value>>) -> String {
    let mut row_digests = rows
        .into_iter()
        .map(|row| digest_row(&row))
        .collect::<Vec<_>>();
    row_digests.sort_unstable();

    let mut hasher = Sha256::new();
    hasher.update((row_digests.len() as u64).to_le_bytes());
    for digest in row_digests {
        hasher.update(digest);
    }
    hex::encode(hasher.finalize())
}

fn digest_row(row: &[Value]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((row.len() as u64).to_le_bytes());
    for value in row {
        update_value_digest(&mut hasher, value);
    }
    hasher.finalize().into()
}

async fn first_row_difference(
    oracle: &mut Conn,
    subject: &mut Conn,
    table: &str,
) -> Result<(Option<String>, Option<String>)> {
    let mysql_rows = row_fingerprints(oracle, table).await?;
    let elyra_rows = row_fingerprints(subject, table).await?;
    let position = mysql_rows
        .iter()
        .zip(&elyra_rows)
        .position(|((left, _), (right, _))| left != right)
        .unwrap_or_else(|| mysql_rows.len().min(elyra_rows.len()));
    Ok((
        mysql_rows.get(position).map(|(_, row)| row.clone()),
        elyra_rows.get(position).map(|(_, row)| row.clone()),
    ))
}

async fn row_fingerprints(connection: &mut Conn, table: &str) -> Result<Vec<([u8; 32], String)>> {
    let quoted = quote_identifier(table);
    let rows: Vec<Row> = connection
        .query(format!("SELECT * FROM {quoted}"))
        .await
        .with_context(|| format!("query diagnostic rows from {table}"))?;
    let mut fingerprints = rows
        .into_iter()
        .map(Row::unwrap)
        .map(|row| (digest_row(&row), format_row(&row)))
        .collect::<Vec<_>>();
    fingerprints.sort_unstable_by_key(|(digest, _)| *digest);
    Ok(fingerprints)
}

fn format_row(row: &[Value]) -> String {
    let cells = row
        .iter()
        .map(|value| match value {
            Value::Bytes(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => format!("Bytes({text:?})"),
                Err(_) => format!("Bytes(0x{})", hex::encode(bytes)),
            },
            value => format!("{value:?}"),
        })
        .collect::<Vec<_>>();
    format!("[{}]", cells.join(", "))
}

fn update_value_digest(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::NULL => hasher.update([0]),
        Value::Bytes(bytes) => {
            hasher.update([1]);
            hasher.update((bytes.len() as u64).to_le_bytes());
            hasher.update(bytes);
        }
        Value::Int(value) => {
            hasher.update([2]);
            hasher.update(value.to_le_bytes());
        }
        Value::UInt(value) => {
            hasher.update([3]);
            hasher.update(value.to_le_bytes());
        }
        Value::Float(value) => {
            hasher.update([4]);
            hasher.update(value.to_bits().to_le_bytes());
        }
        Value::Double(value) => {
            hasher.update([5]);
            hasher.update(value.to_bits().to_le_bytes());
        }
        Value::Date(year, month, day, hour, minute, second, micros) => {
            hasher.update([6]);
            hasher.update(year.to_le_bytes());
            hasher.update([*month, *day, *hour, *minute, *second]);
            hasher.update(micros.to_le_bytes());
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            hasher.update([7, u8::from(*negative)]);
            hasher.update(days.to_le_bytes());
            hasher.update([*hours, *minutes, *seconds]);
            hasher.update(micros.to_le_bytes());
        }
    }
}

fn first_sequence_difference<T: std::fmt::Debug + PartialEq>(
    label: &str,
    oracle: &[T],
    subject: &[T],
) -> String {
    let position = oracle
        .iter()
        .zip(subject)
        .position(|(left, right)| left != right)
        .unwrap_or_else(|| oracle.len().min(subject.len()));
    format!(
        "{label} differ at position {position}: mysql={:?}, elyra={:?}; lengths {} vs {}",
        oracle.get(position),
        subject.get(position),
        oracle.len(),
        subject.len()
    )
}

fn write_failure(artifacts: &Path, statement: &str) -> Result<()> {
    let path = artifacts.join("failure.sql");
    fs::write(&path, statement).with_context(|| format!("write {}", path.display()))
}

fn reset_artifacts(artifacts: &Path) -> Result<()> {
    for name in ["generated.sql", "report.json", "failure.sql"] {
        let path = artifacts.join(name);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).with_context(|| format!("remove {}", path.display())),
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use std::{fs, num::NonZeroU32};

    use clap::{error::ErrorKind, Parser as _};
    use mysql_async::Value;

    use super::{
        digest_rows, first_sequence_difference, generate_dump, Args, DumpMode, TimingStats,
    };

    #[test]
    fn sequence_difference_reports_the_first_mismatch() {
        let detail = first_sequence_difference("values", &[1, 2, 4], &[1, 3, 4]);
        assert!(detail.contains("position 1"));
        assert!(detail.contains("mysql=Some(2)"));
        assert!(detail.contains("elyra=Some(3)"));
    }

    #[test]
    fn cli_rejects_a_zero_batch_size() {
        let error = Args::try_parse_from(["testbench", "--batch-size", "0"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
    }

    #[test]
    fn sequence_difference_reports_a_length_mismatch() {
        let detail = first_sequence_difference("values", &[1, 2], &[1]);
        assert!(detail.contains("position 1"));
        assert!(detail.contains("lengths 2 vs 1"));
    }

    #[test]
    fn generation_maps_source_types_to_mysql() {
        let temp_dir = tempfile::tempdir().unwrap();
        let model = temp_dir.path().join("model.yaml");
        let artifacts = temp_dir.path().join("artifacts");
        fs::create_dir(&artifacts).unwrap();
        fs::write(
            &model,
            r#"version: 1
kind: model
source:
  dialect: postgres
  fingerprint_policy: ignore
defaults:
  inference: disabled
tables:
  entries:
    rows: { kind: fixed, count: 1 }
    schema:
      name: entries
      columns:
        - { name: id, type: integer, nullable: false, primary_key: true }
        - { name: description, type: varchar, nullable: false }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      description: { generator: { kind: string, min_length: 1, max_length: 8 } }
"#,
        )
        .unwrap();
        let args = Args {
            model,
            artifacts,
            seed: 42,
            max_rows: 1,
            mode: DumpMode::SchemaOnly,
            batch_size: NonZeroU32::new(17).unwrap(),
            profile_iterations: 0,
            mysql_url: None,
        };

        let generated = generate_dump(&args).unwrap();
        let sql = fs::read_to_string(generated.sql_path).unwrap();

        assert!(sql.contains("`description` TEXT NOT NULL"));
    }

    #[test]
    fn row_digest_is_order_independent_and_value_sensitive() {
        let left = digest_rows([
            vec![Value::Int(1), Value::Bytes(b"first".to_vec())],
            vec![Value::Int(2), Value::NULL],
        ]);
        let reordered = digest_rows([
            vec![Value::Int(2), Value::NULL],
            vec![Value::Int(1), Value::Bytes(b"first".to_vec())],
        ]);
        let changed = digest_rows([
            vec![Value::Int(1), Value::Bytes(b"changed".to_vec())],
            vec![Value::Int(2), Value::NULL],
        ]);

        assert_eq!(left, reordered);
        assert_ne!(left, changed);
    }

    #[test]
    fn timing_stats_reports_distribution_and_handles_empty_samples() {
        let stats = TimingStats::new(&[50, 10, 40, 20, 30]);
        assert_eq!(stats.samples, 5);
        assert_eq!(stats.total_ns, 150);
        assert_eq!(stats.mean_ns, 30);
        assert_eq!(stats.median_ns, 30);
        assert_eq!(stats.p95_ns, 50);
        assert_eq!(stats.max_ns, 50);

        let empty = TimingStats::new(&[]);
        assert_eq!(empty.samples, 0);
        assert_eq!(empty.total_ns, 0);
    }
}
