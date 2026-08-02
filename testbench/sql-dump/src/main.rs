use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use clap::{Parser as ClapParser, ValueEnum};
use elyra_engine::Engine;
use elyra_server::{serve, Auth, ServerConfig};
use elyra_storage::Db;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Error as MysqlError, Opts, OptsBuilder, Row, ServerError, Value};
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

const DEFAULT_MODEL: &str = "fixtures/car_dealership.yaml";
const DEFAULT_TIMEOUT_SECONDS: u64 = 120;

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
    /// Defaults to a unique directory under artifacts/runs.
    #[arg(long)]
    artifacts: Option<PathBuf>,

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

    /// Per-statement and per-query timeout in seconds.
    #[arg(long, default_value_t = NonZeroU64::new(DEFAULT_TIMEOUT_SECONDS).unwrap())]
    timeout_seconds: NonZeroU64,

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
    artifacts: String,
    source_revision: Option<String>,
    source_dirty: Option<bool>,
    executable_sha256: Option<String>,
    host_os: &'static str,
    host_arch: &'static str,
    model: String,
    model_sha256: Option<String>,
    dump_sha256: Option<String>,
    seed: u64,
    max_rows: u64,
    mode: DumpMode,
    batch_size: u32,
    timeout_seconds: u64,
    planner_rows_processed: Option<u64>,
    diagnostics: Vec<Diagnostic>,
    validation: Option<ValidationSummary>,
    mysql_version: Option<String>,
    mysql_image: Option<String>,
    mysql_environment: Option<MysqlEnvironment>,
    elyra_version: Option<String>,
    statements_executed: usize,
    performance: PerformanceReport,
    outcome: Outcome,
}

#[derive(Debug, Serialize)]
struct MysqlEnvironment {
    sql_mode: String,
    time_zone: String,
    character_set: String,
    collation: String,
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
    ordered_page: Option<TimingComparison>,
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
        error: SqlRejection,
    },
    ElyraRejected {
        statement: usize,
        error: SqlRejection,
    },
    GenerationRejected {
        detail: String,
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
    InfrastructureFailed {
        stage: String,
        target: Option<Target>,
        statement: Option<usize>,
        error: String,
    },
    TimedOut {
        stage: String,
        target: Option<Target>,
        statement: Option<usize>,
        timeout_seconds: u64,
    },
}

#[derive(Debug, Serialize)]
struct SqlRejection {
    code: u16,
    state: String,
    message: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Target {
    Mysql,
    Elyra,
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

struct ProfileFailure {
    tables: BTreeMap<String, TableProfile>,
    target: Option<Target>,
    error: anyhow::Error,
}

struct ProfileQueryFailure {
    target: Option<Target>,
    error: anyhow::Error,
}

impl ProfileQueryFailure {
    fn new(target: Option<Target>, error: impl Into<anyhow::Error>) -> Self {
        Self {
            target,
            error: error.into(),
        }
    }
}

struct ArtifactDirectory {
    path: PathBuf,
    lock_path: PathBuf,
    _lock: File,
}

struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SchemaSnapshot {
    columns: Vec<ColumnMetadata>,
    indexes: Vec<IndexMetadata>,
    references: Vec<ReferenceMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnMetadata {
    table: String,
    ordinal: u64,
    name: String,
    column_type: String,
    nullable: bool,
    default: Option<String>,
    extra: String,
    generation_expression: String,
    charset: Option<String>,
    collation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexMetadata {
    table: String,
    primary: bool,
    unique: bool,
    columns: Vec<String>,
    prefix_lengths: Vec<Option<u64>>,
    index_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ReferenceMetadata {
    table: String,
    columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
    update_rule: String,
    delete_rule: String,
}

struct RunFailure {
    stage: &'static str,
    target: Option<Target>,
    error: anyhow::Error,
}

impl RunFailure {
    fn new(stage: &'static str, target: Option<Target>, error: impl Into<anyhow::Error>) -> Self {
        Self {
            stage,
            target,
            error: error.into(),
        }
    }

    fn into_outcome(self, timeout_seconds: u64) -> Outcome {
        if self
            .error
            .chain()
            .any(|source| source.is::<tokio::time::error::Elapsed>())
        {
            Outcome::TimedOut {
                stage: self.stage.to_owned(),
                target: self.target,
                statement: None,
                timeout_seconds,
            }
        } else {
            Outcome::InfrastructureFailed {
                stage: self.stage.to_owned(),
                target: self.target,
                statement: None,
                error: format!("{:#}", self.error),
            }
        }
    }
}

impl ArtifactDirectory {
    fn prepare(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
        let lock_path = path.join(".run.lock");
        let lock = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .with_context(|| {
                format!(
                    "lock {}; another run may be using this artifact directory (remove the lock only after confirming no run is active)",
                    lock_path.display()
                )
            })?;
        let mut artifacts = Self {
            path,
            lock_path,
            _lock: lock,
        };
        writeln!(artifacts._lock, "{}", std::process::id())
            .with_context(|| format!("write {}", artifacts.lock_path.display()))?;
        reset_artifacts(&artifacts.path)?;
        Ok(artifacts)
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ArtifactDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.lock_path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "warning: could not remove artifact lock {}: {error}",
                    self.lock_path.display()
                );
            }
        }
    }
}

impl<W> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn finish(self) -> (W, String) {
        (self.inner, hex::encode(self.hasher.finalize()))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl RunReport {
    fn new(args: &Args, artifacts: &Path) -> Self {
        let (source_revision, source_dirty) = source_state();
        Self {
            artifacts: artifacts.display().to_string(),
            source_revision,
            source_dirty,
            executable_sha256: std::env::current_exe()
                .ok()
                .and_then(|path| sha256_file(&path).ok()),
            host_os: std::env::consts::OS,
            host_arch: std::env::consts::ARCH,
            model: args.model.display().to_string(),
            model_sha256: None,
            dump_sha256: None,
            seed: args.seed,
            max_rows: args.max_rows,
            mode: args.mode,
            batch_size: args.batch_size.get(),
            timeout_seconds: args.timeout_seconds.get(),
            planner_rows_processed: None,
            diagnostics: Vec::new(),
            validation: None,
            mysql_version: None,
            mysql_image: std::env::var("ELYRA_STRESS_MYSQL_IMAGE").ok(),
            mysql_environment: None,
            elyra_version: None,
            statements_executed: 0,
            performance: empty_performance(0),
            outcome: Outcome::InfrastructureFailed {
                stage: "initialize".to_owned(),
                target: None,
                statement: None,
                error: "run did not start".to_owned(),
            },
        }
    }

    fn passed(&self) -> bool {
        matches!(self.outcome, Outcome::Passed { .. })
    }
}

fn empty_performance(query_iterations: u16) -> PerformanceReport {
    PerformanceReport {
        import_statements: TimingComparison::new(&[], &[]),
        query_iterations,
        tables: BTreeMap::new(),
    }
}

fn default_artifact_path() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    PathBuf::from(format!("artifacts/runs/{timestamp}-{}", std::process::id()))
}

fn source_state() -> (Option<String>, Option<bool>) {
    let revision = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|revision| revision.trim().to_owned());
    let dirty = revision.as_ref().and_then(|_| {
        Command::new("git")
            .args(["status", "--porcelain"])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| !output.stdout.is_empty())
    });
    (revision, dirty)
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
    let artifacts =
        ArtifactDirectory::prepare(args.artifacts.clone().unwrap_or_else(default_artifact_path))?;
    let mut report = RunReport::new(&args, artifacts.path());
    if let Err(failure) = execute_run(&args, artifacts.path(), &mut report).await {
        report.outcome = failure.into_outcome(args.timeout_seconds.get());
    }

    let passed = report.passed();
    let report_path = artifacts.path().join("report.json");
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

async fn execute_run(
    args: &Args,
    artifacts: &Path,
    report: &mut RunReport,
) -> Result<(), RunFailure> {
    report.model_sha256 =
        Some(sha256_file(&args.model).map_err(|error| RunFailure::new("hash_model", None, error))?);
    let generated =
        generate_dump(args, artifacts).map_err(|error| RunFailure::new("generate", None, error))?;
    report.dump_sha256 = Some(generated.dump_sha256.clone());
    report.planner_rows_processed = Some(generated.planner_rows_processed);
    report.diagnostics = generated.diagnostics.clone();
    let validation_has_errors = generated.validation.has_errors();
    report.validation = Some(generated.validation);
    if validation_has_errors {
        report.outcome = Outcome::GenerationRejected {
            detail: format!(
                "SQL Splitter validation rejected the generated dump; see {}",
                generated.sql_path.display()
            ),
        };
        return Ok(());
    }

    let elyra = EmbeddedElyra::start()
        .await
        .map_err(|error| RunFailure::new("start_elyra", Some(Target::Elyra), error))?;
    let mut subject = elyra
        .connect()
        .await
        .map_err(|error| RunFailure::new("connect_elyra", Some(Target::Elyra), error))?;
    let mysql_url = args
        .mysql_url
        .clone()
        .or_else(|| std::env::var("ELYRA_STRESS_MYSQL_URL").ok())
        .ok_or_else(|| {
            RunFailure::new(
                "configure_mysql",
                Some(Target::Mysql),
                anyhow::anyhow!("pass --mysql-url or set ELYRA_STRESS_MYSQL_URL"),
            )
        })?;
    let mysql_opts = Opts::from_url(&mysql_url)
        .map_err(|error| RunFailure::new("configure_mysql", Some(Target::Mysql), error))?;
    let mut oracle = Conn::new(mysql_opts)
        .await
        .map_err(|error| RunFailure::new("connect_mysql", Some(Target::Mysql), error))?;

    let timeout = Duration::from_secs(args.timeout_seconds.get());

    ensure_empty(&mut oracle, "MySQL oracle", timeout)
        .await
        .map_err(|error| RunFailure::new("check_empty", Some(Target::Mysql), error))?;
    ensure_empty(&mut subject, "ElyraSQL", timeout)
        .await
        .map_err(|error| RunFailure::new("check_empty", Some(Target::Elyra), error))?;

    report.mysql_version = Some(
        version(&mut oracle, timeout)
            .await
            .map_err(|error| RunFailure::new("version", Some(Target::Mysql), error))?,
    );
    report.elyra_version = Some(
        version(&mut subject, timeout)
            .await
            .map_err(|error| RunFailure::new("version", Some(Target::Elyra), error))?,
    );
    report.mysql_environment = Some(
        mysql_environment(&mut oracle, timeout)
            .await
            .map_err(|error| RunFailure::new("mysql_environment", Some(Target::Mysql), error))?,
    );
    let result = import_and_compare(
        &generated.sql_path,
        artifacts,
        &mut oracle,
        &mut subject,
        args.profile_iterations,
        timeout,
    )
    .await
    .map_err(|error| RunFailure::new("compare", None, error))?;
    report.statements_executed = result.statements_executed;
    report.performance = result.performance;
    report.outcome = result.outcome;
    Ok(())
}

fn generate_dump(args: &Args, artifacts: &Path) -> Result<GeneratedDump> {
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
    let sql_path = artifacts.join("generated.sql");
    let sql_file =
        File::create(&sql_path).with_context(|| format!("create {}", sql_path.display()))?;
    let mut renderer = SqlRenderer::new(HashingWriter::new(sql_file), render);
    let engine_report = GenerationEngine::new(plan).run(&mut renderer)?;
    let writer = renderer.finish()?;
    let (_sql_file, dump_sha256) = writer.finish();

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
    Ok(GeneratedDump {
        sql_path,
        dump_sha256,
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
    timeout: Duration,
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
        let mysql_result = tokio::time::timeout(timeout, oracle.query_drop(&statement)).await;
        mysql_import.push(elapsed_ns(started));
        match mysql_result {
            Ok(Ok(())) => {}
            Ok(Err(MysqlError::Server(error))) => {
                write_failure(artifacts, &statement)?;
                return Ok(import_result(
                    statement_index,
                    Outcome::OracleRejected {
                        statement: statement_index,
                        error: sql_rejection(error),
                    },
                    &mysql_import,
                    &elyra_import,
                    profile_iterations,
                ));
            }
            Ok(Err(error)) => {
                write_failure(artifacts, &statement)?;
                return Ok(import_result(
                    statement_index,
                    Outcome::InfrastructureFailed {
                        stage: "import".to_owned(),
                        target: Some(Target::Mysql),
                        statement: Some(statement_index),
                        error: error.to_string(),
                    },
                    &mysql_import,
                    &elyra_import,
                    profile_iterations,
                ));
            }
            Err(_) => {
                write_failure(artifacts, &statement)?;
                return Ok(import_result(
                    statement_index,
                    Outcome::TimedOut {
                        stage: "import".to_owned(),
                        target: Some(Target::Mysql),
                        statement: Some(statement_index),
                        timeout_seconds: timeout.as_secs(),
                    },
                    &mysql_import,
                    &elyra_import,
                    profile_iterations,
                ));
            }
        }
        let started = Instant::now();
        let elyra_result = tokio::time::timeout(timeout, subject.query_drop(&statement)).await;
        elyra_import.push(elapsed_ns(started));
        match elyra_result {
            Ok(Ok(())) => {}
            Ok(Err(MysqlError::Server(error))) => {
                write_failure(artifacts, &statement)?;
                return Ok(import_result(
                    statement_index,
                    Outcome::ElyraRejected {
                        statement: statement_index,
                        error: sql_rejection(error),
                    },
                    &mysql_import,
                    &elyra_import,
                    profile_iterations,
                ));
            }
            Ok(Err(error)) => {
                write_failure(artifacts, &statement)?;
                return Ok(import_result(
                    statement_index,
                    Outcome::InfrastructureFailed {
                        stage: "import".to_owned(),
                        target: Some(Target::Elyra),
                        statement: Some(statement_index),
                        error: error.to_string(),
                    },
                    &mysql_import,
                    &elyra_import,
                    profile_iterations,
                ));
            }
            Err(_) => {
                write_failure(artifacts, &statement)?;
                return Ok(import_result(
                    statement_index,
                    Outcome::TimedOut {
                        stage: "import".to_owned(),
                        target: Some(Target::Elyra),
                        statement: Some(statement_index),
                        timeout_seconds: timeout.as_secs(),
                    },
                    &mysql_import,
                    &elyra_import,
                    profile_iterations,
                ));
            }
        }
    }

    macro_rules! comparison_query {
        ($future:expr, $stage:literal, $target:expr) => {
            match $future.await {
                Ok(value) => value,
                Err(error) => {
                    return Ok(import_result(
                        statement_index,
                        query_failure($stage, $target, error, timeout),
                        &mysql_import,
                        &elyra_import,
                        profile_iterations,
                    ));
                }
            }
        };
    }

    let oracle_tables = comparison_query!(
        table_names(oracle, timeout),
        "table_metadata",
        Target::Mysql
    );
    let subject_tables = comparison_query!(
        table_names(subject, timeout),
        "table_metadata",
        Target::Elyra
    );
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
            profile_iterations,
        ));
    }

    let oracle_schema = comparison_query!(
        schema_snapshot(oracle, timeout),
        "schema_metadata",
        Target::Mysql
    );
    let subject_schema = comparison_query!(
        schema_snapshot(subject, timeout),
        "schema_metadata",
        Target::Elyra
    );
    if oracle_schema != subject_schema {
        return Ok(import_result(
            statement_index,
            Outcome::MetadataMismatch {
                detail: first_schema_difference(&oracle_schema, &subject_schema),
            },
            &mysql_import,
            &elyra_import,
            profile_iterations,
        ));
    }

    let oracle_counts = comparison_query!(
        row_counts(oracle, &oracle_tables, timeout),
        "row_counts",
        Target::Mysql
    );
    let subject_counts = comparison_query!(
        row_counts(subject, &subject_tables, timeout),
        "row_counts",
        Target::Elyra
    );
    if oracle_counts != subject_counts {
        return Ok(import_result(
            statement_index,
            Outcome::RowCountMismatch {
                mysql: oracle_counts,
                elyra: subject_counts,
            },
            &mysql_import,
            &elyra_import,
            profile_iterations,
        ));
    }

    let oracle_digests = comparison_query!(
        content_digests(oracle, &oracle_tables, &oracle_schema.columns, timeout),
        "content_digest",
        Target::Mysql
    );
    let subject_digests = comparison_query!(
        content_digests(subject, &subject_tables, &subject_schema.columns, timeout),
        "content_digest",
        Target::Elyra
    );
    if let Some((table, mysql_sha256)) = oracle_digests
        .iter()
        .find(|(table, digest)| subject_digests.get(*table) != Some(*digest))
    {
        let column_types = table_column_types(table, &oracle_schema.columns);
        let mysql_hashes = comparison_query!(
            sorted_row_hashes(oracle, table, &column_types, timeout),
            "difference_diagnostic",
            Target::Mysql
        );
        let elyra_hashes = comparison_query!(
            sorted_row_hashes(subject, table, &column_types, timeout),
            "difference_diagnostic",
            Target::Elyra
        );
        let position = mysql_hashes
            .iter()
            .zip(&elyra_hashes)
            .position(|(left, right)| left != right)
            .unwrap_or_else(|| mysql_hashes.len().min(elyra_hashes.len()));
        let mysql_row = comparison_query!(
            find_row_with_hash(
                oracle,
                table,
                mysql_hashes.get(position).copied(),
                &column_types,
                timeout,
            ),
            "difference_diagnostic",
            Target::Mysql
        );
        let elyra_row = comparison_query!(
            find_row_with_hash(
                subject,
                table,
                elyra_hashes.get(position).copied(),
                &column_types,
                timeout,
            ),
            "difference_diagnostic",
            Target::Elyra
        );
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
            profile_iterations,
        ));
    }

    let tables = if profile_iterations == 0 {
        BTreeMap::new()
    } else {
        match profile_tables(
            oracle,
            subject,
            &oracle_tables,
            &oracle_schema.columns,
            &oracle_schema.indexes,
            profile_iterations,
            timeout,
        )
        .await
        {
            Ok(tables) => tables,
            Err(failure) => {
                let outcome = if failure
                    .error
                    .chain()
                    .any(|source| source.is::<tokio::time::error::Elapsed>())
                {
                    Outcome::TimedOut {
                        stage: "profile".to_owned(),
                        target: failure.target,
                        statement: None,
                        timeout_seconds: timeout.as_secs(),
                    }
                } else {
                    Outcome::ProfileFailed {
                        detail: format!("{:#}", failure.error),
                    }
                };
                return Ok(ImportResult {
                    statements_executed: statement_index,
                    outcome,
                    performance: PerformanceReport {
                        import_statements: TimingComparison::new(&mysql_import, &elyra_import),
                        query_iterations: profile_iterations,
                        tables: failure.tables,
                    },
                });
            }
        }
    };
    Ok(ImportResult {
        statements_executed: statement_index,
        outcome: Outcome::Passed {
            table_count: oracle_tables.len(),
            column_count: oracle_schema.columns.len(),
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
    query_iterations: u16,
) -> ImportResult {
    ImportResult {
        statements_executed,
        outcome,
        performance: PerformanceReport {
            import_statements: TimingComparison::new(mysql_import, elyra_import),
            query_iterations,
            tables: BTreeMap::new(),
        },
    }
}

fn sql_rejection(error: ServerError) -> SqlRejection {
    SqlRejection {
        code: error.code,
        state: error.state,
        message: error.message,
    }
}

fn query_failure(stage: &str, target: Target, error: anyhow::Error, timeout: Duration) -> Outcome {
    if error
        .chain()
        .any(|source| source.is::<tokio::time::error::Elapsed>())
    {
        Outcome::TimedOut {
            stage: stage.to_owned(),
            target: Some(target),
            statement: None,
            timeout_seconds: timeout.as_secs(),
        }
    } else {
        Outcome::InfrastructureFailed {
            stage: stage.to_owned(),
            target: Some(target),
            statement: None,
            error: format!("{error:#}"),
        }
    }
}

async fn profile_tables(
    oracle: &mut Conn,
    subject: &mut Conn,
    tables: &[String],
    columns: &[ColumnMetadata],
    indexes: &[IndexMetadata],
    iterations: u16,
    timeout: Duration,
) -> std::result::Result<BTreeMap<String, TableProfile>, ProfileFailure> {
    let mut profiles = BTreeMap::new();
    macro_rules! profile_step {
        ($future:expr, $table:expr) => {
            match $future.await {
                Ok(value) => value,
                Err(failure) => {
                    return Err(ProfileFailure {
                        tables: profiles,
                        target: failure.target,
                        error: failure.error.context(format!("profile table {}", $table)),
                    });
                }
            }
        };
    }

    for table in tables {
        let quoted_table = quote_identifier(table);
        let column_types = table_column_types(table, columns);
        let count = profile_step!(
            profile_query(
                oracle,
                subject,
                &format!("SELECT COUNT(*) FROM {quoted_table}"),
                None,
                iterations,
                timeout,
            ),
            table
        );

        let (ordered_page, point_lookup) = match profile_key(table, indexes, columns) {
            Some(key) => {
                let quoted_key = key
                    .iter()
                    .map(|column| quote_identifier(column))
                    .collect::<Vec<_>>();
                let order_by = quoted_key.join(", ");
                let ordered_page_sql =
                    format!("SELECT * FROM {quoted_table} ORDER BY {order_by} LIMIT 100");
                let ordered_page = Some(profile_step!(
                    profile_query(
                        oracle,
                        subject,
                        &ordered_page_sql,
                        Some(&column_types),
                        iterations,
                        timeout,
                    ),
                    table
                ));

                let first_row: Option<Row> = profile_step!(
                    profile_first_row(
                        oracle,
                        format!(
                            "SELECT {} FROM {quoted_table} ORDER BY {order_by} LIMIT 1",
                            quoted_key.join(", ")
                        ),
                        timeout,
                    ),
                    table
                );
                let point_lookup = match first_row {
                    Some(row) => {
                        let values = row.unwrap();
                        let predicates = quoted_key
                            .iter()
                            .zip(&values)
                            .map(|(column, value)| match value {
                                Value::NULL => format!("{column} IS NULL"),
                                value => format!("{column} = {}", mysql_value_literal(value)),
                            })
                            .collect::<Vec<_>>()
                            .join(" AND ");
                        let sql =
                            format!("SELECT * FROM {quoted_table} WHERE {predicates} LIMIT 1");
                        Some(profile_step!(
                            profile_query(
                                oracle,
                                subject,
                                &sql,
                                Some(&column_types),
                                iterations,
                                timeout,
                            ),
                            table
                        ))
                    }
                    None => None,
                };
                (ordered_page, point_lookup)
            }
            None => (None, None),
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
    column_types: Option<&[String]>,
    iterations: u16,
    timeout: Duration,
) -> std::result::Result<TimingComparison, ProfileQueryFailure> {
    let (mysql_warmup, _) = timed_rows(oracle, sql, timeout)
        .await
        .map_err(|error| ProfileQueryFailure::new(Some(Target::Mysql), error))?;
    let (elyra_warmup, _) = timed_rows(subject, sql, timeout)
        .await
        .map_err(|error| ProfileQueryFailure::new(Some(Target::Elyra), error))?;
    ensure_same_query_result(sql, mysql_warmup, elyra_warmup, column_types)
        .map_err(|error| ProfileQueryFailure::new(None, error))?;

    let mut mysql_samples = Vec::with_capacity(iterations as usize);
    let mut elyra_samples = Vec::with_capacity(iterations as usize);
    for iteration in 0..iterations {
        let (mysql_rows, mysql_ns, elyra_rows, elyra_ns) = if iteration % 2 == 0 {
            let (mysql_rows, mysql_ns) = timed_rows(oracle, sql, timeout)
                .await
                .map_err(|error| ProfileQueryFailure::new(Some(Target::Mysql), error))?;
            let (elyra_rows, elyra_ns) = timed_rows(subject, sql, timeout)
                .await
                .map_err(|error| ProfileQueryFailure::new(Some(Target::Elyra), error))?;
            (mysql_rows, mysql_ns, elyra_rows, elyra_ns)
        } else {
            let (elyra_rows, elyra_ns) = timed_rows(subject, sql, timeout)
                .await
                .map_err(|error| ProfileQueryFailure::new(Some(Target::Elyra), error))?;
            let (mysql_rows, mysql_ns) = timed_rows(oracle, sql, timeout)
                .await
                .map_err(|error| ProfileQueryFailure::new(Some(Target::Mysql), error))?;
            (mysql_rows, mysql_ns, elyra_rows, elyra_ns)
        };
        ensure_same_query_result(sql, mysql_rows, elyra_rows, column_types)
            .map_err(|error| ProfileQueryFailure::new(None, error))?;
        mysql_samples.push(mysql_ns);
        elyra_samples.push(elyra_ns);
    }
    Ok(TimingComparison::new(&mysql_samples, &elyra_samples))
}

async fn profile_first_row(
    connection: &mut Conn,
    sql: String,
    timeout: Duration,
) -> std::result::Result<Option<Row>, ProfileQueryFailure> {
    tokio::time::timeout(timeout, connection.query_first(&sql))
        .await
        .context("point-lookup key query timed out")
        .map_err(|error| ProfileQueryFailure::new(Some(Target::Mysql), error))?
        .with_context(|| format!("point-lookup key query failed: {sql}"))
        .map_err(|error| ProfileQueryFailure::new(Some(Target::Mysql), error))
}

async fn timed_rows(
    connection: &mut Conn,
    sql: &str,
    timeout: Duration,
) -> Result<(Vec<Row>, u64)> {
    let started = Instant::now();
    let rows = tokio::time::timeout(timeout, connection.query(sql))
        .await
        .context("profile query timed out")?
        .with_context(|| format!("profile query failed: {sql}"))?;
    Ok((rows, elapsed_ns(started)))
}

fn ensure_same_query_result(
    sql: &str,
    mysql_rows: Vec<Row>,
    elyra_rows: Vec<Row>,
    column_types: Option<&[String]>,
) -> Result<()> {
    let mysql_digest =
        digest_rows_ordered_typed(mysql_rows.into_iter().map(Row::unwrap), column_types);
    let elyra_digest =
        digest_rows_ordered_typed(elyra_rows.into_iter().map(Row::unwrap), column_types);
    if mysql_digest != elyra_digest {
        bail!("profile query returned different results: {sql}")
    }
    Ok(())
}

fn profile_key(
    table: &str,
    indexes: &[IndexMetadata],
    columns: &[ColumnMetadata],
) -> Option<Vec<String>> {
    let mut candidates = indexes
        .iter()
        .filter(|index| {
            index.table == table
                && index.unique
                && index.prefix_lengths.iter().all(Option::is_none)
                && index.columns.iter().all(|name| {
                    columns
                        .iter()
                        .find(|column| column.table == table && column.name == *name)
                        .is_some_and(|column| !column.nullable)
                })
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|index| (!index.primary, &index.columns));
    candidates.first().map(|index| index.columns.clone())
}

fn table_column_types(table: &str, columns: &[ColumnMetadata]) -> Vec<String> {
    columns
        .iter()
        .filter(|column| column.table == table)
        .map(|column| column.column_type.clone())
        .collect()
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

async fn ensure_empty(connection: &mut Conn, label: &str, timeout: Duration) -> Result<()> {
    let tables = table_names(connection, timeout).await?;
    if !tables.is_empty() {
        bail!("{label} database is not empty: {tables:?}")
    }
    Ok(())
}

async fn version(connection: &mut Conn, timeout: Duration) -> Result<String> {
    tokio::time::timeout(timeout, connection.query_first("SELECT VERSION()"))
        .await
        .context("VERSION() query timed out")??
        .context("VERSION() returned no row")
}

async fn mysql_environment(connection: &mut Conn, timeout: Duration) -> Result<MysqlEnvironment> {
    let (sql_mode, time_zone, character_set, collation): (String, String, String, String) =
        tokio::time::timeout(
            timeout,
            connection.query_first(
                "SELECT @@SESSION.sql_mode, @@SESSION.time_zone, \
                 @@SESSION.character_set_connection, @@SESSION.collation_connection",
            ),
        )
        .await
        .context("MySQL environment query timed out")??
        .context("MySQL environment query returned no row")?;
    Ok(MysqlEnvironment {
        sql_mode,
        time_zone,
        character_set,
        collation,
    })
}

async fn table_names(connection: &mut Conn, timeout: Duration) -> Result<Vec<String>> {
    tokio::time::timeout(
        timeout,
        connection.query(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = SCHEMA() AND TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_NAME",
        ),
    )
    .await
    .context("table metadata query timed out")?
    .context("query table metadata")
}

async fn schema_snapshot(connection: &mut Conn, timeout: Duration) -> Result<SchemaSnapshot> {
    type RawColumn = (
        String,
        u64,
        String,
        String,
        String,
        Option<String>,
        String,
        String,
        Option<String>,
        Option<String>,
    );
    let raw_columns: Vec<RawColumn> = tokio::time::timeout(
        timeout,
        connection.query(
            "SELECT TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME, COLUMN_TYPE, \
                    IS_NULLABLE, COLUMN_DEFAULT, EXTRA, \
                    GENERATION_EXPRESSION, CHARACTER_SET_NAME, COLLATION_NAME \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = SCHEMA() \
             ORDER BY TABLE_NAME, ORDINAL_POSITION",
        ),
    )
    .await
    .context("column metadata query timed out")?
    .context("query column metadata")?;
    let columns = raw_columns
        .into_iter()
        .map(
            |(
                table,
                ordinal,
                name,
                column_type,
                nullable,
                default,
                extra,
                generation_expression,
                charset,
                collation,
            )| {
                let column_type = canonical_column_type(&column_type);
                let is_text = column_type == "text";
                ColumnMetadata {
                    table,
                    ordinal,
                    name,
                    column_type,
                    nullable: nullable.eq_ignore_ascii_case("YES"),
                    default: canonical_default(default.as_deref()),
                    extra: canonical_extra(&extra),
                    generation_expression: canonical_expression(&generation_expression),
                    charset: is_text
                        .then(|| canonical_charset(charset.as_deref()))
                        .flatten(),
                    collation: is_text
                        .then(|| canonical_collation(collation.as_deref()))
                        .flatten(),
                }
            },
        )
        .collect();

    type RawIndex = (String, String, u64, u64, String, Option<u64>, String);
    let raw_indexes: Vec<RawIndex> = tokio::time::timeout(
        timeout,
        connection.query(
            "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, \
                    COLUMN_NAME, SUB_PART, INDEX_TYPE \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = SCHEMA() \
             ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
        ),
    )
    .await
    .context("index metadata query timed out")?
    .context("query index metadata")?;
    type RawKey = (String, String, String, u64, Option<String>, Option<String>);
    let raw_keys: Vec<RawKey> = tokio::time::timeout(
        timeout,
        connection.query(
            "SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, \
                    REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
             FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = SCHEMA() \
             ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION",
        ),
    )
    .await
    .context("key metadata query timed out")?
    .context("query key metadata")?;

    let raw_references: Vec<(String, String, String)> = tokio::time::timeout(
        timeout,
        connection.query(
            "SELECT CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE \
             FROM information_schema.REFERENTIAL_CONSTRAINTS \
             WHERE CONSTRAINT_SCHEMA = SCHEMA() \
             ORDER BY CONSTRAINT_NAME",
        ),
    )
    .await
    .context("foreign-key metadata query timed out")?
    .context("query foreign-key metadata")?;
    let reference_actions = raw_references
        .into_iter()
        .map(|(name, update_rule, delete_rule)| {
            (
                name,
                (
                    canonical_reference_action(&update_rule),
                    canonical_reference_action(&delete_rule),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut declared_fk_columns = BTreeMap::<(String, String), Vec<(u64, String)>>::new();
    for (name, table, column, ordinal, referenced_table, _) in &raw_keys {
        if referenced_table.is_some() {
            declared_fk_columns
                .entry((table.clone(), name.clone()))
                .or_default()
                .push((*ordinal, column.clone()));
        }
    }
    let declared_fk_columns = declared_fk_columns
        .into_iter()
        .map(|((table, _), mut columns)| {
            columns.sort_unstable_by_key(|(ordinal, _)| *ordinal);
            (
                table,
                columns
                    .into_iter()
                    .map(|(_, column)| column)
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    type IndexGroup = (bool, String, Vec<(u64, String, Option<u64>)>);
    let mut index_groups = BTreeMap::<(String, String), IndexGroup>::new();
    for (table, name, non_unique, sequence, column, prefix_length, index_type) in raw_indexes {
        let group = index_groups
            .entry((table, name))
            .or_insert_with(|| (non_unique == 0, index_type.to_ascii_uppercase(), Vec::new()));
        group.2.push((sequence, column, prefix_length));
    }
    let mut indexes = index_groups
        .into_iter()
        .filter_map(|((table, name), (unique, index_type, mut columns))| {
            columns.sort_unstable_by_key(|(sequence, _, _)| *sequence);
            let column_names = columns
                .iter()
                .map(|(_, column, _)| column.clone())
                .collect::<Vec<_>>();
            let backs_foreign_key =
                !unique && is_implicit_fk_index(&table, &column_names, &declared_fk_columns);
            if backs_foreign_key {
                return None;
            }
            Some(IndexMetadata {
                table,
                primary: name.eq_ignore_ascii_case("PRIMARY"),
                unique,
                columns: column_names,
                prefix_lengths: columns
                    .into_iter()
                    .map(|(_, _, prefix_length)| prefix_length)
                    .collect(),
                index_type,
            })
        })
        .collect::<Vec<_>>();
    indexes.sort();

    type ForeignKeyGroup = (Vec<(u64, String, String)>, String);
    let mut foreign_key_groups = BTreeMap::<(String, String), ForeignKeyGroup>::new();
    for (name, table, column, ordinal, referenced_table, referenced_column) in raw_keys {
        let (Some(referenced_table), Some(referenced_column)) =
            (referenced_table, referenced_column)
        else {
            continue;
        };
        let group = foreign_key_groups
            .entry((table, name))
            .or_insert_with(|| (Vec::new(), referenced_table));
        group.0.push((ordinal, column, referenced_column));
    }
    let mut references = Vec::with_capacity(foreign_key_groups.len());
    for ((table, name), (mut columns, referenced_table)) in foreign_key_groups {
        columns.sort_unstable_by_key(|(ordinal, _, _)| *ordinal);
        let (update_rule, delete_rule) = reference_actions
            .get(&name)
            .cloned()
            .with_context(|| format!("missing referential metadata for constraint {name}"))?;
        references.push(ReferenceMetadata {
            table,
            columns: columns
                .iter()
                .map(|(_, column, _)| column.clone())
                .collect(),
            referenced_table,
            referenced_columns: columns
                .into_iter()
                .map(|(_, _, referenced_column)| referenced_column)
                .collect(),
            update_rule,
            delete_rule,
        });
    }
    references.sort();

    Ok(SchemaSnapshot {
        columns,
        indexes,
        references,
    })
}

fn is_implicit_fk_index(
    table: &str,
    columns: &[String],
    declared_foreign_keys: &[(String, Vec<String>)],
) -> bool {
    declared_foreign_keys
        .iter()
        .any(|(fk_table, fk_columns)| fk_table == table && columns == fk_columns)
}

fn canonical_column_type(column_type: &str) -> String {
    let normalized = column_type
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let unsigned = normalized.ends_with(" unsigned");
    let base = normalized.strip_suffix(" unsigned").unwrap_or(&normalized);
    let family = base.split(['(', ' ']).next().unwrap_or(base);
    match family {
        "bool" | "boolean" => "boolean".to_owned(),
        "tinyint" if base == "tinyint(1)" => "boolean".to_owned(),
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
            if unsigned {
                "unsigned_integer".to_owned()
            } else {
                "integer".to_owned()
            }
        }
        "float" | "double" | "real" => "float".to_owned(),
        "decimal" | "numeric" => base.replace(' ', ""),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set" => {
            "text".to_owned()
        }
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            "bytes".to_owned()
        }
        "timestamp" | "datetime" => "datetime".to_owned(),
        "date" | "time" | "json" => family.to_owned(),
        _ => normalized,
    }
}

fn canonical_default(default: Option<&str>) -> Option<String> {
    default.map(|value| {
        let value = value.trim();
        let unquoted = value
            .strip_prefix('\'')
            .and_then(|value| value.strip_suffix('\''))
            .unwrap_or(value)
            .replace("''", "'");
        let lower = unquoted.to_ascii_lowercase();
        if lower == "current_timestamp()" {
            "current_timestamp".to_owned()
        } else {
            unquoted
        }
    })
}

fn canonical_extra(extra: &str) -> String {
    extra
        .split_whitespace()
        .map(str::to_ascii_lowercase)
        .filter(|part| part != "default_generated")
        .collect::<Vec<_>>()
        .join(" ")
}

fn canonical_expression(expression: &str) -> String {
    expression
        .trim()
        .replace('`', "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn canonical_charset(charset: Option<&str>) -> Option<String> {
    charset.map(str::to_ascii_lowercase)
}

fn canonical_collation(collation: Option<&str>) -> Option<String> {
    collation.map(|value| {
        let value = value.to_ascii_lowercase();
        if value == "binary" || value.ends_with("_bin") || value.ends_with("_cs") {
            "binary".to_owned()
        } else {
            "case_insensitive".to_owned()
        }
    })
}

fn canonical_reference_action(action: &str) -> String {
    match action.trim().to_ascii_uppercase().as_str() {
        "NO ACTION" | "RESTRICT" => "restrict".to_owned(),
        action => action.to_ascii_lowercase(),
    }
}

fn first_schema_difference(oracle: &SchemaSnapshot, subject: &SchemaSnapshot) -> String {
    if oracle.columns != subject.columns {
        first_sequence_difference("columns", &oracle.columns, &subject.columns)
    } else if oracle.indexes != subject.indexes {
        first_sequence_difference("indexes", &oracle.indexes, &subject.indexes)
    } else {
        first_sequence_difference("foreign keys", &oracle.references, &subject.references)
    }
}

async fn row_counts(
    connection: &mut Conn,
    tables: &[String],
    timeout: Duration,
) -> Result<BTreeMap<String, u64>> {
    let mut counts = BTreeMap::new();
    for table in tables {
        let quoted = quote_identifier(table);
        let count = tokio::time::timeout(
            timeout,
            connection.query_first(format!("SELECT COUNT(*) FROM {quoted}")),
        )
        .await
        .with_context(|| format!("COUNT(*) timed out for {table}"))??
        .with_context(|| format!("COUNT(*) returned no row for {table}"))?;
        counts.insert(table.clone(), count);
    }
    Ok(counts)
}

async fn content_digests(
    connection: &mut Conn,
    tables: &[String],
    columns: &[ColumnMetadata],
    timeout: Duration,
) -> Result<BTreeMap<String, String>> {
    let mut digests = BTreeMap::new();
    for table in tables {
        let column_types = table_column_types(table, columns);
        let row_hashes = sorted_row_hashes(connection, table, &column_types, timeout).await?;
        digests.insert(table.clone(), digest_hashes(&row_hashes));
    }
    Ok(digests)
}

#[cfg(test)]
fn digest_rows(rows: impl IntoIterator<Item = Vec<Value>>) -> String {
    let mut row_digests = rows
        .into_iter()
        .map(|row| digest_row(&row))
        .collect::<Vec<_>>();
    row_digests.sort_unstable();

    digest_hashes(&row_digests)
}

#[cfg(test)]
fn digest_rows_ordered(rows: impl IntoIterator<Item = Vec<Value>>) -> String {
    digest_rows_ordered_typed(rows, None)
}

fn digest_rows_ordered_typed(
    rows: impl IntoIterator<Item = Vec<Value>>,
    column_types: Option<&[String]>,
) -> String {
    let row_digests = rows
        .into_iter()
        .map(|row| match column_types {
            Some(column_types) => digest_row_typed(&row, column_types),
            None => digest_row(&row),
        })
        .collect::<Vec<_>>();
    digest_hashes(&row_digests)
}

fn digest_hashes(row_digests: &[[u8; 32]]) -> String {
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

fn digest_row_typed(row: &[Value], column_types: &[String]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((row.len() as u64).to_le_bytes());
    if row.len() != column_types.len() {
        hasher.update(b"column-type-count-mismatch");
        hasher.update((column_types.len() as u64).to_le_bytes());
    }
    for (index, value) in row.iter().enumerate() {
        match column_types.get(index).map(String::as_str) {
            Some(column_type) => update_typed_value_digest(&mut hasher, column_type, value),
            None => update_value_digest(&mut hasher, value),
        }
    }
    hasher.finalize().into()
}

async fn sorted_row_hashes(
    connection: &mut Conn,
    table: &str,
    column_types: &[String],
    timeout: Duration,
) -> Result<Vec<[u8; 32]>> {
    let quoted = quote_identifier(table);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut result = tokio::time::timeout_at(
        deadline,
        connection.query_iter(format!("SELECT * FROM {quoted}")),
    )
    .await
    .with_context(|| format!("query rows timed out for {table}"))?
    .with_context(|| format!("query diagnostic rows from {table}"))?;
    let mut hashes = Vec::new();
    while let Some(row) = tokio::time::timeout_at(deadline, result.next())
        .await
        .with_context(|| format!("reading rows timed out for {table}"))?
        .with_context(|| format!("read rows from {table}"))?
    {
        hashes.push(digest_row_typed(&row.unwrap(), column_types));
    }
    hashes.sort_unstable();
    Ok(hashes)
}

async fn find_row_with_hash(
    connection: &mut Conn,
    table: &str,
    target: Option<[u8; 32]>,
    column_types: &[String],
    timeout: Duration,
) -> Result<Option<String>> {
    let Some(target) = target else {
        return Ok(None);
    };
    let quoted = quote_identifier(table);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut result = tokio::time::timeout_at(
        deadline,
        connection.query_iter(format!("SELECT * FROM {quoted}")),
    )
    .await
    .with_context(|| format!("diagnostic query timed out for {table}"))?
    .with_context(|| format!("query diagnostic rows from {table}"))?;
    while let Some(row) = tokio::time::timeout_at(deadline, result.next())
        .await
        .with_context(|| format!("reading diagnostic rows timed out for {table}"))?
        .with_context(|| format!("read diagnostic rows from {table}"))?
    {
        let row = row.unwrap();
        if digest_row_typed(&row, column_types) == target {
            return Ok(Some(format_row(&row)));
        }
    }
    Ok(None)
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

fn update_typed_value_digest(hasher: &mut Sha256, column_type: &str, value: &Value) {
    hasher.update((column_type.len() as u64).to_le_bytes());
    hasher.update(column_type.as_bytes());
    if matches!(value, Value::NULL) {
        hasher.update([0]);
        return;
    }

    match column_type {
        "integer" | "unsigned_integer" | "boolean" => match value {
            Value::Int(value) => update_digest_bytes(hasher, 1, value.to_string().as_bytes()),
            Value::UInt(value) => update_digest_bytes(hasher, 1, value.to_string().as_bytes()),
            Value::Bytes(value) => update_digest_bytes(hasher, 1, value),
            value => update_value_digest(hasher, value),
        },
        "float" => match value {
            Value::Float(value) => {
                hasher.update([2]);
                hasher.update(f64::from(*value).to_bits().to_le_bytes());
            }
            Value::Double(value) => {
                hasher.update([2]);
                hasher.update(value.to_bits().to_le_bytes());
            }
            Value::Bytes(bytes) => match std::str::from_utf8(bytes)
                .ok()
                .and_then(|value| value.parse::<f64>().ok())
            {
                Some(value) => {
                    hasher.update([2]);
                    hasher.update(value.to_bits().to_le_bytes());
                }
                None => update_value_digest(hasher, value),
            },
            value => update_value_digest(hasher, value),
        },
        "date" | "datetime" | "time" => match canonical_temporal_value(column_type, value) {
            Some(value) => update_digest_bytes(hasher, 3, value.as_bytes()),
            None => update_value_digest(hasher, value),
        },
        "json" => match value {
            Value::Bytes(bytes) => match serde_json::from_slice::<serde_json::Value>(bytes) {
                Ok(value) => match serde_json::to_vec(&value) {
                    Ok(bytes) => update_digest_bytes(hasher, 4, &bytes),
                    Err(_) => update_digest_bytes(hasher, 4, bytes),
                },
                Err(_) => update_digest_bytes(hasher, 4, bytes),
            },
            value => update_value_digest(hasher, value),
        },
        _ => update_value_digest(hasher, value),
    }
}

fn canonical_temporal_value(column_type: &str, value: &Value) -> Option<String> {
    match value {
        Value::Bytes(bytes) => std::str::from_utf8(bytes).ok().map(normalize_temporal_text),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            let date = format!("{year:04}-{month:02}-{day:02}");
            if column_type == "date" {
                Some(date)
            } else {
                let mut value = format!("{date} {hour:02}:{minute:02}:{second:02}");
                if *micros != 0 {
                    value.push_str(&format!(".{micros:06}"));
                }
                Some(normalize_temporal_text(&value))
            }
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            let sign = if *negative { "-" } else { "" };
            let total_hours = days * 24 + u32::from(*hours);
            let mut value = format!("{sign}{total_hours:02}:{minutes:02}:{seconds:02}");
            if *micros != 0 {
                value.push_str(&format!(".{micros:06}"));
            }
            Some(normalize_temporal_text(&value))
        }
        _ => None,
    }
}

fn normalize_temporal_text(value: &str) -> String {
    let mut value = value.trim().replace('T', " ");
    if value.contains('.') {
        while value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
    }
    value
}

fn update_digest_bytes(hasher: &mut Sha256, tag: u8, bytes: &[u8]) {
    hasher.update([tag]);
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn update_value_digest(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::NULL => hasher.update([0]),
        Value::Bytes(bytes) => update_digest_bytes(hasher, 1, bytes),
        Value::Int(value) => update_digest_bytes(hasher, 2, value.to_string().as_bytes()),
        Value::UInt(value) => update_digest_bytes(hasher, 2, value.to_string().as_bytes()),
        Value::Float(value) => {
            hasher.update([3]);
            hasher.update(f64::from(*value).to_bits().to_le_bytes());
        }
        Value::Double(value) => {
            hasher.update([3]);
            hasher.update(value.to_bits().to_le_bytes());
        }
        Value::Date(year, month, day, hour, minute, second, micros) => {
            hasher.update([4]);
            hasher.update(year.to_le_bytes());
            hasher.update([*month, *day, *hour, *minute, *second]);
            hasher.update(micros.to_le_bytes());
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => {
            hasher.update([5, u8::from(*negative)]);
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
    use std::{fs, num::NonZeroU32, num::NonZeroU64, path::PathBuf};

    use clap::{error::ErrorKind, Parser as _};
    use mysql_async::Value;

    use super::{
        canonical_column_type, digest_row_typed, digest_rows, digest_rows_ordered,
        first_sequence_difference, generate_dump, is_implicit_fk_index, profile_key, Args,
        ArtifactDirectory, ColumnMetadata, ConfigLoader, DumpMode, IndexMetadata, TimingStats,
        DEFAULT_TIMEOUT_SECONDS,
    };

    fn test_args(model: PathBuf, artifacts: PathBuf) -> Args {
        Args {
            model,
            artifacts: Some(artifacts),
            seed: 42,
            max_rows: 3,
            mode: DumpMode::SchemaOnly,
            batch_size: NonZeroU32::new(17).unwrap(),
            profile_iterations: 0,
            timeout_seconds: NonZeroU64::new(DEFAULT_TIMEOUT_SECONDS).unwrap(),
            mysql_url: None,
        }
    }

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
        let mut args = test_args(model, artifacts.clone());
        args.max_rows = 1;

        let generated = generate_dump(&args, &artifacts).unwrap();
        let sql = fs::read_to_string(generated.sql_path).unwrap();

        assert!(sql.contains("`description` VARCHAR(255) NOT NULL"));
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
    fn ordered_row_digest_is_order_sensitive() {
        let left = digest_rows_ordered([
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ]);
        let reordered = digest_rows_ordered([
            vec![Value::Int(3)],
            vec![Value::Int(2)],
            vec![Value::Int(1)],
        ]);

        assert_ne!(left, reordered);
    }

    #[test]
    fn typed_digest_normalizes_wire_representations() {
        let column_types = ["datetime", "time", "float", "integer", "json"]
            .map(str::to_owned)
            .to_vec();
        let mysql = digest_row_typed(
            &[
                Value::Date(2026, 8, 1, 12, 34, 56, 123_000),
                Value::Time(false, 0, 1, 2, 3, 0),
                Value::Float(1.5),
                Value::Int(7),
                Value::Bytes(br#"{"b":2,"a":1}"#.to_vec()),
            ],
            &column_types,
        );
        let elyra = digest_row_typed(
            &[
                Value::Bytes(b"2026-08-01T12:34:56.123".to_vec()),
                Value::Bytes(b"01:02:03.000000".to_vec()),
                Value::Bytes(b"1.5".to_vec()),
                Value::UInt(7),
                Value::Bytes(br#"{ "a": 1, "b": 2 }"#.to_vec()),
            ],
            &column_types,
        );
        let changed = digest_row_typed(
            &[
                Value::Bytes(b"2026-08-01 12:34:57.123".to_vec()),
                Value::Bytes(b"01:02:03".to_vec()),
                Value::Bytes(b"1.5".to_vec()),
                Value::UInt(7),
                Value::Bytes(br#"{"a":1,"b":2}"#.to_vec()),
            ],
            &column_types,
        );

        assert_eq!(mysql, elyra);
        assert_ne!(mysql, changed);
    }

    #[test]
    fn column_types_are_normalized_to_elyra_families() {
        assert_eq!(canonical_column_type("INT UNSIGNED"), "unsigned_integer");
        assert_eq!(canonical_column_type("bigint"), "integer");
        assert_eq!(canonical_column_type("TINYINT(1)"), "boolean");
        assert_eq!(canonical_column_type("VARCHAR(255)"), "text");
        assert_eq!(canonical_column_type("DECIMAL(30, 10)"), "decimal(30,10)");
    }

    #[test]
    fn profiling_prefers_the_complete_primary_key() {
        let columns = ["tenant_id", "sequence", "slug"]
            .into_iter()
            .map(|name| ColumnMetadata {
                table: "items".to_owned(),
                ordinal: 1,
                name: name.to_owned(),
                column_type: "integer".to_owned(),
                nullable: false,
                default: None,
                extra: String::new(),
                generation_expression: String::new(),
                charset: None,
                collation: None,
            })
            .collect::<Vec<_>>();
        let indexes = vec![
            IndexMetadata {
                table: "items".to_owned(),
                primary: false,
                unique: true,
                columns: vec!["slug".to_owned()],
                prefix_lengths: vec![None],
                index_type: "BTREE".to_owned(),
            },
            IndexMetadata {
                table: "items".to_owned(),
                primary: true,
                unique: true,
                columns: vec!["tenant_id".to_owned(), "sequence".to_owned()],
                prefix_lengths: vec![None, None],
                index_type: "BTREE".to_owned(),
            },
        ];

        assert_eq!(
            profile_key("items", &indexes, &columns),
            Some(vec!["tenant_id".to_owned(), "sequence".to_owned()])
        );
    }

    #[test]
    fn profiling_skips_nullable_or_prefix_unique_keys() {
        let columns = vec![ColumnMetadata {
            table: "items".to_owned(),
            ordinal: 1,
            name: "slug".to_owned(),
            column_type: "text".to_owned(),
            nullable: true,
            default: None,
            extra: String::new(),
            generation_expression: String::new(),
            charset: Some("utf8mb4".to_owned()),
            collation: Some("case_insensitive".to_owned()),
        }];
        let index = IndexMetadata {
            table: "items".to_owned(),
            primary: false,
            unique: true,
            columns: vec!["slug".to_owned()],
            prefix_lengths: vec![Some(16)],
            index_type: "BTREE".to_owned(),
        };

        assert_eq!(profile_key("items", &[index], &columns), None);
    }

    #[test]
    fn only_exact_foreign_key_backing_indexes_are_implicit() {
        let foreign_keys = vec![("items".to_owned(), vec!["owner_id".to_owned()])];

        assert!(is_implicit_fk_index(
            "items",
            &["owner_id".to_owned()],
            &foreign_keys
        ));
        assert!(!is_implicit_fk_index(
            "items",
            &["owner_id".to_owned(), "created_at".to_owned()],
            &foreign_keys
        ));
    }

    #[test]
    fn explicit_artifact_directories_are_locked() {
        let temp_dir = tempfile::tempdir().unwrap();
        let artifacts = temp_dir.path().join("artifacts");
        let first = ArtifactDirectory::prepare(artifacts.clone()).unwrap();
        let error = ArtifactDirectory::prepare(artifacts.clone()).err().unwrap();
        assert!(error.to_string().contains("another run may be using"));

        drop(first);
        ArtifactDirectory::prepare(artifacts).unwrap();
    }

    #[test]
    fn every_bundled_fixture_is_self_contained() {
        let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures");
        let mut models = fs::read_dir(&fixtures)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "yaml")
            })
            .collect::<Vec<_>>();
        models.sort();

        assert_eq!(models.len(), 8);
        for model in models {
            ConfigLoader::load(&model)
                .unwrap_or_else(|error| panic!("{} did not load: {error}", model.display()));
        }
    }

    #[test]
    fn relational_fixture_renders_declared_keys_and_relationships() {
        let temp_dir = tempfile::tempdir().unwrap();
        let artifacts = temp_dir.path().join("artifacts");
        fs::create_dir(&artifacts).unwrap();
        let model =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/02_relational_graph.yaml");
        let args = test_args(model, artifacts.clone());

        let generated = generate_dump(&args, &artifacts).unwrap();
        assert!(!generated.validation.has_errors());
        let sql = fs::read_to_string(generated.sql_path).unwrap();

        assert_eq!(sql.matches("PRIMARY KEY").count(), 7);
        assert_eq!(sql.matches("FOREIGN KEY").count(), 11);
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
