//! Embedded ElyraSQL — the engine as an in-process library.
//!
//! [`Database`] opens an `.edb` file directly: no server process, no socket and
//! no MySQL wire protocol. The SQL semantics are identical to the server's
//! because it is the same engine — `elyra-engine` depends on `elyra-core`,
//! `elyra-storage`, `elyra-vector` and `elyra-olap`, and on nothing above them.
//! A file written here opens in `elyrasql serve`, and the other way round.
//!
//! ```no_run
//! use elyra_embed::{Database, Value};
//!
//! let db = Database::temporary()?;
//! let conn = db.connect();
//! conn.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")?;
//! conn.execute("INSERT INTO t VALUES (1, 'Ada')")?;
//!
//! let rows = conn.query("SELECT name FROM t")?;
//! assert_eq!(rows.rows[0][0], Value::Text("Ada".into()));
//! # Ok::<(), elyra_embed::Error>(())
//! ```
//!
//! # Everything here blocks
//!
//! The engine is async, and its reads go through `spawn_blocking`, so it needs a
//! Tokio runtime with a blocking pool. This crate owns one so a caller does not
//! have to: every method drives it with `block_on` and returns a plain value.
//!
//! The consequence is that these methods must **not** be called from inside an
//! async context — `block_on` panics when a runtime is already entered on the
//! thread. Rather than panic, every entry point checks for an ambient runtime
//! and returns [`Error::Unsupported`]. Async callers should use `elyra-engine`
//! directly; it is a normal async API and this crate adds nothing they need.
//!
//! # Rendering values
//!
//! [`Value`] carries the engine's own representation — `Decimal` is an unscaled
//! integer and a scale, `Date` is a day count. To print a value the way a
//! `mysql` client would, use `Value::to_wire_string`, which is the exact
//! rendering the server sends over the text protocol (`None` for SQL NULL).
//!
//! # One writer per file
//!
//! The database file is locked exclusively, so one process holds it at a time —
//! the same rule SQLite's single-writer model follows. Opening a file that
//! another live handle holds fails; opening one whose handle has just closed
//! waits briefly, for the reason described on [`DEFAULT_LOCK_WAIT`].

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use elyra_engine::{Engine, RowStream, Session};
use elyra_storage::Db;
use tokio::runtime::Runtime;

pub use elyra_core::{Collation, ColumnDef, ColumnType, Error, Privilege, Result, Schema, Value};

/// How long [`Database::open`] retries while the database file is still locked.
///
/// The storage writer runs on a detached OS thread that holds the storage handle
/// (and therefore the file lock) until it observes its job queue close. Nothing
/// synchronises that back to whoever dropped the handle, so the lock outlives the
/// [`Database`] by a short, unbounded-in-principle interval. A server opens its
/// file once per process and never sees this; an embedded caller that opens and
/// closes the same file — a test suite, above all — sees it constantly.
///
/// Retrying converts that race into a wait. It does not paper over a genuine
/// conflict: two handles really held at once still fail, just after this long.
///
/// A deterministic close at the storage layer would remove the need for this
/// entirely; tracked in <https://github.com/kwhorne/ElyraSQL/issues/110>.
pub const DEFAULT_LOCK_WAIT: Duration = Duration::from_secs(2);

/// Rows pulled from a stream per step while materialising a result set. Matches
/// the engine's own scan chunk, so a batch boundary never splits a storage read.
const BATCH: usize = 1024;

/// Outcome of one statement. A `;`-separated script produces one per statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A result set, fully materialised.
    Rows(Rows),
    /// A statement that changed state, and how many rows it changed.
    Affected(u64),
    /// An `INSERT`, with the id it generated (0 when the table has no
    /// auto-increment column).
    Insert {
        affected_rows: u64,
        last_insert_id: u64,
    },
}

impl Outcome {
    /// The result set, if this statement produced one.
    pub fn rows(&self) -> Option<&Rows> {
        match self {
            Outcome::Rows(r) => Some(r),
            _ => None,
        }
    }

    /// Rows changed by a DML statement; 0 for a `SELECT`.
    pub fn affected_rows(&self) -> u64 {
        match self {
            Outcome::Rows(_) => 0,
            Outcome::Affected(n) => *n,
            Outcome::Insert { affected_rows, .. } => *affected_rows,
        }
    }
}

/// A materialised result set: the column metadata the server would send, and
/// every row.
///
/// Streaming is deliberately not exposed. The engine streams internally and
/// spills to disk when a result outgrows memory, so a large `SELECT` is bounded
/// there; what this type gives up is the ability to stop reading early. Callers
/// who need that should use `elyra-engine`'s `RowStream` directly.
#[derive(Debug, Clone, PartialEq)]
pub struct Rows {
    pub schema: Schema,
    pub rows: Vec<Vec<Value>>,
}

impl Rows {
    /// Column names, in result order.
    pub fn columns(&self) -> Vec<&str> {
        self.schema
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect()
    }

    /// Index of a column by name, case-insensitively, as SQL resolves it.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// One value by row index and column name. `None` when either is unknown.
    pub fn get(&self, row: usize, column: &str) -> Option<&Value> {
        let col = self.column_index(column)?;
        self.rows.get(row)?.get(col)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Vec<Value>> {
        self.rows.iter()
    }
}

/// How to open an embedded database.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Worker threads for the owned runtime. `None` uses Tokio's default (one
    /// per core). Set it low for a test process running many databases at once.
    pub worker_threads: Option<usize>,
    /// Directory for an append-only binlog, enabling point-in-time recovery
    /// exactly as `elyrasql serve --binlog` does. Off by default.
    pub binlog: Option<PathBuf>,
    /// How long [`Database::open`] waits for a file lock still held by a handle
    /// that is closing. `None` uses [`DEFAULT_LOCK_WAIT`]; `Some(ZERO)` fails
    /// immediately, which is what a caller wanting to *detect* a concurrent
    /// open should ask for.
    pub lock_wait: Option<Duration>,
}

/// An embedded ElyraSQL database: one file, one owned runtime.
///
/// Cheap to keep for the life of a process and safe to share across threads;
/// call [`Database::connect`] per unit of work that needs its own session state
/// (current database, transaction, user variables).
pub struct Database {
    engine: Engine,
    rt: Arc<Runtime>,
    path: PathBuf,
    /// Paths to delete on drop, for [`Database::temporary`].
    cleanup: Option<Vec<PathBuf>>,
}

impl Database {
    /// Open (or create) a database file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Config::default())
    }

    /// Open (or create) a database file with explicit configuration.
    pub fn open_with(path: impl AsRef<Path>, config: Config) -> Result<Self> {
        reject_ambient_runtime("Database::open")?;
        let path = path.as_ref().to_path_buf();
        let rt = build_runtime(&config)?;

        let db = open_locked(&path, config.binlog.clone(), config.lock_wait)?;
        let engine = Engine::new(db);

        // The collation migration re-keys text primary keys and index entries
        // when the folding version changed. The server runs it before it accepts
        // a connection so no query can observe a half-migrated keyspace; doing it
        // inside `open` is how an embedded caller gets the same guarantee without
        // having to know the rule exists. A database whose indexed text is pure
        // ASCII is untouched.
        rt.block_on(engine.migrate_collation())?;

        Ok(Self {
            engine,
            rt: Arc::new(rt),
            path,
            cleanup: None,
        })
    }

    /// Open a throwaway database in the system temp directory, deleted when the
    /// returned handle drops.
    ///
    /// This is the shape a test suite wants: real MySQL semantics, real
    /// persistence within the test, and no server or fixture to tear down.
    pub fn temporary() -> Result<Self> {
        Self::temporary_with(Config::default())
    }

    /// [`Database::temporary`] with explicit configuration.
    pub fn temporary_with(config: Config) -> Result<Self> {
        let path = temp_path();
        let mut db = Self::open_with(&path, config)?;
        // The vector index cache is written to a sibling directory rather than
        // into the database file, so removing the file alone would leak it.
        db.cleanup = Some(vec![
            path.clone(),
            PathBuf::from(format!("{}.vidx", path.display())),
        ]);
        Ok(db)
    }

    /// A new session over the shared database, with full privileges.
    ///
    /// `Admin` is the honest default in-process: the caller already holds the
    /// database file, so a lower level would restrict nothing an attacker could
    /// not simply bypass. Use [`Database::connect_as`] to exercise grants.
    pub fn connect(&self) -> Connection {
        self.connect_as(Privilege::Admin, "")
    }

    /// A session at a given privilege level, acting as `user` so per-table
    /// grants resolve. Useful for testing an application's own grant model.
    pub fn connect_as(&self, privilege: Privilege, user: &str) -> Connection {
        Connection {
            engine: self.engine.clone(),
            rt: self.rt.clone(),
            session: self.engine.session(),
            privilege,
            user: user.to_string(),
        }
    }

    /// Path of the database file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Copy a consistent snapshot to a new file, without blocking writers.
    /// Refuses to overwrite an existing target, like `BACKUP TO`.
    pub fn backup_to(&self, dest: impl AsRef<Path>) -> Result<u64> {
        reject_ambient_runtime("Database::backup_to")?;
        let dest = dest.as_ref().to_path_buf();
        self.rt.block_on(self.engine.db().backup_to(dest))
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("path", &self.path)
            .field("temporary", &self.cleanup.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let Some(paths) = self.cleanup.take() else {
            return;
        };
        for p in paths {
            // Best effort: a failure here leaks a temp file, which is not worth
            // panicking in a destructor over.
            if p.is_dir() {
                let _ = std::fs::remove_dir_all(&p);
            } else {
                let _ = std::fs::remove_file(&p);
            }
        }
    }
}

/// One session over an embedded database: its own current database, transaction
/// and user variables, exactly like one server connection.
pub struct Connection {
    engine: Engine,
    rt: Arc<Runtime>,
    session: Session,
    privilege: Privilege,
    user: String,
}

impl Connection {
    /// Run one or more `;`-separated statements, returning one [`Outcome`] each.
    pub fn execute(&self, sql: &str) -> Result<Vec<Outcome>> {
        reject_ambient_runtime("Connection::execute")?;
        self.rt.block_on(async {
            let results = self
                .engine
                .execute_as(sql, self.privilege, &self.user, &self.session)
                .await?;
            let mut out = Vec::with_capacity(results.len());
            for r in results {
                out.push(materialise(r).await?);
            }
            Ok(out)
        })
    }

    /// Run one statement that returns a result set.
    ///
    /// Errors when the SQL produced no result set (a DML statement) or more than
    /// one (a multi-statement script) — use [`Connection::execute`] for those,
    /// so the ambiguity is never silently resolved.
    pub fn query(&self, sql: &str) -> Result<Rows> {
        let mut outcomes = self.execute(sql)?;
        if outcomes.len() != 1 {
            return Err(Error::Unsupported(format!(
                "query() expects exactly one statement returning rows, got {}",
                outcomes.len()
            )));
        }
        match outcomes.remove(0) {
            Outcome::Rows(rows) => Ok(rows),
            _ => Err(Error::Unsupported(
                "query() expects a statement that returns rows; use execute()".into(),
            )),
        }
    }

    /// Run one DML statement and return the number of rows it changed.
    pub fn affected(&self, sql: &str) -> Result<u64> {
        let mut outcomes = self.execute(sql)?;
        if outcomes.len() != 1 {
            return Err(Error::Unsupported(format!(
                "affected() expects exactly one statement, got {}",
                outcomes.len()
            )));
        }
        match outcomes.remove(0) {
            Outcome::Rows(_) => Err(Error::Unsupported(
                "affected() expects a statement that changes rows; use query()".into(),
            )),
            other => Ok(other.affected_rows()),
        }
    }

    /// Column metadata a statement would return, without running it.
    pub fn describe(&self, sql: &str) -> Result<Option<Schema>> {
        reject_ambient_runtime("Connection::describe")?;
        Ok(self
            .rt
            .block_on(self.engine.describe_query(sql, &self.session)))
    }

    /// Current default database (`elyra` unless changed). Equivalent to `USE`.
    pub fn database(&self) -> String {
        self.session.database()
    }

    /// Set the default database for unqualified table names.
    pub fn use_database(&self, name: &str) {
        self.session.set_database(name);
    }

    /// Id generated by the last `INSERT` on this session.
    pub fn last_insert_id(&self) -> i64 {
        self.session.last_insert_id()
    }

    /// The underlying engine session, for the parts of the API this facade does
    /// not wrap (isolation level, explicit locks, SQL mode).
    pub fn session(&self) -> &Session {
        &self.session
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("database", &self.session.database())
            .field("privilege", &self.privilege)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

/// Pull a whole stream into memory. Kept out of `Connection` so both `execute`
/// and any future helper share one definition of "materialised".
async fn materialise(result: elyra_engine::QueryResult) -> Result<Outcome> {
    match result {
        elyra_engine::QueryResult::Affected(n) => Ok(Outcome::Affected(n)),
        elyra_engine::QueryResult::Insert {
            affected_rows,
            last_insert_id,
        } => Ok(Outcome::Insert {
            affected_rows,
            last_insert_id,
        }),
        elyra_engine::QueryResult::Rows(stream) => Ok(Outcome::Rows(drain(stream).await?)),
    }
}

async fn drain(mut stream: RowStream) -> Result<Rows> {
    let schema = stream.schema.clone();
    let mut rows = Vec::new();
    loop {
        let batch = stream.next_batch(BATCH).await?;
        if batch.is_empty() {
            break;
        }
        rows.extend(batch);
    }
    Ok(Rows { schema, rows })
}

/// Open the storage, waiting out a lock still held by a closing handle.
fn open_locked(path: &Path, binlog: Option<PathBuf>, lock_wait: Option<Duration>) -> Result<Db> {
    let deadline = std::time::Instant::now() + lock_wait.unwrap_or(DEFAULT_LOCK_WAIT);
    let mut backoff = Duration::from_millis(1);
    loop {
        match Db::open_with_binlog(path, binlog.clone()) {
            Ok(db) => return Ok(db),
            Err(e) if is_lock_conflict(&e) && std::time::Instant::now() < deadline => {
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(50));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Whether an open failed because the file lock was held.
///
/// Matched on the message because that is all there is: the lock is redb's, and
/// it reaches us as an opaque string inside [`Error::Storage`]. Distinguishing it
/// structurally would mean a storage-level error kind, which is the same change
/// that would let a caller wait for the writer thread deterministically and make
/// this retry unnecessary in the first place — see
/// <https://github.com/kwhorne/ElyraSQL/issues/110>.
fn is_lock_conflict(e: &Error) -> bool {
    matches!(e, Error::Storage(msg) if msg.contains("Cannot acquire lock"))
}

fn build_runtime(config: &Config) -> Result<Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = config.worker_threads {
        // Zero would be rejected by Tokio with a panic; turn it into an error.
        if n == 0 {
            return Err(Error::Unsupported(
                "worker_threads must be at least 1".into(),
            ));
        }
        builder.worker_threads(n);
    }
    builder
        .build()
        .map_err(|e| Error::Storage(format!("could not start the embedded runtime: {e}")))
}

/// `block_on` panics inside an async context. Callers get an error instead, with
/// the name of the method they reached it through.
fn reject_ambient_runtime(method: &str) -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::Unsupported(format!(
            "{method} blocks and cannot be called from an async context; \
             use elyra-engine's async API directly"
        )));
    }
    Ok(())
}

fn temp_path() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "elyra_embed_{}_{}_{}.edb",
        std::process::id(),
        nanos,
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    path
}
