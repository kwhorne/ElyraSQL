//! C ABI for embedded ElyraSQL.
//!
//! A thin, allocation-explicit layer over [`elyra_embed`], so a host language
//! with an FFI — PHP, Python, Node, Ruby, Go — can open an `.edb` file in its
//! own process and run MySQL-compatible SQL with no server.
//!
//! # Conventions
//!
//! * Every fallible function returns [`ELYRA_OK`] or [`ELYRA_ERR`]. On error,
//!   [`elyra_last_error`] returns a thread-local message, valid until the next
//!   failing call on that thread.
//! * Handles are opaque and owned by the caller; each has a matching `*_free`.
//!   Freeing null is a no-op, so cleanup paths need no guard.
//! * Strings borrowed from a handle stay valid until that handle is freed. No
//!   value string is separately owned, so there is nothing per-value to free.
//! * Every entry point is `extern "C"`, which aborts rather than unwinding a
//!   Rust panic into the host — a panic can never cross this boundary.
//!
//! # Threads
//!
//! An `ElyraDb` and its connections may be used from several threads, but a
//! single `ElyraConn` carries session state (current database, transaction,
//! user variables) and must not be used from two threads at once. Give each
//! thread its own connection.

use std::ffi::{c_char, c_int, CStr, CString};
use std::path::PathBuf;
use std::ptr;

use elyra_embed::{Config, Connection, Database, Outcome, Rows as EmbedRows};

/// Call succeeded.
pub const ELYRA_OK: c_int = 0;
/// Call failed; see [`elyra_last_error`].
pub const ELYRA_ERR: c_int = -1;

thread_local! {
    static LAST_ERROR: std::cell::RefCell<Option<CString>> =
        const { std::cell::RefCell::new(None) };
}

fn set_error(msg: impl std::fmt::Display) -> c_int {
    let text = msg.to_string();
    // A message with an interior NUL cannot be a C string; replace rather than
    // lose the error entirely.
    let cstring = CString::new(text)
        .unwrap_or_else(|_| CString::new("error message contained a NUL byte").expect("no NUL"));
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(cstring));
    ELYRA_ERR
}

fn clear_error() {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
}

/// The last error on this thread, or null if the last call succeeded.
///
/// Valid until the next failing call on the same thread.
#[no_mangle]
pub extern "C" fn elyra_last_error() -> *const c_char {
    LAST_ERROR.with(|e| match &*e.borrow() {
        Some(s) => s.as_ptr(),
        None => ptr::null(),
    })
}

/// The ElyraSQL version this library was built from, as a static string.
#[no_mangle]
pub extern "C" fn elyra_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

/// Read a caller-supplied C string.
///
/// # Safety
/// `s` must be null or a valid NUL-terminated string.
unsafe fn borrow_str<'a>(s: *const c_char, what: &str) -> Result<&'a str, c_int> {
    if s.is_null() {
        return Err(set_error(format!("{what} must not be null")));
    }
    CStr::from_ptr(s)
        .to_str()
        .map_err(|_| set_error(format!("{what} must be valid UTF-8")))
}

// ---------------------------------------------------------------- database

/// An open database. Free with [`elyra_db_free`].
pub struct ElyraDb {
    inner: Database,
    /// Owned copy of the path, so [`elyra_db_path`] can hand out a C string
    /// that lives exactly as long as this handle.
    path: CString,
}

fn wrap_db(inner: Database) -> Result<*mut ElyraDb, c_int> {
    let path = CString::new(inner.path().to_string_lossy().into_owned())
        .map_err(|_| set_error("database path contained a NUL byte"))?;
    Ok(Box::into_raw(Box::new(ElyraDb { inner, path })))
}

/// Open (or create) a database file.
///
/// # Safety
/// `path` must be a valid NUL-terminated UTF-8 string; `out` must be a valid
/// pointer to a `*mut ElyraDb`.
#[no_mangle]
pub unsafe extern "C" fn elyra_db_open(path: *const c_char, out: *mut *mut ElyraDb) -> c_int {
    clear_error();
    if out.is_null() {
        return set_error("out must not be null");
    }
    let path = match borrow_str(path, "path") {
        Ok(p) => PathBuf::from(p),
        Err(code) => return code,
    };
    match Database::open(path).map_err(set_error).and_then(wrap_db) {
        Ok(ptr) => {
            *out = ptr;
            ELYRA_OK
        }
        Err(code) => code,
    }
}

/// Open a throwaway database, deleted when the handle is freed.
///
/// # Safety
/// `out` must be a valid pointer to a `*mut ElyraDb`.
#[no_mangle]
pub unsafe extern "C" fn elyra_db_open_temporary(out: *mut *mut ElyraDb) -> c_int {
    clear_error();
    if out.is_null() {
        return set_error("out must not be null");
    }
    match Database::temporary().map_err(set_error).and_then(wrap_db) {
        Ok(ptr) => {
            *out = ptr;
            ELYRA_OK
        }
        Err(code) => code,
    }
}

/// Limit the worker threads of a database opened afterwards. Mainly for hosts
/// that run many databases in one process, where one runtime per core each is
/// far more threads than the work needs.
///
/// # Safety
/// `path` and `out` as for [`elyra_db_open`].
#[no_mangle]
pub unsafe extern "C" fn elyra_db_open_with_threads(
    path: *const c_char,
    worker_threads: usize,
    out: *mut *mut ElyraDb,
) -> c_int {
    clear_error();
    if out.is_null() {
        return set_error("out must not be null");
    }
    let path = match borrow_str(path, "path") {
        Ok(p) => PathBuf::from(p),
        Err(code) => return code,
    };
    let config = Config {
        worker_threads: Some(worker_threads),
        ..Config::default()
    };
    match Database::open_with(path, config)
        .map_err(set_error)
        .and_then(wrap_db)
    {
        Ok(ptr) => {
            *out = ptr;
            ELYRA_OK
        }
        Err(code) => code,
    }
}

/// The database file's path, valid until the handle is freed.
///
/// # Safety
/// `db` must be a live handle from an `elyra_db_open*` call, or null.
#[no_mangle]
pub unsafe extern "C" fn elyra_db_path(db: *const ElyraDb) -> *const c_char {
    match db.as_ref() {
        Some(db) => db.path.as_ptr(),
        None => ptr::null(),
    }
}

/// Release a database handle. A temporary database's file is deleted here.
///
/// # Safety
/// `db` must be null, or a handle from an `elyra_db_open*` call not yet freed.
#[no_mangle]
pub unsafe extern "C" fn elyra_db_free(db: *mut ElyraDb) {
    if !db.is_null() {
        drop(Box::from_raw(db));
    }
}

// -------------------------------------------------------------- connection

/// One session over a database. Free with [`elyra_conn_free`].
pub struct ElyraConn {
    inner: Connection,
}

/// Open a session. The session may outlive the `ElyraDb` handle it came from.
///
/// # Safety
/// `db` must be a live handle; `out` must be a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn elyra_db_connect(db: *const ElyraDb, out: *mut *mut ElyraConn) -> c_int {
    clear_error();
    if out.is_null() {
        return set_error("out must not be null");
    }
    let Some(db) = db.as_ref() else {
        return set_error("db must not be null");
    };
    *out = Box::into_raw(Box::new(ElyraConn {
        inner: db.inner.connect(),
    }));
    ELYRA_OK
}

/// Release a session.
///
/// # Safety
/// `conn` must be null, or a handle from [`elyra_db_connect`] not yet freed.
#[no_mangle]
pub unsafe extern "C" fn elyra_conn_free(conn: *mut ElyraConn) {
    if !conn.is_null() {
        drop(Box::from_raw(conn));
    }
}

/// Run one or more `;`-separated statements.
///
/// `affected_out` may be null; otherwise it receives the summed number of rows
/// changed by the statements that changed any.
///
/// # Safety
/// `conn` must be live, `sql` a valid NUL-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn elyra_conn_execute(
    conn: *const ElyraConn,
    sql: *const c_char,
    affected_out: *mut u64,
) -> c_int {
    clear_error();
    let Some(conn) = conn.as_ref() else {
        return set_error("conn must not be null");
    };
    let sql = match borrow_str(sql, "sql") {
        Ok(s) => s,
        Err(code) => return code,
    };
    match conn.inner.execute(sql) {
        Ok(outcomes) => {
            if !affected_out.is_null() {
                *affected_out = outcomes.iter().map(Outcome::affected_rows).sum();
            }
            ELYRA_OK
        }
        Err(e) => set_error(e),
    }
}

/// Run one statement and collect its result set.
///
/// # Safety
/// `conn` must be live, `sql` valid UTF-8, `out` a valid pointer.
#[no_mangle]
pub unsafe extern "C" fn elyra_conn_query(
    conn: *const ElyraConn,
    sql: *const c_char,
    out: *mut *mut ElyraRows,
) -> c_int {
    clear_error();
    if out.is_null() {
        return set_error("out must not be null");
    }
    let Some(conn) = conn.as_ref() else {
        return set_error("conn must not be null");
    };
    let sql = match borrow_str(sql, "sql") {
        Ok(s) => s,
        Err(code) => return code,
    };
    match conn.inner.query(sql) {
        Ok(rows) => match ElyraRows::from_rows(&rows) {
            Ok(rows) => {
                *out = Box::into_raw(Box::new(rows));
                ELYRA_OK
            }
            Err(code) => code,
        },
        Err(e) => set_error(e),
    }
}

/// The id generated by this session's last `INSERT`.
///
/// # Safety
/// `conn` must be live or null (null yields 0).
#[no_mangle]
pub unsafe extern "C" fn elyra_conn_last_insert_id(conn: *const ElyraConn) -> i64 {
    conn.as_ref().map(|c| c.inner.last_insert_id()).unwrap_or(0)
}

// -------------------------------------------------------------------- rows

/// A materialised result set. Free with [`elyra_rows_free`].
///
/// Values are converted to their MySQL text form once, at construction, so a
/// pointer handed out by [`elyra_rows_value`] borrows from this handle and needs
/// no separate free. `None` is SQL NULL.
pub struct ElyraRows {
    columns: Vec<CString>,
    cells: Vec<Vec<Option<CString>>>,
}

impl ElyraRows {
    fn from_rows(rows: &EmbedRows) -> Result<Self, c_int> {
        let mut columns = Vec::with_capacity(rows.schema.columns.len());
        for c in &rows.schema.columns {
            columns.push(
                CString::new(c.name.as_str())
                    .map_err(|_| set_error(format!("column name {:?} contained a NUL", c.name)))?,
            );
        }
        let mut cells = Vec::with_capacity(rows.rows.len());
        for row in &rows.rows {
            let mut out = Vec::with_capacity(row.len());
            for value in row {
                out.push(match value.to_wire_string() {
                    // A TEXT or BLOB column can legitimately hold a NUL byte,
                    // which no C string can carry. Reporting it is better than
                    // silently truncating at the NUL.
                    Some(s) => Some(CString::new(s).map_err(|_| {
                        set_error("value contained a NUL byte and cannot cross the C ABI")
                    })?),
                    None => None,
                });
            }
            cells.push(out);
        }
        Ok(Self { columns, cells })
    }
}

/// Number of rows.
///
/// # Safety
/// `rows` must be live or null (null yields 0).
#[no_mangle]
pub unsafe extern "C" fn elyra_rows_count(rows: *const ElyraRows) -> usize {
    rows.as_ref().map(|r| r.cells.len()).unwrap_or(0)
}

/// Number of columns.
///
/// # Safety
/// `rows` must be live or null (null yields 0).
#[no_mangle]
pub unsafe extern "C" fn elyra_rows_columns(rows: *const ElyraRows) -> usize {
    rows.as_ref().map(|r| r.columns.len()).unwrap_or(0)
}

/// A column's name, or null if the index is out of range.
///
/// # Safety
/// `rows` must be live or null.
#[no_mangle]
pub unsafe extern "C" fn elyra_rows_column_name(
    rows: *const ElyraRows,
    col: usize,
) -> *const c_char {
    match rows.as_ref().and_then(|r| r.columns.get(col)) {
        Some(name) => name.as_ptr(),
        None => ptr::null(),
    }
}

/// One value in its MySQL text form, borrowed from `rows`.
///
/// Returns null both for SQL NULL and for an out-of-range index; call
/// [`elyra_rows_is_null`] to tell those apart.
///
/// # Safety
/// `rows` must be live or null.
#[no_mangle]
pub unsafe extern "C" fn elyra_rows_value(
    rows: *const ElyraRows,
    row: usize,
    col: usize,
) -> *const c_char {
    match rows
        .as_ref()
        .and_then(|r| r.cells.get(row))
        .and_then(|r| r.get(col))
    {
        Some(Some(value)) => value.as_ptr(),
        _ => ptr::null(),
    }
}

/// 1 when the value is SQL NULL, 0 when it is not, -1 when out of range.
///
/// # Safety
/// `rows` must be live or null.
#[no_mangle]
pub unsafe extern "C" fn elyra_rows_is_null(
    rows: *const ElyraRows,
    row: usize,
    col: usize,
) -> c_int {
    match rows
        .as_ref()
        .and_then(|r| r.cells.get(row))
        .and_then(|r| r.get(col))
    {
        Some(Some(_)) => 0,
        Some(None) => 1,
        None => -1,
    }
}

/// Release a result set.
///
/// # Safety
/// `rows` must be null, or a handle from [`elyra_conn_query`] not yet freed.
#[no_mangle]
pub unsafe extern "C" fn elyra_rows_free(rows: *mut ElyraRows) {
    if !rows.is_null() {
        drop(Box::from_raw(rows));
    }
}
