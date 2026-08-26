//! The C ABI exercised from Rust, so CI covers it without a C toolchain.
//! `crates/elyra-embed-capi/examples/basic.c` is the same walkthrough in C.

use std::ffi::{CStr, CString};
use std::ptr;

use elyrasql::*;

/// Call a C-string-taking entry point without repeating the CString dance.
fn cstr(s: &str) -> CString {
    CString::new(s).expect("no interior NUL in test SQL")
}

unsafe fn read(ptr: *const std::ffi::c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
    }
}

#[test]
fn open_query_and_free() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(elyra_db_open_temporary(&mut db), ELYRA_OK);
        assert!(!db.is_null());

        let path = read(elyra_db_path(db)).expect("a path");
        assert!(std::path::Path::new(&path).exists());

        let mut conn = ptr::null_mut();
        assert_eq!(elyra_db_connect(db, &mut conn), ELYRA_OK);

        assert_eq!(
            elyra_conn_execute(
                conn,
                cstr("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)").as_ptr(),
                ptr::null_mut()
            ),
            ELYRA_OK
        );

        let mut affected = 0u64;
        assert_eq!(
            elyra_conn_execute(
                conn,
                cstr("INSERT INTO t (v) VALUES ('a'), ('b')").as_ptr(),
                &mut affected
            ),
            ELYRA_OK
        );
        assert_eq!(affected, 2);
        assert_eq!(elyra_conn_last_insert_id(conn), 1);

        let mut rows = ptr::null_mut();
        assert_eq!(
            elyra_conn_query(
                conn,
                cstr("SELECT id, v FROM t ORDER BY id").as_ptr(),
                &mut rows
            ),
            ELYRA_OK
        );
        assert_eq!(elyra_rows_count(rows), 2);
        assert_eq!(elyra_rows_columns(rows), 2);
        assert_eq!(read(elyra_rows_column_name(rows, 1)).as_deref(), Some("v"));
        assert_eq!(read(elyra_rows_value(rows, 0, 1)).as_deref(), Some("a"));
        assert_eq!(read(elyra_rows_value(rows, 1, 1)).as_deref(), Some("b"));

        elyra_rows_free(rows);
        elyra_conn_free(conn);
        elyra_db_free(db);

        assert!(
            !std::path::Path::new(&path).exists(),
            "a temporary database's file must be removed when the handle is freed"
        );
    }
}

#[test]
fn sql_null_is_distinguishable_from_a_bad_index() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(elyra_db_open_temporary(&mut db), ELYRA_OK);
        let mut conn = ptr::null_mut();
        assert_eq!(elyra_db_connect(db, &mut conn), ELYRA_OK);
        assert_eq!(
            elyra_conn_execute(
                conn,
                cstr("CREATE TABLE t (id INT PRIMARY KEY, note TEXT)").as_ptr(),
                ptr::null_mut()
            ),
            ELYRA_OK
        );
        assert_eq!(
            elyra_conn_execute(
                conn,
                cstr("INSERT INTO t VALUES (1, NULL), (2, 'set')").as_ptr(),
                ptr::null_mut()
            ),
            ELYRA_OK
        );

        let mut rows = ptr::null_mut();
        assert_eq!(
            elyra_conn_query(
                conn,
                cstr("SELECT note FROM t ORDER BY id").as_ptr(),
                &mut rows
            ),
            ELYRA_OK
        );

        // Both a NULL value and an out-of-range read return a null pointer, so
        // the two must be separable some other way.
        assert!(elyra_rows_value(rows, 0, 0).is_null());
        assert_eq!(elyra_rows_is_null(rows, 0, 0), 1);

        assert_eq!(elyra_rows_is_null(rows, 1, 0), 0);
        assert_eq!(read(elyra_rows_value(rows, 1, 0)).as_deref(), Some("set"));

        assert_eq!(elyra_rows_is_null(rows, 99, 0), -1, "row out of range");
        assert_eq!(elyra_rows_is_null(rows, 0, 99), -1, "column out of range");
        assert!(elyra_rows_column_name(rows, 99).is_null());

        elyra_rows_free(rows);
        elyra_conn_free(conn);
        elyra_db_free(db);
    }
}

#[test]
fn failures_report_a_message_and_successes_clear_it() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(elyra_db_open_temporary(&mut db), ELYRA_OK);
        let mut conn = ptr::null_mut();
        assert_eq!(elyra_db_connect(db, &mut conn), ELYRA_OK);

        let mut rows = ptr::null_mut();
        assert_eq!(
            elyra_conn_query(conn, cstr("SELECT * FROM missing").as_ptr(), &mut rows),
            ELYRA_ERR
        );
        let msg = read(elyra_last_error()).expect("an error message");
        assert!(msg.contains("missing"), "unexpected message: {msg}");
        assert!(rows.is_null(), "no result set on failure");

        // A later success must not leave the stale message behind.
        assert_eq!(
            elyra_conn_query(conn, cstr("SELECT 1").as_ptr(), &mut rows),
            ELYRA_OK
        );
        assert!(elyra_last_error().is_null(), "success must clear the error");

        elyra_rows_free(rows);
        elyra_conn_free(conn);
        elyra_db_free(db);
    }
}

#[test]
fn null_arguments_are_errors_not_crashes() {
    unsafe {
        assert_eq!(elyra_db_open(ptr::null(), &mut ptr::null_mut()), ELYRA_ERR);
        assert!(read(elyra_last_error()).unwrap().contains("path"));

        assert_eq!(elyra_db_open_temporary(ptr::null_mut()), ELYRA_ERR);
        assert_eq!(
            elyra_db_connect(ptr::null(), &mut ptr::null_mut()),
            ELYRA_ERR
        );
        assert_eq!(
            elyra_conn_execute(ptr::null(), cstr("SELECT 1").as_ptr(), ptr::null_mut()),
            ELYRA_ERR
        );

        // Accessors tolerate null handles.
        assert!(elyra_db_path(ptr::null()).is_null());
        assert_eq!(elyra_rows_count(ptr::null()), 0);
        assert_eq!(elyra_rows_columns(ptr::null()), 0);
        assert_eq!(elyra_rows_is_null(ptr::null(), 0, 0), -1);
        assert_eq!(elyra_conn_last_insert_id(ptr::null()), 0);

        // Freeing null is a no-op, so cleanup paths need no guard.
        elyra_rows_free(ptr::null_mut());
        elyra_conn_free(ptr::null_mut());
        elyra_db_free(ptr::null_mut());
    }
}

#[test]
fn a_connection_may_outlive_the_database_handle() {
    unsafe {
        let mut db = ptr::null_mut();
        assert_eq!(elyra_db_open_temporary(&mut db), ELYRA_OK);
        let mut conn = ptr::null_mut();
        assert_eq!(elyra_db_connect(db, &mut conn), ELYRA_OK);
        assert_eq!(
            elyra_conn_execute(
                conn,
                cstr("CREATE TABLE t (id INT PRIMARY KEY)").as_ptr(),
                ptr::null_mut()
            ),
            ELYRA_OK
        );

        // The session holds its own handle on the engine, so this must not
        // invalidate it — a host language freeing in the wrong order is a
        // realistic accident, and it should not be a use-after-free.
        elyra_db_free(db);

        let mut rows = ptr::null_mut();
        assert_eq!(
            elyra_conn_query(conn, cstr("SELECT COUNT(*) FROM t").as_ptr(), &mut rows),
            ELYRA_OK
        );
        assert_eq!(read(elyra_rows_value(rows, 0, 0)).as_deref(), Some("0"));

        elyra_rows_free(rows);
        elyra_conn_free(conn);
    }
}

#[test]
fn version_is_reported() {
    unsafe {
        let v = read(elyra_version()).expect("a version");
        assert!(v.starts_with(char::is_numeric), "unexpected version: {v}");
    }
}
