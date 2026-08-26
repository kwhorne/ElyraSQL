//! Behaviour of the embedded facade. These run against the real engine and a
//! real file, because that is the whole claim being made: same semantics, same
//! format, no server.

use std::time::Duration;

use elyra_embed::{Config, Database, Error, Outcome, Privilege, Value};

#[test]
fn crud_round_trip() {
    let db = Database::temporary().unwrap();
    let conn = db.connect();

    conn.execute(
        "CREATE TABLE people (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, score DECIMAL(4,2))",
    )
    .unwrap();
    conn.execute("INSERT INTO people (name, score) VALUES ('Ada', 9.50), ('Linus', 8.25)")
        .unwrap();

    let rows = conn
        .query("SELECT name, score FROM people ORDER BY id")
        .unwrap();
    assert_eq!(rows.columns(), vec!["name", "score"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.get(0, "name"), Some(&Value::Text("Ada".into())));
    assert_eq!(rows.get(1, "name"), Some(&Value::Text("Linus".into())));

    assert_eq!(
        conn.affected("UPDATE people SET score = 10.00 WHERE name = 'Ada'")
            .unwrap(),
        1
    );
    assert_eq!(
        conn.affected("DELETE FROM people WHERE name = 'Linus'")
            .unwrap(),
        1
    );
    assert_eq!(
        conn.query("SELECT COUNT(*) FROM people").unwrap().rows[0][0],
        Value::Int(1)
    );
}

#[test]
fn auto_increment_id_is_reported() {
    let db = Database::temporary().unwrap();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)")
        .unwrap();

    let out = conn.execute("INSERT INTO t (v) VALUES (7)").unwrap();
    match out[0] {
        Outcome::Insert {
            affected_rows,
            last_insert_id,
        } => {
            assert_eq!(affected_rows, 1);
            assert_eq!(last_insert_id, 1);
        }
        ref other => panic!("expected an insert outcome, got {other:?}"),
    }
    assert_eq!(conn.last_insert_id(), 1);
}

/// The point of the crate: identical arithmetic to the server, because it is the
/// same evaluator. `ROUND(1.005, 2)` is the shape that separates exact DECIMAL
/// from an f64 detour — binary rounding gives 1.00.
#[test]
fn decimal_semantics_match_the_server() {
    let db = Database::temporary().unwrap();
    let conn = db.connect();

    let rows = conn
        .query("SELECT ROUND(1.005, 2), 10/3, 7.00/2, MOD(7.5, 2)")
        .unwrap();
    assert_eq!(rows.rows[0][0], Value::Decimal(101, 2));
    assert_eq!(rows.rows[0][1], Value::Decimal(33333, 4));
    assert_eq!(rows.rows[0][2], Value::Decimal(3500000, 6));
    assert_eq!(rows.rows[0][3], Value::Decimal(15, 1));
}

#[test]
fn data_survives_reopen_of_the_same_file() {
    let dir = std::env::temp_dir().join(format!("elyra_embed_reopen_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("db.edb");

    {
        let db = Database::open(&path).unwrap();
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .unwrap();
        conn.execute("INSERT INTO t VALUES (1, 'kept')").unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        let conn = db.connect();
        let rows = conn.query("SELECT v FROM t WHERE id = 1").unwrap();
        assert_eq!(rows.rows[0][0], Value::Text("kept".into()));
    }

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn temporary_database_is_removed_on_drop() {
    let path = {
        let db = Database::temporary().unwrap();
        let conn = db.connect();
        conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        let p = db.path().to_path_buf();
        assert!(
            p.exists(),
            "temp file should exist while the handle is alive"
        );
        p
    };
    assert!(!path.exists(), "temp file should be gone after drop");
}

#[test]
fn connections_share_committed_data() {
    let db = Database::temporary().unwrap();
    let writer = db.connect();
    let reader = db.connect();

    writer
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();
    writer.execute("INSERT INTO t VALUES (1)").unwrap();

    assert_eq!(
        reader.query("SELECT COUNT(*) FROM t").unwrap().rows[0][0],
        Value::Int(1)
    );
}

#[test]
fn sessions_keep_their_own_default_database() {
    let db = Database::temporary().unwrap();
    let a = db.connect();
    let b = db.connect();

    a.use_database("information_schema");

    assert_eq!(a.database(), "information_schema");
    assert_eq!(
        b.database(),
        "elyra",
        "one session's USE must not move another"
    );
}

#[test]
fn read_privilege_cannot_write() {
    let db = Database::temporary().unwrap();
    db.connect()
        .execute("CREATE TABLE t (id INT PRIMARY KEY)")
        .unwrap();

    let reader = db.connect_as(Privilege::Read, "reader");
    assert!(reader.query("SELECT COUNT(*) FROM t").is_ok());
    assert!(
        reader.execute("INSERT INTO t VALUES (1)").is_err(),
        "a Read session must not be able to insert"
    );
}

#[test]
fn query_and_affected_reject_the_wrong_statement_shape() {
    let db = Database::temporary().unwrap();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    // A DML statement has no result set.
    assert!(matches!(
        conn.query("INSERT INTO t VALUES (1)"),
        Err(Error::Unsupported(_))
    ));
    // A SELECT changes nothing.
    assert!(matches!(
        conn.affected("SELECT * FROM t"),
        Err(Error::Unsupported(_))
    ));
    // Multi-statement scripts are ambiguous for both.
    assert!(matches!(
        conn.query("SELECT 1; SELECT 2"),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn describe_reports_columns_without_running_the_query() {
    let db = Database::temporary().unwrap();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'x')").unwrap();

    let schema = conn
        .describe("SELECT id, name FROM t")
        .unwrap()
        .expect("a schema");
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name"]);
}

#[test]
fn backup_writes_a_file_that_opens() {
    let db = Database::temporary().unwrap();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
        .unwrap();
    conn.execute("INSERT INTO t VALUES (1, 'backed up')")
        .unwrap();

    let dest = std::env::temp_dir().join(format!("elyra_embed_backup_{}.edb", std::process::id()));
    std::fs::remove_file(&dest).ok();
    db.backup_to(&dest).unwrap();

    let restored = Database::open(&dest).unwrap();
    let rows = restored.connect().query("SELECT v FROM t").unwrap();
    assert_eq!(rows.rows[0][0], Value::Text("backed up".into()));

    drop(restored);
    std::fs::remove_file(&dest).ok();
    std::fs::remove_dir_all(format!("{}.vidx", dest.display())).ok();
}

/// Blocking inside a runtime would panic. The facade must report it instead, so
/// an async caller gets a diagnosable error rather than a thread abort.
#[test]
fn blocking_calls_inside_a_runtime_error_rather_than_panic() {
    let db = Database::temporary().unwrap();
    let conn = db.connect();

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        match conn.execute("SELECT 1") {
            Err(Error::Unsupported(msg)) => {
                assert!(msg.contains("async context"), "unexpected message: {msg}");
            }
            other => panic!("expected an Unsupported error, got {other:?}"),
        }
        assert!(matches!(Database::temporary(), Err(Error::Unsupported(_))));
    });
}

#[test]
fn zero_worker_threads_is_an_error_not_a_panic() {
    let config = Config {
        worker_threads: Some(0),
        ..Config::default()
    };
    assert!(matches!(
        Database::temporary_with(config),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn large_result_sets_materialise_past_the_batch_boundary() {
    let db = Database::temporary().unwrap();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();

    // More than one BATCH (1024) so the drain loop runs several times.
    let values: Vec<String> = (1..=2500).map(|i| format!("({i})")).collect();
    conn.execute(&format!("INSERT INTO t VALUES {}", values.join(",")))
        .unwrap();

    let rows = conn.query("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2500);
    assert_eq!(rows.rows[0][0], Value::Int(1));
    assert_eq!(rows.rows[2499][0], Value::Int(2500));
}

/// The storage writer thread holds the file lock briefly after the handle is
/// dropped, so an immediate reopen of the same path used to fail. `open` waits
/// it out. This is the flow a test suite runs: open, use, drop, repeat.
#[test]
fn reopen_immediately_after_drop_succeeds() {
    let dir = std::env::temp_dir().join(format!("elyra_embed_churn_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("db.edb");

    for i in 0..5 {
        let db = Database::open(&path).unwrap();
        let conn = db.connect();
        if i == 0 {
            conn.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        }
        conn.execute(&format!("INSERT INTO t VALUES ({i})"))
            .unwrap();
    }

    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.connect().query("SELECT COUNT(*) FROM t").unwrap().rows[0][0],
        Value::Int(5)
    );
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
}

/// Waiting out a closing handle must not hide a genuine conflict: two handles
/// held at the same time still fail.
#[test]
fn a_concurrently_held_file_still_conflicts() {
    let dir = std::env::temp_dir().join(format!("elyra_embed_conflict_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("db.edb");

    let held = Database::open(&path).unwrap();
    let config = Config {
        lock_wait: Some(Duration::from_millis(50)),
        ..Config::default()
    };
    let err = Database::open_with(&path, config).unwrap_err();
    assert!(
        format!("{err}").contains("Cannot acquire lock"),
        "expected a lock conflict, got: {err}"
    );

    drop(held);
    std::fs::remove_dir_all(&dir).ok();
}
