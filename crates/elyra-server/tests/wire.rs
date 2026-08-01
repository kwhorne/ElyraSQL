//! End-to-end wire-protocol / SQL regression tests.
//!
//! Each test starts a real ElyraSQL server and drives it with the independent
//! `mysql_async` driver, so a regression in the wire layer, the parser, or the
//! executor fails the build.

mod common;

use common::TestServer;
use mysql_async::prelude::*;

#[tokio::test]
async fn literals_and_arithmetic() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    let one: i64 = c.query_first("SELECT 1").await.unwrap().unwrap();
    assert_eq!(one, 1);

    let two: i64 = c.query_first("SELECT 1 + 1").await.unwrap().unwrap();
    assert_eq!(two, 2);

    let msg: String = c
        .query_first("SELECT 'hei fra ElyraSQL'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg, "hei fra ElyraSQL");

    let ver: String = c.query_first("SELECT VERSION()").await.unwrap().unwrap();
    assert!(ver.contains("ElyraSQL"), "version was {ver}");
    assert!(ver.starts_with("8.0.12-"), "version was {ver}");

    drop(c);
}

#[tokio::test]
async fn mysql_dump_literals_remain_exact() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE literal_matrix (
            id INT PRIMARY KEY,
            amount DECIMAL(30,10) NOT NULL,
            payload VARBINARY(16) NOT NULL,
            email VARCHAR(255) NOT NULL
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO literal_matrix VALUES
         (1, -170812946.3720907892, X'00AF10', 'otilia@example.com')",
    )
    .await
    .unwrap();

    let row: (String, Vec<u8>, String) = c
        .query_first("SELECT amount, payload, email FROM literal_matrix WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.0, "-170812946.3720907892");
    assert_eq!(row.1, vec![0x00, 0xaf, 0x10]);
    assert_eq!(row.2, "otilia@example.com");

    let odd_0x: Vec<u8> = c.query_first("SELECT 0xF").await.unwrap().unwrap();
    assert_eq!(odd_0x, vec![0x0f]);
    assert!(c.query_drop("SELECT X'F'").await.is_err());

    assert!(c
        .query_drop("SELECT 17014118346046923173168730371588410572.7 + 0.1")
        .await
        .is_err());
    let still_connected: i64 = c.query_first("SELECT 1").await.unwrap().unwrap();
    assert_eq!(still_connected, 1);
}

#[tokio::test]
async fn ddl_dml_roundtrip() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT)")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO users (id, name, age) VALUES (1,'Ada',36),(2,'Linus',54),(3,'Grace',85)",
    )
    .await
    .unwrap();

    let count: i64 = c
        .query_first("SELECT COUNT(*) FROM users")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 3);

    let rows: Vec<(i64, String, i64)> = c
        .query("SELECT id, name, age FROM users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, "Ada".into(), 36),
            (2, "Linus".into(), 54),
            (3, "Grace".into(), 85)
        ]
    );

    c.query_drop("UPDATE users SET age = 37 WHERE id = 1")
        .await
        .unwrap();
    let age: i64 = c
        .query_first("SELECT age FROM users WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(age, 37);

    c.query_drop("DELETE FROM users WHERE id = 3")
        .await
        .unwrap();
    let count: i64 = c
        .query_first("SELECT COUNT(*) FROM users")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn explicit_auto_increment_value_is_returned_in_ok_packet() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE ai_explicit (
            id BIGINT NOT NULL AUTO_INCREMENT,
            a VARCHAR(10),
            PRIMARY KEY (id)
        )",
    )
    .await
    .unwrap();

    c.query_drop("INSERT INTO ai_explicit (id, a) VALUES (1697842, 'explicit')")
        .await
        .unwrap();
    assert_eq!(c.last_insert_id(), Some(1_697_842));

    // An explicit value affects the OK packet, but not SQL LAST_INSERT_ID().
    let session_id: u64 = c
        .query_first("SELECT LAST_INSERT_ID()")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(session_id, 0);

    c.query_drop("INSERT INTO ai_explicit (a) VALUES ('auto')")
        .await
        .unwrap();
    assert_eq!(c.last_insert_id(), Some(1_697_843));

    // The binary prepared-statement path uses the same statement-local value.
    c.exec_drop(
        "INSERT INTO ai_explicit (id, a) VALUES (?, ?)",
        (1_800_000_u64, "prepared"),
    )
    .await
    .unwrap();
    assert_eq!(c.last_insert_id(), Some(1_800_000));

    c.query_drop(
        "INSERT IGNORE INTO ai_explicit (id, a)
         VALUES (1800000, 'ignored')",
    )
    .await
    .unwrap();
    assert_eq!(c.last_insert_id(), None);
}

#[tokio::test]
async fn mysql_index_and_foreign_key_drop_forms() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE parents (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE children (
            id INT PRIMARY KEY,
            parent_id INT,
            name VARCHAR(20),
            INDEX idx_name (name),
            CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parents(id)
        )",
    )
    .await
    .unwrap();

    c.query_drop("ALTER TABLE children DROP INDEX IDX_NAME")
        .await
        .unwrap();
    let indexes: Vec<String> = c
        .query("SHOW INDEX FROM children")
        .await
        .unwrap()
        .into_iter()
        .map(|row: mysql_async::Row| row.get("Key_name").unwrap())
        .collect();
    assert!(!indexes.iter().any(|name| name == "idx_name"));

    c.query_drop("CREATE UNIQUE INDEX uniq_name ON children (name)")
        .await
        .unwrap();
    c.query_drop("DROP INDEX uniq_name ON children")
        .await
        .unwrap();
    let indexes: Vec<String> = c
        .query("SHOW INDEX FROM children")
        .await
        .unwrap()
        .into_iter()
        .map(|row: mysql_async::Row| row.get("Key_name").unwrap())
        .collect();
    assert!(!indexes.iter().any(|name| name == "uniq_name"));

    c.query_drop("ALTER TABLE children DROP FOREIGN KEY fk_parent")
        .await
        .unwrap();
    c.query_drop("INSERT INTO children VALUES (1, 999, 'orphan')")
        .await
        .unwrap();
}

#[tokio::test]
async fn multi_object_drop_processes_every_name() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    for name in ["drop_first", "drop_second", "drop_third"] {
        c.query_drop(format!("CREATE TABLE {name} (id INT PRIMARY KEY)"))
            .await
            .unwrap();
    }
    c.query_drop("DROP TABLE drop_first, drop_second, drop_third")
        .await
        .unwrap();
    let remaining: i64 = c
        .query_first(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_name IN ('drop_first', 'drop_second', 'drop_third')",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remaining, 0);

    c.query_drop("CREATE TABLE preserved_first (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE preserved_second (id INT PRIMARY KEY)")
        .await
        .unwrap();
    assert!(c
        .query_drop("DROP TABLE preserved_first, missing_table, preserved_second")
        .await
        .is_err());
    c.query_drop("INSERT INTO preserved_first VALUES (1)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO preserved_second VALUES (1)")
        .await
        .unwrap();
    c.query_drop("DROP TABLE IF EXISTS preserved_first, missing_table, preserved_second")
        .await
        .unwrap();

    c.query_drop("CREATE TABLE view_source (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE VIEW first_view AS SELECT id FROM view_source")
        .await
        .unwrap();
    c.query_drop("CREATE VIEW second_view AS SELECT id FROM view_source")
        .await
        .unwrap();
    c.query_drop("DROP VIEW first_view, second_view")
        .await
        .unwrap();
    assert!(c.query_drop("SELECT * FROM first_view").await.is_err());
    assert!(c.query_drop("SELECT * FROM second_view").await.is_err());
}

#[tokio::test]
async fn stored_set_operations_and_deep_view_chains_are_stack_safe() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE set_view_source (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO set_view_source VALUES (1), (2)")
        .await
        .unwrap();
    c.query_drop("CREATE VIEW set_view_inner AS SELECT id FROM set_view_source")
        .await
        .unwrap();
    c.query_drop(
        "CREATE VIEW set_view_outer AS
         SELECT id FROM set_view_inner UNION ALL SELECT id FROM set_view_source",
    )
    .await
    .unwrap();

    let count: i64 = c
        .query_first("SELECT COUNT(*) FROM set_view_outer")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 4);

    let mut previous = "set_view_outer".to_string();
    for depth in 0..24 {
        let view = format!("nested_set_view_{depth}");
        c.query_drop(format!("CREATE VIEW {view} AS SELECT id FROM {previous}"))
            .await
            .unwrap();
        previous = view;
    }
    let nested_count: i64 = c
        .query_first(format!("SELECT COUNT(*) FROM {previous}"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(nested_count, 4);

    c.query_drop("CREATE VIEW cyclic_set_view_a AS SELECT id FROM cyclic_set_view_b")
        .await
        .unwrap();
    c.query_drop("CREATE VIEW cyclic_set_view_b AS SELECT id FROM cyclic_set_view_a")
        .await
        .unwrap();
    let error = c
        .query_drop("SELECT * FROM cyclic_set_view_a")
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("query nesting exceeds 64 levels"),
        "unexpected error: {error}"
    );

    let source_count: i64 = c
        .query_first("SELECT COUNT(*) FROM set_view_source")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source_count, 2);
}

#[tokio::test]
async fn dropping_a_column_preserves_foreign_key_positions() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE parents (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE children (
            id INT PRIMARY KEY,
            obsolete INT,
            parent_id INT,
            payload INT,
            CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parents(id)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO parents VALUES (7)")
        .await
        .unwrap();

    c.query_drop("ALTER TABLE children DROP COLUMN obsolete")
        .await
        .unwrap();
    c.query_drop("INSERT INTO children VALUES (1, 7, 999)")
        .await
        .unwrap();
    assert!(c
        .query_drop("INSERT INTO children VALUES (2, 999, 7)")
        .await
        .is_err());

    let column: String = c
        .query_first(
            "SELECT column_name FROM information_schema.key_column_usage \
             WHERE table_name = 'children' AND constraint_name = 'fk_parent'",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(column, "parent_id");
}

#[tokio::test]
async fn dropping_an_indexed_column_removes_dependent_indexes() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE indexed_columns (
            id INT PRIMARY KEY,
            obsolete VARCHAR(20),
            retained INT,
            INDEX idx_obsolete (obsolete),
            UNIQUE INDEX uniq_obsolete_retained (obsolete, retained),
            INDEX idx_retained (retained)
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO indexed_columns VALUES
            (1, 'first', 10),
            (2, 'second', 20)",
    )
    .await
    .unwrap();

    c.query_drop("ALTER TABLE indexed_columns DROP COLUMN obsolete")
        .await
        .unwrap();

    let indexes: Vec<String> = c
        .query("SHOW INDEX FROM indexed_columns")
        .await
        .unwrap()
        .into_iter()
        .map(|row: mysql_async::Row| row.get("Key_name").unwrap())
        .collect();
    assert!(!indexes.iter().any(|name| name == "idx_obsolete"));
    assert!(!indexes.iter().any(|name| name == "uniq_obsolete_retained"));
    assert!(indexes.iter().any(|name| name == "idx_retained"));

    let rows: Vec<(i64, i64)> = c
        .query("SELECT id, retained FROM indexed_columns ORDER BY retained")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn adding_not_null_columns_backfills_mysql_implicit_values() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE alter_defaults (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO alter_defaults VALUES (1)")
        .await
        .unwrap();

    c.query_drop("ALTER TABLE alter_defaults ADD label VARCHAR(20) NOT NULL")
        .await
        .unwrap();
    c.query_drop("ALTER TABLE alter_defaults ADD attempts INT NOT NULL")
        .await
        .unwrap();

    let row: (String, i64) = c
        .query_first("SELECT label, attempts FROM alter_defaults WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row, (String::new(), 0));

    assert!(c
        .query_drop("INSERT INTO alter_defaults (id) VALUES (2)")
        .await
        .is_err());
}

#[tokio::test]
async fn update_order_by_limit_changes_only_the_ordered_rows() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE users (
            id INT PRIMARY KEY,
            first_name VARCHAR(20),
            active INT
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO users VALUES
            (1, 'Zoe', 0),
            (2, 'Ada', 0),
            (3, 'Ada', 0),
            (4, 'Mia', 0)",
    )
    .await
    .unwrap();

    c.query_drop(
        "UPDATE users SET active = 1
         ORDER BY first_name ASC, users.id ASC LIMIT 1",
    )
    .await
    .unwrap();
    assert_eq!(c.affected_rows(), 1);

    let active: Vec<i64> = c
        .query("SELECT id FROM users WHERE active = 1 ORDER BY id")
        .await
        .unwrap();
    assert_eq!(active, vec![2]);

    c.query_drop(
        "UPDATE users SET active = 2
         ORDER BY first_name DESC, id DESC LIMIT 2",
    )
    .await
    .unwrap();
    let updated: Vec<i64> = c
        .query("SELECT id FROM users WHERE active = 2 ORDER BY id")
        .await
        .unwrap();
    assert_eq!(updated, vec![1, 4]);

    c.query_drop(
        "UPDATE users SET first_name = 'ORDER BY is data', active = 3
         WHERE active = 0
         ORDER BY first_name ASC, id ASC LIMIT 1",
    )
    .await
    .unwrap();
    let filtered: Vec<i64> = c
        .query("SELECT id FROM users WHERE active = 3")
        .await
        .unwrap();
    assert_eq!(filtered, vec![3]);
}

#[tokio::test]
async fn update_and_delete_limit_bound_the_matching_rows() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE limited_mutations (
            id INT PRIMARY KEY,
            bucket INT,
            active INT
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO limited_mutations VALUES
            (1, 7, 0),
            (2, 7, 0),
            (3, 7, 0)",
    )
    .await
    .unwrap();

    c.query_drop("UPDATE limited_mutations SET active = 1 WHERE bucket = 7 LIMIT 0")
        .await
        .unwrap();
    assert_eq!(c.affected_rows(), 0);
    c.query_drop("DELETE FROM limited_mutations WHERE bucket = 7 LIMIT 0")
        .await
        .unwrap();
    assert_eq!(c.affected_rows(), 0);

    c.query_drop("UPDATE limited_mutations SET active = 1 WHERE bucket = 7 LIMIT 1")
        .await
        .unwrap();
    assert_eq!(c.affected_rows(), 1);
    let updated: i64 = c
        .query_first("SELECT COUNT(*) FROM limited_mutations WHERE active = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated, 1);

    c.query_drop("DELETE FROM limited_mutations WHERE bucket = 7 LIMIT 1")
        .await
        .unwrap();
    assert_eq!(c.affected_rows(), 1);
    let remaining: i64 = c
        .query_first("SELECT COUNT(*) FROM limited_mutations")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remaining, 2);
}

#[tokio::test]
async fn create_database_fails_instead_of_succeeding_as_a_noop() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    let error = c
        .query_drop("CREATE DATABASE unsupported_database")
        .await
        .unwrap_err();
    match error {
        mysql_async::Error::Server(error) => {
            assert_eq!(error.code, 1235);
            assert!(error.message.contains("single `elyra` database"));
        }
        other => panic!("expected a server error, got {other:?}"),
    }

    let databases: Vec<String> = c.query("SHOW DATABASES").await.unwrap();
    assert_eq!(databases, vec!["information_schema", "elyra"]);

    let error = c.query_drop("DROP DATABASE elyra").await.unwrap_err();
    assert!(
        matches!(error, mysql_async::Error::Server(error) if error.code == 1235),
        "DROP DATABASE must fail loudly too"
    );
}

/// `UNSIGNED` is a constraint, not a width. Every integer width is stored as 64
/// bits here, but the signedness has to be enforced on all of them or the same
/// schema is enforced inconsistently -- which is what happened while only
/// `BIGINT UNSIGNED` mapped to the unsigned type.
#[tokio::test]
async fn unsigned_is_enforced_on_every_integer_width() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    for (i, ty) in [
        "TINYINT UNSIGNED",
        "SMALLINT UNSIGNED",
        "MEDIUMINT UNSIGNED",
        "INT UNSIGNED",
        "INTEGER UNSIGNED",
        "BIGINT UNSIGNED",
    ]
    .iter()
    .enumerate()
    {
        let table = format!("uns_{i}");
        c.query_drop(format!("CREATE TABLE {table} (a {ty})"))
            .await
            .unwrap();

        // MySQL answers 1264 / 22003 for a value the column cannot hold.
        match c
            .query_drop(format!("INSERT INTO {table} VALUES (-1)"))
            .await
            .unwrap_err()
        {
            mysql_async::Error::Server(e) => {
                assert_eq!(e.code, 1264, "{ty}");
                assert_eq!(e.state, "22003", "{ty}");
            }
            other => panic!("{ty} accepted -1 or failed oddly: {other:?}"),
        }

        // Non-negative values still store and read back unchanged.
        c.query_drop(format!("INSERT INTO {table} VALUES (7)"))
            .await
            .unwrap();
        let v: Option<u64> = c
            .query_first(format!("SELECT a FROM {table}"))
            .await
            .unwrap();
        assert_eq!(v, Some(7), "{ty}");

        // ... and the column advertises itself as unsigned.
        let ddl: Option<(String, String)> = c
            .query_first(format!("SHOW CREATE TABLE {table}"))
            .await
            .unwrap();
        assert!(ddl.unwrap().1.contains("UNSIGNED"), "{ty}");
    }

    // A signed column is unaffected.
    c.query_drop("CREATE TABLE signed_col (a INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO signed_col VALUES (-1)")
        .await
        .unwrap();
    let v: Option<i64> = c.query_first("SELECT a FROM signed_col").await.unwrap();
    assert_eq!(v, Some(-1));

    // AUTO_INCREMENT over an unsigned key is the shape Laravel generates for
    // `$table->increments('id')`, so it has to keep working.
    c.query_drop(
        "CREATE TABLE uns_ai (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, n INT UNSIGNED)",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO uns_ai (n) VALUES (5),(7)")
        .await
        .unwrap();
    let ids: Vec<u64> = c.query("SELECT id FROM uns_ai ORDER BY id").await.unwrap();
    assert_eq!(ids, vec![1, 2]);
    let sum: Option<u64> = c.query_first("SELECT SUM(n) FROM uns_ai").await.unwrap();
    assert_eq!(sum, Some(12));
}

/// Result metadata must name the source table. Without it a client cannot tell
/// the two `id` columns of a join apart: not by name (they collide, exactly as
/// in MySQL) and not by metadata. Every expectation below was read off MySQL 8.4
/// on the same schema.
#[tokio::test]
async fn result_metadata_reports_the_source_table() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE meta_a (id INT PRIMARY KEY, name VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE meta_b (id INT PRIMARY KEY, a_id INT, label VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO meta_a VALUES (1,'Ada')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO meta_b VALUES (1,1,'post')")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE `meta.dot` (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO `meta.dot` VALUES (1)")
        .await
        .unwrap();

    // (sql, expected "table.column" per result column)
    for (sql, expected) in [
        (
            "SELECT * FROM meta_a JOIN meta_b ON meta_b.a_id = meta_a.id",
            vec![
                "meta_a.id",
                "meta_a.name",
                "meta_b.id",
                "meta_b.a_id",
                "meta_b.label",
            ],
        ),
        (
            "SELECT meta_a.name, meta_b.label FROM meta_a JOIN meta_b ON meta_b.a_id = meta_a.id",
            vec!["meta_a.name", "meta_b.label"],
        ),
        ("SELECT * FROM meta_a", vec!["meta_a.id", "meta_a.name"]),
        (
            "SELECT elyra.meta_a.id FROM elyra.meta_a",
            vec!["meta_a.id"],
        ),
        // The alias, not the table name -- as MySQL reports it.
        ("SELECT x.id FROM meta_a x", vec!["x.id"]),
        ("SELECT x.* FROM meta_a x", vec!["x.id", "x.name"]),
        ("SELECT elyra.x.id FROM elyra.meta_a x", vec!["x.id"]),
        (
            "SELECT `meta.dot`.id FROM elyra.`meta.dot`",
            vec!["meta.dot.id"],
        ),
        (
            "SELECT `alias.dot`.id FROM meta_a AS `alias.dot`",
            vec!["alias.dot.id"],
        ),
        // A computed column has no source table in MySQL either.
        (
            "SELECT id + 1 AS next, name FROM meta_a",
            vec![".next", "meta_a.name"],
        ),
        ("SELECT COUNT(*) AS c FROM meta_a", vec![".c"]),
    ] {
        let result = c.query_iter(sql).await.unwrap();
        let got: Vec<String> = result
            .columns()
            .expect("result set has columns")
            .iter()
            .map(|col| format!("{}.{}", col.table_str(), col.name_str()))
            .collect();
        result.drop_result().await.unwrap();
        assert_eq!(got, expected, "{sql}");
    }
}

/// `CREATE TABLE ... AS SELECT` over an aggregate used to fail with an
/// unresolvable column: the option-stripper treated the first `(` in the
/// statement as the start of a column list, so `COUNT(*)` truncated the query
/// there. Materialized views run through the same path, which is why an
/// aggregate view was impossible to create.
#[tokio::test]
async fn ctas_and_materialized_views_accept_aggregates() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE src (id INT PRIMARY KEY, g INT, v INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO src VALUES (1,1,10),(2,1,20),(3,2,30)")
        .await
        .unwrap();

    c.query_drop("CREATE TABLE agg AS SELECT g, COUNT(*) AS c, SUM(v) AS s FROM src GROUP BY g")
        .await
        .unwrap();
    let rows: Vec<(i64, i64, i64)> = c.query("SELECT g, c, s FROM agg ORDER BY g").await.unwrap();
    assert_eq!(rows, vec![(1, 2, 30), (2, 1, 30)]);

    // The same shape through a materialized view, plus a bare aggregate and a
    // derived table -- all of which contain a paren before any column list.
    c.query_drop("CREATE MATERIALIZED VIEW mv AS SELECT g, COUNT(*) AS c FROM src GROUP BY g")
        .await
        .unwrap();
    let mv: Vec<(i64, i64)> = c.query("SELECT g, c FROM mv ORDER BY g").await.unwrap();
    assert_eq!(mv, vec![(1, 2), (2, 1)]);

    c.query_drop("CREATE TABLE total AS SELECT COUNT(*) AS n FROM src")
        .await
        .unwrap();
    let total: Option<i64> = c.query_first("SELECT n FROM total").await.unwrap();
    assert_eq!(total, Some(3));

    c.query_drop(
        "CREATE TABLE derived AS SELECT * FROM (SELECT g, SUM(v) AS s FROM src GROUP BY g) x",
    )
    .await
    .unwrap();
    let derived: Option<i64> = c.query_first("SELECT COUNT(*) FROM derived").await.unwrap();
    assert_eq!(derived, Some(2));

    // Real table options are still stripped rather than reaching the parser.
    c.query_drop("CREATE TABLE opts (id INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        .await
        .unwrap();
}

/// Constraints that are enforced must also be visible, or a dump taken through
/// `SHOW CREATE TABLE` silently loses them.
#[tokio::test]
async fn show_create_table_echoes_checks_and_foreign_keys() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE parent (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO parent VALUES (1)").await.unwrap();
    c.query_drop("CREATE TABLE child (id INT PRIMARY KEY, pid INT, qty INT, CHECK (qty > 0))")
        .await
        .unwrap();
    // Added by ALTER rather than declared, which is the form that was invisible.
    c.query_drop("ALTER TABLE child ADD FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE")
        .await
        .unwrap();

    let ddl: Option<(String, String)> = c.query_first("SHOW CREATE TABLE child").await.unwrap();
    let ddl = ddl.expect("SHOW CREATE TABLE returned no row").1;
    assert!(ddl.contains("CHECK (qty > 0)"), "{ddl}");
    assert!(
        ddl.contains("FOREIGN KEY (`pid`) REFERENCES `parent` (`id`)"),
        "{ddl}"
    );
    assert!(ddl.contains("ON DELETE CASCADE"), "{ddl}");

    // And the emitted DDL has to be accepted back, with the constraints live.
    let copy = ddl.replace("`child`", "`child_copy`");
    c.query_drop(copy).await.unwrap();
    let orphan = c
        .query_drop("INSERT INTO child_copy VALUES (1, 999, 1)")
        .await
        .unwrap_err();
    assert!(
        matches!(orphan, mysql_async::Error::Server(ref e) if e.code == 1452),
        "the round-tripped foreign key must still be enforced: {orphan:?}"
    );
}

/// Clients branch on the error code: an unknown column is not a missing table.
#[tokio::test]
async fn catalog_errors_use_the_specific_mysql_codes() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE codes (id INT PRIMARY KEY)")
        .await
        .unwrap();

    for (sql, code, state) in [
        ("SELECT nope FROM codes", 1054u16, "42S22"),
        ("SELECT * FROM missing_table", 1146, "42S02"),
    ] {
        match c.query_drop(sql).await.unwrap_err() {
            mysql_async::Error::Server(e) => {
                assert_eq!(e.code, code, "{sql}");
                assert_eq!(e.state, state, "{sql}");
            }
            other => panic!("expected a server error for {sql}, got {other:?}"),
        }
    }
}

/// A fractional literal compared against an integer key is a *comparison*, not a
/// value to store, so it must not be rounded into the key's domain: `k > 1024.5`
/// means `k >= 1025`, and `k = 1024.5` matches nothing. Every answer below was
/// verified against MySQL 8.4 on the same data.
#[tokio::test]
async fn fractional_bounds_on_an_integer_key_match_mysql() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE frac (k INT PRIMARY KEY, s INT)")
        .await
        .unwrap();
    let rows: Vec<String> = (-8..=2048).map(|i| format!("({i},{i})")).collect();
    for chunk in rows.chunks(500) {
        c.query_drop(format!("INSERT INTO frac VALUES {}", chunk.join(",")))
            .await
            .unwrap();
    }
    c.query_drop("CREATE INDEX ix_frac_s ON frac (s)")
        .await
        .unwrap();

    for (sql, expected) in [
        // The bound sits between two keys: strictness must not shift by one row.
        ("SELECT COUNT(*) FROM frac WHERE k > 1024.5", 1024),
        ("SELECT COUNT(*) FROM frac WHERE k >= 1024.5", 1024),
        ("SELECT COUNT(*) FROM frac WHERE k < 1024.5", 1033),
        ("SELECT COUNT(*) FROM frac WHERE k <= 1024.5", 1033),
        // Reversed operands, and a secondary index rather than the PK.
        ("SELECT COUNT(*) FROM frac WHERE 1024.5 < k", 1024),
        ("SELECT COUNT(*) FROM frac WHERE s > 1024.5", 1024),
        // Negative bounds round away from zero, so the direction matters there too.
        ("SELECT COUNT(*) FROM frac WHERE k > -3.5", 2052),
        ("SELECT COUNT(*) FROM frac WHERE k < -3.5", 5),
        // No integer equals a fractional literal -- via `=`, `IN` or `BETWEEN`.
        ("SELECT COUNT(*) FROM frac WHERE k = 1024.5", 0),
        ("SELECT COUNT(*) FROM frac WHERE s = 1024.5", 0),
        ("SELECT COUNT(*) FROM frac WHERE k IN (1024.5, 7)", 1),
        (
            "SELECT COUNT(*) FROM frac WHERE k BETWEEN 1024.5 AND 2048.5",
            1024,
        ),
        // Exact bounds keep their existing meaning.
        ("SELECT COUNT(*) FROM frac WHERE k > 1024", 1024),
        (
            "SELECT COUNT(*) FROM frac WHERE k BETWEEN 1025 AND 2048",
            1024,
        ),
    ] {
        let got: Option<i64> = c.query_first(sql).await.unwrap();
        assert_eq!(got, Some(expected), "{sql}");
    }
}

#[tokio::test]
async fn conditional_database_ddl_is_a_no_op() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    // Laravel's MigrateCommand, container entrypoints and our own benches ask
    // for the database to *exist*, not to be new; that is satisfiable here.
    c.query_drop("CREATE DATABASE IF NOT EXISTS elyra")
        .await
        .unwrap();
    c.query_drop("CREATE SCHEMA IF NOT EXISTS laravel")
        .await
        .unwrap();
    c.query_drop("DROP DATABASE IF EXISTS never_created")
        .await
        .unwrap();

    let databases: Vec<String> = c.query("SHOW DATABASES").await.unwrap();
    assert_eq!(databases, vec!["information_schema", "elyra"]);

    // ... but dropping the database the session is using is not a no-op.
    let error = c
        .query_drop("DROP DATABASE IF EXISTS elyra")
        .await
        .unwrap_err();
    assert!(
        matches!(error, mysql_async::Error::Server(error) if error.code == 1235),
        "dropping the live database must fail even with IF EXISTS"
    );

    // The connection is still usable after each refusal.
    let one: Option<i32> = c.query_first("SELECT 1").await.unwrap();
    assert_eq!(one, Some(1));
}

#[tokio::test]
async fn alter_change_and_modify_accept_collation() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE collate_changes (
            id INT PRIMARY KEY,
            a TEXT,
            INDEX idx_a (a)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO collate_changes VALUES (1, 'Alpha')")
        .await
        .unwrap();

    c.query_drop(
        "ALTER TABLE collate_changes
         CHANGE a a2 TEXT COLLATE 'utf8mb4_bin'",
    )
    .await
    .unwrap();
    let binary_matches: i64 = c
        .query_first("SELECT COUNT(*) FROM collate_changes WHERE a2 = 'alpha'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binary_matches, 0);

    c.query_drop(
        "ALTER TABLE collate_changes
         MODIFY a2 TEXT COLLATE utf8mb4_0900_ai_ci",
    )
    .await
    .unwrap();
    let insensitive_matches: i64 = c
        .query_first("SELECT COUNT(*) FROM collate_changes WHERE a2 = 'alpha'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(insensitive_matches, 1);
}

#[tokio::test]
async fn standalone_rename_table_preserves_rows_indexes_and_auto_increment() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE rename_source (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            a INT,
            INDEX idx_a (a)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO rename_source (a) VALUES (10)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE rename_child (
            id INT PRIMARY KEY,
            source_id INT,
            CONSTRAINT fk_rename_source
                FOREIGN KEY (source_id) REFERENCES rename_source(id)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO rename_child VALUES (1, 1)")
        .await
        .unwrap();

    c.query_drop("RENAME TABLE rename_source TO rename_target")
        .await
        .unwrap();
    c.query_drop("UPDATE rename_child SET source_id = 1 WHERE id = 1")
        .await
        .unwrap();
    c.query_drop("INSERT INTO rename_target (a) VALUES (20)")
        .await
        .unwrap();
    let rows: Vec<(i64, i64)> = c
        .query("SELECT id, a FROM rename_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    let indexes: Vec<String> = c
        .query("SHOW INDEX FROM rename_target")
        .await
        .unwrap()
        .into_iter()
        .map(|row: mysql_async::Row| row.get("Key_name").unwrap())
        .collect();
    assert!(indexes.iter().any(|name| name == "idx_a"));
    assert!(c.query_drop("SELECT * FROM rename_source").await.is_err());
}

#[tokio::test]
async fn alter_table_rename_index_rekeys_existing_entries() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE rename_index_table (
            id INT PRIMARY KEY,
            a INT,
            INDEX idx_old (a)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO rename_index_table VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    c.query_drop("ALTER TABLE rename_index_table RENAME INDEX idx_old TO idx_new")
        .await
        .unwrap();
    let indexes: Vec<String> = c
        .query("SHOW INDEX FROM rename_index_table")
        .await
        .unwrap()
        .into_iter()
        .map(|row: mysql_async::Row| row.get("Key_name").unwrap())
        .collect();
    assert!(!indexes.iter().any(|name| name == "idx_old"));
    assert!(indexes.iter().any(|name| name == "idx_new"));

    let id: i64 = c
        .query_first("SELECT id FROM rename_index_table WHERE a = 20")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id, 2);
}

#[tokio::test]
async fn add_auto_increment_primary_key_backfills_existing_rows() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE add_auto_primary (
            a INT NOT NULL,
            label VARCHAR(10),
            INDEX idx_a (a)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO add_auto_primary VALUES (30, 'c'), (10, 'a'), (20, 'b')")
        .await
        .unwrap();

    c.query_drop(
        "ALTER TABLE add_auto_primary
         ADD id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY",
    )
    .await
    .unwrap();
    let rows: Vec<(u64, i64, String)> = c
        .query("SELECT id, a, label FROM add_auto_primary ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, 30, "c".into()),
            (2, 10, "a".into()),
            (3, 20, "b".into())
        ]
    );

    c.query_drop("INSERT INTO add_auto_primary (a, label) VALUES (40, 'd')")
        .await
        .unwrap();
    assert_eq!(c.last_insert_id(), Some(4));
    let indexed_id: u64 = c
        .query_first("SELECT id FROM add_auto_primary WHERE a = 20")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(indexed_id, 3);
}

#[tokio::test]
async fn transactions_commit_and_rollback() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();

    // committed insert persists
    c.query_drop("BEGIN").await.unwrap();
    c.query_drop("INSERT INTO t VALUES (1, 10)").await.unwrap();
    c.query_drop("COMMIT").await.unwrap();

    // rolled-back insert does not
    c.query_drop("BEGIN").await.unwrap();
    c.query_drop("INSERT INTO t VALUES (2, 20)").await.unwrap();
    c.query_drop("ROLLBACK").await.unwrap();

    let ids: Vec<i64> = c.query("SELECT id FROM t ORDER BY id").await.unwrap();
    assert_eq!(ids, vec![1]);
}

#[tokio::test]
async fn transactional_update_keeps_unchanged_index_entries_visible() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE records (
            id INT PRIMARY KEY,
            lookup_key INT,
            note VARCHAR(20),
            INDEX lookup_key_idx (lookup_key)
        )",
    )
    .await
    .unwrap();

    c.query_drop("BEGIN").await.unwrap();
    c.query_drop("INSERT INTO records VALUES (1, 7, 'before')")
        .await
        .unwrap();
    c.query_drop("UPDATE records SET note = 'after' WHERE id = 1")
        .await
        .unwrap();

    let note: String = c
        .query_first("SELECT note FROM records WHERE lookup_key = 7")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(note, "after");

    c.query_drop("COMMIT").await.unwrap();
    let note: String = c
        .query_first("SELECT note FROM records WHERE lookup_key = 7")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(note, "after");
}

#[tokio::test]
async fn aggregation_and_group_by() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE sales (region VARCHAR(16), amount INT)")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO sales VALUES ('north',10),('north',30),('south',5),('south',15),('south',20)",
    )
    .await
    .unwrap();

    let mut rows: Vec<(String, i64, i64)> = c
        .query("SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region")
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows, vec![("north".into(), 2, 40), ("south".into(), 3, 40)]);

    let total: i64 = c
        .query_first("SELECT SUM(amount) FROM sales")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(total, 80);
}

#[tokio::test]
async fn joins() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE authors (id INT PRIMARY KEY, name VARCHAR(32))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE books (id INT PRIMARY KEY, author_id INT, title VARCHAR(64))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO authors VALUES (1,'Tolkien'),(2,'Le Guin')")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO books VALUES (1,1,'The Hobbit'),(2,1,'LOTR'),(3,2,'A Wizard of Earthsea')",
    )
    .await
    .unwrap();

    let mut rows: Vec<(String, String)> = c
        .query(
            "SELECT a.name, b.title FROM authors a JOIN books b ON b.author_id = a.id ORDER BY b.id",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("Le Guin".into(), "A Wizard of Earthsea".into()),
            ("Tolkien".into(), "LOTR".into()),
            ("Tolkien".into(), "The Hobbit".into()),
        ]
    );
}

/// Native (binary) prepared statements via `exec*` -- exercises
/// COM_STMT_PREPARE + COM_STMT_EXECUTE with binary parameter binding and
/// binary result rows. This is the critical wire-protocol path.
#[tokio::test]
async fn native_prepared_statements() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    // Constant expression through a prepared statement.
    let sum: i64 = c
        .exec_first("SELECT ? + ?", (40, 2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sum, 42);

    c.query_drop("CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR(32), qty INT)")
        .await
        .unwrap();

    // Parameterised INSERT executed repeatedly (prepared once, executed thrice).
    let params = vec![(1, "apple", 5), (2, "pear", 8), (3, "plum", 13)];
    for (id, name, qty) in &params {
        c.exec_drop(
            "INSERT INTO items (id, name, qty) VALUES (?, ?, ?)",
            (id, name, qty),
        )
        .await
        .unwrap();
    }

    // Parameterised SELECT with a bound predicate.
    let name: String = c
        .exec_first("SELECT name FROM items WHERE id = ?", (2,))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(name, "pear");

    let rows: Vec<(i64, String, i64)> = c
        .exec(
            "SELECT id, name, qty FROM items WHERE qty >= ? ORDER BY id",
            (8,),
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![(2, "pear".into(), 8), (3, "plum".into(), 13)]);
}

#[tokio::test]
async fn prepared_aggregate_rows_match_declared_result_types() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE measurements (id INT PRIMARY KEY, amount DOUBLE)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO measurements VALUES (1, 500.0), (2, 100.0)")
        .await
        .unwrap();

    let row: (String, i64) = c
        .exec_first(
            "SELECT
                COALESCE(
                    MAX(CASE WHEN id = 1 THEN amount END) -
                    MIN(CASE WHEN id = 2 THEN amount END),
                    0
                ) AS difference,
                ABS(SUM(CASE WHEN id = ? THEN 1 ELSE 0 END)) AS matches
             FROM measurements",
            (1,),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row, ("400".into(), 1));
}

/// Qualified wildcard `alias.*` in the projection expands to that table's
/// columns. [ESQL-9]
#[tokio::test]
async fn qualified_wildcard() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE qa (id INT PRIMARY KEY, name VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE qb (id INT PRIMARY KEY, a_id INT, label VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO qa VALUES (1,'Ada'),(2,'Lin')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO qb VALUES (1,1,'post'),(2,2,'blog')")
        .await
        .unwrap();

    let result = c
        .query_iter("SELECT qa.* FROM qa JOIN qb ON qb.a_id = qa.id ORDER BY qa.id")
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "name"]);
    result.drop_result().await.unwrap();

    let result = c
        .query_iter("SELECT * FROM qa JOIN qb ON qb.a_id = qa.id ORDER BY qa.id")
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "name", "id", "a_id", "label"]);
    result.drop_result().await.unwrap();

    // a.* -> only qa's two columns
    let rows: Vec<(i64, String)> = c
        .query("SELECT qa.* FROM qa JOIN qb ON qb.a_id = qa.id ORDER BY qa.id")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, "Ada".into()), (2, "Lin".into())]);

    // b.* -> qb's three columns
    let rows: Vec<(i64, i64, String)> = c
        .query("SELECT qb.* FROM qa JOIN qb ON qb.a_id = qa.id WHERE qa.id = 1")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 1, "post".into())]);
}

#[tokio::test]
async fn relation_qualifiers_follow_mysql_case_rules() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE qualifier_case_a (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE qualifier_case_b (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO qualifier_case_a VALUES (1, 10)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO qualifier_case_b VALUES (1)")
        .await
        .unwrap();

    let value: i64 = c
        .query_first("SELECT a.id FROM qualifier_case_a AS a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 1);
    let wrong_case = c
        .query_drop("SELECT A.id FROM qualifier_case_a AS a")
        .await
        .unwrap_err();
    assert!(matches!(
        wrong_case,
        mysql_async::Error::Server(ref error)
            if error.code == 1054 && error.state == "42S22"
    ));
    assert!(c
        .query_drop("SELECT ELYRA.qualifier_case_a.id FROM elyra.qualifier_case_a")
        .await
        .is_err());
    for invalid in [
        "SELECT garbage.a.id FROM qualifier_case_a AS a",
        "SELECT qualifier_case_a.id FROM qualifier_case_a AS a",
        "SELECT wrong.qualifier_case_a.id FROM qualifier_case_a",
        "SELECT extra.elyra.qualifier_case_a.id FROM qualifier_case_a",
        "SELECT a.id FROM qualifier_case_a AS a
         WHERE EXISTS (
             SELECT 1 FROM qualifier_case_b AS b
             WHERE A.id = b.id
         )",
    ] {
        assert!(
            c.query_drop(invalid).await.is_err(),
            "query succeeded: {invalid}"
        );
    }

    let joined: Vec<(i64, i64)> = c
        .query(
            "SELECT Dup.id, dUP.id
             FROM qualifier_case_a AS Dup
             JOIN qualifier_case_b AS dUP ON Dup.id = dUP.id",
        )
        .await
        .unwrap();
    assert_eq!(joined, [(1, 1)]);

    assert!(c
        .query_drop("UPDATE qualifier_case_a AS a SET A.value = 11 WHERE A.id = 1")
        .await
        .is_err());
    let unchanged: i64 = c
        .query_first("SELECT value FROM qualifier_case_a WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged, 10);

    let derived: i64 = c
        .query_first(
            "SELECT arbitrary.d.id
             FROM (SELECT id FROM qualifier_case_a) AS d",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(derived, 1);
    assert!(c
        .query_drop(
            "SELECT arbitrary.d.*
             FROM (SELECT id FROM qualifier_case_a) AS d",
        )
        .await
        .is_err());

    let cte: i64 = c
        .query_first(
            "WITH case_cte AS (SELECT id FROM qualifier_case_a)
             SELECT elyra.case_cte.id FROM case_cte",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cte, 1);
    assert!(c
        .query_drop(
            "WITH case_cte AS (SELECT id FROM qualifier_case_a)
             SELECT elyra.case_cte.* FROM case_cte",
        )
        .await
        .is_err());
}

#[tokio::test]
async fn column_identifiers_remain_case_insensitive() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE column_case (`MixedCase` INT, `Ünicode` INT, `ẞ` INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO column_case VALUES (7, 13, 19)")
        .await
        .unwrap();

    let row: (i64, i64, i64) = c
        .query_first("SELECT mixedcase, `ünicode`, `ẞ` FROM column_case")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row, (7, 13, 19));
    assert!(c.query_drop("SELECT `ß` FROM column_case").await.is_err());

    c.query_drop(
        "CREATE TABLE unicode_alias_case (
            id INT PRIMARY KEY,
            `Ünicode` INT,
            plain INT
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO unicode_alias_case VALUES
         (1, 1, 30), (2, 1, 21), (3, 2, 10), (4, 3, 1)",
    )
    .await
    .unwrap();

    let ordered_alias: Vec<i64> = c
        .query("SELECT -id AS `İ` FROM unicode_alias_case ORDER BY i")
        .await
        .unwrap();
    assert_eq!(ordered_alias, [-4, -3, -2, -1]);

    let grouped_alias: Vec<(i64, i64)> = c
        .query(
            "SELECT plain % 2 AS `İ`, COUNT(*)
             FROM unicode_alias_case GROUP BY i ORDER BY i",
        )
        .await
        .unwrap();
    assert_eq!(grouped_alias, [(0, 2), (1, 2)]);

    let ordered_output: Vec<i64> = c
        .query(
            "SELECT plain AS `ünicode`
             FROM unicode_alias_case ORDER BY `ünicode`, id",
        )
        .await
        .unwrap();
    assert_eq!(ordered_output, [1, 10, 21, 30]);
}

#[tokio::test]
async fn unicode_column_identifiers_match_across_writes_indexes_and_joins() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE unicode_dml (id INT PRIMARY KEY, `Ünicode` INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO unicode_dml (id, `ünicode`) VALUES (1, 10)")
        .await
        .unwrap();
    c.query_drop("UPDATE unicode_dml SET `ünicode` = 11 WHERE id = 1")
        .await
        .unwrap();
    let invalid_target = c
        .query_drop("UPDATE unicode_dml AS a SET b.`Ünicode` = 13 WHERE a.id = 1")
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_target,
        mysql_async::Error::Server(ref error) if error.code == 1054
    ));
    let invalid_long_target = c
        .query_drop(
            "UPDATE unicode_dml AS a
             SET extra.unicode_dml.a.`Ünicode` = 13 WHERE a.id = 1",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_long_target,
        mysql_async::Error::Server(ref error) if error.code == 1064
    ));
    let invalid_upsert_target = c
        .query_drop(
            "INSERT INTO unicode_dml VALUES (1, 13)
             ON DUPLICATE KEY UPDATE b.`Ünicode` = 13",
        )
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_upsert_target,
        mysql_async::Error::Server(ref error) if error.code == 1054
    ));

    c.query_drop("CREATE INDEX unicode_idx ON unicode_dml (`ünicode`)")
        .await
        .unwrap();
    let value: i64 = c
        .query_first("SELECT `Ünicode` FROM unicode_dml WHERE `ünicode` = 11")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 11);
    let filtered_sum: i64 = c
        .query_first("SELECT SUM(id) FROM unicode_dml WHERE `ünicode` = 11")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(filtered_sum, 1);

    c.query_drop("CREATE TABLE unicode_join_left (`Ünicode` INT, left_value INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE unicode_join_right (`ünicode` INT, right_value INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO unicode_join_left VALUES (7, 70)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO unicode_join_right VALUES (7, 700)")
        .await
        .unwrap();

    let using_row: (i64, i64) = c
        .query_first(
            "SELECT l.left_value, r.right_value
             FROM unicode_join_left AS l
             JOIN unicode_join_right AS r USING (`ünicode`)",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(using_row, (70, 700));

    let mut natural = c
        .query_iter("SELECT * FROM unicode_join_left NATURAL JOIN unicode_join_right")
        .await
        .unwrap();
    let names = natural
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, ["Ünicode", "left_value", "right_value"]);
    let rows: Vec<(i64, i64, i64)> = natural.collect().await.unwrap();
    assert_eq!(rows, [(7, 70, 700)]);
}

#[tokio::test]
async fn unicode_column_grants_follow_identifier_case_rules() {
    let srv = TestServer::start_with_auth("root", "rootpw").await;
    let mut root = srv.conn_as("root", "rootpw").await;

    root.query_drop("CREATE TABLE unicode_grants (id INT PRIMARY KEY, `Ünicode` INT)")
        .await
        .unwrap();
    root.query_drop("INSERT INTO unicode_grants VALUES (1, 10)")
        .await
        .unwrap();
    root.query_drop("CREATE USER 'unicode_reader' IDENTIFIED BY 'passw0rd'")
        .await
        .unwrap();
    root.query_drop("GRANT SELECT (id, `Ünicode`) ON unicode_grants TO 'unicode_reader'")
        .await
        .unwrap();

    let mut reader = srv.conn_as("unicode_reader", "passw0rd").await;
    let value: i64 = reader
        .query_first("SELECT `ünicode` FROM unicode_grants")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 10);

    root.query_drop("REVOKE SELECT (`ünicode`) ON unicode_grants FROM 'unicode_reader'")
        .await
        .unwrap();
    assert!(reader
        .query_drop("SELECT `Ünicode` FROM unicode_grants")
        .await
        .is_err());
}

#[tokio::test]
async fn using_join_coalesces_keys_and_keeps_qualified_access() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE using_l (id INT PRIMARY KEY, lval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE using_r (id INT PRIMARY KEY, rval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_l VALUES (1,'L1'),(2,'L2'),(4,'L4')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_r VALUES (1,'R1'),(3,'R3'),(4,'R4')")
        .await
        .unwrap();

    let mut result = c
        .query_iter("SELECT * FROM using_l AS l JOIN using_r AS r USING(id) ORDER BY id")
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "lval", "rval"]);
    let tables: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.table_str().into_owned())
        .collect();
    assert_eq!(tables, ["l", "l", "r"]);
    let rows: Vec<(i64, String, String)> = result.collect().await.unwrap();
    assert_eq!(
        rows,
        [(1, "L1".into(), "R1".into()), (4, "L4".into(), "R4".into())]
    );

    let rows: Vec<(i64, String, Option<String>)> = c
        .query("SELECT * FROM using_l AS l LEFT JOIN using_r AS r USING(id) ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            (1, "L1".into(), Some("R1".into())),
            (2, "L2".into(), None),
            (4, "L4".into(), Some("R4".into())),
        ]
    );

    let mut result = c
        .query_iter("SELECT * FROM using_l AS l RIGHT JOIN using_r AS r USING(id) ORDER BY id")
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "rval", "lval"]);
    let tables: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.table_str().into_owned())
        .collect();
    assert_eq!(tables, ["r", "r", "l"]);
    let rows: Vec<(i64, String, Option<String>)> = result.collect().await.unwrap();
    assert_eq!(
        rows,
        [
            (1, "R1".into(), Some("L1".into())),
            (3, "R3".into(), None),
            (4, "R4".into(), Some("L4".into())),
        ]
    );

    let rows: Vec<(i64, Option<i64>, i64)> = c
        .query(
            "SELECT id AS bare_id, l.id AS left_id, r.id AS right_id
             FROM using_l AS l RIGHT JOIN using_r AS r USING(id)
             ORDER BY r.id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [(1, Some(1), 1), (3, None, 3), (4, Some(4), 4)]);

    let mut result = c
        .query_iter(
            "SELECT l.*, r.*
             FROM using_l AS l LEFT JOIN using_r AS r USING(id)
             ORDER BY l.id",
        )
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "lval", "id", "rval"]);
    let rows: Vec<(i64, String, Option<i64>, Option<String>)> = result.collect().await.unwrap();
    assert_eq!(
        rows,
        [
            (1, "L1".into(), Some(1), Some("R1".into())),
            (2, "L2".into(), None, None),
            (4, "L4".into(), Some(4), Some("R4".into())),
        ]
    );

    let rows: Vec<i64> = c
        .query(
            "SELECT id + 0
             FROM using_l AS l JOIN using_r AS r USING(id)
             GROUP BY id
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [1, 4]);
    let rows: Vec<i64> = c
        .query(
            "SELECT id + COUNT(*)
             FROM using_l AS l JOIN using_r AS r USING(id)
             GROUP BY id
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [2, 5]);

    assert!(c
        .query_drop("SELECT * FROM using_l JOIN using_r USING(missing)")
        .await
        .is_err());
}

#[tokio::test]
async fn natural_join_uses_all_common_columns_and_mysql_star_order() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE natural_l (id INT PRIMARY KEY, code INT, lval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE natural_r (code INT, id INT PRIMARY KEY, rval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO natural_l VALUES (1,10,'L1'),(2,20,'L2'),(4,40,'L4')")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO natural_r VALUES
         (10,1,'R1'),(30,3,'R3'),(40,4,'R4'),(20,99,'code-only'),(99,2,'id-only')",
    )
    .await
    .unwrap();

    let mut result = c
        .query_iter("SELECT * FROM natural_l AS l NATURAL JOIN natural_r AS r ORDER BY id")
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "code", "lval", "rval"]);
    let rows: Vec<(i64, i64, String, String)> = result.collect().await.unwrap();
    assert_eq!(
        rows,
        [
            (1, 10, "L1".into(), "R1".into()),
            (4, 40, "L4".into(), "R4".into()),
        ]
    );

    let mut result = c
        .query_iter(
            "SELECT *
             FROM natural_l AS l NATURAL JOIN natural_r AS r
             GROUP BY id, code
             ORDER BY id",
        )
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "code", "lval", "rval"]);
    let rows: Vec<(i64, i64, String, String)> = result.collect().await.unwrap();
    assert_eq!(
        rows,
        [
            (1, 10, "L1".into(), "R1".into()),
            (4, 40, "L4".into(), "R4".into()),
        ]
    );

    let mut result = c
        .query_iter("SELECT * FROM natural_l AS l JOIN natural_r AS r USING(code, id) ORDER BY id")
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "code", "lval", "rval"]);
    let rows: Vec<(i64, i64, String, String)> = result.collect().await.unwrap();
    assert_eq!(
        rows,
        [
            (1, 10, "L1".into(), "R1".into()),
            (4, 40, "L4".into(), "R4".into()),
        ]
    );

    let rows: Vec<(i64, i64, String, Option<String>)> = c
        .query("SELECT * FROM natural_l AS l NATURAL LEFT JOIN natural_r AS r ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            (1, 10, "L1".into(), Some("R1".into())),
            (2, 20, "L2".into(), None),
            (4, 40, "L4".into(), Some("R4".into())),
        ]
    );

    let mut result = c
        .query_iter(
            "SELECT * FROM natural_l AS l NATURAL RIGHT JOIN natural_r AS r ORDER BY id, code",
        )
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["code", "id", "rval", "lval"]);
    let rows: Vec<(i64, i64, String, Option<String>)> = result.collect().await.unwrap();
    assert_eq!(
        rows,
        [
            (10, 1, "R1".into(), Some("L1".into())),
            (99, 2, "id-only".into(), None),
            (30, 3, "R3".into(), None),
            (40, 4, "R4".into(), Some("L4".into())),
            (20, 99, "code-only".into(), None),
        ]
    );

    let mut result = c
        .query_iter(
            "SELECT l.*, r.*
             FROM natural_l AS l NATURAL LEFT JOIN natural_r AS r
             ORDER BY l.id",
        )
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "code", "lval", "code", "id", "rval"]);
    type QualifiedNaturalRow = (i64, i64, String, Option<i64>, Option<i64>, Option<String>);
    let rows: Vec<QualifiedNaturalRow> = result.collect().await.unwrap();
    assert_eq!(
        rows,
        [
            (1, 10, "L1".into(), Some(10), Some(1), Some("R1".into())),
            (2, 20, "L2".into(), None, None, None),
            (4, 40, "L4".into(), Some(40), Some(4), Some("R4".into())),
        ]
    );

    c.query_drop("CREATE TABLE natural_x (x INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE natural_y (y INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO natural_x VALUES (1),(2)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO natural_y VALUES (8),(9)")
        .await
        .unwrap();
    let rows: Vec<(i64, i64)> = c
        .query("SELECT * FROM natural_x NATURAL JOIN natural_y ORDER BY x, y")
        .await
        .unwrap();
    assert_eq!(rows, [(1, 8), (1, 9), (2, 8), (2, 9)]);
}

#[tokio::test]
async fn using_and_natural_joins_preserve_quoted_identifier_boundaries() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE `join.left` (`join.key` INT PRIMARY KEY, left_value VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE `join.right` (`join.key` INT PRIMARY KEY, right_value VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE `join.third` (`join.key` INT PRIMARY KEY, third_value VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO `join.left` VALUES (1,'L1'),(2,'L2')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO `join.right` VALUES (1,'R1'),(3,'R3')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO `join.third` VALUES (1,'T1'),(4,'T4')")
        .await
        .unwrap();

    let sql = "SELECT *
               FROM `join.left` AS `left.alias`
               JOIN `join.right` AS `right.alias` USING (`join.key`)
               JOIN `join.third` AS `third.alias` USING (`join.key`)";
    let mut result = c.query_iter(sql).await.unwrap();
    let columns = result.columns_ref();
    let names = columns
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["join.key", "left_value", "right_value", "third_value"]
    );
    let tables = columns
        .iter()
        .map(|column| column.table_str().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        tables,
        ["left.alias", "left.alias", "right.alias", "third.alias"]
    );
    let rows: Vec<(i64, String, String, String)> = result.collect().await.unwrap();
    assert_eq!(rows, [(1, "L1".into(), "R1".into(), "T1".into())]);

    let qualified: Vec<(i64, i64)> = c
        .query(
            "SELECT `left.alias`.`join.key`, `right.alias`.`join.key`
             FROM `join.left` AS `left.alias`
             JOIN `join.right` AS `right.alias` USING (`join.key`)",
        )
        .await
        .unwrap();
    assert_eq!(qualified, [(1, 1)]);

    let natural: Vec<(i64, String, String)> = c
        .query(
            "SELECT *
             FROM `join.left` AS `left.alias`
             NATURAL JOIN `join.right` AS `right.alias`",
        )
        .await
        .unwrap();
    assert_eq!(natural, [(1, "L1".into(), "R1".into())]);
}

#[tokio::test]
async fn using_join_visibility_survives_later_on_and_cross_joins() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE mix_a (id INT PRIMARY KEY, aval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE mix_b (id INT PRIMARY KEY, bval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE mix_c (id INT PRIMARY KEY, cval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE mix_d (id INT PRIMARY KEY, dval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO mix_a VALUES (1,'A1'),(2,'A2')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO mix_b VALUES (1,'B1'),(2,'B2')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO mix_c VALUES (1,'C1'),(3,'C3')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO mix_d VALUES (1,'D1'),(2,'D2'),(3,'D3')")
        .await
        .unwrap();

    let mut result = c
        .query_iter(
            "SELECT *
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             JOIN mix_c AS c ON c.id = a.id",
        )
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "aval", "bval", "id", "cval"]);
    let rows: Vec<(i64, String, String, i64, String)> = result.collect().await.unwrap();
    assert_eq!(rows, [(1, "A1".into(), "B1".into(), 1, "C1".into())]);

    let rows: Vec<(i64, i64, i64)> = c
        .query(
            "SELECT a.id, b.id, c.id
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             JOIN mix_c AS c ON c.id = a.id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [(1, 1, 1)]);

    let err = c
        .query_drop(
            "SELECT id
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             JOIN mix_c AS c ON c.id = a.id",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().to_ascii_lowercase().contains("ambiguous"));
    let err = c
        .query_drop(
            "SELECT *
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             JOIN mix_c AS c ON c.id = a.id
             JOIN mix_d AS d USING(id)",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().to_ascii_lowercase().contains("ambiguous"));

    let mut result = c
        .query_iter(
            "SELECT *
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             CROSS JOIN mix_c AS c
             WHERE a.id = 1 AND c.id = 3",
        )
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "aval", "bval", "id", "cval"]);
    let rows: Vec<(i64, String, String, i64, String)> = result.collect().await.unwrap();
    assert_eq!(rows, [(1, "A1".into(), "B1".into(), 3, "C3".into())]);
    let rows: Vec<(i64, i64, i64)> = c
        .query(
            "SELECT a.id, b.id, c.id
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             CROSS JOIN mix_c AS c
             WHERE a.id = 1 AND c.id = 3",
        )
        .await
        .unwrap();
    assert_eq!(rows, [(1, 1, 3)]);
    let err = c
        .query_drop(
            "SELECT id
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             CROSS JOIN mix_c AS c",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().to_ascii_lowercase().contains("ambiguous"));
    let err = c
        .query_drop(
            "SELECT *
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             CROSS JOIN mix_c AS c
             JOIN mix_d AS d USING(id)",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().to_ascii_lowercase().contains("ambiguous"));

    let mut result = c
        .query_iter(
            "SELECT *
             FROM mix_a AS a
             JOIN mix_b AS b USING(id)
             JOIN mix_c AS c ON c.id = a.id
             GROUP BY a.id, c.id",
        )
        .await
        .unwrap();
    let names: Vec<String> = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect();
    assert_eq!(names, ["id", "aval", "bval", "id", "cval"]);
    let rows: Vec<(i64, String, String, i64, String)> = result.collect().await.unwrap();
    assert_eq!(rows, [(1, "A1".into(), "B1".into(), 1, "C1".into())]);
}

#[tokio::test]
async fn native_prepared_using_wildcard_metadata_matches_runtime_shape() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE prep_using_l (id INT PRIMARY KEY, lval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE prep_using_r (id INT PRIMARY KEY, rval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO prep_using_l VALUES (1,'L1')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO prep_using_r VALUES (1,'R1')")
        .await
        .unwrap();

    // Run this test with ELYRASQL_STMT_DESCRIBE=1 to exercise static PREPARE
    // metadata. The execute-time result has one coalesced key, not both physical
    // key columns.
    let mut result = c
        .exec_iter(
            "SELECT *
             FROM prep_using_l AS l JOIN prep_using_r AS r USING(id)",
            (),
        )
        .await
        .unwrap();
    let columns = result.columns_ref();
    let names = columns
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>();
    let tables = columns
        .iter()
        .map(|column| column.table_str().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, ["id", "lval", "rval"]);
    assert_eq!(tables, ["l", "l", "r"]);
    let rows: Vec<(i64, String, String)> = result.collect().await.unwrap();
    assert_eq!(rows, [(1, "L1".into(), "R1".into())]);
}

#[tokio::test]
async fn using_join_fast_paths_validate_bare_on_references_for_ambiguity() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE on_using_a (id INT PRIMARY KEY, label VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE on_using_b (id INT PRIMARY KEY, label VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE on_using_c (id INT PRIMARY KEY, label VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO on_using_a VALUES (1,'same')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO on_using_b VALUES (1,'same')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO on_using_c VALUES (1,'same')")
        .await
        .unwrap();

    let error = c
        .query_drop(
            "SELECT *
             FROM on_using_a AS a
             NATURAL JOIN on_using_b AS b
             JOIN on_using_c AS c ON label = c.label",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().to_ascii_lowercase().contains("ambiguous"),
        "{error:?}"
    );

    let rows: Vec<(String, String)> = c
        .query(
            "SELECT a.label, c.label
             FROM on_using_a AS a
             NATURAL JOIN on_using_b AS b
             JOIN on_using_c AS c ON a.label = c.label",
        )
        .await
        .unwrap();
    assert_eq!(rows, [("same".into(), "same".into())]);

    c.query_drop(
        "CREATE TABLE on_using_indexed (
             label VARCHAR(8) PRIMARY KEY,
             payload VARCHAR(8)
         )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO on_using_indexed VALUES ('same','indexed')")
        .await
        .unwrap();
    let error = c
        .query_drop(
            "SELECT *
             FROM on_using_a AS a
             NATURAL JOIN on_using_b AS b
             JOIN on_using_indexed AS i ON label = i.label",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().to_ascii_lowercase().contains("ambiguous"),
        "indexed NLJ must reject the same ambiguous ON reference: {error:?}"
    );
    let rows: Vec<(String, String)> = c
        .query(
            "SELECT a.label, i.payload
             FROM on_using_a AS a
             NATURAL JOIN on_using_b AS b
             JOIN on_using_indexed AS i ON a.label = i.label",
        )
        .await
        .unwrap();
    assert_eq!(rows, [("same".into(), "indexed".into())]);

    let count: i64 = c
        .query_first(
            "SELECT COUNT(*)
             FROM on_using_a AS a
             JOIN on_using_indexed AS i ON CURRENT_DATE IS NOT NULL",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn natural_join_bare_coalesced_key_correlates_as_one_outer_column() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE corr_natural_l (
             id INT PRIMARY KEY,
             label VARCHAR(8) COLLATE utf8mb4_general_ci
         )",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE corr_natural_r (
             id INT PRIMARY KEY,
             label VARCHAR(8) COLLATE utf8mb4_bin
         )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO corr_natural_l VALUES (1,'same')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO corr_natural_r VALUES (1,'same')")
        .await
        .unwrap();

    let rows: Vec<(String, String)> = c
        .query(
            "SELECT label, (SELECT label)
             FROM corr_natural_l AS l NATURAL JOIN corr_natural_r AS r",
        )
        .await
        .unwrap();
    assert_eq!(rows, [("same".into(), "same".into())]);

    c.query_drop("CREATE TABLE corr_natural_c (id INT PRIMARY KEY, label VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO corr_natural_c VALUES (1,'same')")
        .await
        .unwrap();
    let error = c
        .query_drop(
            "SELECT (SELECT label)
             FROM corr_natural_l AS l
             NATURAL JOIN corr_natural_r AS r
             JOIN corr_natural_c AS c ON l.id = c.id",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().to_ascii_lowercase().contains("ambiguous"),
        "{error:?}"
    );
}

#[tokio::test]
async fn natural_join_bare_coalesced_key_correlates_in_filters() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE corr_filter_l (id INT PRIMARY KEY, label VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE corr_filter_r (id INT PRIMARY KEY, label VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO corr_filter_l VALUES (1,'same'),(2,'other')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO corr_filter_r VALUES (1,'same'),(2,'other')")
        .await
        .unwrap();

    for sql in [
        "SELECT id FROM corr_filter_l AS l NATURAL JOIN corr_filter_r AS r
         WHERE (SELECT label) = 'same'",
        "SELECT id FROM corr_filter_l AS l NATURAL JOIN corr_filter_r AS r
         WHERE EXISTS (SELECT 1 WHERE label = 'same')",
        "SELECT id FROM corr_filter_l AS l NATURAL JOIN corr_filter_r AS r
         WHERE 1 IN (SELECT 1 WHERE label = 'same')",
    ] {
        let rows: Vec<i64> = c
            .query(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(rows, [1], "{sql}");
    }

    c.query_drop("CREATE TABLE corr_filter_local (marker INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO corr_filter_local VALUES (1)")
        .await
        .unwrap();
    let rows: Vec<i64> = c
        .query(
            "SELECT id
             FROM corr_filter_l AS l NATURAL JOIN corr_filter_r AS r
             WHERE EXISTS (
                 SELECT 1 FROM corr_filter_local WHERE marker = 1
             )
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [1, 2]);

    c.query_drop("CREATE TABLE corr_filter_c (id INT PRIMARY KEY, label VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO corr_filter_c VALUES (1,'same')")
        .await
        .unwrap();
    let error = c
        .query_drop(
            "SELECT *
             FROM corr_filter_l AS l
             NATURAL JOIN corr_filter_r AS r
             JOIN corr_filter_c AS c ON l.id = c.id
             WHERE EXISTS (SELECT 1 WHERE label = 'same')",
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().to_ascii_lowercase().contains("ambiguous"),
        "{error:?}"
    );
}

#[tokio::test]
async fn quoted_dotted_coalesced_keys_correlate_without_capturing_qualified_refs() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE corr_dot_l (
             `dot.key` VARCHAR(8) PRIMARY KEY,
             left_value VARCHAR(8)
         )",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE corr_dot_r (
             `dot.key` VARCHAR(8) PRIMARY KEY,
             right_value VARCHAR(8)
         )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO corr_dot_l VALUES ('same','left'),('other','left2')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO corr_dot_r VALUES ('same','right'),('other','right2')")
        .await
        .unwrap();

    let rows: Vec<(String, String)> = c
        .query(
            "SELECT `dot.key`, (SELECT `dot.key`)
             FROM corr_dot_l AS l NATURAL JOIN corr_dot_r AS r
             ORDER BY `dot.key`",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        [
            ("other".into(), "other".into()),
            ("same".into(), "same".into()),
        ]
    );

    let rows: Vec<String> = c
        .query(
            "SELECT `dot.key`
             FROM corr_dot_l AS l NATURAL JOIN corr_dot_r AS r
             WHERE EXISTS (SELECT 1 WHERE `dot.key` = 'same')",
        )
        .await
        .unwrap();
    assert_eq!(rows, ["same"]);

    c.query_drop(
        "SELECT (SELECT dot.key)
             FROM corr_dot_l AS l NATURAL JOIN corr_dot_r AS r",
    )
    .await
    .unwrap_err();
}

#[tokio::test]
async fn multi_key_using_preserves_sql_coercion_and_collation_semantics() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE typed_l (id INT, code INT, lval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE typed_r (id VARCHAR(8), code INT, rval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO typed_l VALUES (5,7,'L5'),(NULL,8,'LN')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO typed_r VALUES ('5',7,'R5'),(NULL,8,'RN')")
        .await
        .unwrap();
    let rows: Vec<(i64, String, String, String)> = c
        .query(
            "SELECT l.id, r.id, l.lval, r.rval
             FROM typed_l AS l JOIN typed_r AS r USING(id, code)",
        )
        .await
        .unwrap();
    assert_eq!(rows, [(5, "5".into(), "L5".into(), "R5".into())]);

    c.query_drop("CREATE TABLE decimal_l (bucket INT, amount DECIMAL(10,2))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE decimal_r (bucket INT, amount INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO decimal_l VALUES (1,5.00),(2,NULL)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO decimal_r VALUES (1,5),(2,NULL)")
        .await
        .unwrap();
    let count: Option<i64> = c
        .query_first(
            "SELECT COUNT(*)
             FROM decimal_l AS l JOIN decimal_r AS r USING(bucket, amount)",
        )
        .await
        .unwrap();
    assert_eq!(count, Some(1));

    c.query_drop("CREATE TABLE null_l (id INT, code INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE null_r (id INT, code INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO null_l VALUES (NULL,1)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO null_r VALUES (NULL,1)")
        .await
        .unwrap();
    let count: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM null_l JOIN null_r USING(id, code)")
        .await
        .unwrap();
    assert_eq!(count, Some(0));

    c.query_drop(
        "CREATE TABLE coll_l (
             grp INT,
             label VARCHAR(8) COLLATE utf8mb4_general_ci
         )",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE coll_r (
             grp INT,
             label VARCHAR(8) COLLATE utf8mb4_bin
         )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO coll_l VALUES (1,'X'),(2,'x')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO coll_r VALUES (1,'X'),(2,'X'),(3,'x')")
        .await
        .unwrap();
    let rows: Vec<(i64, String, String)> = c
        .query(
            "SELECT l.grp, l.label, r.label
             FROM coll_l AS l JOIN coll_r AS r USING(grp, label)",
        )
        .await
        .unwrap();
    assert_eq!(rows, [(1, "X".into(), "X".into())]);

    let mut result = c
        .query_iter(
            "SELECT label, COUNT(*) AS n
             FROM coll_l AS l RIGHT JOIN coll_r AS r USING(grp, label)
             GROUP BY label
             ORDER BY label",
        )
        .await
        .unwrap();
    let columns = result.columns_ref();
    assert_eq!(columns[0].table_str(), "r");
    assert_eq!(columns[1].table_str(), "");
    let rows: Vec<(String, i64)> = result.collect().await.unwrap();
    assert_eq!(rows, [("X".into(), 2), ("x".into(), 1)]);

    let rows: Vec<String> = c
        .query(
            "SELECT DISTINCT label
             FROM coll_l AS l RIGHT JOIN coll_r AS r USING(grp, label)
             GROUP BY label
             ORDER BY label DESC",
        )
        .await
        .unwrap();
    assert_eq!(rows, ["x", "X"]);
}

#[tokio::test]
async fn coercive_composite_using_and_natural_joins_keep_binary_collation() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE coercive_coll_l (
             id INT,
             label VARCHAR(8) COLLATE utf8mb4_general_ci,
             lval VARCHAR(8)
         )",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE coercive_coll_r (
             id VARCHAR(8),
             label VARCHAR(8) COLLATE utf8mb4_bin,
             rval VARCHAR(8)
         )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO coercive_coll_l VALUES (5,'x','lower'),(6,'X','exact')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO coercive_coll_r VALUES ('5','X','wrong'),('6','X','right')")
        .await
        .unwrap();

    let using_rows: Vec<(i64, String, String, String)> = c
        .query(
            "SELECT l.id, l.label, l.lval, r.rval
             FROM coercive_coll_l AS l
             JOIN coercive_coll_r AS r USING(id, label)
             ORDER BY l.id",
        )
        .await
        .unwrap();
    assert_eq!(
        using_rows,
        [(6, "X".into(), "exact".into(), "right".into())]
    );

    let natural_rows: Vec<(i64, String, String, String)> = c
        .query(
            "SELECT l.id, l.label, l.lval, r.rval
             FROM coercive_coll_l AS l
             NATURAL JOIN coercive_coll_r AS r
             ORDER BY l.id",
        )
        .await
        .unwrap();
    assert_eq!(natural_rows, using_rows);
}

#[tokio::test]
async fn chained_using_and_natural_fallbacks_resolve_keys_per_side() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE chain_int_a (id INT, aval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE chain_int_b (id INT, bval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE chain_text_c (id VARCHAR(8), cval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO chain_int_a VALUES (5,'A5'),(6,'A6')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO chain_int_b VALUES (5,'B5'),(6,'B6')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO chain_text_c VALUES ('5','C5'),('7','C7')")
        .await
        .unwrap();

    let using_rows: Vec<(i64, String, String, String)> = c
        .query(
            "SELECT id, a.aval, b.bval, c.cval
             FROM chain_int_a AS a
             JOIN chain_int_b AS b USING(id)
             JOIN chain_text_c AS c USING(id)",
        )
        .await
        .unwrap();
    assert_eq!(using_rows, [(5, "A5".into(), "B5".into(), "C5".into())]);

    let natural_rows: Vec<(i64, String, String, String)> = c
        .query(
            "SELECT id, a.aval, b.bval, c.cval
             FROM chain_int_a AS a
             NATURAL JOIN chain_int_b AS b
             NATURAL JOIN chain_text_c AS c",
        )
        .await
        .unwrap();
    assert_eq!(natural_rows, using_rows);

    c.query_drop("CREATE TABLE chain_float_a (id DOUBLE, aval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE chain_float_b (id DOUBLE, bval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE chain_float_c (id DOUBLE, cval VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO chain_float_a VALUES (1.5,'A1'),(2.5,'A2')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO chain_float_b VALUES (1.5,'B1'),(2.5,'B2')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO chain_float_c VALUES (1.5,'C1'),(3.5,'C3')")
        .await
        .unwrap();

    let using_float_rows: Vec<(f64, String, String, String)> = c
        .query(
            "SELECT id, a.aval, b.bval, c.cval
             FROM chain_float_a AS a
             JOIN chain_float_b AS b USING(id)
             JOIN chain_float_c AS c USING(id)",
        )
        .await
        .unwrap();
    assert_eq!(
        using_float_rows,
        [(1.5, "A1".into(), "B1".into(), "C1".into())]
    );

    let natural_float_rows: Vec<(f64, String, String, String)> = c
        .query(
            "SELECT id, a.aval, b.bval, c.cval
             FROM chain_float_a AS a
             NATURAL JOIN chain_float_b AS b
             NATURAL JOIN chain_float_c AS c",
        )
        .await
        .unwrap();
    assert_eq!(natural_float_rows, using_float_rows);
}

#[tokio::test]
async fn nested_derived_joins_preserve_binary_collation() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE nested_coll_source (
             label VARCHAR(8) COLLATE utf8mb4_bin,
             lval VARCHAR(8)
         )",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE nested_coll_partner (
             label VARCHAR(8) COLLATE utf8mb4_general_ci,
             rval VARCHAR(8)
         )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO nested_coll_source VALUES ('x','wrong'),('X','exact')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO nested_coll_partner VALUES ('X','right')")
        .await
        .unwrap();

    let using_rows: Vec<(String, String, String)> = c
        .query(
            "SELECT d.label, d.lval, p.rval
             FROM (
                 SELECT inner_d.label, inner_d.lval
                 FROM (
                     SELECT label, lval FROM nested_coll_source
                 ) AS inner_d
             ) AS d
             JOIN nested_coll_partner AS p USING(label)",
        )
        .await
        .unwrap();
    assert_eq!(using_rows, [("X".into(), "exact".into(), "right".into())]);

    let natural_rows: Vec<(String, String, String)> = c
        .query(
            "SELECT d.label, d.lval, p.rval
             FROM (
                 SELECT inner_d.label, inner_d.lval
                 FROM (
                     SELECT label, lval FROM nested_coll_source
                 ) AS inner_d
             ) AS d
             NATURAL JOIN nested_coll_partner AS p",
        )
        .await
        .unwrap();
    assert_eq!(natural_rows, using_rows);
}

#[tokio::test]
async fn correlated_exists_preserves_inner_bare_columns() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE lists (id INT PRIMARY KEY, owner_id INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE levels (id INT PRIMARY KEY, list_id INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO lists VALUES (10, 1), (20, 2)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO levels VALUES (20, 10), (30, 20)")
        .await
        .unwrap();

    let rows: Vec<i64> = c
        .query(
            "SELECT id FROM lists WHERE owner_id = 1 AND EXISTS (\
             SELECT * FROM levels WHERE lists.id = levels.list_id AND id = 20)",
        )
        .await
        .unwrap();
    assert_eq!(rows, [10]);
}

#[tokio::test]
async fn nested_correlation_resolves_missing_inner_columns_from_outer_scope() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE contacts (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE items (\
         id INT PRIMARY KEY, contact_id INT, kind VARCHAR(8), target_id INT)",
    )
    .await
    .unwrap();
    c.query_drop("CREATE TABLE targets (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO contacts VALUES (1), (2)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO items VALUES (10, 1, 'match', 100), (20, 2, 'other', 200)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO targets VALUES (100), (200)")
        .await
        .unwrap();

    let rows: Vec<i64> = c
        .query(
            "SELECT id FROM contacts WHERE EXISTS (\
             SELECT * FROM items WHERE contacts.id = items.contact_id AND EXISTS (\
             SELECT * FROM targets WHERE items.target_id = targets.id \
             AND kind = 'match' AND target_id = 100))",
        )
        .await
        .unwrap();
    assert_eq!(rows, [1]);
}

#[tokio::test]
async fn correlated_join_preserves_shadowed_inner_qualifiers() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE entries (id INT PRIMARY KEY, category_id INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE links (id INT PRIMARY KEY, entry_id INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO entries VALUES (10, 7), (20, 7), (30, 8)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO links VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    let rows: Vec<i64> = c
        .query(
            "SELECT links.id FROM links \
             LEFT JOIN entries ON entries.id = links.entry_id \
             WHERE EXISTS (SELECT * FROM entries \
             WHERE links.entry_id = entries.id AND category_id = 7) \
             ORDER BY links.id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [1, 2]);
}

#[tokio::test]
async fn correlated_null_equality_does_not_enter_index_key_encoding() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE parents (id INT PRIMARY KEY, child_id INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE children (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO parents VALUES (1, NULL), (2, 20)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO children VALUES (20)")
        .await
        .unwrap();

    let rows: Vec<i64> = c
        .query(
            "SELECT id FROM parents WHERE EXISTS (\
             SELECT * FROM children WHERE parents.child_id = children.id)",
        )
        .await
        .unwrap();
    assert_eq!(rows, [2]);
}

#[tokio::test]
async fn correlated_scalar_subquery_alias_can_order_rows() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE records (id INT PRIMARY KEY, active INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE record_logs (id INT PRIMARY KEY, record_id INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO records VALUES (1, 1), (2, 1), (3, 0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO record_logs VALUES (10, 1), (11, 1), (12, 2)")
        .await
        .unwrap();

    let rows: Vec<(i64, i64)> = c
        .query(
            "SELECT records.id, (SELECT COUNT(*) FROM record_logs \
             WHERE record_logs.record_id = records.id) AS log_count \
             FROM records WHERE active = 1 ORDER BY log_count, records.id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [(2, 1), (1, 2)]);
}

#[tokio::test]
async fn prepared_correlated_projection_preserves_unsigned_metadata() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE count_parents (id BIGINT UNSIGNED PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE count_children (
            id BIGINT UNSIGNED PRIMARY KEY,
            parent_id BIGINT UNSIGNED,
            deleted_at DATETIME NULL
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO count_parents VALUES (396208)")
        .await
        .unwrap();

    let row: Option<(u64, i64)> = c
        .exec_first(
            "SELECT id,
                    (SELECT COUNT(*)
                     FROM count_children
                     WHERE count_parents.id = count_children.parent_id
                       AND count_children.deleted_at IS NULL) AS child_count
             FROM count_parents
             WHERE count_parents.id IN (?)",
            (396208_u64,),
        )
        .await
        .unwrap();
    assert_eq!(row, Some((396208, 0)));
}

#[tokio::test]
async fn correlated_filter_can_feed_join_aggregation() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE aggregate_parents (
            id INT PRIMARY KEY,
            scope_id INT,
            deleted_at DATETIME NULL
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE aggregate_records (
            id INT PRIMARY KEY,
            parent_id INT,
            item_id INT,
            deleted_at DATETIME NULL
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE aggregate_items (
            id INT PRIMARY KEY,
            name VARCHAR(16)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO aggregate_parents VALUES (1, 7, NULL), (2, 8, NULL)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO aggregate_items VALUES (10, 'Zebra'), (20, 'Apple')")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO aggregate_records VALUES
         (100, 1, 10, NULL), (101, 1, 20, NULL), (102, 2, 10, NULL)",
    )
    .await
    .unwrap();

    let count: Option<i64> = c
        .exec_first(
            "SELECT COUNT(*) AS aggregate_count
             FROM aggregate_records
             INNER JOIN aggregate_items
                 ON aggregate_items.id = aggregate_records.item_id
             WHERE EXISTS (
                 SELECT * FROM aggregate_parents
                 WHERE aggregate_records.parent_id = aggregate_parents.id
                   AND scope_id = ?
                   AND aggregate_parents.deleted_at IS NULL
             )
             AND aggregate_records.deleted_at IS NULL",
            (7,),
        )
        .await
        .unwrap();
    assert_eq!(count, Some(2));

    let grouped_sql = "SELECT aggregate_records.item_id,
                aggregate_items.name AS item_name,
                COUNT(*) AS record_count
         FROM aggregate_records
         INNER JOIN aggregate_items
             ON aggregate_items.id = aggregate_records.item_id
         WHERE EXISTS (
             SELECT * FROM aggregate_parents
             WHERE aggregate_records.parent_id = aggregate_parents.id
               AND scope_id = ?
               AND aggregate_parents.deleted_at IS NULL
         )
         AND aggregate_records.deleted_at IS NULL
         GROUP BY aggregate_records.item_id, aggregate_items.name
         ORDER BY aggregate_items.name";
    let mut result = c.exec_iter(grouped_sql, (7,)).await.unwrap();
    let columns = result.columns().unwrap();
    let names = columns
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, ["item_id", "item_name", "record_count"]);
    let grouped: Vec<(i64, String, i64)> = result.collect().await.unwrap();
    assert_eq!(grouped, [(20, "Apple".into(), 1), (10, "Zebra".into(), 1)]);
}

#[tokio::test]
async fn joined_correlated_projection_expands_qualified_wildcard() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE parents (id INT PRIMARY KEY, label VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE links (parent_id INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE children (id INT PRIMARY KEY, parent_id INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO parents VALUES (1, 'one')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO links VALUES (1)").await.unwrap();
    c.query_drop("INSERT INTO children VALUES (10, 1), (11, 1)")
        .await
        .unwrap();

    let rows: Vec<(i64, String, i64, i64)> = c
        .query(
            "SELECT parents.*, (SELECT COUNT(*) FROM children \
             WHERE children.parent_id = parents.id) AS child_count, \
             links.parent_id AS pivot_id FROM parents \
             INNER JOIN links ON parents.id = links.parent_id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [(1, "one".into(), 2, 1)]);
}

#[tokio::test]
async fn non_strict_sql_mode_coerces_invalid_integer_text() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE mode_values (
            id INT PRIMARY KEY,
            value INT,
            unsigned_value BIGINT UNSIGNED
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci', \
         SESSION sql_mode='NO_ENGINE_SUBSTITUTION'",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO mode_values VALUES (1, '{\"name\":\"row\"}', '')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO mode_values VALUES (2, '123.5tail', '456.5tail')")
        .await
        .unwrap();
    c.exec_drop(
        "INSERT INTO mode_values VALUES (?, ?, ?)",
        (3, "112.5", "137.5"),
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO mode_values VALUES (4, -112.5, -112.5)")
        .await
        .unwrap();
    let values: Vec<(i64, u64)> = c
        .query("SELECT value, unsigned_value FROM mode_values ORDER BY id")
        .await
        .unwrap();
    assert_eq!(values, [(0, 0), (124, 457), (113, 138), (-113, 0)]);

    let count: i64 = c
        .query_first("SELECT COUNT(*) FROM mode_values WHERE value = 'Channel1'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 1);

    c.query_drop("SET SESSION sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
        .await
        .unwrap();
    assert!(c
        .query_drop("INSERT INTO mode_values VALUES (5, 'invalid', 'invalid')")
        .await
        .is_err());
    assert!(c
        .query_drop("INSERT INTO mode_values VALUES (6, 1, '')")
        .await
        .is_err());
}

#[tokio::test]
async fn non_strict_sql_mode_coerces_invalid_temporal_text() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE temporal_values (id INT PRIMARY KEY, d DATE, dt DATETIME, tm TIME)")
        .await
        .unwrap();
    c.query_drop("SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'")
        .await
        .unwrap();
    c.query_drop("INSERT INTO temporal_values VALUES (1, '', '', '')")
        .await
        .unwrap();

    let row: (String, String, String) = c
        .query_first("SELECT d, dt, tm FROM temporal_values WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row,
        (
            "0000-00-00".into(),
            "0000-00-00 00:00:00".into(),
            "00:00:00".into(),
        )
    );

    c.query_drop("SET SESSION sql_mode='STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
        .await
        .unwrap();
    assert!(c
        .query_drop("INSERT INTO temporal_values VALUES (2, '', '', '')")
        .await
        .is_err());
}

/// Join followed by GROUP BY over an indexed partner -- exercises the streaming
/// index nested-loop aggregation path (bounded memory) and must produce exactly
/// the same result as the materialising join.
#[tokio::test]
async fn join_group_by_streaming() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE dim (id INT PRIMARY KEY, category VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE facts (id INT PRIMARY KEY, dim_id INT, amount INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO dim VALUES (1,'A'),(2,'B'),(3,'A')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO facts VALUES (1,1,10),(2,1,20),(3,2,5),(4,3,7),(5,2,15),(6,1,3)")
        .await
        .unwrap();

    // category A = dim {1,3}: facts 1,2,6 (10,20,3) + fact 4 (7) => count 4, sum 40
    // category B = dim {2}:   facts 3,5 (5,15)               => count 2, sum 20
    let mut rows: Vec<(String, i64, i64)> = c
        .query(
            "SELECT d.category, COUNT(*), SUM(f.amount) \
             FROM facts f JOIN dim d ON f.dim_id = d.id \
             GROUP BY d.category",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows, vec![("A".into(), 4, 40), ("B".into(), 2, 20)]);

    // WHERE (pushed through the join) + GROUP BY
    let mut rows: Vec<(String, i64, i64)> = c
        .query(
            "SELECT d.category, COUNT(*), SUM(f.amount) \
             FROM facts f JOIN dim d ON f.dim_id = d.id \
             WHERE f.amount >= 10 GROUP BY d.category",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(rows, vec![("A".into(), 2, 30), ("B".into(), 1, 15)]);

    // HAVING + ORDER BY over the grouped output
    let rows: Vec<(String, i64)> = c
        .query(
            "SELECT d.category, COUNT(*) c \
             FROM facts f JOIN dim d ON f.dim_id = d.id \
             GROUP BY d.category HAVING COUNT(*) > 2 ORDER BY d.category",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![("A".into(), 4)]);
}

/// INNER comma-joins (`FROM a, b WHERE a.k = b.k`) are normalized to a JOIN
/// chain, so they stream (ORDER BY / GROUP BY) like explicit joins. [ESQL-6]
#[tokio::test]
async fn comma_join_streaming() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE ord (id INT PRIMARY KEY, cust INT, amt INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE cust (id INT PRIMARY KEY, name VARCHAR(8), reg_id INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE reg (id INT PRIMARY KEY, r VARCHAR(4))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO reg VALUES (10,'N'),(20,'S')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO cust VALUES (1,'A',10),(2,'B',20)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO ord VALUES (1,1,100),(2,2,50),(3,1,80)")
        .await
        .unwrap();

    // comma join + ORDER BY
    let rows: Vec<(i64, String, i64)> = c
        .query("SELECT ord.id, cust.name, ord.amt FROM ord, cust WHERE ord.cust = cust.id ORDER BY ord.amt DESC")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, "A".into(), 100),
            (3, "A".into(), 80),
            (2, "B".into(), 50)
        ]
    );

    // comma join + GROUP BY
    let mut g: Vec<(String, i64)> = c
        .query("SELECT cust.name, SUM(ord.amt) FROM ord, cust WHERE ord.cust = cust.id GROUP BY cust.name")
        .await
        .unwrap();
    g.sort();
    assert_eq!(g, vec![("A".into(), 180), ("B".into(), 50)]);

    // three-table comma join
    let rows: Vec<(i64, String)> = c
        .query("SELECT ord.id, reg.r FROM ord, cust, reg WHERE ord.cust = cust.id AND cust.reg_id = reg.id ORDER BY ord.id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, "N".into()), (2, "S".into()), (3, "N".into())]
    );
}

/// Three-table (left-deep) join streams for both ORDER BY and GROUP BY via the
/// chained hash-join. [ESQL-6]
#[tokio::test]
async fn three_table_join_streaming() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE fct (id INT PRIMARY KEY, d1 INT, d2 INT, amt INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE dm1 (id INT PRIMARY KEY, name VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE dm2 (id INT PRIMARY KEY, region VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO dm1 VALUES (1,'A'),(2,'B')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO dm2 VALUES (10,'N'),(20,'S')")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO fct VALUES (1,1,10,100),(2,2,20,50),(3,1,20,80),(4,2,10,120),(5,1,10,30)",
    )
    .await
    .unwrap();

    // GROUP BY over the 3-table join
    let mut g: Vec<(String, String, i64, i64)> = c
        .query(
            "SELECT d1.name, d2.region, COUNT(*), SUM(f.amt) \
             FROM fct f JOIN dm1 d1 ON f.d1 = d1.id JOIN dm2 d2 ON f.d2 = d2.id \
             GROUP BY d1.name, d2.region",
        )
        .await
        .unwrap();
    g.sort();
    assert_eq!(
        g,
        vec![
            ("A".into(), "N".into(), 2, 130),
            ("A".into(), "S".into(), 1, 80),
            ("B".into(), "N".into(), 1, 120),
            ("B".into(), "S".into(), 1, 50),
        ]
    );

    // ORDER BY over the 3-table join
    let o: Vec<(i64, String, String)> = c
        .query(
            "SELECT f.id, d1.name, d2.region \
             FROM fct f JOIN dm1 d1 ON f.d1 = d1.id JOIN dm2 d2 ON f.d2 = d2.id \
             ORDER BY f.amt DESC LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(
        o,
        vec![(4, "B".into(), "N".into()), (1, "A".into(), "N".into())]
    );
}

/// Join + ORDER BY + LIMIT: the streaming hash-join feeds the spilling sorter,
/// so the result matches the materialising path (top-N by amount). [ESQL-6]
#[tokio::test]
async fn join_order_by_streaming() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE so_dim (id INT PRIMARY KEY, cat VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE so_facts (id INT PRIMARY KEY, dim_id INT, amt INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO so_dim VALUES (1,'A'),(2,'B'),(3,'C')")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO so_facts VALUES (1,1,50),(2,2,90),(3,3,10),(4,1,70),(5,2,90),(6,3,30)",
    )
    .await
    .unwrap();

    // top 3 by amt desc, id asc as tiebreak
    let rows: Vec<(i64, String, i64)> = c
        .query(
            "SELECT f.id, d.cat, f.amt FROM so_facts f JOIN so_dim d ON f.dim_id = d.id \
             ORDER BY f.amt DESC, f.id ASC LIMIT 3",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (2, "B".into(), 90),
            (5, "B".into(), 90),
            (4, "A".into(), 70)
        ]
    );

    // LEFT join: an unmatched driving row appears with NULL partner, ordered
    let rows: Vec<(i64, Option<String>)> = c
        .query(
            "SELECT f.id, d.cat FROM so_facts f LEFT JOIN so_dim d ON f.dim_id = d.id \
             WHERE f.id IN (3, 6) ORDER BY f.id",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![(3, Some("C".into())), (6, Some("C".into()))]);

    // MySQL resolves an unqualified ORDER BY name against the projected output
    // before the joined input. The qualified wildcard exposes only the driving
    // table's `id`, so the partner's `id` does not make this ambiguous.
    let rows: Vec<(i64, i64, i64, String)> = c
        .query(
            "SELECT f.*, d.cat AS through_key
             FROM so_facts f JOIN so_dim d ON f.dim_id = d.id
             ORDER BY id DESC LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![(6, 3, 30, "C".into()), (5, 2, 90, "B".into())]);

    let error = c
        .query_drop(
            "SELECT * FROM so_facts f JOIN so_dim d ON f.dim_id = d.id
             ORDER BY id LIMIT 1",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ambiguous column: id"));
}

/// ESQL-50: the streaming join builds each combined row in a reusable buffer,
/// and for a *wide* partner whose columns the query mostly ignores it writes only
/// the read positions, leaving the rest at the NULL laid down once per driving
/// row. That makes two things silently breakable: a value from one partner row
/// surviving into the next combination, and a pair rejected by a residual `ON`
/// leaving its values behind. Both are asserted here on values.
///
/// The partner is deliberately 14 columns wide, because the selective copy is
/// only chosen when it beats copying the whole partner half.
#[tokio::test]
async fn wide_partner_rows_do_not_leak_between_combinations() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    let pad = (0..10)
        .map(|i| format!("p{i} VARCHAR(16)"))
        .collect::<Vec<_>>()
        .join(", ");
    c.query_drop(format!(
        "CREATE TABLE wl (k INT PRIMARY KEY, g INT, v INT, s VARCHAR(16), {pad})"
    ))
    .await
    .unwrap();
    c.query_drop(format!(
        "CREATE TABLE wr (k INT PRIMARY KEY, g INT, v INT, s VARCHAR(16), {pad})"
    ))
    .await
    .unwrap();
    // Two partner rows per key so a leak between combinations is observable, and
    // one row with a NULL key, which must match nothing.
    for i in 1..=6 {
        let p = (0..10)
            .map(|j| format!("'l{i}-{j}'"))
            .collect::<Vec<_>>()
            .join(",");
        c.query_drop(format!(
            "INSERT INTO wl VALUES ({i}, {}, {i}, 'l{i}', {p})",
            i % 3
        ))
        .await
        .unwrap();
        let p = (0..10)
            .map(|j| format!("'r{i}-{j}'"))
            .collect::<Vec<_>>()
            .join(",");
        c.query_drop(format!(
            "INSERT INTO wr VALUES ({i}, {}, {}, 'r{i}', {p})",
            i % 3,
            i * 10
        ))
        .await
        .unwrap();
    }
    c.query_drop(
        "INSERT INTO wr VALUES (99, NULL, 0, 'null-key', 'x','x','x','x','x','x','x','x','x','x')",
    )
    .await
    .unwrap();

    // 6 left rows x 2 partners per g-group (g = 1,2,0 -> two rows each).
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM wl JOIN wr ON wl.g = wr.g")
        .await
        .unwrap();
    assert_eq!(got, Some(12), "NULL-keyed partner row must match nothing");

    // Exactly one partner column is read, so this takes the selective copy. Each
    // combination must see *its own* partner value.
    let rows: Vec<(i64, i64)> = c
        .query("SELECT wl.k, wr.v FROM wl JOIN wr ON wl.g = wr.g ORDER BY wl.k, wr.v")
        .await
        .unwrap();
    let mut expect: Vec<(i64, i64)> = Vec::new();
    for l in 1..=6i64 {
        for r in 1..=6i64 {
            if l % 3 == r % 3 {
                expect.push((l, r * 10));
            }
        }
    }
    expect.sort();
    assert_eq!(rows, expect, "partner value leaked across combinations");

    // A residual ON rejects some pairs; a LEFT join must then NULL-extend rather
    // than show the rejected pair's values.
    let rows: Vec<(i64, Option<i64>)> = c
        .query(
            "SELECT wl.k, wr.v FROM wl LEFT JOIN wr ON wl.g = wr.g AND wr.v > 40 \
             ORDER BY wl.k, wr.v",
        )
        .await
        .unwrap();
    let mut expect: Vec<(i64, Option<i64>)> = Vec::new();
    for l in 1..=6i64 {
        let ms: Vec<i64> = (1..=6i64)
            .filter(|r| l % 3 == r % 3 && r * 10 > 40)
            .map(|r| r * 10)
            .collect();
        if ms.is_empty() {
            expect.push((l, None));
        } else {
            for m in ms {
                expect.push((l, Some(m)));
            }
        }
    }
    expect.sort();
    assert_eq!(rows, expect, "rejected pair leaked into the NULL-extension");

    // Reading many partner columns takes the whole-half copy instead: same answer.
    let rows: Vec<(i64, i64, String, String)> = c
        .query("SELECT wl.k, wr.v, wr.s, wr.p9 FROM wl JOIN wr ON wl.k = wr.k ORDER BY wl.k")
        .await
        .unwrap();
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[2], (3, 30, "r3".into(), "r3-9".into()));
}

/// Late materialisation (ESQL-49), single table: `ORDER BY ... LIMIT` decodes
/// only the filter and sort-key columns until a row wins the top-N admission
/// test. Rows must still come back complete and in the right order, and the
/// filter must see columns nothing else references.
#[tokio::test]
async fn ordered_limit_late_materialisation_returns_complete_rows() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE lm_t (k INT PRIMARY KEY, n INT, s VARCHAR(16), pad VARCHAR(16))")
        .await
        .unwrap();
    let vals = (1..=200)
        .map(|i| format!("({i},{},'s{}','pad{i}')", (i * 37) % 200, i % 7))
        .collect::<Vec<_>>()
        .join(",");
    c.query_drop(format!("INSERT INTO lm_t VALUES {vals}"))
        .await
        .unwrap();

    // ORDER BY and WHERE on columns outside the projection; the payload column
    // is only ever read for the rows that survive.
    let rows: Vec<(i64, String)> = c
        .query("SELECT k, pad FROM lm_t WHERE s = 's3' ORDER BY n LIMIT 3")
        .await
        .unwrap();
    let expect: Vec<(i64, String)> = {
        let mut v: Vec<(i64, i64)> = (1..=200i64)
            .filter(|i| i % 7 == 3)
            .map(|i| (i, (i * 37) % 200))
            .collect();
        v.sort_by_key(|&(k, n)| (n, k));
        v.into_iter()
            .take(3)
            .map(|(k, _)| (k, format!("pad{k}")))
            .collect()
    };
    assert_eq!(rows, expect);

    // OFFSET must still be honoured by the heap, and DESC too.
    let rows: Vec<(i64,)> = c
        .query("SELECT k FROM lm_t ORDER BY n DESC LIMIT 2 OFFSET 1")
        .await
        .unwrap();
    let mut all: Vec<(i64, i64)> = (1..=200i64).map(|i| (i, (i * 37) % 200)).collect();
    all.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    assert_eq!(rows, vec![(all[1].0,), (all[2].0,)]);
}

/// Late materialisation (ESQL-49): the streaming join paths decode only the
/// columns a query reads. Every column that is read but *not* projected -- the
/// join key, a WHERE column, an ORDER BY key, an aggregate argument -- must
/// still be materialised, and columns of the same bare name on both sides must
/// not be confused. Getting that wrong yields NULLs or wrong values silently
/// rather than an error, so it is asserted here on values, not just row counts.
#[tokio::test]
async fn join_late_materialisation_reads_unprojected_columns() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    // Deliberately identical column names on both sides (`g`, `s`): a mask built
    // by bare-name matching would map `r.g` onto `l.g`.
    c.query_drop("CREATE TABLE lm_l (k INT PRIMARY KEY, g INT, s VARCHAR(16), pad VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE lm_r (k INT PRIMARY KEY, g INT, s VARCHAR(16), pad VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO lm_l VALUES (1,10,'a','pl1'),(2,20,'b','pl2'),(3,30,'c','pl3')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO lm_r VALUES (1,11,'x','pr1'),(2,22,'y','pr2'),(3,33,'z','pr3')")
        .await
        .unwrap();

    // COUNT(*) reads no column of either side -- but the join key still must be.
    let n: Vec<i64> = c
        .query("SELECT COUNT(*) FROM lm_l JOIN lm_r ON lm_l.k = lm_r.k")
        .await
        .unwrap();
    assert_eq!(n, vec![3]);

    // Aggregate argument and WHERE column, neither of them projected.
    let n: Vec<i64> = c
        .query("SELECT SUM(lm_r.g) FROM lm_l JOIN lm_r ON lm_l.k = lm_r.k WHERE lm_l.g > 10")
        .await
        .unwrap();
    assert_eq!(n, vec![55], "same-named columns on both sides confused");

    // GROUP BY on the partner's `s`, projecting only the aggregate.
    let mut rows: Vec<(String, i64)> = c
        .query("SELECT lm_r.s, COUNT(*) FROM lm_l JOIN lm_r ON lm_l.k = lm_r.k GROUP BY lm_r.s")
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![("x".into(), 1), ("y".into(), 1), ("z".into(), 1)]
    );

    // ORDER BY a column that is neither projected nor filtered: with LIMIT this
    // is the top-N admission path, which must not drop or reorder rows.
    let rows: Vec<(i64, String)> = c
        .query(
            "SELECT lm_l.k, lm_l.s FROM lm_l JOIN lm_r ON lm_l.k = lm_r.k \
             ORDER BY lm_r.g DESC LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![(3, "c".into()), (2, "b".into())]);

    // `SELECT *` reads every column, including the ones nothing else touches.
    type WideRow = (i64, i64, String, String, i64, i64, String, String);
    let rows: Vec<WideRow> = c
        .query("SELECT * FROM lm_l JOIN lm_r ON lm_l.k = lm_r.k ORDER BY lm_l.k LIMIT 1")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(
            1,
            10,
            "a".into(),
            "pl1".into(),
            1,
            11,
            "x".into(),
            "pr1".into()
        )]
    );
}

/// Join + GROUP BY where the partner is NOT indexed on the join key, so the
/// streaming path declines and the materialising `join_select` handles the
/// aggregation. Same correct result -- this guards the fallback path.
#[tokio::test]
async fn join_group_by_fallback() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE authors (id INT PRIMARY KEY, name VARCHAR(32))")
        .await
        .unwrap();
    // author_id is a plain column (no index) -> streaming NLJ does not apply
    c.query_drop("CREATE TABLE books (id INT PRIMARY KEY, author_id INT, price INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO authors VALUES (1,'Tolkien'),(2,'Le Guin')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO books VALUES (1,1,30),(2,1,20),(3,2,25)")
        .await
        .unwrap();

    let mut rows: Vec<(String, i64, i64)> = c
        .query(
            "SELECT a.name, COUNT(*), SUM(b.price) \
             FROM authors a JOIN books b ON b.author_id = a.id \
             GROUP BY a.name",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![("Le Guin".into(), 1, 25), ("Tolkien".into(), 2, 50)]
    );
}

/// LEFT join + GROUP BY: an unmatched driving row must form a NULL-category
/// group, matching MySQL semantics.
#[tokio::test]
async fn left_join_group_by_streaming() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE dim (id INT PRIMARY KEY, category VARCHAR(8))")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE facts (id INT PRIMARY KEY, dim_id INT, amount INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO dim VALUES (1,'A'),(2,'B'),(3,'A')")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO facts VALUES (1,1,10),(2,1,20),(3,2,5),(4,3,7),(5,2,15),(6,1,3),(7,99,100)",
    )
    .await
    .unwrap();

    // fact 7 has dim_id=99 (no match) -> NULL category group of count 1
    let mut rows: Vec<(Option<String>, i64)> = c
        .query(
            "SELECT d.category, COUNT(*) \
             FROM facts f LEFT JOIN dim d ON f.dim_id = d.id \
             GROUP BY d.category",
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![(None, 1), (Some("A".into()), 4), (Some("B".into()), 2)]
    );
}

/// MySQL's `INSERT ... SET col = val` shorthand (rewritten to the standard form).
/// BIGINT UNSIGNED: values above i64::MAX and unsigned bitwise results round-trip
/// and display correctly (Value::UInt). [ESQL-10]
#[tokio::test]
async fn bigint_unsigned() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    // bitwise results are BIGINT UNSIGNED (64-bit), not signed
    let v: u64 = c
        .query_first("SELECT 18446744073709551615 & 18446744073709551615")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v, u64::MAX);
    let v: u64 = c.query_first("SELECT 1 << 63").await.unwrap().unwrap();
    assert_eq!(v, 1u64 << 63);

    // native (binary) protocol path
    let v: u64 = c
        .exec_first("SELECT ? << ?", (1u64, 63u64))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v, 1u64 << 63);

    // BIGINT UNSIGNED column stores and reads values above i64::MAX exactly
    c.query_drop("CREATE TABLE u (id INT PRIMARY KEY, big BIGINT UNSIGNED)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO u VALUES (1, 18446744073709551610), (2, 42)")
        .await
        .unwrap();
    let rows: Vec<(i64, u64)> = c.query("SELECT id, big FROM u ORDER BY id").await.unwrap();
    assert_eq!(rows, vec![(1, 18446744073709551610), (2, 42)]);
}

/// Unary bitwise-NOT `~` (rewritten to XOR with all-ones), returning BIGINT
/// UNSIGNED, including unsigned arithmetic on the result. [ESQL-3 / ESQL-10]
#[tokio::test]
async fn bitwise_not() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    let v: u64 = c.query_first("SELECT ~5").await.unwrap().unwrap();
    assert_eq!(v, u64::MAX - 5);
    let v: u64 = c.query_first("SELECT ~0").await.unwrap().unwrap();
    assert_eq!(v, u64::MAX);
    let v: u64 = c.query_first("SELECT ~~5").await.unwrap().unwrap();
    assert_eq!(v, 5); // double NOT
    let v: u64 = c.query_first("SELECT ~(1 + 1)").await.unwrap().unwrap();
    assert_eq!(v, u64::MAX - 2);
    // unsigned arithmetic on the NOT result
    let v: u64 = c.query_first("SELECT ~1 + 1").await.unwrap().unwrap();
    assert_eq!(v, u64::MAX);

    c.query_drop("CREATE TABLE bn (id INT PRIMARY KEY, flags INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO bn VALUES (1, 5)").await.unwrap();
    let v: u64 = c
        .query_first("SELECT ~flags FROM bn")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v, u64::MAX - 5);
}

/// Bitwise shift operators `<<` and `>>` (parsed via the generic-dialect
/// fallback, evaluated as 64-bit shifts). [ESQL-3]
#[tokio::test]
async fn bitwise_shift_operators() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    let v: i64 = c.query_first("SELECT 8 << 2").await.unwrap().unwrap();
    assert_eq!(v, 32);
    let v: i64 = c.query_first("SELECT 255 >> 4").await.unwrap().unwrap();
    assert_eq!(v, 15);

    c.query_drop("CREATE TABLE bw (id INT PRIMARY KEY, flags INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO bw VALUES (1,5),(2,8)")
        .await
        .unwrap();
    let rows: Vec<(i64, i64, i64)> = c
        .query("SELECT id, flags << 1, flags >> 1 FROM bw ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 10, 2), (2, 16, 4)]);
}

/// GROUP BY ... WITH ROLLUP adds per-prefix subtotal rows and a grand total,
/// re-aggregating base rows at each level (so AVG stays correct). [ESQL-3]
#[tokio::test]
async fn group_by_with_rollup() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE sales (region VARCHAR(8), product VARCHAR(8), amt INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO sales VALUES ('N','A',10),('N','B',20),('S','A',5),('S','A',15)")
        .await
        .unwrap();

    // two-column rollup: details + per-region subtotals (product NULL) + grand
    // total (both NULL). ORDER BY sorts NULLs first.
    let rows: Vec<(Option<String>, Option<String>, i64)> = c
        .query(
            "SELECT region, product, SUM(amt) FROM sales \
             GROUP BY region, product WITH ROLLUP ORDER BY region, product",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (None, None, 50), // grand total
            (Some("N".into()), None, 30),
            (Some("N".into()), Some("A".into()), 10),
            (Some("N".into()), Some("B".into()), 20),
            (Some("S".into()), None, 20),
            (Some("S".into()), Some("A".into()), 20),
        ]
    );
}

/// WITH ROLLUP re-aggregates base rows per level, so AVG is the true overall
/// average, not an average of group averages. [ESQL-3]
#[tokio::test]
async fn rollup_avg_is_reaggregated() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE t (g VARCHAR(4), v INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t VALUES ('a',10),('a',20),('a',30),('b',100)")
        .await
        .unwrap();
    let rows: Vec<(Option<String>, f64, i64)> = c
        .query("SELECT g, AVG(v), COUNT(*) FROM t GROUP BY g WITH ROLLUP ORDER BY g")
        .await
        .unwrap();
    // grand AVG = (10+20+30+100)/4 = 40, not (20+100)/2 = 60
    assert_eq!(rows[0], (None, 40.0, 4));
    assert_eq!(rows[1], (Some("a".into()), 20.0, 3));
    assert_eq!(rows[2], (Some("b".into()), 100.0, 1));
}

/// MySQL's comma-style multi-table UPDATE (rewritten to CROSS JOIN + WHERE).
#[tokio::test]
async fn comma_multi_table_update() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE b (id INT PRIMARY KEY, w INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO a VALUES (1,0),(2,0)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO b VALUES (1,10),(2,20)")
        .await
        .unwrap();

    c.query_drop("UPDATE a, b SET a.v = b.w WHERE a.id = b.id")
        .await
        .unwrap();

    let rows: Vec<(i64, i64)> = c.query("SELECT id, v FROM a ORDER BY id").await.unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);
}

/// MySQL's `INSERT ... SET col = val` shorthand (rewritten to the standard form).
#[tokio::test]
async fn insert_set_shorthand() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(32), qty INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t SET id = 1, name = 'a,b', qty = 5")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t SET id = 2, name = 'x', qty = 9")
        .await
        .unwrap();
    // ON DUPLICATE KEY UPDATE preserved
    c.query_drop(
        "INSERT INTO t SET id = 1, name = 'z', qty = 1 ON DUPLICATE KEY UPDATE qty = qty + 100",
    )
    .await
    .unwrap();

    let rows: Vec<(i64, String, i64)> = c
        .query("SELECT id, name, qty FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, "a,b".into(), 105), (2, "x".into(), 9)]);
}

#[tokio::test]
async fn scalar_subqueries_in_insert_value_rows() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE properties (id INT PRIMARY KEY, `key` VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE entity_values (id INT PRIMARY KEY, property_id INT, value VARCHAR(16))",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO properties VALUES (10, 'first'), (20, 'second')")
        .await
        .unwrap();

    c.query_drop(
        "INSERT INTO entity_values (id, property_id, value) VALUES
         (1, (SELECT id FROM properties WHERE `key` = 'first'), 'a'),
         (2, (SELECT id FROM properties WHERE `key` = 'second'), 'b')
         ON DUPLICATE KEY UPDATE value = VALUES(value)",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO entity_values (id, property_id, value) VALUES
         (1, (SELECT id FROM properties WHERE `key` = 'first'), 'updated')
         ON DUPLICATE KEY UPDATE value = VALUES(value)",
    )
    .await
    .unwrap();

    let rows: Vec<(i64, i64, String)> = c
        .query("SELECT id, property_id, value FROM entity_values ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 10, "updated".into()), (2, 20, "b".into())]);
}

#[tokio::test]
async fn duplicate_key_update_honors_unique_secondary_indexes() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop(
        "CREATE TABLE keyed_values (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            `key` VARCHAR(16) UNIQUE,
            label VARCHAR(16)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO keyed_values (`key`, label) VALUES ('a', 'old-a'), ('b', 'old-b')")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO keyed_values (`key`, label) VALUES ('a', 'new-a'), ('b', 'new-b')
         ON DUPLICATE KEY UPDATE label = VALUES(label)",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO keyed_values (`key`, label) VALUES ('c', 'first'), ('c', 'second')
         ON DUPLICATE KEY UPDATE label = VALUES(label)",
    )
    .await
    .unwrap();

    let rows: Vec<(i64, String, String)> = c
        .query("SELECT id, `key`, label FROM keyed_values ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, "a".into(), "new-a".into()),
            (2, "b".into(), "new-b".into()),
            (5, "c".into(), "second".into()),
        ]
    );

    c.query_drop("CREATE TABLE unique_only (`key` VARCHAR(16) UNIQUE, label VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO unique_only VALUES ('a', 'old')")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO unique_only VALUES ('a', 'new')
         ON DUPLICATE KEY UPDATE label = VALUES(label)",
    )
    .await
    .unwrap();
    let rows: Vec<(String, String)> = c.query("SELECT * FROM unique_only").await.unwrap();
    assert_eq!(rows, vec![("a".into(), "new".into())]);

    c.query_drop("INSERT INTO keyed_values (`key`, label) VALUES ('d', 'old-d')")
        .await
        .unwrap();
    let err = c
        .query_drop(
            "INSERT INTO keyed_values (id, `key`, label) VALUES (1, 'd', 'conflict')
             ON DUPLICATE KEY UPDATE label = VALUES(label)",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("more than one unique row"));
}

/// SELECT DISTINCT deduplicates (was previously a no-op), applies LIMIT after
/// dedup, and is collation-aware. [ESQL-8 / ESQL-4]
#[tokio::test]
async fn select_distinct() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE u (id INT PRIMARY KEY, name VARCHAR(16), g VARCHAR(8) COLLATE utf8mb4_bin)",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO u VALUES (1,'a','X'),(2,'a','X'),(3,'b','x'),(4,'a','x')")
        .await
        .unwrap();

    // basic dedup
    let names: Vec<String> = c
        .query("SELECT DISTINCT name FROM u ORDER BY name")
        .await
        .unwrap();
    assert_eq!(names, vec!["a", "b"]);

    // multi-column dedup
    let pairs: Vec<(String, String)> = c
        .query("SELECT DISTINCT name, g FROM u ORDER BY name, g")
        .await
        .unwrap();
    // (a,X),(a,X),(b,x),(a,x) -> three distinct pairs (g is _bin, so X != x)
    assert_eq!(
        pairs,
        vec![
            ("a".into(), "X".into()),
            ("a".into(), "x".into()),
            ("b".into(), "x".into())
        ]
    );

    // LIMIT applies AFTER distinct
    let limited: Vec<String> = c
        .query("SELECT DISTINCT name FROM u ORDER BY name LIMIT 1")
        .await
        .unwrap();
    assert_eq!(limited, vec!["a"]);

    // _bin column: 'X' and 'x' are distinct
    let gs: Vec<String> = c
        .query("SELECT DISTINCT g FROM u ORDER BY g")
        .await
        .unwrap();
    assert_eq!(gs, vec!["X", "x"]);
}

/// Default (case-insensitive) DISTINCT folds case, so 'A' and 'a' collapse.
#[tokio::test]
async fn select_distinct_case_insensitive() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE v (id INT PRIMARY KEY, name VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO v VALUES (1,'A'),(2,'a'),(3,'b')")
        .await
        .unwrap();
    let names: Vec<String> = c
        .query("SELECT DISTINCT name FROM v ORDER BY name")
        .await
        .unwrap();
    assert_eq!(names.len(), 2); // A/a fold to one group
}

/// A `_bin` column sorts and groups case-sensitively (byte order); the default
/// column is case-insensitive. [ESQL-4]
#[tokio::test]
async fn binary_collation_order_and_group() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(16) COLLATE utf8mb4_bin)")
        .await
        .unwrap();
    c.query_drop(
        "INSERT INTO t VALUES (1,'Apple'),(2,'apple'),(3,'Banana'),(4,'apple'),(5,'BANANA')",
    )
    .await
    .unwrap();

    // ORDER BY on a _bin column uses byte order: uppercase (0x41..) before
    // lowercase (0x61..), so all-caps 'BANANA' sorts before 'Banana'.
    let ordered: Vec<String> = c.query("SELECT name FROM t ORDER BY name").await.unwrap();
    assert_eq!(ordered, vec!["Apple", "BANANA", "Banana", "apple", "apple"]);

    // GROUP BY on a _bin column keeps distinct case as distinct groups.
    let mut groups: Vec<(String, i64)> = c
        .query("SELECT name, COUNT(*) FROM t GROUP BY name")
        .await
        .unwrap();
    groups.sort();
    assert_eq!(
        groups,
        vec![
            ("Apple".into(), 1),
            ("BANANA".into(), 1),
            ("Banana".into(), 1),
            ("apple".into(), 2),
        ]
    );
}

/// An equi-join on a `_bin` column matches by exact bytes (case-sensitive);
/// the default column matches case-insensitively. [ESQL-4]
#[tokio::test]
async fn binary_collation_join_key() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE a (id INT PRIMARY KEY, code VARCHAR(8) COLLATE utf8mb4_bin)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE b (id INT PRIMARY KEY, code VARCHAR(8) COLLATE utf8mb4_bin, label VARCHAR(16))",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO a VALUES (1,'X'),(2,'x')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO b VALUES (1,'X','upper'),(2,'x','lower')")
        .await
        .unwrap();

    let rows: Vec<(i64, String)> = c
        .query("SELECT a.id, b.label FROM a JOIN b ON a.code = b.code ORDER BY a.id")
        .await
        .unwrap();
    // X matches X, x matches x -- not the cross product
    assert_eq!(rows, vec![(1, "upper".into()), (2, "lower".into())]);
}

/// The default (case-insensitive) column still groups case-insensitively, so the
/// _bin behavior above is genuinely opt-in.
#[tokio::test]
async fn default_collation_group_is_case_insensitive() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t VALUES (1,'Apple'),(2,'apple'),(3,'APPLE')")
        .await
        .unwrap();

    let groups: Vec<(String, i64)> = c
        .query("SELECT name, COUNT(*) FROM t GROUP BY name")
        .await
        .unwrap();
    // one case-insensitive group of 3
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].1, 3);
}

/// ENUM columns are constrained to their declared members (via a synthesized
/// CHECK). [ESQL-2]
#[tokio::test]
async fn enum_value_validation() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, status ENUM('active','inactive','pending'))")
        .await
        .unwrap();

    c.query_drop("INSERT INTO t VALUES (1,'active')")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t VALUES (2, NULL)")
        .await
        .unwrap(); // nullable enum

    // a value outside the member list is rejected
    let bad = c.query_drop("INSERT INTO t VALUES (3,'bogus')").await;
    assert!(bad.is_err(), "ENUM must reject a non-member value");

    let rows: Vec<(i64, Option<String>)> = c
        .query("SELECT id, status FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, Some("active".into())), (2, None)]);
}

/// SET columns accept a comma-separated subset of their members (and empty/NULL),
/// and reject any value containing a non-member. [ESQL-2]
#[tokio::test]
async fn set_value_validation() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, perms SET('read','write','admin'))")
        .await
        .unwrap();

    for (id, v) in [(1, "read"), (2, "read,write"), (3, "")] {
        c.query_drop(format!("INSERT INTO t VALUES ({id},'{v}')"))
            .await
            .unwrap();
    }
    c.query_drop("INSERT INTO t VALUES (4, NULL)")
        .await
        .unwrap();

    // a non-member (alone or within a subset) is rejected
    assert!(c
        .query_drop("INSERT INTO t VALUES (5,'delete')")
        .await
        .is_err());
    assert!(c
        .query_drop("INSERT INTO t VALUES (6,'read,bogus')")
        .await
        .is_err());

    let n: i64 = c
        .query_first("SELECT COUNT(*) FROM t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 4);
}

#[tokio::test]
async fn data_types() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop(
        "CREATE TABLE dt (id INT PRIMARY KEY, price DECIMAL(10,2), d DATE, doc JSON, big BIGINT)",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO dt VALUES (1, 19.95, '2026-07-13', '{\"a\": 1}', 9000000000)")
        .await
        .unwrap();

    // DECIMAL and DATE read back as strings (no chrono/bigdecimal features).
    let (price, d, big): (String, String, i64) = c
        .query_first("SELECT price, d, big FROM dt WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(price, "19.95");
    assert_eq!(d, "2026-07-13");
    assert_eq!(big, 9_000_000_000);

    let doc: String = c
        .query_first("SELECT doc FROM dt WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert!(doc.contains("\"a\""), "json was {doc}");
}

// MySQL accepts datetime-shaped bound strings for DATE columns and stores only
// the date component, including both midnight and non-midnight values.
#[tokio::test]
async fn date_columns_accept_datetime_shaped_prepared_values() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE bound_dates (id INT PRIMARY KEY, d DATE NOT NULL)")
        .await
        .unwrap();
    c.exec_drop(
        "INSERT INTO bound_dates VALUES (?, ?), (?, ?)",
        (1, "2026-08-02 00:00:00", 2, "2026-08-03 23:59:59"),
    )
    .await
    .unwrap();

    let rows: Vec<(i64, String)> = c
        .query("SELECT id, d FROM bound_dates ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, "2026-08-02".into()), (2, "2026-08-03".into())]
    );
    assert!(c
        .exec_drop(
            "INSERT INTO bound_dates VALUES (?, ?)",
            (3, "2026-02-30 00:00:00"),
        )
        .await
        .is_err());
}

#[tokio::test]
async fn introspection() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE widgets (id INT PRIMARY KEY, label VARCHAR(32))")
        .await
        .unwrap();

    let tables: Vec<String> = c.query("SHOW TABLES").await.unwrap();
    assert!(tables.iter().any(|t| t == "widgets"), "tables: {tables:?}");

    let n: i64 = c
        .query_first("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'widgets'")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 2);
}

// Keep the standard table-introspection columns present and binary-protocol
// compatible even when only best-effort size metadata is available.
#[tokio::test]
async fn table_introspection_columns_are_available() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE introspected_table (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let rows: Vec<(String, String, i64, String, String, String)> = c
        .exec(
            "SELECT table_name AS `name`, table_schema AS `schema`, \
                    (data_length + index_length) AS `size`, \
                    table_comment AS `comment`, engine AS `engine`, \
                    table_collation AS `collation` \
             FROM information_schema.tables \
             WHERE table_type IN ('BASE TABLE', 'SYSTEM VERSIONED') \
               AND table_schema IN ('elyra') \
             ORDER BY table_schema, table_name",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(
            "introspected_table".into(),
            "elyra".into(),
            0,
            String::new(),
            "ElyraSQL".into(),
            "utf8mb4_0900_ai_ci".into(),
        )]
    );

    let rows: Vec<(String, String, Option<i64>, String, String, String)> = c
        .exec(
            "SELECT t.TABLE_NAME, t.ENGINE, t.AUTO_INCREMENT,
                    t.TABLE_COMMENT, t.CREATE_OPTIONS, ccsa.CHARACTER_SET_NAME
             FROM information_schema.TABLES t
             INNER JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY ccsa
                ON ccsa.COLLATION_NAME = t.TABLE_COLLATION
             WHERE t.TABLE_SCHEMA = 'elyra'
               AND t.TABLE_NAME = 'introspected_table'
               AND t.TABLE_TYPE = 'BASE TABLE'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(
            "introspected_table".into(),
            "ElyraSQL".into(),
            None,
            String::new(),
            String::new(),
            "utf8mb4".into(),
        )]
    );
}

#[tokio::test]
async fn catalog_qualified_wildcards_validate_complete_relation_paths() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE wildcard_catalog_target (id INT PRIMARY KEY)")
        .await
        .unwrap();

    for sql in [
        "SELECT catalog_table.*
         FROM information_schema.TABLES AS catalog_table
         WHERE catalog_table.TABLE_NAME = 'wildcard_catalog_target'",
        "SELECT information_schema.catalog_table.*
         FROM information_schema.TABLES AS catalog_table
         WHERE catalog_table.TABLE_NAME = 'wildcard_catalog_target'",
        "SELECT information_schema.TABLES.*
         FROM information_schema.TABLES
         WHERE information_schema.TABLES.TABLE_NAME = 'wildcard_catalog_target'",
        "SELECT catalog_table.*, COUNT(*) AS matching_rows
         FROM information_schema.TABLES AS catalog_table
         WHERE catalog_table.TABLE_NAME = 'wildcard_catalog_target'
         GROUP BY catalog_table.TABLE_NAME",
        "SELECT mysql.user.*
         FROM mysql.user
         WHERE mysql.user.User = 'root'",
        "SELECT mysql.catalog_user.*
         FROM mysql.user AS catalog_user
         WHERE catalog_user.User = 'root'",
        "SELECT catalog_user.*, COUNT(*) AS matching_rows
         FROM mysql.user AS catalog_user
         WHERE catalog_user.User = 'root'
         GROUP BY catalog_user.User",
        "SELECT information_schema.TABLES.*, mysql.user.*
         FROM information_schema.TABLES
         JOIN mysql.user ON mysql.user.User = 'root'
         WHERE information_schema.TABLES.TABLE_NAME = 'wildcard_catalog_target'",
        "SELECT catalog_table.*, catalog_user.*
         FROM information_schema.TABLES AS catalog_table
         JOIN mysql.user AS catalog_user ON catalog_user.User = 'root'
         WHERE catalog_table.TABLE_NAME = 'wildcard_catalog_target'",
    ] {
        let rows: Vec<mysql_async::Row> = c.query(sql).await.unwrap();
        assert_eq!(rows.len(), 1, "{sql}");
        if sql.contains("TABLES") {
            assert_eq!(
                rows[0].get::<String, _>("TABLE_NAME").as_deref(),
                Some("wildcard_catalog_target"),
                "{sql}"
            );
        }
        if sql.contains("user") {
            assert_eq!(
                rows[0].get::<String, _>("User").as_deref(),
                Some("root"),
                "{sql}"
            );
        }
        assert!(
            rows[0]
                .columns_ref()
                .iter()
                .all(|column| !column.name_str().contains('.')),
            "wildcard output names must remain unqualified: {sql}"
        );
        if sql.contains("matching_rows") {
            assert_eq!(rows[0].get::<i64, _>("matching_rows"), Some(1), "{sql}");
        }
    }

    for sql in [
        // Plain virtual-relation projection: a multi-part wildcard must name the
        // exact unaliased relation, or schema plus alias for a two-part source.
        "SELECT missing_alias.* FROM information_schema.TABLES AS catalog_table LIMIT 1",
        "SELECT mysql.TABLES.* FROM information_schema.TABLES LIMIT 1",
        "SELECT def.information_schema.TABLES.* FROM information_schema.TABLES LIMIT 1",
        "SELECT mysql.catalog_table.* FROM information_schema.TABLES AS catalog_table LIMIT 1",
        "SELECT def.information_schema.catalog_table.*
         FROM information_schema.TABLES AS catalog_table LIMIT 1",
        "SELECT information_schema.TABLES.*
         FROM information_schema.TABLES AS catalog_table LIMIT 1",
        "SELECT information_schema.user.* FROM mysql.user LIMIT 1",
        "SELECT def.mysql.user.* FROM mysql.user LIMIT 1",
        "SELECT information_schema.catalog_user.*
         FROM mysql.user AS catalog_user LIMIT 1",
        "SELECT def.mysql.catalog_user.* FROM mysql.user AS catalog_user LIMIT 1",
        "SELECT mysql.user.* FROM mysql.user AS catalog_user LIMIT 1",
        // Aggregate projection has its own wildcard expansion plan.
        "SELECT mysql.TABLES.*, COUNT(*)
         FROM information_schema.TABLES GROUP BY TABLE_NAME LIMIT 1",
        "SELECT information_schema.user.*, COUNT(*)
         FROM mysql.user GROUP BY User LIMIT 1",
        // Joined relations are qualified after materialisation and must not let
        // an unrelated schema prefix select a relation by its final name.
        "SELECT mysql.TABLES.*
         FROM information_schema.TABLES
         JOIN mysql.user ON mysql.user.User = 'root' LIMIT 1",
        "SELECT def.mysql.user.*
         FROM information_schema.TABLES
         JOIN mysql.user ON mysql.user.User = 'root' LIMIT 1",
        "SELECT mysql.catalog_table.*
         FROM information_schema.TABLES AS catalog_table
         JOIN mysql.user AS catalog_user ON catalog_user.User = 'root' LIMIT 1",
    ] {
        let error = c.query_drop(sql).await.unwrap_err();
        assert!(
            matches!(error, mysql_async::Error::Server(_)),
            "expected invalid wildcard relation for {sql}, got {error:?}"
        );
    }
}

#[tokio::test]
async fn malformed_virtual_source_paths_are_rejected_everywhere() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    for sql in [
        // Plain projection, unaliased and aliased, for both virtual schemas.
        "SELECT def.information_schema.TABLES.*
         FROM def.information_schema.TABLES LIMIT 1",
        "SELECT t.* FROM def.information_schema.TABLES AS t LIMIT 1",
        "SELECT def.mysql.user.* FROM def.mysql.user LIMIT 1",
        "SELECT u.* FROM def.mysql.user AS u LIMIT 1",
        // Aggregate wildcard planning is a separate engine path.
        "SELECT def.information_schema.TABLES.*, COUNT(*)
         FROM def.information_schema.TABLES GROUP BY TABLE_NAME LIMIT 1",
        "SELECT t.*, COUNT(*)
         FROM def.information_schema.TABLES AS t GROUP BY TABLE_NAME LIMIT 1",
        "SELECT def.mysql.user.*, COUNT(*)
         FROM def.mysql.user GROUP BY User LIMIT 1",
        "SELECT u.*, COUNT(*) FROM def.mysql.user AS u GROUP BY User LIMIT 1",
        // Materialised joins must reject the malformed source before loading it.
        "SELECT def.information_schema.TABLES.*
         FROM def.information_schema.TABLES
         JOIN mysql.user AS u ON u.User = 'root' LIMIT 1",
        "SELECT t.* FROM def.information_schema.TABLES AS t
         JOIN mysql.user AS u ON u.User = 'root' LIMIT 1",
        "SELECT def.mysql.user.* FROM information_schema.TABLES AS t
         JOIN def.mysql.user ON User = 'root' LIMIT 1",
        "SELECT u.* FROM information_schema.TABLES AS t
         JOIN def.mysql.user AS u ON u.User = 'root' LIMIT 1",
    ] {
        let error = c.query_drop(sql).await.unwrap_err();
        assert!(
            matches!(error, mysql_async::Error::Server(_)),
            "expected malformed virtual source error for {sql}, got {error:?}"
        );
    }

    for (sql, params) in [
        (
            "SELECT def.information_schema.TABLES.*
             FROM def.information_schema.TABLES WHERE TABLE_NAME = ?",
            ("missing",),
        ),
        (
            "SELECT t.* FROM def.information_schema.TABLES AS t WHERE TABLE_NAME = ?",
            ("missing",),
        ),
        (
            "SELECT def.mysql.user.* FROM def.mysql.user WHERE User = ?",
            ("root",),
        ),
        (
            "SELECT u.* FROM def.mysql.user AS u WHERE User = ?",
            ("root",),
        ),
    ] {
        let error = c.exec_drop(sql, params).await.unwrap_err();
        assert!(
            matches!(error, mysql_async::Error::Server(_)),
            "native prepared execution accepted malformed source {sql}: {error:?}"
        );
    }
}

#[tokio::test]
async fn schema_qualified_alias_wildcards_follow_mysql_identity() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop(
        "CREATE TABLE wildcard_alias_source (
            id INT PRIMARY KEY,
            label VARCHAR(16)
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO wildcard_alias_source VALUES (1, 'stored')")
        .await
        .unwrap();

    for (sql, expected_names) in [
        (
            "SELECT information_schema.t.*
             FROM information_schema.TABLES AS t
             WHERE t.TABLE_NAME = 'wildcard_alias_source'",
            vec![
                "TABLE_SCHEMA",
                "TABLE_NAME",
                "TABLE_TYPE",
                "ENGINE",
                "TABLE_ROWS",
                "DATA_LENGTH",
                "INDEX_LENGTH",
                "TABLE_COMMENT",
                "TABLE_COLLATION",
                "AUTO_INCREMENT",
                "CREATE_OPTIONS",
            ],
        ),
        (
            "SELECT mysql.u.* FROM mysql.user AS u WHERE u.User = 'root'",
            vec![
                "Host",
                "User",
                "Select_priv",
                "Insert_priv",
                "Update_priv",
                "Delete_priv",
                "Create_priv",
                "Drop_priv",
                "Super_priv",
                "plugin",
                "authentication_string",
                "account_locked",
                "password_expired",
            ],
        ),
        (
            "SELECT elyra.o.* FROM elyra.wildcard_alias_source AS o",
            vec!["id", "label"],
        ),
        (
            "SELECT elyra.wildcard_alias_source.* FROM wildcard_alias_source",
            vec!["id", "label"],
        ),
        (
            "SELECT elyra.o.* FROM wildcard_alias_source AS o",
            vec!["id", "label"],
        ),
    ] {
        let mut result = c.query_iter(sql).await.unwrap();
        let names = result
            .columns_ref()
            .iter()
            .map(|column| column.name_str().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, expected_names, "{sql}");
        let rows: Vec<mysql_async::Row> = result.collect().await.unwrap();
        assert_eq!(rows.len(), 1, "{sql}");
    }

    for sql in [
        "SELECT mysql.t.* FROM information_schema.TABLES AS t LIMIT 1",
        "SELECT def.information_schema.t.* FROM information_schema.TABLES AS t LIMIT 1",
        "SELECT information_schema.TABLES.* FROM information_schema.TABLES AS t LIMIT 1",
        "SELECT information_schema.u.* FROM mysql.user AS u LIMIT 1",
        "SELECT def.mysql.u.* FROM mysql.user AS u LIMIT 1",
        "SELECT mysql.user.* FROM mysql.user AS u LIMIT 1",
        "SELECT mysql.o.* FROM elyra.wildcard_alias_source AS o",
        "SELECT def.elyra.o.* FROM elyra.wildcard_alias_source AS o",
        "SELECT elyra.wildcard_alias_source.* FROM elyra.wildcard_alias_source AS o",
    ] {
        let error = c.query_drop(sql).await.unwrap_err();
        assert!(
            matches!(error, mysql_async::Error::Server(_)),
            "expected invalid schema-qualified alias for {sql}, got {error:?}"
        );
    }
}

#[tokio::test]
async fn catalog_wildcards_bind_to_one_same_named_join_relation() {
    const TABLE_FIELDS: [&str; 11] = [
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "TABLE_TYPE",
        "ENGINE",
        "TABLE_ROWS",
        "DATA_LENGTH",
        "INDEX_LENGTH",
        "TABLE_COMMENT",
        "TABLE_COLLATION",
        "AUTO_INCREMENT",
        "CREATE_OPTIONS",
    ];

    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE TABLES (stored_id INT PRIMARY KEY, stored_label VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO TABLES VALUES (1, 'stored')")
        .await
        .unwrap();

    let plain_sql = "SELECT information_schema.TABLES.*
        FROM information_schema.TABLES
        JOIN TABLES ON TABLES.stored_id = 1
        WHERE information_schema.TABLES.TABLE_NAME = 'TABLES'";
    let mut result = c.query_iter(plain_sql).await.unwrap();
    let names = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, TABLE_FIELDS, "plain join metadata");
    let rows: Vec<mysql_async::Row> = result.collect().await.unwrap();
    assert_eq!(rows.len(), 1);

    let aggregate_sql = "SELECT information_schema.TABLES.*, COUNT(*) AS matching_rows
        FROM information_schema.TABLES
        JOIN TABLES ON TABLES.stored_id = 1
        WHERE information_schema.TABLES.TABLE_NAME = 'TABLES'
        GROUP BY information_schema.TABLES.TABLE_NAME";
    let mut result = c.query_iter(aggregate_sql).await.unwrap();
    let names = result
        .columns_ref()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>();
    let mut aggregate_fields = TABLE_FIELDS.to_vec();
    aggregate_fields.push("matching_rows");
    assert_eq!(names, aggregate_fields, "aggregate join metadata");
    let rows: Vec<mysql_async::Row> = result.collect().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("matching_rows"), Some(1));

    let prepared_sql = "SELECT information_schema.TABLES.*
        FROM information_schema.TABLES
        JOIN TABLES ON TABLES.stored_id = 1
        WHERE information_schema.TABLES.TABLE_NAME = ?";
    let mut result = c.exec_iter(prepared_sql, ("TABLES",)).await.unwrap();
    let names = result
        .columns()
        .unwrap()
        .iter()
        .map(|column| column.name_str().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, TABLE_FIELDS, "native prepared metadata");
    let rows: Vec<mysql_async::Row> = result.collect().await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn qualified_wildcards_keep_the_complete_unaliased_relation_identity() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE same_named_relation (id INT PRIMARY KEY, label VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO same_named_relation VALUES (1, 'stored')")
        .await
        .unwrap();

    for sql in [
        "SELECT elyra.same_named_relation.*
         FROM elyra.same_named_relation
         JOIN shadow.same_named_relation ON shadow.same_named_relation.id = 1",
        "SELECT elyra.same_named_relation.*, COUNT(*) AS matching_rows
         FROM elyra.same_named_relation
         JOIN shadow.same_named_relation ON shadow.same_named_relation.id = 1
         GROUP BY elyra.same_named_relation.id",
    ] {
        let mut result = c.query_iter(sql).await.unwrap();
        let names = result
            .columns_ref()
            .iter()
            .map(|column| column.name_str().into_owned())
            .collect::<Vec<_>>();
        let expected = if sql.contains("COUNT") {
            vec!["id", "label", "matching_rows"]
        } else {
            vec!["id", "label"]
        };
        assert_eq!(names, expected, "{sql}");
        let rows: Vec<mysql_async::Row> = result.collect().await.unwrap();
        assert_eq!(rows.len(), 1, "{sql}");
    }

    let mut result = c
        .exec_iter(
            "SELECT elyra.same_named_relation.*
             FROM elyra.same_named_relation
             JOIN shadow.same_named_relation ON shadow.same_named_relation.id = ?",
            (1,),
        )
        .await
        .unwrap();
    assert_eq!(result.columns().unwrap().len(), 2);
    let rows: Vec<mysql_async::Row> = result.collect().await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn complete_virtual_relation_names_bind_in_correlated_subqueries() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE correlated_catalog_target (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let names: Vec<String> = c
        .query(
            "SELECT information_schema.TABLES.TABLE_NAME
             FROM information_schema.TABLES
             JOIN mysql.user ON mysql.user.User = 'root'
             WHERE EXISTS (
                 SELECT 1
                 WHERE information_schema.TABLES.TABLE_NAME = 'correlated_catalog_target'
             )",
        )
        .await
        .unwrap();
    assert_eq!(names, ["correlated_catalog_target"]);
}

#[tokio::test]
async fn fully_qualified_join_identity_is_consistent_across_plans_and_mutations() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE qa (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE qb (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO qa VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO qb VALUES (1), (2)")
        .await
        .unwrap();

    let joined: Vec<(i64, i64)> = c
        .query(
            "SELECT elyra.qa.id, elyra.qa.value
             FROM elyra.qa
             JOIN elyra.qb ON elyra.qa.id = elyra.qb.id
             ORDER BY elyra.qa.id",
        )
        .await
        .unwrap();
    assert_eq!(joined, [(1, 10), (2, 20)]);

    let short_qualified: Vec<(i64, i64)> = c
        .query(
            "SELECT qa.id, qa.value
             FROM elyra.qa
             JOIN elyra.qb ON qa.id = qb.id
             ORDER BY qa.id",
        )
        .await
        .unwrap();
    assert_eq!(short_qualified, [(1, 10), (2, 20)]);

    let aliased: Vec<(i64, i64)> = c
        .query(
            "SELECT elyra.q.id, elyra.q.value
             FROM elyra.qa AS q
             JOIN elyra.qb AS b ON elyra.q.id = elyra.b.id
             ORDER BY elyra.q.id",
        )
        .await
        .unwrap();
    assert_eq!(aliased, [(1, 10), (2, 20)]);

    let correlated: Vec<i64> = c
        .query(
            "SELECT elyra.qa.id
             FROM elyra.qa
             JOIN elyra.qb ON elyra.qa.id = elyra.qb.id
             WHERE EXISTS (SELECT 1 WHERE qa.id = 1)
             ORDER BY elyra.qa.id",
        )
        .await
        .unwrap();
    assert_eq!(correlated, [1]);

    let aliased_correlated: Vec<i64> = c
        .query(
            "SELECT elyra.q.id
             FROM elyra.qa AS q
             JOIN elyra.qb AS b ON elyra.q.id = elyra.b.id
             WHERE EXISTS (SELECT 1 WHERE elyra.q.id = 1)
             ORDER BY elyra.q.id",
        )
        .await
        .unwrap();
    assert_eq!(aliased_correlated, [1]);

    let shadowed: Vec<i64> = c
        .query(
            "SELECT elyra.qa.id FROM elyra.qa
             WHERE EXISTS (
                 SELECT 1 FROM qa WHERE elyra.qa.id = 2
             )
             ORDER BY elyra.qa.id",
        )
        .await
        .unwrap();
    assert_eq!(shadowed, [1, 2]);

    c.query_drop(
        "UPDATE elyra.qa AS q
         JOIN elyra.qb AS b ON elyra.q.id = elyra.b.id
         SET elyra.q.value = 35
         WHERE elyra.q.id = 1",
    )
    .await
    .unwrap();
    let aliased_update: i64 = c
        .query_first("SELECT value FROM qa WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(aliased_update, 35);

    c.query_drop(
        "UPDATE elyra.qa
         JOIN elyra.qb ON qa.id = qb.id
         SET qa.value = 40
         WHERE qa.id = 1",
    )
    .await
    .unwrap();
    let short_updated: i64 = c
        .query_first("SELECT value FROM qa WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(short_updated, 40);

    c.query_drop(
        "UPDATE elyra.qa
         JOIN elyra.qb ON elyra.qa.id = elyra.qb.id
         SET elyra.qa.value = 30
         WHERE elyra.qa.id = 1",
    )
    .await
    .unwrap();
    let updated: i64 = c
        .query_first("SELECT value FROM qa WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated, 30);

    c.query_drop(
        "DELETE elyra.qa
         FROM elyra.qa
         JOIN elyra.qb ON elyra.qa.id = elyra.qb.id
         WHERE elyra.qa.id = 2",
    )
    .await
    .unwrap();
    let remaining: Vec<i64> = c.query("SELECT id FROM qa ORDER BY id").await.unwrap();
    assert_eq!(remaining, [1]);

    c.query_drop("INSERT INTO qa VALUES (2, 20)").await.unwrap();
    c.query_drop(
        "DELETE qa
         FROM elyra.qa
         JOIN elyra.qb ON qa.id = qb.id
         WHERE qa.id = 2",
    )
    .await
    .unwrap();
    let remaining: Vec<i64> = c.query("SELECT id FROM qa ORDER BY id").await.unwrap();
    assert_eq!(remaining, [1]);
}

#[tokio::test]
async fn fully_qualified_wildcards_preserve_unaliased_view_identity() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE view_source (id INT PRIMARY KEY, label VARCHAR(20))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO view_source VALUES (1, 'one'), (2, 'two')")
        .await
        .unwrap();
    c.query_drop("CREATE VIEW qualified_view AS SELECT id, label FROM view_source")
        .await
        .unwrap();

    let rows: Vec<(i64, String)> = c
        .query("SELECT elyra.qualified_view.* FROM elyra.qualified_view ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, [(1, "one".into()), (2, "two".into())]);

    let count: i64 = c
        .query_first("SELECT COUNT(elyra.qualified_view.id) FROM elyra.qualified_view")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 2);

    let distinct: Vec<(i64, String)> = c
        .query(
            "SELECT DISTINCT elyra.qualified_view.*
             FROM elyra.qualified_view ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(distinct, [(1, "one".into()), (2, "two".into())]);

    let prepared_rows: Vec<mysql_async::Row> = c
        .exec_iter(
            "SELECT DISTINCT elyra.qualified_view.* FROM elyra.qualified_view",
            (),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(prepared_rows.len(), 2);

    let rollup_rows: Vec<mysql_async::Row> = c
        .query(
            "SELECT elyra.qualified_view.*, COUNT(*)
             FROM elyra.qualified_view
             GROUP BY id, label WITH ROLLUP",
        )
        .await
        .unwrap();
    assert!(!rollup_rows.is_empty());
}

#[tokio::test]
async fn quoted_dotted_relation_names_keep_identifier_boundaries() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE `dot.name` (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE dot_partner (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO `dot.name` VALUES (1)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO dot_partner VALUES (1)")
        .await
        .unwrap();

    let id: i64 = c
        .query_first(
            "SELECT `dot.name`.id
             FROM elyra.`dot.name`
             JOIN elyra.dot_partner
               ON `dot.name`.id = dot_partner.id",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id, 1);
}

#[tokio::test]
async fn quoted_dotted_column_names_keep_identifier_boundaries() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE dotcol (id INT PRIMARY KEY, `a.b` INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE dotcol_partner (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO dotcol VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO dotcol_partner VALUES (1), (2)")
        .await
        .unwrap();

    for (sql, expected_table) in [
        ("SELECT dotcol.* FROM dotcol ORDER BY id", "dotcol"),
        ("SELECT d.* FROM dotcol AS d ORDER BY id", "d"),
        ("SELECT d.* FROM dotcol AS d JOIN dotcol_partner AS p ON p.id = d.id ORDER BY d.id", "d"),
        ("SELECT d.*, COUNT(*) AS n FROM dotcol AS d JOIN dotcol_partner AS p ON p.id = d.id GROUP BY d.id, d.`a.b` ORDER BY d.id", "d"),
        ("SELECT d.*, ROW_NUMBER() OVER (ORDER BY d.id) AS rn FROM dotcol AS d ORDER BY d.id", "d"),
        ("SELECT d.id, d.`a.b`, ROW_NUMBER() OVER (ORDER BY d.id) AS rn FROM dotcol AS d ORDER BY d.id", "d"),
        ("SELECT d.id, d.`a.b` FROM dotcol AS d JOIN dotcol_partner AS p ON p.id = d.id ORDER BY d.id", "d"),
    ] {
        let mut result = c
            .query_iter(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        let columns = result.columns_ref();
        assert_eq!(columns[0].name_str(), "id", "{sql}");
        assert_eq!(columns[1].name_str(), "a.b", "{sql}");
        assert_eq!(columns[0].table_str(), expected_table, "{sql}");
        assert_eq!(columns[1].table_str(), expected_table, "{sql}");
        let rows: Vec<mysql_async::Row> = result.collect().await.unwrap();
        assert_eq!(rows.len(), 2, "{sql}");
        assert_eq!(rows[0].get::<i64, _>("a.b"), Some(10), "{sql}");
        assert_eq!(rows[1].get::<i64, _>("a.b"), Some(20), "{sql}");
    }
}

#[tokio::test]
async fn qualified_dotted_columns_never_collide_with_flattened_names() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop(
        "CREATE TABLE boundary_t (
            id INT PRIMARY KEY,
            `a.b` INT,
            `boundary_t.a.b` INT,
            `elyra.boundary_t.id` INT
        )",
    )
    .await
    .unwrap();
    c.query_drop("CREATE TABLE boundary_partner (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO boundary_t VALUES (1, 10, 99, 777)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO boundary_partner VALUES (1)")
        .await
        .unwrap();

    let value: i64 = c
        .query_first(
            "SELECT boundary_t.`a.b`
             FROM boundary_t
             WHERE boundary_t.`a.b` = 10",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 10);

    let sum: i64 = c
        .query_first("SELECT SUM(boundary_t.`a.b`) FROM boundary_t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sum, 10);

    let quoted_identifier: i64 = c
        .query_first(
            "SELECT `elyra.boundary_t.id`
             FROM boundary_t
             JOIN boundary_partner ON boundary_t.id = boundary_partner.id",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(quoted_identifier, 777);
}

#[tokio::test]
async fn correlated_set_operation_arms_bind_the_outer_scope_independently() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE set_outer (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO set_outer VALUES (1), (2)")
        .await
        .unwrap();

    for sql in [
        "SELECT o.id FROM set_outer AS o
         WHERE EXISTS (
             SELECT 1 WHERE 0
             UNION ALL
             SELECT 1 WHERE o.id = 2
         ) ORDER BY o.id",
        "SELECT elyra.set_outer.id FROM elyra.set_outer
         WHERE EXISTS (
             SELECT 1 WHERE elyra.set_outer.id = 2
             INTERSECT
             SELECT 1 WHERE elyra.set_outer.id = 2
         ) ORDER BY elyra.set_outer.id",
        "SELECT o.id FROM set_outer AS o
         WHERE EXISTS (
             SELECT 1 WHERE o.id = 2
             EXCEPT
             SELECT 1 WHERE 0
         ) ORDER BY o.id",
        "SELECT o.id FROM set_outer AS o
         WHERE EXISTS (
             SELECT 1 FROM set_outer AS o WHERE o.id = 99
             UNION ALL
             SELECT 1 WHERE o.id = 2
         ) ORDER BY o.id",
        "SELECT o.id FROM set_outer AS o
         WHERE EXISTS (
             SELECT 1 WHERE 0
             UNION ALL
             SELECT 1 WHERE EXISTS (
                 SELECT 1 WHERE o.id = 2
             )
         ) ORDER BY o.id",
    ] {
        let ids: Vec<i64> = c.query(sql).await.unwrap();
        assert_eq!(ids, [2], "{sql}");
    }
}

#[tokio::test]
async fn query_level_order_by_uses_the_local_relation_scope() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE order_outer (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE order_inner (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO order_outer VALUES (1), (2)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO order_inner VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    let rows: Vec<(i64, i64)> = c
        .query(
            "SELECT o.id,
                    (SELECT o.value
                     FROM order_inner AS o
                     ORDER BY o.id DESC LIMIT 1)
             FROM order_outer AS o
             ORDER BY o.id",
        )
        .await
        .unwrap();
    assert_eq!(rows, [(1, 20), (2, 20)]);
}

#[tokio::test]
async fn quoted_relation_names_do_not_collide_with_qualified_suffixes() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE name (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE `dot.name` (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO name VALUES (1)").await.unwrap();
    c.query_drop("INSERT INTO `dot.name` VALUES (1)")
        .await
        .unwrap();

    let ids: Vec<i64> = c
        .query(
            "SELECT name.id FROM name
             JOIN `dot.name` ON name.id = `dot.name`.id",
        )
        .await
        .unwrap();
    assert_eq!(ids, [1]);

    c.query_drop(
        "UPDATE name JOIN `dot.name` ON name.id = `dot.name`.id
         SET name.id = 2 WHERE `dot.name`.id = 1",
    )
    .await
    .unwrap();
    let ids: Vec<i64> = c.query("SELECT id FROM name").await.unwrap();
    assert_eq!(ids, [2]);

    let joined: i64 = c
        .query_first(
            "SELECT COUNT(*) FROM name
             JOIN `dot.name` ON 1 = 1 WHERE `dot.name`.id = 1",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(joined, 1);

    let result = c
        .query_iter(
            "DELETE `dot.name` FROM name
             JOIN `dot.name` ON 1 = 1 WHERE `dot.name`.id = 1",
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 1);
    result.drop_result().await.unwrap();
    let count: i64 = c
        .query_first("SELECT COUNT(*) FROM `dot.name`")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn duplicate_mutation_aliases_are_rejected_before_writes() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE duplicate_alias_a (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE duplicate_alias_b (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO duplicate_alias_a VALUES (1, 10)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO duplicate_alias_b VALUES (1, 20)")
        .await
        .unwrap();

    for sql in [
        "UPDATE duplicate_alias_a AS x
         JOIN duplicate_alias_b AS x ON x.id = x.id
         SET x.value = 99",
        "DELETE x FROM duplicate_alias_a AS x
         JOIN duplicate_alias_b AS x ON x.id = x.id",
        "UPDATE duplicate_alias_a AS x
         JOIN (SELECT id FROM duplicate_alias_b) AS x ON 1 = 1
         SET x.value = 99",
        "DELETE x FROM duplicate_alias_a AS x
         JOIN (SELECT id FROM duplicate_alias_b) AS x ON 1 = 1",
    ] {
        let error = c.query_drop(sql).await.unwrap_err();
        assert!(
            matches!(error, mysql_async::Error::Server(_)),
            "{sql}: {error:?}"
        );
    }

    let a: i64 = c
        .query_first("SELECT value FROM duplicate_alias_a WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    let b: i64 = c
        .query_first("SELECT value FROM duplicate_alias_b WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((a, b), (10, 20));
}

#[tokio::test]
async fn mutation_target_mapping_preserves_structured_aliases() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE case_target (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE case_partner (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO case_target VALUES (1, 10)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO case_partner VALUES (1, 40)")
        .await
        .unwrap();

    c.query_drop(
        "UPDATE case_target AS X
         JOIN case_partner AS Y ON X.id = Y.id
         SET X.value = 20",
    )
    .await
    .unwrap();
    c.query_drop(
        "UPDATE case_target AS `X.Dot`
         JOIN case_partner AS `Y.Dot` ON `X.Dot`.id = `Y.Dot`.id
         SET `X.Dot`.value = 30",
    )
    .await
    .unwrap();
    let value: i64 = c
        .query_first("SELECT value FROM case_target WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 30);

    let result = c
        .query_iter(
            "DELETE `Y.Dot` FROM case_target AS `X.Dot`
             JOIN case_partner AS `Y.Dot` ON `X.Dot`.id = `Y.Dot`.id",
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 1);
    result.drop_result().await.unwrap();
    let remaining: i64 = c
        .query_first("SELECT COUNT(*) FROM case_partner")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn delete_using_reads_rows_from_the_using_scope() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE using_target (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE using_partner (id INT PRIMARY KEY, target_id INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_target VALUES (1), (2), (99)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_partner VALUES (99, 1)")
        .await
        .unwrap();

    let result = c
        .query_iter(
            "DELETE FROM using_target
             USING using_target
             JOIN using_partner ON using_target.id = using_partner.target_id
             WHERE using_partner.id = 99",
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 1);
    result.drop_result().await.unwrap();

    let ids: Vec<i64> = c
        .query("SELECT id FROM using_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(ids, [2, 99]);

    c.query_drop("CREATE TABLE using_a (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE using_b (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE using_link (
            id INT PRIMARY KEY,
            a_id INT,
            b_id INT
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO using_a VALUES (1)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_b VALUES (2)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_link VALUES (99, 1, 2)")
        .await
        .unwrap();

    let result = c
        .query_iter(
            "DELETE FROM using_a, using_b
             USING using_a
             JOIN using_link ON using_a.id = using_link.a_id
             JOIN using_b ON using_b.id = using_link.b_id
             WHERE using_link.id = 99",
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 2);
    result.drop_result().await.unwrap();
    let a: i64 = c
        .query_first("SELECT COUNT(*) FROM using_a")
        .await
        .unwrap()
        .unwrap();
    let b: i64 = c
        .query_first("SELECT COUNT(*) FROM using_b")
        .await
        .unwrap()
        .unwrap();
    assert_eq!((a, b), (0, 0));
}

#[tokio::test]
async fn delete_using_preserves_foreign_keys_and_after_triggers() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    c.query_drop("CREATE TABLE using_restrict_parent (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE using_restrict_child (
            id INT PRIMARY KEY,
            parent_id INT,
            CONSTRAINT fk_using_restrict
                FOREIGN KEY (parent_id) REFERENCES using_restrict_parent(id)
        )",
    )
    .await
    .unwrap();
    c.query_drop("CREATE TABLE using_restrict_match (id INT PRIMARY KEY, parent_id INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_restrict_parent VALUES (1)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_restrict_child VALUES (1, 1)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_restrict_match VALUES (1, 1)")
        .await
        .unwrap();

    let error = c
        .query_drop(
            "DELETE FROM using_restrict_parent
             USING using_restrict_parent
             JOIN using_restrict_match
               ON using_restrict_parent.id = using_restrict_match.parent_id
             WHERE using_restrict_match.id = 1",
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("reference"), "{error}");
    let counts: (i64, i64) = c
        .query_first(
            "SELECT
                (SELECT COUNT(*) FROM using_restrict_parent),
                (SELECT COUNT(*) FROM using_restrict_child)",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts, (1, 1));

    c.query_drop("CREATE TABLE using_cascade_parent (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE using_cascade_child (
            id INT PRIMARY KEY,
            parent_id INT,
            CONSTRAINT fk_using_cascade
                FOREIGN KEY (parent_id) REFERENCES using_cascade_parent(id)
                ON DELETE CASCADE
        )",
    )
    .await
    .unwrap();
    c.query_drop("CREATE TABLE using_cascade_match (id INT PRIMARY KEY, parent_id INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE using_delete_audit (parent_id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TRIGGER audit_using_delete
         AFTER DELETE ON using_cascade_parent
         FOR EACH ROW INSERT INTO using_delete_audit VALUES (OLD.id)",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO using_cascade_parent VALUES (2)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_cascade_child VALUES (2, 2)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO using_cascade_match VALUES (2, 2)")
        .await
        .unwrap();

    let result = c
        .query_iter(
            "DELETE FROM using_cascade_parent
             USING using_cascade_parent
             JOIN using_cascade_match
               ON using_cascade_parent.id = using_cascade_match.parent_id
             WHERE using_cascade_match.id = 2",
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 1);
    result.drop_result().await.unwrap();
    let counts: (i64, i64, i64) = c
        .query_first(
            "SELECT
                (SELECT COUNT(*) FROM using_cascade_parent),
                (SELECT COUNT(*) FROM using_cascade_child),
                (SELECT COUNT(*) FROM using_delete_audit)",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts, (0, 0, 1));

    c.query_drop("CREATE TABLE using_self_target (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE using_self_audit (parent_id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TRIGGER audit_using_self_delete
         AFTER DELETE ON using_self_target
         FOR EACH ROW INSERT INTO using_self_audit VALUES (OLD.id)",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO using_self_target VALUES (3)")
        .await
        .unwrap();

    let result = c
        .query_iter(
            "DELETE FROM a, b
             USING using_self_target AS a
             JOIN using_self_target AS b ON a.id = b.id
             WHERE a.id = 3",
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 1);
    result.drop_result().await.unwrap();
    let counts: (i64, i64) = c
        .query_first(
            "SELECT
                (SELECT COUNT(*) FROM using_self_target),
                (SELECT COUNT(*) FROM using_self_audit)",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts, (0, 1));

    c.query_drop("INSERT INTO using_self_target VALUES (4), (5)")
        .await
        .unwrap();
    let result = c
        .query_iter(
            "DELETE FROM a, b
             USING using_self_target AS a
             JOIN using_self_target AS b ON a.id <> b.id
             WHERE a.id = 4 AND b.id = 5",
        )
        .await
        .unwrap();
    assert_eq!(result.affected_rows(), 2);
    result.drop_result().await.unwrap();
    let counts: (i64, i64) = c
        .query_first(
            "SELECT
                (SELECT COUNT(*) FROM using_self_target),
                (SELECT COUNT(*) FROM using_self_audit)",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counts, (0, 3));
}

#[tokio::test]
async fn relation_aliases_hide_original_names_in_all_expression_paths() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE hidden_original (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE hidden_partner (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO hidden_original VALUES (1, 10)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO hidden_partner VALUES (1)")
        .await
        .unwrap();

    for sql in [
        "SELECT hidden_original.id FROM hidden_original AS x",
        "SELECT hidden_original.id FROM elyra.hidden_original AS x",
        "SELECT x.id FROM hidden_original AS x WHERE hidden_original.id = 1",
        "SELECT hidden_original.id FROM hidden_original AS x
         JOIN hidden_partner AS p ON p.id = x.id",
        "SELECT x.id FROM hidden_original AS x
         JOIN hidden_partner AS p ON p.id = x.id
         WHERE hidden_original.id = 1",
        "UPDATE hidden_original AS x
         SET x.value = 99 WHERE hidden_original.id = 1",
        "UPDATE hidden_original AS x
         SET x.value = 99 ORDER BY hidden_original.id LIMIT 1",
        "UPDATE hidden_original AS x
         JOIN hidden_partner AS p ON p.id = x.id
         SET x.value = 99 WHERE hidden_original.id = 1",
        "DELETE x FROM hidden_original AS x
         WHERE hidden_original.id = 1",
        "DELETE FROM hidden_original AS x
         ORDER BY hidden_original.id LIMIT 1",
        "DELETE x FROM hidden_original AS x
         JOIN hidden_partner AS p ON p.id = x.id
         WHERE hidden_original.id = 1",
    ] {
        let error = c.query_drop(sql).await.unwrap_err();
        assert!(
            matches!(error, mysql_async::Error::Server(_)),
            "{sql}: {error:?}"
        );
    }

    let value: i64 = c
        .query_first("SELECT value FROM hidden_original WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 10);
}

#[tokio::test]
async fn alias_hiding_honors_visible_self_joins_and_nested_expressions() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE self_alias (id INT PRIMARY KEY, value INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO self_alias VALUES (1, 10)")
        .await
        .unwrap();

    let id: i64 = c
        .query_first(
            "SELECT self_alias.id
             FROM self_alias AS x
             JOIN self_alias ON self_alias.id = x.id",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id, 1);

    for sql in [
        "SELECT CASE WHEN self_alias.id = 1 THEN x.value END
         FROM self_alias AS x",
        "SELECT CAST(self_alias.value AS SIGNED)
         FROM self_alias AS x",
        "SELECT x.id FROM self_alias AS x
         WHERE self_alias.value LIKE '1%'",
        "SELECT ROW_NUMBER() OVER (ORDER BY self_alias.id)
         FROM self_alias AS x",
        "SELECT ROW_NUMBER() OVER w FROM self_alias AS x
         WINDOW w AS (ORDER BY self_alias.id)",
        "UPDATE self_alias AS x
         SET x.value = CASE WHEN self_alias.id = 1 THEN 99 ELSE x.value END",
    ] {
        let error = c.query_drop(sql).await.unwrap_err();
        assert!(
            matches!(error, mysql_async::Error::Server(_)),
            "{sql}: {error:?}"
        );
    }
    let value: i64 = c
        .query_first("SELECT value FROM self_alias WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, 10);
}

#[tokio::test]
async fn duplicate_select_aliases_are_rejected() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE select_alias_a (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE select_alias_b (id INT PRIMARY KEY)")
        .await
        .unwrap();

    for sql in ["SELECT * FROM select_alias_a AS dup
         JOIN select_alias_b AS dup ON 1 = 1"]
    {
        let error = c.query_drop(sql).await.unwrap_err();
        assert!(
            matches!(error, mysql_async::Error::Server(_)),
            "{sql}: {error:?}"
        );
    }
}

#[tokio::test]
async fn qualified_join_predicates_preserve_binary_collation() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE bin_first (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE bin_second (
            id INT PRIMARY KEY,
            code VARCHAR(8) COLLATE utf8mb4_bin
        )",
    )
    .await
    .unwrap();
    c.query_drop("INSERT INTO bin_first VALUES (1)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO bin_second VALUES (1, 'x')")
        .await
        .unwrap();

    let count: i64 = c
        .query_first(
            "SELECT COUNT(*)
             FROM bin_first
             JOIN bin_second ON bin_first.id = bin_second.id
             WHERE bin_second.code = 'X'",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn fully_qualified_inner_relations_shadow_outer_join_relations() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE shadow_qa (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE shadow_qb (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO shadow_qa VALUES (1), (2)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO shadow_qb VALUES (1), (2)")
        .await
        .unwrap();

    let ids: Vec<i64> = c
        .query(
            "SELECT elyra.shadow_qa.id
             FROM elyra.shadow_qa
             JOIN elyra.shadow_qb
               ON elyra.shadow_qb.id = elyra.shadow_qa.id
             WHERE EXISTS (
                 SELECT 1 FROM elyra.shadow_qa
                 WHERE elyra.shadow_qa.id = 2
             )
             ORDER BY elyra.shadow_qa.id",
        )
        .await
        .unwrap();
    assert_eq!(ids, [1, 2]);
}

#[tokio::test]
async fn selected_database_names_catalog_rows_for_the_session() {
    let srv = TestServer::start().await;
    let mut c = srv.conn_to_database("application_test").await;
    c.query_drop("CREATE TABLE migrations (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let database: String = c.query_first("SELECT DATABASE()").await.unwrap().unwrap();
    assert_eq!(database, "application_test");

    let exists: i64 = c
        .query_first(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'application_test'
                  AND table_name = 'migrations'
                  AND table_type = 'BASE TABLE'
            )",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exists, 1);

    let exists: i64 = c
        .query_first(
            "SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = 'migrations'
            )",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exists, 1);

    c.query_drop("USE switched_test").await.unwrap();
    let database: String = c.query_first("SELECT DATABASE()").await.unwrap().unwrap();
    assert_eq!(database, "switched_test");
    let schemas: Vec<String> = c.query("SHOW DATABASES").await.unwrap();
    assert_eq!(schemas, vec!["information_schema", "switched_test"]);
}

// Foreign-key introspection joins these two virtual relations and aggregates
// composite-key columns in ordinal order. The join must stay on the virtual
// relation path rather than loading either view as a stored table.
#[tokio::test]
async fn foreign_key_introspection_reports_actions() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop(
        "CREATE TABLE introspected_parent (
            id INT,
            tenant_id INT,
            PRIMARY KEY (id, tenant_id)
        )",
    )
    .await
    .unwrap();
    c.query_drop(
        "CREATE TABLE introspected_child (
            id INT PRIMARY KEY,
            parent_id INT,
            parent_tenant_id INT,
            CONSTRAINT fk_introspected_parent
                FOREIGN KEY (parent_id, parent_tenant_id)
                REFERENCES introspected_parent (id, tenant_id)
                ON UPDATE CASCADE ON DELETE SET NULL
        )",
    )
    .await
    .unwrap();

    let rows: Vec<(String, String, String, String, String, String, String)> = c
        .exec(
            "SELECT kc.constraint_name AS `name`, \
                    GROUP_CONCAT(kc.column_name ORDER BY kc.ordinal_position) AS `columns`, \
                    kc.referenced_table_schema AS `foreign_schema`, \
                    kc.referenced_table_name AS `foreign_table`, \
                    GROUP_CONCAT(kc.referenced_column_name ORDER BY kc.ordinal_position) \
                        AS `foreign_columns`, \
                    rc.update_rule AS `on_update`, rc.delete_rule AS `on_delete` \
             FROM information_schema.key_column_usage kc \
             JOIN information_schema.referential_constraints rc \
               ON kc.constraint_schema = rc.constraint_schema \
              AND kc.constraint_name = rc.constraint_name \
             WHERE kc.table_schema = 'elyra' \
               AND kc.table_name = 'introspected_child' \
               AND kc.referenced_table_name IS NOT NULL \
             GROUP BY kc.constraint_name, kc.referenced_table_schema, \
                      kc.referenced_table_name, rc.update_rule, rc.delete_rule",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(
            "fk_introspected_parent".into(),
            "parent_id,parent_tenant_id".into(),
            "elyra".into(),
            "introspected_parent".into(),
            "id,tenant_id".into(),
            "CASCADE".into(),
            "SET NULL".into(),
        )]
    );

    c.query_drop("CREATE TABLE default_action_parent (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop(
        "CREATE TABLE default_action_child (
            id INT PRIMARY KEY,
            parent_id INT,
            CONSTRAINT fk_default_action
                FOREIGN KEY (parent_id) REFERENCES default_action_parent (id)
        )",
    )
    .await
    .unwrap();
    let actions: (String, String) = c
        .query_first(
            "SELECT update_rule, delete_rule \
             FROM information_schema.referential_constraints \
             WHERE constraint_name = 'fk_default_action'",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actions, ("NO ACTION".into(), "NO ACTION".into()));

    let error = c
        .query_drop(
            "SELECT 1 FROM information_schema.key_column_usage kc \
             JOIN information_schema.missing_constraints mc \
               ON kc.constraint_name = mc.constraint_name LIMIT 1",
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("missing_constraints"), "{error}");
    assert!(
        !error.contains("no such table: key_column_usage"),
        "{error}"
    );
}

#[tokio::test]
async fn authentication_native_password() {
    let srv = TestServer::start_with_auth("root", "s3cret").await;

    // correct credentials connect and query
    let mut c = srv.conn_as("root", "s3cret").await;
    let one: i64 = c.query_first("SELECT 1").await.unwrap().unwrap();
    assert_eq!(one, 1);
    drop(c);

    // wrong password is rejected
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(srv.port)
        .user(Some("root"))
        .pass(Some("wrong"))
        .prefer_socket(false);
    let res = mysql_async::Conn::new(opts).await;
    assert!(res.is_err(), "expected auth failure for wrong password");
}

/// Aggregation invariants over pseudo-random data (deterministic seed):
/// GROUP BY results match a Rust-computed reference, and are independent of the
/// row insertion order. Guards the aggregation paths (streaming, columnar,
/// spilling) against order-dependence and arithmetic drift. [ESQL-7]
#[tokio::test]
async fn aggregation_invariants_random() {
    use std::collections::BTreeMap;

    // Deterministic LCG so failures reproduce.
    let mut seed: u64 = 0x1234_5678_9abc_def0;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as i64
    };

    // Generate rows: (id, g in 0..20, v in 0..1000).
    let n = 3000;
    let mut rows: Vec<(i64, i64, i64)> = (0..n)
        .map(|i| (i, next().rem_euclid(20), next().rem_euclid(1000)))
        .collect();

    // Reference aggregation in Rust.
    let mut ref_cnt: BTreeMap<i64, i64> = BTreeMap::new();
    let mut ref_sum: BTreeMap<i64, i64> = BTreeMap::new();
    let mut ref_min: BTreeMap<i64, i64> = BTreeMap::new();
    let mut ref_max: BTreeMap<i64, i64> = BTreeMap::new();
    for &(_, g, v) in &rows {
        *ref_cnt.entry(g).or_insert(0) += 1;
        *ref_sum.entry(g).or_insert(0) += v;
        let e = ref_min.entry(g).or_insert(v);
        *e = (*e).min(v);
        let e = ref_max.entry(g).or_insert(v);
        *e = (*e).max(v);
    }
    let expected: Vec<(i64, i64, i64, i64, i64)> = ref_cnt
        .keys()
        .map(|&g| (g, ref_cnt[&g], ref_sum[&g], ref_min[&g], ref_max[&g]))
        .collect();

    // Run the same aggregation with two different insertion orders; both must
    // equal the reference (order-independence).
    for pass in 0..2 {
        if pass == 1 {
            // reverse the insertion order
            rows.reverse();
        }
        let srv = TestServer::start().await;
        let mut c = srv.conn().await;
        c.query_drop("CREATE TABLE m (id INT PRIMARY KEY, g INT, v INT)")
            .await
            .unwrap();
        for chunk in rows.chunks(500) {
            let vals: Vec<String> = chunk
                .iter()
                .map(|(id, g, v)| format!("({id},{g},{v})"))
                .collect();
            c.query_drop(format!(
                "INSERT INTO m (id, g, v) VALUES {}",
                vals.join(",")
            ))
            .await
            .unwrap();
        }
        let mut got: Vec<(i64, i64, i64, i64, i64)> = c
            .query("SELECT g, COUNT(*), SUM(v), MIN(v), MAX(v) FROM m GROUP BY g")
            .await
            .unwrap();
        got.sort();
        assert_eq!(got, expected, "aggregation mismatch on pass {pass}");
    }
}

/// ORDER BY produces a total order consistent with a Rust sort, over
/// pseudo-random data with ties. [ESQL-7]
#[tokio::test]
async fn order_by_total_order_random() {
    let mut seed: u64 = 0xdead_beef_0bad_f00d;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as i64
    };
    let n = 1500;
    let data: Vec<(i64, i64)> = (0..n).map(|i| (i, next().rem_euclid(50))).collect();

    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE o (id INT PRIMARY KEY, k INT)")
        .await
        .unwrap();
    for chunk in data.chunks(500) {
        let vals: Vec<String> = chunk.iter().map(|(id, k)| format!("({id},{k})")).collect();
        c.query_drop(format!("INSERT INTO o (id, k) VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }

    // ORDER BY k ASC, id ASC is a total order; compare to a Rust sort.
    let got: Vec<(i64, i64)> = c
        .query("SELECT id, k FROM o ORDER BY k ASC, id ASC")
        .await
        .unwrap();
    let mut expected = data.clone();
    expected.sort_by_key(|(id, k)| (*k, *id));
    assert_eq!(got, expected);
}

// Regression: a deeply-nested flat expression must NOT crash the server. Before
// the fix, a left-deep `1+1+1...` (or `WHERE id=1 OR id=1 OR ...`) chain overflowed
// the worker stack and aborted the whole process (all clients dropped). Now it is
// rejected as a normal SQL error and the server keeps serving.
#[tokio::test]
async fn deep_expression_does_not_crash_server() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t VALUES (1)").await.unwrap();

    // Arithmetic chain: expect an error, not a dropped connection.
    let arith = format!("SELECT 1{}", "+1".repeat(40000));
    assert!(
        c.query_drop(&arith).await.is_err(),
        "deep chain should be rejected, not accepted"
    );

    // Every deep-AST shape must be rejected, not crash: boolean chain, JSON arrow
    // chain, and a token-balanced subscript chain (each on a fresh connection,
    // since a prior error may poison the current one — the point is the *server*
    // stays alive).
    for payload in [
        format!("SELECT * FROM t WHERE {}", vec!["id=1"; 40000].join(" OR ")),
        format!("SELECT '{{}}' {}", "-> '$' ".repeat(40000)),
        format!("SELECT id{} FROM t", "[0]".repeat(40000)),
    ] {
        let mut cn = srv.conn().await;
        assert!(cn.query_drop(&payload).await.is_err());
    }

    // Definitive proof the server survived both: a normal query on a new
    // connection still works.
    let mut c3 = srv.conn().await;
    let n: i64 = c3
        .query_first("SELECT COUNT(*) FROM t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 1);

    // And a legitimately large (but shallow) query is unaffected.
    let in_list = format!(
        "SELECT id FROM t WHERE id IN ({})",
        (0..6000)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let got: Vec<i64> = c3.query(&in_list).await.unwrap();
    assert_eq!(got, vec![1]);
}

// Regression for #15: integer arithmetic must not silently saturate/wrap, `% 0`
// must be NULL, and DOUBLE overflow must be NULL (MySQL semantics), instead of
// returning silently-wrong values.
#[tokio::test]
async fn integer_overflow_and_division_semantics() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    // Signed 64-bit overflow raises an out-of-range error (not saturate/wrap).
    for sql in [
        "SELECT 9223372036854775807 + 1",
        "SELECT 9223372036854775807 * 9223372036854775807",
        "SELECT 9223372036854775807 - (-1)",
        "SELECT -(-9223372036854775808)",
    ] {
        assert!(
            c.query_drop(sql).await.is_err(),
            "expected out-of-range error for `{sql}`"
        );
    }

    // Modulo/division by zero is NULL (the row exists, the value is NULL).
    for sql in ["SELECT 1 % 0", "SELECT MOD(1, 0)", "SELECT 1 / 0"] {
        let v: Option<Option<i64>> = c.query_first(sql).await.unwrap();
        assert_eq!(v, Some(None), "`{sql}` should be NULL");
    }

    // DOUBLE overflow is NULL, not +inf.
    let v: Option<Option<f64>> = c.query_first("SELECT POW(10,308) * 10").await.unwrap();
    assert_eq!(v, Some(None), "double overflow should be NULL");

    // Exact large integer arithmetic (no f64 precision loss) still works.
    let n: i64 = c
        .query_first("SELECT 9223372036854775806 + 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 9223372036854775807);
    let p: i64 = c
        .query_first("SELECT 1000000000 * 1000000000")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p, 1_000_000_000_000_000_000);
    let m: i64 = c.query_first("SELECT 7 % 3").await.unwrap().unwrap();
    assert_eq!(m, 1);

    // A computed write that overflows must error, not store a saturated value.
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO t VALUES (1, 9223372036854775807)")
        .await
        .unwrap();
    assert!(
        c.query_drop("UPDATE t SET v = v + 1 WHERE id = 1")
            .await
            .is_err(),
        "overflowing UPDATE must error"
    );
    let still: i64 = c
        .query_first("SELECT v FROM t WHERE id = 1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        still, 9223372036854775807,
        "value must be unchanged after the failed UPDATE"
    );
}

// Regression for the MySQL-semantics differential audit (ESQL-15): NULL
// propagation and 3VL, math domain -> NULL, byte-length, substring(0), the added
// ISNULL/STRCMP, integer CAST rounding/unsigned-wrap, invalid-date CAST -> NULL,
// and date+interval preserving the DATE type.
#[tokio::test]
async fn mysql_semantics_matches() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    // NULL arithmetic is NULL, not an error.
    let v: Option<Option<i64>> = c.query_first("SELECT NULL + 1").await.unwrap();
    assert_eq!(v, Some(None));
    // Three-valued logic: NULL AND 1 = NULL, NULL AND 0 = 0.
    let v: Option<Option<i64>> = c.query_first("SELECT NULL AND 1").await.unwrap();
    assert_eq!(v, Some(None));
    let v: i64 = c.query_first("SELECT NULL AND 0").await.unwrap().unwrap();
    assert_eq!(v, 0);
    // Math out-of-domain -> NULL, not NaN/inf.
    for sql in ["SELECT SQRT(-1)", "SELECT LN(0)", "SELECT LN(-1)"] {
        let v: Option<Option<f64>> = c.query_first(sql).await.unwrap();
        assert_eq!(v, Some(None), "{sql}");
    }
    // LENGTH is bytes; CHAR_LENGTH is characters.
    let n: i64 = c
        .query_first("SELECT LENGTH('héllo')")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 6);
    let n: i64 = c
        .query_first("SELECT CHAR_LENGTH('héllo')")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 5);
    // SUBSTRING position 0 -> empty string.
    let s: String = c
        .query_first("SELECT SUBSTRING('hello', 0)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s, "");
    // ISNULL / STRCMP.
    let n: i64 = c.query_first("SELECT ISNULL(NULL)").await.unwrap().unwrap();
    assert_eq!(n, 1);
    let n: i64 = c
        .query_first("SELECT STRCMP('a', 'b')")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, -1);
    // Integer CAST rounds (not truncates); UNSIGNED wraps.
    let n: i64 = c
        .query_first("SELECT CAST(3.7 AS SIGNED)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 4);
    let n: i64 = c
        .query_first("SELECT CAST(-3.7 AS SIGNED)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, -4);
    let u: u64 = c
        .query_first("SELECT CAST(-1 AS UNSIGNED)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(u, u64::MAX);
    let n: i64 = c
        .query_first("SELECT CAST('12abc' AS SIGNED)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 12);
    // Invalid date -> NULL (not rolled over).
    let v: Option<Option<String>> = c
        .query_first("SELECT CAST('2024-02-30' AS DATE)")
        .await
        .unwrap();
    assert_eq!(v, Some(None));
    // Adding a day/month interval to a date-shaped string yields a DATE.
    let s: String = c
        .query_first("SELECT DATE_ADD('2024-01-31', INTERVAL 1 MONTH)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s, "2024-02-29");
}

#[tokio::test]
async fn decimal_scale_reduction_rounds_half_away_from_zero() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE decimal_values (id INT PRIMARY KEY, amount DECIMAL(8,2))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO decimal_values VALUES (1, 8.876543211), (2, -8.876543211)")
        .await
        .unwrap();

    let rows: Vec<(i64, String)> = c
        .query("SELECT id, amount FROM decimal_values ORDER BY id")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, "8.88".into()), (2, "-8.88".into())]);
}

// Regression for the second differential batch (ESQL-15): DIV integer division,
// IN-list three-valued logic, and the added BIT_COUNT/TO_DAYS/INSERT/CONV.
#[tokio::test]
async fn mysql_semantics_batch2() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    // DIV truncates toward zero; DIV 0 -> NULL.
    let n: i64 = c.query_first("SELECT 7 DIV 2").await.unwrap().unwrap();
    assert_eq!(n, 3);
    let n: i64 = c.query_first("SELECT -7 DIV 2").await.unwrap().unwrap();
    assert_eq!(n, -3);
    let v: Option<Option<i64>> = c.query_first("SELECT 7 DIV 0").await.unwrap();
    assert_eq!(v, Some(None));

    // IN-list 3VL: a non-matching value with a NULL in the list -> NULL.
    let v: Option<Option<i64>> = c.query_first("SELECT 1 IN (NULL, 2)").await.unwrap();
    assert_eq!(v, Some(None));
    // A match still wins over the NULL.
    let n: i64 = c
        .query_first("SELECT 2 IN (NULL, 2)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 1);
    // No NULL, no match -> FALSE.
    let n: i64 = c.query_first("SELECT 3 IN (1, 2)").await.unwrap().unwrap();
    assert_eq!(n, 0);

    // Added functions.
    let n: i64 = c.query_first("SELECT BIT_COUNT(7)").await.unwrap().unwrap();
    assert_eq!(n, 3);
    let n: i64 = c
        .query_first("SELECT TO_DAYS('2024-01-01')")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 739251);
    let s: String = c
        .query_first("SELECT INSERT('abcd', 2, 1, 'XY')")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s, "aXYcd");
    let s: String = c
        .query_first("SELECT CONV('ff', 16, 10)")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s, "255");
}

// Regression for the third differential batch (ESQL-15): `!` prefix via a
// precedence-preserving rewrite, NOT/BETWEEN three-valued logic, the added
// ORD/BIN/OCT/CRC32, and unsigned bit aggregates.
#[tokio::test]
async fn mysql_semantics_batch3() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;

    // `!` logical NOT with preserved precedence.
    let n: i64 = c.query_first("SELECT !0").await.unwrap().unwrap();
    assert_eq!(n, 1);
    let n: i64 = c.query_first("SELECT !5").await.unwrap().unwrap();
    assert_eq!(n, 0);
    let n: i64 = c.query_first("SELECT !(1 = 1)").await.unwrap().unwrap();
    assert_eq!(n, 0);
    let n: i64 = c.query_first("SELECT !0 = 0").await.unwrap().unwrap(); // (!0)=0 -> 1=0 -> 0
    assert_eq!(n, 0);
    // `!=` must still parse as not-equal.
    let n: i64 = c.query_first("SELECT 1 != 2").await.unwrap().unwrap();
    assert_eq!(n, 1);

    // Three-valued logic: NOT NULL, !NULL, and BETWEEN with a NULL bound -> NULL.
    for sql in [
        "SELECT NOT NULL",
        "SELECT !NULL",
        "SELECT 1 BETWEEN NULL AND 5",
    ] {
        let v: Option<Option<i64>> = c.query_first(sql).await.unwrap();
        assert_eq!(v, Some(None), "{sql}");
    }
    // But a determinable BETWEEN with a NULL bound is FALSE, not NULL.
    let n: i64 = c
        .query_first("SELECT 10 BETWEEN NULL AND 5")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 0);

    // Added functions.
    let n: i64 = c.query_first("SELECT ORD('A')").await.unwrap().unwrap();
    assert_eq!(n, 65);
    let s: String = c.query_first("SELECT BIN(5)").await.unwrap().unwrap();
    assert_eq!(s, "101");
    let s: String = c.query_first("SELECT OCT(8)").await.unwrap().unwrap();
    assert_eq!(s, "10");
    let n: i64 = c
        .query_first("SELECT CRC32('MySQL')")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 3259397556);

    // Unsigned bit aggregate.
    c.query_drop("CREATE TABLE b (id INT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO b VALUES (1, -1)").await.unwrap();
    let u: u64 = c
        .query_first("SELECT BIT_OR(v) FROM b")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(u, u64::MAX);
}

// Fine-grained privilege enforcement (ESQL-16): within the write tier, a user
// may perform only the *specific* action(s) granted (INSERT vs UPDATE vs DELETE),
// per table, and REVOKE removes only the named privilege.
#[tokio::test]
async fn fine_grained_privileges() {
    let srv = TestServer::start_with_auth("root", "rootpw").await;
    let mut a = srv.conn_as("root", "rootpw").await;
    a.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    a.query_drop("INSERT INTO t VALUES (1,10),(2,20)")
        .await
        .unwrap();
    a.query_drop("CREATE USER 'ins' IDENTIFIED BY 'passw0rd'")
        .await
        .unwrap();
    a.query_drop("GRANT SELECT, INSERT ON t TO 'ins'")
        .await
        .unwrap();
    a.query_drop("CREATE USER 'del' IDENTIFIED BY 'passw0rd'")
        .await
        .unwrap();
    a.query_drop("GRANT SELECT, UPDATE, DELETE ON t TO 'del'")
        .await
        .unwrap();

    // Insert-only user: INSERT + SELECT allowed; UPDATE/DELETE denied.
    let mut ins = srv.conn_as("ins", "passw0rd").await;
    ins.query_drop("INSERT INTO t VALUES (3,30)").await.unwrap();
    assert!(ins.query_drop("UPDATE t SET v=1 WHERE id=1").await.is_err());
    assert!(ins.query_drop("DELETE FROM t WHERE id=2").await.is_err());
    let n: i64 = ins
        .query_first("SELECT COUNT(*) FROM t")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(n, 3);

    // Update/delete user: those allowed; INSERT denied.
    let mut del = srv.conn_as("del", "passw0rd").await;
    del.query_drop("UPDATE t SET v=99 WHERE id=1")
        .await
        .unwrap();
    del.query_drop("DELETE FROM t WHERE id=2").await.unwrap();
    assert!(del.query_drop("INSERT INTO t VALUES (4,40)").await.is_err());

    // REVOKE UPDATE leaves DELETE intact.
    a.query_drop("REVOKE UPDATE ON t FROM 'del'").await.unwrap();
    let mut del2 = srv.conn_as("del", "passw0rd").await;
    assert!(del2
        .query_drop("UPDATE t SET v=1 WHERE id=1")
        .await
        .is_err());
    del2.query_drop("DELETE FROM t WHERE id=1").await.unwrap();
}

// Faceted search (ESQL-17): FACET(col[, n]) returns a value->count JSON object
// over the matched rows, computed in the same single-pass aggregation, and
// composes with WHERE / MATCH / GROUP BY.
#[tokio::test]
async fn faceted_search() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE docs (id INT PRIMARY KEY, title TEXT, category VARCHAR(16), brand VARCHAR(16), price INT)")
        .await
        .unwrap();
    for (id, title, cat, brand, price) in [
        (1, "rust database engine", "db", "acme", 100),
        (2, "rust web framework", "web", "acme", 50),
        (3, "python database tool", "db", "globex", 80),
        (4, "rust systems programming", "sys", "acme", 120),
        (5, "rust database driver", "db", "initech", 60),
        (6, "go database", "db", "globex", 90),
    ] {
        c.query_drop(format!(
            "INSERT INTO docs VALUES ({id},'{title}','{cat}','{brand}',{price})"
        ))
        .await
        .unwrap();
    }

    // Multi-facet + total in one pass (ordered count desc, then value asc).
    let (cats, brands, total): (String, String, i64) = c
        .query_first("SELECT FACET(category), FACET(brand), COUNT(*) FROM docs")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cats, r#"{"db": 4, "sys": 1, "web": 1}"#);
    assert_eq!(brands, r#"{"acme": 3, "globex": 2, "initech": 1}"#);
    assert_eq!(total, 6);

    // Top-N cap.
    let top2: String = c
        .query_first("SELECT FACET(brand, 2) FROM docs")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(top2, r#"{"acme": 3, "globex": 2}"#);

    // Composes with a WHERE filter.
    let filtered: String = c
        .query_first("SELECT FACET(category) FROM docs WHERE price >= 80")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(filtered, r#"{"db": 3, "sys": 1}"#);

    // Composes with a full-text MATCH.
    let searched: String = c
        .query_first("SELECT FACET(brand) FROM docs WHERE MATCH(title) AGAINST('python')")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(searched, r#"{"globex": 1}"#);

    // Composes with GROUP BY (facet within each group).
    let per_brand: Vec<(String, String)> = c
        .query("SELECT brand, FACET(category) FROM docs GROUP BY brand ORDER BY brand")
        .await
        .unwrap();
    assert_eq!(per_brand[1], ("globex".into(), r#"{"db": 2}"#.into()));
}

// RIGHT-join streaming (ESQL-6): a two-table RIGHT join is streamed (rewritten to
// B LEFT JOIN A with the output columns reordered back to A, B), for both the
// ORDER BY and GROUP BY shapes, keeping every right-side row.
#[tokio::test]
async fn right_join_streams_correctly() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE fact (id INT PRIMARY KEY, uid INT, amount INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE dim (uid INT PRIMARY KEY, name VARCHAR(16))")
        .await
        .unwrap();
    c.query_drop("INSERT INTO fact VALUES (1,10,100),(2,10,50),(3,20,80)")
        .await
        .unwrap();
    // uid 30 has no matching fact row -> RIGHT join must keep it with NULLs.
    c.query_drop("INSERT INTO dim VALUES (10,'ten'),(20,'twenty'),(30,'thirty')")
        .await
        .unwrap();

    // ORDER BY path: every dim row kept; the unmatched one has NULL fact columns.
    let rows: Vec<(Option<i64>, Option<i64>, i64, String)> = c
        .query(
            "SELECT fact.id, fact.amount, dim.uid, dim.name \
             FROM fact RIGHT JOIN dim ON fact.uid = dim.uid \
             ORDER BY dim.uid, fact.id",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (Some(1), Some(100), 10, "ten".into()),
            (Some(2), Some(50), 10, "ten".into()),
            (Some(3), Some(80), 20, "twenty".into()),
            (None, None, 30, "thirty".into()),
        ]
    );

    // SELECT * column order must be (fact.id, fact.uid, fact.amount, dim.uid,
    // dim.name) as MySQL lists it (fact.* before dim.*). Read by position to keep
    // the assertion type simple.
    let star: Vec<mysql_async::Row> = c
        .query("SELECT * FROM fact RIGHT JOIN dim ON fact.uid = dim.uid ORDER BY dim.uid")
        .await
        .unwrap();
    let first = &star[0];
    assert_eq!(first.get::<i64, _>(0).unwrap(), 1); // fact.id
    assert_eq!(first.get::<i64, _>(2).unwrap(), 100); // fact.amount
    assert_eq!(first.get::<i64, _>(3).unwrap(), 10); // dim.uid
    assert_eq!(first.get::<String, _>(4).unwrap(), "ten"); // dim.name
    let last = &star[3];
    assert_eq!(last.get::<Option<i64>, _>(0).unwrap(), None); // fact.id NULL
    assert_eq!(last.get::<i64, _>(3).unwrap(), 30); // dim.uid
    assert_eq!(last.get::<String, _>(4).unwrap(), "thirty");

    // GROUP BY path: aggregate over the right-preserved rows.
    let g: Vec<(String, i64, Option<i64>)> = c
        .query(
            "SELECT dim.name, COUNT(fact.id) AS n, SUM(fact.amount) AS s \
             FROM fact RIGHT JOIN dim ON fact.uid = dim.uid \
             GROUP BY dim.name ORDER BY dim.name",
        )
        .await
        .unwrap();
    assert_eq!(
        g,
        vec![
            ("ten".into(), 2, Some(150)),
            ("thirty".into(), 0, None),
            ("twenty".into(), 1, Some(80)),
        ]
    );
}

// Percentile aggregate (ESQL-18): PERCENTILE/QUANTILE/MEDIAN via percentile_cont
// (linear interpolation), for metrics p50/p95/p99. Composes with GROUP BY.
#[tokio::test]
async fn percentile_aggregate() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE req (id INT PRIMARY KEY, service VARCHAR(8), latency INT)")
        .await
        .unwrap();
    // service 'a' = 1..=100, service 'b' = {10,20,30}.
    for i in 1..=100 {
        c.query_drop(format!("INSERT INTO req VALUES ({i}, 'a', {i})"))
            .await
            .unwrap();
    }
    for (i, v) in [(201, 10), (202, 20), (203, 30)] {
        c.query_drop(format!("INSERT INTO req VALUES ({i}, 'b', {v})"))
            .await
            .unwrap();
    }

    let p50: f64 = c
        .query_first("SELECT PERCENTILE(latency, 0.5) FROM req WHERE service='a'")
        .await
        .unwrap()
        .unwrap();
    assert!((p50 - 50.5).abs() < 1e-9, "p50 = {p50}");
    let p95: f64 = c
        .query_first("SELECT PERCENTILE(latency, 0.95) FROM req WHERE service='a'")
        .await
        .unwrap()
        .unwrap();
    assert!((p95 - 95.05).abs() < 1e-9, "p95 = {p95}");
    let median: f64 = c
        .query_first("SELECT MEDIAN(latency) FROM req WHERE service='a'")
        .await
        .unwrap()
        .unwrap();
    assert!((median - 50.5).abs() < 1e-9);
    let q90: f64 = c
        .query_first("SELECT QUANTILE(latency, 0.9) FROM req WHERE service='a'")
        .await
        .unwrap()
        .unwrap();
    assert!((q90 - 90.1).abs() < 1e-9, "q90 = {q90}");

    // GROUP BY per service.
    let g: Vec<(String, f64)> = c
        .query(
            "SELECT service, PERCENTILE(latency, 0.95) FROM req GROUP BY service ORDER BY service",
        )
        .await
        .unwrap();
    assert!((g[0].1 - 95.05).abs() < 1e-9); // a
    assert!((g[1].1 - 29.0).abs() < 1e-9); // b

    // Empty group -> NULL.
    let empty: Option<Option<f64>> = c
        .query_first("SELECT PERCENTILE(latency, 0.95) FROM req WHERE 1=0")
        .await
        .unwrap();
    assert_eq!(empty, Some(None));
}

// GROUP BY expression (ESQL-19): group by an arbitrary expression (time-bucketing,
// arithmetic), not just a plain column, so observability time-bucket queries work.
#[tokio::test]
async fn group_by_expression() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE logs (id INT PRIMARY KEY, ts DATETIME, status INT, latency INT)")
        .await
        .unwrap();
    // 120 rows across two minutes; every 10th is a 500.
    for i in 0..120 {
        let ts = format!("2026-07-17 10:{:02}:{:02}", i / 60, i % 60);
        let status = if i % 10 == 0 { 500 } else { 200 };
        c.query_drop(format!(
            "INSERT INTO logs VALUES ({i}, '{ts}', {status}, {})",
            i % 50
        ))
        .await
        .unwrap();
    }

    // Time-bucket by minute: reqs, errors, and p95 per minute.
    let rows: Vec<(String, i64, i64)> = c
        .query(
            "SELECT DATE_FORMAT(ts, '%Y-%m-%d %H:%i:00') AS m, COUNT(*), SUM(status >= 500) \
             FROM logs GROUP BY DATE_FORMAT(ts, '%Y-%m-%d %H:%i:00') ORDER BY m",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("2026-07-17 10:00:00".into(), 60, 6),
            ("2026-07-17 10:01:00".into(), 60, 6),
        ]
    );

    // With full-group enforcement disabled, MySQL permits ordering groups by a
    // representative source value that is not part of the returned projection.
    let rows: Vec<(String, i64)> = c
        .query(
            "SELECT DATE_FORMAT(ts, '%Y-%m-%d') AS day, COUNT(*)
             FROM logs GROUP BY day ORDER BY ts DESC",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![("2026-07-17".into(), 120)]);

    let rows: Vec<(String, i64)> = c
        .query(
            "SELECT DATE_FORMAT(ts, '%Y-%m-%d %H:%i:00') AS bucket, COUNT(*) \
             FROM logs GROUP BY bucket ORDER BY bucket",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("2026-07-17 10:00:00".into(), 60),
            ("2026-07-17 10:01:00".into(), 60),
        ]
    );

    // Arithmetic grouping (status class).
    let klass: Vec<(i64, i64)> = c
        .query("SELECT status DIV 100 AS k, COUNT(*) FROM logs GROUP BY status DIV 100 ORDER BY k")
        .await
        .unwrap();
    assert_eq!(klass, vec![(2, 108), (5, 12)]);

    // Group expression composes with a percentile aggregate (ordering by the
    // projected bucket alias, as observability queries do).
    let p95: Vec<(i64, f64)> = c
        .query("SELECT status DIV 100 AS k, PERCENTILE(latency, 0.95) FROM logs GROUP BY status DIV 100 ORDER BY k")
        .await
        .unwrap();
    assert_eq!(p95.len(), 2);
    assert!(p95[0].1 > 0.0);
}

#[tokio::test]
async fn grouped_wildcard_projects_a_representative_row() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop(
        "CREATE TABLE events (id INT PRIMARY KEY, category VARCHAR(8), amount INT, ended_at DATETIME)",
    )
    .await
    .unwrap();
    c.query_drop(
        "INSERT INTO events VALUES
         (1, 'a', 10, NULL), (2, 'a', 20, NULL), (3, 'b', 30, NULL)",
    )
    .await
    .unwrap();

    let rows: Vec<(i64, String, i64, Option<String>)> = c
        .query(
            "SELECT * FROM events
             WHERE ended_at IS NULL
             GROUP BY category
             ORDER BY category",
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert!(
        matches!(&rows[0], (1, category, 10, None) if category == "a")
            || matches!(&rows[0], (2, category, 20, None) if category == "a")
    );
    assert!(matches!(&rows[1], (3, category, 30, None) if category == "b"));
}

// Indexed ORDER BY ... LIMIT (ESQL-20): reverse primary-key scan and secondary-
// index ordered scan return correct top-N without a full sort. Correctness is
// checked against a locally computed expectation.
#[tokio::test]
async fn indexed_order_by_limit() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, revenue INT NOT NULL)")
        .await
        .unwrap();
    // Deterministic pseudo-random revenue values.
    let mut shadow: Vec<(i64, i64)> = Vec::new();
    let mut vals = String::new();
    let mut seed: u64 = 0x1234_5678;
    for id in 1..=2000i64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let rev = ((seed >> 20) % 1_000_000) as i64;
        shadow.push((id, rev));
        if !vals.is_empty() {
            vals.push(',');
        }
        vals.push_str(&format!("({id},{rev})"));
    }
    c.query_drop(format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
    c.query_drop("CREATE INDEX ix_rev ON t (revenue)")
        .await
        .unwrap();

    // Reverse PK: ORDER BY id DESC LIMIT 40.
    let got: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY id DESC LIMIT 40")
        .await
        .unwrap();
    let exp: Vec<i64> = (1961..=2000).rev().collect();
    assert_eq!(got, exp, "reverse PK top-N");

    // Secondary index ASC: ORDER BY revenue LIMIT 40 (compare the ordered values).
    let got: Vec<(i64, i64)> = c
        .query("SELECT id, revenue FROM t ORDER BY revenue LIMIT 40")
        .await
        .unwrap();
    let mut by_rev = shadow.clone();
    by_rev.sort_by_key(|&(id, rev)| (rev, id));
    let exp_vals: Vec<i64> = by_rev.iter().take(40).map(|&(_, r)| r).collect();
    let got_vals: Vec<i64> = got.iter().map(|&(_, r)| r).collect();
    assert_eq!(got_vals, exp_vals, "secondary index ASC top-N values");

    // Secondary index DESC.
    let got: Vec<(i64, i64)> = c
        .query("SELECT id, revenue FROM t ORDER BY revenue DESC LIMIT 40")
        .await
        .unwrap();
    let mut by_rev_d = shadow.clone();
    by_rev_d.sort_by_key(|&(id, rev)| (std::cmp::Reverse(rev), id));
    let exp_vals: Vec<i64> = by_rev_d.iter().take(40).map(|&(_, r)| r).collect();
    let got_vals: Vec<i64> = got.iter().map(|&(_, r)| r).collect();
    assert_eq!(got_vals, exp_vals, "secondary index DESC top-N values");

    // OFFSET is honoured on the ordered walk.
    let got: Vec<i64> = c
        .query("SELECT revenue FROM t ORDER BY revenue LIMIT 10 OFFSET 20")
        .await
        .unwrap();
    let exp_vals: Vec<i64> = by_rev.iter().skip(20).take(10).map(|&(_, r)| r).collect();
    assert_eq!(got, exp_vals, "ordered walk with OFFSET");

    // Deep OFFSET: the index-level skip steps over pre-offset rows without a row
    // lookup, and must return exactly the same page as a full sort.
    let got: Vec<i64> = c
        .query("SELECT revenue FROM t ORDER BY revenue LIMIT 5 OFFSET 1900")
        .await
        .unwrap();
    let exp_vals: Vec<i64> = by_rev.iter().skip(1900).take(5).map(|&(_, r)| r).collect();
    assert_eq!(got, exp_vals, "secondary index deep OFFSET");

    // Deep OFFSET on the reverse PK walk.
    let got: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY id DESC LIMIT 5 OFFSET 1900")
        .await
        .unwrap();
    let exp: Vec<i64> = (1..=2000).rev().skip(1900).take(5).collect();
    assert_eq!(got, exp, "reverse PK deep OFFSET");
}

// Filtered indexed ORDER BY ... LIMIT (ESQL-21): a residual WHERE is applied
// during the ordered walk (fast path), and a very selective residual falls back
// to the sorter -- both must be correct.
#[tokio::test]
async fn filtered_indexed_order_by_limit() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, revenue INT NOT NULL, active INT NOT NULL)")
        .await
        .unwrap();
    let mut shadow: Vec<(i64, i64, i64)> = Vec::new();
    let mut vals = String::new();
    let mut seed: u64 = 0xDEAD_BEEF;
    for id in 1..=4000i64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let rev = ((seed >> 20) % 1_000_000) as i64;
        let active = ((seed >> 5) & 1) as i64; // ~50%
        shadow.push((id, rev, active));
        if !vals.is_empty() {
            vals.push(',');
        }
        vals.push_str(&format!("({id},{rev},{active})"));
    }
    c.query_drop(format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
    c.query_drop("CREATE INDEX ix_rev ON t (revenue)")
        .await
        .unwrap();

    // Non-selective residual (active = 1, ~50%) -> served by the ordered walk.
    let got: Vec<i64> = c
        .query("SELECT revenue FROM t WHERE active = 1 ORDER BY revenue DESC LIMIT 40")
        .await
        .unwrap();
    let mut act: Vec<(i64, i64)> = shadow
        .iter()
        .filter(|&&(_, _, a)| a == 1)
        .map(|&(id, r, _)| (id, r))
        .collect();
    act.sort_by_key(|&(id, r)| (std::cmp::Reverse(r), id));
    let exp: Vec<i64> = act.iter().take(40).map(|&(_, r)| r).collect();
    assert_eq!(got, exp, "filtered ordered walk (non-selective, fast path)");

    // Filtered + OFFSET.
    let got: Vec<i64> = c
        .query("SELECT revenue FROM t WHERE active = 1 ORDER BY revenue DESC LIMIT 10 OFFSET 15")
        .await
        .unwrap();
    let exp: Vec<i64> = act.iter().skip(15).take(10).map(|&(_, r)| r).collect();
    assert_eq!(got, exp, "filtered ordered walk with OFFSET");

    // Very selective residual (few matches) -> budget bail -> sorter fallback,
    // still correct. Force an immediate bail via a tiny budget.
    std::env::set_var("ELYRASQL_ORDER_SCAN_BUDGET", "1");
    let got: Vec<i64> = c
        .query("SELECT id FROM t WHERE revenue < 2000 ORDER BY revenue LIMIT 40")
        .await
        .unwrap();
    std::env::remove_var("ELYRASQL_ORDER_SCAN_BUDGET");
    let mut small: Vec<(i64, i64)> = shadow
        .iter()
        .filter(|&&(_, r, _)| r < 2000)
        .map(|&(id, r, _)| (id, r))
        .collect();
    small.sort_by_key(|&(id, r)| (r, id));
    let exp: Vec<i64> = small.iter().take(40).map(|&(id, _)| id).collect();
    assert_eq!(
        got, exp,
        "selective residual falls back to sorter, still correct"
    );
}

// Nullable indexed ORDER BY ... LIMIT (ESQL-22): the index omits NULL-keyed rows,
// so the NULL block is spliced in -- last for DESC, first for ASC (MySQL order).
#[tokio::test]
async fn nullable_indexed_order_by_limit() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, revenue INT)")
        .await
        .unwrap();
    // ~1 in 8 rows has a NULL revenue.
    let mut shadow: Vec<(i64, Option<i64>)> = Vec::new();
    let mut vals = String::new();
    let mut seed: u64 = 0x51ED;
    for id in 1..=3000i64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let is_null = (seed >> 3).is_multiple_of(8);
        let rev = if is_null {
            None
        } else {
            Some(((seed >> 20) % 1_000_000) as i64)
        };
        shadow.push((id, rev));
        if !vals.is_empty() {
            vals.push(',');
        }
        match rev {
            Some(v) => vals.push_str(&format!("({id},{v})")),
            None => vals.push_str(&format!("({id},NULL)")),
        }
    }
    c.query_drop(format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
    c.query_drop("CREATE INDEX idx_revenue ON t (revenue)")
        .await
        .unwrap();

    // DESC: non-NULL descending, NULLs last. Compare the revenue sequence
    // (NULL-keyed rows are ties, so only the value order is well-defined).
    let got: Vec<Option<i64>> = c
        .query_map(
            "SELECT revenue FROM t ORDER BY revenue DESC LIMIT 40",
            |rev: Option<i64>| rev,
        )
        .await
        .unwrap();
    let mut desc = shadow.clone();
    desc.sort_by_key(|&(id, r)| (r.is_none(), std::cmp::Reverse(r), id));
    let exp: Vec<Option<i64>> = desc.iter().take(40).map(|&(_, r)| r).collect();
    assert_eq!(got, exp, "DESC nullable: non-NULL desc then NULLs");

    // ASC: NULLs first, then non-NULL ascending.
    let got: Vec<Option<i64>> = c
        .query_map(
            "SELECT revenue FROM t ORDER BY revenue LIMIT 40",
            |rev: Option<i64>| rev,
        )
        .await
        .unwrap();
    let mut asc = shadow.clone();
    asc.sort_by_key(|&(id, r)| (r.is_some(), r, id));
    let exp: Vec<Option<i64>> = asc.iter().take(40).map(|&(_, r)| r).collect();
    assert_eq!(got, exp, "ASC nullable: NULLs first then non-NULL asc");

    // ASC with OFFSET spanning the NULL/non-NULL boundary.
    let got: Vec<Option<i64>> = c
        .query_map(
            "SELECT revenue FROM t ORDER BY revenue LIMIT 20 OFFSET 30",
            |rev: Option<i64>| rev,
        )
        .await
        .unwrap();
    let exp: Vec<Option<i64>> = asc.iter().skip(30).take(20).map(|&(_, r)| r).collect();
    assert_eq!(got, exp, "ASC nullable with OFFSET");
}

// Compound ORDER BY with a PK tiebreaker (ESQL-25): a non-unique secondary index
// stores (value, clustered pk), so walking it also orders by the trailing PK --
// the stable-pagination pattern `ORDER BY <col> DESC, id DESC`.
#[tokio::test]
async fn compound_order_by_pk_tiebreaker() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, revenue INT)")
        .await
        .unwrap();
    // Low-cardinality revenue so the id tiebreaker actually decides order; ~1/16
    // rows NULL.
    let mut shadow: Vec<(i64, Option<i64>)> = Vec::new();
    let mut vals = String::new();
    let mut seed: u64 = 0xC0FFEE;
    for id in 1..=3000i64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let rev = if (seed >> 7).is_multiple_of(16) {
            None
        } else {
            Some(((seed >> 20) % 20) as i64)
        };
        shadow.push((id, rev));
        if !vals.is_empty() {
            vals.push(',');
        }
        match rev {
            Some(v) => vals.push_str(&format!("({id},{v})")),
            None => vals.push_str(&format!("({id},NULL)")),
        }
    }
    c.query_drop(format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
    c.query_drop("CREATE INDEX ix_rev ON t (revenue)")
        .await
        .unwrap();

    // DESC, id DESC: non-NULL by (revenue desc, id desc); NULLs last.
    let got: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY revenue DESC, id DESC LIMIT 40")
        .await
        .unwrap();
    let mut desc = shadow.clone();
    desc.sort_by_key(|&(id, r)| (r.is_none(), std::cmp::Reverse(r), std::cmp::Reverse(id)));
    let exp: Vec<i64> = desc.iter().take(40).map(|&(id, _)| id).collect();
    assert_eq!(got, exp, "compound DESC with id tiebreaker");

    // Same, deep-ish OFFSET.
    let got: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY revenue DESC, id DESC LIMIT 20 OFFSET 500")
        .await
        .unwrap();
    let exp: Vec<i64> = desc.iter().skip(500).take(20).map(|&(id, _)| id).collect();
    assert_eq!(got, exp, "compound DESC tiebreaker with OFFSET");
}

// NULL-indexed ordered walk (ESQL-24): a single-column index now stores NULL-keyed
// rows under the indexnull:: keyspace, so ORDER BY on a nullable column -- ASC or
// DESC, with a PK tiebreaker -- is a complete index walk (no data scan, no
// fallback), and stays correct across mutations.
#[tokio::test]
async fn null_indexed_order_walk() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY, revenue INT)")
        .await
        .unwrap();
    // ~1/8 NULL, low cardinality so the id tiebreaker decides ties.
    let mut shadow: std::collections::HashMap<i64, Option<i64>> = std::collections::HashMap::new();
    let mut vals = String::new();
    let mut seed: u64 = 0x24;
    for id in 1..=2500i64 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let rev = if (seed >> 5).is_multiple_of(8) {
            None
        } else {
            Some(((seed >> 20) % 30) as i64)
        };
        shadow.insert(id, rev);
        if !vals.is_empty() {
            vals.push(',');
        }
        match rev {
            Some(v) => vals.push_str(&format!("({id},{v})")),
            None => vals.push_str(&format!("({id},NULL)")),
        }
    }
    c.query_drop(format!("INSERT INTO t VALUES {vals}"))
        .await
        .unwrap();
    c.query_drop("CREATE INDEX idx_revenue ON t (revenue)")
        .await
        .unwrap();

    // Mutations must keep the NULL entries consistent.
    c.query_drop("UPDATE t SET revenue = NULL WHERE id <= 20")
        .await
        .unwrap();
    c.query_drop("UPDATE t SET revenue = 5 WHERE id BETWEEN 100 AND 110")
        .await
        .unwrap();
    c.query_drop("DELETE FROM t WHERE id BETWEEN 300 AND 350")
        .await
        .unwrap();
    for id in 1..=20 {
        shadow.insert(id, None);
    }
    for id in 100..=110 {
        shadow.insert(id, Some(5));
    }
    for id in 300..=350 {
        shadow.remove(&id);
    }

    let asc_key = |&(id, r): &(i64, Option<i64>)| (r.is_some(), r, id);
    let desc_key =
        |&(id, r): &(i64, Option<i64>)| (r.is_none(), std::cmp::Reverse(r), std::cmp::Reverse(id));
    let mut rows: Vec<(i64, Option<i64>)> = shadow.iter().map(|(&id, &r)| (id, r)).collect();

    // ASC, id ASC: NULLs first (ordered by id), then ascending.
    let got: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY revenue ASC, id ASC LIMIT 40")
        .await
        .unwrap();
    rows.sort_by_key(asc_key);
    let exp: Vec<i64> = rows.iter().take(40).map(|&(id, _)| id).collect();
    assert_eq!(got, exp, "nullable ASC with id tiebreaker (indexed NULLs)");

    // ASC with OFFSET spanning the NULL/non-NULL boundary.
    let got: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY revenue ASC, id ASC LIMIT 20 OFFSET 30")
        .await
        .unwrap();
    let exp: Vec<i64> = rows.iter().skip(30).take(20).map(|&(id, _)| id).collect();
    assert_eq!(got, exp, "nullable ASC OFFSET across boundary");

    // DESC, id DESC: descending, then NULLs last (ordered by id desc).
    let got: Vec<i64> = c
        .query("SELECT id FROM t ORDER BY revenue DESC, id DESC LIMIT 40")
        .await
        .unwrap();
    rows.sort_by_key(desc_key);
    let exp: Vec<i64> = rows.iter().take(40).map(|&(id, _)| id).collect();
    assert_eq!(got, exp, "nullable DESC with id tiebreaker (indexed NULLs)");
}

// N-table (3+) left-deep join streaming (ESQL-29): a chain of INNER/LEFT equi
// joins with ORDER BY / GROUP BY must stream (build_join_chain) and be correct.
// Locks in the capability so it cannot silently regress to the materialising path.
#[tokio::test]
async fn multi_table_join_streams_correctly() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE a (id INT PRIMARY KEY, bid INT, v INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE b (id INT PRIMARY KEY, cid INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE d (id INT PRIMARY KEY, region INT)")
        .await
        .unwrap();
    for i in 1..=200i64 {
        c.query_drop(format!(
            "INSERT INTO a VALUES ({i}, {}, {})",
            (i % 50) + 1,
            i * 10
        ))
        .await
        .unwrap();
    }
    for i in 1..=50i64 {
        c.query_drop(format!("INSERT INTO b VALUES ({i}, {})", (i % 10) + 1))
            .await
            .unwrap();
    }
    for i in 1..=10i64 {
        c.query_drop(format!("INSERT INTO d VALUES ({i}, {})", i))
            .await
            .unwrap();
    }

    // 3-table chain + ORDER BY + LIMIT (streaming_join_order over build_join_chain).
    let ids: Vec<i64> = c
        .query(
            "SELECT a.id FROM a JOIN b ON a.bid = b.id JOIN d ON b.cid = d.id \
             ORDER BY a.id DESC LIMIT 5",
        )
        .await
        .unwrap();
    assert_eq!(ids, vec![200, 199, 198, 197, 196]);

    // 3-table chain + GROUP BY (streaming_join_aggregate); every a-row joins.
    let total: i64 = c
        .query_first("SELECT SUM(a.v) FROM a JOIN b ON a.bid = b.id JOIN d ON b.cid = d.id")
        .await
        .unwrap()
        .unwrap();
    let expected: i64 = (1..=200).map(|i| i * 10).sum();
    assert_eq!(total, expected, "3-table INNER chain keeps all 200 a-rows");

    // 3-table LEFT chain keeps all driving rows.
    let cnt: i64 = c
        .query_first(
            "SELECT COUNT(*) FROM a LEFT JOIN b ON a.bid = b.id LEFT JOIN d ON b.cid = d.id",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cnt, 200);
}

// A query-supplied NTILE bucket count must cost O(rows), not O(buckets): before
// this was fixed, NTILE(1e12) spun a CPU core forever (and survived client
// disconnect), so a tiny query could take the whole server down. Also pins the
// MySQL-verified distribution.
#[tokio::test]
async fn ntile_is_bounded_by_row_count() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE nt (id INT PRIMARY KEY)")
        .await
        .unwrap();
    for i in 1..=10 {
        c.query_drop(format!("INSERT INTO nt VALUES ({i})"))
            .await
            .unwrap();
    }
    // Distributions verified against real MySQL 8.4.
    for (buckets, want) in [
        (3u64, vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 3]),
        (4, vec![1, 1, 1, 2, 2, 2, 3, 3, 4, 4]),
        (7, vec![1, 1, 2, 2, 3, 3, 4, 5, 6, 7]),
        (20, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
    ] {
        let got: Vec<i64> = c
            .query(format!(
                "SELECT NTILE({buckets}) OVER (ORDER BY id) FROM nt"
            ))
            .await
            .unwrap();
        assert_eq!(got, want, "NTILE({buckets})");
    }
    // The DoS case: a huge bucket count must return immediately (each row in its
    // own bucket, extra buckets empty - MySQL's answer too).
    let started = std::time::Instant::now();
    let got: Vec<i64> = c
        .query("SELECT NTILE(1000000000000) OVER (ORDER BY id) FROM nt")
        .await
        .unwrap();
    assert_eq!(got, (1..=10).collect::<Vec<i64>>());
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "NTILE with a huge bucket count must not iterate buckets"
    );
}

// String-expanding functions must not allocate unbounded memory: past the byte
// budget they return NULL, exactly as MySQL does past max_allowed_packet.
#[tokio::test]
async fn string_expansion_is_bounded() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    // Within budget: normal results.
    let n: Option<u64> = c.query_first("SELECT LENGTH(SPACE(100))").await.unwrap();
    assert_eq!(n, Some(Some(100)).flatten());
    let n: Option<u64> = c
        .query_first("SELECT LENGTH(REPEAT('ab', 500))")
        .await
        .unwrap();
    assert_eq!(n, Some(1000));
    let n: Option<u64> = c
        .query_first("SELECT LENGTH(LPAD('a', 8, '-'))")
        .await
        .unwrap();
    assert_eq!(n, Some(8));
    // Over budget: NULL, not a multi-gigabyte allocation (MySQL-verified).
    for q in [
        "SELECT LENGTH(SPACE(10000000000))",
        "SELECT LENGTH(REPEAT('x', 200000000))",
        "SELECT LENGTH(LPAD('a', 10000000000, '-'))",
        "SELECT LENGTH(RPAD('a', 10000000000, '-'))",
    ] {
        let n: Option<Option<u64>> = c.query_first(q).await.unwrap();
        assert_eq!(n, Some(None), "{q} must be NULL, not a huge allocation");
    }
}

// Prepared statements are capped server-wide (MySQL's max_prepared_stmt_count),
// so the slot MUST be returned when a statement is closed. Preparing far more
// distinct statements than the limit (the driver evicts and closes as it goes)
// therefore has to keep working: if closing leaked its slot, this would start
// failing with error 1461 partway through.
#[tokio::test]
async fn prepared_statement_slots_are_reclaimed_on_close() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    // Comfortably more than the 16382 default limit.
    for i in 0..20_000u32 {
        // Distinct SQL each time so the driver cannot serve it from its cache;
        // it closes evicted statements, which must return their slots.
        let stmt = c
            .prep(format!("SELECT ? + {i}"))
            .await
            .unwrap_or_else(|e| panic!("prepare {i} failed (slot leak?): {e}"));
        let got: Option<u32> = c.exec_first(&stmt, (1u32,)).await.unwrap();
        assert_eq!(got, Some(i + 1));
        c.close(stmt).await.unwrap();
    }
}

// Connections are capped server-wide (MySQL's max_connections), so a slot MUST be
// returned when a connection ends. Cycling through far more connections than the
// 151 default would start failing with error 1040 partway through if the permit
// leaked on disconnect.
#[tokio::test]
async fn connection_slots_are_reclaimed_on_disconnect() {
    let srv = TestServer::start().await;
    for i in 0..500u32 {
        let mut c = srv.conn().await;
        let got: Option<u32> = c
            .query_first("SELECT 1")
            .await
            .unwrap_or_else(|e| panic!("connection {i} failed (slot leak?): {e}"));
        assert_eq!(got, Some(1));
        drop(c);
    }
}

// REGEXP follows the operand's collation, as MySQL does: case-insensitive under
// the default collation, case-sensitive for a `_bin` column. All expectations
// below were verified against real MySQL 8.4.
#[tokio::test]
async fn regexp_case_sensitivity_follows_collation() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE cs (s VARCHAR(20), sb VARCHAR(20) COLLATE utf8mb4_bin)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO cs VALUES ('Hello','Hello')")
        .await
        .unwrap();

    for (sql, want) in [
        // Default collation is case-insensitive.
        ("SELECT 'Hello' REGEXP 'h'", 1i64),
        ("SELECT 'hello' REGEXP 'H'", 1),
        ("SELECT 'Hello' RLIKE 'ELL'", 1),
        ("SELECT 'Hello' NOT REGEXP 'h'", 0),
        ("SELECT s REGEXP 'h' FROM cs", 1),
        // An inline flag still overrides the collation default.
        ("SELECT 'Hello' REGEXP '(?-i)h'", 0),
        // A _bin operand is case-sensitive.
        ("SELECT sb REGEXP 'h' FROM cs", 0),
        ("SELECT sb REGEXP 'H' FROM cs", 1),
    ] {
        let got: Option<i64> = c.query_first(sql).await.unwrap();
        assert_eq!(got, Some(want), "{sql}");
    }

    // The scalar regex functions use MySQL's default (case-insensitive) too.
    let got: Option<String> = c
        .query_first("SELECT REGEXP_REPLACE('a1B2','[b]','x')")
        .await
        .unwrap();
    assert_eq!(got.as_deref(), Some("a1x2"));
    let got: Option<String> = c
        .query_first("SELECT REGEXP_SUBSTR('ABC','b')")
        .await
        .unwrap();
    assert_eq!(got.as_deref(), Some("B"));
}

// A CPU-heavy query must not monopolise a runtime worker: the server has to keep
// serving other sessions while one grinds away, with no query timeout configured.
// This is the property that keeps the listener responsive under load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heavy_query_does_not_block_other_sessions() {
    let srv = TestServer::start().await;
    let mut setup = srv.conn().await;
    setup
        .query_drop("CREATE TABLE h (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    for i in 1..=250 {
        setup
            .query_drop(format!("INSERT INTO h VALUES ({i}, {})", i % 3))
            .await
            .unwrap();
    }

    // Saturate both workers with a materialising join that takes real CPU time.
    let mut hogs = Vec::new();
    for _ in 0..2 {
        let mut c = srv.conn().await;
        hogs.push(tokio::spawn(async move {
            let _: Result<Option<i64>, _> = c
                .query_first("SELECT COUNT(*) FROM h a, h b, h c WHERE a.v = b.v AND b.v = c.v")
                .await;
        }));
    }
    // Give them time to get going.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // A different session must still be served promptly.
    let mut probe = srv.conn().await;
    let started = std::time::Instant::now();
    let got: Option<i64> = probe.query_first("SELECT COUNT(*) FROM h").await.unwrap();
    let waited = started.elapsed();
    assert_eq!(got, Some(250));
    assert!(
        waited < std::time::Duration::from_secs(10),
        "a concurrent session waited {waited:?} behind CPU-heavy queries"
    );
    // The hogs have served their purpose; abort rather than wait them out.
    for h in hogs {
        h.abort();
    }
}

// Two correctness bugs that shipped in 1.4.12, both in the join paths.
//
// 1. The hash-join key was a collation key pushed through `from_utf8_lossy`, so
//    every byte that is not valid UTF-8 became U+FFFD and unrelated values
//    collided. Every integer in 128..255 hashed to one key, so a 1:1 join on those
//    ids returned their cartesian product.
// 2. `SpillAgg::finalize` finalised all 256 spill partitions including the empty
//    ones, and for an aggregate with **no GROUP BY** finalising an empty group set
//    legitimately means "zero rows in, one row out" — so 256 bogus zero rows were
//    appended after the real result.
//
// Expectations verified against real MySQL 8.4.
#[tokio::test]
async fn join_results_are_exact_across_the_byte_boundary() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE jl (id INT PRIMARY KEY, g INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE jr (id INT PRIMARY KEY, g INT)")
        .await
        .unwrap();
    // Spans 128..255, where the key encoding used to collapse.
    for i in 1..=400 {
        c.query_drop(format!("INSERT INTO jl VALUES ({i}, {})", i % 8))
            .await
            .unwrap();
        c.query_drop(format!("INSERT INTO jr VALUES ({i}, {})", i % 8))
            .await
            .unwrap();
    }

    // A 1:1 join on the primary key must return exactly one row per key, and a
    // bare aggregate over it exactly one row.
    let rows: Vec<i64> = c
        .query("SELECT COUNT(*) FROM jl a JOIN jr b ON a.id = b.id")
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![400],
        "bare aggregate over a join must be one row"
    );

    // No pair may mismatch: a collision would produce a.id <> b.id pairs.
    let rows: Vec<i64> = c
        .query("SELECT COUNT(*) FROM jl a JOIN jr b ON a.id = b.id WHERE a.id <> b.id")
        .await
        .unwrap();
    assert_eq!(rows, vec![0], "join keys must not collide");

    // Only the ids that used to collide.
    let rows: Vec<i64> = c
        .query("SELECT COUNT(*) FROM jl a JOIN jr b ON a.id = b.id WHERE a.id BETWEEN 128 AND 255")
        .await
        .unwrap();
    assert_eq!(rows, vec![128]);

    // Other aggregate shapes over a join: still exactly one row each.
    let rows: Vec<(i64, i64, i64)> = c
        .query("SELECT MIN(a.id), MAX(a.id), SUM(a.id) FROM jl a JOIN jr b ON a.id = b.id")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 400, 80200)]);

    // An empty join still yields the single zero row.
    let rows: Vec<i64> = c
        .query("SELECT COUNT(*) FROM jl a JOIN jr b ON a.id = b.id WHERE a.id < 0")
        .await
        .unwrap();
    assert_eq!(rows, vec![0]);

    // GROUP BY over a join was already correct; keep it pinned.
    let rows: Vec<(i64, i64)> = c
        .query("SELECT a.g, COUNT(*) FROM jl a JOIN jr b ON a.id = b.id GROUP BY a.g ORDER BY a.g")
        .await
        .unwrap();
    assert_eq!(rows.len(), 8);
    assert_eq!(rows.iter().map(|(_, n)| n).sum::<i64>(), 400);
}

// Cross joins stream, so their product is never buffered: the shape that once grew
// the process to 97 GB now runs in flat memory. The shapes that still materialise
// remain bounded by the row/byte ceilings, which this also keeps covered.
#[tokio::test]
async fn cross_join_streams_and_materialising_shapes_stay_bounded() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE jb (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    // Small enough that the full product is quick, large enough to be a real product
    // (120^3 = 1.7M combinations).
    for i in 1..=120 {
        c.query_drop(format!("INSERT INTO jb VALUES ({i}, {})", i % 4))
            .await
            .unwrap();
    }

    // An ordinary selective join is unaffected.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM jb a JOIN jb b ON a.id = b.id")
        .await
        .unwrap();
    assert_eq!(got, Some(120));

    // A three-way comma cross join with a residual predicate: previously refused by
    // the row cap (or fatal before that), now streamed and exact.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM jb a, jb b, jb c WHERE a.v + b.v + c.v >= 0")
        .await
        .unwrap();
    assert_eq!(got, Some(120 * 120 * 120));

    // Aggregates, GROUP BY and ORDER BY over a streamed cross join.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM jb a, jb b WHERE a.v = 1 AND b.v = 2")
        .await
        .unwrap();
    assert_eq!(got, Some(30 * 30));
    let rows: Vec<(i64, i64)> = c
        .query("SELECT a.v, COUNT(*) FROM jb a, jb b GROUP BY a.v ORDER BY a.v")
        .await
        .unwrap();
    assert_eq!(rows, vec![(0, 3600), (1, 3600), (2, 3600), (3, 3600)]);
    let rows: Vec<(i64, i64)> = c
        .query("SELECT a.id, b.id FROM jb a, jb b WHERE a.v = 0 AND b.v = 1 ORDER BY a.id DESC, b.id LIMIT 3")
        .await
        .unwrap();
    assert_eq!(rows, vec![(120, 1), (120, 5), (120, 9)]);

    // A non-equi join under an aggregate now streams too (ESQL-39), so it is
    // answered rather than refused -- exactly like the comma cross join above,
    // which it is a filtered form of.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM jb a JOIN jb b ON a.v < b.v")
        .await
        .unwrap();
    // v cycles 1,2,3,0 over ids 1..120, so each value has 30 rows.
    let by_v = 30i64;
    let expect = by_v * by_v * (3 + 2 + 1); // 0<1,2,3; 1<2,3; 2<3
    assert_eq!(got, Some(expect));

    // An equality plus an extra condition keeps the hash key and applies the rest
    // as a residual: same answer as the equality alone would give, filtered.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM jb a JOIN jb b ON a.id = b.id AND a.v > 1")
        .await
        .unwrap();
    assert_eq!(got, Some(60));

    // A LEFT join's residual is an ON condition, not a filter: rows it rejects
    // are unmatched, so the left row survives with NULLs.
    let rows: Vec<(i64, Option<i64>)> = c
        .query("SELECT a.id, b.id FROM jb a LEFT JOIN jb b ON a.id = b.id AND a.v > 1 ORDER BY a.id LIMIT 4")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, None), (2, Some(2)), (3, Some(3)), (4, None)]);

    // Shapes that still materialise keep the ceilings: a plain (non-aggregated,
    // unordered) non-equi join has no streaming consumer, so the cap path stays
    // covered.
    let err = c
        .query_drop("SELECT a.id, b.id FROM jb a JOIN jb b ON a.v < b.v JOIN jb c ON a.v < c.v JOIN jb d ON a.v < d.v JOIN jb e ON a.v < e.v")
        .await;
    if let Err(e) = err {
        let msg = e.to_string();
        assert!(
            msg.contains("ELYRASQL_JOIN_MAX_ROWS") || msg.contains("ELYRASQL_JOIN_MAX_BYTES"),
            "a refused materialising join should name a tunable limit, got: {msg}"
        );
    }

    // The session stays usable either way.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM jb a JOIN jb b ON a.id = b.id")
        .await
        .unwrap();
    assert_eq!(got, Some(120));
}

/// A join `ON` whose sides are expressions must be attributed to the right
/// relation. `equi_keys` asked "does this expression reference only left-schema
/// columns?" through a resolver that falls back to matching on the bare column
/// name, so in `ON a.v + b.v = 4` the reference `b.v` matched `a.v` and the whole
/// sum was taken as the *probe key*, with the literal `4` as the partner key --
/// a hash join on a condition that is not an equality between the two sides.
/// The result was silently wrong (2x the rows), not an error. Expectations
/// verified against MySQL 8.4.
#[tokio::test]
async fn join_on_expression_is_attributed_to_the_right_side() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE ja (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    c.query_drop("CREATE TABLE jb2 (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    for i in 1..=40 {
        c.query_drop(format!("INSERT INTO ja VALUES ({i}, {})", i % 4))
            .await
            .unwrap();
        c.query_drop(format!("INSERT INTO jb2 VALUES ({i}, {})", i % 4))
            .await
            .unwrap();
    }

    // 10 rows per v on each side. a.v + b.v = 4 holds for (1,3), (2,2), (3,1)
    // -> 3 x 100 = 300.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM ja a JOIN jb2 b ON a.v + b.v = 4")
        .await
        .unwrap();
    assert_eq!(got, Some(300), "ON expression spanning both sides");

    // a.id + b.id = 41 holds for id = 1..40 paired with 40..1 -> 40.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM ja a JOIN jb2 b ON a.id + b.id = 41")
        .await
        .unwrap();
    assert_eq!(got, Some(40));

    // The same shape under LEFT: every left row matches something here, and the
    // count must not double.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM ja a LEFT JOIN jb2 b ON a.v + b.v = 4")
        .await
        .unwrap();
    assert_eq!(got, Some(310), "10 unmatched left rows (v = 0) + 300");

    // Ordinary equi joins, including an expression on one side only, are
    // unaffected -- the attribution must not become too strict either.
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM ja a JOIN jb2 b ON a.id = b.id")
        .await
        .unwrap();
    assert_eq!(got, Some(40));
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM ja a JOIN jb2 b ON a.id = b.id + 0")
        .await
        .unwrap();
    assert_eq!(got, Some(40));
    let got: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM ja a JOIN jb2 b ON a.v = b.v")
        .await
        .unwrap();
    assert_eq!(got, Some(400));
}

// DISTINCT aggregates must not be split across parallel scan workers: partial
// results merge additively, so a value seen by two workers was counted twice.
// COUNT(DISTINCT x) came out as `workers x` the right answer -- machine-dependent
// wrong results. Expectations verified against real MySQL 8.4.
#[tokio::test]
async fn distinct_aggregates_are_exact_at_scale() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE da (k INT PRIMARY KEY, g INT, s VARCHAR(10))")
        .await
        .unwrap();
    // Large enough to trigger the parallel aggregation paths.
    for lo in (1..=20_000).step_by(1000) {
        let vals: Vec<String> = (lo..lo + 1000)
            .map(|i| format!("({i},{},'w{}')", i % 8, i % 8))
            .collect();
        c.query_drop(format!("INSERT INTO da VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }
    let n: Option<i64> = c
        .query_first("SELECT COUNT(DISTINCT g) FROM da")
        .await
        .unwrap();
    assert_eq!(
        n,
        Some(8),
        "COUNT(DISTINCT) must not scale with worker count"
    );
    let n: Option<i64> = c
        .query_first("SELECT SUM(DISTINCT g) FROM da")
        .await
        .unwrap();
    assert_eq!(n, Some(28), "SUM(DISTINCT) must not double-count");
    let n: Option<i64> = c
        .query_first("SELECT COUNT(DISTINCT s) FROM da")
        .await
        .unwrap();
    assert_eq!(n, Some(8));
    // AVG(DISTINCT) looked correct even when broken (both halves of the ratio were
    // inflated equally), so it is pinned too.
    let v: Option<f64> = c
        .query_first("SELECT AVG(DISTINCT g) FROM da")
        .await
        .unwrap();
    assert_eq!(v, Some(3.5));
    // Grouped DISTINCT and the plain aggregates must stay correct.
    let rows: Vec<(i64, i64)> = c
        .query("SELECT g, COUNT(DISTINCT s) FROM da GROUP BY g ORDER BY g")
        .await
        .unwrap();
    assert_eq!(rows.len(), 8);
    assert!(rows.iter().all(|&(_, n)| n == 1));
    let n: Option<i64> = c.query_first("SELECT COUNT(*) FROM da").await.unwrap();
    assert_eq!(n, Some(20_000));

    // Many distinct values spanning the 128..255 byte range: the distinct set used
    // to be keyed on a lossily UTF-8-converted collation key, so those values
    // collided and COUNT(DISTINCT) *under*-counted (258 of 500) - the opposite error
    // to the worker-count inflation above, and the two masked each other.
    c.query_drop("CREATE TABLE dw (k INT PRIMARY KEY, w INT)")
        .await
        .unwrap();
    for lo in (1..=20_000).step_by(1000) {
        let vals: Vec<String> = (lo..lo + 1000)
            .map(|i| format!("({i},{})", i % 500))
            .collect();
        c.query_drop(format!("INSERT INTO dw VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }
    let n: Option<i64> = c
        .query_first("SELECT COUNT(DISTINCT w) FROM dw")
        .await
        .unwrap();
    assert_eq!(
        n,
        Some(500),
        "distinct keys must not collide across byte values"
    );
    let n: Option<i64> = c
        .query_first("SELECT SUM(DISTINCT w) FROM dw")
        .await
        .unwrap();
    assert_eq!(n, Some((0..500).sum::<i64>()));
    // Cross-check against an independent path that computes the same thing.
    let n: Option<i64> = c
        .query_first("SELECT COUNT(*) FROM (SELECT DISTINCT w FROM dw) x")
        .await
        .unwrap();
    assert_eq!(n, Some(500));
}

// Functions that return an integer must be typed as one even when their argument
// is an aggregate: `LENGTH(GROUP_CONCAT(s))` used to reach the client as the
// string "23" rather than the number 23.
#[tokio::test]
async fn integer_functions_over_aggregates_keep_their_type() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE it (k INT PRIMARY KEY, s VARCHAR(10))")
        .await
        .unwrap();
    for i in 1..=50 {
        c.query_drop(format!("INSERT INTO it VALUES ({i}, 'w{}')", i % 8))
            .await
            .unwrap();
    }
    // Decoding into i64 only succeeds if the column is typed as an integer.
    let n: Option<i64> = c
        .query_first("SELECT LENGTH(MAX(s)) FROM it")
        .await
        .unwrap();
    assert_eq!(n, Some(2));
    let n: Option<i64> = c
        .query_first("SELECT CHAR_LENGTH(MAX(s)) FROM it")
        .await
        .unwrap();
    assert_eq!(n, Some(2));
    let n: Option<i64> = c.query_first("SELECT ASCII(MAX(s)) FROM it").await.unwrap();
    assert_eq!(n, Some(119));
    let n: Option<i64> = c
        .query_first("SELECT LENGTH(MAX(s)) + 1 FROM it")
        .await
        .unwrap();
    assert_eq!(n, Some(3));
}

// GROUP_CONCAT must honour its own ORDER BY. It previously parsed the clause and
// ignored it, returning values in scan order. All expectations verified against
// real MySQL 8.4. Note the deliberate use of sort keys that are *not* the
// concatenated column: ordering by the aggregate's own argument would pass even
// when the sort-key columns are never decoded.
#[tokio::test]
async fn group_concat_honours_its_order_by() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE gc (k INT PRIMARY KEY, g INT, s VARCHAR(8), n INT)")
        .await
        .unwrap();
    // s runs counter to k, so scan order and sorted order differ visibly.
    for i in 1..=12 {
        c.query_drop(format!(
            "INSERT INTO gc VALUES ({i}, {}, 'w{}', {})",
            i % 3,
            13 - i,
            i % 4
        ))
        .await
        .unwrap();
    }
    for (sql, want) in [
        (
            "SELECT GROUP_CONCAT(DISTINCT s ORDER BY s) FROM gc",
            "w1,w10,w11,w12,w2,w3,w4,w5,w6,w7,w8,w9",
        ),
        (
            "SELECT GROUP_CONCAT(DISTINCT s ORDER BY s DESC) FROM gc",
            "w9,w8,w7,w6,w5,w4,w3,w2,w12,w11,w10,w1",
        ),
        // Ordering by a column other than the concatenated one.
        (
            "SELECT GROUP_CONCAT(s ORDER BY k) FROM gc",
            "w12,w11,w10,w9,w8,w7,w6,w5,w4,w3,w2,w1",
        ),
        (
            "SELECT GROUP_CONCAT(s ORDER BY k DESC) FROM gc",
            "w1,w2,w3,w4,w5,w6,w7,w8,w9,w10,w11,w12",
        ),
        // Multiple keys, mixed directions, and a custom separator.
        (
            "SELECT GROUP_CONCAT(s ORDER BY n, k DESC) FROM gc",
            "w1,w5,w9,w4,w8,w12,w3,w7,w11,w2,w6,w10",
        ),
        (
            "SELECT GROUP_CONCAT(DISTINCT s ORDER BY s DESC SEPARATOR '|') FROM gc",
            "w9|w8|w7|w6|w5|w4|w3|w2|w12|w11|w10|w1",
        ),
    ] {
        let got: Option<String> = c.query_first(sql).await.unwrap();
        assert_eq!(got.as_deref(), Some(want), "{sql}");
    }
    // Per group, and unordered GROUP_CONCAT still works.
    let rows: Vec<(i64, String)> = c
        .query("SELECT g, GROUP_CONCAT(s ORDER BY k) FROM gc GROUP BY g ORDER BY g")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].1, "w10,w7,w4,w1");
    let got: Option<String> = c
        .query_first("SELECT GROUP_CONCAT(s) FROM gc")
        .await
        .unwrap();
    assert_eq!(got.map(|s| s.split(',').count()), Some(12));
}

// Index introspection commonly prepares this information_schema query. The
// GROUP BY path used to declare `NOT non_unique` as text while returning an
// integer, which terminated the binary-protocol connection.
#[tokio::test]
async fn prepared_index_introspection_keeps_not_result_numeric() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop(
        "CREATE TABLE indexed_items (id BIGINT PRIMARY KEY, slug VARCHAR(32), INDEX ix_slug (slug))",
    )
    .await
    .unwrap();

    let mut rows: Vec<(String, String, String, i64)> = c
        .exec(
            "SELECT index_name AS `name`, \
                    GROUP_CONCAT(column_name ORDER BY seq_in_index) AS `columns`, \
                    index_type AS `type`, NOT non_unique AS `unique` \
             FROM information_schema.statistics \
             WHERE table_schema = schema() AND table_name = 'indexed_items' \
             GROUP BY index_name, index_type, non_unique",
            (),
        )
        .await
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("PRIMARY".into(), "id".into(), "BTREE".into(), 1),
            ("ix_slug".into(), "slug".into(), "BTREE".into(), 0),
        ]
    );

    let rows: Vec<(i64, String, String, Option<i64>, String)> = c
        .query(
            "SELECT NON_UNIQUE, INDEX_NAME, COLUMN_NAME, SUB_PART, INDEX_TYPE
             FROM information_schema.STATISTICS
             WHERE TABLE_SCHEMA = 'elyra'
               AND TABLE_NAME = 'indexed_items'
               AND INDEX_NAME = 'ix_slug'
             ORDER BY SEQ_IN_INDEX",
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![(1, "ix_slug".into(), "slug".into(), None, "BTREE".into())]
    );
}

// A secondary-index range must not be used when it matches most of the table: the
// index fetches every matching row by key, which is far dearer per row than a
// sequential decode. The observable contract is correctness (the planner may choose
// either strategy), so this pins the *results* across the selectivity spectrum,
// including right at the fallback threshold.
#[tokio::test]
async fn wide_index_ranges_return_correct_rows() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE ir (id INT PRIMARY KEY, g INT, amt INT)")
        .await
        .unwrap();
    // 20k rows: past the small-table floor, so the budget actually applies.
    for lo in (1..=20_000).step_by(1000) {
        let vals: Vec<String> = (lo..lo + 1000)
            .map(|i| format!("({i},{},{})", i % 100, (i * 7919) % 20000))
            .collect();
        c.query_drop(format!("INSERT INTO ir VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }
    c.query_drop("CREATE INDEX ix_amt ON ir (amt)")
        .await
        .unwrap();
    c.query_drop("CREATE INDEX ix_g ON ir (g)").await.unwrap();

    // Independently computed expectations from the same generator.
    let amt = |i: i64| (i * 7919) % 20000;
    for (sql, want) in [
        // Very selective: the index is used.
        (
            "SELECT COUNT(*) FROM ir WHERE amt > 19900",
            (1..=20_000i64).filter(|&i| amt(i) > 19900).count() as i64,
        ),
        // Around the fallback threshold.
        (
            "SELECT COUNT(*) FROM ir WHERE amt > 18800",
            (1..=20_000i64).filter(|&i| amt(i) > 18800).count() as i64,
        ),
        // Wide: falls back to a scan, and must still be exact.
        (
            "SELECT COUNT(*) FROM ir WHERE amt > 0",
            (1..=20_000i64).filter(|&i| amt(i) > 0).count() as i64,
        ),
        ("SELECT COUNT(*) FROM ir WHERE amt >= 0", 20_000),
        (
            "SELECT COUNT(*) FROM ir WHERE g > 50",
            (1..=20_000i64).filter(|&i| i % 100 > 50).count() as i64,
        ),
        (
            "SELECT COUNT(*) FROM ir WHERE amt BETWEEN 5000 AND 15000",
            (1..=20_000i64)
                .filter(|&i| (5000..=15000).contains(&amt(i)))
                .count() as i64,
        ),
        // A primary-key range is a sequential read and is never diverted.
        ("SELECT COUNT(*) FROM ir WHERE id > 10000", 10_000),
    ] {
        let got: Option<i64> = c.query_first(sql).await.unwrap();
        assert_eq!(got, Some(want), "{sql}");
    }

    // The fallback must also preserve non-COUNT aggregates and row output.
    let got: Option<i64> = c
        .query_first("SELECT SUM(amt) FROM ir WHERE amt > 0")
        .await
        .unwrap();
    let want: i64 = (1..=20_000i64).map(amt).filter(|&a| a > 0).sum();
    assert_eq!(got, Some(want));
    let rows: Vec<i64> = c
        .query("SELECT id FROM ir WHERE amt > 0 ORDER BY id LIMIT 5")
        .await
        .unwrap();
    let want: Vec<i64> = (1..=20_000i64).filter(|&i| amt(i) > 0).take(5).collect();
    assert_eq!(rows, want);
}

// `col IN (literals)` on an indexed column is served by index lookups rather than a
// scan that tests membership per row. The planner may still choose a scan for a wide
// list, so this pins the *results* across every shape that path has to get right --
// duplicates, NULL in the list, negative literals, NOT IN, a residual conjunct, the
// primary key, strings, and LIMIT over the deduplicated set.
#[tokio::test]
async fn in_list_lookups_return_correct_rows() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE il (id INT PRIMARY KEY, g INT, s VARCHAR(16))")
        .await
        .unwrap();
    for lo in (1..=8_000).step_by(1000) {
        let vals: Vec<String> = (lo..lo + 1000)
            .map(|i| format!("({i},{},'v{}')", i % 40, i % 40))
            .collect();
        c.query_drop(format!("INSERT INTO il VALUES {}", vals.join(",")))
            .await
            .unwrap();
    }
    c.query_drop("CREATE INDEX ix_g ON il (g)").await.unwrap();
    c.query_drop("CREATE INDEX ix_s ON il (s)").await.unwrap();

    let per_group = 8_000 / 40; // 200
    for (sql, want) in [
        ("SELECT COUNT(*) FROM il WHERE g IN (1,2,3,4,5)", 5 * per_group),
        // Duplicates must not double-count, and a NULL element matches nothing.
        ("SELECT COUNT(*) FROM il WHERE g IN (1,1,2,2,NULL,3)", 3 * per_group),
        ("SELECT COUNT(*) FROM il WHERE g IN (-1,-2)", 0),
        ("SELECT COUNT(*) FROM il WHERE g IN (7)", per_group),
        // NOT IN is the complement and must not use the lookup path.
        (
            "SELECT COUNT(*) FROM il WHERE g NOT IN (1,2,3,4,5)",
            8_000 - 5 * per_group,
        ),
        // A residual conjunct is re-applied to the fetched rows.
        (
            "SELECT COUNT(*) FROM il WHERE g IN (1,2) AND id > 4000",
            (1..=8_000i64).filter(|&i| (i % 40 == 1 || i % 40 == 2) && i > 4000).count() as i64,
        ),
        ("SELECT COUNT(*) FROM il WHERE id IN (1,2,3,4,5)", 5),
        ("SELECT COUNT(*) FROM il WHERE id IN (1,1,2)", 2),
        ("SELECT COUNT(*) FROM il WHERE s IN ('v1','v2')", 2 * per_group),
        // A list covering the whole table falls back to a scan; still exact.
        (
            "SELECT COUNT(*) FROM il WHERE g IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39)",
            8_000,
        ),
        // OR of two INs must not be mistaken for one index-usable list.
        ("SELECT COUNT(*) FROM il WHERE g IN (1) OR g IN (2)", 2 * per_group),
    ] {
        let got: Option<i64> = c.query_first(sql).await.unwrap();
        assert_eq!(got, Some(want), "{sql}");
    }

    // Literals of a different type than the column must be coerced before they are
    // encoded as index keys. Clients can bind integers as quoted strings; without
    // coercion the lookup found nothing while an equivalent scan found the rows.
    for (sql, want) in [
        ("SELECT COUNT(*) FROM il WHERE id IN ('1','2')", 2i64),
        ("SELECT COUNT(*) FROM il WHERE id IN ('1',2)", 2),
        ("SELECT COUNT(*) FROM il WHERE id IN (1.0,2.0)", 2),
        (
            "SELECT COUNT(*) FROM il WHERE g IN ('1','2')",
            2 * per_group,
        ),
        // Not representable in the column type: matches nothing, as in MySQL.
        ("SELECT COUNT(*) FROM il WHERE id IN ('abc')", 0),
    ] {
        let got: Option<i64> = c.query_first(sql).await.unwrap();
        assert_eq!(got, Some(want), "{sql}");
    }

    // Aggregates and ordered output over the lookup path.
    let got: Option<i64> = c
        .query_first("SELECT SUM(id) FROM il WHERE g IN (7,8,9)")
        .await
        .unwrap();
    let want: i64 = (1..=8_000i64)
        .filter(|&i| (7..=9).contains(&(i % 40)))
        .sum();
    assert_eq!(got, Some(want));
    let rows: Vec<i64> = c
        .query("SELECT id FROM il WHERE g IN (1,2) ORDER BY id LIMIT 5")
        .await
        .unwrap();
    assert_eq!(rows, vec![1, 2, 41, 42, 81]);
}

// Window functions: the projection is classified once rather than rebuilt per row,
// and partition keys are raw bytes with a fast path for the unpartitioned case. Those
// are pure optimisations, so this test pins the *results* for every shape the
// classification distinguishes: the item being a window call, containing one inside a
// larger expression, and containing none.
//
// Every ORDER BY here includes a tiebreaker on purpose. Ordering by a column with
// duplicates leaves ROW_NUMBER implementation-defined for the tied rows, which makes
// such a query useless as an oracle -- a lesson learned when an unstable query looked
// like a regression.
#[tokio::test]
async fn window_functions_are_exact() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE w (id INT PRIMARY KEY, g INT, amt INT)")
        .await
        .unwrap();
    // amt deliberately has ties within each g, so the tiebreaker matters.
    for i in 1..=60 {
        c.query_drop(format!("INSERT INTO w VALUES ({i}, {}, {})", i % 3, i % 10))
            .await
            .unwrap();
    }

    // Unpartitioned: the fast path that skips partition hashing entirely.
    let rows: Vec<(i64, i64)> = c
        .query("SELECT id, ROW_NUMBER() OVER (ORDER BY amt, id) FROM w ORDER BY id LIMIT 5")
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    // id 10,20,30,... have amt 0, so id=10 is row 1 overall.
    assert_eq!(rows[0].0, 1);

    // Row numbers must be a permutation of 1..n with no gaps or repeats.
    let nums: Vec<i64> = c
        .query("SELECT ROW_NUMBER() OVER (ORDER BY amt, id) FROM w")
        .await
        .unwrap();
    let mut sorted = nums.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, (1..=60).collect::<Vec<i64>>());

    // Partitioned: each partition restarts at 1 and covers its own rows.
    let rows: Vec<(i64, i64)> = c
        .query("SELECT g, MAX(r) FROM (SELECT g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY amt, id) r FROM w) x GROUP BY g ORDER BY g")
        .await
        .unwrap();
    assert_eq!(rows, vec![(0, 20), (1, 20), (2, 20)]);

    // An aggregate window over a partition.
    let rows: Vec<(i64, i64)> = c
        .query("SELECT g, SUM(s) FROM (SELECT DISTINCT g, SUM(amt) OVER (PARTITION BY g) s FROM w) x GROUP BY g ORDER BY g")
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    // A window call nested inside a larger expression (the substitution path).
    let rows: Vec<(i64, i64)> = c
        .query("SELECT id, ROW_NUMBER() OVER (ORDER BY amt, id) + 100 FROM w ORDER BY id LIMIT 3")
        .await
        .unwrap();
    assert!(rows.iter().all(|&(_, v)| v > 100));

    // A projection item with no window function alongside one that has.
    let rows: Vec<(i64, i64, i64)> = c
        .query("SELECT id, id * 2, ROW_NUMBER() OVER (ORDER BY id) FROM w ORDER BY id LIMIT 4")
        .await
        .unwrap();
    assert_eq!(rows, vec![(1, 2, 1), (2, 4, 2), (3, 6, 3), (4, 8, 4)]);

    // Wildcards expand alongside a window expression, including when the
    // window query is wrapped to filter on its generated row number.
    let rows: Vec<(i64, i64, i64, i64)> = c
        .query(
            "SELECT * FROM (
                 SELECT *, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id DESC) AS row_num
                 FROM w WHERE id <= 6
             ) AS limited
             WHERE row_num <= 1 ORDER BY g",
        )
        .await
        .unwrap();
    assert_eq!(rows, vec![(6, 0, 6, 1), (4, 1, 4, 1), (5, 2, 5, 1)]);

    // RANK/DENSE_RANK over ties: tie-insensitive, so exact regardless of order.
    let rows: Vec<(i64, i64, i64)> = c
        .query("SELECT amt, RANK() OVER (ORDER BY amt), DENSE_RANK() OVER (ORDER BY amt) FROM w ORDER BY amt, id LIMIT 7")
        .await
        .unwrap();
    assert!(rows.iter().all(|&(_, r, d)| r >= d));
    assert_eq!(rows[0].1, 1);
    assert_eq!(rows[0].2, 1);
}

// Statements containing non-ASCII text must not panic the connection. The keyword
// sniffers sliced the SQL by byte offset, so a multi-byte character straddling that
// offset (`SELECT 'æ'='ae'` -- 'æ' spans bytes 8..10, "drop user" is 9 bytes) aborted
// the worker. Newly reachable once the default collation made such literals ordinary.
#[tokio::test]
async fn non_ascii_literals_do_not_panic_the_connection() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    for sql in [
        "SELECT 'æ'='ae'",
        "SELECT 'café'='cafe'",
        "SELECT 'Straße'='Strasse'",
        "SELECT 'ø'='o'",
        "SELECT 'Ærlig' < 'cat'",
    ] {
        let got: Option<i64> = c.query_first(sql).await.unwrap_or_else(|e| {
            panic!("{sql} should evaluate, got: {e}");
        });
        assert_eq!(
            got,
            Some(1),
            "{sql} should be true under utf8mb4_0900_ai_ci"
        );
    }
    // The session survives and still works.
    let got: Option<i64> = c.query_first("SELECT 1").await.unwrap();
    assert_eq!(got, Some(1));
}

// Once the default collation is accent-insensitive, two keys that fold together are a
// genuine duplicate. The collation migration reports exactly this as a collision when
// it finds the pair in existing data rather than dropping one of the rows; here the
// same rule is checked at insert time, which is the path every new database takes.
#[tokio::test]
async fn keys_that_fold_together_collide_instead_of_silently_replacing() {
    let srv = TestServer::start().await;
    let mut c = srv.conn().await;
    c.query_drop("CREATE TABLE cm (name VARCHAR(60) PRIMARY KEY, n INT)")
        .await
        .unwrap();
    c.query_drop("INSERT INTO cm VALUES ('aeble', 1)")
        .await
        .unwrap();
    let err = c.query_drop("INSERT INTO cm VALUES ('\u{e6}ble', 2)").await;
    assert!(
        err.is_err(),
        "'\u{e6}ble' folds to 'aeble' and must be refused as a duplicate key"
    );
    // The original row is untouched -- the second insert must not have replaced it.
    let got: Option<i64> = c
        .query_first("SELECT n FROM cm WHERE name = 'aeble'")
        .await
        .unwrap();
    assert_eq!(got, Some(1));
    let got: Option<i64> = c.query_first("SELECT COUNT(*) FROM cm").await.unwrap();
    assert_eq!(got, Some(1));
    // Both spellings find the surviving row.
    let got: Option<i64> = c
        .query_first("SELECT n FROM cm WHERE name = '\u{e6}ble'")
        .await
        .unwrap();
    assert_eq!(got, Some(1));
}
