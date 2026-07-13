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

    drop(c);
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
