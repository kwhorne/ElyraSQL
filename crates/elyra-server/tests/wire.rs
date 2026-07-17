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
