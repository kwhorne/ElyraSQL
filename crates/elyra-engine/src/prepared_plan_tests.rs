//! A prepared plan runs a statement as the text path runs it with the
//! parameters substituted as literals: the same result set (names, types,
//! values), the same affected rows and insert id, the same error, the same
//! warnings and the same data afterwards.

use super::{Engine, Privilege, QueryResult, Session};

const SETUP: &[&str] = &[
    "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(64), score DECIMAL(10,2), \
     born DATE, at DATETIME, flag TINYINT, data BLOB, note TEXT, big BIGINT UNSIGNED, KEY (name))",
    "INSERT INTO t VALUES \
     (1, 'name1', 1.50, '2020-01-01', '2020-01-01 10:00:00', 1, x'00ff', NULL, 1), \
     (2, 'name2', -2.25, '2021-06-15', '2021-06-15 23:59:59', 0, NULL, 'x', 18446744073709551615), \
     (3, 'Name3', 30.00, NULL, NULL, NULL, x'41', 'O''Brien', 0), \
     (4, 'na_e4', 4.00, '1999-12-31', '1999-12-31 00:00:00', 1, NULL, 'a\\\\b', 4), \
     (5, 'name%5', 5.75, '2000-02-29', '2000-02-29 12:30:00', 0, NULL, '', 5)",
    "CREATE TABLE a (id INT PRIMARY KEY AUTO_INCREMENT, tid INT, v VARCHAR(16), UNIQUE KEY (v))",
    "INSERT INTO a (tid, v) VALUES (1, 'a'), (2, 'b'), (2, 'c')",
];

/// Each `?` outside single quotes, numbered (`?1`..) or replaced by its literal.
fn render(template: &str, literals: &[&str], numbered: bool) -> String {
    let mut out = String::new();
    let mut quoted = false;
    let mut k = 0;
    for c in template.chars() {
        match c {
            '\'' => {
                quoted = !quoted;
                out.push(c);
            }
            '?' if !quoted => {
                if numbered {
                    out.push_str(&format!("?{}", k + 1));
                } else {
                    out.push_str(literals[k]);
                }
                k += 1;
            }
            _ => out.push(c),
        }
    }
    assert_eq!(k, literals.len(), "{template}");
    out
}

async fn engine() -> (Engine, Session) {
    let e = Engine::new(elyra_storage::Db::in_memory().unwrap());
    let s = e.session();
    for sql in SETUP {
        e.execute(sql, Privilege::Admin, &s).await.unwrap();
    }
    (e, s)
}

async fn describe(results: super::Result<Vec<QueryResult>>) -> String {
    match results {
        Err(e) => format!("error: {e}"),
        Ok(results) => {
            let mut out = String::new();
            for r in results {
                match r {
                    QueryResult::Rows(mut rows) => {
                        out.push_str(&format!("{:?}\n", rows.schema));
                        loop {
                            let batch = rows.next_batch(64).await.unwrap();
                            if batch.is_empty() {
                                break;
                            }
                            out.push_str(&format!("{batch:?}\n"));
                        }
                    }
                    QueryResult::Affected(n) => out.push_str(&format!("affected {n}\n")),
                    QueryResult::Insert {
                        affected_rows,
                        last_insert_id,
                    } => out.push_str(&format!("insert {affected_rows} {last_insert_id}\n")),
                }
            }
            out
        }
    }
}

/// What the statement left behind: warnings, LAST_INSERT_ID, the tables.
async fn after(e: &Engine, s: &Session) -> String {
    let mut out = String::new();
    for sql in [
        "SHOW WARNINGS",
        "SELECT LAST_INSERT_ID()",
        "SELECT * FROM t ORDER BY id",
        "SELECT * FROM a ORDER BY id",
    ] {
        out.push_str(&describe(e.execute(sql, Privilege::Admin, s).await).await);
    }
    out
}

/// The cases whose plan the engine refuses (they keep the text path).
const TEXT_ONLY: &[&str] = &[
    "SELECT BINARY ? = name FROM t WHERE id = 3",
    "SELECT name INTO @x FROM t WHERE id = ?",
    "SELECT @x := ?",
    "SELECT ? + @@max_allowed_packet",
    "SET @y = ?",
    "CALL nothing(?)",
    "SHOW TABLES LIKE ?",
    "SELECT 1; SELECT ?",
];

const CASES: &[(&str, &[&str])] = &[
    ("SELECT * FROM t WHERE id = ?", &["5"]),
    ("SELECT * FROM t WHERE id = ?", &["'5'"]),
    ("SELECT * FROM t WHERE id = ?", &["99"]),
    ("SELECT * FROM t WHERE id = ?", &["NULL"]),
    ("SELECT * FROM t WHERE id = ?", &["'abc'"]),
    ("SELECT * FROM t WHERE id = ?", &["2.5"]),
    ("SELECT id, name FROM t WHERE name = ?", &["'NAME3'"]),
    ("SELECT id FROM t WHERE name LIKE ?", &["'name%'"]),
    ("SELECT id FROM t WHERE name LIKE ?", &["'na\\\\_e%'"]),
    ("SELECT id FROM t WHERE name LIKE ?", &["'name\\\\%%'"]),
    (
        "SELECT id FROM t WHERE name LIKE ? ESCAPE ?",
        &["'na|_e%'", "'|'"],
    ),
    ("SELECT id FROM t WHERE id IN (?, ?, ?)", &["1", "3", "99"]),
    (
        "SELECT id FROM t WHERE COALESCE(note, ?) = ?",
        &["'x'", "'x'"],
    ),
    ("SELECT id FROM t WHERE score > ?", &["-1.5"]),
    ("SELECT id FROM t WHERE score > ? AND id < ?", &["-3", "4"]),
    ("SELECT ? * 2, ? - ?, ? + 1", &["-5", "3", "-2", "-4"]),
    // As `COM_STMT_EXECUTE` renders negative numbers: after a space, so that
    // after a minus they don't make `--`.
    ("SELECT ?-?, -?, ?-?", &["3", " -2", " -4", "1.5", " -0.5"]),
    (
        "SELECT ?, ?, ?, ?",
        &["18446744073709551615", "-9223372036854775808", "0.1", "1e3"],
    ),
    ("SELECT id FROM t WHERE born = ?", &["'2020-01-01'"]),
    (
        "SELECT id FROM t WHERE at BETWEEN ? AND ?",
        &["'2000-01-01 00:00:00'", "'2020-12-31'"],
    ),
    ("SELECT DATE_ADD(?, INTERVAL ? DAY)", &["'2020-02-28'", "2"]),
    ("SELECT id FROM t ORDER BY id LIMIT ?", &["2"]),
    ("SELECT id FROM t ORDER BY id LIMIT ? OFFSET ?", &["2", "1"]),
    ("SELECT id FROM t ORDER BY id LIMIT ?, ?", &["1", "2"]),
    ("SELECT id FROM t ORDER BY id LIMIT ?", &["'2'"]),
    ("SELECT ? AS x, '?' AS y", &["'?'"]),
    ("SELECT id FROM t WHERE data = ?", &["x'00ff'"]),
    ("SELECT id FROM t WHERE note = ?", &["'O''Brien'"]),
    ("SELECT id FROM t WHERE note = ?", &["'a\\\\b'"]),
    ("SELECT id FROM t WHERE note = ?", &["''"]),
    (
        "SELECT id FROM t WHERE id = (SELECT MAX(id) FROM t WHERE score < ?)",
        &["10"],
    ),
    (
        "SELECT t.id, a.v FROM t JOIN a ON a.tid = t.id WHERE a.v <> ? ORDER BY a.id",
        &["'b'"],
    ),
    ("SELECT CASE WHEN ? THEN 'y' ELSE 'n' END", &["1"]),
    (
        "SELECT CAST(? AS SIGNED), CONVERT(?, CHAR), CAST(? AS DECIMAL(5,1))",
        &["'12abc'", "42", "'3.14159'"],
    ),
    ("SELECT JSON_EXTRACT(?, '$.a')", &["'{\"a\": [1, 2]}'"]),
    ("SELECT CONCAT(?, name) FROM t WHERE id = ?", &["'x'", "1"]),
    ("SELECT id FROM t WHERE id = ? LOCK IN SHARE MODE", &["1"]),
    ("SELECT id FROM t WHERE id = ? FOR UPDATE", &["1"]),
    ("WITH c AS (SELECT ? AS x) SELECT x + 1 FROM c", &["41"]),
    ("SELECT ~?", &["0"]),
    ("SELECT !?", &["0"]),
    ("SELECT id FROM t WHERE flag <=> ?", &["NULL"]),
    (
        "SELECT COUNT(*), ? FROM t GROUP BY flag ORDER BY 1",
        &["'g'"],
    ),
    ("SELECT id FROM t WHERE big = ?", &["18446744073709551615"]),
    ("SELECT id FROM t WHERE big > ?", &["-1"]),
    ("INSERT INTO t (id, name) VALUES (?, ?)", &["10", "'ten'"]),
    ("INSERT INTO t (id, name) VALUES (?, ?)", &["1", "'dup'"]),
    (
        "INSERT INTO t (id, name, score, born) VALUES (?, ?, ?, ?)",
        &["11", "NULL", "'1.239'", "'2020-02-30'"],
    ),
    ("INSERT INTO t (id, flag) VALUES (?, ?)", &["12", "300"]),
    (
        "INSERT INTO t (id, data) VALUES (?, ?)",
        &["13", "x'00010203ff'"],
    ),
    ("INSERT INTO t SET id = ?, name = ?", &["14", "'set'"]),
    (
        "INSERT IGNORE INTO t (id, name) VALUES (?, ?)",
        &["1", "'ignored'"],
    ),
    (
        "INSERT INTO t (id, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = ?",
        &["1", "'x'", "'upd'"],
    ),
    (
        "INSERT INTO t (id, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name)",
        &["2", "'vals'"],
    ),
    (
        "REPLACE INTO t (id, name) VALUES (?, ?)",
        &["3", "'replaced'"],
    ),
    ("INSERT INTO a (tid, v) VALUES (?, ?)", &["9", "'z'"]),
    (
        "INSERT INTO a (tid, v) VALUES (?, ?), (?, ?)",
        &["9", "'y'", "9", "'a'"],
    ),
    (
        "INSERT INTO t (id, name) SELECT id + ?, v FROM a WHERE tid = ?",
        &["100", "2"],
    ),
    ("UPDATE t SET score = score + ? WHERE id = ?", &["1.5", "1"]),
    (
        "UPDATE t SET name = ? WHERE id IN (?, ?)",
        &["'two'", "1", "2"],
    ),
    ("UPDATE t SET flag = ? WHERE id = ?", &["1000", "1"]),
    (
        "UPDATE t SET name = ? WHERE id > 2 ORDER BY id DESC LIMIT 2",
        &["'last'"],
    ),
    (
        "UPDATE t SET name = ? WHERE id > ? LIMIT 1",
        &["'lim'", "1"],
    ),
    ("UPDATE IGNORE t SET id = ? WHERE id = ?", &["1", "2"]),
    (
        "UPDATE t, a SET t.note = ? WHERE a.tid = t.id AND a.v = ?",
        &["'multi'", "'b'"],
    ),
    ("DELETE FROM t WHERE id = ?", &["4"]),
    ("DELETE FROM t WHERE id > ? LIMIT ?", &["1", "2"]),
    (
        "DELETE FROM t WHERE id > ? ORDER BY id DESC LIMIT 1",
        &["1"],
    ),
    ("DELETE FROM t WHERE name LIKE ?", &["'name%'"]),
];

async fn compare(template: &str, literals: &[&str]) -> bool {
    let text = render(template, literals, false);
    let (e1, s1) = engine().await;
    let expected = describe(e1.execute_as(&text, Privilege::Admin, "root", &s1).await).await
        + &after(&e1, &s1).await;
    let (e2, s2) = engine().await;
    let Some(plan) = e2.prepare_plan(&render(template, literals, true), literals.len(), &s2) else {
        return false;
    };
    let got = describe(
        e2.execute_plan(
            &plan,
            &literals.iter().map(|l| l.to_string()).collect::<Vec<_>>(),
            Privilege::Admin,
            "root",
            &s2,
        )
        .await
        .expect("the plan runs"),
    )
    .await
        + &after(&e2, &s2).await;
    assert_eq!(
        got, expected,
        "\n{template} with {literals:?}\ntext: {text}"
    );
    true
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plan_runs_as_the_text_with_its_literals() {
    let mut text_only = Vec::new();
    for (template, literals) in CASES {
        if !compare(template, literals).await {
            text_only.push(*template);
        }
    }
    // The shapes the plan is for all get one; a refused one would only be
    // slower, but the point is that they are not.
    let expected_text_only = [
        "SELECT id FROM t WHERE name LIKE ? ESCAPE ?",
        "SELECT ~?",
        "SELECT !?",
    ];
    assert_eq!(text_only, expected_text_only);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn statements_the_plan_does_not_take_keep_the_text_path() {
    let (e, s) = engine().await;
    for template in TEXT_ONLY {
        let n = template.matches('?').count();
        assert!(
            e.prepare_plan(&render(template, &vec!["1"; n], true), n, &s)
                .is_none(),
            "{template}"
        );
    }
}
