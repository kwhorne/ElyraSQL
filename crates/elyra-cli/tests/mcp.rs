//! `elyrasql mcp` driven the way a client drives it: the real binary, real
//! stdio, real JSON-RPC.
//!
//! Spawning the binary rather than calling the module keeps two things under
//! test that a unit test cannot see — that the protocol owns stdout with nothing
//! else written to it, and that the subcommand wires the engine up correctly.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

/// Run a session: send `requests`, return one parsed response per request that
/// had an id. `initialize` is always sent first and its response dropped.
fn session(
    data: &std::path::Path,
    extra_args: &[&str],
    requests: &[String],
) -> Vec<serde_json::Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_elyrasql"))
        .arg("mcp")
        .arg("--data")
        .arg(data)
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn elyrasql mcp");

    {
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(
            stdin,
            r#"{{"jsonrpc":"2.0","id":0,"method":"initialize","params":{{"protocolVersion":"2025-06-18","capabilities":{{}}}}}}"#
        )
        .unwrap();
        for r in requests {
            writeln!(stdin, "{r}").unwrap();
        }
    }
    // Dropping stdin closes it, which is how the server is told to stop.
    drop(child.stdin.take());

    let stdout = child.stdout.take().expect("stdout");
    let mut lines = Vec::new();
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("read line");
        if line.trim().is_empty() {
            continue;
        }
        lines.push(serde_json::from_str::<serde_json::Value>(&line).expect("valid JSON per line"));
    }
    child.wait().expect("wait");
    lines
}

/// A database with two tables, created through the engine so the file is real.
fn seeded(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elyra_mcp_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("m.edb");
    std::fs::remove_file(&path).ok();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let engine = elyra_engine::Engine::new(elyra_storage::Db::open(&path).unwrap());
        let session = engine.session();
        for sql in [
            "CREATE TABLE authors (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT NOT NULL)",
            "INSERT INTO authors (name) VALUES ('Ada'), ('Grace')",
            "CREATE TABLE books (id INT PRIMARY KEY, title TEXT, price DECIMAL(6,2))",
            "INSERT INTO books VALUES (1, 'Notes', 24.50)",
        ] {
            engine
                .execute(sql, elyra_core::Privilege::Admin, &session)
                .await
                .unwrap();
        }
        // Every handle has to go before the file is released -- the session
        // holds its own clone of the database, not just the engine.
        let closed = engine.db().close_waiter();
        drop(session);
        drop(engine);
        assert!(
            closed.wait(std::time::Duration::from_secs(5)),
            "the seeded file must be released before the child opens it"
        );
    });
    path
}

/// The text of a `tools/call` result, and whether it was reported as an error.
fn tool_result(response: &serde_json::Value) -> (bool, String) {
    let result = &response["result"];
    (
        result["isError"].as_bool().unwrap_or(false),
        result["content"][0]["text"]
            .as_str()
            .unwrap_or("")
            .to_string(),
    )
}

fn call(name: &str, args: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"{name}","arguments":{args}}}}}"#
    )
}

#[test]
fn it_handshakes_and_advertises_its_tools() {
    let data = seeded("handshake");
    let out = session(
        &data,
        &[],
        &[r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_string()],
    );

    let init = &out[0]["result"];
    assert!(init["protocolVersion"].is_string());
    assert_eq!(init["serverInfo"]["name"], "ElyraSQL");
    assert!(init["capabilities"]["tools"].is_object());
    assert!(
        init["instructions"].as_str().unwrap().contains("read-only"),
        "the handshake should tell the model it cannot write"
    );

    let names: Vec<&str> = out[1]["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["list_tables", "describe_table", "query"]);

    std::fs::remove_dir_all(data.parent().unwrap()).ok();
}

#[test]
fn it_introspects_and_queries() {
    let data = seeded("query");
    let out = session(
        &data,
        &[],
        &[
            call("list_tables", "{}"),
            call("describe_table", r#"{"table":"books"}"#),
            call("query", r#"{"sql":"SELECT name FROM authors ORDER BY id"}"#),
        ],
    );

    let (_, tables) = tool_result(&out[1]);
    assert!(
        tables.contains("authors") && tables.contains("books"),
        "{tables}"
    );

    let (_, described) = tool_result(&out[2]);
    assert!(described.contains("price"), "{described}");

    let (is_error, rows) = tool_result(&out[3]);
    assert!(!is_error, "{rows}");
    let parsed: serde_json::Value = serde_json::from_str(&rows).unwrap();
    assert_eq!(parsed["row_count"], 2);
    assert_eq!(parsed["rows"][0]["name"], "Ada");

    std::fs::remove_dir_all(data.parent().unwrap()).ok();
}

/// DECIMAL must not be rendered through a float. An agent reading prices should
/// see what the column holds, exactly.
#[test]
fn decimal_keeps_its_scale() {
    let data = seeded("decimal");
    let out = session(
        &data,
        &[],
        &[call("query", r#"{"sql":"SELECT price FROM books"}"#)],
    );
    let (_, text) = tool_result(&out[1]);
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["rows"][0]["price"], "24.50");
    std::fs::remove_dir_all(data.parent().unwrap()).ok();
}

/// Refused by the engine's privilege check, not by inspecting the SQL — which
/// is the only way to refuse a write reliably.
#[test]
fn writes_are_refused_by_default() {
    let data = seeded("readonly");
    let out = session(
        &data,
        &[],
        &[
            call("query", r#"{"sql":"DELETE FROM authors"}"#),
            call("query", r#"{"sql":"SELECT COUNT(*) AS n FROM authors"}"#),
        ],
    );

    let (is_error, message) = tool_result(&out[1]);
    assert!(is_error, "a write must be refused");
    assert!(message.contains("WRITE privilege"), "{message}");

    // And nothing happened.
    let (_, rows) = tool_result(&out[2]);
    assert!(rows.contains("\"n\":2"), "{rows}");

    std::fs::remove_dir_all(data.parent().unwrap()).ok();
}

/// `--allow-writes` grants `Write`, deliberately not `Admin`: rows may change,
/// but the table they live in cannot be dropped and privileges cannot be
/// granted.
#[test]
fn allow_writes_permits_dml_but_not_ddl_or_grants() {
    let data = seeded("writes");
    let out = session(
        &data,
        &["--allow-writes"],
        &[
            call(
                "query",
                r#"{"sql":"INSERT INTO authors (name) VALUES ('Barbara')"}"#,
            ),
            call("query", r#"{"sql":"DROP TABLE books"}"#),
            call("query", r#"{"sql":"GRANT ALL ON *.* TO agent"}"#),
        ],
    );

    let (is_error, inserted) = tool_result(&out[1]);
    assert!(!is_error, "{inserted}");
    assert!(inserted.contains("\"affected_rows\":1"), "{inserted}");

    for (index, what) in [(2usize, "DROP TABLE"), (3, "GRANT")] {
        let (is_error, message) = tool_result(&out[index]);
        assert!(is_error, "{what} should be refused");
        assert!(message.contains("ADMIN"), "{what}: {message}");
    }

    std::fs::remove_dir_all(data.parent().unwrap()).ok();
}

/// A failing tool is a *result*, so the model can read the message and retry.
/// A missing method is a transport error, because the client got it wrong.
#[test]
fn failures_reach_the_right_layer() {
    let data = seeded("failures");
    let out = session(
        &data,
        &[],
        &[
            call("query", r#"{"sql":"SELECT * FROM no_such_table"}"#),
            call("query", "{}"),
            call("nonexistent_tool", "{}"),
            r#"{"jsonrpc":"2.0","id":1,"method":"resources/list"}"#.to_string(),
            "not json at all".to_string(),
        ],
    );

    for (index, expected) in [
        (1usize, "no such table"),
        (2, "missing required argument"),
        (3, "unknown tool"),
    ] {
        let (is_error, message) = tool_result(&out[index]);
        assert!(is_error, "response {index} should be a tool error");
        assert!(message.contains(expected), "response {index}: {message}");
    }

    assert_eq!(out[4]["error"]["code"], -32601, "unknown method");
    assert_eq!(out[5]["error"]["code"], -32700, "malformed JSON");

    std::fs::remove_dir_all(data.parent().unwrap()).ok();
}

#[test]
fn large_results_are_truncated_and_say_so() {
    let data = seeded("truncate");
    let out = session(
        &data,
        &[],
        &[call(
            "query",
            r#"{"sql":"SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3","max_rows":2}"#,
        )],
    );
    let (_, text) = tool_result(&out[1]);
    let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(parsed["row_count"], 2);
    assert_eq!(parsed["truncated"], true);
    assert!(parsed["note"].as_str().unwrap().contains("max_rows"));
    std::fs::remove_dir_all(data.parent().unwrap()).ok();
}

/// A notification carries no id and must draw no response at all; answering one
/// desynchronises a client that is counting replies.
#[test]
fn notifications_are_not_answered() {
    let data = seeded("notify");
    let out = session(
        &data,
        &[],
        &[
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.to_string(),
            r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#.to_string(),
        ],
    );
    // initialize + ping only.
    assert_eq!(out.len(), 2, "got {out:#?}");
    assert!(out[1]["result"].is_object());
    std::fs::remove_dir_all(data.parent().unwrap()).ok();
}
