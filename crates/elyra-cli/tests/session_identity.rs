//! `USER()`, `CURRENT_USER()` and `CONNECTION_ID()` answer for the connection
//! that asks. They used to answer `root@%` and `1` whoever was connected, so a
//! view that showed each user their own rows showed nobody anything, and an
//! audit column recorded root for every change. Checked over the wire, as an
//! application sees it, against what MySQL 8.4 answers in the same places.

use mysql_async::prelude::*;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

struct Server(Child, std::path::PathBuf);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
        let _ = std::fs::remove_file(&self.1);
    }
}

async fn connect(port: u16, user: &str, password: &str) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password))
        .db_name(Some("elyra"))
        .prefer_socket(false);
    for _ in 0..200 {
        if let Ok(c) = mysql_async::Conn::new(opts.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("could not connect to 127.0.0.1:{port} as {user}");
}

#[tokio::test]
async fn identity_functions_answer_for_the_connection() {
    let data = std::env::temp_dir().join(format!("elyrasql-identity-{}.edb", std::process::id()));
    let _ = std::fs::remove_file(&data);
    let port = free_port();
    let child = Command::new(BIN)
        .args(["serve", "--listen", &format!("127.0.0.1:{port}")])
        .arg("--data")
        .arg(&data)
        .args(["--user", "root", "--password", "rootpw"])
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn elyrasql");
    let _server = Server(child, data);

    let mut root = connect(port, "root", "rootpw").await;
    for sql in [
        "CREATE TABLE docs (id INT PRIMARY KEY, owner VARCHAR(32))",
        "INSERT INTO docs VALUES (1, 'alice'), (2, 'bob')",
        // Each user sees their own rows, and has no right to the table itself.
        "CREATE VIEW my_docs AS SELECT id FROM docs WHERE owner = SUBSTRING_INDEX(USER(), '@', 1)",
        "CREATE TABLE audit (id INT PRIMARY KEY, who VARCHAR(100) DEFAULT (CURRENT_USER()), \
         by_trigger VARCHAR(100))",
        "CREATE TRIGGER audit_by BEFORE INSERT ON audit FOR EACH ROW SET NEW.by_trigger = CURRENT_USER()",
        "CREATE USER alice IDENTIFIED BY 'Passw0rd!x'",
        "REVOKE ALL PRIVILEGES ON *.* FROM alice",
        "GRANT SELECT ON elyra.my_docs TO alice",
        "GRANT SELECT, INSERT ON elyra.audit TO alice",
    ] {
        root.query_drop(sql).await.unwrap();
    }

    let mut alice = connect(port, "alice", "Passw0rd!x").await;
    let (user, current): (String, String) = alice
        .query_first("SELECT USER(), CURRENT_USER")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(user, "alice@127.0.0.1");
    assert_eq!(current, "alice@%");

    // A prepared statement (binary protocol) answers the same.
    let prepared: Option<String> = alice.exec_first("SELECT USER()", ()).await.unwrap();
    assert_eq!(prepared.as_deref(), Some("alice@127.0.0.1"));

    let mine: Vec<i64> = alice.query("SELECT id FROM my_docs").await.unwrap();
    assert_eq!(mine, vec![1], "the view must show alice her own rows");

    // A column default records who inserted; a trigger runs as its definer.
    alice
        .query_drop("INSERT INTO audit (id) VALUES (1)")
        .await
        .unwrap();
    let (who, by_trigger): (String, String) = root
        .query_first("SELECT who, by_trigger FROM audit")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(who, "alice@%");
    assert_eq!(by_trigger, "root@%");

    // Each connection has its own id, the one SHOW PROCESSLIST lists.
    let alice_id: u64 = alice
        .query_first("SELECT CONNECTION_ID()")
        .await
        .unwrap()
        .unwrap();
    let root_id: u64 = root
        .query_first("SELECT CONNECTION_ID()")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(alice_id, root_id);
    let listed: Vec<mysql_async::Row> = root.query("SHOW PROCESSLIST").await.unwrap();
    let ids: Vec<u64> = listed
        .iter()
        .map(|r| r.get::<String, _>(0).unwrap().parse().unwrap())
        .collect();
    assert!(ids.contains(&alice_id), "{alice_id} not in {ids:?}");
}
