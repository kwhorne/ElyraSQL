//! A replica applies its primary's writes below the SQL session, so any cache
//! the engine keeps of the schema must be invalidated by the storage layer, not
//! only by statements. Before that, a replica kept serving a table's old
//! definition after the primary's `ALTER TABLE`, and did not enforce a column
//! grant created after it had looked for one -- until it was restarted.

use mysql_async::prelude::*;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");
const SECRET: &str = "replica-schema-test-secret";

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

struct Proc(Child, std::path::PathBuf);

impl Drop for Proc {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
        let _ = std::fs::remove_file(&self.1);
    }
}

fn spawn(args: &[&str], data: std::path::PathBuf) -> Proc {
    let _ = std::fs::remove_file(&data);
    let child = Command::new(BIN)
        .args(args)
        .arg("--data")
        .arg(&data)
        .args(["--user", "root", "--password", "rootpw"])
        .env("ELYRASQL_CLUSTER_SECRET", SECRET)
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn elyrasql");
    Proc(child, data)
}

async fn connect(port: u16, user: &str, password: &str) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some(user))
        .pass(Some(password))
        .prefer_socket(false);
    for _ in 0..200 {
        if let Ok(c) = mysql_async::Conn::new(opts.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("could not connect to 127.0.0.1:{port} as {user}");
}

/// Poll until `$check` holds (replication is asynchronous).
macro_rules! eventually {
    ($what:literal, $check:expr) => {{
        let mut held = false;
        for _ in 0..100 {
            if $check {
                held = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(held, concat!("replica never ", $what));
    }};
}

#[tokio::test]
async fn a_replica_follows_ddl_and_grants_made_after_it_looked() {
    let (mysql_p, repl_p, mysql_r) = (free_port(), free_port(), free_port());
    let tmp = std::env::temp_dir();
    let pid = std::process::id();
    let _primary = spawn(
        &[
            "serve",
            "--listen",
            &format!("127.0.0.1:{mysql_p}"),
            "--replication-listen",
            &format!("127.0.0.1:{repl_p}"),
        ],
        tmp.join(format!("elyrasql-replschema-p-{pid}.edb")),
    );
    let mut p = connect(mysql_p, "root", "rootpw").await;
    for sql in [
        "CREATE TABLE rt (id INT PRIMARY KEY, public TEXT, secret TEXT)",
        "INSERT INTO rt VALUES (1, 'hello', 'classified')",
        "CREATE USER lim IDENTIFIED BY 'passw0rd'",
    ] {
        p.query_drop(sql).await.unwrap();
    }
    let _replica = spawn(
        &[
            "replica",
            "--primary",
            &format!("127.0.0.1:{repl_p}"),
            "--listen",
            &format!("127.0.0.1:{mysql_r}"),
        ],
        tmp.join(format!("elyrasql-replschema-r-{pid}.edb")),
    );
    let mut r = connect(mysql_r, "root", "rootpw").await;
    eventually!(
        "received the table",
        r.query_drop("SELECT * FROM rt").await.is_ok()
    );
    // The replica has now cached the definition, and found no column grants.
    let mut lim = connect(mysql_r, "lim", "passw0rd").await;
    let secret: Option<String> = lim.query_first("SELECT secret FROM rt").await.unwrap();
    assert_eq!(secret.as_deref(), Some("classified"));

    // DDL on the primary.
    p.query_drop("ALTER TABLE rt ADD COLUMN extra INT DEFAULT 7")
        .await
        .unwrap();
    eventually!(
        "showed the added column",
        r.query_first::<i64, _>("SELECT extra FROM rt WHERE id = 1")
            .await
            .ok()
            .flatten()
            == Some(7)
    );

    // A column restriction on the primary.
    p.query_drop("GRANT SELECT(public) ON rt TO lim")
        .await
        .unwrap();
    eventually!(
        "enforced the column grant",
        lim.query_drop("SELECT secret FROM rt").await.is_err()
    );
    let public: Option<String> = lim.query_first("SELECT public FROM rt").await.unwrap();
    assert_eq!(public.as_deref(), Some("hello"));
}
