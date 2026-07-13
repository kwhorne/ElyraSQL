//! Crash-recovery / durability test.
//!
//! Starts the real `elyrasql` binary, commits rows with full (fsync-per-commit)
//! durability, then **SIGKILLs the process** -- an ungraceful crash, no
//! shutdown hooks -- restarts it on the same data file, and verifies the
//! committed rows survived. This guards the single-file ACID promise against
//! regressions.

use std::process::{Child, Command};
use std::time::Duration;

use mysql_async::prelude::*;

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn spawn(data: &std::path::Path, port: u16) -> Child {
    Command::new(BIN)
        .args(["serve", "--data"])
        .arg(data)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .env("ELYRASQL_SYNC", "full") // fsync every commit
        .env("RUST_LOG", "error")
        .spawn()
        .expect("spawn elyrasql")
}

async fn wait_ready(port: u16) {
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            // TCP accept doesn't mean the handshake is ready; give it a beat.
            tokio::time::sleep(Duration::from_millis(100)).await;
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server on port {port} never became ready");
}

async fn conn(port: u16) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .prefer_socket(false);
    mysql_async::Conn::new(opts).await.expect("connect")
}

#[tokio::test]
async fn committed_rows_survive_sigkill() {
    let port = free_port();
    let data = std::env::temp_dir().join(format!("elyrasql-durability-{}.edb", std::process::id()));
    let _ = std::fs::remove_file(&data);

    // --- first run: write and commit ---
    let mut child = spawn(&data, port);
    wait_ready(port).await;

    {
        let mut c = conn(port).await;
        c.query_drop("CREATE TABLE ledger (id INT PRIMARY KEY, amount INT)")
            .await
            .unwrap();
        for i in 1..=50 {
            c.query_drop(format!("INSERT INTO ledger VALUES ({i}, {})", i * 100))
                .await
                .unwrap();
        }
        // read-back confirms they are committed before we crash
        let n: i64 = c
            .query_first("SELECT COUNT(*) FROM ledger")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 50);
        drop(c);
    }

    // --- ungraceful crash: SIGKILL (std kill() sends SIGKILL on Unix) ---
    child.kill().expect("kill");
    let _ = child.wait();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- second run: same data file, verify survival ---
    let mut child2 = spawn(&data, port);
    wait_ready(port).await;

    {
        let mut c = conn(port).await;
        let n: i64 = c
            .query_first("SELECT COUNT(*) FROM ledger")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 50, "committed rows must survive an ungraceful crash");

        let sum: i64 = c
            .query_first("SELECT SUM(amount) FROM ledger")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sum, (1..=50).map(|i| i * 100).sum::<i64>());
        drop(c);
    }

    let _ = child2.kill();
    let _ = child2.wait();
    let _ = std::fs::remove_file(&data);
}
