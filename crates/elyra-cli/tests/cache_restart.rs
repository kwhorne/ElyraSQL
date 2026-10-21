//! The engine caches a table's columns (and zone maps and a row-count
//! estimate) tagged with the storage write sequence, and serves an entry while
//! the committed sequence still equals its tag. The sequence used to restart at
//! 1 with every process, so after a restart it ran through values the data file
//! had already had: an aggregate cached right after the restart was served
//! again, stale, once that many writes had gone by.

use mysql_async::prelude::*;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

async fn start(data: &Path) -> (Server, mysql_async::Conn) {
    let port = free_port();
    let child = Command::new(BIN)
        .args(["serve", "--listen", &format!("127.0.0.1:{port}")])
        .arg("--data")
        .arg(data)
        .args(["--user", "root", "--password", "rootpw"])
        .env("ELYRASQL_COLUMN_CACHE_MB", "64")
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn elyrasql");
    let server = Server(child);
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("rootpw"))
        .db_name(Some("elyra"))
        .prefer_socket(false);
    for _ in 0..200 {
        if let Ok(c) = mysql_async::Conn::new(opts.clone()).await {
            return (server, c);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("could not connect to 127.0.0.1:{port}");
}

async fn count_sum(c: &mut mysql_async::Conn) -> (i64, i64) {
    let row: mysql_async::Row = c
        .query_first("SELECT COUNT(*), SUM(v) FROM t")
        .await
        .unwrap()
        .unwrap();
    (row.get(0).unwrap(), row.get(1).unwrap())
}

fn temp(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "elyrasql-cache-restart-{}-{name}.edb",
        std::process::id()
    ))
}

#[tokio::test]
async fn a_restart_does_not_bring_back_a_cached_aggregate() {
    let base = temp("base");
    let _ = std::fs::remove_file(&base);
    {
        let (_server, mut c) = start(&base).await;
        for sql in [
            "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
            "CREATE TABLE u (id INT PRIMARY KEY)",
        ] {
            c.query_drop(sql).await.unwrap();
        }
        // Enough writes that a restarted counter has room to climb back to here.
        for i in 0..20 {
            c.query_drop(format!("INSERT INTO u VALUES ({i})"))
                .await
                .unwrap();
        }
    }
    let persisted = elyra_storage::Storage::open(&base)
        .unwrap()
        .write_epoch()
        .unwrap();

    // Each trial: restart, cache `t`'s aggregate, change `t`, then make `k`
    // unrelated writes before reading `t` again. A counter that restarted
    // lands on `persisted` -- the cached entry's tag -- for one of these `k`.
    let trial = temp("trial");
    for k in 0..persisted {
        let _ = std::fs::remove_file(&trial);
        std::fs::copy(&base, &trial).unwrap();
        let (_server, mut c) = start(&trial).await;
        assert_eq!(count_sum(&mut c).await, (3, 60));
        c.query_drop("INSERT INTO t VALUES (4, 40)").await.unwrap();
        for i in 0..k {
            c.query_drop(format!("INSERT INTO u VALUES ({})", 1000 + i))
                .await
                .unwrap();
        }
        assert_eq!(
            count_sum(&mut c).await,
            (4, 100),
            "stale aggregate after {k} more writes (persisted wseq {persisted})"
        );
    }
    let _ = std::fs::remove_file(&trial);
    let _ = std::fs::remove_file(&base);
}
