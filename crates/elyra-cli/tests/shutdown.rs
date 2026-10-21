//! SIGTERM is how `docker stop`, systemd and Kubernetes stop a service. The
//! server handled only SIGINT: as PID 1 in a container it ignored SIGTERM until
//! `docker stop` killed it, and anywhere else SIGTERM ended it on the spot,
//! reported as a crash (killed by a signal) rather than a clean stop.

use mysql_async::prelude::*;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn spawn(data: &std::path::Path, port: u16, env: &[(&str, &str)]) -> Child {
    Command::new(BIN)
        .args(["serve", "--listen", &format!("127.0.0.1:{port}")])
        .arg("--data")
        .arg(data)
        .args(["--user", "root", "--password", "rootpw"])
        .envs(env.iter().copied())
        .env("RUST_LOG", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn elyrasql")
}

async fn connect(port: u16) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("rootpw"))
        .db_name(Some("elyra"))
        .prefer_socket(false);
    for _ in 0..200 {
        if let Ok(c) = mysql_async::Conn::new(opts.clone()).await {
            return c;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("could not connect to 127.0.0.1:{port}");
}

#[tokio::test]
async fn sigterm_stops_the_server_cleanly_and_keeps_its_commits() {
    let data = std::env::temp_dir().join(format!("elyrasql-sigterm-{}.edb", std::process::id()));
    let _ = std::fs::remove_file(&data);

    // Relaxed durability, with the periodic flush far off: the commits are
    // still unflushed when the signal arrives, and stopping must not lose them.
    let port = free_port();
    let mut server = spawn(
        &data,
        port,
        &[
            ("ELYRASQL_SYNC", "normal"),
            ("ELYRASQL_SYNC_INTERVAL_MS", "10000"),
        ],
    );
    let mut c = connect(port).await;
    c.query_drop("CREATE TABLE t (id INT PRIMARY KEY)")
        .await
        .unwrap();
    for i in 0..50 {
        c.query_drop(format!("INSERT INTO t VALUES ({i})"))
            .await
            .unwrap();
    }
    drop(c);

    let sent = Command::new("kill")
        .args(["-TERM", &server.id().to_string()])
        .status()
        .unwrap();
    assert!(sent.success());
    let started = Instant::now();
    let status = loop {
        if let Some(status) = server.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > Duration::from_secs(10) {
            server.kill().ok();
            panic!("the server did not stop on SIGTERM");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(status.success(), "SIGTERM ended the server with {status}");

    let port = free_port();
    let mut server = spawn(&data, port, &[]);
    let mut c = connect(port).await;
    let n: Option<i64> = c.query_first("SELECT COUNT(*) FROM t").await.unwrap();
    server.kill().ok();
    server.wait().ok();
    let _ = std::fs::remove_file(&data);
    assert_eq!(n, Some(50), "commits acknowledged before SIGTERM were lost");
}
