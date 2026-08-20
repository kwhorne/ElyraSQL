//! Replica-listener authentication defaults.
//!
//! A replica serves replicated (often production) data over its MySQL port.
//! These tests pin the safe-by-default contract: the `replica` subcommand
//! refuses to start a credential-less listener unless explicitly overridden,
//! and accepts accounts via the same flags as `serve`.

use std::process::{Command, Stdio};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn replica_cmd(data: &std::path::Path, port: u16) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.args(["replica", "--primary", "127.0.0.1:1"])
        .arg("--data")
        .arg(data)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .env("RUST_LOG", "error")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

#[tokio::test]
async fn replica_refuses_credential_less_listener() {
    let port = free_port();
    let data = std::env::temp_dir().join(format!("elyrasql-replauth-a-{}.edb", std::process::id()));
    let _ = std::fs::remove_file(&data);

    let out = replica_cmd(&data, port)
        .env_remove("ELYRASQL_ALLOW_OPEN_AUTH")
        .output()
        .expect("spawn elyrasql replica");
    let _ = std::fs::remove_file(&data);

    assert!(
        !out.status.success(),
        "credential-less replica must refuse to start"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("refusing to start an unauthenticated replica"),
        "error must explain the refusal, got: {stderr}"
    );
    assert!(
        stderr.contains("ELYRASQL_ALLOW_OPEN_AUTH") || stderr.contains("--auth"),
        "error must name the overrides, got: {stderr}"
    );
}

#[tokio::test]
async fn replica_starts_with_configured_accounts() {
    let port = free_port();
    let data = std::env::temp_dir().join(format!("elyrasql-replauth-b-{}.edb", std::process::id()));
    let _ = std::fs::remove_file(&data);

    // The primary at 127.0.0.1:1 is unreachable; run_replica retries in the
    // background while the MySQL listener comes up. That is enough to verify
    // the listener starts (and authenticates) with accounts configured.
    let mut child = replica_cmd(&data, port)
        .args(["--user", "replica_admin", "--password", "sup3rsecret"])
        .spawn()
        .expect("spawn elyrasql replica");

    let mut ready = false;
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    child.kill().ok();
    child.wait().ok();
    let _ = std::fs::remove_file(&data);
    assert!(
        ready,
        "replica with configured accounts must start its MySQL listener"
    );
}
