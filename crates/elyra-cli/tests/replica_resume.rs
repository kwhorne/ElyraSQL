//! A restarted replica resumes from the last write it applied instead of
//! copying the whole database again, and starts over only when its data cannot
//! be continued.
//!
//! A replica used to delete its file on every start, so any restart copied the
//! whole database from the primary. And when the primary could not continue
//! the replica's history -- it restarted without a binlog, so its LSNs began
//! again -- a reconnecting replica applied a full snapshot over its old data:
//! rows the primary had deleted in the meantime stayed on the replica.

use mysql_async::prelude::*;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");
const SECRET: &str = "replica-resume-test-secret";

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

struct Proc(Child);

impl Proc {
    /// Wait for the process to exit on its own, as a supervisor would see it.
    fn exit_code(&mut self, within: Duration) -> Option<i32> {
        let deadline = std::time::Instant::now() + within;
        while std::time::Instant::now() < deadline {
            if let Some(status) = self.0.try_wait().unwrap() {
                return status.code();
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        None
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

/// Start `elyrasql` with `args`, appending its log to `log`. Does not touch
/// the data file: what is on disk is what a restart finds.
fn start(args: &[&str], log: &Path) -> Proc {
    let out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .unwrap();
    let child = Command::new(BIN)
        .args(args)
        .args(["--user", "root", "--password", "rootpw"])
        .env("ELYRASQL_CLUSTER_SECRET", SECRET)
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .stdout(out.try_clone().unwrap())
        .stderr(out)
        .spawn()
        .expect("spawn elyrasql");
    Proc(child)
}

/// Connect to the server on `port`. A loaded CI runner can take a while to
/// start one, so this waits up to 30 s, and then fails with every process's
/// log: the bare "could not connect" it used to give said nothing about why.
async fn connect(f: &Files, port: u16) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("rootpw"))
        .prefer_socket(false);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last = None;
    while std::time::Instant::now() < deadline {
        match mysql_async::Conn::new(opts.clone()).await {
            Ok(c) => return c,
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "could not connect to 127.0.0.1:{port} within 30 s (last error: {last:?})\n{}",
        f.logs()
    );
}

/// `(rows, sum of ids)` of `t`, or `None` while the table is not there yet.
async fn shape(f: &Files, port: u16) -> Option<(i64, i64)> {
    let mut c = connect(f, port).await;
    c.query_first("SELECT COUNT(*), COALESCE(SUM(id), 0) FROM t")
        .await
        .ok()
        .flatten()
}

async fn until_same(f: &Files, primary: u16, replica: u16) -> (i64, i64) {
    let want = shape(f, primary).await.unwrap();
    for _ in 0..200 {
        if shape(f, replica).await == Some(want) {
            return want;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "replica never matched the primary: primary {want:?}, replica {:?}\n{}",
        shape(f, replica).await,
        f.logs()
    );
}

struct Files {
    dir: PathBuf,
}

impl Files {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("elyrasql-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Files { dir }
    }
    fn path(&self, name: &str) -> String {
        self.dir.join(name).to_string_lossy().into_owned()
    }
    fn log(&self, name: &str) -> String {
        std::fs::read_to_string(self.dir.join(name)).unwrap_or_default()
    }
    /// The primary's and the replica's logs, for a failure message.
    fn logs(&self) -> String {
        format!(
            "--- primary log ---\n{}--- replica log ---\n{}",
            self.log("p.log"),
            self.log("r.log")
        )
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn primary_args<'a>(f: &'a Files, mysql: &'a str, repl: &'a str, binlog: bool) -> Vec<String> {
    let mut a = vec![
        "serve".to_string(),
        "--data".into(),
        f.path("primary.edb"),
        "--listen".into(),
        mysql.into(),
        "--replication-listen".into(),
        repl.into(),
    ];
    if binlog {
        a.extend(["--binlog".into(), f.path("binlog")]);
    }
    a
}

fn replica_args(f: &Files, primary_repl: &str, mysql: &str) -> Vec<String> {
    vec![
        "replica".into(),
        "--primary".into(),
        primary_repl.into(),
        "--data".into(),
        f.path("replica.edb"),
        "--listen".into(),
        mysql.into(),
    ]
}

fn strs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_replica_catches_up_from_where_it_stopped() {
    let f = Files::new("replresume");
    let (pm, pr, rm) = (free_port(), free_port(), free_port());
    let (pm_a, pr_a, rm_a) = (
        format!("127.0.0.1:{pm}"),
        format!("127.0.0.1:{pr}"),
        format!("127.0.0.1:{rm}"),
    );
    let _primary = start(
        &strs(&primary_args(&f, &pm_a, &pr_a, true)),
        &f.dir.join("p.log"),
    );
    let mut p = connect(&f, pm).await;
    p.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    for i in 1..=100 {
        p.exec_drop("INSERT INTO t VALUES (?, ?)", (i, i))
            .await
            .unwrap();
    }
    let replica = start(&strs(&replica_args(&f, &pr_a, &rm_a)), &f.dir.join("r.log"));
    assert_eq!(until_same(&f, pm, rm).await, (100, 5050));
    drop(replica);

    // Writes the stopped replica misses, deletes among them.
    p.query_drop("DELETE FROM t WHERE id <= 10").await.unwrap();
    for i in 101..=150 {
        p.exec_drop("INSERT INTO t VALUES (?, ?)", (i, i))
            .await
            .unwrap();
    }
    let _replica = start(&strs(&replica_args(&f, &pr_a, &rm_a)), &f.dir.join("r.log"));
    assert_eq!(until_same(&f, pm, rm).await, (140, 11270));

    let log = f.log("r.log");
    assert!(
        log.contains("resuming replica from its applied LSN"),
        "{log}"
    );
    assert!(
        log.matches("bootstrapping the replica from scratch")
            .count()
            <= 1,
        "only the first start bootstraps: {log}"
    );
    let primary_log = f.log("p.log");
    assert!(
        primary_log.contains("replica caught up incrementally from binlog"),
        "{primary_log}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_starts_over_when_the_primary_cannot_continue_it() {
    // Without a binlog the primary's LSNs begin again when it restarts, so it
    // cannot tell the replica what changed: the replica must bootstrap again,
    // and must not keep rows deleted in the meantime.
    let f = Files::new("replrestart");
    let (pm, pr, rm) = (free_port(), free_port(), free_port());
    let (pm_a, pr_a, rm_a) = (
        format!("127.0.0.1:{pm}"),
        format!("127.0.0.1:{pr}"),
        format!("127.0.0.1:{rm}"),
    );
    let primary = start(
        &strs(&primary_args(&f, &pm_a, &pr_a, false)),
        &f.dir.join("p.log"),
    );
    let mut p = connect(&f, pm).await;
    p.query_drop("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    for i in 1..=100 {
        p.exec_drop("INSERT INTO t VALUES (?, ?)", (i, i))
            .await
            .unwrap();
    }
    let mut replica = start(&strs(&replica_args(&f, &pr_a, &rm_a)), &f.dir.join("r.log"));
    assert_eq!(until_same(&f, pm, rm).await, (100, 5050));

    drop(p);
    drop(primary);
    let _primary = start(
        &strs(&primary_args(&f, &pm_a, &pr_a, false)),
        &f.dir.join("p.log"),
    );
    let mut p = connect(&f, pm).await;
    p.query_drop("DELETE FROM t WHERE id > 50").await.unwrap();

    // The replica gives up on its data and exits for its supervisor...
    let code = replica.exit_code(Duration::from_secs(20));
    assert_eq!(
        code,
        Some(75),
        "replica log:\n{}\nprimary log:\n{}",
        f.log("r.log"),
        f.log("p.log")
    );
    assert!(Path::new(&format!("{}.resync", f.path("replica.edb"))).exists());
    // ...which restarts it: it bootstraps from scratch and matches.
    let _replica = start(&strs(&replica_args(&f, &pr_a, &rm_a)), &f.dir.join("r.log"));
    assert_eq!(until_same(&f, pm, rm).await, (50, 1275));
    assert!(!Path::new(&format!("{}.resync", f.path("replica.edb"))).exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_refuses_queries_until_its_copy_is_complete() {
    // A new replica whose primary cannot be reached yet: its tables are empty,
    // and it used to answer queries from them as if the primary had no rows.
    let f = Files::new("replfence");
    let rm = free_port();
    let rm_a = format!("127.0.0.1:{rm}");
    let nowhere = format!("127.0.0.1:{}", free_port());
    let _replica = start(
        &strs(&replica_args(&f, &nowhere, &rm_a)),
        &f.dir.join("r.log"),
    );
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(rm)
        .user(Some("root"))
        .pass(Some("rootpw"))
        .prefer_socket(false);
    // The client's own setup queries are refused too, with the reason.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let err = loop {
        match mysql_async::Conn::new(opts.clone()).await {
            Ok(mut c) => match c.query_drop("SELECT 1").await {
                Ok(()) => panic!("a replica without its data answered a query"),
                Err(e) => break e,
            },
            Err(e @ mysql_async::Error::Server(_)) => break e,
            Err(_) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("the replica never came up: {e}\n{}", f.logs()),
        }
    };
    assert!(
        err.to_string().contains("copying the primary's data"),
        "{err}"
    );
}
