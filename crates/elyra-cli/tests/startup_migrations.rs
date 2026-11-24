//! Every way of starting a server on a data file brings it up to the current
//! format first: `cluster` and `replica` used to skip the migrations `serve`
//! runs, so a node or replica upgraded from before 1.12.17 kept indexes over
//! FLOAT/DOUBLE and binary columns that held none of their rows.

mod ports;

use elyra_engine::Engine;
use elyra_storage::Db;
use mysql_async::prelude::*;
use ports::free_port;
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");

/// A directory of its own for one test, removed when it ends.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("elyrasql-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

/// A data file as a version before 1.12.17 left it: a DOUBLE index without
/// entries for its rows, and no marker saying they were filled in.
async fn old_database(path: &Path, replica_lsn: Option<u64>) {
    let db = Db::open(path).unwrap();
    let engine = Engine::new(db.clone());
    let sess = engine.session();
    for sql in [
        "CREATE TABLE t (id INT PRIMARY KEY, price DOUBLE, KEY by_price (price))",
        "INSERT INTO t VALUES (1, 3.5), (2, 1.5), (3, 2.5)",
    ] {
        engine
            .execute(sql, elyra_core::Privilege::Admin, &sess)
            .await
            .unwrap();
    }
    let index_keys: Vec<Vec<u8>> = db
        .snapshot()
        .unwrap()
        .scan_range(b"index::", Some(b"index:;"), 1000)
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert!(!index_keys.is_empty());
    let mut deletes = index_keys;
    deletes.push(b"meta::index_key_version".to_vec());
    let puts = match replica_lsn {
        Some(lsn) => vec![(b"meta::repl::lsn".to_vec(), lsn.to_le_bytes().to_vec())],
        None => Vec::new(),
    };
    db.commit(puts, deletes).await.unwrap();
    let closed = db.close_waiter();
    drop(sess);
    drop(engine);
    drop(db);
    assert!(closed.wait(Duration::from_secs(10)));
}

/// The ids of `t` by price, from the server on `port`, once it answers.
async fn by_price(port: u16, log: &Path) -> Vec<i64> {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .pass(Some("rootpw"))
        .prefer_socket(false);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut c) = mysql_async::Conn::new(opts.clone()).await {
            if let Ok(ids) = c
                .query::<i64, _>("SELECT id FROM t ORDER BY price LIMIT 10")
                .await
            {
                return ids;
            }
        }
        assert!(
            Instant::now() < deadline,
            "the server never answered\n{}",
            std::fs::read_to_string(log).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn spawn(args: &[String], log: &Path) -> Proc {
    let out = std::fs::File::create(log).unwrap();
    Proc(
        Command::new(BIN)
            .args(args)
            .env("ELYRASQL_CLUSTER_SECRET", "startup-migrations-secret")
            .env("NO_COLOR", "1")
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .spawn()
            .expect("spawn elyrasql"),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_node_migrates_its_data_at_start() {
    let dir = TempDir::new("migrate-cluster");
    let data = dir.path().join("n1.edb");
    old_database(&data, None).await;
    let (mysql, control, repl) = (free_port(), free_port(), free_port());
    let log = dir.path().join("n1.log");
    let _node = spawn(
        &[
            "cluster".into(),
            "--id".into(),
            "1".into(),
            "--data".into(),
            data.display().to_string(),
            "--listen".into(),
            format!("127.0.0.1:{mysql}"),
            "--control-listen".into(),
            format!("127.0.0.1:{control}"),
            "--replication-listen".into(),
            format!("127.0.0.1:{repl}"),
        ],
        &log,
    );
    // A single node: it leads itself and answers reads at once.
    assert_eq!(by_price(mysql, &log).await, [2, 3, 1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_replica_migrates_its_data_at_start() {
    let dir = TempDir::new("migrate-replica");
    let data = dir.path().join("replica.edb");
    // A replica that completed a bootstrap: it resumes this file.
    old_database(&data, Some(3)).await;
    let mysql = free_port();
    let log = dir.path().join("replica.log");
    let _replica = spawn(
        &[
            "replica".into(),
            "--primary".into(),
            // Nobody there: the replica keeps trying, and serves its data.
            format!("127.0.0.1:{}", free_port()),
            "--data".into(),
            data.display().to_string(),
            "--listen".into(),
            format!("127.0.0.1:{mysql}"),
            "--user".into(),
            "root".into(),
            "--password".into(),
            "rootpw".into(),
        ],
        &log,
    );
    assert_eq!(by_price(mysql, &log).await, [2, 3, 1]);
}
