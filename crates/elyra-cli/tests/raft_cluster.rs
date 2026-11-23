//! Raft clusters of real `elyrasql cluster` processes: nodes are started,
//! killed and restarted like a deployment would see them.
//!
//! `Cluster` is the harness. It starts nodes on free ports, each with its data
//! in a directory of its own, finds the leader by trying a write on every
//! running node, and keeps each node's log for the failure message.

use mysql_async::prelude::*;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");
const SECRET: &str = "raft-cluster-test-secret";

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

struct Node {
    id: u64,
    mysql: u16,
    control: u16,
    replication: u16,
    child: Option<Child>,
}

struct Cluster {
    dir: PathBuf,
    nodes: Vec<Node>,
    /// Environment every node is started with.
    env: Vec<(String, String)>,
}

impl Cluster {
    /// `n` nodes, configured as peers of each other, not started yet.
    fn new(name: &str, n: u64) -> Self {
        let dir = std::env::temp_dir().join(format!("elyrasql-raft-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut c = Cluster {
            dir,
            nodes: Vec::new(),
            env: Vec::new(),
        };
        for _ in 0..n {
            c.add_node();
        }
        c
    }

    /// Start every node with `name` set to `value`.
    fn with_env(mut self, name: &str, value: &str) -> Self {
        self.env.push((name.into(), value.into()));
        self
    }

    /// Configure one more node (ids count from 1); returns its id.
    fn add_node(&mut self) -> u64 {
        let id = self.nodes.len() as u64 + 1;
        self.nodes.push(Node {
            id,
            mysql: free_port(),
            control: free_port(),
            replication: free_port(),
            child: None,
        });
        id
    }

    fn node(&self, id: u64) -> &Node {
        &self.nodes[(id - 1) as usize]
    }

    /// The directory node `id` keeps its data, Raft log and election state in.
    fn data_dir(&self, id: u64) -> PathBuf {
        self.dir.join(format!("n{id}"))
    }

    fn control_addr(&self, id: u64) -> String {
        format!("127.0.0.1:{}", self.node(id).control)
    }

    /// Start node `id` with every other configured node as its peer.
    fn start(&mut self, id: u64) {
        let peers: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| n.id != id)
            .map(|n| format!("{}@127.0.0.1:{}", n.id, n.control))
            .collect();
        let n = self.node(id);
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(format!("n{id}.log")))
            .unwrap();
        std::fs::create_dir_all(self.data_dir(id)).unwrap();
        let mut cmd = Command::new(BIN);
        cmd.args(["cluster", "--id", &id.to_string()])
            .arg("--data")
            .arg(self.data_dir(id).join("data.edb"))
            .args(["--listen", &format!("127.0.0.1:{}", n.mysql)])
            .args(["--control-listen", &format!("127.0.0.1:{}", n.control)])
            .args([
                "--replication-listen",
                &format!("127.0.0.1:{}", n.replication),
            ]);
        for p in &peers {
            cmd.args(["--peer", p]);
        }
        for (k, v) in &self.env {
            cmd.env(k, v);
        }
        let child = cmd
            .env("ELYRASQL_CLUSTER_SECRET", SECRET)
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .expect("spawn elyrasql cluster");
        self.nodes[(id - 1) as usize].child = Some(child);
    }

    fn start_all(&mut self) {
        for id in 1..=self.nodes.len() as u64 {
            self.start(id);
        }
    }

    /// Kill node `id` without warning (`SIGKILL`).
    fn kill(&mut self, id: u64) {
        if let Some(mut c) = self.nodes[(id - 1) as usize].child.take() {
            c.kill().ok();
            c.wait().ok();
        }
    }

    /// Stop node `id` from writing anything but its database file: its Raft log
    /// and election state can be neither written nor replaced. `false` if the
    /// permissions do not stop this user (root), and nothing changed.
    #[cfg(unix)]
    fn deny_disk(&self, id: u64) -> bool {
        use std::os::unix::fs::PermissionsExt;
        let dir = self.data_dir(id);
        let set = |p: &std::path::Path, mode| {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap()
        };
        for f in ["data.raftlog", "data.raftstate"] {
            if dir.join(f).exists() {
                set(&dir.join(f), 0o444);
            }
        }
        set(&dir, 0o555);
        if std::fs::write(dir.join("probe"), b"x").is_ok() {
            let _ = std::fs::remove_file(dir.join("probe"));
            self.allow_disk(id);
            return false;
        }
        true
    }

    #[cfg(unix)]
    fn allow_disk(&self, id: u64) {
        use std::os::unix::fs::PermissionsExt;
        let dir = self.data_dir(id);
        let set = |p: &std::path::Path, mode| {
            std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap()
        };
        set(&dir, 0o755);
        for f in ["data.raftlog", "data.raftstate"] {
            if dir.join(f).exists() {
                set(&dir.join(f), 0o644);
            }
        }
    }

    fn running(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|n| n.child.is_some())
            .map(|n| n.id)
            .collect()
    }

    /// Ask the running cluster to add node `id` to its membership. Only the
    /// leader accepts it; returns the node that did.
    fn join(&self, id: u64) -> u64 {
        let peer = format!("{id}@{}", self.control_addr(id));
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            for via in self.running() {
                if via == id {
                    continue;
                }
                let ok = Command::new(BIN)
                    .args(["cluster-ctl", "--node", &self.control_addr(via)])
                    .args(["--action", "add", "--peer", &peer])
                    .env("ELYRASQL_CLUSTER_SECRET", SECRET)
                    .output()
                    .map(|o| o.status.success())
                    .unwrap_or(false);
                if ok {
                    return via;
                }
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!(
            "no node accepted node {id} into the cluster\n{}",
            self.logs()
        );
    }

    async fn conn(&self, id: u64) -> Option<mysql_async::Conn> {
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname("127.0.0.1")
            .tcp_port(self.node(id).mysql)
            .user(Some("root"))
            .prefer_socket(false);
        tokio::time::timeout(Duration::from_secs(2), mysql_async::Conn::new(opts))
            .await
            .ok()?
            .ok()
    }

    /// Run `sql` on whichever node accepts it as the leader, within `within`.
    /// Returns the node that did.
    async fn write(&self, sql: &str, within: Duration) -> Option<u64> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            for id in self.running() {
                let Some(mut c) = self.conn(id).await else {
                    continue;
                };
                let attempt = tokio::time::timeout(Duration::from_secs(3), c.query_drop(sql)).await;
                if let Ok(Ok(())) = attempt {
                    return Some(id);
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }

    async fn count(&self, id: u64, table: &str) -> Option<i64> {
        let mut c = self.conn(id).await?;
        c.query_first(format!("SELECT COUNT(*) FROM {table}"))
            .await
            .ok()
            .flatten()
    }

    /// Wait until node `id` holds `rows` rows in `table`.
    async fn until_count(&self, id: u64, table: &str, rows: i64) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if self.count(id, table).await == Some(rows) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "node {id} never held {rows} rows in {table} (has {:?})\n{}",
            self.count(id, table).await,
            self.logs()
        );
    }

    fn logs(&self) -> String {
        let mut out = String::new();
        for n in &self.nodes {
            let log = std::fs::read_to_string(self.dir.join(format!("n{}.log", n.id)))
                .unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(25).collect();
            out.push_str(&format!("--- node {} (last lines) ---\n", n.id));
            for line in tail.into_iter().rev() {
                out.push_str(line);
                out.push('\n');
            }
        }
        out
    }

    fn log(&self, id: u64) -> String {
        std::fs::read_to_string(self.dir.join(format!("n{id}.log"))).unwrap_or_default()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for id in self.running() {
            self.kill(id);
        }
        #[cfg(unix)]
        for n in &self.nodes {
            self.allow_disk(n.id);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_replicates_writes_and_survives_losing_its_leader() {
    let mut c = Cluster::new("basic", 3);
    c.start_all();
    let leader = c
        .write(
            "CREATE TABLE t (id INT PRIMARY KEY)",
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    for i in 1..=20 {
        c.write(
            &format!("INSERT INTO t VALUES ({i})"),
            Duration::from_secs(10),
        )
        .await
        .expect("write");
    }
    for id in 1..=3 {
        c.until_count(id, "t", 20).await;
    }
    // A majority remains: a new leader takes the writes and both survivors have
    // everything.
    c.kill(leader);
    for i in 21..=30 {
        c.write(
            &format!("INSERT INTO t VALUES ({i})"),
            Duration::from_secs(20),
        )
        .await
        .unwrap_or_else(|| panic!("no new leader after killing {leader}\n{}", c.logs()));
    }
    for id in c.running() {
        c.until_count(id, "t", 30).await;
    }
    // The old leader comes back as a follower and catches up.
    c.start(leader);
    c.until_count(leader, "t", 30).await;
}

/// Keep only a few entries in each log, so a node that is behind needs a
/// snapshot; send it in small parts, so it takes several.
fn compacting(c: Cluster) -> Cluster {
    c.with_env("ELYRASQL_RAFT_LOG_KEEP_ENTRIES", "3")
        .with_env("ELYRASQL_RAFT_SNAPSHOT_CHUNK_BYTES", "64")
}

async fn insert(c: &Cluster, ids: impl IntoIterator<Item = i64>) {
    for i in ids {
        c.write(
            &format!("INSERT INTO t VALUES ({i})"),
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|| panic!("insert {i} was not accepted\n{}", c.logs()));
    }
}

async fn sum(c: &Cluster, id: u64) -> Option<i64> {
    let mut conn = c.conn(id).await?;
    conn.query_first("SELECT COALESCE(SUM(id), 0) FROM t")
        .await
        .ok()
        .flatten()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_added_after_compaction_catches_up_from_a_snapshot() {
    // A node added later needs entries no node keeps. It used to accept the
    // log's tail and report it matched with an empty database, and once that
    // was refused it could not catch up at all. Now it is sent a snapshot,
    // holds what the others do, and counts toward the quorum.
    let mut c = compacting(Cluster::new("joiner", 3));
    c.start_all();
    c.write(
        "CREATE TABLE t (id INT PRIMARY KEY)",
        Duration::from_secs(30),
    )
    .await
    .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    insert(&c, 1..=20).await;
    for id in 1..=3 {
        c.until_count(id, "t", 20).await;
    }

    let new = c.add_node();
    c.start(new);
    let leader = c.join(new);
    c.until_count(new, "t", 20).await;
    assert!(
        c.log(new).contains("installed a snapshot from the leader"),
        "the new node did not install a snapshot\n{}",
        c.logs()
    );
    // Four members need three: with a follower down, the new node is one.
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    c.kill(follower);
    insert(&c, 21..=25).await;
    c.until_count(new, "t", 25).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_down_past_compaction_catches_up_deletes_included() {
    let mut c = compacting(Cluster::new("behind", 3));
    c.start_all();
    let leader = c
        .write(
            "CREATE TABLE t (id INT PRIMARY KEY)",
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    insert(&c, 1..=20).await;
    for id in 1..=3 {
        c.until_count(id, "t", 20).await;
    }
    let down = (1..=3).find(|&id| id != leader).unwrap();
    c.kill(down);
    // What it misses: rows it holds are deleted, others added, more than the
    // logs keep.
    c.write("DELETE FROM t WHERE id <= 10", Duration::from_secs(10))
        .await
        .expect("delete");
    insert(&c, 21..=40).await;
    c.start(down);
    c.until_count(down, "t", 30).await;
    assert_eq!(sum(&c, down).await, Some((11..=40).sum::<i64>()));
    assert!(
        c.log(down).contains("installed a snapshot from the leader"),
        "the follower did not install a snapshot\n{}",
        c.logs()
    );
    // And it goes on from there with entries.
    insert(&c, 41..=45).await;
    c.until_count(down, "t", 35).await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_that_cannot_write_its_log_acknowledges_nothing() {
    // The Raft log used to be written best effort: a failed write was dropped
    // without a word, and the leader counted itself toward the quorum of
    // entries it did not have. With one follower down, a write was then
    // acknowledged with a single node holding it on disk.
    let mut c = Cluster::new("nodisk", 3);
    c.start_all();
    let leader = c
        .write(
            "CREATE TABLE t (id INT PRIMARY KEY)",
            Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    for i in 1..=5 {
        c.write(
            &format!("INSERT INTO t VALUES ({i})"),
            Duration::from_secs(10),
        )
        .await
        .expect("write");
    }
    for id in 1..=3 {
        c.until_count(id, "t", 5).await;
    }
    // The leader's write must land on it: only the leader and one follower
    // are up, and both are needed.
    let down = (1..=3).find(|&id| id != leader).unwrap();
    c.kill(down);
    let leader = c
        .write("INSERT INTO t VALUES (6)", Duration::from_secs(20))
        .await
        .unwrap_or_else(|| panic!("the remaining two nodes took no write\n{}", c.logs()));
    if !c.deny_disk(leader) {
        eprintln!("skipped: file permissions do not stop this user from writing");
        return;
    }
    let accepted = c
        .write("INSERT INTO t VALUES (7)", Duration::from_secs(8))
        .await;
    assert_eq!(
        accepted,
        None,
        "a write was acknowledged while only one of three nodes could store it\n{}",
        c.logs()
    );
    assert!(
        c.log(leader).contains("cannot write the Raft log"),
        "the leader did not report its disk\n{}",
        c.logs()
    );
    // With its disk back and the third node up, the cluster takes writes again
    // and every node ends up with the same rows. The refused write may have been
    // committed since (its outcome was unknown, not failed): 7 rows or 8.
    c.allow_disk(leader);
    c.start(down);
    c.write("INSERT INTO t VALUES (8)", Duration::from_secs(30))
        .await
        .unwrap_or_else(|| panic!("the cluster did not recover\n{}", c.logs()));
    let deadline = Instant::now() + Duration::from_secs(20);
    let rows = loop {
        let mut counts = Vec::new();
        for id in 1..=3 {
            counts.push(c.count(id, "t").await);
        }
        if counts.iter().all(|n| *n == counts[0]) && counts[0].is_some() {
            tokio::time::sleep(Duration::from_millis(600)).await;
            if c.count(leader, "t").await == counts[0] {
                break counts[0].unwrap();
            }
        }
        assert!(
            Instant::now() < deadline,
            "the nodes never agreed: {counts:?}\n{}",
            c.logs()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(rows == 7 || rows == 8, "{rows} rows\n{}", c.logs());
}
