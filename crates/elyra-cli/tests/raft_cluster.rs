//! Raft clusters of real `elyrasql cluster` processes: nodes are started,
//! killed and restarted like a deployment would see them.
//!
//! `Cluster` is the harness. It starts nodes on free ports with their data in
//! a temporary directory, finds the leader by trying a write on every running
//! node, and keeps each node's log for the failure message.

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
        };
        for _ in 0..n {
            c.add_node();
        }
        c
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
        let mut cmd = Command::new(BIN);
        cmd.args(["cluster", "--id", &id.to_string()])
            .arg("--data")
            .arg(self.dir.join(format!("n{id}.edb")))
            .args(["--listen", &format!("127.0.0.1:{}", n.mysql)])
            .args(["--control-listen", &format!("127.0.0.1:{}", n.control)])
            .args([
                "--replication-listen",
                &format!("127.0.0.1:{}", n.replication),
            ]);
        for p in &peers {
            cmd.args(["--peer", p]);
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

#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_needs_compacted_entries_never_counts_toward_a_quorum() {
    // The log is compacted as soon as every member has an entry, so a node
    // added later needs entries no node still has. It used to accept the log's
    // tail, apply none of it, and report it as matched: the leader counted a
    // node with an empty database toward its quorum. Until snapshots can be
    // transferred such a node cannot catch up -- but it must not make up a
    // majority.
    let mut c = Cluster::new("stranded", 3);
    c.start_all();
    c.write(
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

    let new = c.add_node();
    c.start(new);
    let leader = c.join(new);
    // Membership travels with the leader's heartbeats: wait until it has tried
    // the new node, and its followers have heard of it.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !c
        .log(leader)
        .contains("needs log entries that were compacted away")
    {
        assert!(
            Instant::now() < deadline,
            "the leader never tried the new node\n{}",
            c.logs()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Four members need three for a majority. Without one follower that is the
    // leader, the other follower and the new node -- which has nothing.
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    c.kill(follower);
    let accepted = c
        .write("INSERT INTO t VALUES (100)", Duration::from_secs(8))
        .await;
    assert_eq!(
        accepted,
        None,
        "a write was acknowledged by a majority that counted the empty new node \
         (new node has {:?} rows)\n{}",
        c.count(new, "t").await,
        c.logs()
    );
    // With the follower back, a real majority exists again.
    c.start(follower);
    c.write("INSERT INTO t VALUES (101)", Duration::from_secs(30))
        .await
        .unwrap_or_else(|| panic!("the cluster did not recover its majority\n{}", c.logs()));
    // The refused write stayed in the leader's log, and may be committed now
    // that a majority is back: an error after a step-down means "unknown", as in
    // Raft generally. Either way, the three nodes agree.
    let deadline = Instant::now() + Duration::from_secs(20);
    let rows = loop {
        let mut counts = Vec::new();
        for id in 1..=3 {
            counts.push(c.count(id, "t").await);
        }
        let settled = counts.iter().all(|n| *n == counts[0]) && counts[0].is_some();
        if settled {
            // Stable across a few heartbeats, not just equal for an instant.
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
    assert!(rows == 21 || rows == 22, "{rows} rows\n{}", c.logs());
}
