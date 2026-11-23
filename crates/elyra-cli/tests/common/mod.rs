//! Raft clusters of real `elyrasql cluster` processes: nodes are started,
//! killed and restarted like a deployment would see them.
//!
//! `Cluster` is the harness. It starts nodes on free ports, each with its data
//! in a directory of its own, finds the leader by trying a write on every
//! running node, and keeps each node's log for the failure message.

#![allow(dead_code)]

use mysql_async::prelude::*;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

pub const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");
pub const SECRET: &str = "raft-cluster-test-secret";

pub fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

pub struct Node {
    pub id: u64,
    pub mysql: u16,
    control: u16,
    replication: u16,
    child: Option<Child>,
}

pub struct Cluster {
    dir: PathBuf,
    nodes: Vec<Node>,
    /// Environment every node is started with.
    env: Vec<(String, String)>,
}

impl Cluster {
    /// `n` nodes, configured as peers of each other, not started yet.
    pub fn new(name: &str, n: u64) -> Self {
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
    pub fn with_env(mut self, name: &str, value: &str) -> Self {
        self.env.push((name.into(), value.into()));
        self
    }

    /// Configure one more node (ids count from 1); returns its id.
    pub fn add_node(&mut self) -> u64 {
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

    pub fn node(&self, id: u64) -> &Node {
        &self.nodes[(id - 1) as usize]
    }

    /// The directory node `id` keeps its data, Raft log and election state in.
    pub fn data_dir(&self, id: u64) -> PathBuf {
        self.dir.join(format!("n{id}"))
    }

    pub fn control_addr(&self, id: u64) -> String {
        format!("127.0.0.1:{}", self.node(id).control)
    }

    /// Start node `id` with every other configured node as its peer.
    pub fn start(&mut self, id: u64) {
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

    pub fn start_all(&mut self) {
        for id in 1..=self.nodes.len() as u64 {
            self.start(id);
        }
    }

    /// Kill node `id` without warning (`SIGKILL`).
    pub fn kill(&mut self, id: u64) {
        if let Some(mut c) = self.nodes[(id - 1) as usize].child.take() {
            c.kill().ok();
            c.wait().ok();
        }
    }

    /// Stop node `id` from writing anything but its database file: its Raft log
    /// and election state can be neither written nor replaced. `false` if the
    /// permissions do not stop this user (root), and nothing changed.
    #[cfg(unix)]
    pub fn deny_disk(&self, id: u64) -> bool {
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
    pub fn allow_disk(&self, id: u64) {
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

    pub fn running(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|n| n.child.is_some())
            .map(|n| n.id)
            .collect()
    }

    /// Ask the running cluster to add node `id` to its membership. Only the
    /// leader accepts it; returns the node that did.
    pub fn join(&self, id: u64) -> u64 {
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

    pub async fn conn(&self, id: u64) -> Option<mysql_async::Conn> {
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
    pub async fn write(&self, sql: &str, within: Duration) -> Option<u64> {
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

    pub async fn count(&self, id: u64, table: &str) -> Option<i64> {
        let mut c = self.conn(id).await?;
        c.query_first(format!("SELECT COUNT(*) FROM {table}"))
            .await
            .ok()
            .flatten()
    }

    /// Wait until node `id` holds `rows` rows in `table`.
    pub async fn until_count(&self, id: u64, table: &str, rows: i64) {
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

    pub fn logs(&self) -> String {
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

    pub fn log(&self, id: u64) -> String {
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
