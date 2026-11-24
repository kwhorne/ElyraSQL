//! Raft clusters of real `elyrasql cluster` processes: nodes are started,
//! killed and restarted like a deployment would see them.
//!
//! `Cluster` is the harness. It starts nodes on free ports, each with its data
//! in a directory of its own, finds the leader by trying a write on every
//! running node, and keeps each node's log for the failure message. A failing
//! test leaves the logs, whole, in `target/tmp/raft-logs/<cluster>`, and the
//! message says what each node last answered a write with.

#![allow(dead_code)]

use mysql_async::prelude::*;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const BIN: &str = env!("CARGO_BIN_EXE_elyrasql");
pub const SECRET: &str = "raft-cluster-test-secret";

#[path = "../ports/mod.rs"]
mod ports;
pub use ports::free_port;

pub struct Node {
    pub id: u64,
    pub mysql: u16,
    control: u16,
    replication: u16,
    child: Option<Child>,
    /// Started at least once.
    started: bool,
    /// Some node has been started with this one's control port as a peer:
    /// its ports can no longer change.
    known: bool,
}

pub struct Cluster {
    name: String,
    dir: PathBuf,
    started: Instant,
    /// What each node last answered a write with, when it was not a success:
    /// the error, and when, in seconds since the cluster was made.
    refusals: Mutex<BTreeMap<u64, String>>,
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
            name: name.into(),
            dir,
            started: Instant::now(),
            refusals: Mutex::new(BTreeMap::new()),
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
            started: false,
            known: false,
        });
        id
    }

    /// Give node `id` new ports, and start it from nothing.
    fn move_ports(&mut self, id: u64) {
        let n = &mut self.nodes[(id - 1) as usize];
        assert!(!n.known && n.child.is_none());
        (n.mysql, n.control, n.replication) = (free_port(), free_port(), free_port());
        let note = format!(
            "--- harness: a port was taken; node {id} starts again on mysql {}, control {}, \
             replication {} ---\n",
            n.mysql, n.control, n.replication
        );
        let _ = std::fs::remove_dir_all(self.data_dir(id));
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(format!("n{id}.log")))
            .unwrap();
        let _ = std::io::Write::write_all(&mut log, note.as_bytes());
    }

    /// Wait until node `id`, started for the first time, listens on all its
    /// ports. `false` if it exited because one was taken.
    ///
    /// A port is checked free when it is chosen, but something else can take
    /// it before the node binds it.
    fn listens(&mut self, id: u64) -> bool {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            // Only what this start wrote, not an earlier attempt's.
            let whole = self.log(id);
            let log = whole.rsplit("--- harness:").next().unwrap_or_default();
            if [
                "raft control plane listening",
                "accepting MySQL-protocol connections",
                "replication endpoint listening",
            ]
            .iter()
            .all(|l| log.contains(l))
            {
                return true;
            }
            let child = self.nodes[(id - 1) as usize].child.as_mut();
            if let Some(Ok(Some(_))) = child.map(|c| c.try_wait()) {
                self.nodes[(id - 1) as usize].child = None;
                return !log.contains("cannot listen on");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        true
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

    /// Start node `id` with every other configured node as its peer. A node
    /// started for the first time that no other node knows yet moves to other
    /// ports if one of its own was taken.
    pub fn start(&mut self, id: u64) {
        let first = !self.node(id).started && !self.node(id).known;
        self.spawn(id);
        if first {
            for _ in 0..3 {
                if self.listens(id) {
                    return;
                }
                self.move_ports(id);
                self.spawn(id);
            }
        }
    }

    fn spawn(&mut self, id: u64) {
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
        let mut child = Some(child);
        for n in self.nodes.iter_mut() {
            if n.id == id {
                n.child = child.take();
                n.started = true;
            } else {
                n.known = true;
            }
        }
    }

    /// Start every node of a new cluster. If a port was taken, the whole
    /// cluster starts again on new ones: each node was given the others'.
    pub fn start_all(&mut self) {
        let ids: Vec<u64> = self.nodes.iter().map(|n| n.id).collect();
        for attempt in 0..3 {
            for &id in &ids {
                self.spawn(id);
            }
            let mut all = true;
            for &id in &ids {
                all &= self.listens(id);
            }
            if all || attempt == 2 {
                return;
            }
            for &id in &ids {
                self.kill(id);
            }
            for n in self.nodes.iter_mut() {
                n.known = false;
            }
            for &id in &ids {
                self.move_ports(id);
            }
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
    ///
    /// An attempt whose outcome is unknown -- no answer, a dropped connection,
    /// or a leader deposed before it could confirm the commit -- may still be
    /// committed by the next leader. Retried, a `CREATE TABLE` then finds the
    /// table and an `INSERT` its row: after such an attempt, those answers
    /// mean the statement is done.
    pub async fn write(&self, sql: &str, within: Duration) -> Option<u64> {
        let deadline = Instant::now() + within;
        let mut uncertain = false;
        while Instant::now() < deadline {
            for id in self.running() {
                let Some(mut c) = self.conn(id).await else {
                    self.refused(id, sql, "no connection within 2s");
                    continue;
                };
                let attempt = tokio::time::timeout(Duration::from_secs(3), c.query_drop(sql)).await;
                match attempt {
                    Ok(Ok(())) => return Some(id),
                    Ok(Err(mysql_async::Error::Server(e))) => {
                        if uncertain && (e.code == 1050 || e.code == 1062) {
                            return Some(id);
                        }
                        uncertain |= e.message.contains("commit not confirmed");
                        self.refused(id, sql, &format!("{} ({})", e.message, e.code));
                    }
                    Ok(Err(e)) => {
                        uncertain = true;
                        self.refused(id, sql, &e.to_string());
                    }
                    Err(_) => {
                        uncertain = true;
                        self.refused(id, sql, "no answer within 3s");
                    }
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

    /// Note what node `id` answered `sql` with. A node that is not the leader
    /// refuses every write, so only the last answer is kept.
    fn refused(&self, id: u64, sql: &str, why: &str) {
        let at = self.started.elapsed().as_secs_f64();
        let sql: String = sql.chars().take(60).collect();
        self.refusals
            .lock()
            .unwrap()
            .insert(id, format!("at {at:.1}s, {sql:?}: {why}"));
    }

    /// The end of every node's log, and what each last refused a write with.
    pub fn logs(&self) -> String {
        let mut out = format!(
            "--- cluster {:?}, {:.1}s old; whole logs are kept in {} ---\n",
            self.name,
            self.started.elapsed().as_secs_f64(),
            self.kept_logs().display()
        );
        for (id, why) in self.refusals.lock().unwrap().iter() {
            out.push_str(&format!("node {id} last refused a write {why}\n"));
        }
        for n in &self.nodes {
            let log = std::fs::read_to_string(self.dir.join(format!("n{}.log", n.id)))
                .unwrap_or_default();
            let tail: Vec<&str> = log.lines().rev().take(40).collect();
            out.push_str(&format!(
                "--- node {} (mysql {}, control {}, replication {}; last lines) ---\n",
                n.id, n.mysql, n.control, n.replication
            ));
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

    /// Where a failed test leaves its nodes' logs (CI uploads the directory).
    fn kept_logs(&self) -> PathBuf {
        PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join("raft-logs")
            .join(format!("{}-{}", self.name, std::process::id()))
    }

    fn keep_logs(&self) {
        let to = self.kept_logs();
        if std::fs::create_dir_all(&to).is_err() {
            return;
        }
        for n in &self.nodes {
            let f = format!("n{}.log", n.id);
            let _ = std::fs::copy(self.dir.join(&f), to.join(&f));
        }
        let refusals: String = self
            .refusals
            .lock()
            .unwrap()
            .iter()
            .map(|(id, why)| format!("node {id}: {why}\n"))
            .collect();
        let _ = std::fs::write(to.join("refusals.txt"), refusals);
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for id in self.running() {
            self.kill(id);
        }
        if std::thread::panicking() {
            self.keep_logs();
        }
        #[cfg(unix)]
        for n in &self.nodes {
            self.allow_disk(n.id);
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
