//! Chaos for a Raft cluster of real `elyrasql cluster` processes: clients write
//! all along while nodes are killed with `SIGKILL` and restarted, a follower
//! is down long enough to need a snapshot, the leader is killed, and a node is
//! added after the logs were compacted. At the end every node must hold the
//! same data, every acknowledged write must be there, nothing no client wrote
//! may be, and transfers between accounts must have kept the total.
//!
//! It runs for `ELYRASQL_RAFT_CHAOS_SECS` (default 30) and is ignored by a
//! plain `cargo test`; CI runs it in the durability job:
//!
//! ```text
//! cargo test -p elyra-cli --release --test raft_chaos -- --ignored --nocapture
//! ```
//!
//! `ELYRASQL_RAFT_CHAOS_SEED` replays the random part of a run.

mod common;

use common::Cluster;
use mysql_async::prelude::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const WRITERS: i64 = 4;
const ACCOUNTS: i64 = 10;
const OPENING_BALANCE: i64 = 100;

/// xorshift64*: enough randomness to vary a run, and replayable.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// What the clients know about their writes.
#[derive(Default)]
struct Outcomes {
    /// Rows whose INSERT was acknowledged: they must be there.
    acked: BTreeSet<i64>,
    /// Rows whose INSERT failed in a way that leaves it unknown whether it
    /// was committed (the connection or the leader went away): they may be.
    maybe: BTreeSet<i64>,
    transfers: u64,
}

/// An error that says the statement was not committed: a follower refused
/// it, or the node is not the leader or cannot be queried right now.
fn refused(e: &mysql_async::Error) -> bool {
    let m = e.to_string();
    m.contains("access denied")
        || m.contains("not the leader")
        || m.contains("installing a snapshot")
}

async fn connect(port: u16) -> Option<mysql_async::Conn> {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("127.0.0.1")
        .tcp_port(port)
        .user(Some("root"))
        .prefer_socket(false);
    tokio::time::timeout(Duration::from_secs(1), mysql_async::Conn::new(opts))
        .await
        .ok()?
        .ok()
}

/// One client: INSERTs rows of its own and moves money between accounts,
/// against whichever node takes writes, until told to stop.
async fn writer(
    w: i64,
    seed: u64,
    ports: Arc<Mutex<Vec<u16>>>,
    stop: Arc<AtomicBool>,
    out: Arc<Mutex<Outcomes>>,
) {
    let mut rng = Rng(seed ^ (w as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let mut leader: Option<(u16, mysql_async::Conn)> = None;
    let mut n = 0i64;
    while !stop.load(Ordering::Relaxed) {
        let transfer = rng.below(3) == 0;
        let id = w * 10_000_000 + n;
        let sql = if transfer {
            let from = rng.below(ACCOUNTS as u64) as i64;
            let to = (from + 1 + rng.below(ACCOUNTS as u64 - 1) as i64) % ACCOUNTS;
            format!(
                "START TRANSACTION; \
                 UPDATE acct SET bal = bal - 1 WHERE id = {from}; \
                 UPDATE acct SET bal = bal + 1 WHERE id = {to}; \
                 COMMIT"
            )
        } else {
            n += 1;
            format!("INSERT INTO t VALUES ({id}, {w})")
        };
        // The node that took the last write first, then every other one.
        let mut candidates: Vec<u16> = ports.lock().unwrap().clone();
        if let Some((p, _)) = &leader {
            let p = *p;
            candidates.retain(|&q| q != p);
            candidates.insert(0, p);
        }
        let mut outcome = None;
        for port in candidates {
            let mut conn = match leader.take() {
                Some((p, c)) if p == port => c,
                _ => match connect(port).await {
                    Some(c) => c,
                    None => continue,
                },
            };
            let r = tokio::time::timeout(Duration::from_secs(5), conn.query_drop(&sql)).await;
            match r {
                Ok(Ok(())) => {
                    leader = Some((port, conn));
                    outcome = Some(true);
                    break;
                }
                Ok(Err(e)) if refused(&e) => continue,
                // Anything else on a node that took the write: unknown.
                _ => {
                    outcome = Some(false);
                    break;
                }
            }
        }
        {
            let mut o = out.lock().unwrap();
            match (outcome, transfer) {
                (Some(true), true) => o.transfers += 1,
                (Some(true), false) => {
                    o.acked.insert(id);
                }
                (Some(false), false) => {
                    o.maybe.insert(id);
                }
                // A transfer either happened or did not: the total holds
                // either way. An INSERT no node took was refused everywhere,
                // so it was not committed.
                (Some(false), true) | (None, _) => {}
            }
        }
        if outcome.is_none() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// `(rows, sum of ids, total balance)` on node `id`.
async fn shape(c: &Cluster, id: u64) -> Option<(i64, i64, i64)> {
    let mut conn = c.conn(id).await?;
    let (rows, ids): (i64, i64) = conn
        .query_first("SELECT COUNT(*), COALESCE(SUM(id), 0) FROM t")
        .await
        .ok()
        .flatten()?;
    let bal: i64 = conn
        .query_first("SELECT COALESCE(SUM(bal), 0) FROM acct")
        .await
        .ok()
        .flatten()?;
    Some((rows, ids, bal))
}

/// The id of the leader: the node that takes a write that changes nothing.
async fn leader(c: &Cluster) -> u64 {
    c.write("DELETE FROM t WHERE id < 0", Duration::from_secs(20))
        .await
        .unwrap_or_else(|| panic!("no leader\n{}", c.logs()))
}

/// Wait until node `id` logs that it installed a snapshot.
async fn until_snapshot_installed(c: &Cluster, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !c.log(id).contains("installed a snapshot from the leader") {
        assert!(
            Instant::now() < deadline,
            "node {id} never installed a snapshot\n{}",
            c.logs()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "chaos run of a minute or more; CI runs it in the durability job"]
async fn a_cluster_under_chaos_loses_and_invents_nothing() {
    let secs: u64 = std::env::var("ELYRASQL_RAFT_CHAOS_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let seed: u64 = std::env::var("ELYRASQL_RAFT_CHAOS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1)
                | 1
        });
    eprintln!("raft chaos: {secs} s, ELYRASQL_RAFT_CHAOS_SEED={seed}");
    let mut rng = Rng(seed);

    // Small logs, so a node that misses a few seconds needs a snapshot, sent in
    // many parts.
    let mut c = Cluster::new("chaos", 3)
        .with_env("ELYRASQL_RAFT_LOG_KEEP_ENTRIES", "20")
        .with_env("ELYRASQL_RAFT_SNAPSHOT_CHUNK_BYTES", "2048");
    c.start_all();
    c.write(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, w INT)",
        Duration::from_secs(30),
    )
    .await
    .unwrap_or_else(|| panic!("no leader accepted a write\n{}", c.logs()));
    c.write(
        "CREATE TABLE acct (id INT PRIMARY KEY, bal INT)",
        Duration::from_secs(10),
    )
    .await
    .unwrap_or_else(|| panic!("create acct was not accepted\n{}", c.logs()));
    let accounts: Vec<String> = (0..ACCOUNTS)
        .map(|i| format!("({i}, {OPENING_BALANCE})"))
        .collect();
    c.write(
        &format!("INSERT INTO acct VALUES {}", accounts.join(", ")),
        Duration::from_secs(10),
    )
    .await
    .unwrap_or_else(|| panic!("open accounts was not accepted\n{}", c.logs()));

    let ports = Arc::new(Mutex::new(
        (1..=3).map(|id| c.node(id).mysql).collect::<Vec<_>>(),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| tokio::spawn(writer(w, seed, ports.clone(), stop.clone(), out.clone())))
        .collect();
    let started = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // A follower down long enough for the others to compact past it.
    let l = leader(&c).await;
    let behind = (1..=3).find(|&id| id != l).unwrap();
    eprintln!("chaos: node {behind} down while the others write on");
    c.kill(behind);
    tokio::time::sleep(Duration::from_secs(4)).await;
    c.start(behind);
    until_snapshot_installed(&c, behind).await;

    // The leader dies.
    let l = leader(&c).await;
    eprintln!("chaos: leader {l} killed");
    c.kill(l);
    tokio::time::sleep(Duration::from_secs(3)).await;
    c.start(l);

    // A node joins after compaction.
    let new = c.add_node();
    c.start(new);
    c.join(new);
    ports.lock().unwrap().push(c.node(new).mysql);
    eprintln!("chaos: node {new} added");
    until_snapshot_installed(&c, new).await;

    // Then whatever comes: one node down at a time, any of them.
    let deadline = started + Duration::from_secs(secs.max(10));
    while Instant::now() < deadline {
        let all: Vec<u64> = (1..=new).collect();
        let down: Vec<u64> = all
            .iter()
            .copied()
            .filter(|id| !c.running().contains(id))
            .collect();
        if let Some(&id) = down.first() {
            eprintln!("chaos: node {id} restarted");
            c.start(id);
        } else {
            let id = all[rng.below(all.len() as u64) as usize];
            eprintln!("chaos: node {id} killed");
            c.kill(id);
        }
        tokio::time::sleep(Duration::from_millis(1500 + rng.below(1500))).await;
    }

    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.unwrap();
    }
    for id in 1..=new {
        if !c.running().contains(&id) {
            c.start(id);
        }
    }

    // Every node ends up with the same data.
    let deadline = Instant::now() + Duration::from_secs(90);
    let settled = loop {
        let mut shapes = Vec::new();
        for id in 1..=new {
            shapes.push(shape(&c, id).await);
        }
        if shapes[0].is_some() && shapes.iter().all(|s| *s == shapes[0]) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if shape(&c, 1).await == shapes[0] {
                break shapes[0].unwrap();
            }
        }
        assert!(
            Instant::now() < deadline,
            "the nodes never agreed: {shapes:?}\n{}",
            c.logs()
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    let o = std::mem::take(&mut *out.lock().unwrap());
    eprintln!(
        "raft chaos: {} rows acknowledged, {} unknown, {} transfers; nodes hold {:?}",
        o.acked.len(),
        o.maybe.len(),
        o.transfers,
        settled
    );
    assert!(
        o.acked.len() > 100,
        "the clients hardly wrote: {}",
        o.acked.len()
    );
    assert_eq!(
        settled.2,
        ACCOUNTS * OPENING_BALANCE,
        "transfers did not keep the total\n{}",
        c.logs()
    );
    let mut conn = c.conn(1).await.expect("node 1");
    let held: BTreeSet<i64> = conn
        .query::<i64, _>("SELECT id FROM t")
        .await
        .expect("read rows")
        .into_iter()
        .collect();
    let lost: Vec<_> = o.acked.difference(&held).collect();
    assert!(
        lost.is_empty(),
        "acknowledged rows lost: {lost:?}\n{}",
        c.logs()
    );
    let invented: Vec<_> = held
        .iter()
        .filter(|id| !o.acked.contains(id) && !o.maybe.contains(id))
        .collect();
    assert!(
        invented.is_empty(),
        "rows no write acknowledged or left unknown: {invented:?}"
    );
}
