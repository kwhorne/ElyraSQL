//! Raft consensus for the cluster: leader election **and** replicated-log write
//! path (this is the full consensus, not just election).
//!
//! Each node runs a Raft state machine: terms, votes with the §5.4.1 election
//! restriction (a vote is granted only to a candidate whose log is at least as
//! up-to-date), and `AppendEntries` replication. Writes on the leader are
//! proposed through the replicated log ([`elyra_storage::Consensus`]): the entry
//! is appended, replicated, **committed once a quorum has it**, and only then
//! **applied** to the state machine and acknowledged to the client. Followers
//! append entries (with the consistency check + conflicting-suffix truncation
//! from [`crate::raftlog`]) and apply up to the leader's commit index.
//!
//! This gives no-data-loss failover: an acknowledged write is on a quorum's
//! durable log before the client is told success, and the election restriction
//! guarantees any new leader already has it.

use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use elyra_storage::{Consensus, Db, WriteOp};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, watch, Notify};
use tracing::{info, warn};

use crate::raftlog::{LogEntry, RaftLog};

/// A peer node in the cluster.
#[derive(Clone)]
pub struct Peer {
    pub id: u64,
    pub control_addr: String,
}

/// Cluster configuration for this node.
pub struct ClusterConfig {
    pub id: u64,
    pub control_listen: String,
    /// Advertised address (informational; the Raft data path uses the control
    /// plane, so this is just surfaced to clients/tools).
    pub replication_addr: String,
    pub peers: Vec<Peer>,
    /// File for the persistent election state (term + vote).
    pub state_path: Option<PathBuf>,
    /// File for the persistent Raft log.
    pub log_path: Option<PathBuf>,
}

fn load_state(path: &Option<PathBuf>) -> (u64, Option<u64>) {
    let Some(p) = path else { return (0, None) };
    let Ok(s) = std::fs::read_to_string(p) else {
        return (0, None);
    };
    let mut lines = s.lines();
    let term = lines
        .next()
        .and_then(|l| l.trim().parse().ok())
        .unwrap_or(0);
    let voted_for = lines.next().and_then(|l| {
        l.trim()
            .parse::<u64>()
            .ok()
            .filter(|_| !l.trim().is_empty())
    });
    (term, voted_for)
}

fn persist_state(path: &Option<PathBuf>, term: u64, voted_for: Option<u64>) {
    if let Some(p) = path {
        let body = format!(
            "{term}\n{}\n",
            voted_for.map(|v| v.to_string()).unwrap_or_default()
        );
        if let Err(e) = std::fs::write(p, body) {
            warn!(error = %e, "failed to persist election state");
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Msg {
    RequestVote {
        term: u64,
        candidate: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    Vote {
        term: u64,
        granted: bool,
    },
    /// Leader → follower: replicate entries (empty = heartbeat) and advance the
    /// follower's commit index. Also carries membership + leader identity.
    AppendEntries {
        term: u64,
        leader: u64,
        repl_addr: String,
        members: Vec<(u64, String)>,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        /// Highest index safe to compact on all nodes (min follower match).
        compact_index: u64,
    },
    AppendAck {
        term: u64,
        success: bool,
        match_index: u64,
    },
    AddPeer {
        id: u64,
        control_addr: String,
    },
    RemovePeer {
        id: u64,
    },
    CtlAck {
        ok: bool,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

struct State {
    term: u64,
    voted_for: Option<u64>,
    role: Role,
    leader: Option<u64>,
    election_deadline: Instant,
}

const HEARTBEAT_MS: u64 = 150;
const ELECTION_LO_MS: u64 = 1000;
const ELECTION_HI_MS: u64 = 2000;
/// A leader that has not confirmed contact with a quorum for this long gives up
/// leadership (step-down). Kept below the minimum election timeout so the old
/// leader stops acting before a new one can be elected — the basis for safe
/// lease-based leader reads.
const LEADER_LEASE_MS: u64 = 900;

fn election_timeout(id: u64) -> Duration {
    use std::sync::atomic::AtomicU64;
    static C: AtomicU64 = AtomicU64::new(0);
    let c = C.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut x =
        id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ c.wrapping_mul(0xD1B5_4A32_D192_ED03) ^ nanos;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    Duration::from_millis(ELECTION_LO_MS + x % (ELECTION_HI_MS - ELECTION_LO_MS))
}

/// The elected leader, published to the node runner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Leader {
    pub id: u64,
    pub is_self: bool,
    pub repl_addr: String,
}

/// Per-follower replication progress (Raft `nextIndex` / `matchIndex`).
#[derive(Clone, Copy)]
struct Progress {
    next: u64,
    match_: u64,
}

/// A running Raft node: election + replicated-log write path.
pub struct Node {
    cfg: Arc<ClusterConfig>,
    peers: Mutex<Vec<Peer>>,
    state: Arc<Mutex<State>>,
    log: Arc<Mutex<RaftLog>>,
    db: Db,
    /// Client proposals awaiting commit+apply, keyed by log index.
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<elyra_core::Result<()>>>>>,
    /// Per-follower progress (leader only).
    progress: Arc<Mutex<HashMap<u64, Progress>>>,
    /// Wakes the leader replication loop when there is new work.
    replicate: Arc<Notify>,
    /// Last time this leader confirmed contact with a quorum (its lease anchor).
    lease: Mutex<Instant>,
    leader_tx: watch::Sender<Option<Leader>>,
    pub leader_rx: watch::Receiver<Option<Leader>>,
}

impl Node {
    pub fn new(cfg: ClusterConfig, db: Db) -> Arc<Self> {
        let cfg_id = cfg.id;
        let peers = Mutex::new(cfg.peers.clone());
        let (leader_tx, leader_rx) = watch::channel(None);
        let (term, voted_for) = load_state(&cfg.state_path);
        let log = match &cfg.log_path {
            Some(p) => RaftLog::open(p.clone()).unwrap_or_default(),
            None => RaftLog::new(),
        };
        Arc::new(Node {
            cfg: Arc::new(cfg),
            peers,
            state: Arc::new(Mutex::new(State {
                term,
                voted_for,
                role: Role::Follower,
                leader: None,
                election_deadline: Instant::now() + election_timeout(cfg_id),
            })),
            log: Arc::new(Mutex::new(log)),
            db,
            pending: Arc::new(Mutex::new(HashMap::new())),
            progress: Arc::new(Mutex::new(HashMap::new())),
            replicate: Arc::new(Notify::new()),
            lease: Mutex::new(Instant::now()),
            leader_tx,
            leader_rx,
        })
    }

    fn peer_snapshot(&self) -> Vec<Peer> {
        self.peers.lock().unwrap().clone()
    }

    fn majority(&self) -> usize {
        let n = self.peers.lock().unwrap().len() + 1;
        n / 2 + 1
    }

    fn adopt_members(&self, members: &[(u64, String)]) {
        let mut p = self.peers.lock().unwrap();
        *p = members
            .iter()
            .filter(|(id, _)| *id != self.cfg.id)
            .map(|(id, addr)| Peer {
                id: *id,
                control_addr: addr.clone(),
            })
            .collect();
    }

    fn members(&self) -> Vec<(u64, String)> {
        let mut m = vec![(self.cfg.id, self.cfg.control_listen.clone())];
        for p in self.peers.lock().unwrap().iter() {
            m.push((p.id, p.control_addr.clone()));
        }
        m
    }

    fn publish(&self, leader: Option<Leader>) {
        let _ = self.leader_tx.send(leader);
    }

    /// Fail every pending client proposal (called on step-down / leadership loss).
    fn drain_pending(&self, err: &str) {
        let mut p = self.pending.lock().unwrap();
        for (_, tx) in p.drain() {
            let _ = tx.send(Err(elyra_core::Error::Storage(err.into())));
        }
    }

    /// Start the control listener and the election / replication loops.
    pub async fn run(self: Arc<Self>) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.cfg.control_listen).await?;
        info!(id = self.cfg.id, addr = %self.cfg.control_listen, "raft control plane listening");
        let srv = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let _ = stream.set_nodelay(true);
                        let n = srv.clone();
                        tokio::spawn(async move {
                            let _ = n.handle_control(stream).await;
                        });
                    }
                    Err(e) => warn!(error = %e, "control accept failed"),
                }
            }
        });
        self.main_loop().await;
        Ok(())
    }

    async fn handle_control(&self, mut stream: TcpStream) -> std::io::Result<()> {
        // Loop over messages so a leader can reuse one connection for many
        // AppendEntries (batched throughput); one-shot RPCs simply send one
        // message and close, ending the loop at EOF.
        while let Some(msg) = recv(&mut stream).await? {
            let reply = match msg {
                Msg::AddPeer { id, control_addr } => {
                    {
                        let mut p = self.peers.lock().unwrap();
                        if id != self.cfg.id && !p.iter().any(|x| x.id == id) {
                            p.push(Peer { id, control_addr });
                        }
                    }
                    info!(id = self.cfg.id, added = id, "peer added");
                    Msg::CtlAck { ok: true }
                }
                Msg::RemovePeer { id } => {
                    self.peers.lock().unwrap().retain(|x| x.id != id);
                    Msg::CtlAck { ok: true }
                }
                Msg::RequestVote {
                    term,
                    candidate,
                    last_log_index,
                    last_log_term,
                } => self.on_request_vote(term, candidate, last_log_index, last_log_term),
                Msg::AppendEntries {
                    term,
                    leader,
                    repl_addr,
                    members,
                    prev_index,
                    prev_term,
                    entries,
                    leader_commit,
                    compact_index,
                } => {
                    self.on_append_entries(
                        term,
                        leader,
                        repl_addr,
                        members,
                        prev_index,
                        prev_term,
                        entries,
                        leader_commit,
                        compact_index,
                    )
                    .await
                }
                _ => Msg::CtlAck { ok: false },
            };
            send(&mut stream, &reply).await?;
        }
        Ok(())
    }

    fn on_request_vote(
        &self,
        term: u64,
        candidate: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Msg {
        let mut s = self.state.lock().unwrap();
        let mut changed = false;
        if term > s.term {
            s.term = term;
            s.voted_for = None;
            if s.role != Role::Follower {
                s.role = Role::Follower;
            }
            changed = true;
        }
        // Election restriction: only vote for an at-least-as-up-to-date log.
        let up_to_date = self
            .log
            .lock()
            .unwrap()
            .is_up_to_date(last_log_term, last_log_index);
        let granted = term == s.term
            && (s.voted_for.is_none() || s.voted_for == Some(candidate))
            && up_to_date;
        if granted {
            s.voted_for = Some(candidate);
            s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
            changed = true;
        }
        let reply_term = s.term;
        if changed {
            persist_state(&self.cfg.state_path, s.term, s.voted_for);
        }
        Msg::Vote {
            term: reply_term,
            granted,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn on_append_entries(
        &self,
        term: u64,
        leader: u64,
        repl_addr: String,
        members: Vec<(u64, String)>,
        prev_index: u64,
        prev_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
        compact_index: u64,
    ) -> Msg {
        {
            let mut s = self.state.lock().unwrap();
            if term < s.term {
                return Msg::AppendAck {
                    term: s.term,
                    success: false,
                    match_index: 0,
                };
            }
            if term > s.term {
                s.term = term;
                s.voted_for = None;
            }
            let was_leader = s.role == Role::Leader;
            s.role = Role::Follower;
            s.leader = Some(leader);
            s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
            persist_state(&self.cfg.state_path, s.term, s.voted_for);
            if was_leader {
                drop(s);
                self.drain_pending("stepped down: no longer leader");
            }
        }
        self.adopt_members(&members);
        self.publish(Some(Leader {
            id: leader,
            is_self: leader == self.cfg.id,
            repl_addr,
        }));

        // Append + advance commit under the log lock, collecting entries to apply.
        let (success, match_index, to_apply) = {
            let mut log = self.log.lock().unwrap();
            if log.append_entries(prev_index, prev_term, &entries) {
                log.advance_commit(leader_commit);
                (true, log.last_index(), log.take_applicable())
            } else {
                (false, 0, Vec::new())
            }
        };
        // Apply committed entries in log order, but submit them together so the
        // DB group-commit folds them into few fsyncs (batched throughput).
        let ops: Vec<WriteOp> = to_apply
            .iter()
            .filter_map(|e| bincode::deserialize::<WriteOp>(&e.data).ok())
            .collect();
        let _ =
            futures_util::future::join_all(ops.into_iter().map(|op| self.db.apply_op_local(op)))
                .await;
        // Compact applied entries up to the cluster-wide safe point.
        if success {
            self.log.lock().unwrap().compact(compact_index);
        }
        let reply_term = self.state.lock().unwrap().term;
        Msg::AppendAck {
            term: reply_term,
            success,
            match_index,
        }
    }

    async fn main_loop(self: Arc<Self>) {
        // Persistent AppendEntries connections to followers, reused across rounds
        // for batched throughput; cleared when we stop being leader.
        let mut conns: HashMap<u64, TcpStream> = HashMap::new();
        loop {
            let (role, deadline) = {
                let s = self.state.lock().unwrap();
                (s.role, s.election_deadline)
            };
            match role {
                Role::Leader => {
                    self.replicate_round(&mut conns).await;
                    // Leader lease: if we haven't confirmed a quorum for the lease
                    // window, step down so writes fail fast and a healthy
                    // majority can elect a new leader (liveness).
                    if self.lease.lock().unwrap().elapsed() > Duration::from_millis(LEADER_LEASE_MS)
                    {
                        self.step_down("lost quorum contact (lease expired)");
                        conns.clear();
                        continue;
                    }
                    tokio::select! {
                        _ = self.replicate.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(HEARTBEAT_MS)) => {}
                    }
                }
                _ => {
                    conns.clear(); // stale once we're not leader
                    let now = Instant::now();
                    if now >= deadline {
                        self.start_election().await;
                    } else {
                        tokio::time::sleep(deadline - now).await;
                    }
                }
            }
        }
    }

    async fn start_election(self: &Arc<Self>) {
        let (term, last_index, last_term) = {
            let mut s = self.state.lock().unwrap();
            s.term += 1;
            s.role = Role::Candidate;
            s.voted_for = Some(self.cfg.id);
            s.leader = None;
            s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
            persist_state(&self.cfg.state_path, s.term, s.voted_for);
            let log = self.log.lock().unwrap();
            (s.term, log.last_index(), log.last_term())
        };
        info!(id = self.cfg.id, term, last_index, "standing for election");
        let mut votes = 1;
        for peer in self.peer_snapshot() {
            let m = Msg::RequestVote {
                term,
                candidate: self.cfg.id,
                last_log_index: last_index,
                last_log_term: last_term,
            };
            if let Ok(Msg::Vote { term: t, granted }) = rpc(&peer.control_addr, &m).await {
                let mut s = self.state.lock().unwrap();
                if t > s.term {
                    s.term = t;
                    s.role = Role::Follower;
                    s.voted_for = None;
                    persist_state(&self.cfg.state_path, s.term, s.voted_for);
                    return;
                }
                if granted && s.role == Role::Candidate && s.term == term {
                    votes += 1;
                }
            }
        }
        let become_leader = {
            let mut s = self.state.lock().unwrap();
            if s.role == Role::Candidate && s.term == term && votes >= self.majority() {
                s.role = Role::Leader;
                s.leader = Some(self.cfg.id);
                true
            } else {
                false
            }
        };
        if become_leader {
            info!(id = self.cfg.id, term, votes, "elected leader");
            *self.lease.lock().unwrap() = Instant::now(); // fresh lease on election
                                                          // Initialise follower progress and append a no-op so entries from
                                                          // prior terms become committable in this term (Raft §5.4.2).
            let last = self.log.lock().unwrap().last_index();
            {
                let mut prog = self.progress.lock().unwrap();
                prog.clear();
                for peer in self.peer_snapshot() {
                    prog.insert(
                        peer.id,
                        Progress {
                            next: last + 1,
                            match_: 0,
                        },
                    );
                }
            }
            let noop = bincode::serialize(&WriteOp::Plain {
                puts: vec![],
                deletes: vec![],
            })
            .unwrap_or_default();
            self.log.lock().unwrap().leader_append(term, noop);
            self.publish(Some(Leader {
                id: self.cfg.id,
                is_self: true,
                repl_addr: self.cfg.replication_addr.clone(),
            }));
            self.replicate.notify_one();
        }
    }

    /// One leader replication round: push entries to every follower, collect
    /// acks, advance the commit index, and apply newly committed entries.
    async fn replicate_round(self: &Arc<Self>, conns: &mut HashMap<u64, TcpStream>) {
        let term = {
            let s = self.state.lock().unwrap();
            if s.role != Role::Leader {
                return;
            }
            s.term
        };
        // Durably persist all newly-appended entries once (batched fsync) before
        // they are replicated and counted toward commit.
        let leader_commit = {
            let mut log = self.log.lock().unwrap();
            log.sync();
            log.commit_index()
        };
        let members = self.members();
        let peers = self.peer_snapshot();
        let live: std::collections::HashSet<u64> = peers.iter().map(|p| p.id).collect();
        conns.retain(|id, _| live.contains(id));
        // Cluster-wide safe compaction point = the slowest follower's match
        // index (everyone has entries up to here, so they can be discarded);
        // for a single node it's simply what has been applied.
        let compact_index = {
            let prog = self.progress.lock().unwrap();
            if peers.is_empty() {
                self.log.lock().unwrap().last_applied()
            } else {
                peers
                    .iter()
                    .map(|p| prog.get(&p.id).map(|x| x.match_).unwrap_or(0))
                    .min()
                    .unwrap_or(0)
            }
        };
        let mut quorum_acks = 1usize; // this leader counts itself
        for peer in peers {
            let (prev_index, prev_term, entries) = {
                let log = self.log.lock().unwrap();
                let next = self
                    .progress
                    .lock()
                    .unwrap()
                    .get(&peer.id)
                    .map(|p| p.next)
                    .unwrap_or(1);
                let prev_index = next.saturating_sub(1);
                let prev_term = log.term_at(prev_index).unwrap_or(0);
                (prev_index, prev_term, log.entries_after(prev_index))
            };
            let m = Msg::AppendEntries {
                term,
                leader: self.cfg.id,
                repl_addr: self.cfg.replication_addr.clone(),
                members: members.clone(),
                prev_index,
                prev_term,
                entries,
                leader_commit,
                compact_index,
            };
            // Reuse the persistent connection; reconnect once on failure.
            let reply = match conns.get_mut(&peer.id) {
                Some(s) => append_rpc(s, &m).await.ok(),
                None => None,
            };
            let reply = match reply {
                Some(r) => Some(r),
                None => {
                    conns.remove(&peer.id);
                    match TcpStream::connect(&peer.control_addr).await {
                        Ok(mut s) => {
                            let _ = s.set_nodelay(true);
                            match append_rpc(&mut s, &m).await {
                                Ok(r) => {
                                    conns.insert(peer.id, s);
                                    Some(r)
                                }
                                Err(_) => None,
                            }
                        }
                        Err(_) => None,
                    }
                }
            };
            match reply {
                Some(Msg::AppendAck {
                    term: t,
                    success,
                    match_index,
                }) => {
                    if t > term {
                        let mut s = self.state.lock().unwrap();
                        if t > s.term {
                            s.term = t;
                            s.role = Role::Follower;
                            s.voted_for = None;
                            persist_state(&self.cfg.state_path, s.term, s.voted_for);
                            drop(s);
                            self.drain_pending("stepped down during replication");
                        }
                        return;
                    }
                    let mut prog = self.progress.lock().unwrap();
                    let p = prog
                        .entry(peer.id)
                        .or_insert(Progress { next: 1, match_: 0 });
                    if success {
                        p.match_ = match_index;
                        p.next = match_index + 1;
                        quorum_acks += 1;
                    } else if p.next > 1 {
                        p.next -= 1; // back off and retry with an earlier prefix
                    }
                }
                Some(_) | None => {
                    // Unreachable or unexpected reply: drop, retry next round.
                    conns.remove(&peer.id);
                }
            }
        }

        // Renew the leader lease if a quorum acknowledged this round.
        if quorum_acks >= self.majority() {
            *self.lease.lock().unwrap() = Instant::now();
        }
        self.advance_commit_and_apply(term).await;
        // Compact the leader's own log up to the cluster-wide safe point.
        self.log.lock().unwrap().compact(compact_index);
    }

    /// Relinquish leadership: become a follower, fail pending proposals fast, and
    /// signal "no leader" so connections go read-only until a new election.
    fn step_down(&self, reason: &str) {
        {
            let mut s = self.state.lock().unwrap();
            if s.role != Role::Leader {
                return;
            }
            s.role = Role::Follower;
            s.leader = None;
            s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
        }
        self.drain_pending(reason);
        self.publish(None);
        warn!(id = self.cfg.id, reason, "stepped down");
    }

    /// Compute the quorum commit index and apply newly committed entries.
    async fn advance_commit_and_apply(self: &Arc<Self>, term: u64) {
        let to_apply = {
            let mut log = self.log.lock().unwrap();
            let mut match_indexes = vec![log.last_index()]; // the leader itself
            for p in self.progress.lock().unwrap().values() {
                match_indexes.push(p.match_);
            }
            log.maybe_commit(&mut match_indexes, term);
            log.take_applicable()
        };
        if to_apply.is_empty() {
            return;
        }
        // Apply in log order but submit together: the DB group-commit folds the
        // batch into few fsyncs. join_all polls in order, so jobs reach the
        // writer in order (deterministic apply, matching followers).
        let indexes: Vec<u64> = to_apply.iter().map(|e| e.index).collect();
        let futs = to_apply.into_iter().map(|e| {
            let db = self.db.clone();
            async move {
                match bincode::deserialize::<WriteOp>(&e.data) {
                    Ok(op) => db.apply_op_local(op).await,
                    Err(err) => Err(elyra_core::Error::Storage(err.to_string())),
                }
            }
        });
        let results = futures_util::future::join_all(futs).await;
        let mut pending = self.pending.lock().unwrap();
        for (index, result) in indexes.into_iter().zip(results) {
            if let Some(tx) = pending.remove(&index) {
                let _ = tx.send(result);
            }
        }
    }
}

#[async_trait::async_trait]
impl Consensus for Node {
    async fn propose(&self, op: WriteOp) -> elyra_core::Result<()> {
        let term = {
            let s = self.state.lock().unwrap();
            if s.role != Role::Leader {
                return Err(elyra_core::Error::Storage(
                    "not the leader: writes must go to the current leader".into(),
                ));
            }
            s.term
        };
        let data =
            bincode::serialize(&op).map_err(|e| elyra_core::Error::Storage(e.to_string()))?;
        let (tx, rx) = oneshot::channel();
        let index = {
            let mut log = self.log.lock().unwrap();
            let index = log.leader_append(term, data);
            self.pending.lock().unwrap().insert(index, tx);
            index
        };
        self.replicate.notify_one();
        let _ = index;
        match rx.await {
            Ok(r) => r,
            Err(_) => Err(elyra_core::Error::Storage(
                "commit not confirmed (leadership change)".into(),
            )),
        }
    }
}

/// Drive the read-only flag from elected leadership (leader = writable).
pub async fn follow_leadership(node: Arc<Node>, read_only: Arc<AtomicBool>) {
    let mut rx = node.leader_rx.clone();
    loop {
        let is_leader = matches!(rx.borrow_and_update().as_ref(), Some(l) if l.is_self);
        read_only.store(!is_leader, Ordering::Relaxed);
        if rx.changed().await.is_err() {
            break;
        }
    }
}

async fn send(stream: &mut TcpStream, m: &Msg) -> std::io::Result<()> {
    let bytes = bincode::serialize(m).map_err(|e| Error::other(e.to_string()))?;
    stream
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .await?;
    stream.write_all(&bytes).await?;
    Ok(())
}

/// Read one framed message; `None` on a clean end-of-stream.
async fn recv(stream: &mut TcpStream) -> std::io::Result<Option<Msg>> {
    let mut len = [0u8; 4];
    match stream.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = u32::from_le_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await?;
    bincode::deserialize(&buf)
        .map(Some)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
}

/// One-shot request/response RPC to a peer with a short timeout (elections,
/// membership).
async fn rpc(addr: &str, m: &Msg) -> std::io::Result<Msg> {
    let fut = async {
        let mut stream = TcpStream::connect(addr).await?;
        let _ = stream.set_nodelay(true);
        send(&mut stream, m).await?;
        recv(&mut stream)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "peer closed"))
    };
    tokio::time::timeout(Duration::from_millis(500), fut)
        .await
        .map_err(|_| Error::new(ErrorKind::TimedOut, "rpc timeout"))?
}

/// Send an AppendEntries on a persistent connection and await the ack (bounded).
async fn append_rpc(stream: &mut TcpStream, m: &Msg) -> std::io::Result<Msg> {
    let fut = async {
        send(stream, m).await?;
        recv(stream)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "peer closed"))
    };
    tokio::time::timeout(Duration::from_millis(500), fut)
        .await
        .map_err(|_| Error::new(ErrorKind::TimedOut, "append rpc timeout"))?
}

/// Operator command: add/remove a peer on the node at `node_addr`.
pub async fn send_membership(
    node_addr: &str,
    add: bool,
    id: u64,
    control_addr: String,
) -> std::io::Result<()> {
    let m = if add {
        Msg::AddPeer { id, control_addr }
    } else {
        Msg::RemovePeer { id }
    };
    match rpc(node_addr, &m).await? {
        Msg::CtlAck { ok: true } => Ok(()),
        _ => Err(Error::other("membership change was not acknowledged")),
    }
}

/// Parse `id@host:port` peer specs.
pub fn parse_peer(spec: &str) -> std::io::Result<Peer> {
    let (id, addr) = spec
        .split_once('@')
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "peer must be id@host:port"))?;
    Ok(Peer {
        id: id
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "peer id must be a number"))?,
        control_addr: addr.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn majority_sizes() {
        let db = Db::in_memory().unwrap();
        let mk = |n: usize| {
            let peers = (0..n)
                .map(|i| Peer {
                    id: i as u64,
                    control_addr: String::new(),
                })
                .collect();
            Node::new(
                ClusterConfig {
                    id: 99,
                    control_listen: String::new(),
                    replication_addr: String::new(),
                    peers,
                    state_path: None,
                    log_path: None,
                },
                db.clone(),
            )
            .majority()
        };
        assert_eq!(mk(0), 1);
        assert_eq!(mk(2), 2);
        assert_eq!(mk(4), 3);
    }
}
