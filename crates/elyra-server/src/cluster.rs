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
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

use crate::repl::{cluster_client_tls_cached, cluster_server_tls, DuplexStream};

/// A boxed, possibly-TLS control-plane connection to a peer.
type PeerConn = Box<dyn DuplexStream>;
use tokio::sync::{oneshot, watch, Notify};
use tracing::{error, info, warn};

use crate::raftlog::{atomic_write, LogEntry, RaftLog};

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

/// The persisted election state: `(term, voted_for)`, `(0, None)` for a node
/// that never stored one. The file is written whole (`atomic_write`), so one
/// that cannot be read is an error. It used to count as "never voted": a
/// restarted node could then vote a second time in a term it had voted in.
/// The index of the last Raft entry the database holds, stored in the same
/// transaction as the entries it covers.
pub const APPLIED_KEY: &[u8] = b"meta::raft::applied";

/// Most committed entries applied in one transaction.
const APPLY_BATCH: usize = 1024;

/// The index of the last entry `db` holds (0 if none was ever applied).
fn applied_index(db: &Db) -> std::io::Result<u64> {
    let stored = db
        .snapshot()
        .and_then(|s| s.get(APPLIED_KEY))
        .map_err(|e| Error::other(format!("cannot read the applied Raft index: {e}")))?;
    match stored {
        None => Ok(0),
        Some(v) => v
            .as_slice()
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "the applied Raft index is damaged")),
    }
}

fn load_state(path: &Option<PathBuf>) -> std::io::Result<(u64, Option<u64>)> {
    let Some(p) = path else { return Ok((0, None)) };
    let s = match std::fs::read_to_string(p) {
        Ok(s) => s,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok((0, None)),
        Err(e) => return Err(e),
    };
    let bad = || {
        Error::new(
            ErrorKind::InvalidData,
            format!("{} is not a Raft election state", p.display()),
        )
    };
    let mut lines = s.lines().map(str::trim);
    let term = lines.next().and_then(|l| l.parse().ok()).ok_or_else(bad)?;
    let voted_for = match lines.next() {
        None | Some("") => None,
        Some(l) => Some(l.parse::<u64>().map_err(|_| bad())?),
    };
    Ok((term, voted_for))
}

fn persist_state(path: &Option<PathBuf>, term: u64, voted_for: Option<u64>) -> std::io::Result<()> {
    let Some(p) = path else { return Ok(()) };
    let body = format!(
        "{term}\n{}\n",
        voted_for.map(|v| v.to_string()).unwrap_or_default()
    );
    atomic_write(p, body.as_bytes())
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
    /// The `(term, voted_for)` last made durable. A node may learn of a later
    /// term without storing it (it only steps down), but it neither votes nor
    /// acknowledges entries until its term and vote are on disk.
    saved: (u64, Option<u64>),
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
    /// The Raft log could not be written the last time it was tried.
    log_failing: AtomicBool,
    /// The election state could not be written the last time it was tried.
    state_failing: AtomicBool,
    /// Committed entries could not be stored in the database the last time
    /// it was tried.
    apply_failing: AtomicBool,
    /// Held while committed entries are applied, so two appliers -- a
    /// follower's AppendEntries and a leader's round across a change of role
    /// -- never apply the same entries twice or out of order.
    apply_lock: tokio::sync::Mutex<()>,
}

impl Node {
    /// A node with its election state and log loaded from `cfg`'s files. A file
    /// that exists but cannot be read is an error: starting without what it
    /// holds would let the node vote and acknowledge as if it had never
    /// stored a thing.
    pub fn new(cfg: ClusterConfig, db: Db) -> std::io::Result<Arc<Self>> {
        let cfg_id = cfg.id;
        let peers = Mutex::new(cfg.peers.clone());
        let (leader_tx, leader_rx) = watch::channel(None);
        let (term, voted_for) = load_state(&cfg.state_path).map_err(|e| {
            Error::new(
                e.kind(),
                format!("cannot load the Raft election state: {e}"),
            )
        })?;
        let mut log = match &cfg.log_path {
            Some(p) => RaftLog::open(p.clone()).map_err(|e| {
                Error::new(
                    e.kind(),
                    format!("cannot load the Raft log {}: {e}", p.display()),
                )
            })?,
            None => RaftLog::new(),
        };
        // Go on from the last entry the database holds rather than applying
        // everything since the last compaction again.
        let applied = applied_index(&db)?;
        if applied > 0 {
            info!(
                id = cfg_id,
                applied, "resuming from the last applied Raft entry"
            );
        }
        log.restore_applied(applied);
        Ok(Arc::new(Node {
            cfg: Arc::new(cfg),
            peers,
            state: Arc::new(Mutex::new(State {
                term,
                voted_for,
                saved: (term, voted_for),
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
            log_failing: AtomicBool::new(false),
            state_failing: AtomicBool::new(false),
            apply_failing: AtomicBool::new(false),
            apply_lock: tokio::sync::Mutex::new(()),
        }))
    }

    /// Report a write of the Raft log or election state: an error once when
    /// writes start failing, and once more when they work again, rather than
    /// on every heartbeat in between.
    fn disk<T, E: std::fmt::Display>(
        &self,
        failing: &AtomicBool,
        what: &str,
        r: Result<T, E>,
    ) -> Result<T, E> {
        match &r {
            Err(e) => {
                if !failing.swap(true, Ordering::Relaxed) {
                    error!(
                        id = self.cfg.id,
                        error = %e,
                        "cannot write the Raft {what}: this node acknowledges, votes for \
                         and leads nothing that would depend on it until it can"
                    );
                }
            }
            Ok(_) => {
                if failing.swap(false, Ordering::Relaxed) {
                    info!(id = self.cfg.id, "the Raft {what} can be written again");
                }
            }
        }
        r
    }

    /// Make `s`'s term and vote durable if they changed since they last were.
    fn save_state(&self, s: &mut State) -> std::io::Result<()> {
        if (s.term, s.voted_for) == s.saved {
            return Ok(());
        }
        let r = persist_state(&self.cfg.state_path, s.term, s.voted_for);
        self.disk(&self.state_failing, "election state", r)?;
        s.saved = (s.term, s.voted_for);
        Ok(())
    }

    /// Make the whole log durable.
    fn sync_log(&self) -> std::io::Result<()> {
        let r = self.log.lock().unwrap().sync();
        self.disk(&self.log_failing, "log", r)
    }

    fn is_leader(&self) -> bool {
        self.state.lock().unwrap().role == Role::Leader
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
        if crate::listen_is_exposed(&self.cfg.control_listen)
            && !has_cluster_secret()
            && !crate::env_flag("ELYRASQL_ALLOW_OPEN_AUTH")
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "refusing to expose Raft control on {} without authentication; set \
                     ELYRASQL_CLUSTER_SECRET, bind to localhost, or explicitly enable \
                     ELYRASQL_ALLOW_OPEN_AUTH",
                    self.cfg.control_listen
                ),
            ));
        }
        let listener = TcpListener::bind(&self.cfg.control_listen).await?;
        let tls = cluster_server_tls()?.map(TlsAcceptor::from);
        info!(id = self.cfg.id, addr = %self.cfg.control_listen, tls = tls.is_some(), "raft control plane listening");
        if tls.is_none() {
            warn!(id = self.cfg.id, addr = %self.cfg.control_listen, "raft control plane is UNENCRYPTED - set ELYRASQL_CLUSTER_TLS_CERT/KEY, or run it on a private network/VPN");
        }
        let srv = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let _ = stream.set_nodelay(true);
                        let n = srv.clone();
                        let tls = tls.clone();
                        tokio::spawn(async move {
                            match tls {
                                Some(acceptor) => match acceptor.accept(stream).await {
                                    Ok(s) => {
                                        let _ = n.handle_control(s).await;
                                    }
                                    Err(e) => warn!(error = %e, "control TLS handshake failed"),
                                },
                                None => {
                                    let _ = n.handle_control(stream).await;
                                }
                            }
                        });
                    }
                    Err(e) => warn!(error = %e, "control accept failed"),
                }
            }
        });
        self.main_loop().await;
        Ok(())
    }

    async fn handle_control<S>(&self, mut stream: S) -> std::io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        // Authenticate the peer (no-op unless a cluster secret is configured).
        // Runs over whatever transport `S` is (plain TCP or a TLS stream).
        auth_accept(&mut stream).await?;
        // Loop over messages so a leader can reuse one connection for many
        // AppendEntries (batched throughput); one-shot RPCs simply send one
        // message and close, ending the loop at EOF.
        while let Some(msg) = recv(&mut stream).await? {
            let reply = match msg {
                // Membership is the leader's: it sends its member list with
                // every AppendEntries, and followers adopt it. A change made on
                // a follower was acknowledged and then overwritten by the next
                // heartbeat, so it is refused there.
                Msg::AddPeer { .. } | Msg::RemovePeer { .. } if !self.is_leader() => {
                    warn!(
                        id = self.cfg.id,
                        "membership change refused: not the leader"
                    );
                    Msg::CtlAck { ok: false }
                }
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
                    // An error closes the connection without an answer: the
                    // leader treats this node as unreachable, which it is for
                    // as long as it cannot store what it would acknowledge.
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
                    .await?
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
        if term > s.term {
            s.term = term;
            s.voted_for = None;
            if s.role != Role::Follower {
                s.role = Role::Follower;
            }
            let _ = self.save_state(&mut s);
        }
        // Election restriction: only vote for an at-least-as-up-to-date log.
        let up_to_date = self
            .log
            .lock()
            .unwrap()
            .is_up_to_date(last_log_term, last_log_index);
        let mut granted = term == s.term
            && (s.voted_for.is_none() || s.voted_for == Some(candidate))
            && up_to_date;
        let voted_before = s.voted_for;
        if granted {
            s.voted_for = Some(candidate);
            s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
        }
        // A vote counts only once it is on disk: one granted and then lost in
        // a restart lets this node vote a second time in the same term.
        if granted && self.save_state(&mut s).is_err() {
            s.voted_for = voted_before;
            granted = false;
        }
        let reply_term = s.term;
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
    ) -> std::io::Result<Msg> {
        {
            let mut s = self.state.lock().unwrap();
            if term < s.term {
                return Ok(Msg::AppendAck {
                    term: s.term,
                    success: false,
                    match_index: 0,
                });
            }
            if term > s.term {
                s.term = term;
                s.voted_for = None;
            }
            let was_leader = s.role == Role::Leader;
            s.role = Role::Follower;
            s.leader = Some(leader);
            s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
            // The term must be on disk before anything in it is acknowledged:
            // restarted with an earlier one, this node would take entries from
            // a deposed leader and could drop ones already committed.
            let saved = self.save_state(&mut s);
            if was_leader {
                drop(s);
                self.drain_pending("stepped down: no longer leader");
            }
            saved?;
        }
        self.adopt_members(&members);
        self.publish(Some(Leader {
            id: leader,
            is_self: leader == self.cfg.id,
            repl_addr,
        }));

        // Append + advance commit under the log lock.
        let (success, match_index) = {
            let mut log = self.log.lock().unwrap();
            let appended = log.append_entries(prev_index, prev_term, &entries);
            if self.disk(&self.log_failing, "log", appended)? {
                log.advance_commit(leader_commit);
                (true, log.last_index())
            } else {
                (false, 0)
            }
        };
        if success {
            self.apply_committed().await;
        }
        // Compact applied entries up to the cluster-wide safe point.
        if success {
            let compacted = self.log.lock().unwrap().compact(compact_index);
            let _ = self.disk(&self.log_failing, "log", compacted);
        }
        let reply_term = self.state.lock().unwrap().term;
        Ok(Msg::AppendAck {
            term: reply_term,
            success,
            match_index,
        })
    }

    async fn main_loop(self: Arc<Self>) {
        // Followers' connections and exchanges, kept across rounds for batched
        // throughput; reset when we stop being leader.
        let mut repl = Replication::new();
        loop {
            let (role, deadline) = {
                let s = self.state.lock().unwrap();
                (s.role, s.election_deadline)
            };
            match role {
                Role::Leader => {
                    self.replicate_round(&mut repl).await;
                    // Leader lease: if we haven't confirmed a quorum for the lease
                    // window, step down so writes fail fast and a healthy
                    // majority can elect a new leader (liveness).
                    if self.lease.lock().unwrap().elapsed() > Duration::from_millis(LEADER_LEASE_MS)
                    {
                        self.step_down("lost quorum contact (lease expired)");
                        repl.reset();
                        continue;
                    }
                    tokio::select! {
                        _ = self.replicate.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(HEARTBEAT_MS)) => {}
                    }
                }
                _ => {
                    repl.reset(); // stale once we're not leader
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
        // A candidate is elected on the log it presents, so that log -- and the
        // vote for itself -- must be on disk first. A node that cannot write
        // them does not stand.
        let log_ok = self.sync_log().is_ok();
        let (term, last_index, last_term) = {
            let mut s = self.state.lock().unwrap();
            s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
            if !log_ok {
                return;
            }
            let before = (s.term, s.voted_for);
            s.term += 1;
            s.voted_for = Some(self.cfg.id);
            if self.save_state(&mut s).is_err() {
                (s.term, s.voted_for) = before;
                return;
            }
            s.role = Role::Candidate;
            s.leader = None;
            let log = self.log.lock().unwrap();
            (s.term, log.last_index(), log.last_term())
        };
        info!(id = self.cfg.id, term, last_index, "standing for election");
        let m = Msg::RequestVote {
            term,
            candidate: self.cfg.id,
            last_log_index: last_index,
            last_log_term: last_term,
        };
        // Every peer at once, as a replication round does.
        let peers = self.peer_snapshot();
        let replies =
            futures_util::future::join_all(peers.iter().map(|p| rpc(&p.control_addr, &m))).await;
        let mut votes = 1;
        for reply in replies {
            if let Ok(Msg::Vote { term: t, granted }) = reply {
                let mut s = self.state.lock().unwrap();
                if t > s.term {
                    s.term = t;
                    s.role = Role::Follower;
                    s.voted_for = None;
                    let _ = self.save_state(&mut s);
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

    /// One leader replication round: push entries to every follower that is not
    /// still answering an earlier round, and commit as soon as a majority has
    /// them.
    ///
    /// Each exchange runs as its own task and answers on `repl.replies`. The
    /// round used to wait for every follower, so one that was unreachable held
    /// every write back until its deadline (`PEER_RPC_MS`) though the others
    /// formed a majority within a millisecond. A follower still answering is
    /// left to finish and is sent the next round's entries once it has.
    async fn replicate_round(self: &Arc<Self>, repl: &mut Replication) {
        let term = {
            let s = self.state.lock().unwrap();
            if s.role != Role::Leader {
                return;
            }
            s.term
        };
        // Durably persist all newly-appended entries once (batched fsync) before
        // they are replicated and counted toward commit. A leader that cannot
        // store its log steps down: a node that can should lead.
        if let Err(e) = self.sync_log() {
            self.step_down(&format!("cannot write the Raft log: {e}"));
            return;
        }
        let leader_commit = self.log.lock().unwrap().commit_index();
        let members = self.members();
        let peers = self.peer_snapshot();
        let live: std::collections::HashSet<u64> = peers.iter().map(|p| p.id).collect();
        repl.conns.retain(|id, _| live.contains(id));
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
        repl.round += 1;
        let round = repl.round;
        let mut launched = 0usize;
        for peer in &peers {
            if repl.inflight.contains_key(&peer.id) {
                continue;
            }
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
                // The follower needs entries this log no longer has. It refuses
                // the tail (see `RaftLog::append_entries`), so it is not counted
                // toward a quorum, but it cannot catch up either: that takes a
                // snapshot transfer, which does not exist yet.
                if prev_index < log.snapshot_index() {
                    if repl.stranded.insert(peer.id) {
                        warn!(
                            peer = peer.id,
                            needs_from = next,
                            log_starts_at = log.snapshot_index() + 1,
                            "a follower needs log entries that were compacted away; it \
                             cannot catch up and does not count toward the quorum \
                             (snapshot transfer is not supported yet)"
                        );
                    }
                } else {
                    repl.stranded.remove(&peer.id);
                }
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
            let conn = repl.conns.remove(&peer.id);
            let addr = peer.control_addr.clone();
            let tx = repl.tx.clone();
            let peer_id = peer.id;
            repl.inflight.insert(peer_id, round);
            launched += 1;
            tokio::spawn(async move {
                let (conn, reply) = tokio::time::timeout(
                    Duration::from_millis(PEER_RPC_MS),
                    append_exchange(conn, &addr, &m),
                )
                .await
                .unwrap_or((None, None));
                let _ = tx.send(PeerReply {
                    peer: peer_id,
                    term,
                    round,
                    conn,
                    reply,
                });
            });
        }

        // Collect replies until a majority of this round has acknowledged,
        // every follower sent to has answered, or the deadline passes.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(PEER_RPC_MS);
        let mut acks = 1usize; // this leader counts itself
        let mut answered = 0usize;
        while acks < self.majority() && answered < launched {
            let Ok(Some(reply)) = tokio::time::timeout_at(deadline, repl.replies.recv()).await
            else {
                break;
            };
            let this_round = reply.round == round;
            match self.take_reply(repl, term, reply) {
                ReplyOutcome::SteppedDown => return,
                ReplyOutcome::Acked if this_round => acks += 1,
                _ => {}
            }
            answered += usize::from(this_round);
        }
        // Whatever else has arrived only moves followers' progress on.
        while let Ok(reply) = repl.replies.try_recv() {
            if let ReplyOutcome::SteppedDown = self.take_reply(repl, term, reply) {
                return;
            }
        }

        // Renew the leader lease if a quorum acknowledged this round.
        if acks >= self.majority() {
            *self.lease.lock().unwrap() = Instant::now();
        }
        self.advance_commit_and_apply(term).await;
        // Compact the leader's own log up to the cluster-wide safe point.
        let compacted = self.log.lock().unwrap().compact(compact_index);
        let _ = self.disk(&self.log_failing, "log", compacted);
    }

    /// Record one follower's answer: keep its connection, move its progress on,
    /// or step down if it has seen a later term. An answer from an earlier
    /// leadership is dropped.
    fn take_reply(&self, repl: &mut Replication, term: u64, reply: PeerReply) -> ReplyOutcome {
        if repl.inflight.get(&reply.peer) == Some(&reply.round) {
            repl.inflight.remove(&reply.peer);
        }
        if reply.term != term {
            return ReplyOutcome::Ignored;
        }
        if let Some(conn) = reply.conn {
            repl.conns.insert(reply.peer, conn);
        }
        match reply.reply {
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
                        let _ = self.save_state(&mut s);
                        drop(s);
                        self.drain_pending("stepped down during replication");
                    }
                    return ReplyOutcome::SteppedDown;
                }
                let mut prog = self.progress.lock().unwrap();
                let p = prog
                    .entry(reply.peer)
                    .or_insert(Progress { next: 1, match_: 0 });
                if success {
                    p.match_ = match_index;
                    p.next = match_index + 1;
                    ReplyOutcome::Acked
                } else {
                    if p.next > 1 {
                        p.next -= 1; // back off and retry with an earlier prefix
                    }
                    ReplyOutcome::Refused
                }
            }
            Some(_) | None => {
                // Unreachable or unexpected reply: drop, retry next round.
                repl.conns.remove(&reply.peer);
                ReplyOutcome::Refused
            }
        }
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

    /// Store every committed entry the database does not hold yet, in order,
    /// a batch at a time: each batch in one transaction together with the
    /// index of its last entry ([`APPLIED_KEY`]). Returns each applied entry's
    /// result -- a write refused as a conflict or duplicate is applied too, as
    /// a refusal, the same on every node.
    ///
    /// Entries used to be submitted as separate writes, their results ignored
    /// on a follower and `last_applied` advanced before they were stored: an
    /// entry the database failed to store counted as applied, and when one
    /// group of writes failed a later one could still land, out of order. Now
    /// a batch that cannot be stored leaves nothing behind, and the next round
    /// or heartbeat tries it again.
    async fn apply_committed(&self) -> Vec<(u64, elyra_core::Result<()>)> {
        let _serial = self.apply_lock.lock().await;
        let mut done = Vec::new();
        loop {
            let batch = self.log.lock().unwrap().applicable(APPLY_BATCH);
            let Some(last) = batch.last().map(|e| e.index) else {
                break;
            };
            // An entry that does not decode is the same bytes on every node:
            // it is applied as nothing, and its proposer told so.
            let mut undecodable = HashMap::new();
            let ops: Vec<WriteOp> = batch
                .iter()
                .map(|e| {
                    bincode::deserialize::<WriteOp>(&e.data).unwrap_or_else(|err| {
                        undecodable.insert(e.index, err.to_string());
                        WriteOp::Plain {
                            puts: vec![],
                            deletes: vec![],
                        }
                    })
                })
                .collect();
            let meta = vec![(APPLIED_KEY.to_vec(), last.to_le_bytes().to_vec())];
            let stored = self.db.apply_ordered(ops, meta).await;
            let results = match self.disk(&self.apply_failing, "applied entries", stored) {
                Ok(results) => results,
                Err(_) => break,
            };
            self.log.lock().unwrap().mark_applied(last);
            for (e, r) in batch.iter().zip(results) {
                let r = match undecodable.remove(&e.index) {
                    Some(err) => Err(elyra_core::Error::Storage(err)),
                    None => r,
                };
                done.push((e.index, r));
            }
        }
        done
    }

    /// Compute the quorum commit index and apply newly committed entries.
    async fn advance_commit_and_apply(self: &Arc<Self>, term: u64) {
        {
            let mut log = self.log.lock().unwrap();
            // The leader counts itself only with what is on its disk: entries
            // proposed since its last sync may already be on the followers, but
            // they are not on this node until the next round syncs them.
            let mut match_indexes = vec![log.durable_index()];
            for p in self.progress.lock().unwrap().values() {
                match_indexes.push(p.match_);
            }
            log.maybe_commit(&mut match_indexes, term);
        }
        let applied = self.apply_committed().await;
        let mut pending = self.pending.lock().unwrap();
        for (index, result) in applied {
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

async fn send<W: AsyncWrite + Unpin>(stream: &mut W, m: &Msg) -> std::io::Result<()> {
    let bytes = bincode::serialize(m).map_err(|e| Error::other(e.to_string()))?;
    stream
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .await?;
    stream.write_all(&bytes).await?;
    Ok(())
}

/// Read one framed message; `None` on a clean end-of-stream.
/// Reject absurd frame lengths (corrupt/malicious peer) before allocating.
async fn recv<R: AsyncRead + Unpin>(stream: &mut R) -> std::io::Result<Option<Msg>> {
    let mut len = [0u8; 4];
    match stream.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = u32::from_le_bytes(len) as usize;
    if n > elyra_core::max_frame_bytes() {
        return Err(Error::new(ErrorKind::InvalidData, "frame too large"));
    }
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await?;
    bincode::deserialize(&buf)
        .map(Some)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
}

/// Optional pre-shared cluster secret (`ELYRASQL_CLUSTER_SECRET`). When set,
/// every Raft control/replication connection must complete a challenge-response
/// handshake, so an unauthenticated peer cannot inject fake writes or votes.
fn cluster_secret() -> Option<Vec<u8>> {
    std::env::var("ELYRASQL_CLUSTER_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
        .map(String::into_bytes)
}

/// Whether a pre-shared cluster secret is configured (gates exposing the
/// unauthenticated replication/control endpoints to remote peers).
pub(crate) fn has_cluster_secret() -> bool {
    cluster_secret().is_some()
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn response(secret: &[u8], nonce: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(secret);
    h.update(nonce);
    let d = h.finalize();
    let mut out = [0u8; 20];
    out.copy_from_slice(&d);
    out
}

/// Server side of the handshake: send a nonce, verify the peer's response.
/// Generic over the transport so it also runs over a TLS stream.
pub async fn auth_accept<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
) -> std::io::Result<()> {
    let Some(secret) = cluster_secret() else {
        return Ok(());
    };
    let mut nonce = [0u8; 16];
    getrandom::getrandom(&mut nonce).map_err(|_| Error::other("rng failure"))?;
    stream.write_all(&nonce).await?;
    let mut resp = [0u8; 20];
    stream.read_exact(&mut resp).await?;
    if !ct_eq(&resp, &response(&secret, &nonce)) {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "cluster auth failed",
        ));
    }
    Ok(())
}

/// Client side of the handshake: read the nonce, send the response.
/// Generic over the transport so it also runs over a TLS stream.
pub async fn auth_connect<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
) -> std::io::Result<()> {
    let Some(secret) = cluster_secret() else {
        return Ok(());
    };
    let mut nonce = [0u8; 16];
    stream.read_exact(&mut nonce).await?;
    stream.write_all(&response(&secret, &nonce)).await?;
    Ok(())
}

/// Client side of *peer* verification: after proving ourselves, require the
/// peer to prove knowledge of the shared secret too. Without this step any
/// host that accepts a TCP connection could impersonate a primary and feed
/// the replica arbitrary data (the replica applies what its primary sends).
/// No-op when no cluster secret is configured (explicit open-auth mode).
pub async fn auth_verify_peer<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
) -> std::io::Result<()> {
    let Some(secret) = cluster_secret() else {
        return Ok(());
    };
    let mut nonce = [0u8; 16];
    getrandom::getrandom(&mut nonce).map_err(|_| Error::other("rng failure"))?;
    stream.write_all(&nonce).await?;
    let mut resp = [0u8; 20];
    stream.read_exact(&mut resp).await?;
    if !ct_eq(&resp, &response(&secret, &nonce)) {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "primary failed cluster authentication",
        ));
    }
    Ok(())
}

/// Server side of peer verification: answer the client's challenge.
pub async fn auth_respond_to_peer<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
) -> std::io::Result<()> {
    let Some(secret) = cluster_secret() else {
        return Ok(());
    };
    let mut nonce = [0u8; 16];
    stream.read_exact(&mut nonce).await?;
    stream.write_all(&response(&secret, &nonce)).await?;
    Ok(())
}

/// Deadline for one exchange with a peer -- connecting, authenticating and the
/// request and its reply. Well inside the leader lease, so a round in which
/// some peers never answer still renews it with the others' acks.
const PEER_RPC_MS: u64 = 500;

/// The leader's side of replication, kept across rounds: persistent
/// AppendEntries connections to followers, the followers whose exchange is
/// still under way (and from which round), and the channel their answers
/// arrive on.
struct Replication {
    conns: HashMap<u64, PeerConn>,
    inflight: HashMap<u64, u64>,
    round: u64,
    tx: tokio::sync::mpsc::UnboundedSender<PeerReply>,
    replies: tokio::sync::mpsc::UnboundedReceiver<PeerReply>,
    /// Followers already reported as needing entries this log has compacted.
    stranded: std::collections::HashSet<u64>,
}

impl Replication {
    fn new() -> Self {
        let (tx, replies) = tokio::sync::mpsc::unbounded_channel();
        Replication {
            conns: HashMap::new(),
            inflight: HashMap::new(),
            round: 0,
            tx,
            replies,
            stranded: std::collections::HashSet::new(),
        }
    }

    /// Forget every follower's connection and exchange: no longer leader.
    /// Answers still to come are from an earlier term and will be dropped.
    fn reset(&mut self) {
        self.conns.clear();
        self.inflight.clear();
    }
}

/// One follower's answer to an AppendEntries, with the connection to keep.
struct PeerReply {
    peer: u64,
    term: u64,
    round: u64,
    conn: Option<PeerConn>,
    reply: Option<Msg>,
}

enum ReplyOutcome {
    Acked,
    Refused,
    Ignored,
    SteppedDown,
}

/// Open a control-plane connection to a peer: plain TCP, or TLS (verifying the
/// peer's certificate against `ELYRASQL_CLUSTER_TLS_CA`) when configured.
async fn connect_peer(addr: &str) -> std::io::Result<PeerConn> {
    let stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);
    match cluster_client_tls_cached()? {
        Some((connector, name)) => {
            let tls = connector.connect(name, stream).await?;
            Ok(Box::new(tls))
        }
        None => Ok(Box::new(stream)),
    }
}

/// One-shot request/response RPC to a peer with a short timeout (elections,
/// membership).
async fn rpc(addr: &str, m: &Msg) -> std::io::Result<Msg> {
    let fut = async {
        let mut stream = connect_peer(addr).await?;
        auth_connect(&mut stream).await?;
        send(&mut stream, m).await?;
        recv(&mut stream)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "peer closed"))
    };
    tokio::time::timeout(Duration::from_millis(PEER_RPC_MS), fut)
        .await
        .map_err(|_| Error::new(ErrorKind::TimedOut, "rpc timeout"))?
}

/// Send an AppendEntries on a persistent connection and await the ack (bounded).
async fn append_rpc<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    m: &Msg,
) -> std::io::Result<Msg> {
    let fut = async {
        send(stream, m).await?;
        recv(stream)
            .await?
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "peer closed"))
    };
    tokio::time::timeout(Duration::from_millis(PEER_RPC_MS), fut)
        .await
        .map_err(|_| Error::new(ErrorKind::TimedOut, "append rpc timeout"))?
}

/// One AppendEntries to a follower over its persistent connection, connecting
/// afresh if there is none or it failed. Returns the connection to keep, if
/// any, and the reply. Unbounded itself: the caller sets the deadline.
async fn append_exchange(
    conn: Option<PeerConn>,
    addr: &str,
    m: &Msg,
) -> (Option<PeerConn>, Option<Msg>) {
    if let Some(mut s) = conn {
        if let Ok(reply) = append_rpc(&mut s, m).await {
            return (Some(s), Some(reply));
        }
    }
    let Ok(mut s) = connect_peer(addr).await else {
        return (None, None);
    };
    if auth_connect(&mut s).await.is_err() {
        return (None, None);
    }
    match append_rpc(&mut s, m).await {
        Ok(reply) => (Some(s), Some(reply)),
        Err(_) => (None, None),
    }
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
        _ => Err(Error::other(
            "membership change was not acknowledged (send it to the current leader)",
        )),
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
            .unwrap()
            .majority()
        };
        assert_eq!(mk(0), 1);
        assert_eq!(mk(2), 2);
        assert_eq!(mk(4), 3);
    }

    /// A replication round and an election ask every peer at once, each within
    /// one deadline, so peers that never answer cost one deadline in all, not
    /// one each. Asked in turn, two silent peers kept a round past the leader
    /// lease, and a reconnect that stalled had no deadline at all.
    #[tokio::test]
    async fn silent_peers_cost_one_deadline_not_one_each() {
        // Listeners that are never accepted from: the connection is made, and
        // nothing is ever answered.
        let mut silent = Vec::new();
        let mut peers = Vec::new();
        for id in [2, 3] {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            peers.push(Peer {
                id,
                control_addr: l.local_addr().unwrap().to_string(),
            });
            silent.push(l);
        }
        let node = Node::new(
            ClusterConfig {
                id: 1,
                control_listen: String::new(),
                replication_addr: String::new(),
                peers,
                state_path: None,
                log_path: None,
            },
            Db::in_memory().unwrap(),
        )
        .unwrap();
        let within = Duration::from_millis(PEER_RPC_MS * 8 / 5);

        let started = Instant::now();
        node.start_election().await;
        assert!(
            started.elapsed() < within,
            "election took {:?}",
            started.elapsed()
        );
        assert_eq!(node.state.lock().unwrap().role, Role::Candidate);

        {
            let mut s = node.state.lock().unwrap();
            s.role = Role::Leader;
        }
        let started = Instant::now();
        node.replicate_round(&mut Replication::new()).await;
        assert!(
            started.elapsed() < within,
            "round took {:?}",
            started.elapsed()
        );
        assert!(started.elapsed() < Duration::from_millis(LEADER_LEASE_MS));
    }

    /// A round commits once a majority has the entries: one follower that
    /// answers is enough beside the leader of three, and one that never answers
    /// no longer holds every write back until its deadline -- nor the next
    /// round, which goes on without it while it is still being waited for.
    #[tokio::test]
    async fn a_round_commits_on_a_majority_without_waiting_for_a_silent_peer() {
        let answering = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peers = vec![
            Peer {
                id: 2,
                control_addr: answering.local_addr().unwrap().to_string(),
            },
            Peer {
                id: 3,
                control_addr: silent.local_addr().unwrap().to_string(),
            },
        ];
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = answering.accept().await {
                tokio::spawn(async move {
                    while let Ok(Some(Msg::AppendEntries {
                        term,
                        prev_index,
                        entries,
                        ..
                    })) = recv(&mut stream).await
                    {
                        let ack = Msg::AppendAck {
                            term,
                            success: true,
                            match_index: prev_index + entries.len() as u64,
                        };
                        if send(&mut stream, &ack).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        let node = Node::new(
            ClusterConfig {
                id: 1,
                control_listen: String::new(),
                replication_addr: String::new(),
                peers,
                state_path: None,
                log_path: None,
            },
            Db::in_memory().unwrap(),
        )
        .unwrap();
        {
            let mut s = node.state.lock().unwrap();
            s.role = Role::Leader;
            s.term = 1;
        }
        let write = bincode::serialize(&WriteOp::Plain {
            puts: vec![(b"k".to_vec(), b"v".to_vec())],
            deletes: vec![],
        })
        .unwrap();
        let mut repl = Replication::new();
        for round in 0..2 {
            let index = node.log.lock().unwrap().leader_append(1, write.clone());
            let started = Instant::now();
            node.replicate_round(&mut repl).await;
            assert!(
                started.elapsed() < Duration::from_millis(PEER_RPC_MS / 2),
                "round {round} took {:?}",
                started.elapsed()
            );
            assert!(
                node.log.lock().unwrap().commit_index() >= index,
                "round {round} did not commit"
            );
        }
        drop(silent);
    }

    // The control-plane framing must work over any AsyncRead+AsyncWrite transport
    // (this is what lets it run over a TLS stream, not just a raw TcpStream).
    fn single_node(dir: &std::path::Path) -> (Arc<Node>, Db) {
        let db = Db::open(dir.join("n.edb")).unwrap();
        let node = Node::new(
            ClusterConfig {
                id: 1,
                control_listen: String::new(),
                replication_addr: String::new(),
                peers: Vec::new(),
                state_path: Some(dir.join("n.raftstate")),
                log_path: Some(dir.join("n.raftlog")),
            },
            db.clone(),
        )
        .unwrap();
        (node, db)
    }

    fn entry(op: &WriteOp) -> Vec<u8> {
        bincode::serialize(op).unwrap()
    }

    /// A restarted node goes on from the last entry its database holds. It used
    /// to start at the last compaction and apply the entries after it again,
    /// on top of the newer state they had produced -- where a write that had
    /// been refused could now succeed, so the node diverged from the others.
    #[tokio::test]
    async fn a_restarted_node_does_not_apply_entries_again() {
        let dir = std::env::temp_dir().join(format!("elyra-raft-applied-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (y, z) = (b"data::t::y".to_vec(), b"data::t::z".to_vec());
        let put = |k: &[u8], v: &[u8]| WriteOp::Plain {
            puts: vec![(k.to_vec(), v.to_vec())],
            deletes: vec![],
        };
        {
            let (node, db) = single_node(&dir);
            {
                let mut log = node.log.lock().unwrap();
                // 1: y = 6. 2: a transaction that read y = 5, so it is refused,
                // and would have written z. 3: y = 5.
                log.leader_append(1, entry(&put(&y, b"6")));
                log.leader_append(
                    1,
                    entry(&WriteOp::Validated {
                        keys: vec![(y.clone(), Some(b"5".to_vec()))],
                        ranges: vec![],
                        puts: vec![(z.clone(), b"1".to_vec())],
                        deletes: vec![],
                    }),
                );
                log.leader_append(1, entry(&put(&y, b"5")));
                log.sync().unwrap();
                log.advance_commit(3);
            }
            let results = node.apply_committed().await;
            assert_eq!(results.len(), 3);
            assert!(matches!(results[1].1, Err(elyra_core::Error::Conflict(_))));
            // The first entry is compacted away; 2 and 3 stay in the log.
            node.log.lock().unwrap().compact(1).unwrap();
            assert_eq!(applied_index(&db).unwrap(), 3);
            let closed = db.close_waiter();
            drop(node);
            drop(db);
            assert!(closed.wait(Duration::from_secs(10)));
        }
        let (node, db) = single_node(&dir);
        assert_eq!(node.log.lock().unwrap().last_applied(), 3);
        // The leader tells it the commit index, as after any restart.
        node.log.lock().unwrap().advance_commit(3);
        assert!(node.apply_committed().await.is_empty());
        // Applied again on top of y = 5, entry 2 would now pass and write z.
        assert_eq!(db.get(z.clone()).await.unwrap(), None);
        assert_eq!(db.get(y.clone()).await.unwrap(), Some(b"5".to_vec()));
        let closed = db.close_waiter();
        drop(node);
        drop(db);
        closed.wait(Duration::from_secs(10));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_damaged_election_state_is_an_error_not_a_blank_one() {
        let dir = std::env::temp_dir().join(format!("elyra-raftstate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = Some(dir.join("n.raftstate"));
        assert_eq!(load_state(&path).unwrap(), (0, None));
        persist_state(&path, 7, Some(3)).unwrap();
        assert_eq!(load_state(&path).unwrap(), (7, Some(3)));
        persist_state(&path, 8, None).unwrap();
        assert_eq!(load_state(&path).unwrap(), (8, None));
        // Read as "never voted", this let a restarted node vote twice in a term.
        std::fs::write(path.as_ref().unwrap(), b"8\nnobody\n").unwrap();
        assert!(load_state(&path).is_err());
        std::fs::write(path.as_ref().unwrap(), b"").unwrap();
        assert!(load_state(&path).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn send_recv_roundtrip_over_generic_stream() {
        let (mut a, mut b) = tokio::io::duplex(4096);
        send(&mut a, &Msg::CtlAck { ok: true }).await.unwrap();
        send(&mut a, &Msg::RemovePeer { id: 7 }).await.unwrap();
        assert!(matches!(
            recv(&mut b).await.unwrap(),
            Some(Msg::CtlAck { ok: true })
        ));
        assert!(matches!(
            recv(&mut b).await.unwrap(),
            Some(Msg::RemovePeer { id: 7 })
        ));
    }
}
