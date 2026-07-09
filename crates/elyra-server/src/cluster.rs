//! Automatic failover via Raft-style leader election.
//!
//! Each node runs an election state machine (terms, votes, majority, heartbeats,
//! step-down). The elected leader accepts writes and serves the replication
//! endpoint; followers are read-only and replicate from the current leader. On
//! leader failure, a follower whose election timer fires stands for election and,
//! with a majority of votes, becomes the new leader — no manual intervention.
//!
//! This provides leader election and fencing (a node only accepts writes while it
//! believes it is the leader for the current term). Log replication remains
//! asynchronous, so a newly elected leader may lack the old leader's last
//! unreplicated writes (the standard async-failover trade-off).

use std::io::{Error, ErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{info, warn};

/// A peer node in the cluster.
#[derive(Clone)]
pub struct Peer {
    pub id: u64,
    /// Control-plane address (election RPC).
    pub control_addr: String,
}

/// Cluster configuration for this node.
pub struct ClusterConfig {
    pub id: u64,
    pub control_listen: String,
    /// This node's replication endpoint, advertised to followers.
    pub replication_addr: String,
    pub peers: Vec<Peer>,
    /// File where the persistent election state (term + vote) is stored, so it
    /// survives restarts (a Raft safety requirement). `None` = in-memory only.
    pub state_path: Option<std::path::PathBuf>,
}

/// Load `(current_term, voted_for)` persisted by a previous run.
fn load_state(path: &Option<std::path::PathBuf>) -> (u64, Option<u64>) {
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

/// Durably persist `(current_term, voted_for)` before responding to any RPC.
fn persist_state(path: &Option<std::path::PathBuf>, term: u64, voted_for: Option<u64>) {
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
        /// Candidate's highest applied LSN — voters reject less up-to-date
        /// candidates (Raft election restriction) so failover never elects a
        /// node missing acknowledged writes.
        last_lsn: u64,
    },
    Vote {
        term: u64,
        granted: bool,
    },
    Heartbeat {
        term: u64,
        leader: u64,
        repl_addr: String,
        /// Current cluster membership (id, control_addr), propagated by the
        /// leader so followers converge on dynamic add/remove.
        members: Vec<(u64, String)>,
    },
    HeartbeatAck {
        term: u64,
    },
    /// Operator → node: add a peer to this node's membership.
    AddPeer {
        id: u64,
        control_addr: String,
    },
    /// Operator → node: remove a peer by id.
    RemovePeer {
        id: u64,
    },
    /// Response to AddPeer/RemovePeer.
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
    /// Deadline after which a follower/candidate starts a new election.
    election_deadline: Instant,
}

const HEARTBEAT_MS: u64 = 300;
const ELECTION_LO_MS: u64 = 1000;
const ELECTION_HI_MS: u64 = 2000;

/// A randomized election timeout, seeded by the node id, a global call counter,
/// and the clock, so different nodes reliably pick different timeouts (avoiding
/// perpetually split votes).
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
    /// The leader's replication address (for followers to replicate from).
    pub repl_addr: String,
}

/// A source of this node's highest applied LSN (for the election restriction).
pub type LsnSource = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Handle to a running election node.
pub struct Node {
    cfg: Arc<ClusterConfig>,
    /// Live membership (mutable for dynamic add/remove).
    peers: Mutex<Vec<Peer>>,
    state: Arc<Mutex<State>>,
    lsn_source: LsnSource,
    leader_tx: watch::Sender<Option<Leader>>,
    pub leader_rx: watch::Receiver<Option<Leader>>,
}

impl Node {
    pub fn new(cfg: ClusterConfig, lsn_source: LsnSource) -> Arc<Self> {
        let cfg_id = cfg.id;
        let peers = Mutex::new(cfg.peers.clone());
        let (leader_tx, leader_rx) = watch::channel(None);
        // Resume persistent election state (term + vote) across restarts.
        let (term, voted_for) = load_state(&cfg.state_path);
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
            lsn_source,
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

    /// Adopt a membership list advertised by the leader (peers = members minus
    /// self). Idempotent.
    fn adopt_members(&self, members: &[(u64, String)]) {
        let mut p = self.peers.lock().unwrap();
        let new: Vec<Peer> = members
            .iter()
            .filter(|(id, _)| *id != self.cfg.id)
            .map(|(id, addr)| Peer {
                id: *id,
                control_addr: addr.clone(),
            })
            .collect();
        *p = new;
    }

    /// The full membership (this node + peers) as (id, control_addr) pairs.
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

    /// Start the control listener and the election/heartbeat loops.
    pub async fn run(self: Arc<Self>) -> std::io::Result<()> {
        let listener = TcpListener::bind(&self.cfg.control_listen).await?;
        info!(id = self.cfg.id, addr = %self.cfg.control_listen, "cluster control plane listening");
        let srv = self.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let n = srv.clone();
                        tokio::spawn(async move {
                            let _ = n.handle_control(stream).await;
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "control accept failed");
                    }
                }
            }
        });
        self.election_loop().await;
        Ok(())
    }

    async fn handle_control(&self, mut stream: TcpStream) -> std::io::Result<()> {
        let msg = recv(&mut stream).await?;

        // Membership control commands don't touch election state.
        match &msg {
            Msg::AddPeer { id, control_addr } => {
                {
                    let mut p = self.peers.lock().unwrap();
                    if *id != self.cfg.id && !p.iter().any(|x| x.id == *id) {
                        p.push(Peer {
                            id: *id,
                            control_addr: control_addr.clone(),
                        });
                    }
                }
                info!(id = self.cfg.id, added = *id, "peer added");
                return send(&mut stream, &Msg::CtlAck { ok: true }).await;
            }
            Msg::RemovePeer { id } => {
                self.peers.lock().unwrap().retain(|x| x.id != *id);
                info!(id = self.cfg.id, removed = *id, "peer removed");
                return send(&mut stream, &Msg::CtlAck { ok: true }).await;
            }
            _ => {}
        }

        // Compute the reply (and any leader to publish) entirely under the lock,
        // then release it before doing any IO.
        let mut adopt: Option<Vec<(u64, String)>> = None;
        let before;
        let after;
        let (reply, publish) = {
            let mut s = self.state.lock().unwrap();
            before = (s.term, s.voted_for);
            let r = match msg {
                Msg::RequestVote {
                    term,
                    candidate,
                    last_lsn,
                } => {
                    if term > s.term {
                        s.term = term;
                        s.voted_for = None;
                        s.role = Role::Follower;
                    }
                    // Election restriction: only vote for a candidate at least as
                    // up-to-date as us, so an elected leader has every
                    // quorum-acknowledged write (no zero-data-loss violation).
                    let up_to_date = last_lsn >= (self.lsn_source)();
                    let granted = term == s.term
                        && (s.voted_for.is_none() || s.voted_for == Some(candidate))
                        && up_to_date;
                    if granted {
                        s.voted_for = Some(candidate);
                        s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
                    }
                    (
                        Msg::Vote {
                            term: s.term,
                            granted,
                        },
                        None,
                    )
                }
                Msg::Heartbeat {
                    term,
                    leader,
                    repl_addr,
                    members,
                } => {
                    if term >= s.term {
                        s.term = term;
                        s.role = Role::Follower;
                        s.leader = Some(leader);
                        s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
                        adopt = Some(members);
                        (
                            Msg::HeartbeatAck { term },
                            Some(Leader {
                                id: leader,
                                is_self: leader == self.cfg.id,
                                repl_addr,
                            }),
                        )
                    } else {
                        (Msg::HeartbeatAck { term: s.term }, None)
                    }
                }
                _ => (Msg::HeartbeatAck { term: s.term }, None),
            };
            after = (s.term, s.voted_for);
            r
        };
        // Persist term/vote before responding whenever they changed (Raft safety).
        if after != before {
            persist_state(&self.cfg.state_path, after.0, after.1);
        }
        if let Some(m) = adopt {
            self.adopt_members(&m);
        }
        if let Some(l) = publish {
            self.publish(Some(l));
        }
        send(&mut stream, &reply).await
    }

    async fn election_loop(&self) {
        loop {
            let (role, deadline) = {
                let s = self.state.lock().unwrap();
                (s.role, s.election_deadline)
            };
            match role {
                Role::Leader => {
                    self.send_heartbeats().await;
                    tokio::time::sleep(Duration::from_millis(HEARTBEAT_MS)).await;
                }
                _ => {
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

    async fn start_election(&self) {
        let term = {
            let mut s = self.state.lock().unwrap();
            s.term += 1;
            s.role = Role::Candidate;
            s.voted_for = Some(self.cfg.id);
            s.leader = None;
            s.election_deadline = Instant::now() + election_timeout(self.cfg.id);
            s.term
        };
        // Persist the incremented term + self-vote before soliciting votes.
        persist_state(&self.cfg.state_path, term, Some(self.cfg.id));
        let last_lsn = (self.lsn_source)();
        info!(id = self.cfg.id, term, last_lsn, "standing for election");
        let mut votes = 1; // vote for self
        for peer in self.peer_snapshot() {
            let m = Msg::RequestVote {
                term,
                candidate: self.cfg.id,
                last_lsn,
            };
            if let Ok(Msg::Vote { term: t, granted }) = rpc(&peer.control_addr, &m).await {
                let mut s = self.state.lock().unwrap();
                if t > s.term {
                    s.term = t;
                    s.role = Role::Follower;
                    s.voted_for = None;
                    return;
                }
                if granted && s.role == Role::Candidate && s.term == term {
                    votes += 1;
                }
            }
        }
        let mut s = self.state.lock().unwrap();
        if s.role == Role::Candidate && s.term == term && votes >= self.majority() {
            s.role = Role::Leader;
            s.leader = Some(self.cfg.id);
            drop(s);
            info!(id = self.cfg.id, term, votes, "elected leader");
            self.publish(Some(Leader {
                id: self.cfg.id,
                is_self: true,
                repl_addr: self.cfg.replication_addr.clone(),
            }));
        }
    }

    async fn send_heartbeats(&self) {
        let term = {
            let s = self.state.lock().unwrap();
            if s.role != Role::Leader {
                return;
            }
            s.term
        };
        let members = self.members();
        for peer in self.peer_snapshot() {
            let m = Msg::Heartbeat {
                term,
                leader: self.cfg.id,
                repl_addr: self.cfg.replication_addr.clone(),
                members: members.clone(),
            };
            if let Ok(Msg::HeartbeatAck { term: t }) = rpc(&peer.control_addr, &m).await {
                if t > term {
                    let mut s = self.state.lock().unwrap();
                    if t > s.term {
                        s.term = t;
                        s.role = Role::Follower;
                        s.voted_for = None;
                    }
                    return;
                }
            }
        }
    }
}

/// Drive a dynamic read-only flag + replica connection from elected leadership.
pub async fn follow_leadership(
    node: Arc<Node>,
    db: elyra_storage::Db,
    read_only: Arc<AtomicBool>,
    self_id: u64,
) {
    let mut rx = node.leader_rx.clone();
    let mut current: Option<String> = None; // replication addr we're following
    let mut replica_task: Option<tokio::task::JoinHandle<()>> = None;
    loop {
        let leader = rx.borrow_and_update().clone();
        match leader {
            Some(l) if l.is_self || l.id == self_id => {
                read_only.store(false, Ordering::Relaxed);
                if let Some(t) = replica_task.take() {
                    t.abort();
                }
                current = None;
            }
            Some(l) => {
                read_only.store(true, Ordering::Relaxed);
                if current.as_deref() != Some(l.repl_addr.as_str()) {
                    if let Some(t) = replica_task.take() {
                        t.abort();
                    }
                    let db2 = db.clone();
                    let addr = l.repl_addr.clone();
                    current = Some(addr.clone());
                    replica_task = Some(tokio::spawn(async move {
                        if let Err(e) = crate::repl::run_replica(addr, db2).await {
                            warn!(error = %e, "replica stream ended");
                        }
                    }));
                }
            }
            None => {
                // No known leader: stay read-only until one is elected.
                read_only.store(true, Ordering::Relaxed);
            }
        }
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

async fn recv(stream: &mut TcpStream) -> std::io::Result<Msg> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let n = u32::from_le_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    stream.read_exact(&mut buf).await?;
    bincode::deserialize(&buf).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
}

/// One-shot request/response RPC to a peer with a short timeout.
async fn rpc(addr: &str, m: &Msg) -> std::io::Result<Msg> {
    let fut = async {
        let mut stream = TcpStream::connect(addr).await?;
        send(&mut stream, m).await?;
        recv(&mut stream).await
    };
    tokio::time::timeout(Duration::from_millis(500), fut)
        .await
        .map_err(|_| Error::new(ErrorKind::TimedOut, "rpc timeout"))?
}

/// Operator command: add or remove a peer at `control_addr` on the node whose
/// control plane is at `node_addr`. Prefer sending to the current leader, which
/// propagates membership to followers via heartbeats.
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
                },
                Arc::new(|| 0),
            )
            .majority()
        };
        assert_eq!(mk(0), 1); // single node
        assert_eq!(mk(2), 2); // 3 nodes -> 2
        assert_eq!(mk(4), 3); // 5 nodes -> 3
    }
}
