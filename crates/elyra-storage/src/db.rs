//! High-concurrency database handle.
//!
//! Built for **large data + high traffic** on top of the single-file
//! [`Storage`](crate::Storage) engine:
//!
//! * **Reads scale freely.** Each read opens its own MVCC snapshot and runs
//!   on the blocking pool, so thousands of concurrent `SELECT`s never block
//!   the async reactor and never contend with each other.
//! * **Writes are group-committed.** All mutations funnel through one
//!   dedicated writer thread that folds every pending write into a single
//!   transaction. This turns a write lock-convoy under high concurrency into
//!   a throughput win.
//!
//! `Db` is cheap to clone (it is just handles) and safe to share across all
//! connection tasks.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use elyra_core::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot, Notify};

use crate::{RangeSnapshot, Snapshot, Storage};

/// A committed write-set, tagged with a monotonic log sequence number, streamed
/// to replicas. Applying events in LSN order is idempotent (puts overwrite,
/// deletes are absolute), so a replica converges to the primary's state.
#[derive(Debug)]
pub struct WriteEvent {
    pub lsn: u64,
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
    pub deletes: Vec<Vec<u8>>,
}

/// Commit-log sink owned by the writer thread: assigns LSNs, appends to the
/// binlog (for point-in-time recovery) and broadcasts to replicas.
struct Repl {
    lsn: Arc<AtomicU64>,
    tx: broadcast::Sender<Arc<WriteEvent>>,
    binlog: Option<crate::binlog::BinlogWriter>,
}

impl Repl {
    /// Whether any replica is currently attached.
    fn has_replicas(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    /// True when a committed write-set must be recorded (binlog or replicas).
    fn active(&self) -> bool {
        self.binlog.is_some() || self.has_replicas()
    }

    /// Assign the next LSN, append to the binlog, and broadcast a write-set.
    fn publish(&mut self, puts: Vec<(Vec<u8>, Vec<u8>)>, deletes: Vec<Vec<u8>>) {
        if puts.is_empty() && deletes.is_empty() {
            return;
        }
        let lsn = self.lsn.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(bl) = &mut self.binlog {
            if let Err(e) = bl.append(lsn, &puts, &deletes) {
                tracing::error!(error = %e, "binlog append failed");
            }
        }
        // Ignore send errors: no subscribers is fine.
        let _ = self.tx.send(Arc::new(WriteEvent { lsn, puts, deletes }));
    }
}

/// Broadcast backlog kept for lagging replicas before they must re-snapshot.
const REPL_CAPACITY: usize = 16_384;

/// Optimistic validation performed atomically at commit: read/written keys must
/// still equal their snapshot values, and scanned ranges must be unchanged.
#[derive(Default, Serialize, Deserialize)]
pub struct Validation {
    pub keys: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub ranges: Vec<RangeSnapshot>,
}

/// A replicable, deterministic mutation — the payload of a Raft log entry. Every
/// node applies these in log order and gets the same result.
#[derive(Serialize, Deserialize, Clone)]
pub enum WriteOp {
    /// Plain put/delete (no validation).
    Plain {
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    },
    /// Validated (transactional) commit: `keys`/`ranges` must still match.
    Validated {
        keys: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        ranges: Vec<RangeSnapshot>,
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    },
    /// Plain INSERT: `new` keys must not already exist.
    Insert {
        new: Vec<(Vec<u8>, Vec<u8>)>,
        aux: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    },
}

/// A consensus layer that replicates a mutation to a quorum before it is applied
/// (Raft). When installed on a [`Db`], the leader routes every mutation through
/// `propose`, which returns only once the entry is committed and applied.
#[async_trait::async_trait]
pub trait Consensus: Send + Sync {
    /// Propose a mutation; returns the deterministic apply result once committed.
    async fn propose(&self, op: WriteOp) -> Result<()>;
}

/// Max writes folded into a single group commit.
const GROUP_COMMIT_MAX: usize = 1024;
/// Bound on the writer queue → backpressure under write storms.
const WRITE_QUEUE_DEPTH: usize = 4096;

/// A single mutation submitted to the group-commit writer.
struct WriteJob {
    puts: Vec<(Vec<u8>, Vec<u8>)>,
    deletes: Vec<Vec<u8>>,
    /// When set, validate these `(key, snapshot-value)` pairs before applying
    /// (transactional commit); such jobs are applied alone, not merged.
    validation: Option<Validation>,
    /// When set, these keys must not already exist (plain `INSERT`); the write
    /// transaction detects duplicates itself. Applied alone, not merged.
    insert_new: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    ack: oneshot::Sender<Result<()>>,
}

/// Shareable, high-concurrency database handle.
#[derive(Clone)]
pub struct Db {
    storage: Arc<Storage>,
    writer: mpsc::Sender<WriteJob>,
    lsn: Arc<AtomicU64>,
    repl_tx: broadcast::Sender<Arc<WriteEvent>>,
    /// Per-replica highest acknowledged LSN (keyed by a registration id), for
    /// quorum/synchronous replication.
    replicas: Arc<Mutex<HashMap<u64, u64>>>,
    /// Woken whenever a replica advances its ack, so commit barriers re-check.
    ack_notify: Arc<Notify>,
    /// Allocator for replica registration ids.
    next_replica: Arc<AtomicU64>,
    /// Number of replica acks a commit must collect (0 = asynchronous).
    sync_replicas: Arc<AtomicU64>,
    /// Commit-barrier wait in ms (0 = wait indefinitely in strict mode).
    sync_timeout_ms: Arc<AtomicU64>,
    /// Strict mode: on timeout, fail the commit-confirmation instead of
    /// silently degrading to asynchronous.
    sync_strict: Arc<AtomicBool>,
    /// Binlog directory, if point-in-time recovery is enabled.
    binlog_dir: Option<std::path::PathBuf>,
    /// Optional consensus layer: when installed (cluster mode), the leader
    /// routes mutations through it (Raft) instead of committing locally.
    consensus: Arc<Mutex<Option<Arc<dyn Consensus>>>>,
}

impl Db {
    /// Open the single ElyraSQL file and start the writer thread.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_storage(Storage::open(path)?, None)
    }

    /// Open with an append-only binlog for point-in-time recovery.
    pub fn open_with_binlog(
        path: impl AsRef<Path>,
        binlog: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        Self::from_storage(Storage::open(path)?, binlog)
    }

    /// In-memory database (tests / ephemeral).
    pub fn in_memory() -> Result<Self> {
        Self::from_storage(Storage::in_memory()?, None)
    }

    fn from_storage(storage: Storage, binlog: Option<std::path::PathBuf>) -> Result<Self> {
        let storage = Arc::new(storage);
        let (tx, rx) = mpsc::channel::<WriteJob>(WRITE_QUEUE_DEPTH);
        // Resume the LSN counter from the binlog so it stays monotonic across
        // restarts (correct binlog ordering + incremental replica catch-up).
        let initial_lsn = match &binlog {
            Some(p) => crate::binlog::max_lsn(p).unwrap_or(0),
            None => 0,
        };
        let lsn = Arc::new(AtomicU64::new(initial_lsn));
        let (repl_tx, _) = broadcast::channel::<Arc<WriteEvent>>(REPL_CAPACITY);
        let replicas = Arc::new(Mutex::new(HashMap::new()));
        let ack_notify = Arc::new(Notify::new());
        let next_replica = Arc::new(AtomicU64::new(0));
        let sync_replicas = Arc::new(AtomicU64::new(0));
        let sync_timeout_ms = Arc::new(AtomicU64::new(0));
        let sync_strict = Arc::new(AtomicBool::new(false));
        let binlog_dir = binlog.clone();
        let binlog = match binlog {
            Some(p) => Some(crate::binlog::BinlogWriter::open(p)?),
            None => None,
        };
        let repl = Repl {
            lsn: lsn.clone(),
            tx: repl_tx.clone(),
            binlog,
        };

        // Dedicated OS thread owns all writes. redb is single-writer, so
        // serialising here (and group-committing) is the fast path, not a
        // bottleneck.
        let writer_storage = storage.clone();
        thread::Builder::new()
            .name("elyra-writer".into())
            .spawn(move || writer_loop(writer_storage, rx, repl))
            .map_err(Error::Io)?;

        Ok(Self {
            storage,
            writer: tx,
            lsn,
            repl_tx,
            replicas,
            ack_notify,
            next_replica,
            sync_replicas,
            sync_timeout_ms,
            sync_strict,
            binlog_dir,
            consensus: Arc::new(Mutex::new(None)),
        })
    }

    /// The binlog directory, if point-in-time recovery is enabled.
    pub fn binlog_dir(&self) -> Option<&std::path::Path> {
        self.binlog_dir.as_deref()
    }

    /// Install a consensus layer (Raft). After this, mutations are proposed
    /// through it (on the leader) rather than committed directly.
    pub fn set_consensus(&self, c: Arc<dyn Consensus>) {
        *self.consensus.lock().unwrap() = Some(c);
    }

    fn consensus(&self) -> Option<Arc<dyn Consensus>> {
        self.consensus.lock().unwrap().clone()
    }

    /// Apply a [`WriteOp`] to the local store via the group-commit writer,
    /// bypassing consensus. This is what the Raft apply loop calls on every node
    /// once an entry is committed.
    pub async fn apply_op_local(&self, op: WriteOp) -> Result<()> {
        match op {
            WriteOp::Plain { puts, deletes } => self.submit(puts, deletes, None).await,
            WriteOp::Validated {
                keys,
                ranges,
                puts,
                deletes,
            } => {
                self.submit(puts, deletes, Some(Validation { keys, ranges }))
                    .await
            }
            WriteOp::Insert { new, aux, deletes } => self.submit_insert(new, aux, deletes).await,
        }
    }

    /// Enable semi-synchronous replication: a commit waits up to `ms` for one
    /// replica to acknowledge before returning (0 disables). Kept for
    /// compatibility; equivalent to `set_sync_policy(1, ms, false)`.
    pub fn set_sync_timeout_ms(&self, ms: u64) {
        self.set_sync_policy(if ms == 0 { 0 } else { 1 }, ms, false);
    }

    /// Configure the commit replication barrier:
    /// * `required` — replica acks a commit must collect (0 = asynchronous),
    /// * `timeout_ms` — how long to wait (0 = indefinitely, only meaningful with
    ///   `strict`),
    /// * `strict` — on timeout, fail the commit-confirmation with an error
    ///   instead of degrading to asynchronous (no silent data-loss window).
    pub fn set_sync_policy(&self, required: u64, timeout_ms: u64, strict: bool) {
        self.sync_replicas.store(required, Ordering::SeqCst);
        self.sync_timeout_ms.store(timeout_ms, Ordering::SeqCst);
        self.sync_strict.store(strict, Ordering::SeqCst);
    }

    /// Register a replica for quorum accounting; returns its id.
    pub fn register_replica(&self) -> u64 {
        let id = self.next_replica.fetch_add(1, Ordering::SeqCst);
        self.replicas.lock().unwrap().insert(id, 0);
        self.ack_notify.notify_waiters();
        id
    }

    /// Deregister a replica (on disconnect).
    pub fn unregister_replica(&self, id: u64) {
        self.replicas.lock().unwrap().remove(&id);
        self.ack_notify.notify_waiters();
    }

    /// Record that replica `id` has applied up to `lsn`.
    pub fn report_ack(&self, id: u64, lsn: u64) {
        {
            let mut m = self.replicas.lock().unwrap();
            let e = m.entry(id).or_insert(0);
            if lsn > *e {
                *e = lsn;
            }
        }
        self.ack_notify.notify_waiters();
    }

    fn has_replicas(&self) -> bool {
        self.repl_tx.receiver_count() > 0
    }

    /// Number of replicas that have acknowledged through `target`.
    fn acked_count(&self, target: u64) -> usize {
        self.replicas
            .lock()
            .unwrap()
            .values()
            .filter(|&&v| v >= target)
            .count()
    }

    /// Commit replication barrier: wait until `sync_replicas` replicas have
    /// acknowledged the current LSN. In non-strict mode, degrade to success on
    /// timeout; in strict mode, return an error so the caller knows the commit
    /// was not confirmed by the quorum (the local write is already durable).
    async fn await_sync(&self) -> Result<()> {
        let required = self.sync_replicas.load(Ordering::SeqCst) as usize;
        if required == 0 {
            return Ok(());
        }
        let strict = self.sync_strict.load(Ordering::SeqCst);
        // Non-strict with no replicas attached: nothing to wait for.
        if !strict && !self.has_replicas() {
            return Ok(());
        }
        let target = self.current_lsn();
        let ms = self.sync_timeout_ms.load(Ordering::SeqCst);
        let deadline = if ms == 0 {
            None
        } else {
            Some(tokio::time::Instant::now() + std::time::Duration::from_millis(ms))
        };
        loop {
            let notified = self.ack_notify.notified();
            if self.acked_count(target) >= required {
                return Ok(());
            }
            tokio::pin!(notified);
            match deadline {
                None => notified.await,
                Some(dl) => {
                    tokio::select! {
                        _ = &mut notified => {}
                        _ = tokio::time::sleep_until(dl) => {
                            if self.acked_count(target) >= required {
                                return Ok(());
                            }
                            if strict {
                                return Err(Error::Storage(
                                    "sync replication timeout: commit not confirmed by quorum"
                                        .into(),
                                ));
                            }
                            return Ok(()); // degrade to asynchronous
                        }
                    }
                }
            }
        }
    }

    /// The current log sequence number (number of committed write-sets).
    pub fn current_lsn(&self) -> u64 {
        self.lsn.load(Ordering::SeqCst)
    }

    /// Subscribe to the stream of committed write-sets (for replication).
    pub fn repl_subscribe(&self) -> broadcast::Receiver<Arc<WriteEvent>> {
        self.repl_tx.subscribe()
    }

    /// Take an MVCC read snapshot of the current committed state.
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.storage.snapshot()
    }

    /// Hot-copy the whole database to a fresh file at `dest` from a consistent
    /// point-in-time snapshot, without blocking writers. Returns rows copied.
    pub async fn backup_to(&self, dest: std::path::PathBuf) -> Result<u64> {
        let snap = self.storage.snapshot()?;
        spawn_read(move || snap.backup_to(&dest)).await
    }

    /// Fetch a value by key (concurrent snapshot read).
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let storage = self.storage.clone();
        spawn_read(move || storage.get(&key)).await
    }

    /// Fetch many values in a single read transaction (see
    /// [`Storage::multi_get`]).
    pub async fn multi_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>> {
        let storage = self.storage.clone();
        spawn_read(move || storage.multi_get(&keys)).await
    }

    /// Cursor-based streaming scan: up to `limit` pairs under `prefix`,
    /// strictly after `after`. Backs bounded-memory table scans.
    pub async fn scan_batch(
        &self,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let storage = self.storage.clone();
        spawn_read(move || storage.scan_batch(&prefix, after.as_deref(), limit)).await
    }

    /// Ordered range scan over `[start, end)` (see [`Storage::scan_range`]).
    pub async fn scan_range(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let storage = self.storage.clone();
        spawn_read(move || storage.scan_range(&start, end.as_deref(), limit)).await
    }

    /// Submit a mutation to the group-commit writer and await durability.
    pub async fn commit(&self, puts: Vec<(Vec<u8>, Vec<u8>)>, deletes: Vec<Vec<u8>>) -> Result<()> {
        if let Some(c) = self.consensus() {
            return c.propose(WriteOp::Plain { puts, deletes }).await;
        }
        self.submit(puts, deletes, None).await?;
        self.await_sync().await
    }

    /// Submit a validated (transactional) commit: the validation must still
    /// hold before applying, else fail with [`elyra_core::Error::Conflict`].
    pub async fn commit_validated(
        &self,
        validation: Validation,
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    ) -> Result<()> {
        if let Some(c) = self.consensus() {
            let Validation { keys, ranges } = validation;
            return c
                .propose(WriteOp::Validated {
                    keys,
                    ranges,
                    puts,
                    deletes,
                })
                .await;
        }
        self.submit(puts, deletes, Some(validation)).await?;
        self.await_sync().await
    }

    /// Submit a plain `INSERT`: `new` keys must not already exist (duplicate
    /// detection happens in the write transaction), `aux` puts (index entries,
    /// counters) may overwrite. No separate existence read.
    pub async fn commit_insert(
        &self,
        new: Vec<(Vec<u8>, Vec<u8>)>,
        aux: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    ) -> Result<()> {
        if let Some(c) = self.consensus() {
            return c.propose(WriteOp::Insert { new, aux, deletes }).await;
        }
        self.submit_insert(new, aux, deletes).await?;
        self.await_sync().await
    }

    /// The local INSERT writer path (no consensus, no sync barrier).
    async fn submit_insert(
        &self,
        new: Vec<(Vec<u8>, Vec<u8>)>,
        aux: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    ) -> Result<()> {
        let (ack, wait) = oneshot::channel();
        self.writer
            .send(WriteJob {
                puts: aux,
                deletes,
                validation: None,
                insert_new: Some(new),
                ack,
            })
            .await
            .map_err(|_| Error::Storage("writer thread stopped".into()))?;
        wait.await
            .map_err(|_| Error::Storage("write acknowledgement lost".into()))?
    }

    async fn submit(
        &self,
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
        validation: Option<Validation>,
    ) -> Result<()> {
        let (ack, wait) = oneshot::channel();
        self.writer
            .send(WriteJob {
                puts,
                deletes,
                validation,
                insert_new: None,
                ack,
            })
            .await
            .map_err(|_| Error::Storage("writer thread stopped".into()))?;
        wait.await
            .map_err(|_| Error::Storage("write acknowledgement lost".into()))?
    }
}

/// Run a blocking storage closure on the blocking pool so the async reactor
/// stays free for other connections.
async fn spawn_read<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Storage(format!("read task failed: {e}")))?
}

/// The writer thread: block for one job, then greedily drain the queue and
/// commit everything in a single transaction (group commit).
fn writer_loop(storage: Arc<Storage>, mut rx: mpsc::Receiver<WriteJob>, mut repl: Repl) {
    // A job pulled from the queue that belongs to the *other* batch kind is
    // carried to the next iteration instead of being pushed back.
    let mut carry: Option<WriteJob> = None;
    loop {
        let first = match carry.take() {
            Some(j) => j,
            None => match rx.blocking_recv() {
                Some(j) => j,
                None => break,
            },
        };

        if first.validation.is_some() {
            // Group consecutive validated (transactional) commits into one
            // transaction (one fsync). Each is validated in turn against the
            // running state, so first-committer-wins ordering is preserved and a
            // conflict fails only that transaction.
            let mut jobs = vec![first];
            while jobs.len() < GROUP_COMMIT_MAX {
                match rx.try_recv() {
                    Ok(job) if job.validation.is_some() => jobs.push(job),
                    Ok(job) => {
                        carry = Some(job); // plain/insert: next iteration
                        break;
                    }
                    Err(_) => break,
                }
            }
            apply_validated_group(&storage, jobs, &mut repl);
            continue;
        }

        // Group consecutive plain / INSERT writes into one transaction (one
        // fsync). INSERT jobs carry keys that must be new; duplicates are
        // detected inside the shared transaction.
        let mut jobs = vec![first];
        while jobs.len() < GROUP_COMMIT_MAX {
            match rx.try_recv() {
                Ok(job) if job.validation.is_none() => jobs.push(job),
                Ok(job) => {
                    carry = Some(job); // validated: next iteration
                    break;
                }
                Err(_) => break,
            }
        }
        apply_job_group(&storage, jobs, &mut repl);
    }
}

/// Apply a group of validated (transactional) commits in one write transaction,
/// acking each with its individual result and replicating those that committed.
fn apply_validated_group(storage: &Arc<Storage>, jobs: Vec<WriteJob>, repl: &mut Repl) {
    // Fast path: a single transaction avoids building the borrow vector.
    if jobs.len() == 1 {
        let job = jobs.into_iter().next().unwrap();
        let v = job.validation.as_ref().unwrap();
        let r = storage.apply_validated(&v.keys, &v.ranges, &job.puts, &job.deletes);
        if r.is_ok() && repl.active() {
            repl.publish(job.puts.clone(), job.deletes.clone());
        }
        let _ = job.ack.send(r);
        return;
    }

    let commits: Vec<crate::ValidatedCommit> = jobs
        .iter()
        .map(|j| {
            let v = j.validation.as_ref().unwrap();
            crate::ValidatedCommit {
                keys: &v.keys,
                ranges: &v.ranges,
                puts: &j.puts,
                deletes: &j.deletes,
            }
        })
        .collect();

    match storage.apply_validated_batch(&commits) {
        Ok(results) => {
            for (job, r) in jobs.into_iter().zip(results) {
                if r.is_ok() && repl.active() {
                    repl.publish(job.puts.clone(), job.deletes.clone());
                }
                let _ = job.ack.send(r);
            }
        }
        Err(e) => {
            for job in jobs {
                let _ = job.ack.send(Err(Error::Storage(e.to_string())));
            }
        }
    }
}

/// The put set a job applies (INSERT `new` keys become plain puts on a replica).
fn job_puts(j: &WriteJob) -> Vec<(Vec<u8>, Vec<u8>)> {
    match &j.insert_new {
        Some(new) => {
            let mut p = new.clone();
            p.extend_from_slice(&j.puts);
            p
        }
        None => j.puts.clone(),
    }
}

/// Apply a group of plain / INSERT jobs in a single transaction (group commit).
/// The common case is one commit for the whole group. If the group contains an
/// INSERT whose key already exists, the combined transaction aborts and the
/// jobs are retried individually so only the offending statement fails.
fn apply_job_group(storage: &Arc<Storage>, jobs: Vec<WriteJob>, repl: &mut Repl) {
    let apply_one = |j: &WriteJob| match &j.insert_new {
        Some(new) => storage.apply_insert(new, &j.puts, &j.deletes),
        None => storage.apply(&j.puts, &j.deletes),
    };

    if jobs.len() == 1 {
        let r = apply_one(&jobs[0]);
        if r.is_ok() && repl.active() {
            repl.publish(job_puts(&jobs[0]), jobs[0].deletes.clone());
        }
        let _ = jobs.into_iter().next().unwrap().ack.send(r);
        return;
    }

    let mut new = Vec::new();
    let mut puts = Vec::new();
    let mut deletes = Vec::new();
    for j in &jobs {
        if let Some(n) = &j.insert_new {
            new.extend_from_slice(n);
        }
        puts.extend_from_slice(&j.puts);
        deletes.extend_from_slice(&j.deletes);
    }

    match storage.apply_insert(&new, &puts, &deletes) {
        Ok(()) => {
            if repl.active() {
                let mut all_puts = new.clone();
                all_puts.extend_from_slice(&puts);
                repl.publish(all_puts, deletes.clone());
            }
            for job in jobs {
                let _ = job.ack.send(Ok(()));
            }
        }
        Err(Error::Duplicate(_)) => {
            // A duplicate somewhere in the group: redo each job on its own so
            // only the statement with the duplicate fails.
            for job in jobs {
                let r = apply_one(&job);
                if r.is_ok() && repl.active() {
                    repl.publish(job_puts(&job), job.deletes.clone());
                }
                let _ = job.ack.send(r);
            }
        }
        Err(e) => {
            for job in jobs {
                let _ = job.ack.send(Err(Error::Storage(e.to_string())));
            }
        }
    }
}
