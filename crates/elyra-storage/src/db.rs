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

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use elyra_core::{Error, Result};
use tokio::sync::{broadcast, mpsc, oneshot};

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

/// Replication publisher shared with the writer thread.
#[derive(Clone)]
struct Repl {
    lsn: Arc<AtomicU64>,
    tx: broadcast::Sender<Arc<WriteEvent>>,
}

impl Repl {
    /// Assign the next LSN and broadcast a committed write-set.
    fn publish(&self, puts: Vec<(Vec<u8>, Vec<u8>)>, deletes: Vec<Vec<u8>>) {
        if puts.is_empty() && deletes.is_empty() {
            return;
        }
        let lsn = self.lsn.fetch_add(1, Ordering::SeqCst) + 1;
        // Ignore send errors: no subscribers is fine.
        let _ = self.tx.send(Arc::new(WriteEvent { lsn, puts, deletes }));
    }
}

/// Broadcast backlog kept for lagging replicas before they must re-snapshot.
const REPL_CAPACITY: usize = 16_384;

/// Optimistic validation performed atomically at commit: read/written keys must
/// still equal their snapshot values, and scanned ranges must be unchanged.
#[derive(Default)]
pub struct Validation {
    pub keys: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub ranges: Vec<RangeSnapshot>,
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
}

impl Db {
    /// Open the single ElyraSQL file and start the writer thread.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_storage(Storage::open(path)?)
    }

    /// In-memory database (tests / ephemeral).
    pub fn in_memory() -> Result<Self> {
        Self::from_storage(Storage::in_memory()?)
    }

    fn from_storage(storage: Storage) -> Result<Self> {
        let storage = Arc::new(storage);
        let (tx, rx) = mpsc::channel::<WriteJob>(WRITE_QUEUE_DEPTH);
        let lsn = Arc::new(AtomicU64::new(0));
        let (repl_tx, _) = broadcast::channel::<Arc<WriteEvent>>(REPL_CAPACITY);
        let repl = Repl {
            lsn: lsn.clone(),
            tx: repl_tx.clone(),
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
        })
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
        self.submit(puts, deletes, None).await
    }

    /// Submit a validated (transactional) commit: the validation must still
    /// hold before applying, else fail with [`elyra_core::Error::Conflict`].
    pub async fn commit_validated(
        &self,
        validation: Validation,
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    ) -> Result<()> {
        self.submit(puts, deletes, Some(validation)).await
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
fn writer_loop(storage: Arc<Storage>, mut rx: mpsc::Receiver<WriteJob>, repl: Repl) {
    while let Some(first) = rx.blocking_recv() {
        // A validated (transactional) commit is applied on its own, so a
        // conflict fails only that transaction, not batched neighbours.
        if let Some(v) = &first.validation {
            let r = storage.apply_validated(&v.keys, &v.ranges, &first.puts, &first.deletes);
            if r.is_ok() {
                repl.publish(first.puts.clone(), first.deletes.clone());
            }
            let _ = first.ack.send(r);
            continue;
        }

        // Group consecutive plain / INSERT writes into one transaction (one
        // fsync). INSERT jobs carry keys that must be new; duplicates are
        // detected inside the shared transaction.
        let mut jobs = vec![first];
        let mut pending: Option<WriteJob> = None;
        while jobs.len() < GROUP_COMMIT_MAX {
            match rx.try_recv() {
                Ok(job) if job.validation.is_none() => jobs.push(job),
                Ok(job) => {
                    pending = Some(job); // validated: handled alone below
                    break;
                }
                Err(_) => break,
            }
        }

        apply_job_group(&storage, jobs, &repl);

        if let Some(job) = pending {
            let v = job.validation.as_ref().unwrap();
            let r = storage.apply_validated(&v.keys, &v.ranges, &job.puts, &job.deletes);
            if r.is_ok() {
                repl.publish(job.puts.clone(), job.deletes.clone());
            }
            let _ = job.ack.send(r);
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
fn apply_job_group(storage: &Arc<Storage>, jobs: Vec<WriteJob>, repl: &Repl) {
    let apply_one = |j: &WriteJob| match &j.insert_new {
        Some(new) => storage.apply_insert(new, &j.puts, &j.deletes),
        None => storage.apply(&j.puts, &j.deletes),
    };

    if jobs.len() == 1 {
        let r = apply_one(&jobs[0]);
        if r.is_ok() {
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
            let mut all_puts = new.clone();
            all_puts.extend_from_slice(&puts);
            repl.publish(all_puts, deletes.clone());
            for job in jobs {
                let _ = job.ack.send(Ok(()));
            }
        }
        Err(Error::Duplicate(_)) => {
            // A duplicate somewhere in the group: redo each job on its own so
            // only the statement with the duplicate fails.
            for job in jobs {
                let r = apply_one(&job);
                if r.is_ok() {
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
