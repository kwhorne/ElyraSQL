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
use std::sync::Arc;
use std::thread;

use elyra_core::{Error, Result};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::{RangeSnapshot, Snapshot, Storage};

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
    ack: oneshot::Sender<Result<()>>,
}

/// Shareable, high-concurrency database handle.
#[derive(Clone)]
pub struct Db {
    storage: Arc<Storage>,
    writer: mpsc::Sender<WriteJob>,
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

        // Dedicated OS thread owns all writes. redb is single-writer, so
        // serialising here (and group-committing) is the fast path, not a
        // bottleneck.
        let writer_storage = storage.clone();
        thread::Builder::new()
            .name("elyra-writer".into())
            .spawn(move || writer_loop(writer_storage, rx))
            .map_err(Error::Io)?;

        Ok(Self {
            storage,
            writer: tx,
        })
    }

    /// Take an MVCC read snapshot of the current committed state.
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.storage.snapshot()
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
fn writer_loop(storage: Arc<Storage>, mut rx: mpsc::Receiver<WriteJob>) {
    while let Some(first) = rx.blocking_recv() {
        // A validated (transactional) commit is applied on its own, so a
        // conflict fails only that transaction, not batched neighbours.
        if let Some(v) = &first.validation {
            let result = storage.apply_validated(&v.keys, &v.ranges, &first.puts, &first.deletes);
            let _ = first.ack.send(result);
            continue;
        }

        // Group consecutive plain writes into one transaction.
        let mut jobs = vec![first];
        let mut pending: Option<WriteJob> = None;
        while jobs.len() < GROUP_COMMIT_MAX {
            match rx.try_recv() {
                Ok(job) if job.validation.is_none() => jobs.push(job),
                Ok(job) => {
                    pending = Some(job); // validated: handle after this group
                    break;
                }
                Err(_) => break,
            }
        }

        // Fast path: a single job applies its own buffers with no copying.
        let result = if jobs.len() == 1 {
            storage.apply(&jobs[0].puts, &jobs[0].deletes)
        } else {
            let mut puts = Vec::new();
            let mut deletes = Vec::new();
            for j in &jobs {
                puts.extend_from_slice(&j.puts);
                deletes.extend_from_slice(&j.deletes);
            }
            storage.apply(&puts, &deletes)
        };
        for job in jobs {
            let r = match &result {
                Ok(()) => Ok(()),
                Err(e) => Err(Error::Storage(e.to_string())),
            };
            if job.ack.send(r).is_err() {
                warn!("write submitter dropped before commit acknowledgement");
            }
        }

        if let Some(job) = pending {
            let v = job.validation.as_ref().unwrap();
            let r = storage.apply_validated(&v.keys, &v.ranges, &job.puts, &job.deletes);
            let _ = job.ack.send(r);
        }
    }
}
