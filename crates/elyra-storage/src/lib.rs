//! ElyraSQL storage engine.
//!
//! Everything ElyraSQL persists lives in **one file**. Internally that file
//! is a `redb` database (ACID, MVCC, crash-safe) partitioned into logical
//! namespaces via table-name prefixes:
//!
//! | Namespace prefix | Contents                          |
//! |------------------|-----------------------------------|
//! | `catalog::`      | table/schema definitions          |
//! | `data::<table>`  | row data, keyed by primary/rowid  |
//! | `index::<name>`  | secondary + vector index payloads |
//! | `meta::`         | server metadata, version, config  |
//!
//! Callers only ever see ElyraSQL types; `redb` never leaks past this crate.

pub mod binlog;
mod db;
pub use db::{CloseWaiter, Consensus, Db, Validation, WriteEvent, WriteOp};

mod cache;
pub use cache::{page_cache_bytes, CacheSource, PAGE_CACHE_ENV};

use std::path::Path;
use std::sync::Arc;

use elyra_core::{DuplicateError, Error, Result};
use redb::{
    Database, Durability, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction,
};

/// A scanned range and its content at snapshot time, validated on a
/// serializable commit to detect phantoms and concurrent range changes.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct RangeSnapshot {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub content: Vec<(Vec<u8>, Vec<u8>)>,
}

/// One validated (transactional) commit, borrowed for group-commit batching
/// (see [`Storage::apply_validated_batch`]).
pub struct ValidatedCommit<'a> {
    pub keys: &'a [(Vec<u8>, Option<Vec<u8>>)],
    pub ranges: &'a [RangeSnapshot],
    pub puts: &'a [(Vec<u8>, Vec<u8>)],
    pub deletes: &'a [Vec<u8>],
}

/// A point-in-time MVCC read snapshot. Reads through it always observe the
/// committed state as of when it was taken, regardless of later commits — the
/// basis for snapshot-isolated transactions.
#[derive(Clone)]
pub struct Snapshot {
    rtx: Arc<ReadTransaction>,
}

impl Snapshot {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let t = self
            .rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(t.get(key)
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|v| v.value().to_vec()))
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let t = self
            .rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(
                t.get(k.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?
                    .map(|v| v.value().to_vec()),
            );
        }
        Ok(out)
    }

    /// Copy this consistent snapshot's entire keyspace into a fresh ElyraSQL
    /// database file at `dest`, returning the number of key/value pairs written.
    /// The snapshot is a point-in-time view, so the backup is consistent even
    /// while the source is being written concurrently. Refuses to overwrite an
    /// existing file. Writes are flushed in bounded batches to cap memory.
    pub fn backup_to(&self, dest: &Path) -> Result<u64> {
        if dest.exists() {
            return Err(Error::Storage(format!(
                "backup target already exists: {}",
                dest.display()
            )));
        }
        const BATCH: usize = 50_000;
        let out = Database::create(dest).map_err(|e| Error::Storage(e.to_string()))?;
        let src = self
            .rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;

        let flush = |batch: &mut Vec<(Vec<u8>, Vec<u8>)>| -> Result<()> {
            if batch.is_empty() {
                return Ok(());
            }
            let wtx = out
                .begin_write()
                .map_err(|e| Error::Storage(e.to_string()))?;
            {
                let mut t = wtx
                    .open_table(KV)
                    .map_err(|e| Error::Storage(e.to_string()))?;
                for (k, v) in batch.iter() {
                    t.insert(k.as_slice(), v.as_slice())
                        .map_err(|e| Error::Storage(e.to_string()))?;
                }
            }
            wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
            batch.clear();
            Ok(())
        };

        let mut count = 0u64;
        let mut batch: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH);
        for item in src.iter().map_err(|e| Error::Storage(e.to_string()))? {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            batch.push((k.value().to_vec(), v.value().to_vec()));
            count += 1;
            if batch.len() >= BATCH {
                flush(&mut batch)?;
            }
        }
        flush(&mut batch)?;
        Ok(count)
    }

    /// Ordered range scan over `[start, end)` within the snapshot.
    pub fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::ops::Bound;
        let t = self
            .rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let upper = match end {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        };
        let mut out = Vec::with_capacity(limit.min(1024));
        for item in t
            .range::<&[u8]>((Bound::Included(start), upper))
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            out.push((k.value().to_vec(), v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Streaming ordered scan over `[start, end)` within this snapshot, invoking
    /// `f` for each key/value in order. Reads through the snapshot's fixed read
    /// transaction, so several parallel workers that share one snapshot observe a
    /// single consistent point-in-time view -- unlike opening a fresh read per
    /// worker, which would let workers see the table at different commit points.
    pub fn scan_range_each<F>(&self, start: &[u8], end: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        use std::ops::Bound;
        let t = self
            .rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        for item in t
            .range::<&[u8]>((Bound::Included(start), Bound::Excluded(end)))
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            f(k.value(), v.value())?;
        }
        Ok(())
    }

    /// Streaming scan over every key/value under `prefix` within this snapshot.
    pub fn scan_prefix_each<F>(&self, prefix: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        use std::ops::Bound;
        let t = self
            .rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let upper = prefix_upper_bound(prefix);
        let range = match &upper {
            Some(e) => t.range::<&[u8]>((Bound::Included(prefix), Bound::Excluded(e.as_slice()))),
            None => t.range::<&[u8]>((Bound::Included(prefix), Bound::Unbounded)),
        }
        .map_err(|e| Error::Storage(e.to_string()))?;
        for item in range {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            f(k.value(), v.value())?;
        }
        Ok(())
    }
}

/// Key/value blob table. All logical namespaces share this definition; the
/// key carries the namespace prefix so we keep a single flat keyspace and
/// therefore a single file.
const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("elyra_kv");

/// Monotonic write sequence, persisted at `meta::wseq` *inside every write
/// transaction*. Because it commits atomically with the data, any read snapshot
/// observes the `wseq` that exactly identifies its committed state -- the basis
/// for race-free cache invalidation in the engine (a cached artifact tagged
/// with a `wseq` is valid iff the current committed `wseq` still matches).
///
/// The counter is per database and resumes from the persisted value when the
/// file is opened. It used to be one process-wide counter starting at 1, so
/// after a restart the sequence ran through values the file had already had,
/// and an engine cache tagged with one of them was served again for a
/// different state of the data.
const WSEQ_KEY: &[u8] = b"meta::wseq";

fn decode_wseq(v: Option<&[u8]>) -> u64 {
    v.and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

/// Key prefixes of the schema: table definitions and their declared widths and
/// storage generations, views, materialized views, accounts, grants, roles,
/// triggers and procedures. The engine caches what it reads under these.
pub const SCHEMA_PREFIXES: &[&[u8]] = &[
    b"sys::",
    b"catalog::",
    b"view::",
    b"viewsec::",
    b"matview::",
    b"coldecl::",
    b"colwidth::",
    b"meta::generation::",
];

/// Generation of the schema keyspace ([`SCHEMA_PREFIXES`]). Bumped *after*
/// every committed write that touches a schema key, whichever path it came by
/// -- a session, a replica applying its primary's stream, a cluster follower
/// -- and whenever a database is opened. A cache of schema reads tagged with
/// the generation it was read at is therefore valid while the generation is
/// unchanged. Bumping after the commit is what makes that race-free: a reader
/// loads the generation before reading, so any commit it might have missed
/// bumps the generation past the tag. Process-wide, so a database reopened in
/// the same process (a replica re-bootstrapping) never matches an old tag.
static SCHEMA_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// The current [`SCHEMA_GEN`] generation.
pub fn schema_generation() -> u64 {
    SCHEMA_GEN.load(std::sync::atomic::Ordering::Acquire)
}

fn bump_schema_generation() {
    SCHEMA_GEN.fetch_add(1, std::sync::atomic::Ordering::Release);
}

/// Whether any key is a schema key.
fn touches_schema<'a>(mut keys: impl Iterator<Item = &'a [u8]>) -> bool {
    keys.any(|k| SCHEMA_PREFIXES.iter().any(|p| k.starts_with(p)))
}

/// Counters that hand out row identities: the AUTO_INCREMENT counter and the
/// hidden row id of a table without a primary key.
const COUNTER_PREFIXES: [&[u8]; 2] = [b"meta::autoinc::", b"meta::rowid::"];

/// Whether `key` is an id counter (see [`COUNTER_PREFIXES`]): a write that
/// must set one exactly -- not only move it on -- deletes it in the same write.
pub fn is_id_counter(key: &[u8]) -> bool {
    COUNTER_PREFIXES.iter().any(|p| key.starts_with(p))
}

/// Write `value` at `key` -- except that a counter (see [`COUNTER_PREFIXES`])
/// never moves backwards unless the same write deletes it first.
///
/// Concurrent statements take ids from one in-memory counter and each persists
/// its view of it, but their commits can land in either order. A later commit
/// of a smaller value would wind the stored counter back, and after a restart
/// it would hand out ids that rows already hold. A deliberate reset (TRUNCATE,
/// DROP, a rebuild) deletes the key, which still works.
fn put_kv(
    t: &mut redb::Table<'_, &[u8], &[u8]>,
    key: &[u8],
    value: &[u8],
    deletes: &[Vec<u8>],
) -> Result<()> {
    if value.len() == 8
        && COUNTER_PREFIXES.iter().any(|p| key.starts_with(p))
        && !deletes.iter().any(|d| d.as_slice() == key)
    {
        let stored_is_ahead = match t.get(key).map_err(|e| Error::Storage(e.to_string()))? {
            Some(current) => {
                let current = current.value();
                current.len() == 8
                    && u64::from_le_bytes(current.try_into().expect("checked length"))
                        >= u64::from_le_bytes(value.try_into().expect("checked length"))
            }
            None => false,
        };
        if stored_is_ahead {
            return Ok(());
        }
    }
    t.insert(key, value)
        .map_err(|e| Error::Storage(e.to_string()))?;
    Ok(())
}

/// The LSN of the last binlog record whose write-set the database holds,
/// written in the same transaction as that write-set. At open, binlog records
/// past it are write-sets the binlog made durable but the database did not.
pub const BINLOG_LSN_KEY: &[u8] = b"meta::binlog_lsn";

/// Runs inside a group's write transaction, after every job is applied and
/// before the commit, with each job's outcome (`true` = applied). Returns the
/// LSN to record under [`BINLOG_LSN_KEY`], if any. An error aborts the whole
/// transaction: nothing of the group is committed.
pub type BeforeCommit<'a> = &'a mut dyn FnMut(&[bool]) -> Result<Option<u64>>;

/// Record the LSN the [`BeforeCommit`] hook returned, in the open transaction.
fn record_binlog_lsn(
    t: &mut redb::Table<'_, &[u8], &[u8]>,
    outcomes: &[bool],
    before_commit: BeforeCommit<'_>,
) -> Result<()> {
    if let Some(lsn) = before_commit(outcomes)? {
        t.insert(BINLOG_LSN_KEY, lsn.to_le_bytes().as_slice())
            .map_err(|e| Error::Storage(e.to_string()))?;
    }
    Ok(())
}

/// One plain or `INSERT` write for [`Storage::apply_jobs`]: `new` keys must
/// not exist yet (an `INSERT`), `puts` may overwrite, `deletes` go first.
pub struct PlainJob<'a> {
    pub new: Option<&'a [(Vec<u8>, Vec<u8>)]>,
    pub puts: &'a [(Vec<u8>, Vec<u8>)],
    pub deletes: &'a [Vec<u8>],
}

/// One write of any kind for [`Storage::apply_ordered_with`]: validated
/// against `keys` and `ranges` (a transactional commit; both empty for any
/// other write), an `INSERT` when `new` is set, plain otherwise.
pub struct OrderedJob<'a> {
    pub keys: &'a [(Vec<u8>, Option<Vec<u8>>)],
    pub ranges: &'a [RangeSnapshot],
    pub new: Option<&'a [(Vec<u8>, Vec<u8>)]>,
    pub puts: &'a [(Vec<u8>, Vec<u8>)],
    pub deletes: &'a [Vec<u8>],
}

/// Whether every `(key, value)` in `keys` and every range in `ranges` still
/// holds what the transaction read: false means another write changed it.
fn validation_holds(
    t: &redb::Table<'_, &[u8], &[u8]>,
    keys: &[(Vec<u8>, Option<Vec<u8>>)],
    ranges: &[RangeSnapshot],
) -> Result<bool> {
    use std::ops::Bound;
    for (k, exp) in keys {
        let current = t
            .get(k.as_slice())
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|v| v.value().to_vec());
        if &current != exp {
            return Ok(false);
        }
    }
    for r in ranges {
        let upper = match &r.end {
            Some(e) => Bound::Excluded(e.as_slice()),
            None => Bound::Unbounded,
        };
        let mut current = Vec::with_capacity(r.content.len());
        for item in t
            .range::<&[u8]>((Bound::Included(r.start.as_slice()), upper))
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            current.push((k.value().to_vec(), v.value().to_vec()));
        }
        if current != r.content {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether an `INSERT` of `new` collides: a key present after the job's own
/// `deletes`, or one it inserts twice.
fn insert_collides(
    t: &redb::Table<'_, &[u8], &[u8]>,
    new: &[(Vec<u8>, Vec<u8>)],
    deletes: &[Vec<u8>],
) -> Result<bool> {
    let deleted: std::collections::HashSet<&[u8]> = deletes.iter().map(Vec::as_slice).collect();
    let mut seen = std::collections::HashSet::with_capacity(new.len());
    for (k, _) in new {
        let present = !deleted.contains(k.as_slice())
            && t.get(k.as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?
                .is_some();
        if present || !seen.insert(k.as_slice()) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn duplicate_entry() -> Error {
    Error::Duplicate(
        DuplicateError::Entry,
        "Duplicate entry for key 'PRIMARY'".into(),
    )
}

/// Smallest key strictly greater than every key starting with `prefix`
/// (`None` when the prefix is all `0xFF`). Used to bound prefix range scans to
/// a single keyspace instead of the whole database.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut u = prefix.to_vec();
    loop {
        match u.last_mut() {
            Some(b) if *b < 0xff => {
                *b += 1;
                return Some(u);
            }
            Some(_) => {
                u.pop();
            }
            None => return None,
        }
    }
}

/// Handle to an ElyraSQL storage file.
pub struct Storage {
    db: Database,
    /// The data file path (for deriving sibling artifacts like the vector-index
    /// cache). `None` for in-memory/unknown backings.
    path: Option<std::path::PathBuf>,
    /// Per-commit durability for data writes. `Immediate` (default) fsyncs on
    /// every commit; `Eventual` (relaxed "normal" mode) lets commits return
    /// before the fsync, with the group-commit writer forcing a durable flush
    /// on idle and periodically -- trading a bounded crash-loss window for much
    /// higher small-batch write throughput. Never risks corruption: redb keeps
    /// the file consistent and rolls back to the last durable commit on crash.
    durability: Durability,
    /// Monotonic marker so a forced [`flush`](Self::flush) is always a real
    /// (non-empty) `Immediate` commit that persists queued `Eventual` writes.
    flush_seq: std::sync::atomic::AtomicU64,
    /// How many committed writes deleted each id counter (see
    /// [`COUNTER_PREFIXES`]), on any path: see [`Storage::counter_resets`].
    counter_resets: std::sync::Mutex<std::collections::HashMap<Vec<u8>, u64>>,
    /// The last write sequence handed out (see [`WSEQ_KEY`]).
    wseq: std::sync::atomic::AtomicU64,
}

/// Read the configured durability from `ELYRASQL_SYNC` (`full` -> Immediate,
/// the default; `normal`/`relaxed` -> commits without a sync of their own, see
/// [`DEFERRED`]).
fn configured_durability() -> Durability {
    match std::env::var("ELYRASQL_SYNC").ok().as_deref() {
        Some("normal") | Some("relaxed") | Some("eventual") => DEFERRED,
        _ => Durability::Immediate,
    }
}

/// How a commit is made when it is not to be synced itself -- relaxed
/// durability, or a database whose binlog is the durable log -- and the
/// periodic flush (an `Immediate` commit) makes it durable.
///
/// It used to be `Durability::Eventual`, which in redb 2.6 still syncs the
/// file on every commit: a full `fdatasync` on Linux (a cheaper
/// `F_BARRIERFSYNC` on macOS). `ELYRASQL_SYNC=normal` was then no faster than
/// `full` on Linux, and with `--binlog` each group paid two syncs, the
/// binlog's and the data file's. A `Durability::None` commit is not synced;
/// redb keeps the file consistent and, after a crash, at its last durable
/// commit.
const DEFERRED: Durability = Durability::None;

impl Storage {
    /// Open (or create) the single ElyraSQL database file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let pathbuf = path.as_ref().to_path_buf();
        // Sized to the memory available rather than redb's fixed 1 GiB, which
        // left every scan over a larger table re-reading each page from the
        // file (see `cache`).
        let (cache_bytes, source) = page_cache_bytes();
        match source {
            CacheSource::Configured => tracing::info!(
                cache_mb = cache_bytes / (1024 * 1024),
                "page cache set by {PAGE_CACHE_ENV}"
            ),
            CacheSource::Memory { available } => tracing::info!(
                cache_mb = cache_bytes / (1024 * 1024),
                available_mb = available / (1024 * 1024),
                "page cache sized to a quarter of available memory"
            ),
            CacheSource::Fallback => tracing::info!(
                cache_mb = cache_bytes / (1024 * 1024),
                "page cache at the default; available memory could not be read"
            ),
        }
        let db = redb::Builder::new()
            .set_cache_size(usize::try_from(cache_bytes).unwrap_or(usize::MAX))
            .create(&pathbuf)
            .map_err(|e| match e {
                // redb reports this as its own variant, so the distinction survives
                // without anyone matching on the message text.
                redb::DatabaseError::DatabaseAlreadyOpen => {
                    Error::StorageLocked(pathbuf.display().to_string())
                }
                other => Error::Storage(other.to_string()),
            })?;
        // Ensure the KV table exists so first reads don't fail, and pick up
        // the write sequence where the file left it.
        let wtx = db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let wseq = {
            let t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            let v = t.get(WSEQ_KEY).map_err(|e| Error::Storage(e.to_string()))?;
            decode_wseq(v.as_ref().map(|v| v.value()))
        };
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        // A (re)opened database: nothing cached from another applies.
        bump_schema_generation();
        Ok(Self {
            db,
            path: Some(pathbuf),
            durability: configured_durability(),
            flush_seq: std::sync::atomic::AtomicU64::new(0),
            wseq: std::sync::atomic::AtomicU64::new(wseq),
            counter_resets: Default::default(),
        })
    }

    /// The data file path this storage was opened from, if known.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// The next write sequence, to be written at [`WSEQ_KEY`] in the write
    /// transaction being built. Write transactions are serialised, so the
    /// committed values follow the order of the commits.
    fn next_wseq(&self) -> u64 {
        self.wseq.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1
    }

    /// Begin a write transaction at the configured data-write durability.
    fn raw_begin_write(&self) -> Result<WriteTransaction> {
        let mut wtx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
        wtx.set_durability(self.durability);
        Ok(wtx)
    }

    /// True when commits are not synced themselves (see [`DEFERRED`]), so the
    /// group-commit writer knows it must force periodic durable flushes.
    /// Commit data writes without an fsync each, for a database whose binlog
    /// is the durable log (see `Db::open_with_binlog`): the periodic flush
    /// makes the file durable, and records it lacks after a crash are replayed
    /// from the binlog at open.
    pub(crate) fn defer_data_sync(&mut self) {
        self.durability = DEFERRED;
    }

    /// How many committed writes have deleted the id counter at `key` since
    /// this file was opened -- a TRUNCATE, a DROP, a rebuild, however the
    /// write arrived: a statement, or a write applied from a Raft leader, a
    /// primary, a binlog or a snapshot. The engine's in-memory id allocator
    /// for `key` starts again from what is stored when it has moved.
    pub fn counter_resets(&self, key: &[u8]) -> u64 {
        self.counter_resets
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    /// Record that a committed write deleted `keys`, counting the id counters
    /// among them.
    fn note_counter_deletes<'a>(&self, keys: impl Iterator<Item = &'a [u8]>) {
        let mut counters = keys
            .filter(|k| COUNTER_PREFIXES.iter().any(|p| k.starts_with(p)))
            .peekable();
        if counters.peek().is_none() {
            return;
        }
        let mut resets = self.counter_resets.lock().unwrap();
        for k in counters {
            *resets.entry(k.to_vec()).or_insert(0) += 1;
        }
    }

    pub fn is_relaxed_durability(&self) -> bool {
        !matches!(self.durability, Durability::Immediate)
    }

    /// Force everything committed so far to disk with an `Immediate` commit.
    /// Writes a monotonic marker so the commit is never an empty no-op, which
    /// guarantees queued `Eventual` commits are persisted. Used by the writer to
    /// bound the crash-loss window in relaxed mode; a no-op-cost safety net in
    /// the default mode (already durable).
    pub fn flush(&self) -> Result<()> {
        let seq = self
            .flush_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let mut wtx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
        wtx.set_durability(Durability::Immediate);
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            t.insert(b"meta::flush_seq".as_slice(), seq.to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    /// Take an MVCC read snapshot of the current committed state.
    pub fn snapshot(&self) -> Result<Snapshot> {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Snapshot { rtx: Arc::new(rtx) })
    }

    /// Fully in-memory database (tests, ephemeral sessions).
    pub fn in_memory() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let wtx = db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
        {
            wtx.open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        // A (re)opened database: nothing cached from another applies.
        bump_schema_generation();
        Ok(Self {
            db,
            path: None,
            durability: Durability::Immediate,
            flush_seq: std::sync::atomic::AtomicU64::new(0),
            wseq: std::sync::atomic::AtomicU64::new(0),
            counter_resets: Default::default(),
        })
    }

    /// Store a value under a namespaced key.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let wtx = self.raw_begin_write()?;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            t.insert(key, value)
                .map_err(|e| Error::Storage(e.to_string()))?;
            t.insert(WSEQ_KEY, self.next_wseq().to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        if touches_schema(std::iter::once(key)) {
            bump_schema_generation();
        }
        Ok(())
    }

    /// Fetch a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let out = t
            .get(key)
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|v| v.value().to_vec());
        Ok(out)
    }

    /// Fetch many values in a single read transaction. Output aligns with
    /// `keys` (index i -> value for keys[i], `None` if absent). Avoids the
    /// per-call transaction overhead of looping [`Storage::get`].
    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(
                t.get(k.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?
                    .map(|v| v.value().to_vec()),
            );
        }
        Ok(out)
    }

    /// Delete a key. Returns whether something was removed.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let wtx = self.raw_begin_write()?;
        let existed;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            existed = t
                .remove(key)
                .map_err(|e| Error::Storage(e.to_string()))?
                .is_some();
            t.insert(WSEQ_KEY, self.next_wseq().to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.note_counter_deletes(std::iter::once(key));
        if touches_schema(std::iter::once(key)) {
            bump_schema_generation();
        }
        Ok(existed)
    }

    /// Iterate all key/value pairs whose key starts with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for item in t.iter().map_err(|e| Error::Storage(e.to_string()))? {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let key = k.value().to_vec();
            if key.starts_with(prefix) {
                out.push((key, v.value().to_vec()));
            }
        }
        Ok(out)
    }

    /// Cursor-based range scan. Returns up to `limit` key/value pairs whose
    /// key starts with `prefix` and is strictly greater than `after` (when
    /// given). This is the primitive behind streaming table scans: callers
    /// pass the last-seen key back as `after` to fetch the next batch, so
    /// memory stays bounded regardless of table size.
    pub fn scan_batch(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;

        // Start just after the cursor, or at the prefix itself.
        let lower: Vec<u8> = match after {
            Some(a) => {
                let mut k = a.to_vec();
                k.push(0); // smallest key strictly greater than `a`
                k
            }
            None => prefix.to_vec(),
        };

        let mut out = Vec::with_capacity(limit.min(1024));
        let range = t
            .range(lower.as_slice()..)
            .map_err(|e| Error::Storage(e.to_string()))?;
        for item in range {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let key = k.value().to_vec();
            if !key.starts_with(prefix) {
                break; // left the table's keyspace — done
            }
            out.push((key, v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Count the keys under `prefix` without materialising any values -- backs
    /// bare `COUNT(*)` over a table.
    pub fn count_prefix(&self, prefix: &[u8]) -> Result<u64> {
        use std::ops::Bound;
        let upper = prefix_upper_bound(prefix);
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let upper_bound = match &upper {
            Some(u) => Bound::Excluded(u.as_slice()),
            None => Bound::Unbounded,
        };
        let range = t
            .range::<&[u8]>((Bound::Included(prefix), upper_bound))
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut n = 0u64;
        for item in range {
            item.map_err(|e| Error::Storage(e.to_string()))?; // don't touch value
            n += 1;
        }
        Ok(n)
    }

    /// Iterate every key/value whose key starts with `prefix`, in key order,
    /// passing **borrowed** slices to `f` -- no per-row `Vec` allocation and a
    /// single read transaction for the whole scan. This backs streaming
    /// scan/filter/aggregate operators that decode directly from the stored
    /// bytes instead of copying each row out first.
    pub fn scan_prefix_each<F>(&self, prefix: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let range = t
            .range(prefix..)
            .map_err(|e| Error::Storage(e.to_string()))?;
        for item in range {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let key = k.value();
            if !key.starts_with(prefix) {
                break; // left the table's keyspace -- done
            }
            f(key, v.value())?;
        }
        Ok(())
    }

    /// Like [`Storage::scan_prefix_each`] but `f` returns `false` to stop the
    /// scan early (e.g. after collecting a `LIMIT`'s worth of matches). Avoids
    /// reading and copying rows past the ones actually needed.
    pub fn scan_prefix_until<F>(&self, prefix: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let range = t
            .range(prefix..)
            .map_err(|e| Error::Storage(e.to_string()))?;
        for item in range {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let key = k.value();
            if !key.starts_with(prefix) {
                break;
            }
            if !f(key, v.value())? {
                break;
            }
        }
        Ok(())
    }

    /// Like [`Storage::scan_prefix_until`] but visits keys in **descending**
    /// order (largest first). Backs `ORDER BY <pk> DESC LIMIT n`: walk the
    /// clustered keyspace backwards and stop as soon as `n` rows are collected,
    /// instead of scanning and sorting the whole table.
    ///
    /// The first `skip` keys are stepped over without invoking `f` (so their row
    /// value is never decoded), making a deep `OFFSET` cheap. Only pass `skip > 0`
    /// when there is no residual filter.
    pub fn scan_prefix_rev_until<F>(&self, prefix: &[u8], skip: usize, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        use std::ops::Bound;
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        // Bound the range to exactly this prefix's keyspace so `.rev()` starts at
        // the prefix's last key (not the last key of the whole database).
        let upper = prefix_upper_bound(prefix);
        let ub = match &upper {
            Some(u) => Bound::Excluded(u.as_slice()),
            None => Bound::Unbounded,
        };
        let range = t
            .range::<&[u8]>((Bound::Included(prefix), ub))
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut remaining_skip = skip;
        for item in range.rev() {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let key = k.value();
            if !key.starts_with(prefix) {
                break;
            }
            if remaining_skip > 0 {
                remaining_skip -= 1;
                continue;
            }
            if !f(key, v.value())? {
                break;
            }
        }
        Ok(())
    }

    /// Walk secondary-index entries under `index_prefix` in key order (ascending,
    /// or descending when `rev`) and, for each entry, look up the referenced data
    /// row (the entry's value is the data key) in the **same** read transaction,
    /// passing `(data_key, row_bytes)` to `f`. Stops when `f` returns `false`.
    ///
    /// Because the index key is order-preserving, this yields rows already sorted
    /// by the indexed columns — so `ORDER BY <indexed col> LIMIT n` can stop after
    /// `n` matches instead of sorting the whole table. One read transaction keeps
    /// the index walk and the row lookups on a single consistent snapshot.
    ///
    /// The first `skip` entries are stepped over **without** looking up their row
    /// (their data key is not dereferenced), so a deep `OFFSET` costs index steps
    /// rather than `offset` row reads. The caller must only pass `skip > 0` when
    /// there is no residual filter (otherwise skipped rows might not have matched).
    pub fn scan_index_ordered<F>(
        &self,
        index_prefix: &[u8],
        rev: bool,
        skip: usize,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        use std::ops::Bound;
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let upper = prefix_upper_bound(index_prefix);
        let ub = match &upper {
            Some(u) => Bound::Excluded(u.as_slice()),
            None => Bound::Unbounded,
        };
        let range = t
            .range::<&[u8]>((Bound::Included(index_prefix), ub))
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut remaining_skip = skip;
        // Fetch the row referenced by one index entry (entry value = data key)
        // and hand it to `f`. Returns whether to continue.
        let mut visit = |data_key: &[u8]| -> Result<bool> {
            match t.get(data_key).map_err(|e| Error::Storage(e.to_string()))? {
                Some(row) => f(data_key, row.value()),
                // Dangling index entry (row already gone from this snapshot);
                // skip it and keep going.
                None => Ok(true),
            }
        };
        if rev {
            for item in range.rev() {
                let (_k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
                if remaining_skip > 0 {
                    remaining_skip -= 1;
                    continue;
                }
                let data_key = v.value().to_vec();
                if !visit(&data_key)? {
                    break;
                }
            }
        } else {
            for item in range {
                let (_k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
                if remaining_skip > 0 {
                    remaining_skip -= 1;
                    continue;
                }
                let data_key = v.value().to_vec();
                if !visit(&data_key)? {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Like [`Storage::scan_index_ordered`] but walks **two** index prefixes in
    /// sequence within one read transaction — `first` then `second`, each in the
    /// same direction (`rev`) — dereferencing each entry's row. Backs an ordered
    /// walk over a NULL-indexing secondary index, where the NULL-keyed entries
    /// live under a separate prefix: for `ASC` the caller passes the NULL prefix
    /// first (NULLs sort first), for `DESC` the value prefix first (NULLs last).
    /// A single transaction keeps both ranges on one consistent snapshot.
    pub fn scan_two_ordered<F>(
        &self,
        first: &[u8],
        second: &[u8],
        rev: bool,
        skip: usize,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        use std::ops::Bound;
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut remaining_skip = skip;
        let mut stop = false;
        for prefix in [first, second] {
            if stop {
                break;
            }
            let upper = prefix_upper_bound(prefix);
            let ub = match &upper {
                Some(u) => Bound::Excluded(u.as_slice()),
                None => Bound::Unbounded,
            };
            let range = t
                .range::<&[u8]>((Bound::Included(prefix), ub))
                .map_err(|e| Error::Storage(e.to_string()))?;
            macro_rules! handle {
                ($v:expr) => {{
                    if remaining_skip > 0 {
                        remaining_skip -= 1;
                    } else {
                        let dk = $v.value().to_vec();
                        if let Some(row) = t
                            .get(dk.as_slice())
                            .map_err(|e| Error::Storage(e.to_string()))?
                        {
                            if !f(&dk, row.value())? {
                                stop = true;
                                break;
                            }
                        }
                    }
                }};
            }
            if rev {
                for item in range.rev() {
                    let (_k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
                    handle!(v);
                }
            } else {
                for item in range {
                    let (_k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
                    handle!(v);
                }
            }
        }
        Ok(())
    }

    /// Zero-copy iterate `[start, end)` (end exclusive), passing borrowed
    /// slices to `f`. Backs the parallel clustered scan: each worker folds a
    /// disjoint key sub-range in its own read transaction.
    pub fn scan_range_each<F>(&self, start: &[u8], end: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        use std::ops::Bound;
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let range = t
            .range::<&[u8]>((Bound::Included(start), Bound::Excluded(end)))
            .map_err(|e| Error::Storage(e.to_string()))?;
        for item in range {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            f(k.value(), v.value())?;
        }
        Ok(())
    }

    /// The first and last stored keys under `prefix`, in key order (or `None`
    /// if the prefix has no rows). Used to plan a parallel split of a scan.
    pub fn prefix_bounds(&self, prefix: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        use std::ops::Bound;
        // Bound the range to exactly this prefix's keyspace. Without the upper
        // bound, `next_back()` starts at the last key of the *entire* database
        // and walks backwards through every later keyspace (e.g. all `index::`
        // entries and other tables) before reaching this prefix's last row --
        // an O(total DB size) cost paid on every parallel aggregation. With the
        // upper bound it is O(tree height).
        let upper = prefix_upper_bound(prefix);
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let upper_bound = match &upper {
            Some(u) => Bound::Excluded(u.as_slice()),
            None => Bound::Unbounded,
        };
        let mut range = t
            .range::<&[u8]>((Bound::Included(prefix), upper_bound))
            .map_err(|e| Error::Storage(e.to_string()))?;
        let first = match range.next() {
            Some(item) => {
                let (k, _) = item.map_err(|e| Error::Storage(e.to_string()))?;
                k.value().to_vec()
            }
            None => return Ok(None),
        };
        // The range is exactly the prefix keyspace, so next_back() lands on the
        // last row directly.
        let last = match range.next_back() {
            Some(item) => {
                let (k, _) = item.map_err(|e| Error::Storage(e.to_string()))?;
                k.value().to_vec()
            }
            None => first.clone(),
        };
        Ok(Some((first, last)))
    }

    /// Scan keys in `[start, end)` (end exclusive; unbounded when `None`), up
    /// to `limit` pairs. Backs ordered range lookups on the clustered data
    /// keyspace and on secondary indexes.
    pub fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::ops::Bound;
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let lower = Bound::Included(start);
        let upper = match end {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        };
        let mut out = Vec::with_capacity(limit.min(1024));
        for item in t
            .range::<&[u8]>((lower, upper))
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            out.push((k.value().to_vec(), v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Like [`Storage::apply`], but first validate that each key in `expected`
    /// currently holds the given value (its value at the transaction's
    /// Group-commit several validated (transactional) commits in **one** write
    /// transaction (one fsync), instead of one transaction each. Each job is
    /// validated in turn against the current state *including earlier jobs in
    /// this batch*, so first-committer-wins ordering (and write-write conflict
    /// detection) is preserved: a job that read/wrote a key an earlier job in
    /// the batch has since changed conflicts. Returns a per-job result (Ok or
    /// `Error::Conflict`); only non-conflicting jobs are applied.
    ///
    /// This amortises the single writer's fsync across many concurrent
    /// transactions, which is the throughput win under high write concurrency.
    pub fn apply_validated_batch(&self, jobs: &[ValidatedCommit]) -> Result<Vec<Result<()>>> {
        self.apply_validated_batch_with(jobs, &mut |_| Ok(None))
    }

    /// [`Storage::apply_validated_batch`], running `before_commit` just before
    /// the commit (see [`BeforeCommit`]).
    pub fn apply_validated_batch_with(
        &self,
        jobs: &[ValidatedCommit],
        before_commit: BeforeCommit<'_>,
    ) -> Result<Vec<Result<()>>> {
        let wtx = self.raw_begin_write()?;
        let mut results = Vec::with_capacity(jobs.len());
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            for job in jobs {
                if validation_holds(&t, job.keys, job.ranges)? {
                    for k in job.deletes {
                        t.remove(k.as_slice())
                            .map_err(|e| Error::Storage(e.to_string()))?;
                    }
                    for (k, v) in job.puts {
                        put_kv(&mut t, k, v, job.deletes)?;
                    }
                    results.push(Ok(()));
                } else {
                    results.push(Err(Error::Conflict(
                        "row modified by another transaction since snapshot".into(),
                    )));
                }
            }
            t.insert(WSEQ_KEY, self.next_wseq().to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
            let outcomes: Vec<bool> = results.iter().map(Result::is_ok).collect();
            record_binlog_lsn(&mut t, &outcomes, before_commit)?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.note_counter_deletes(
            jobs.iter()
                .flat_map(|j| j.deletes.iter().map(Vec::as_slice)),
        );
        if jobs.iter().any(|j| {
            touches_schema(j.puts.iter().map(|(k, _)| k.as_slice()))
                || touches_schema(j.deletes.iter().map(Vec::as_slice))
        }) {
            bump_schema_generation();
        }
        Ok(results)
    }

    /// snapshot). If any differs, another transaction changed it since the
    /// snapshot: return [`Error::Conflict`] and commit nothing. This is the
    /// write-write conflict check behind snapshot isolation.
    pub fn apply_validated(
        &self,
        keys: &[(Vec<u8>, Option<Vec<u8>>)],
        ranges: &[RangeSnapshot],
        puts: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
    ) -> Result<()> {
        use std::ops::Bound;
        let wtx = self.raw_begin_write()?;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            for (k, exp) in keys {
                let current = t
                    .get(k.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?
                    .map(|v| v.value().to_vec());
                if &current != exp {
                    return Err(Error::Conflict(
                        "row modified by another transaction since snapshot".into(),
                    ));
                }
            }
            for r in ranges {
                let upper = match &r.end {
                    Some(e) => Bound::Excluded(e.as_slice()),
                    None => Bound::Unbounded,
                };
                let mut current = Vec::with_capacity(r.content.len());
                for item in t
                    .range::<&[u8]>((Bound::Included(r.start.as_slice()), upper))
                    .map_err(|e| Error::Storage(e.to_string()))?
                {
                    let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
                    current.push((k.value().to_vec(), v.value().to_vec()));
                }
                if current != r.content {
                    return Err(Error::Conflict(
                        "a scanned range changed since snapshot (phantom or update)".into(),
                    ));
                }
            }
            for k in deletes {
                t.remove(k.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
            for (k, v) in puts {
                put_kv(&mut t, k, v, deletes)?;
            }
            t.insert(WSEQ_KEY, self.next_wseq().to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.note_counter_deletes(deletes.iter().map(Vec::as_slice));
        if touches_schema(puts.iter().map(|(k, _)| k.as_slice()))
            || touches_schema(deletes.iter().map(Vec::as_slice))
        {
            bump_schema_generation();
        }
        Ok(())
    }

    /// Atomically apply many puts and deletes in a single write transaction.
    /// This is the primitive the group-commit writer uses to fold many
    /// pending writes into one commit under high write traffic.
    pub fn apply(&self, puts: &[(Vec<u8>, Vec<u8>)], deletes: &[Vec<u8>]) -> Result<()> {
        let wtx = self.raw_begin_write()?;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            // Deletes first, then puts: a key present in both (e.g. an index
            // entry that is unchanged across an UPDATE) ends up kept.
            for k in deletes {
                t.remove(k.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
            for (k, v) in puts {
                put_kv(&mut t, k, v, deletes)?;
            }
            t.insert(WSEQ_KEY, self.next_wseq().to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.note_counter_deletes(deletes.iter().map(Vec::as_slice));
        if touches_schema(puts.iter().map(|(k, _)| k.as_slice()))
            || touches_schema(deletes.iter().map(Vec::as_slice))
        {
            bump_schema_generation();
        }
        Ok(())
    }

    /// Apply several plain or `INSERT` writes in one write transaction (one
    /// fsync) -- the writer's group commit -- each **in turn**: its deletes,
    /// then its new keys, then its puts, as [`Storage::apply_insert`] and
    /// [`Storage::apply`] do for one. The group used to be merged into a single
    /// write with every delete applied before every put, so a key one job wrote
    /// and a later job in the same group deleted survived. A Raft follower
    /// catching up applies a batch of committed entries this way, so a row
    /// deleted on the leader stayed on the follower.
    ///
    /// A job whose new keys are already present (or repeat within it) is
    /// refused before any of it is written, with the duplicate error
    /// `apply_insert` returns; the other jobs are unaffected. Returns a result
    /// per job, in order.
    pub fn apply_jobs(&self, jobs: &[PlainJob<'_>]) -> Result<Vec<Result<()>>> {
        self.apply_jobs_with(jobs, &mut |_| Ok(None))
    }

    /// [`Storage::apply_jobs`], running `before_commit` just before the commit
    /// (see [`BeforeCommit`]).
    pub fn apply_jobs_with(
        &self,
        jobs: &[PlainJob<'_>],
        before_commit: BeforeCommit<'_>,
    ) -> Result<Vec<Result<()>>> {
        let wtx = self.raw_begin_write()?;
        let mut results = Vec::with_capacity(jobs.len());
        let mut schema = false;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            for job in jobs {
                if let Some(new) = job.new {
                    // What `apply_insert` would find.
                    if insert_collides(&t, new, job.deletes)? {
                        results.push(Err(duplicate_entry()));
                        continue;
                    }
                }
                for k in job.deletes {
                    t.remove(k.as_slice())
                        .map_err(|e| Error::Storage(e.to_string()))?;
                }
                for (k, v) in job.new.unwrap_or_default().iter().chain(job.puts) {
                    put_kv(&mut t, k, v, job.deletes)?;
                }
                schema |= touches_schema(
                    job.new
                        .unwrap_or_default()
                        .iter()
                        .chain(job.puts)
                        .map(|(k, _)| k.as_slice()),
                ) || touches_schema(job.deletes.iter().map(Vec::as_slice));
                results.push(Ok(()));
            }
            t.insert(WSEQ_KEY, self.next_wseq().to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
            let outcomes: Vec<bool> = results.iter().map(Result::is_ok).collect();
            record_binlog_lsn(&mut t, &outcomes, before_commit)?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.note_counter_deletes(
            jobs.iter()
                .flat_map(|j| j.deletes.iter().map(Vec::as_slice)),
        );
        if schema {
            bump_schema_generation();
        }
        Ok(results)
    }

    /// Apply writes of any kind in **one** write transaction, each in turn
    /// against the state the earlier ones left, then `meta` -- puts written
    /// whatever the jobs' outcomes. A job that fails validation or inserts a
    /// key that exists is refused (a result per job, in order) and leaves no
    /// trace; any other error commits nothing at all.
    ///
    /// This is how a Raft node applies committed entries: the entries and the
    /// index of the last one (in `meta`) are stored together or not at all.
    /// Applied as separate writes, one group could fail while a later one was
    /// committed, so entries landed out of order and the stored index could
    /// pass an entry that was never applied.
    pub fn apply_ordered_with(
        &self,
        jobs: &[OrderedJob<'_>],
        meta: &[(Vec<u8>, Vec<u8>)],
        before_commit: BeforeCommit<'_>,
    ) -> Result<Vec<Result<()>>> {
        let wtx = self.raw_begin_write()?;
        let mut results = Vec::with_capacity(jobs.len());
        let mut schema = false;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            for job in jobs {
                if !validation_holds(&t, job.keys, job.ranges)? {
                    results.push(Err(Error::Conflict(
                        "row modified by another transaction since snapshot".into(),
                    )));
                    continue;
                }
                if let Some(new) = job.new {
                    if insert_collides(&t, new, job.deletes)? {
                        results.push(Err(duplicate_entry()));
                        continue;
                    }
                }
                for k in job.deletes {
                    t.remove(k.as_slice())
                        .map_err(|e| Error::Storage(e.to_string()))?;
                }
                for (k, v) in job.new.unwrap_or_default().iter().chain(job.puts) {
                    put_kv(&mut t, k, v, job.deletes)?;
                }
                schema |= touches_schema(
                    job.new
                        .unwrap_or_default()
                        .iter()
                        .chain(job.puts)
                        .map(|(k, _)| k.as_slice()),
                ) || touches_schema(job.deletes.iter().map(Vec::as_slice));
                results.push(Ok(()));
            }
            for (k, v) in meta {
                t.insert(k.as_slice(), v.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
            t.insert(WSEQ_KEY, self.next_wseq().to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
            let outcomes: Vec<bool> = results.iter().map(Result::is_ok).collect();
            record_binlog_lsn(&mut t, &outcomes, before_commit)?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.note_counter_deletes(
            jobs.iter()
                .flat_map(|j| j.deletes.iter().map(Vec::as_slice)),
        );
        if schema {
            bump_schema_generation();
        }
        Ok(results)
    }

    /// Insert `new` keys that must not already exist (plain `INSERT`), plus
    /// `aux` puts (index entries, counters) that may overwrite. If any `new`
    /// key is already present the whole transaction is aborted and a duplicate
    /// error is returned — detecting duplicates in the write transaction itself,
    /// with no separate read.
    pub fn apply_insert(
        &self,
        new: &[(Vec<u8>, Vec<u8>)],
        aux: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
    ) -> Result<()> {
        let wtx = self.raw_begin_write()?;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            for k in deletes {
                t.remove(k.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
            for (k, v) in new {
                let existed = t
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?
                    .is_some();
                if existed {
                    return Err(Error::Duplicate(
                        DuplicateError::Entry,
                        "Duplicate entry for key 'PRIMARY'".into(),
                    ));
                }
            }
            for (k, v) in aux {
                put_kv(&mut t, k, v, deletes)?;
            }
            t.insert(WSEQ_KEY, self.next_wseq().to_le_bytes().as_slice())
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.note_counter_deletes(deletes.iter().map(Vec::as_slice));
        if touches_schema(new.iter().chain(aux).map(|(k, _)| k.as_slice()))
            || touches_schema(deletes.iter().map(Vec::as_slice))
        {
            bump_schema_generation();
        }
        Ok(())
    }

    /// The current committed write sequence (see [`WSEQ_KEY`]). `0` if unset.
    pub fn write_epoch(&self) -> Result<u64> {
        Ok(decode_wseq(self.get(WSEQ_KEY)?.as_deref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_kv(k: &str, v: &str) -> (Vec<u8>, Vec<u8>) {
        (k.as_bytes().to_vec(), v.as_bytes().to_vec())
    }

    fn plain<'a>(puts: &'a [(Vec<u8>, Vec<u8>)], deletes: &'a [Vec<u8>]) -> OrderedJob<'a> {
        OrderedJob {
            keys: &[],
            ranges: &[],
            new: None,
            puts,
            deletes,
        }
    }

    /// Raft entries are applied this way: in order, a refusal per entry, the
    /// applied index stored whatever the outcomes, and nothing at all when the
    /// transaction fails.
    #[test]
    fn ordered_jobs_apply_in_turn_in_one_transaction() {
        let s = Storage::in_memory().unwrap();
        let row = text_kv("data::t::1", "a");
        let insert_new = [row.clone()];
        let del = [b"data::t::1".to_vec()];
        let stale = [(b"data::t::1".to_vec(), Some(b"a".to_vec()))];
        let z = [text_kv("data::t::z", "1")];
        let jobs = [
            // An INSERT, then a DELETE of the same row.
            OrderedJob {
                keys: &[],
                ranges: &[],
                new: Some(&insert_new),
                puts: &[],
                deletes: &[],
            },
            plain(&[], &del),
            // Validated against the row the DELETE removed: refused.
            OrderedJob {
                keys: &stale,
                ranges: &[],
                new: None,
                puts: &z,
                deletes: &[],
            },
        ];
        let meta = [text_kv("meta::raft::applied", "3")];
        let results = s
            .apply_ordered_with(&jobs, &meta, &mut |_| Ok(None))
            .unwrap();
        assert!(results[0].is_ok() && results[1].is_ok());
        assert!(matches!(results[2], Err(Error::Conflict(_))));
        assert_eq!(s.get(b"data::t::1").unwrap(), None);
        assert_eq!(s.get(b"data::t::z").unwrap(), None);
        // Stored although the last job was refused.
        assert_eq!(s.get(b"meta::raft::applied").unwrap(), Some(b"3".to_vec()));

        // An INSERT of a key that is there is refused, and only that job.
        let a = [text_kv("data::t::a", "x")];
        let b = [text_kv("data::t::b", "x")];
        let jobs = [
            plain(&a, &[]),
            OrderedJob {
                keys: &[],
                ranges: &[],
                new: Some(&a),
                puts: &b,
                deletes: &[],
            },
        ];
        let results = s.apply_ordered_with(&jobs, &[], &mut |_| Ok(None)).unwrap();
        assert!(results[0].is_ok());
        assert!(matches!(results[1], Err(Error::Duplicate(..))));
        assert_eq!(s.get(b"data::t::b").unwrap(), None);

        // A failure before the commit leaves no trace of any job or of `meta`.
        let c = [text_kv("data::t::c", "x")];
        let meta = [text_kv("meta::raft::applied", "9")];
        let failed = s.apply_ordered_with(&[plain(&c, &[])], &meta, &mut |_| {
            Err(Error::Storage("disk gone".into()))
        });
        assert!(failed.is_err());
        assert_eq!(s.get(b"data::t::c").unwrap(), None);
        assert_eq!(s.get(b"meta::raft::applied").unwrap(), Some(b"3".to_vec()));
    }

    /// Every write path that commits a schema key moves the generation on,
    /// after the commit. (Other tests run concurrently and may bump it too, so
    /// only "moved on" is asserted here; which keys count is tested below.)
    #[test]
    fn schema_writes_on_every_path_bump_the_generation() {
        let s = Storage::in_memory().unwrap();
        let k = b"sys::ugrant::alice".to_vec();
        let v = 7u32.to_le_bytes().to_vec();
        let paths: [&dyn Fn(); 6] = [
            &|| s.put(&k, &v).unwrap(),
            &|| {
                s.delete(&k).unwrap();
            },
            &|| s.apply(&[(k.clone(), v.clone())], &[]).unwrap(),
            &|| s.apply(&[], std::slice::from_ref(&k)).unwrap(),
            &|| s.apply_insert(&[], &[(k.clone(), v.clone())], &[]).unwrap(),
            &|| {
                s.apply_validated(&[], &[], &[], std::slice::from_ref(&k))
                    .unwrap()
            },
        ];
        for (i, write) in paths.iter().enumerate() {
            let before = schema_generation();
            write();
            assert!(schema_generation() > before, "path {i} did not bump");
        }
        let before = schema_generation();
        let job = ValidatedCommit {
            keys: &[],
            ranges: &[],
            puts: &[(k.clone(), v.clone())],
            deletes: &[],
        };
        s.apply_validated_batch(&[job]).unwrap();
        assert!(schema_generation() > before, "validated batch did not bump");
    }

    /// The write sequence continues from the file's value when it is opened
    /// again, one step per commit, on every write path.
    #[test]
    fn the_write_sequence_resumes_where_the_file_left_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wseq.edb");
        let kv = (b"data::t::1".to_vec(), b"x".to_vec());
        let before = {
            let s = Storage::open(&path).unwrap();
            s.apply(std::slice::from_ref(&kv), &[]).unwrap();
            s.apply(std::slice::from_ref(&kv), &[]).unwrap();
            s.write_epoch().unwrap()
        };
        assert!(before >= 2);
        let s = Storage::open(&path).unwrap();
        assert_eq!(s.write_epoch().unwrap(), before);
        s.put(&kv.0, &kv.1).unwrap();
        assert_eq!(s.write_epoch().unwrap(), before + 1);
        s.delete(&kv.0).unwrap();
        assert_eq!(s.write_epoch().unwrap(), before + 2);
        s.apply_insert(std::slice::from_ref(&kv), &[], &[]).unwrap();
        assert_eq!(s.write_epoch().unwrap(), before + 3);
    }

    #[test]
    fn only_schema_keys_count() {
        let data: [&[u8]; 4] = [
            b"data::t::1",
            b"meta::wseq",
            b"meta::wcount::t",
            b"stats::t",
        ];
        assert!(!touches_schema(data.iter().copied()));
        for key in [
            &b"sys::user::bob"[..],
            b"catalog::t",
            b"view::v",
            b"matview::m",
            b"coldecl::t",
            b"colwidth::t",
            b"meta::generation::t",
        ] {
            assert!(
                touches_schema([&b"data::t::1"[..], key].into_iter()),
                "{key:?}"
            );
        }
    }

    fn kv(k: &[u8], v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (k.to_vec(), v.to_vec())
    }

    /// Jobs in one group apply in order. Merged, every delete ran before every
    /// put, so a key written by one job and deleted by the next survived.
    #[test]
    fn grouped_jobs_apply_in_order() {
        let s = Storage::in_memory().unwrap();
        let put_k = [kv(b"data::t::k", b"1")];
        let del_k = [b"data::t::k".to_vec()];
        let none: [(Vec<u8>, Vec<u8>); 0] = [];
        let no_del: [Vec<u8>; 0] = [];
        // INSERT k, then DELETE k.
        let r = s
            .apply_jobs(&[
                PlainJob {
                    new: Some(&put_k),
                    puts: &none,
                    deletes: &no_del,
                },
                PlainJob {
                    new: None,
                    puts: &none,
                    deletes: &del_k,
                },
            ])
            .unwrap();
        assert!(r.iter().all(|r| r.is_ok()));
        assert_eq!(s.get(b"data::t::k").unwrap(), None, "deleted key survived");
        // DELETE k, then put k: present.
        s.apply_jobs(&[
            PlainJob {
                new: None,
                puts: &none,
                deletes: &del_k,
            },
            PlainJob {
                new: None,
                puts: &put_k,
                deletes: &no_del,
            },
        ])
        .unwrap();
        assert_eq!(s.get(b"data::t::k").unwrap().as_deref(), Some(&b"1"[..]));
    }

    /// A duplicate fails its own job, before any of it is written; the others
    /// apply. A job may delete a key and insert it again (as apply_insert).
    #[test]
    fn a_duplicate_fails_only_its_own_job() {
        let s = Storage::in_memory().unwrap();
        s.put(b"data::t::1", b"old").unwrap();
        let none: [(Vec<u8>, Vec<u8>); 0] = [];
        let no_del: [Vec<u8>; 0] = [];
        let dup = [kv(b"data::t::2", b"x"), kv(b"data::t::1", b"y")];
        let dup_aux = [kv(b"idx::t::a", b"aux")];
        let fine = [kv(b"data::t::3", b"z")];
        let twice = [kv(b"data::t::4", b"a"), kv(b"data::t::4", b"b")];
        let reinsert = [kv(b"data::t::1", b"new")];
        let del_1 = [b"data::t::1".to_vec()];
        let r = s
            .apply_jobs(&[
                PlainJob {
                    new: Some(&dup),
                    puts: &dup_aux,
                    deletes: &no_del,
                },
                PlainJob {
                    new: Some(&fine),
                    puts: &none,
                    deletes: &no_del,
                },
                PlainJob {
                    new: Some(&twice),
                    puts: &none,
                    deletes: &no_del,
                },
                PlainJob {
                    new: Some(&reinsert),
                    puts: &none,
                    deletes: &del_1,
                },
            ])
            .unwrap();
        assert!(matches!(r[0], Err(Error::Duplicate(..))));
        assert!(r[1].is_ok());
        assert!(matches!(r[2], Err(Error::Duplicate(..))));
        assert!(r[3].is_ok());
        // Nothing of the refused jobs was written.
        assert_eq!(s.get(b"data::t::2").unwrap(), None);
        assert_eq!(s.get(b"idx::t::a").unwrap(), None);
        assert_eq!(s.get(b"data::t::4").unwrap(), None);
        assert_eq!(s.get(b"data::t::3").unwrap().as_deref(), Some(&b"z"[..]));
        assert_eq!(s.get(b"data::t::1").unwrap().as_deref(), Some(&b"new"[..]));
    }

    #[test]
    fn put_get_delete_roundtrip() {
        let s = Storage::in_memory().unwrap();
        s.put(b"data::t::1", b"hello").unwrap();
        assert_eq!(
            s.get(b"data::t::1").unwrap().as_deref(),
            Some(&b"hello"[..])
        );
        assert!(s.delete(b"data::t::1").unwrap());
        assert_eq!(s.get(b"data::t::1").unwrap(), None);
    }

    #[test]
    fn prefix_scan_isolates_namespaces() {
        let s = Storage::in_memory().unwrap();
        s.put(b"data::t::1", b"a").unwrap();
        s.put(b"data::t::2", b"b").unwrap();
        s.put(b"catalog::t", b"schema").unwrap();
        assert_eq!(s.scan_prefix(b"data::t::").unwrap().len(), 2);
        assert_eq!(s.scan_prefix(b"catalog::").unwrap().len(), 1);
    }
}
