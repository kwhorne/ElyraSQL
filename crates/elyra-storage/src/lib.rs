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
pub use db::{Consensus, Db, Validation, WriteEvent, WriteOp};

use std::path::Path;
use std::sync::Arc;

use elyra_core::{Error, Result};
use redb::{Database, ReadTransaction, ReadableTable, TableDefinition};

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
}

/// Key/value blob table. All logical namespaces share this definition; the
/// key carries the namespace prefix so we keep a single flat keyspace and
/// therefore a single file.
const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("elyra_kv");

/// Handle to an ElyraSQL storage file.
pub struct Storage {
    db: Database,
}

impl Storage {
    /// Open (or create) the single ElyraSQL database file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path).map_err(|e| Error::Storage(e.to_string()))?;
        // Ensure the KV table exists so first reads don't fail.
        let wtx = db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
        {
            wtx.open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { db })
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
        Ok(Self { db })
    }

    /// Store a value under a namespaced key.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let wtx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            t.insert(key, value)
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
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
        let wtx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let existed;
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            existed = t
                .remove(key)
                .map_err(|e| Error::Storage(e.to_string()))?
                .is_some();
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
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
        let rtx = self
            .db
            .begin_read()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx
            .open_table(KV)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut range = t
            .range::<&[u8]>((Bound::Included(prefix), Bound::Unbounded))
            .map_err(|e| Error::Storage(e.to_string()))?;
        let first = match range.next() {
            Some(item) => {
                let (k, _) = item.map_err(|e| Error::Storage(e.to_string()))?;
                let kv = k.value();
                if !kv.starts_with(prefix) {
                    return Ok(None);
                }
                kv.to_vec()
            }
            None => return Ok(None),
        };
        // Walk from the back, skipping any keys that belong to a later prefix,
        // until the last key still under `prefix`.
        let mut last = first.clone();
        while let Some(item) = range.next_back() {
            let (k, _) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let kv = k.value();
            if kv.starts_with(prefix) {
                last = kv.to_vec();
                break;
            }
        }
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
        use std::ops::Bound;
        let wtx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
        let mut results = Vec::with_capacity(jobs.len());
        {
            let mut t = wtx
                .open_table(KV)
                .map_err(|e| Error::Storage(e.to_string()))?;
            for job in jobs {
                let mut ok = true;
                for (k, exp) in job.keys {
                    let current = t
                        .get(k.as_slice())
                        .map_err(|e| Error::Storage(e.to_string()))?
                        .map(|v| v.value().to_vec());
                    if &current != exp {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    for r in job.ranges {
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
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    for k in job.deletes {
                        t.remove(k.as_slice())
                            .map_err(|e| Error::Storage(e.to_string()))?;
                    }
                    for (k, v) in job.puts {
                        t.insert(k.as_slice(), v.as_slice())
                            .map_err(|e| Error::Storage(e.to_string()))?;
                    }
                    results.push(Ok(()));
                } else {
                    results.push(Err(Error::Conflict(
                        "row modified by another transaction since snapshot".into(),
                    )));
                }
            }
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
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
        let wtx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
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
                t.insert(k.as_slice(), v.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    /// Atomically apply many puts and deletes in a single write transaction.
    /// This is the primitive the group-commit writer uses to fold many
    /// pending writes into one commit under high write traffic.
    pub fn apply(&self, puts: &[(Vec<u8>, Vec<u8>)], deletes: &[Vec<u8>]) -> Result<()> {
        let wtx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
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
                t.insert(k.as_slice(), v.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
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
        let wtx = self
            .db
            .begin_write()
            .map_err(|e| Error::Storage(e.to_string()))?;
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
                    return Err(Error::Duplicate("Duplicate entry for key 'PRIMARY'".into()));
                }
            }
            for (k, v) in aux {
                t.insert(k.as_slice(), v.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
