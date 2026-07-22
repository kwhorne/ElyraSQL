//! Per-connection session with snapshot-isolated transactions.
//!
//! A `Session` is the data-access authority the executor uses instead of the
//! raw [`Db`]. In autocommit mode it reads the latest committed state and
//! writes immediately. Inside a transaction (`BEGIN`) it reads from an MVCC
//! [`Snapshot`] taken at `BEGIN` (so reads are repeatable and never see other
//! transactions' concurrent commits) overlaid with the transaction's own
//! buffered writes (read-your-writes). Buffered writes are invisible to other
//! connections until `COMMIT`, which applies them atomically; `ROLLBACK`
//! discards them. This provides snapshot isolation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use std::sync::Arc;

use elyra_core::{Error, Result};
use elyra_storage::{Db, RangeSnapshot, Snapshot, Validation};

use crate::lockmgr::{LockGuard, LockManager, LockMode};

/// Transaction isolation level.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// Snapshot reads + first-committer-wins write-conflict detection.
    Snapshot,
    /// Also validates the read set and scanned ranges at commit (prevents
    /// write skew and phantoms) at the cost of more aborts.
    Serializable,
}

struct TxnState {
    snapshot: Snapshot,
    puts: BTreeMap<Vec<u8>, Vec<u8>>,
    deletes: BTreeSet<Vec<u8>>,
    /// Serializable bookkeeping (unused under snapshot isolation).
    serializable: bool,
    reads: BTreeSet<Vec<u8>>,
    ranges: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    /// Rows explicitly locked with SELECT ... FOR UPDATE / FOR SHARE. Always
    /// validated at commit, so a concurrent change aborts this transaction.
    locked: BTreeSet<Vec<u8>>,
    /// Named savepoints (markers into `undo`/`ranges`), innermost last.
    savepoints: Vec<Savepoint>,
    /// Reversible log of buffered-write mutations, recorded only while at least
    /// one savepoint is active. Lets `ROLLBACK TO` revert in
    /// O(changes-since-savepoint) instead of cloning the whole staged write set
    /// per savepoint (which was O(writes x savepoints)).
    undo: Vec<UndoEntry>,
    /// Approximate bytes buffered by `puts` + `deletes`, maintained
    /// incrementally, to bound in-transaction memory (see `txn_max_bytes`).
    mem: usize,
}

/// A savepoint marker: positions into the undo log and range list rather than a
/// full copy of the staged transaction state.
struct Savepoint {
    name: String,
    undo_mark: usize,
    ranges_len: usize,
}

/// One reversible mutation to the buffered write set for a single key: the
/// state of that key (in `puts` / `deletes`) *before* the mutation.
struct UndoEntry {
    key: Vec<u8>,
    prev_put: Option<Vec<u8>>,
    prev_deleted: bool,
}

pub struct Session {
    db: Db,
    txn: Mutex<Option<TxnState>>,
    isolation: Mutex<Isolation>,
    /// Nested `CALL` depth (guards against runaway procedure recursion).
    call_depth: std::sync::atomic::AtomicUsize,
    /// Ready-to-run trigger-body SQL queued by the last DML, fired by the engine.
    pending_triggers: Mutex<Vec<String>>,
    /// Session user variables (`@name`).
    user_vars: Mutex<std::collections::HashMap<String, elyra_core::Value>>,
    /// Shared pessimistic table-lock manager.
    locks: Arc<LockManager>,
    /// Explicit `LOCK TABLES` guards held until `UNLOCK TABLES` or disconnect.
    held_locks: Mutex<Vec<LockGuard>>,
    /// `LAST_INSERT_ID()` -- first auto-generated id of the last INSERT.
    last_insert_id: std::sync::atomic::AtomicI64,
    /// `ROW_COUNT()` -- rows changed by the last DML (-1 after a SELECT/DDL).
    row_count: std::sync::atomic::AtomicI64,
}

fn is_meta(k: &[u8]) -> bool {
    k.starts_with(b"meta::")
}

/// Upper bound on bytes buffered by an uncommitted transaction before writes
/// are rejected (default 1 GiB), preventing a single runaway transaction from
/// exhausting server memory. Override with `ELYRASQL_TXN_MAX_BYTES`.
fn txn_max_bytes() -> usize {
    std::env::var("ELYRASQL_TXN_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1usize << 30)
}

/// Max rows in a single scanned range that a SERIALIZABLE commit will
/// materialize for phantom validation (`ELYRASQL_SERIALIZABLE_MAX_RANGE`,
/// default 5,000,000). A larger range aborts the commit rather than risking OOM.
fn serializable_max_range() -> usize {
    std::env::var("ELYRASQL_SERIALIZABLE_MAX_RANGE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(5_000_000)
}

fn txn_overflow(budget: usize) -> Error {
    Error::Query(format!(
        "transaction write buffer exceeded {budget} bytes; COMMIT or ROLLBACK \
         (raise ELYRASQL_TXN_MAX_BYTES to allow larger transactions)"
    ))
}

impl Session {
    pub fn new(db: Db, locks: Arc<LockManager>) -> Self {
        Session {
            db,
            txn: Mutex::new(None),
            isolation: Mutex::new(Isolation::Snapshot),
            call_depth: std::sync::atomic::AtomicUsize::new(0),
            pending_triggers: Mutex::new(Vec::new()),
            user_vars: Mutex::new(std::collections::HashMap::new()),
            locks,
            held_locks: Mutex::new(Vec::new()),
            last_insert_id: std::sync::atomic::AtomicI64::new(0),
            row_count: std::sync::atomic::AtomicI64::new(-1),
        }
    }

    /// Value returned by `LAST_INSERT_ID()`.
    pub fn last_insert_id(&self) -> i64 {
        self.last_insert_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Record the first auto-generated id of an INSERT (0 = none this statement,
    /// which leaves the previous value visible, matching MySQL).
    pub fn set_last_insert_id(&self, id: i64) {
        if id != 0 {
            self.last_insert_id
                .store(id, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Value returned by `ROW_COUNT()`.
    pub fn row_count(&self) -> i64 {
        self.row_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_row_count(&self, n: i64) {
        self.row_count
            .store(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// The shared lock manager (engine-wide).
    pub fn lock_manager(&self) -> &Arc<LockManager> {
        &self.locks
    }

    /// Whether this session already holds an explicit lock on `table`.
    pub fn holds_lock(&self, table: &str) -> bool {
        self.held_locks
            .lock()
            .unwrap()
            .iter()
            .any(|g| g.table().eq_ignore_ascii_case(table))
    }

    /// Acquire an explicit `LOCK TABLES` lock, held until `UNLOCK TABLES` or the
    /// session ends.
    pub async fn lock_table(&self, table: &str, mode: LockMode) -> Result<()> {
        // Re-locking a table the session already holds is a no-op upgrade-free.
        if self.holds_lock(table) {
            return Ok(());
        }
        let guard = self
            .locks
            .acquire(table, mode, true, std::time::Duration::from_secs(10))
            .await?;
        self.held_locks.lock().unwrap().push(guard);
        Ok(())
    }

    /// Release all explicit locks held by this session (`UNLOCK TABLES`).
    pub fn unlock_tables(&self) {
        self.held_locks.lock().unwrap().clear();
    }

    /// Set a session user variable (`@name`).
    pub fn set_user_var(&self, name: &str, value: elyra_core::Value) {
        self.user_vars
            .lock()
            .unwrap()
            .insert(name.to_ascii_lowercase(), value);
    }

    /// Get a session user variable (NULL if unset).
    pub fn user_var(&self, name: &str) -> elyra_core::Value {
        self.user_vars
            .lock()
            .unwrap()
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or(elyra_core::Value::Null)
    }

    /// Snapshot of all user variables (for substitution).
    pub fn user_vars_snapshot(&self) -> std::collections::HashMap<String, elyra_core::Value> {
        self.user_vars.lock().unwrap().clone()
    }

    /// Queue a trigger body (already rendered to concrete SQL) to run after the
    /// current DML statement.
    pub fn queue_trigger(&self, sql: String) {
        self.pending_triggers.lock().unwrap().push(sql);
    }

    /// Take and clear the queued trigger bodies.
    pub fn take_triggers(&self) -> Vec<String> {
        std::mem::take(&mut *self.pending_triggers.lock().unwrap())
    }

    /// Enter a `CALL`; errors if procedure recursion is too deep.
    pub fn enter_call(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        const MAX_CALL_DEPTH: usize = 32;
        let d = self.call_depth.fetch_add(1, Ordering::SeqCst);
        if d >= MAX_CALL_DEPTH {
            self.call_depth.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::Query("trigger/procedure recursion too deep".into()));
        }
        Ok(())
    }

    /// Leave a `CALL`.
    pub fn leave_call(&self) {
        self.call_depth
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn set_isolation(&self, level: Isolation) {
        *self.isolation.lock().unwrap() = level;
    }

    pub fn in_txn(&self) -> bool {
        self.txn.lock().unwrap().is_some()
    }

    /// The underlying committed-state handle (used for streaming scans in
    /// autocommit mode only).
    /// The stable, process-unique id of the underlying database (for keying
    /// process-global caches by database).
    pub fn db_id(&self) -> u64 {
        self.db.id()
    }

    pub fn raw_db(&self) -> Db {
        self.db.clone()
    }

    // --- transaction control ---

    pub fn begin(&self) -> Result<()> {
        let snapshot = self.db.snapshot()?;
        let serializable = *self.isolation.lock().unwrap() == Isolation::Serializable;
        *self.txn.lock().unwrap() = Some(TxnState {
            snapshot,
            puts: BTreeMap::new(),
            deletes: BTreeSet::new(),
            serializable,
            reads: BTreeSet::new(),
            ranges: Vec::new(),
            locked: BTreeSet::new(),
            savepoints: Vec::new(),
            undo: Vec::new(),
            mem: 0,
        });
        Ok(())
    }

    /// Record rows locked by SELECT ... FOR UPDATE (validated at commit). A
    /// no-op outside a transaction.
    pub fn lock_keys(&self, keys: &[Vec<u8>]) {
        if let Some(tx) = self.txn.lock().unwrap().as_mut() {
            tx.locked.extend(keys.iter().cloned());
        }
    }

    /// Establish (or redefine) a savepoint within the current transaction.
    pub fn savepoint(&self, name: &str) -> Result<()> {
        let mut g = self.txn.lock().unwrap();
        let tx = g
            .as_mut()
            .ok_or_else(|| Error::Query("SAVEPOINT outside a transaction".into()))?;
        tx.savepoints.retain(|s| s.name != name);
        tx.savepoints.push(Savepoint {
            name: name.to_string(),
            undo_mark: tx.undo.len(),
            ranges_len: tx.ranges.len(),
        });
        Ok(())
    }

    /// Roll the transaction's buffered state back to a savepoint (which
    /// remains); savepoints established after it are discarded.
    pub fn rollback_to(&self, name: &str) -> Result<()> {
        let mut g = self.txn.lock().unwrap();
        let tx = g
            .as_mut()
            .ok_or_else(|| Error::Query("ROLLBACK TO SAVEPOINT outside a transaction".into()))?;
        let pos = tx
            .savepoints
            .iter()
            .position(|s| s.name == name)
            .ok_or_else(|| Error::Query(format!("no such savepoint: {name}")))?;
        let mark = tx.savepoints[pos].undo_mark;
        let ranges_len = tx.savepoints[pos].ranges_len;
        // Revert buffered-write mutations back to the savepoint, newest first;
        // each entry restores one key's prior put/delete state.
        while tx.undo.len() > mark {
            let UndoEntry {
                key,
                prev_put,
                prev_deleted,
            } = tx.undo.pop().unwrap();
            if let Some(v) = tx.puts.get(&key) {
                tx.mem -= key.len() + v.len();
            }
            if tx.deletes.contains(&key) {
                tx.mem -= key.len();
            }
            match prev_put {
                Some(v) => {
                    tx.mem += key.len() + v.len();
                    tx.puts.insert(key.clone(), v);
                }
                None => {
                    tx.puts.remove(&key);
                }
            }
            if prev_deleted {
                tx.mem += key.len();
                tx.deletes.insert(key);
            } else {
                tx.deletes.remove(&key);
            }
        }
        // Ranges are append-only, so truncation restores them exactly. `reads`
        // and `locked` are intentionally kept: they only make commit-time
        // conflict validation more conservative (never incorrect), and reverting
        // them would reintroduce the expensive per-savepoint set clones.
        tx.ranges.truncate(ranges_len);
        tx.savepoints.truncate(pos + 1);
        Ok(())
    }

    /// Release (forget) a savepoint and any established after it, without
    /// rolling back.
    pub fn release_savepoint(&self, name: &str) -> Result<()> {
        let mut g = self.txn.lock().unwrap();
        let tx = g
            .as_mut()
            .ok_or_else(|| Error::Query("RELEASE SAVEPOINT outside a transaction".into()))?;
        let pos = tx
            .savepoints
            .iter()
            .position(|s| s.name == name)
            .ok_or_else(|| Error::Query(format!("no such savepoint: {name}")))?;
        tx.savepoints.truncate(pos);
        // With no savepoints left, the undo log is no longer needed.
        if tx.savepoints.is_empty() {
            tx.undo = Vec::new();
        }
        Ok(())
    }

    pub async fn commit(&self) -> Result<()> {
        let staged = self.txn.lock().unwrap().take();
        let Some(tx) = staged else { return Ok(()) };
        let TxnState {
            snapshot,
            puts,
            deletes,
            serializable,
            reads,
            ranges,
            locked,
            savepoints: _,
            undo: _,
            mem: _,
        } = tx;

        // Keys to validate = written keys, plus (serializable) read keys.
        // Per-table monotonic counters (`meta::…`) are excluded: they are bumped
        // by every write and would cause false conflicts between transactions on
        // the same table; real row collisions are still caught via data keys.
        let mut keyset: BTreeSet<Vec<u8>> = BTreeSet::new();
        keyset.extend(puts.keys().filter(|k| !is_meta(k)).cloned());
        keyset.extend(deletes.iter().filter(|k| !is_meta(k)).cloned());
        keyset.extend(locked.iter().filter(|k| !is_meta(k)).cloned());
        if serializable {
            keyset.extend(reads.iter().filter(|k| !is_meta(k)).cloned());
        }
        let keys: Vec<Vec<u8>> = keyset.into_iter().collect();
        let snap = snapshot.clone();
        let kq = keys.clone();
        let snap_vals = spawn(move || snap.multi_get(&kq)).await?;
        let expected: Vec<(Vec<u8>, Option<Vec<u8>>)> = keys.into_iter().zip(snap_vals).collect();

        // Serializable: snapshot content of each scanned range, validated at
        // commit to detect phantoms / concurrent range changes.
        let mut range_snaps: Vec<RangeSnapshot> = Vec::new();
        if serializable {
            // SERIALIZABLE validates every scanned range by re-reading it at
            // commit, so the read set is materialized. Bound that memory: refuse
            // (fail-safe, never silently miss a phantom) a range larger than
            // `ELYRASQL_SERIALIZABLE_MAX_RANGE` rather than risk OOM.
            let cap = serializable_max_range();
            for (start, end) in ranges {
                let snap = snapshot.clone();
                let (s, e) = (start.clone(), end.clone());
                let limit = cap.saturating_add(1);
                let content = spawn(move || snap.scan_range(&s, e.as_deref(), limit)).await?;
                if content.len() > cap {
                    // Transaction was already cleared above -> this aborts it.
                    return Err(Error::Query(format!(
                        "SERIALIZABLE commit read a range of over {cap} rows; narrow the \
                         predicate, raise ELYRASQL_SERIALIZABLE_MAX_RANGE, or use a lower \
                         isolation level"
                    )));
                }
                range_snaps.push(RangeSnapshot {
                    start,
                    end,
                    content,
                });
            }
        }

        let put_vec: Vec<(Vec<u8>, Vec<u8>)> = puts.into_iter().collect();
        let del_vec: Vec<Vec<u8>> = deletes.into_iter().collect();
        // On conflict the transaction is already cleared above -> aborted.
        self.db
            .commit_validated(
                Validation {
                    keys: expected,
                    ranges: range_snaps,
                },
                put_vec,
                del_vec,
            )
            .await
    }

    pub fn rollback(&self) {
        *self.txn.lock().unwrap() = None;
    }

    // --- reads (snapshot + overlay when in a transaction) ---

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let snapshot = {
            let mut guard = self.txn.lock().unwrap();
            match guard.as_mut() {
                None => None,
                Some(tx) => {
                    if tx.serializable && !is_meta(&key) {
                        tx.reads.insert(key.clone());
                    }
                    if tx.deletes.contains(&key) {
                        return Ok(None);
                    }
                    if let Some(v) = tx.puts.get(&key) {
                        return Ok(Some(v.clone()));
                    }
                    Some(tx.snapshot.clone())
                }
            }
        };
        match snapshot {
            Some(snap) => spawn(move || snap.get(&key)).await,
            None => self.db.get(key).await,
        }
    }

    pub async fn multi_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>> {
        let snapshot = {
            let mut guard = self.txn.lock().unwrap();
            match guard.as_mut() {
                None => None,
                Some(tx) => {
                    if tx.serializable {
                        for k in &keys {
                            if !is_meta(k) {
                                tx.reads.insert(k.clone());
                            }
                        }
                    }
                    // Resolve overlay hits; collect misses for the snapshot.
                    let mut result: Vec<Option<Vec<u8>>> = Vec::with_capacity(keys.len());
                    let mut misses: Vec<(usize, Vec<u8>)> = Vec::new();
                    for (i, k) in keys.iter().enumerate() {
                        if tx.deletes.contains(k) {
                            result.push(None);
                        } else if let Some(v) = tx.puts.get(k) {
                            result.push(Some(v.clone()));
                        } else {
                            result.push(None);
                            misses.push((i, k.clone()));
                        }
                    }
                    Some((tx.snapshot.clone(), result, misses))
                }
            }
        };
        match snapshot {
            None => self.db.multi_get(keys).await,
            Some((snap, mut result, misses)) => {
                let miss_keys: Vec<Vec<u8>> = misses.iter().map(|(_, k)| k.clone()).collect();
                let fetched = spawn(move || snap.multi_get(&miss_keys)).await?;
                for ((i, _), v) in misses.into_iter().zip(fetched) {
                    result[i] = v;
                }
                Ok(result)
            }
        }
    }

    pub async fn scan_range(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Snapshot + overlay entries within [start, end), if in a transaction.
        let plan = {
            let mut guard = self.txn.lock().unwrap();
            match guard.as_mut() {
                None => None,
                Some(tx) => {
                    if tx.serializable && !is_meta(&start) {
                        // Record the scanned range for phantom validation.
                        tx.ranges.push((start.clone(), end.clone()));
                    }
                    let mut overlay: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
                    let upper = end.clone();
                    let in_range =
                        |k: &Vec<u8>| k >= &start && upper.as_ref().is_none_or(|e| k < e);
                    for (k, v) in tx.puts.range(start.clone()..) {
                        if !in_range(k) {
                            break;
                        }
                        overlay.push((k.clone(), Some(v.clone())));
                    }
                    for k in tx.deletes.range(start.clone()..) {
                        if !in_range(k) {
                            break;
                        }
                        overlay.push((k.clone(), None));
                    }
                    Some((tx.snapshot.clone(), overlay))
                }
            }
        };
        match plan {
            None => self.db.scan_range(start, end, limit).await,
            Some((snap, overlay)) => {
                let (s, e) = (start.clone(), end.clone());
                let committed = spawn(move || snap.scan_range(&s, e.as_deref(), limit)).await?;
                Ok(merge(committed, overlay, limit))
            }
        }
    }

    pub async fn scan_batch(
        &self,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if !self.in_txn() {
            return self.db.scan_batch(prefix, after, limit).await;
        }
        let start = match after {
            Some(a) => {
                let mut k = a;
                k.push(0);
                k
            }
            None => prefix.clone(),
        };
        let end = prefix_upper_bound(&prefix);
        self.scan_range(start, Some(end), limit).await
    }

    // --- writes (buffered when in a transaction) ---

    pub async fn commit_write(
        &self,
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    ) -> Result<()> {
        // Any write to a `catalog::` key changes a table definition; bump the
        // catalog epoch so cached TableDefs are refreshed. Bumping eagerly (even
        // for a buffered transactional write that may roll back) is safe -- it
        // only forces a re-read, never serves stale schema.
        if puts.iter().any(|(k, _)| k.starts_with(b"catalog::"))
            || deletes.iter().any(|k| k.starts_with(b"catalog::"))
        {
            crate::catalog::bump_epoch();
        }
        crate::catalog::note_feature_writes(&puts, &deletes);
        {
            let mut guard = self.txn.lock().unwrap();
            if let Some(tx) = guard.as_mut() {
                let budget = txn_max_bytes();
                let logging = !tx.savepoints.is_empty();
                for (k, v) in puts {
                    if logging {
                        tx.undo.push(UndoEntry {
                            prev_put: tx.puts.get(&k).cloned(),
                            prev_deleted: tx.deletes.contains(&k),
                            key: k.clone(),
                        });
                    }
                    if let Some(old) = tx.puts.get(&k) {
                        tx.mem -= k.len() + old.len();
                    }
                    if tx.deletes.remove(&k) {
                        tx.mem -= k.len();
                    }
                    tx.mem += k.len() + v.len();
                    tx.puts.insert(k, v);
                    if tx.mem > budget {
                        return Err(txn_overflow(budget));
                    }
                }
                for k in deletes {
                    let klen = k.len();
                    if logging {
                        tx.undo.push(UndoEntry {
                            prev_put: tx.puts.get(&k).cloned(),
                            prev_deleted: tx.deletes.contains(&k),
                            key: k.clone(),
                        });
                    }
                    if let Some(old) = tx.puts.remove(&k) {
                        tx.mem -= klen + old.len();
                    }
                    if tx.deletes.insert(k) {
                        tx.mem += klen;
                    }
                    if tx.mem > budget {
                        return Err(txn_overflow(budget));
                    }
                }
                return Ok(());
            }
        }
        self.db.commit(puts, deletes).await
    }
}

/// Merge a committed window with in-range overlay entries (puts override,
/// deletes remove), returning the first `limit` rows in key order.
fn merge(
    committed: Vec<(Vec<u8>, Vec<u8>)>,
    overlay: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    limit: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut map: BTreeMap<Vec<u8>, Vec<u8>> = committed.into_iter().collect();
    for (k, v) in overlay {
        match v {
            Some(val) => {
                map.insert(k, val);
            }
            None => {
                map.remove(&k);
            }
        }
    }
    map.into_iter().take(limit).collect()
}

fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last().copied() {
        if last < 0xFF {
            *end.last_mut().unwrap() = last + 1;
            return end;
        }
        end.pop();
    }
    end
}

async fn spawn<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Storage(format!("snapshot read failed: {e}")))?
}
