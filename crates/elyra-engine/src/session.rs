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

use elyra_core::{Error, Result, Value};
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
    /// Engine-owned statement checkpoints currently keeping the undo log live.
    /// These are separate from named savepoints so SQL clients cannot collide
    /// with or release an internal marker.
    checkpoints: usize,
    /// Reversible log of buffered-write mutations, recorded only while at least
    /// one savepoint is active. Lets `ROLLBACK TO` revert in
    /// O(changes-since-savepoint) instead of cloning the whole staged write set
    /// per savepoint (which was O(writes x savepoints)).
    undo: Vec<UndoEntry>,
    /// Bytes retained by undo entries while savepoints/checkpoints are active.
    undo_mem: usize,
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

/// Opaque marker used to make an engine-internal operation atomic inside an
/// existing transaction without entering the user-visible savepoint namespace.
pub(crate) struct TransactionCheckpoint {
    undo_mark: usize,
    ranges_len: usize,
    savepoints_len: usize,
}

/// One reversible mutation to the buffered write set for a single key: the
/// state of that key (in `puts` / `deletes`) *before* the mutation.
struct UndoEntry {
    key: Vec<u8>,
    prev_put: Option<Vec<u8>>,
    prev_deleted: bool,
}

fn rollback_tx_to(tx: &mut TxnState, undo_mark: usize, ranges_len: usize) {
    while tx.undo.len() > undo_mark {
        let entry = tx.undo.pop().expect("undo length checked");
        tx.undo_mem -= undo_entry_size(&entry);
        let UndoEntry {
            key,
            prev_put,
            prev_deleted,
        } = entry;
        if let Some(value) = tx.puts.get(&key) {
            tx.mem -= key.len() + value.len();
        }
        if tx.deletes.contains(&key) {
            tx.mem -= key.len();
        }
        match prev_put {
            Some(value) => {
                tx.mem += key.len() + value.len();
                tx.puts.insert(key.clone(), value);
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
    // and `locked` deliberately remain: they only make conflict validation
    // more conservative, never incorrect.
    tx.ranges.truncate(ranges_len);
}

fn coalesce_ranges(mut ranges: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    ranges.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let mut merged: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        let Some((_, previous_end)) = merged.last_mut() else {
            merged.push((start, end));
            continue;
        };
        let overlaps = previous_end
            .as_ref()
            .is_none_or(|previous_end| start.as_slice() <= previous_end.as_slice());
        if !overlaps {
            merged.push((start, end));
            continue;
        }
        match (&*previous_end, end) {
            (None, _) => {}
            (_, None) => *previous_end = None,
            (Some(previous), Some(candidate)) if candidate > *previous => {
                *previous_end = Some(candidate);
            }
            _ => {}
        }
    }
    merged
}

fn undo_entry_size(entry: &UndoEntry) -> usize {
    entry.key.len() + entry.prev_put.as_ref().map_or(0, Vec::len) + 1
}

pub struct Session {
    db: Db,
    txn: Mutex<Option<TxnState>>,
    isolation: Mutex<Isolation>,
    transaction_isolation: Mutex<String>,
    database: Mutex<String>,
    strict_sql_mode: std::sync::atomic::AtomicBool,
    sql_mode: Mutex<String>,
    autocommit: std::sync::atomic::AtomicBool,
    foreign_key_checks: std::sync::atomic::AtomicBool,
    no_auto_value_on_zero: std::sync::atomic::AtomicBool,
    group_concat_max_len: std::sync::atomic::AtomicUsize,
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
    /// Cooperative cancellation for the statement currently running on this
    /// session: armed with the query timeout on entry, checked inside the hot row
    /// loops so a runaway statement stops burning CPU instead of running to
    /// completion after the client has already been given up on.
    cancel: Arc<elyra_core::cancel::QueryCancel>,
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
            transaction_isolation: Mutex::new("REPEATABLE-READ".into()),
            database: Mutex::new("elyra".into()),
            strict_sql_mode: std::sync::atomic::AtomicBool::new(true),
            sql_mode: Mutex::new("STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION".into()),
            autocommit: std::sync::atomic::AtomicBool::new(true),
            foreign_key_checks: std::sync::atomic::AtomicBool::new(true),
            no_auto_value_on_zero: std::sync::atomic::AtomicBool::new(false),
            group_concat_max_len: std::sync::atomic::AtomicUsize::new(1024),
            call_depth: std::sync::atomic::AtomicUsize::new(0),
            pending_triggers: Mutex::new(Vec::new()),
            user_vars: Mutex::new(std::collections::HashMap::new()),
            locks,
            held_locks: Mutex::new(Vec::new()),
            last_insert_id: std::sync::atomic::AtomicI64::new(0),
            row_count: std::sync::atomic::AtomicI64::new(-1),
            cancel: Arc::new(elyra_core::cancel::QueryCancel::new()),
        }
    }

    pub(crate) fn transaction_write_budget_remaining(&self) -> usize {
        let used = self
            .txn
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |transaction| transaction.mem);
        txn_max_bytes().saturating_sub(used)
    }

    pub fn database(&self) -> String {
        self.database.lock().unwrap().clone()
    }

    pub fn set_database(&self, database: &str) {
        *self.database.lock().unwrap() = database.to_string();
    }

    /// Cancellation token for the statement running on this session. Cloned into
    /// the row loops (including work handed to blocking threads, which a
    /// `tokio::time::timeout` cannot reach).
    pub fn cancel_token(&self) -> Arc<elyra_core::cancel::QueryCancel> {
        self.cancel.clone()
    }

    /// A checker for a hot row loop, sampling the token every N rows.
    pub fn cancel_check(&self) -> elyra_core::cancel::CancelCheck {
        elyra_core::cancel::CancelCheck::new(self.cancel.clone())
    }

    /// Apply the configured query timeout to a statement that is about to run,
    /// unless a deadline is already in force.
    ///
    /// Returns whether this call armed the token, so the caller knows whether it
    /// owns the disarm. A nested statement (a trigger body, a procedure) must
    /// **inherit** the outer deadline rather than start a fresh budget — otherwise
    /// a long chain of nested statements could run indefinitely, and finishing a
    /// nested statement would clear the outer statement's deadline.
    pub fn arm_cancel_if_idle(&self) -> bool {
        let timeout = elyra_core::cancel::query_timeout();
        if timeout.is_none() || self.cancel.is_armed() {
            return false;
        }
        self.cancel.arm(timeout);
        true
    }

    /// Clear the deadline once the statement is done.
    pub fn disarm_cancel(&self) {
        self.cancel.disarm();
    }

    /// Whether invalid values fail instead of using MySQL's warning-producing
    /// fallback conversions.
    pub fn strict_sql_mode(&self) -> bool {
        self.strict_sql_mode
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_strict_sql_mode(&self, strict: bool) {
        self.strict_sql_mode
            .store(strict, std::sync::atomic::Ordering::Relaxed);
    }

    /// The current session SQL mode list, in the spelling supplied by the
    /// client. The mode list controls the settings whose behavior ElyraSQL
    /// implements, rather than being a read-only compatibility string.
    pub fn sql_mode(&self) -> String {
        self.sql_mode.lock().unwrap().clone()
    }

    pub fn set_sql_mode(&self, sql_mode: String) {
        let upper = sql_mode.to_ascii_uppercase();
        let has_mode = |mode| upper.split(',').any(|item| item.trim() == mode);
        self.set_strict_sql_mode(has_mode("STRICT_TRANS_TABLES") || has_mode("STRICT_ALL_TABLES"));
        self.no_auto_value_on_zero.store(
            has_mode("NO_AUTO_VALUE_ON_ZERO"),
            std::sync::atomic::Ordering::Relaxed,
        );
        *self.sql_mode.lock().unwrap() = sql_mode;
    }

    pub fn ansi_quotes(&self) -> bool {
        self.sql_mode
            .lock()
            .unwrap()
            .split(',')
            .any(|item| item.trim().eq_ignore_ascii_case("ANSI_QUOTES"))
    }

    pub fn no_auto_value_on_zero(&self) -> bool {
        self.no_auto_value_on_zero
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn autocommit(&self) -> bool {
        self.autocommit.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Change autocommit mode. Switching from off to on commits the current
    /// transaction, matching MySQL's implicit-commit boundary.
    pub async fn set_autocommit(&self, enabled: bool) -> Result<()> {
        if enabled && !self.autocommit() {
            self.commit().await?;
        }
        self.autocommit
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Start the transaction that MySQL opens lazily for the first DML after
    /// `SET autocommit = 0` (and after each COMMIT/ROLLBACK in that mode).
    pub fn begin_implicit_transaction(&self) -> Result<()> {
        if !self.autocommit() && !self.in_txn() {
            self.begin()?;
        }
        Ok(())
    }

    pub fn foreign_key_checks(&self) -> bool {
        self.foreign_key_checks
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_foreign_key_checks(&self, enabled: bool) {
        self.foreign_key_checks
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn group_concat_max_len(&self) -> usize {
        self.group_concat_max_len
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_group_concat_max_len(&self, max_len: usize) {
        self.group_concat_max_len
            .store(max_len.max(4), std::sync::atomic::Ordering::Relaxed);
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
        *self.transaction_isolation.lock().unwrap() = match level {
            Isolation::Snapshot => "REPEATABLE-READ",
            Isolation::Serializable => "SERIALIZABLE",
        }
        .into();
    }

    pub fn set_transaction_isolation(&self, level: &str) -> Result<()> {
        let canonical = level.trim().replace(' ', "-").to_ascii_uppercase();
        let engine_level = match canonical.as_str() {
            "READ-UNCOMMITTED" | "READ-COMMITTED" | "REPEATABLE-READ" => Isolation::Snapshot,
            "SERIALIZABLE" => Isolation::Serializable,
            _ => {
                return Err(Error::Unsupported(format!(
                    "unsupported transaction isolation level: {level}"
                )))
            }
        };
        *self.isolation.lock().unwrap() = engine_level;
        *self.transaction_isolation.lock().unwrap() = canonical;
        Ok(())
    }

    pub fn transaction_isolation(&self) -> String {
        self.transaction_isolation.lock().unwrap().clone()
    }

    /// Resolve a system variable in this connection's session scope. Variables
    /// not managed by the session retain the server's compatibility defaults.
    pub fn system_var(&self, raw: &str) -> Value {
        let scoped = raw.trim_start_matches("@@").to_ascii_lowercase();
        let (scope, name) = match scoped.split_once('.') {
            Some((scope @ ("session" | "local" | "global"), name)) => (Some(scope), name),
            _ => (None, scoped.as_str()),
        };
        if scope == Some("global") {
            return match name {
                "group_concat_max_len" => Value::Int(1024),
                _ => crate::predicate::system_var(name),
            };
        }
        match name {
            "autocommit" => Value::Int(i64::from(self.autocommit())),
            "sql_mode" => Value::Text(self.sql_mode()),
            "foreign_key_checks" => Value::Int(i64::from(self.foreign_key_checks())),
            "group_concat_max_len" => {
                Value::Int(i64::try_from(self.group_concat_max_len()).unwrap_or(i64::MAX))
            }
            "tx_isolation" | "transaction_isolation" => Value::Text(self.transaction_isolation()),
            _ => crate::predicate::system_var(raw),
        }
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

    /// The data file path, if known (used to locate the vector-index cache).
    pub fn data_path(&self) -> Option<std::path::PathBuf> {
        self.db.data_path()
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
            checkpoints: 0,
            undo: Vec::new(),
            undo_mem: 0,
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
        rollback_tx_to(tx, mark, ranges_len);
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
        if tx.savepoints.is_empty() && tx.checkpoints == 0 {
            tx.undo = Vec::new();
            tx.undo_mem = 0;
        }
        Ok(())
    }

    pub(crate) fn transaction_checkpoint(&self) -> Result<TransactionCheckpoint> {
        let mut guard = self.txn.lock().unwrap();
        let tx = guard
            .as_mut()
            .ok_or_else(|| Error::Query("transaction checkpoint outside a transaction".into()))?;
        let checkpoint = TransactionCheckpoint {
            undo_mark: tx.undo.len(),
            ranges_len: tx.ranges.len(),
            savepoints_len: tx.savepoints.len(),
        };
        tx.checkpoints += 1;
        Ok(checkpoint)
    }

    /// Upgrade the active transaction so every subsequently scanned range is
    /// validated at commit. DDL table rewrites need this even under the default
    /// snapshot isolation: otherwise a concurrent insert can land in the old
    /// keyspace after the rewrite's scan and survive the catalog change.
    pub(crate) fn require_serializable_validation(&self) -> Result<()> {
        let mut guard = self.txn.lock().unwrap();
        let transaction = guard
            .as_mut()
            .ok_or_else(|| Error::Query("serializable validation outside a transaction".into()))?;
        transaction.serializable = true;
        Ok(())
    }

    pub(crate) fn release_transaction_checkpoint(
        &self,
        _checkpoint: TransactionCheckpoint,
    ) -> Result<()> {
        let mut guard = self.txn.lock().unwrap();
        let tx = guard
            .as_mut()
            .ok_or_else(|| Error::Query("transaction checkpoint outside a transaction".into()))?;
        tx.checkpoints = tx
            .checkpoints
            .checked_sub(1)
            .ok_or_else(|| Error::Query("no active transaction checkpoint".into()))?;
        if tx.checkpoints == 0 && tx.savepoints.is_empty() {
            tx.undo.clear();
            tx.undo_mem = 0;
        }
        Ok(())
    }

    pub(crate) fn rollback_transaction_checkpoint(
        &self,
        checkpoint: TransactionCheckpoint,
    ) -> Result<()> {
        let mut guard = self.txn.lock().unwrap();
        let tx = guard
            .as_mut()
            .ok_or_else(|| Error::Query("transaction checkpoint outside a transaction".into()))?;
        tx.checkpoints = tx
            .checkpoints
            .checked_sub(1)
            .ok_or_else(|| Error::Query("no active transaction checkpoint".into()))?;
        rollback_tx_to(tx, checkpoint.undo_mark, checkpoint.ranges_len);
        tx.savepoints.truncate(checkpoint.savepoints_len);
        if tx.checkpoints == 0 && tx.savepoints.is_empty() {
            tx.undo.clear();
            tx.undo_mem = 0;
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
            checkpoints: _,
            undo: _,
            mem: _,
            undo_mem: _,
        } = tx;

        let ranges = if serializable {
            coalesce_ranges(ranges)
        } else {
            ranges
        };

        // Keys to validate = written keys, plus (serializable) read keys.
        // Per-table monotonic counters (`meta::…`) are excluded: they are bumped
        // by every write and would cause false conflicts between transactions on
        // the same table; real row collisions are still caught via data keys.
        let range_covers = |key: &[u8]| {
            serializable
                && ranges.iter().any(|(start, end)| {
                    start.as_slice() <= key && end.as_ref().is_none_or(|end| key < end.as_slice())
                })
        };
        let mut keyset: BTreeSet<Vec<u8>> = BTreeSet::new();
        keyset.extend(
            puts.keys()
                .filter(|key| !is_meta(key) && !range_covers(key))
                .cloned(),
        );
        keyset.extend(
            deletes
                .iter()
                .filter(|key| !is_meta(key) && !range_covers(key))
                .cloned(),
        );
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
        if puts
            .iter()
            .any(|(k, _)| k.starts_with(b"catalog::") || k.starts_with(b"sys::trigger::"))
            || deletes
                .iter()
                .any(|k| k.starts_with(b"catalog::") || k.starts_with(b"sys::trigger::"))
        {
            crate::catalog::bump_epoch();
        }
        crate::catalog::note_feature_writes(&puts, &deletes);
        {
            let mut guard = self.txn.lock().unwrap();
            if let Some(tx) = guard.as_mut() {
                let budget = txn_max_bytes();
                let logging = !tx.savepoints.is_empty() || tx.checkpoints > 0;
                // Match the storage-layer write order: deletes first, then
                // puts. An unchanged index entry can occur in both collections
                // during UPDATE, and the replacement must remain visible.
                for k in deletes {
                    let klen = k.len();
                    if logging {
                        let entry = UndoEntry {
                            prev_put: tx.puts.get(&k).cloned(),
                            prev_deleted: tx.deletes.contains(&k),
                            key: k.clone(),
                        };
                        let bytes = undo_entry_size(&entry);
                        if tx.mem + tx.undo_mem + bytes > budget {
                            return Err(txn_overflow(budget));
                        }
                        tx.undo_mem += bytes;
                        tx.undo.push(entry);
                    }
                    if let Some(old) = tx.puts.remove(&k) {
                        tx.mem -= klen + old.len();
                    }
                    if tx.deletes.insert(k) {
                        tx.mem += klen;
                    }
                    if tx.mem + tx.undo_mem > budget {
                        return Err(txn_overflow(budget));
                    }
                }
                for (k, v) in puts {
                    if logging {
                        let entry = UndoEntry {
                            prev_put: tx.puts.get(&k).cloned(),
                            prev_deleted: tx.deletes.contains(&k),
                            key: k.clone(),
                        };
                        let bytes = undo_entry_size(&entry);
                        if tx.mem + tx.undo_mem + bytes > budget {
                            return Err(txn_overflow(budget));
                        }
                        tx.undo_mem += bytes;
                        tx.undo.push(entry);
                    }
                    if let Some(old) = tx.puts.get(&k) {
                        tx.mem -= k.len() + old.len();
                    }
                    if tx.deletes.remove(&k) {
                        tx.mem -= k.len();
                    }
                    tx.mem += k.len() + v.len();
                    tx.puts.insert(k, v);
                    if tx.mem + tx.undo_mem > budget {
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

#[cfg(test)]
mod tests {
    use super::coalesce_ranges;

    fn bytes(value: &str) -> Vec<u8> {
        value.as_bytes().to_vec()
    }

    #[test]
    fn coalesces_overlapping_and_adjacent_ranges() {
        let ranges = vec![
            (bytes("m"), Some(bytes("p"))),
            (bytes("a"), Some(bytes("d"))),
            (bytes("c"), Some(bytes("f"))),
            (bytes("f"), Some(bytes("h"))),
            (bytes("n"), Some(bytes("o"))),
        ];

        assert_eq!(
            coalesce_ranges(ranges),
            vec![
                (bytes("a"), Some(bytes("h"))),
                (bytes("m"), Some(bytes("p"))),
            ]
        );
    }

    #[test]
    fn coalescing_preserves_nested_disjoint_and_unbounded_ranges() {
        let ranges = vec![
            (bytes("z"), Some(bytes("zz"))),
            (bytes("b"), Some(bytes("c"))),
            (bytes("a"), Some(bytes("e"))),
            (bytes("x"), None),
            (bytes("y"), Some(bytes("yz"))),
        ];

        assert_eq!(
            coalesce_ranges(ranges),
            vec![(bytes("a"), Some(bytes("e"))), (bytes("x"), None)]
        );
    }
}
