//! Pessimistic table-level locking.
//!
//! Complements the default optimistic (snapshot / serializable) concurrency with
//! **blocking** table locks for workloads that need them: `LOCK TABLES` /
//! `UNLOCK TABLES` and `SELECT ... LOCK IN SHARE MODE` (a synonym for
//! `FOR SHARE`).
//!
//! A session that holds an explicit lock blocks conflicting access from other
//! sessions: `WRITE` (exclusive) blocks all other readers and writers; `READ`
//! (shared) blocks other writers. While *any* explicit lock is held, other
//! sessions' statements acquire a transient lock on their target table for the
//! duration of the statement, so they block until the explicit holder releases.
//!
//! When no explicit lock is held anywhere, the whole mechanism is skipped
//! (`explicit_active()` is a single atomic load), so the common path pays
//! nothing. Transient acquisition uses a timeout and reports error `1205`
//! (`ER_LOCK_WAIT_TIMEOUT`) to break deadlocks, matching MySQL.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use elyra_core::{Error, Result};
use tokio::sync::Notify;

/// How long a transient (per-statement) lock waits before failing with 1205.
const LOCK_WAIT: Duration = Duration::from_secs(10);
/// Upper bound on a single wait poll, so a missed wakeup can't stall a waiter.
const POLL_CAP: Duration = Duration::from_millis(50);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LockMode {
    Shared,
    Exclusive,
}

#[derive(Default)]
struct TableState {
    readers: usize,
    writer: bool,
}

/// Shared, cluster-of-one lock manager, held behind an `Arc` in the engine and
/// shared across all sessions.
pub struct LockManager {
    tables: Mutex<HashMap<String, TableState>>,
    notify: Notify,
    /// Number of explicit `LOCK TABLES` locks held across all sessions.
    explicit: AtomicUsize,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            tables: Mutex::new(HashMap::new()),
            notify: Notify::new(),
            explicit: AtomicUsize::new(0),
        }
    }

    /// Whether any explicit `LOCK TABLES` lock is currently held (fast path
    /// gate: when false, transient locking is skipped entirely).
    pub fn explicit_active(&self) -> bool {
        self.explicit.load(Ordering::Acquire) > 0
    }

    fn try_take(&self, table: &str, mode: LockMode) -> bool {
        let mut m = self.tables.lock().unwrap();
        let st = m.entry(table.to_string()).or_default();
        let ok = match mode {
            LockMode::Shared => !st.writer,
            LockMode::Exclusive => !st.writer && st.readers == 0,
        };
        if ok {
            match mode {
                LockMode::Shared => st.readers += 1,
                LockMode::Exclusive => st.writer = true,
            }
        }
        ok
    }

    fn give_back(&self, table: &str, mode: LockMode) {
        {
            let mut m = self.tables.lock().unwrap();
            if let Some(st) = m.get_mut(table) {
                match mode {
                    LockMode::Shared => st.readers = st.readers.saturating_sub(1),
                    LockMode::Exclusive => st.writer = false,
                }
                if !st.writer && st.readers == 0 {
                    m.remove(table);
                }
            }
        }
        self.notify.notify_waiters();
    }

    /// Acquire `mode` on `table`, blocking up to `timeout`. Returns a guard whose
    /// drop releases the lock.
    pub async fn acquire(
        self: &Arc<Self>,
        table: &str,
        mode: LockMode,
        explicit: bool,
        timeout: Duration,
    ) -> Result<LockGuard> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.try_take(table, mode) {
                if explicit {
                    self.explicit.fetch_add(1, Ordering::Release);
                }
                return Ok(LockGuard {
                    mgr: self.clone(),
                    table: table.to_string(),
                    mode,
                    explicit,
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::Query(
                    "Lock wait timeout exceeded; try restarting transaction".into(),
                ));
            }
            let wait = (deadline - now).min(POLL_CAP);
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }
}

/// RAII release of a held table lock.
pub struct LockGuard {
    mgr: Arc<LockManager>,
    table: String,
    mode: LockMode,
    explicit: bool,
}

impl LockGuard {
    pub fn table(&self) -> &str {
        &self.table
    }
    pub fn mode(&self) -> LockMode {
        self.mode
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        self.mgr.give_back(&self.table, self.mode);
        if self.explicit {
            self.mgr.explicit.fetch_sub(1, Ordering::Release);
        }
    }
}

/// Acquire a transient (statement-scoped) lock, honouring the wait timeout.
pub async fn transient(mgr: &Arc<LockManager>, table: &str, mode: LockMode) -> Result<LockGuard> {
    mgr.acquire(table, mode, false, LOCK_WAIT).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_locks_coexist_exclusive_conflicts() {
        let m = Arc::new(LockManager::new());
        let a = m
            .acquire("t", LockMode::Shared, true, LOCK_WAIT)
            .await
            .unwrap();
        assert!(m.explicit_active());
        let b = m
            .acquire("t", LockMode::Shared, false, LOCK_WAIT)
            .await
            .unwrap();
        // Exclusive must fail fast while shared locks are held.
        let x = m
            .acquire("t", LockMode::Exclusive, false, Duration::from_millis(80))
            .await;
        assert!(x.is_err());
        drop(a);
        drop(b);
        // Now exclusive succeeds.
        let _x = m
            .acquire("t", LockMode::Exclusive, false, LOCK_WAIT)
            .await
            .unwrap();
        assert!(!m.explicit_active());
    }
}
