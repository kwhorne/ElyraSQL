//! Per-connection transaction state.
//!
//! ElyraSQL implements `BEGIN`/`COMMIT`/`ROLLBACK` with an **undo log**: inside
//! a transaction, writes are applied to storage immediately (so the connection
//! reads its own changes), while the prior value of every touched key is
//! recorded. `COMMIT` discards the log; `ROLLBACK` replays it in reverse to
//! restore the pre-transaction state.
//!
//! Isolation: this gives atomic all-or-nothing rollback and read-your-writes,
//! but writes are visible to other connections before commit (READ
//! UNCOMMITTED). Snapshot isolation is a future enhancement.

use std::collections::HashSet;

use elyra_core::Result;
use elyra_storage::Db;

/// Transaction state for one connection.
#[derive(Default)]
pub struct Txn {
    /// `Some` while a transaction is open: (undo entries, keys already captured).
    log: Option<(Vec<(Vec<u8>, Option<Vec<u8>>)>, HashSet<Vec<u8>>)>,
}

impl Txn {
    pub fn new() -> Self {
        Txn { log: None }
    }

    pub fn in_txn(&self) -> bool {
        self.log.is_some()
    }

    /// Begin a transaction. If one is already open, it is committed first
    /// (MySQL semantics for a nested `BEGIN`).
    pub fn begin(&mut self) {
        self.log = Some((Vec::new(), HashSet::new()));
    }

    /// Commit: drop the undo log (applied writes become permanent).
    pub fn commit(&mut self) {
        self.log = None;
    }

    /// Apply a write. Inside a transaction, capture undo info for any key not
    /// yet touched, then apply; otherwise apply directly (autocommit).
    pub async fn apply(
        &mut self,
        db: &Db,
        puts: Vec<(Vec<u8>, Vec<u8>)>,
        deletes: Vec<Vec<u8>>,
    ) -> Result<()> {
        if let Some((undo, seen)) = &mut self.log {
            let mut capture: Vec<Vec<u8>> = Vec::new();
            for (k, _) in &puts {
                if !seen.contains(k) {
                    capture.push(k.clone());
                }
            }
            for k in &deletes {
                if !seen.contains(k) {
                    capture.push(k.clone());
                }
            }
            if !capture.is_empty() {
                let priors = db.multi_get(capture.clone()).await?;
                for (k, prior) in capture.into_iter().zip(priors) {
                    seen.insert(k.clone());
                    undo.push((k, prior));
                }
            }
        }
        db.commit(puts, deletes).await
    }

    /// Roll back: restore every captured key to its pre-transaction value.
    pub async fn rollback(&mut self, db: &Db) -> Result<()> {
        if let Some((undo, _)) = self.log.take() {
            let mut puts = Vec::new();
            let mut deletes = Vec::new();
            for (k, prior) in undo.into_iter().rev() {
                match prior {
                    Some(v) => puts.push((k, v)),
                    None => deletes.push(k),
                }
            }
            db.commit(puts, deletes).await?;
        }
        Ok(())
    }
}
