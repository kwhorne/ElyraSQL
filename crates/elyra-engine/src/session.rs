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

use elyra_core::{Error, Result};
use elyra_storage::{Db, Snapshot};

struct TxnState {
    snapshot: Snapshot,
    puts: BTreeMap<Vec<u8>, Vec<u8>>,
    deletes: BTreeSet<Vec<u8>>,
}

pub struct Session {
    db: Db,
    txn: Mutex<Option<TxnState>>,
}

impl Session {
    pub fn new(db: Db) -> Self {
        Session {
            db,
            txn: Mutex::new(None),
        }
    }

    pub fn in_txn(&self) -> bool {
        self.txn.lock().unwrap().is_some()
    }

    /// The underlying committed-state handle (used for streaming scans in
    /// autocommit mode only).
    pub fn raw_db(&self) -> Db {
        self.db.clone()
    }

    // --- transaction control ---

    pub fn begin(&self) -> Result<()> {
        let snapshot = self.db.snapshot()?;
        *self.txn.lock().unwrap() = Some(TxnState {
            snapshot,
            puts: BTreeMap::new(),
            deletes: BTreeSet::new(),
        });
        Ok(())
    }

    pub async fn commit(&self) -> Result<()> {
        let staged = self.txn.lock().unwrap().take();
        if let Some(tx) = staged {
            let puts: Vec<(Vec<u8>, Vec<u8>)> = tx.puts.into_iter().collect();
            let deletes: Vec<Vec<u8>> = tx.deletes.into_iter().collect();
            self.db.commit(puts, deletes).await?;
        }
        Ok(())
    }

    pub fn rollback(&self) {
        *self.txn.lock().unwrap() = None;
    }

    // --- reads (snapshot + overlay when in a transaction) ---

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let snapshot = {
            let guard = self.txn.lock().unwrap();
            match &*guard {
                None => None,
                Some(tx) => {
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
            let guard = self.txn.lock().unwrap();
            match &*guard {
                None => None,
                Some(tx) => {
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
            let guard = self.txn.lock().unwrap();
            match &*guard {
                None => None,
                Some(tx) => {
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
        {
            let mut guard = self.txn.lock().unwrap();
            if let Some(tx) = guard.as_mut() {
                for (k, v) in puts {
                    tx.deletes.remove(&k);
                    tx.puts.insert(k, v);
                }
                for k in deletes {
                    tx.puts.remove(&k);
                    tx.deletes.insert(k);
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
