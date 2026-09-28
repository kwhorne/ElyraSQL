//! Cached reads of schema keys.
//!
//! Statements read schema records that almost never change: a point `SELECT`
//! checked whether its table was a view (twice) and read the user's privileges
//! and roles; an `INSERT` or `UPDATE` read the table's declared widths; an
//! `UPDATE` or `DELETE` listed every table to find foreign keys pointing at
//! its own. Each read is a storage transaction and a hop to a blocking thread.
//!
//! This caches those reads -- a key's value, or the keys under a prefix,
//! including "absent" -- tagged with the storage layer's schema generation
//! ([`elyra_storage::schema_generation`]). The generation is bumped after the
//! commit of every write that touches a schema key, on every path: sessions,
//! replicas applying their primary's stream, cluster followers. The generation
//! is loaded *before* the read, so a commit the read might have missed always
//! leaves the entry stale. An entry is therefore served only while it equals
//! the committed state.
//!
//! Inside a transaction every read goes to the session as before, so the
//! transaction sees its own uncommitted writes and serializable read tracking
//! is unaffected; nothing read there is cached.

use crate::session::Session;
use elyra_core::Result;

#[derive(Clone)]
enum Read {
    Value(Option<Vec<u8>>),
    Keys(Vec<Vec<u8>>),
}

/// (database id, 0 = get / 1 = prefix keys, key or prefix).
type Key = (u64, u8, Vec<u8>);

/// Bound on cached reads; past it the cache is cleared and refills.
const MAX_ENTRIES: usize = 16_384;

#[allow(clippy::type_complexity)]
fn cache() -> &'static std::sync::RwLock<std::collections::HashMap<Key, (u64, Read)>> {
    use std::sync::{OnceLock, RwLock};
    static C: OnceLock<RwLock<std::collections::HashMap<Key, (u64, Read)>>> = OnceLock::new();
    C.get_or_init(|| RwLock::new(std::collections::HashMap::new()))
}

fn cached(key: &Key, generation: u64) -> Option<Read> {
    match cache().read().unwrap().get(key) {
        Some((g, read)) if *g == generation => Some(read.clone()),
        _ => None,
    }
}

fn store(key: Key, generation: u64, read: Read) {
    let mut cache = cache().write().unwrap();
    if cache.len() >= MAX_ENTRIES {
        cache.clear();
    }
    cache.insert(key, (generation, read));
}

/// Only schema keys may be cached: nothing else bumps the generation.
fn assert_schema_key(key: &[u8]) {
    debug_assert!(
        elyra_storage::SCHEMA_PREFIXES
            .iter()
            .any(|p| key.starts_with(p)),
        "not a schema key: {:?}",
        String::from_utf8_lossy(key)
    );
}

/// `sess.get(key)` for a schema key, cached outside transactions.
pub async fn get(sess: &Session, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
    assert_schema_key(&key);
    if sess.in_txn() {
        return sess.get(key).await;
    }
    let generation = elyra_storage::schema_generation();
    let ck = (sess.db_id(), 0, key);
    if let Some(Read::Value(v)) = cached(&ck, generation) {
        return Ok(v);
    }
    let v = sess.get(ck.2.clone()).await?;
    store(ck, generation, Read::Value(v.clone()));
    Ok(v)
}

/// Every key under the schema prefix `prefix`, in order, cached outside
/// transactions.
pub async fn keys(sess: &Session, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
    assert_schema_key(prefix);
    let scan = || async {
        const BATCH: usize = 4096;
        let mut keys = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let batch = sess
                .scan_batch(prefix.to_vec(), after.take(), BATCH)
                .await?;
            let full = batch.len() == BATCH;
            keys.extend(batch.into_iter().map(|(k, _)| k));
            if !full {
                return Ok::<_, elyra_core::Error>(keys);
            }
            after = keys.last().cloned();
        }
    };
    if sess.in_txn() {
        return scan().await;
    }
    let generation = elyra_storage::schema_generation();
    let ck = (sess.db_id(), 1, prefix.to_vec());
    if let Some(Read::Keys(keys)) = cached(&ck, generation) {
        return Ok(keys);
    }
    let keys = scan().await?;
    store(ck, generation, Read::Keys(keys.clone()));
    Ok(keys)
}
