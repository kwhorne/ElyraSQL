//! The allocators behind AUTO_INCREMENT values and the hidden row ids of tables
//! without a primary key.
//!
//! Each INSERT used to read the stored counter and count up from it on its own,
//! so concurrent INSERTs into one table handed out the same values: with a
//! primary key all but one failed with a false duplicate-key error, and without
//! one -- the row id being the key, written unchecked -- one row silently
//! replaced the other. Now every value comes from one atomic counter per table,
//! shared by all sessions of the process.
//!
//! The stored counter is only what an allocator resumes from: the first time a
//! table is used after start, and after a write that deletes the counter
//! (TRUNCATE, DROP, a rebuild), which is how a reset reaches the allocator --
//! whichever way that write arrived: a statement here, or a write applied from
//! a Raft leader, a primary, a binlog or a snapshot (see
//! `elyra_storage::Storage::counter_resets`).
//! Storage keeps the stored value from moving backwards when commits that
//! persist it land out of order (see `put_kv` in `elyra-storage`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use elyra_core::Result;

use crate::session::Session;

const COUNTER_PREFIXES: [&[u8]; 2] = [b"meta::autoinc::", b"meta::rowid::"];

/// Each allocator, with how many resets of its counter storage had seen when
/// it was loaded.
type Counters = HashMap<(u64, Vec<u8>), (Arc<AtomicI64>, u64)>;

fn counters() -> &'static Mutex<Counters> {
    static COUNTERS: OnceLock<Mutex<Counters>> = OnceLock::new();
    COUNTERS.get_or_init(Default::default)
}

/// The allocator for the counter stored at `key`: `fetch_add(1) + 1` is the
/// next value, and `fetch_max(n)` records an explicitly inserted one.
pub async fn counter(db: &Session, key: Vec<u8>) -> Result<Arc<AtomicI64>> {
    // Read before the stored value: a reset committed in between moves it on
    // again, and the next call starts over.
    let resets = db.raw_db().counter_resets(&key);
    // The committed value, not a transaction's snapshot: ids are not
    // transactional, as in MySQL.
    let stored = match db.raw_db().get(key.clone()).await? {
        Some(bytes) if bytes.len() == 8 => {
            i64::from_le_bytes(bytes.try_into().expect("checked length"))
        }
        _ => 0,
    };
    let counter = {
        let mut counters = counters().lock().unwrap();
        let entry = counters
            .entry((db.db_id(), key))
            .or_insert_with(|| (Arc::new(AtomicI64::new(stored)), resets));
        // A counter was deleted since this allocator was loaded, maybe by a
        // write this node applied for another: a TRUNCATE on a Raft leader used
        // to leave a follower's allocator at the old values, which it went on
        // from once it led.
        if entry.1 < resets {
            *entry = (Arc::new(AtomicI64::new(stored)), resets);
        }
        entry.0.clone()
    };
    // Another node may have advanced it (a former Raft leader), or it was
    // never loaded; never hand out a value the stored counter already covers.
    counter.fetch_max(stored, Ordering::SeqCst);
    Ok(counter)
}

/// Drop the allocators whose counters a committed write deleted, so the next
/// INSERT starts again from what is stored.
pub fn forget_deleted<'a>(db_id: u64, deletes: impl IntoIterator<Item = &'a Vec<u8>>) {
    let mut gone = deletes
        .into_iter()
        .filter(|k| COUNTER_PREFIXES.iter().any(|p| k.starts_with(p)))
        .peekable();
    if gone.peek().is_none() {
        return;
    }
    let mut counters = counters().lock().unwrap();
    for key in gone {
        counters.remove(&(db_id, key.clone()));
    }
}
