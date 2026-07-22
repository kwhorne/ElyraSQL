//! In-memory vector-index (HNSW) registry with incremental, stale-aware caching.
//!
//! ElyraSQL keeps the authoritative vectors in the single file. The HNSW graph
//! is a cached, memory-resident snapshot keyed by `table.column`. When the
//! table's write counter advances, the next query **reconciles** the cache
//! against storage: rows that changed since the last reconcile are inserted or
//! soft-tombstoned in the existing graph (O(delta) graph work), rather than
//! rebuilding all N vectors from scratch. A full rebuild is used only for the
//! first build, for a large change, or to compact when too many nodes are dead.
//! The diff is content-based (per-row vector hash), so it is correct for
//! INSERT / UPDATE / DELETE regardless of the coarse write counter.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::Mutex as AsyncMutex;

use serde::{Deserialize, Serialize};

use crate::session::Session;
use elyra_core::{Error, Result, Value};
use elyra_vector::{Hnsw, HnswParts, Metric};

use crate::catalog::{data_prefix, wcount_key, TableDef};

/// On-disk vector-index cache (see ESQL-27). The graph is a regenerable cache,
/// so it lives in a sibling directory `<data>.vidx/` (like `<data>.raftstate`),
/// not in the authoritative single file — keeping it out of replication, backups
/// and the global write-sequence that gates the column cache.
const SNAP_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct IndexSnapshot {
    version: u32,
    wcount: u64,
    metric: u8,
    dim: usize,
    vectors: Vec<Vec<f32>>,
    neighbors: Vec<Vec<Vec<u32>>>,
    entry: u32,
    max_level: usize,
    rng_state: u64,
    node_key: Vec<Option<Vec<u8>>>,
    tombstones: usize,
}

fn metric_to_u8(m: Metric) -> u8 {
    match m {
        Metric::L2 => 0,
        Metric::Cosine => 1,
        Metric::InnerProduct => 2,
    }
}
fn metric_from_u8(b: u8) -> Metric {
    match b {
        1 => Metric::Cosine,
        2 => Metric::InnerProduct,
        _ => Metric::L2,
    }
}

/// Path of the persisted snapshot for `key` under the data file's `.vidx` dir.
fn snapshot_path(db: &Session, key: &str) -> Option<std::path::PathBuf> {
    let data = db.data_path()?;
    let mut dir = data.clone().into_os_string();
    dir.push(".vidx");
    let dir = std::path::PathBuf::from(dir);
    // Hash the key so table/column names never produce an unsafe filename.
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    Some(dir.join(format!("{:016x}.hnsw", h.finish())))
}

/// Serialize the current state to bytes (clones the graph data).
fn snapshot_bytes(st: &IndexState) -> Option<Vec<u8>> {
    let parts = st.hnsw.export();
    let snap = IndexSnapshot {
        version: SNAP_VERSION,
        wcount: st.wcount,
        metric: metric_to_u8(parts.metric),
        dim: parts.dim,
        vectors: parts.vectors,
        neighbors: parts.neighbors,
        entry: parts.entry,
        max_level: parts.max_level,
        rng_state: parts.rng_state,
        node_key: st.node_key.clone(),
        tombstones: st.tombstones,
    };
    bincode::serialize(&snap).ok()
}

/// Persist the state to disk (best-effort; failures are logged, not fatal —
/// the cache is regenerable). The serialize + write runs off the async runtime.
async fn persist(db: &Session, key: &str, cached: &CachedIndex) {
    let Some(path) = snapshot_path(db, key) else {
        return;
    };
    let bytes = {
        let st = cached.state.read().unwrap_or_else(|e| e.into_inner());
        snapshot_bytes(&st)
    };
    let Some(bytes) = bytes else { return };
    let _ = tokio::task::spawn_blocking(move || {
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        // Atomic replace: write to a temp file then rename.
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, &bytes).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    })
    .await;
}

/// Load a persisted snapshot into a fresh `CachedIndex`, or `None` if absent,
/// unreadable, corrupt, or a different format version (caller then rebuilds).
async fn load(db: &Session, key: &str) -> Option<CachedIndex> {
    let path = snapshot_path(db, key)?;
    let bytes = tokio::task::spawn_blocking(move || std::fs::read(&path).ok())
        .await
        .ok()??;
    let snap: IndexSnapshot = bincode::deserialize(&bytes).ok()?;
    if snap.version != SNAP_VERSION {
        return None;
    }
    let parts = HnswParts {
        dim: snap.dim,
        metric: metric_from_u8(snap.metric),
        vectors: snap.vectors,
        neighbors: snap.neighbors,
        entry: snap.entry,
        max_level: snap.max_level,
        rng_state: snap.rng_state,
    };
    let hnsw = Hnsw::from_parts(parts);
    // Rebuild the key->node and key->hash maps from the persisted node_key and
    // the graph's stored vectors.
    let mut key_node = HashMap::new();
    let mut key_hash = HashMap::new();
    for (node, slot) in snap.node_key.iter().enumerate() {
        if let Some(k) = slot {
            key_node.insert(k.clone(), node as u32);
            if let Some(v) = hnsw.vector(node as u32) {
                key_hash.insert(k.clone(), vec_hash(v));
            }
        }
    }
    Some(CachedIndex {
        state: RwLock::new(IndexState {
            wcount: snap.wcount,
            hnsw,
            node_key: snap.node_key,
            key_node,
            key_hash,
            tombstones: snap.tombstones,
        }),
    })
}

/// Mutable graph state, guarded by a single `RwLock` so searches read
/// concurrently while a reconcile mutates in place.
struct IndexState {
    wcount: u64,
    hnsw: Hnsw,
    /// `node_key[node]` = the row's data key, or `None` if the node is a
    /// soft-tombstoned (deleted/superseded) graph waypoint.
    node_key: Vec<Option<Vec<u8>>>,
    /// Live data key -> node id.
    key_node: HashMap<Vec<u8>, u32>,
    /// Live data key -> vector content hash (for change detection).
    key_hash: HashMap<Vec<u8>, u64>,
    /// Count of tombstoned nodes (drives compaction).
    tombstones: usize,
}

/// A cached HNSW index over one `table.column`.
pub struct CachedIndex {
    state: RwLock<IndexState>,
}

impl CachedIndex {
    /// Approximate nearest neighbours as `(data_key, distance)`, tombstoned nodes
    /// filtered out. Over-fetches to absorb tombstoned hits and still return `k`.
    pub fn search_keys(&self, q: &[f32], k: usize, ef: usize) -> Vec<(Vec<u8>, f32)> {
        let st = self.state.read().unwrap_or_else(|e| e.into_inner());
        if st.hnsw.is_empty() || k == 0 {
            return Vec::new();
        }
        let want = k
            .saturating_add(st.tombstones.min(k.saturating_mul(4)))
            .max(k)
            + 1;
        let hits = st.hnsw.search(q, want, ef.max(want));
        let mut out = Vec::with_capacity(k);
        for (node, d) in hits {
            if let Some(Some(key)) = st.node_key.get(node as usize) {
                out.push((key.clone(), d));
                if out.len() >= k {
                    break;
                }
            }
        }
        out
    }
}

/// Content hash of a vector for change detection (order-sensitive over the bits).
fn vec_hash(v: &[f32]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    v.len().hash(&mut h);
    for &x in v {
        x.to_bits().hash(&mut h);
    }
    h.finish()
}

#[derive(Clone, Default)]
pub struct VectorRegistry {
    inner: Arc<RwLock<HashMap<String, Arc<CachedIndex>>>>,
    /// Per-index-key build locks providing single-flight rebuilds: only one task
    /// rebuilds a given index at a time while others wait for its result,
    /// preventing a thundering-herd of parallel full-table scans + HNSW builds
    /// after a write invalidates the cache.
    build_locks: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
}

impl VectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch a fresh cached index for `table.col`, building it from storage if
    /// missing or stale.
    pub async fn get(
        &self,
        db: &Session,
        def: &TableDef,
        col: usize,
        metric: Metric,
    ) -> Result<Arc<CachedIndex>> {
        let key = format!("{}.{}", def.name, def.schema.columns[col].name);
        let wcount = read_wcount(db, &def.name).await?;

        // Fast path: cached and fresh.
        if let Some(cached) = self
            .inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .cloned()
        {
            if cached
                .state
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .wcount
                == wcount
            {
                return Ok(cached);
            }
        }

        // Single-flight: serialize rebuilds of this index key so a burst of
        // concurrent queries after a write triggers exactly one rebuild, not one
        // per task.
        let lock = self.build_lock(&key);
        let _guard = lock.lock().await;

        // Double-checked: another task may have finished the rebuild while we
        // waited for the lock. Re-read the write counter in case more writes
        // landed meanwhile, then reuse a cached index that already matches it.
        let wcount = read_wcount(db, &def.name).await?;
        let existing = self
            .inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&key)
            .cloned();
        if let Some(cached) = &existing {
            if cached
                .state
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .wcount
                == wcount
            {
                return Ok(cached.clone());
            }
        }

        // Scan the authoritative vectors once (same I/O the old full rebuild paid).
        let (dim, current) = scan_current(db, def, col).await?;

        if let Some(cached) = existing {
            // Warm cache: reconcile the existing graph against `current` in place.
            // Persist only when a (infrequent) full rebuild/compaction happened;
            // small incremental deltas are cheap to replay from disk on restart.
            let rebuilt = reconcile(&cached, dim, current, metric, wcount);
            if rebuilt {
                persist(db, &key, &cached).await;
            }
            Ok(cached)
        } else {
            // Cold: reuse a persisted snapshot if present (avoids the cold-start
            // rebuild), reconciling it to the current data; otherwise build fresh.
            let cached = match load(db, &key).await {
                Some(idx) => {
                    let idx = Arc::new(idx);
                    reconcile(&idx, dim, current, metric, wcount);
                    idx
                }
                None => Arc::new(full_build(dim, current, metric, wcount)),
            };
            persist(db, &key, &cached).await;
            self.inner
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key, cached.clone());
            Ok(cached)
        }
    }

    /// The shared build lock for `key`, creating it on first use.
    fn build_lock(&self, key: &str) -> Arc<AsyncMutex<()>> {
        self.build_locks
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
}

pub async fn read_wcount(db: &Session, table: &str) -> Result<u64> {
    Ok(match db.get(wcount_key(table)).await? {
        Some(b) if b.len() == 8 => u64::from_le_bytes(b.try_into().expect("len 8")),
        _ => 0,
    })
}

/// Total number of HNSW index (re)builds performed; used to verify
/// single-flight behavior under concurrency.
static BUILDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Number of HNSW index builds performed since process start.
#[cfg(test)]
pub fn build_count() -> u64 {
    BUILDS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Scan the table's non-empty vectors from `col`, in clustered order, as
/// `(data_key, vector)` pairs plus the dimensionality.
async fn scan_current(
    db: &Session,
    def: &TableDef,
    col: usize,
) -> Result<(usize, Vec<(Vec<u8>, Vec<f32>)>)> {
    let prefix = data_prefix(&def.name);
    let mut cursor: Option<Vec<u8>> = None;
    let mut out: Vec<(Vec<u8>, Vec<f32>)> = Vec::new();
    let mut dim = 0usize;
    loop {
        let chunk = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if chunk.is_empty() {
            break;
        }
        let last = chunk.len() < 4096;
        cursor = chunk.last().map(|(k, _)| k.clone());
        for (k, v) in chunk {
            let row: Vec<Value> =
                bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;
            if let Some(Value::Vector(vec)) = row.get(col) {
                if !vec.is_empty() {
                    dim = vec.len();
                    out.push((k, vec.clone()));
                }
            }
        }
        if last {
            break;
        }
    }
    Ok((dim, out))
}

/// Build a fresh index from the full current set (initial build / compaction).
fn full_build(
    dim: usize,
    current: Vec<(Vec<u8>, Vec<f32>)>,
    metric: Metric,
    wcount: u64,
) -> CachedIndex {
    BUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut vectors = Vec::with_capacity(current.len());
    let mut node_key = Vec::with_capacity(current.len());
    let mut key_node = HashMap::with_capacity(current.len());
    let mut key_hash = HashMap::with_capacity(current.len());
    for (node, (k, v)) in current.into_iter().enumerate() {
        key_hash.insert(k.clone(), vec_hash(&v));
        key_node.insert(k.clone(), node as u32);
        node_key.push(Some(k));
        vectors.push(v);
    }
    let hnsw = Hnsw::build(vectors, dim, metric);
    CachedIndex {
        state: RwLock::new(IndexState {
            wcount,
            hnsw,
            node_key,
            key_node,
            key_hash,
            tombstones: 0,
        }),
    }
}

/// Reconcile `cached` against the current row set: apply the changed rows
/// incrementally, or fall back to a full rebuild for a large change / compaction.
/// Returns `true` if a full rebuild (compaction) was performed (so the caller
/// may persist the fresh snapshot).
fn reconcile(
    cached: &CachedIndex,
    dim: usize,
    current: Vec<(Vec<u8>, Vec<f32>)>,
    metric: Metric,
    wcount: u64,
) -> bool {
    // Diff current against the cached live set (read lock only for the compare).
    let mut inserts: Vec<(Vec<u8>, Vec<f32>, u64)> = Vec::new();
    let mut updates: Vec<(Vec<u8>, Vec<f32>, u64)> = Vec::new();
    let mut seen: std::collections::HashSet<Vec<u8>> =
        std::collections::HashSet::with_capacity(current.len());
    {
        let st = cached.state.read().unwrap_or_else(|e| e.into_inner());
        for (k, v) in &current {
            seen.insert(k.clone());
            let h = vec_hash(v);
            match st.key_hash.get(k) {
                Some(&old) if old == h => {} // unchanged
                Some(_) => updates.push((k.clone(), v.clone(), h)),
                None => inserts.push((k.clone(), v.clone(), h)),
            }
        }
    }
    // Deletions: live keys no longer present.
    let deletes: Vec<Vec<u8>> = {
        let st = cached.state.read().unwrap_or_else(|e| e.into_inner());
        st.key_node
            .keys()
            .filter(|k| !seen.contains(*k))
            .cloned()
            .collect()
    };

    let delta = inserts.len() + updates.len() + deletes.len();
    let n = current.len();

    // Decide: full rebuild vs. incremental. Rebuild when the change is as large
    // as the data, or when compaction is warranted (too many dead nodes would
    // remain), or the set is tiny (rebuild is cheap and keeps the graph pristine).
    let tombs_after = {
        let st = cached.state.read().unwrap_or_else(|e| e.into_inner());
        st.tombstones + deletes.len() + updates.len()
    };
    let live_after = n;
    // Only rebuild when something actually changed and it is a large change,
    // compaction is due, or the set is tiny (rebuild is trivially cheap then).
    let rebuild = delta > 0 && (n < 256 || delta >= n.max(1) || tombs_after > live_after);

    if rebuild {
        let fresh = full_build(dim, current, metric, wcount);
        let mut st = cached.state.write().unwrap_or_else(|e| e.into_inner());
        *st = fresh.state.into_inner().unwrap_or_else(|e| e.into_inner());
        return true;
    }

    // Incremental apply under the write lock.
    let mut st = cached.state.write().unwrap_or_else(|e| e.into_inner());
    // Deletes + the old side of updates -> tombstone.
    for k in deletes.iter().chain(updates.iter().map(|(k, _, _)| k)) {
        if let Some(node) = st.key_node.remove(k) {
            if let Some(slot) = st.node_key.get_mut(node as usize) {
                if slot.is_some() {
                    *slot = None;
                    st.tombstones += 1;
                }
            }
            st.key_hash.remove(k);
        }
    }
    // Inserts + the new side of updates -> new graph node.
    for (k, v, h) in inserts.into_iter().chain(updates) {
        let node = st.hnsw.insert_one(v);
        debug_assert_eq!(node as usize, st.node_key.len());
        st.node_key.push(Some(k.clone()));
        st.key_node.insert(k.clone(), node);
        st.key_hash.insert(k, h);
    }
    st.wcount = wcount;
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableDef;
    use crate::lockmgr::LockManager;
    use elyra_core::{ColumnDef, ColumnType, Schema};
    use elyra_storage::Db;

    // `build_count()` is a process-global counter, so the two tests that assert
    // on it must not overlap. Serialize them on a shared async lock.
    static BUILD_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn temp_db() -> (Db, std::path::PathBuf) {
        let mut p = std::env::temp_dir();
        let uniq = format!(
            "elyra_vindex_{}_{}.edb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        p.push(uniq);
        (Db::open(&p).unwrap(), p)
    }

    fn vector_table() -> TableDef {
        TableDef {
            name: "vt".into(),
            schema: Schema::new(vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("v", ColumnType::Vector(4), false),
            ]),
            pk_cols: vec![0],
            indexes: Vec::new(),
            col_meta: Vec::new(),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    async fn bump(sess: &Session) -> (Vec<u8>, Vec<u8>) {
        let next = read_wcount(sess, "vt").await.unwrap() + 1;
        (wcount_key("vt"), next.to_le_bytes().to_vec())
    }
    // Write one (id, vector) row and bump the write counter (like a real DML).
    async fn put(sess: &Session, id: i64, v: [f32; 4]) {
        let mut key = data_prefix("vt");
        key.extend_from_slice(&id.to_le_bytes());
        let row = vec![Value::Int(id), Value::Vector(v.to_vec())];
        let wc = bump(sess).await;
        sess.commit_write(vec![(key, bincode::serialize(&row).unwrap()), wc], vec![])
            .await
            .unwrap();
    }
    async fn del(sess: &Session, id: i64) {
        let mut key = data_prefix("vt");
        key.extend_from_slice(&id.to_le_bytes());
        let wc = bump(sess).await;
        sess.commit_write(vec![wc], vec![key]).await.unwrap();
    }
    fn id_of(key: &[u8]) -> i64 {
        let n = key.len();
        i64::from_le_bytes(key[n - 8..].try_into().unwrap())
    }

    #[tokio::test]
    async fn incremental_reconcile_insert_update_delete() {
        let _serial = BUILD_TEST_LOCK.lock().await;
        let (db, path) = temp_db();
        let sess = Session::new(db, std::sync::Arc::new(LockManager::new()));
        let def = vector_table();
        // Seed 300 rows (> the 256 rebuild floor) clustered far from the probes.
        let mut puts = Vec::new();
        for i in 0..300i64 {
            let mut key = data_prefix("vt");
            key.extend_from_slice(&i.to_le_bytes());
            let row = vec![
                Value::Int(i),
                Value::Vector(vec![i as f32, (i % 7) as f32, 1.0, 0.5]),
            ];
            puts.push((key, bincode::serialize(&row).unwrap()));
        }
        sess.commit_write(puts, vec![]).await.unwrap();

        let reg = VectorRegistry::new();
        // `build_count` is a process-global counter (other tests may bump it in
        // parallel), so assert a tolerant total at the end: an incremental
        // implementation does ONE build here, a rebuild-every-query one does four.
        let before = build_count();
        let _ = reg.get(&sess, &def, 1, Metric::L2).await.unwrap();

        // INSERT a distinctive vector; the next query must find it.
        put(&sess, 1000, [42.5, 2.5, 1.0, 0.5]).await;
        let c = reg.get(&sess, &def, 1, Metric::L2).await.unwrap();
        let hit = c.search_keys(&[42.5, 2.5, 1.0, 0.5], 1, 64);
        assert_eq!(id_of(&hit[0].0), 1000, "new row is searchable");

        // UPDATE the vector in place; the query must reflect the new location.
        put(&sess, 1000, [180.5, 4.5, 1.0, 0.5]).await;
        let c = reg.get(&sess, &def, 1, Metric::L2).await.unwrap();
        let near_old = c.search_keys(&[42.5, 2.5, 1.0, 0.5], 1, 64);
        assert_ne!(
            id_of(&near_old[0].0),
            1000,
            "old vector location no longer id 1000"
        );
        let near_new = c.search_keys(&[180.5, 4.5, 1.0, 0.5], 1, 64);
        assert_eq!(
            id_of(&near_new[0].0),
            1000,
            "updated vector found at new location"
        );

        // DELETE it; it must disappear from results.
        del(&sess, 1000).await;
        let c = reg.get(&sess, &def, 1, Metric::L2).await.unwrap();
        let after_del = c.search_keys(&[180.5, 4.5, 1.0, 0.5], 5, 64);
        assert!(
            after_del.iter().all(|(k, _)| id_of(k) != 1000),
            "deleted row must not appear"
        );

        // Incremental: at most one build here (+ tolerate one concurrent test's
        // build), never four (one rebuild per query).
        assert!(
            build_count() - before <= 2,
            "should reconcile incrementally, not rebuild every query (builds={})",
            build_count() - before
        );

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn persisted_index_survives_cold_start() {
        let _serial = BUILD_TEST_LOCK.lock().await;
        let (db, path) = temp_db();
        let sess = Session::new(db, std::sync::Arc::new(LockManager::new()));
        let def = vector_table();
        let mut puts = Vec::new();
        for i in 0..300i64 {
            let mut key = data_prefix("vt");
            key.extend_from_slice(&i.to_le_bytes());
            let row = vec![
                Value::Int(i),
                Value::Vector(vec![i as f32, (i % 7) as f32, 1.0, 0.5]),
            ];
            puts.push((key, bincode::serialize(&row).unwrap()));
        }
        sess.commit_write(puts, vec![]).await.unwrap();

        // First registry: builds the graph and persists a snapshot.
        let reg1 = VectorRegistry::new();
        let before = build_count();
        let _ = reg1.get(&sess, &def, 1, Metric::L2).await.unwrap();
        assert_eq!(build_count() - before, 1, "one build on first use");
        let built = build_count();

        // Simulate a restart: a fresh registry (empty in-memory) over the same
        // data must load the persisted graph, NOT rebuild it.
        let reg2 = VectorRegistry::new();
        let c = reg2.get(&sess, &def, 1, Metric::L2).await.unwrap();
        assert_eq!(
            build_count(),
            built,
            "cold start reused the on-disk snapshot instead of rebuilding"
        );
        // And it is correct.
        let hit = c.search_keys(&[42.0, 0.0, 1.0, 0.5], 1, 64);
        assert_eq!(id_of(&hit[0].0), 42, "loaded index returns correct nearest");

        // Cleanup the .edb and the sibling .vidx dir.
        let _ = std::fs::remove_file(&path);
        let mut vidx = path.clone().into_os_string();
        vidx.push(".vidx");
        let _ = std::fs::remove_dir_all(std::path::PathBuf::from(vidx));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_stale_queries_rebuild_index_only_once() {
        let _serial = BUILD_TEST_LOCK.lock().await;
        let (db, path) = temp_db();
        let sess = std::sync::Arc::new(Session::new(db, std::sync::Arc::new(LockManager::new())));
        let def = std::sync::Arc::new(vector_table());

        // Seed vectors directly under the data prefix.
        let mut puts = Vec::new();
        for i in 0..500i64 {
            let mut key = data_prefix("vt");
            key.extend_from_slice(&i.to_le_bytes());
            let row = vec![
                Value::Int(i),
                Value::Vector(vec![i as f32, (i % 7) as f32, 1.0, 0.5]),
            ];
            puts.push((key, bincode::serialize(&row).unwrap()));
        }
        sess.commit_write(puts, vec![]).await.unwrap();

        let reg = VectorRegistry::new();
        let before = build_count();

        // Fire many concurrent queries at a cold (missing) cache: without
        // single-flight every task would rebuild the whole index.
        let mut handles = Vec::new();
        for _ in 0..32 {
            let reg = reg.clone();
            let sess = sess.clone();
            let def = def.clone();
            handles.push(tokio::spawn(async move {
                reg.get(&sess, &def, 1, Metric::L2).await.unwrap()
            }));
        }
        let mut results = Vec::new();
        for h in handles {
            results.push(h.await.unwrap());
        }

        // Exactly one build, and every caller saw the same fully-populated index.
        assert_eq!(build_count() - before, 1, "single-flight must rebuild once");
        for r in &results {
            assert_eq!(
                r.state.read().unwrap().key_node.len(),
                500,
                "index must hold all 500 rows"
            );
        }
        // A subsequent call hits the fresh cache (no extra build).
        let _ = reg.get(&sess, &def, 1, Metric::L2).await.unwrap();
        assert_eq!(build_count() - before, 1);

        let _ = std::fs::remove_file(path);
    }
}
