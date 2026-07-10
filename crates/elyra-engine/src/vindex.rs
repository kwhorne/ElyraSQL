//! In-memory vector-index (HNSW) registry with rebuild-when-stale caching.
//!
//! ElyraSQL keeps the authoritative vectors in the single file. The HNSW graph
//! is a cached, memory-resident snapshot keyed by `table.column`. Whenever the
//! table's write counter advances, the cached index is considered stale and
//! rebuilt from storage on the next query. This keeps ANN results correct
//! without any incremental graph maintenance.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::Mutex as AsyncMutex;

use crate::session::Session;
use elyra_core::{Error, Result, Value};
use elyra_vector::{Hnsw, Metric};

use crate::catalog::{data_prefix, wcount_key, TableDef};

/// A cached HNSW plus the data key for each graph node.
pub struct CachedIndex {
    pub wcount: u64,
    pub keys: Vec<Vec<u8>>,
    pub index: Hnsw,
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
        if let Some(cached) = self.inner.read().unwrap().get(&key).cloned() {
            if cached.wcount == wcount {
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
        if let Some(cached) = self.inner.read().unwrap().get(&key).cloned() {
            if cached.wcount == wcount {
                return Ok(cached);
            }
        }

        // (Re)build from storage.
        let built = Arc::new(build(db, def, col, metric, wcount).await?);
        self.inner.write().unwrap().insert(key, built.clone());
        Ok(built)
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

async fn build(
    db: &Session,
    def: &TableDef,
    col: usize,
    metric: Metric,
    wcount: u64,
) -> Result<CachedIndex> {
    BUILDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let prefix = data_prefix(&def.name);
    let mut cursor: Option<Vec<u8>> = None;
    let mut vectors: Vec<Vec<f32>> = Vec::new();
    let mut keys: Vec<Vec<u8>> = Vec::new();
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
                    vectors.push(vec.clone());
                    keys.push(k);
                }
            }
        }
        if last {
            break;
        }
    }

    let index = Hnsw::build(vectors, dim, metric);
    Ok(CachedIndex {
        wcount,
        keys,
        index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::TableDef;
    use crate::lockmgr::LockManager;
    use elyra_core::{ColumnDef, ColumnType, Schema};
    use elyra_storage::Db;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_stale_queries_rebuild_index_only_once() {
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
            assert_eq!(r.keys.len(), 500);
        }
        // A subsequent call hits the fresh cache (no extra build).
        let _ = reg.get(&sess, &def, 1, Metric::L2).await.unwrap();
        assert_eq!(build_count() - before, 1);

        let _ = std::fs::remove_file(path);
    }
}
