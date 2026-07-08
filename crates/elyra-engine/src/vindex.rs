//! In-memory vector-index (HNSW) registry with rebuild-when-stale caching.
//!
//! ElyraSQL keeps the authoritative vectors in the single file. The HNSW graph
//! is a cached, memory-resident snapshot keyed by `table.column`. Whenever the
//! table's write counter advances, the cached index is considered stale and
//! rebuilt from storage on the next query. This keeps ANN results correct
//! without any incremental graph maintenance.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use elyra_core::{Error, Result, Value};
use elyra_storage::Db;
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
}

impl VectorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch a fresh cached index for `table.col`, building it from storage if
    /// missing or stale.
    pub async fn get(
        &self,
        db: &Db,
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

        // (Re)build from storage.
        let built = Arc::new(build(db, def, col, metric, wcount).await?);
        self.inner.write().unwrap().insert(key, built.clone());
        Ok(built)
    }
}

pub async fn read_wcount(db: &Db, table: &str) -> Result<u64> {
    Ok(match db.get(wcount_key(table)).await? {
        Some(b) if b.len() == 8 => u64::from_le_bytes(b.try_into().expect("len 8")),
        _ => 0,
    })
}

async fn build(
    db: &Db,
    def: &TableDef,
    col: usize,
    metric: Metric,
    wcount: u64,
) -> Result<CachedIndex> {
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
    Ok(CachedIndex { wcount, keys, index })
}
