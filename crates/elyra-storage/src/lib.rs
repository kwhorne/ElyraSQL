//! ElyraSQL storage engine.
//!
//! Everything ElyraSQL persists lives in **one file**. Internally that file
//! is a `redb` database (ACID, MVCC, crash-safe) partitioned into logical
//! namespaces via table-name prefixes:
//!
//! | Namespace prefix | Contents                          |
//! |------------------|-----------------------------------|
//! | `catalog::`      | table/schema definitions          |
//! | `data::<table>`  | row data, keyed by primary/rowid  |
//! | `index::<name>`  | secondary + vector index payloads |
//! | `meta::`         | server metadata, version, config  |
//!
//! Callers only ever see ElyraSQL types; `redb` never leaks past this crate.

use std::path::Path;

use elyra_core::{Error, Result};
use redb::{Database, ReadableTable, TableDefinition};

/// Key/value blob table. All logical namespaces share this definition; the
/// key carries the namespace prefix so we keep a single flat keyspace and
/// therefore a single file.
const KV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("elyra_kv");

/// Handle to an ElyraSQL storage file.
pub struct Storage {
    db: Database,
}

impl Storage {
    /// Open (or create) the single ElyraSQL database file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = Database::create(path).map_err(|e| Error::Storage(e.to_string()))?;
        // Ensure the KV table exists so first reads don't fail.
        let wtx = db.begin_write().map_err(|e| Error::Storage(e.to_string()))?;
        {
            wtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { db })
    }

    /// Fully in-memory database (tests, ephemeral sessions).
    pub fn in_memory() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let wtx = db.begin_write().map_err(|e| Error::Storage(e.to_string()))?;
        {
            wtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { db })
    }

    /// Store a value under a namespaced key.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let wtx = self.db.begin_write().map_err(|e| Error::Storage(e.to_string()))?;
        {
            let mut t = wtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
            t.insert(key, value).map_err(|e| Error::Storage(e.to_string()))?;
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    /// Fetch a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let rtx = self.db.begin_read().map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
        let out = t
            .get(key)
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(|v| v.value().to_vec());
        Ok(out)
    }

    /// Delete a key. Returns whether something was removed.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let wtx = self.db.begin_write().map_err(|e| Error::Storage(e.to_string()))?;
        let existed;
        {
            let mut t = wtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
            existed = t.remove(key).map_err(|e| Error::Storage(e.to_string()))?.is_some();
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(existed)
    }

    /// Iterate all key/value pairs whose key starts with `prefix`.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rtx = self.db.begin_read().map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
        let mut out = Vec::new();
        for item in t.iter().map_err(|e| Error::Storage(e.to_string()))? {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let key = k.value().to_vec();
            if key.starts_with(prefix) {
                out.push((key, v.value().to_vec()));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_delete_roundtrip() {
        let s = Storage::in_memory().unwrap();
        s.put(b"data::t::1", b"hello").unwrap();
        assert_eq!(s.get(b"data::t::1").unwrap().as_deref(), Some(&b"hello"[..]));
        assert!(s.delete(b"data::t::1").unwrap());
        assert_eq!(s.get(b"data::t::1").unwrap(), None);
    }

    #[test]
    fn prefix_scan_isolates_namespaces() {
        let s = Storage::in_memory().unwrap();
        s.put(b"data::t::1", b"a").unwrap();
        s.put(b"data::t::2", b"b").unwrap();
        s.put(b"catalog::t", b"schema").unwrap();
        assert_eq!(s.scan_prefix(b"data::t::").unwrap().len(), 2);
        assert_eq!(s.scan_prefix(b"catalog::").unwrap().len(), 1);
    }
}
