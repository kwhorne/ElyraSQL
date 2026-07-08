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

mod db;
pub use db::Db;

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

    /// Fetch many values in a single read transaction. Output aligns with
    /// `keys` (index i -> value for keys[i], `None` if absent). Avoids the
    /// per-call transaction overhead of looping [`Storage::get`].
    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let rtx = self.db.begin_read().map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(
                t.get(k.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?
                    .map(|v| v.value().to_vec()),
            );
        }
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

    /// Cursor-based range scan. Returns up to `limit` key/value pairs whose
    /// key starts with `prefix` and is strictly greater than `after` (when
    /// given). This is the primitive behind streaming table scans: callers
    /// pass the last-seen key back as `after` to fetch the next batch, so
    /// memory stays bounded regardless of table size.
    pub fn scan_batch(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rtx = self.db.begin_read().map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;

        // Start just after the cursor, or at the prefix itself.
        let lower: Vec<u8> = match after {
            Some(a) => {
                let mut k = a.to_vec();
                k.push(0); // smallest key strictly greater than `a`
                k
            }
            None => prefix.to_vec(),
        };

        let mut out = Vec::with_capacity(limit.min(1024));
        let range = t
            .range(lower.as_slice()..)
            .map_err(|e| Error::Storage(e.to_string()))?;
        for item in range {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            let key = k.value().to_vec();
            if !key.starts_with(prefix) {
                break; // left the table's keyspace — done
            }
            out.push((key, v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Scan keys in `[start, end)` (end exclusive; unbounded when `None`), up
    /// to `limit` pairs. Backs ordered range lookups on the clustered data
    /// keyspace and on secondary indexes.
    pub fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::ops::Bound;
        let rtx = self.db.begin_read().map_err(|e| Error::Storage(e.to_string()))?;
        let t = rtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
        let lower = Bound::Included(start);
        let upper = match end {
            Some(e) => Bound::Excluded(e),
            None => Bound::Unbounded,
        };
        let mut out = Vec::with_capacity(limit.min(1024));
        for item in t.range::<&[u8]>((lower, upper)).map_err(|e| Error::Storage(e.to_string()))? {
            let (k, v) = item.map_err(|e| Error::Storage(e.to_string()))?;
            out.push((k.value().to_vec(), v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Atomically apply many puts and deletes in a single write transaction.
    /// This is the primitive the group-commit writer uses to fold many
    /// pending writes into one commit under high write traffic.
    pub fn apply(&self, puts: &[(Vec<u8>, Vec<u8>)], deletes: &[Vec<u8>]) -> Result<()> {
        let wtx = self.db.begin_write().map_err(|e| Error::Storage(e.to_string()))?;
        {
            let mut t = wtx.open_table(KV).map_err(|e| Error::Storage(e.to_string()))?;
            // Deletes first, then puts: a key present in both (e.g. an index
            // entry that is unchanged across an UPDATE) ends up kept.
            for k in deletes {
                t.remove(k.as_slice()).map_err(|e| Error::Storage(e.to_string()))?;
            }
            for (k, v) in puts {
                t.insert(k.as_slice(), v.as_slice())
                    .map_err(|e| Error::Storage(e.to_string()))?;
            }
        }
        wtx.commit().map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
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
