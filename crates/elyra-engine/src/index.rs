//! Secondary indexes.
//!
//! Each index entry lives in the one file under:
//!
//! ```text
//! index::<table>::<index>::<enc(col_value)>\0<clustered_key>  →  <data_key>
//! ```
//!
//! The `enc(col_value)` prefix is order-preserving, so equality and range
//! lookups are B-tree range scans. The trailing clustered key keeps entries
//! unique for non-unique indexes and lets us walk straight to the row.

use elyra_core::{Result, Value};
use elyra_storage::Db;

use crate::catalog::{data_prefix, IndexDef, TableDef};
use crate::keyenc;

fn index_prefix(table: &str, index: &str) -> Vec<u8> {
    format!("index::{table}::{index}::").into_bytes()
}

/// Prefix for all entries with a given column value (equality lookup).
fn value_prefix(table: &str, index: &str, value: &Value) -> Result<Vec<u8>> {
    let mut k = index_prefix(table, index);
    k.extend_from_slice(&keyenc::encode(value)?);
    k.push(0);
    Ok(k)
}

/// Full entry key for a specific row.
fn entry_key(table: &str, index: &str, value: &Value, data_key: &[u8]) -> Result<Vec<u8>> {
    let mut k = value_prefix(table, index, value)?;
    // Append the clustered part of the data key to disambiguate rows.
    let clustered = &data_key[data_prefix(table).len()..];
    k.extend_from_slice(clustered);
    Ok(k)
}

/// Index entries (key, value) for one row. Skips columns whose value cannot
/// be indexed (NULL and non-scalar types).
pub fn entries_for_row(
    def: &TableDef,
    row: &[Value],
    data_key: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for idx in &def.indexes {
        let v = &row[idx.col];
        if v.is_null() {
            continue;
        }
        // Only scalar, order-encodable values get indexed.
        if keyenc::encode(v).is_err() {
            continue;
        }
        out.push((entry_key(&def.name, &idx.name, v, data_key)?, data_key.to_vec()));
    }
    Ok(out)
}

/// Just the index entry keys for a row (used when deleting).
pub fn entry_keys_for_row(def: &TableDef, row: &[Value], data_key: &[u8]) -> Result<Vec<Vec<u8>>> {
    Ok(entries_for_row(def, row, data_key)?
        .into_iter()
        .map(|(k, _)| k)
        .collect())
}

/// Find the index (if any) defined on a given column.
pub fn index_on<'a>(def: &'a TableDef, col: usize) -> Option<&'a IndexDef> {
    def.indexes.iter().find(|i| i.col == col)
}

/// Equality lookup: return the data keys of all rows where the indexed
/// column equals `value`.
pub async fn lookup_eq(
    db: &Db,
    table: &str,
    index: &IndexDef,
    value: &Value,
) -> Result<Vec<Vec<u8>>> {
    let prefix = value_prefix(table, &index.name, value)?;
    let mut cursor: Option<Vec<u8>> = None;
    let mut keys = Vec::new();
    loop {
        let chunk = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if chunk.is_empty() {
            break;
        }
        let last = chunk.len() < 4096;
        cursor = chunk.last().map(|(k, _)| k.clone());
        for (_, data_key) in chunk {
            keys.push(data_key);
        }
        if last {
            break;
        }
    }
    Ok(keys)
}
