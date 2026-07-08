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

/// Range lookup: data keys for rows whose indexed column is within the given
/// bounds. Each bound is `(value, inclusive)`. Entries are ordered, so this is
/// a B-tree range scan.
pub async fn lookup_range(
    db: &Db,
    table: &str,
    index: &IndexDef,
    lo: Option<(&Value, bool)>,
    hi: Option<(&Value, bool)>,
) -> Result<Vec<Vec<u8>>> {
    let prefix = index_prefix(table, &index.name);

    // Lower start (inclusive byte position).
    let mut start = match lo {
        Some((v, incl)) => {
            let mut b = prefix.clone();
            b.extend_from_slice(&keyenc::encode(v)?);
            if !incl {
                b.push(0x01); // skip entries equal to v (which are ..\0..)
            }
            b
        }
        None => prefix.clone(),
    };

    // Upper end (exclusive byte position).
    let end = match hi {
        Some((v, incl)) => {
            let mut b = prefix.clone();
            b.extend_from_slice(&keyenc::encode(v)?);
            if incl {
                b.push(0x01); // include entries equal to v, exclude the next
            }
            b
        }
        None => prefix_upper_bound(&prefix),
    };

    let mut keys = Vec::new();
    loop {
        let batch = db.scan_range(start.clone(), Some(end.clone()), 4096).await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 4096;
        // Next start is strictly after the last key seen.
        start = batch.last().map(|(k, _)| {
            let mut n = k.clone();
            n.push(0);
            n
        }).unwrap();
        for (_, data_key) in batch {
            keys.push(data_key);
        }
        if last {
            break;
        }
    }
    Ok(keys)
}

/// Smallest key strictly greater than every key with `prefix` (increment the
/// last byte below 0xFF).
pub fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last().copied() {
        if last < 0xFF {
            *end.last_mut().unwrap() = last + 1;
            return end;
        }
        end.pop();
    }
    end // all 0xFF -> empty means unbounded; caller guards by table prefix
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
