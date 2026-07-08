//! Secondary indexes (single- or multi-column).
//!
//! Entry layout in the one file:
//!
//! ```text
//! index::<table>::<index>::<enc(col_values)>\0<clustered_key>  →  <data_key>
//! ```
//!
//! `enc(col_values)` is the order-preserving composite encoding of the indexed
//! columns, so equality and (single-column) range lookups are B-tree scans.

use elyra_core::{Result, Value};
use crate::session::Session;

use crate::catalog::{data_prefix, IndexDef, TableDef};
use crate::keyenc;

fn index_prefix(table: &str, index: &str) -> Vec<u8> {
    format!("index::{table}::{index}::").into_bytes()
}

/// Prefix for all entries with a given tuple of column values (equality).
fn value_prefix(table: &str, index: &str, values: &[Value]) -> Result<Vec<u8>> {
    let mut k = index_prefix(table, index);
    k.extend_from_slice(&keyenc::encode_key(values)?);
    k.push(0);
    Ok(k)
}

fn entry_key(table: &str, index: &str, values: &[Value], data_key: &[u8]) -> Result<Vec<u8>> {
    let mut k = value_prefix(table, index, values)?;
    let clustered = &data_key[data_prefix(table).len()..];
    k.extend_from_slice(clustered);
    Ok(k)
}

/// Index entries (key, value) for one row. Skips an index when any of its
/// columns is NULL or non-encodable.
pub fn entries_for_row(
    def: &TableDef,
    row: &[Value],
    data_key: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for idx in &def.indexes {
        if idx.vector {
            continue; // vector indexes are maintained separately
        }
        let values: Vec<Value> = idx.cols.iter().map(|&c| row[c].clone()).collect();
        if values.iter().any(|v| v.is_null()) || keyenc::encode_key(&values).is_err() {
            continue;
        }
        out.push((entry_key(&def.name, &idx.name, &values, data_key)?, data_key.to_vec()));
    }
    Ok(out)
}

pub fn entry_keys_for_row(def: &TableDef, row: &[Value], data_key: &[u8]) -> Result<Vec<Vec<u8>>> {
    Ok(entries_for_row(def, row, data_key)?.into_iter().map(|(k, _)| k).collect())
}

/// A single-column B-tree index on `col`, if one exists (used by the
/// single-column eq/range fast paths).
pub fn index_on<'a>(def: &'a TableDef, col: usize) -> Option<&'a IndexDef> {
    def.indexes.iter().find(|i| !i.vector && i.single_col() == Some(col))
}

/// Equality lookup on the full set of indexed columns: data keys of rows whose
/// indexed tuple equals `values`.
pub async fn lookup_eq(
    db: &Session,
    table: &str,
    index: &IndexDef,
    values: &[Value],
) -> Result<Vec<Vec<u8>>> {
    let prefix = value_prefix(table, &index.name, values)?;
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

/// Range lookup on a single-column index. Bounds are `(value, inclusive)`.
pub async fn lookup_range(
    db: &Session,
    table: &str,
    index: &IndexDef,
    lo: Option<(&Value, bool)>,
    hi: Option<(&Value, bool)>,
) -> Result<Vec<Vec<u8>>> {
    let prefix = index_prefix(table, &index.name);

    let mut start = match lo {
        Some((v, incl)) => {
            let mut b = prefix.clone();
            b.extend_from_slice(&keyenc::encode(v)?);
            if !incl {
                b.push(0x01);
            }
            b
        }
        None => prefix.clone(),
    };
    let end = match hi {
        Some((v, incl)) => {
            let mut b = prefix.clone();
            b.extend_from_slice(&keyenc::encode(v)?);
            if incl {
                b.push(0x01);
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

/// Smallest key strictly greater than every key with `prefix`.
pub fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
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
