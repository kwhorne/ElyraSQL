//! Secondary indexes (single- or multi-column).
//!
//! Entry layout in the one file:
//!
//! ```text
//! index::<table>::<index>::<enc(col_values)>\0<clustered_key>  →  <data_key>
//! ```
//!
//! `enc(col_values)` is the order-preserving composite encoding of the indexed
//! columns, so equality and left-prefix range lookups are B-tree scans.

use crate::session::Session;
use elyra_core::{Collation, Result, Value};

use crate::catalog::{data_prefix, IndexDef, TableDef};
use crate::ft;
use crate::keyenc;

/// Entry keys for a full-text index over `row`: one per unique stemmed term of
/// the indexed text columns, suffixed with the clustered key.
fn fulltext_entry_keys(
    def: &TableDef,
    idx: &IndexDef,
    row: &[Value],
    data_key: &[u8],
) -> Vec<Vec<u8>> {
    let mut text = String::new();
    for &c in &idx.cols {
        if let Some(s) = row.get(c).and_then(|v| v.to_wire_string()) {
            text.push(' ');
            text.push_str(&s);
        }
    }
    let clustered = &data_key[data_prefix(&def.name).len()..];
    ft::unique_terms(&text)
        .into_iter()
        .map(|term| {
            let mut k = format!("index::{}::{}::", def.name, idx.name).into_bytes();
            k.extend_from_slice(term.as_bytes());
            k.push(0);
            k.extend_from_slice(clustered);
            k
        })
        .collect()
}

/// Data keys of rows whose full-text index contains `term` (already stemmed).
pub async fn fulltext_lookup(
    db: &Session,
    table: &str,
    index: &str,
    term: &str,
) -> Result<Vec<Vec<u8>>> {
    let mut prefix = format!("index::{table}::{index}::").into_bytes();
    prefix.extend_from_slice(term.as_bytes());
    prefix.push(0);
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

/// A stored key/value pair.
type Entry = (Vec<u8>, Vec<u8>);

fn index_prefix(table: &str, index: &str) -> Vec<u8> {
    format!("index::{table}::{index}::").into_bytes()
}

/// Public key prefix covering every entry of a secondary index, for an ordered
/// index walk (`ORDER BY <indexed col> LIMIT`). Entries under this prefix are in
/// ascending indexed-value order (the key encoding is order-preserving).
pub fn index_scan_prefix(table: &str, index: &str) -> Vec<u8> {
    index_prefix(table, index)
}

/// Key prefix for a single-column index's NULL-keyed entries (see
/// [`catalog::IndexDef::indexes_nulls`]). Each entry is `prefix ++ clustered pk`
/// with the row's data key as value, so walking this prefix yields the NULL rows
/// ordered by primary key (the stable-pagination tiebreaker for the NULL block).
pub fn indexnull_scan_prefix(table: &str, index: &str) -> Vec<u8> {
    format!("indexnull::{table}::{index}::").into_bytes()
}

/// Entry key for a NULL-keyed row in a single-column index: the NULL prefix
/// followed by the row's clustered primary key (so NULLs never collide and are
/// ordered by PK).
fn null_entry_key(table: &str, index: &str, data_key: &[u8]) -> Vec<u8> {
    let mut k = indexnull_scan_prefix(table, index);
    k.extend_from_slice(&data_key[data_prefix(table).len()..]);
    k
}

/// Prefix for all entries with a given tuple of column values (equality),
/// honoring each column's collation.
fn value_prefix(
    table: &str,
    index: &str,
    values: &[Value],
    colls: &[Collation],
) -> Result<Vec<u8>> {
    let mut k = index_prefix(table, index);
    k.extend_from_slice(&keyenc::encode_key_coll(values, colls)?);
    k.push(0);
    Ok(k)
}

fn value_prefix_encoded(table: &str, index: &str, encoded: &[u8]) -> Vec<u8> {
    let mut key = index_prefix(table, index);
    key.extend_from_slice(encoded);
    key.push(0);
    key
}

fn entry_key_encoded(
    table: &str,
    index: &str,
    encoded: &[u8],
    clustered_key: &[u8],
    unique: bool,
) -> Vec<u8> {
    let mut key = value_prefix_encoded(table, index, encoded);
    if !unique {
        key.extend_from_slice(clustered_key);
    }
    key
}

/// The probe key for a unique index's value tuple (== its entry key). A stored
/// value at this key means some row already holds the tuple.
pub fn unique_probe_key(
    table: &str,
    index: &str,
    values: &[Value],
    colls: &[Collation],
) -> Result<Vec<u8>> {
    value_prefix(table, index, values, colls)
}

/// Split a row's index entries into (non-unique, unique). Unique entries
/// enforce uniqueness by colliding on duplicate value tuples. NULL tuples are
/// skipped in both (multiple NULLs are allowed in a unique index).
pub fn partition_entries_for_row(
    def: &TableDef,
    row: &[Value],
    data_key: &[u8],
) -> Result<(Vec<Entry>, Vec<Entry>)> {
    let mut nonuniq = Vec::new();
    let mut uniq = Vec::new();
    let clustered_key = &data_key[b"data::".len() + def.name.len() + b"::".len()..];
    for idx in &def.indexes {
        if idx.vector {
            continue;
        }
        if idx.fulltext {
            for k in fulltext_entry_keys(def, idx, row, data_key) {
                nonuniq.push((k, data_key.to_vec()));
            }
            continue;
        }
        let has_null = idx.cols.iter().any(|&column| row[column].is_null());
        let encoded = (!has_null)
            .then(|| keyenc::encode_columns_coll(row, &idx.cols, &idx.col_collations).ok())
            .flatten();
        let Some(encoded) = encoded else {
            // Single-column NULL-indexing: record the NULL-keyed row under the
            // `indexnull::` keyspace (never unique -- NULLs don't collide).
            if idx.indexes_nulls && idx.cols.len() == 1 && row[idx.cols[0]].is_null() {
                nonuniq.push((
                    null_entry_key(&def.name, &idx.name, data_key),
                    data_key.to_vec(),
                ));
            }
            continue;
        };
        let entry = (
            entry_key_encoded(&def.name, &idx.name, &encoded, clustered_key, idx.unique),
            data_key.to_vec(),
        );
        if idx.unique {
            uniq.push(entry);
        } else {
            nonuniq.push(entry);
        }
    }
    Ok((nonuniq, uniq))
}

/// Probe keys for every unique index over `row` (skipping NULL tuples), for
/// existence checks on paths that cannot use writer-side collision detection.
pub fn unique_probe_keys(def: &TableDef, row: &[Value]) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for idx in &def.indexes {
        if idx.vector || !idx.unique {
            continue;
        }
        let has_null = idx.cols.iter().any(|&column| row[column].is_null());
        let encoded = (!has_null)
            .then(|| keyenc::encode_columns_coll(row, &idx.cols, &idx.col_collations).ok())
            .flatten();
        let Some(encoded) = encoded else {
            continue;
        };
        out.push(value_prefix_encoded(&def.name, &idx.name, &encoded));
    }
    Ok(out)
}

/// True if the table has any enforceable unique secondary index.
pub fn has_unique(def: &TableDef) -> bool {
    def.indexes.iter().any(|i| i.unique && !i.vector)
}

/// Index entries (key, value) for one row. Skips an index when any of its
/// columns is NULL or non-encodable.
pub fn entries_for_row(
    def: &TableDef,
    row: &[Value],
    data_key: &[u8],
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    let clustered_key = &data_key[b"data::".len() + def.name.len() + b"::".len()..];
    for idx in &def.indexes {
        if idx.vector {
            continue; // vector indexes are maintained separately
        }
        if idx.fulltext {
            for k in fulltext_entry_keys(def, idx, row, data_key) {
                out.push((k, data_key.to_vec()));
            }
            continue;
        }
        let has_null = idx.cols.iter().any(|&column| row[column].is_null());
        let encoded = (!has_null)
            .then(|| keyenc::encode_columns_coll(row, &idx.cols, &idx.col_collations).ok())
            .flatten();
        let Some(encoded) = encoded else {
            // Single-column NULL-indexing (see `partition_entries_for_row`).
            if idx.indexes_nulls && idx.cols.len() == 1 && row[idx.cols[0]].is_null() {
                out.push((
                    null_entry_key(&def.name, &idx.name, data_key),
                    data_key.to_vec(),
                ));
            }
            continue;
        };
        out.push((
            entry_key_encoded(&def.name, &idx.name, &encoded, clustered_key, idx.unique),
            data_key.to_vec(),
        ));
    }
    Ok(out)
}

pub fn entry_keys_for_row(def: &TableDef, row: &[Value], data_key: &[u8]) -> Result<Vec<Vec<u8>>> {
    Ok(entries_for_row(def, row, data_key)?
        .into_iter()
        .map(|(k, _)| k)
        .collect())
}

/// A single-column B-tree index on `col`, if one exists (used by the
/// single-column eq/range fast paths).
pub fn index_on(def: &TableDef, col: usize) -> Option<&IndexDef> {
    def.indexes
        .iter()
        .find(|i| !i.vector && i.single_col() == Some(col))
}

/// Equality lookup on the full set of indexed columns: data keys of rows whose
/// indexed tuple equals `values`.
pub async fn lookup_eq(
    db: &Session,
    table: &str,
    index: &IndexDef,
    values: &[Value],
) -> Result<Vec<Vec<u8>>> {
    let prefix = value_prefix(table, &index.name, values, &index.col_collations)?;
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

    let coll = index.col_collations.first().copied().unwrap_or_default();
    let mut start = match lo {
        Some((v, incl)) => {
            let mut b = prefix.clone();
            b.extend_from_slice(&keyenc::encode_coll(v, coll)?);
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
            b.extend_from_slice(&keyenc::encode_coll(v, coll)?);
            if incl {
                b.push(0x01);
            }
            b
        }
        None => prefix_upper_bound(&prefix),
    };

    let mut keys = Vec::new();
    loop {
        let batch = db
            .scan_range(start.clone(), Some(end.clone()), 4096)
            .await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 4096;
        start = batch
            .last()
            .map(|(k, _)| {
                let mut n = k.clone();
                n.push(0);
                n
            })
            .unwrap();
        for (_, data_key) in batch {
            keys.push(data_key);
        }
        if last {
            break;
        }
    }
    Ok(keys)
}

/// Range lookup on the column immediately following an equality-constrained
/// leading prefix of a composite index. Bounds are `(value, inclusive)`.
///
/// `prefix_values` must correspond to the first N indexed columns and the
/// bounds to column N. Component encodings are self-delimiting, so the encoded
/// tuple prefix is also an exact byte prefix for every matching index entry.
pub async fn lookup_prefix_range(
    db: &Session,
    table: &str,
    index: &IndexDef,
    prefix_values: &[Value],
    lo: Option<(&Value, bool)>,
    hi: Option<(&Value, bool)>,
) -> Result<Vec<Vec<u8>>> {
    let Some((mut start, end)) = prefix_range_scan_bounds(table, index, prefix_values, lo, hi)?
    else {
        return Ok(Vec::new());
    };

    let mut keys = Vec::new();
    loop {
        let batch = db
            .scan_range(start.clone(), Some(end.clone()), 4096)
            .await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 4096;
        start = batch
            .last()
            .map(|(key, _)| {
                let mut next = key.clone();
                next.push(0);
                next
            })
            .expect("a non-empty range batch has a final key");
        keys.extend(batch.into_iter().map(|(_, data_key)| data_key));
        if last {
            break;
        }
    }
    Ok(keys)
}

pub(crate) fn prefix_range_scan_bounds(
    table: &str,
    index: &IndexDef,
    prefix_values: &[Value],
    lo: Option<(&Value, bool)>,
    hi: Option<(&Value, bool)>,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut equality_prefix = index_prefix(table, &index.name);
    equality_prefix.extend_from_slice(&keyenc::encode_key_coll(
        prefix_values,
        &index.col_collations,
    )?);
    let range_collation = index
        .col_collations
        .get(prefix_values.len())
        .copied()
        .unwrap_or_default();

    let bound_prefix = |value: &Value| -> Result<Vec<u8>> {
        let mut bound = equality_prefix.clone();
        bound.extend_from_slice(&keyenc::encode_coll(value, range_collation)?);
        Ok(bound)
    };
    let start = match lo {
        Some((value, true)) => bound_prefix(value)?,
        Some((value, false)) => prefix_upper_bound(&bound_prefix(value)?),
        None => equality_prefix.clone(),
    };
    let end = match hi {
        Some((value, true)) => prefix_upper_bound(&bound_prefix(value)?),
        Some((value, false)) => bound_prefix(value)?,
        None => prefix_upper_bound(&equality_prefix),
    };
    if start >= end {
        return Ok(None);
    }
    Ok(Some((start, end)))
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
