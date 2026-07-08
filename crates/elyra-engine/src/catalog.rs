//! Table catalog: schema definitions persisted in the single DB file.
//!
//! Keyspace layout (all inside the one file):
//! * `catalog::<table>`     → serialized [`TableDef`]
//! * `meta::rowid::<table>` → u64 counter for hidden rowids
//! * `data::<table>::<key>` → serialized row (`Vec<Value>`)

use elyra_core::{Error, Result, Schema};
use crate::session::Session;
use serde::{Deserialize, Serialize};

/// A secondary index over one or more columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    /// Schema indices of the indexed columns (in key order).
    pub cols: Vec<usize>,
    pub unique: bool,
    /// A vector (HNSW ANN) index rather than a B-tree secondary index.
    #[serde(default)]
    pub vector: bool,
}

impl IndexDef {
    /// The single indexed column, if this is a one-column index.
    pub fn single_col(&self) -> Option<usize> {
        (self.cols.len() == 1).then(|| self.cols[0])
    }
}

/// Definition of a table. `pk_cols` are the schema indices of the (possibly
/// composite) primary key, clustered in key order; empty means a hidden rowid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub schema: Schema,
    pub pk_cols: Vec<usize>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
}

impl TableDef {
    pub fn has_pk(&self) -> bool {
        !self.pk_cols.is_empty()
    }
}

pub fn catalog_key(table: &str) -> Vec<u8> {
    format!("catalog::{table}").into_bytes()
}

pub fn rowid_key(table: &str) -> Vec<u8> {
    format!("meta::rowid::{table}").into_bytes()
}

/// Monotonic write counter per table; bumped on every mutation. Used to
/// invalidate cached in-memory indexes (e.g. the vector HNSW).
pub fn wcount_key(table: &str) -> Vec<u8> {
    format!("meta::wcount::{table}").into_bytes()
}

/// Prefix under which all rows of a table live.
pub fn data_prefix(table: &str) -> Vec<u8> {
    format!("data::{table}::").into_bytes()
}

/// Full data key = prefix ++ encoded clustered key.
pub fn data_key(table: &str, encoded: &[u8]) -> Vec<u8> {
    let mut k = data_prefix(table);
    k.extend_from_slice(encoded);
    k
}

impl TableDef {
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Catalog(e.to_string()))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| Error::Catalog(e.to_string()))
    }
}

/// Load a table definition, or error if it does not exist.
pub async fn load(db: &Session, table: &str) -> Result<TableDef> {
    match db.get(catalog_key(table)).await? {
        Some(bytes) => TableDef::decode(&bytes),
        None => Err(Error::Catalog(format!("no such table: {table}"))),
    }
}

/// Check whether a table exists.
pub async fn exists(db: &Session, table: &str) -> Result<bool> {
    Ok(db.get(catalog_key(table)).await?.is_some())
}
