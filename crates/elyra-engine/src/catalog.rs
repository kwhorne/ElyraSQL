//! Table catalog: schema definitions persisted in the single DB file.
//!
//! Keyspace layout (all inside the one file):
//! * `catalog::<table>`     → serialized [`TableDef`]
//! * `meta::rowid::<table>` → u64 counter for hidden rowids
//! * `data::<table>::<key>` → serialized row (`Vec<Value>`)

use crate::session::Session;
use elyra_core::{Error, Result, Schema};
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

/// Per-column defaults, auto-increment, and (stored) generated expressions.
/// Stored as SQL text and evaluated by the engine at write time.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ColMeta {
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub auto_increment: bool,
    #[serde(default)]
    pub generated: Option<String>,
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
    /// Column metadata, parallel to `schema.columns`. May be shorter/empty for
    /// older catalogs or tables with no special columns.
    #[serde(default)]
    pub col_meta: Vec<ColMeta>,
}

impl TableDef {
    pub fn has_pk(&self) -> bool {
        !self.pk_cols.is_empty()
    }

    /// Column metadata for column `i` (empty default if unset).
    pub fn meta(&self, i: usize) -> ColMeta {
        self.col_meta.get(i).cloned().unwrap_or_default()
    }

    /// True if any column has a default, auto-increment, or generated value.
    pub fn has_col_meta(&self) -> bool {
        self.col_meta
            .iter()
            .any(|m| m.default.is_some() || m.auto_increment || m.generated.is_some())
    }
}

pub fn autoinc_key(table: &str) -> Vec<u8> {
    format!("meta::autoinc::{table}").into_bytes()
}

/// Prefix under which all secondary-index entries of a table live.
pub fn index_table_prefix(table: &str) -> Vec<u8> {
    format!("index::{table}::").into_bytes()
}

pub fn catalog_key(table: &str) -> Vec<u8> {
    format!("catalog::{table}").into_bytes()
}

pub fn rowid_key(table: &str) -> Vec<u8> {
    format!("meta::rowid::{table}").into_bytes()
}

/// Key under which a view's SQL definition is stored.
pub fn view_key(name: &str) -> Vec<u8> {
    format!("view::{name}").into_bytes()
}

/// Load a view's stored SELECT text, if it exists.
pub async fn load_view(db: &Session, name: &str) -> Result<Option<String>> {
    match db.get(view_key(name)).await? {
        Some(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
        None => Ok(None),
    }
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

/// List all user table names (excluding internal temp relations), sorted.
pub async fn list_tables(db: &Session) -> Result<Vec<String>> {
    let prefix = b"catalog::".to_vec();
    let mut names = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if batch.is_empty() {
            break;
        }
        cursor = batch.last().map(|(k, _)| k.clone());
        let last = batch.len() < 4096;
        for (k, _) in &batch {
            if let Some(rest) = k.strip_prefix(prefix.as_slice()) {
                let name = String::from_utf8_lossy(rest).into_owned();
                if !name.starts_with("__cte_") {
                    names.push(name);
                }
            }
        }
        if last {
            break;
        }
    }
    names.sort();
    Ok(names)
}

/// Check whether a table exists.
pub async fn exists(db: &Session, table: &str) -> Result<bool> {
    Ok(db.get(catalog_key(table)).await?.is_some())
}
