//! Table catalog: schema definitions persisted in the single DB file.
//!
//! Keyspace layout (all inside the one file):
//! * `catalog::<table>`     → serialized [`TableDef`]
//! * `meta::rowid::<table>` → u64 counter for hidden rowids
//! * `data::<table>::<key>` → serialized row (`Vec<Value>`)

use elyra_core::{Error, Result, Schema};
use elyra_storage::Db;
use serde::{Deserialize, Serialize};

/// Definition of a table. `pk_col` is the schema index of the single-column
/// primary key (InnoDB-style clustered key); `None` means a hidden rowid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub schema: Schema,
    pub pk_col: Option<usize>,
}

pub fn catalog_key(table: &str) -> Vec<u8> {
    format!("catalog::{table}").into_bytes()
}

pub fn rowid_key(table: &str) -> Vec<u8> {
    format!("meta::rowid::{table}").into_bytes()
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
pub async fn load(db: &Db, table: &str) -> Result<TableDef> {
    match db.get(catalog_key(table)).await? {
        Some(bytes) => TableDef::decode(&bytes),
        None => Err(Error::Catalog(format!("no such table: {table}"))),
    }
}

/// Check whether a table exists.
pub async fn exists(db: &Db, table: &str) -> Result<bool> {
    Ok(db.get(catalog_key(table)).await?.is_some())
}
