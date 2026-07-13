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
    /// A full-text (inverted, tokenized) index over a text column.
    #[serde(default)]
    pub fulltext: bool,
    /// Per-column text collation, positional with `cols` (empty ⇒ all `Ci`).
    #[serde(default)]
    pub col_collations: Vec<elyra_core::Collation>,
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
    /// CHECK constraint expressions (SQL text), evaluated on INSERT/UPDATE.
    #[serde(default)]
    pub checks: Vec<String>,
    /// FOREIGN KEY constraints.
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
}

/// A FOREIGN KEY: `columns` in this table reference `ref_columns` of
/// `ref_table`, with the given referential actions.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<usize>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    #[serde(default)]
    pub on_delete: RefAction,
    #[serde(default)]
    pub on_update: RefAction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq)]
pub enum RefAction {
    #[default]
    NoAction,
    Restrict,
    Cascade,
    SetNull,
}

impl TableDef {
    pub fn has_pk(&self) -> bool {
        !self.pk_cols.is_empty()
    }

    /// Collation of column `i` (default `Ci`).
    pub fn collation_of(&self, i: usize) -> elyra_core::Collation {
        self.schema
            .columns
            .get(i)
            .map(|c| c.collation)
            .unwrap_or_default()
    }

    /// Collations of the given columns, in order.
    pub fn collations_of(&self, cols: &[usize]) -> Vec<elyra_core::Collation> {
        cols.iter().map(|&c| self.collation_of(c)).collect()
    }

    /// Collations of the primary-key columns, in key order.
    pub fn pk_collations(&self) -> Vec<elyra_core::Collation> {
        self.collations_of(&self.pk_cols)
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

/// Per-column statistics collected by `ANALYZE TABLE`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ColStat {
    pub name: String,
    /// Number of distinct non-null values (capped; see `ndv_capped`).
    pub ndv: u64,
    /// Whether `ndv` hit the counting cap (a lower bound if true).
    #[serde(default)]
    pub ndv_capped: bool,
    pub nulls: u64,
    pub min: Option<String>,
    pub max: Option<String>,
    /// Equi-height histogram boundaries (B+1 sorted values as wire strings; the
    /// B buckets each hold roughly `rows/B` values). Empty if not collected.
    #[serde(default)]
    pub hist: Vec<String>,
}

/// Numeric-aware comparison of two wire-string values (falls back to byte order).
fn cmp_wire(a: &str, b: &str) -> std::cmp::Ordering {
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
        _ => a.cmp(b),
    }
}

impl ColStat {
    /// Estimated fraction (0..=1) of non-null values strictly below `target`,
    /// from the equi-height histogram. `None` if no histogram is available.
    pub fn frac_below(&self, target: &str) -> Option<f64> {
        if self.hist.len() < 2 {
            return None;
        }
        let buckets = (self.hist.len() - 1) as f64;
        let below = self
            .hist
            .iter()
            .take_while(|b| cmp_wire(b, target) == std::cmp::Ordering::Less)
            .count();
        Some(((below as f64) / buckets).clamp(0.0, 1.0))
    }

    /// Estimated selectivity (0..=1) of `<column> op value` on this column.
    pub fn selectivity(&self, op: SelOp, value: &str) -> Option<f64> {
        match op {
            SelOp::Lt | SelOp::Le => self.frac_below(value),
            SelOp::Gt | SelOp::Ge => self.frac_below(value).map(|f| 1.0 - f),
            SelOp::Eq => {
                if self.ndv > 0 {
                    Some(1.0 / self.ndv as f64)
                } else {
                    None
                }
            }
        }
    }
}

/// Comparison kind for selectivity estimation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
}

/// Persisted table statistics (from `ANALYZE TABLE`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TableStats {
    /// Row count at the last ANALYZE.
    pub rows: u64,
    /// Per-column statistics (positional with the table schema).
    #[serde(default)]
    pub columns: Vec<ColStat>,
}

pub fn stats_key(table: &str) -> Vec<u8> {
    format!("stats::{table}").into_bytes()
}

/// Key under which a stored procedure's body is stored.
pub fn proc_key(name: &str) -> Vec<u8> {
    format!("sys::proc::{}", name.to_ascii_lowercase()).into_bytes()
}

/// The DML event a trigger fires on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrigEvent {
    Insert,
    Update,
    Delete,
}

/// A stored trigger definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDef {
    pub name: String,
    pub table: String,
    /// true = BEFORE, false = AFTER.
    pub before: bool,
    pub event: TrigEvent,
    pub body: String,
}

fn trigger_prefix(table: &str) -> Vec<u8> {
    format!("sys::trigger::{}::", table.to_ascii_lowercase()).into_bytes()
}

pub fn trigger_key(table: &str, name: &str) -> Vec<u8> {
    let mut k = trigger_prefix(table);
    k.extend_from_slice(name.to_ascii_lowercase().as_bytes());
    k
}

/// Index key mapping a trigger name to its table, so DROP TRIGGER is an O(1)
/// lookup instead of scanning every trigger.
pub fn trigname_key(name: &str) -> Vec<u8> {
    format!("sys::trigname::{}", name.to_ascii_lowercase()).into_bytes()
}

/// Load all triggers defined on `table`.
pub async fn load_triggers(db: &Session, table: &str) -> Result<Vec<TriggerDef>> {
    let prefix = trigger_prefix(table);
    let batch = db.scan_batch(prefix, None, 4096).await?;
    Ok(batch
        .iter()
        .filter_map(|(_, v)| bincode::deserialize(v).ok())
        .collect())
}

/// Find a trigger by name (for DROP TRIGGER) via the name->table index — O(1),
/// no full scan. Falls back to a scan only for triggers created before the index
/// existed.
pub async fn find_trigger(db: &Session, name: &str) -> Result<Option<TriggerDef>> {
    if let Some(table) = db.get(trigname_key(name)).await? {
        let table = String::from_utf8_lossy(&table).into_owned();
        if let Some(v) = db.get(trigger_key(&table, name)).await? {
            return Ok(bincode::deserialize(&v).ok());
        }
    }
    // Legacy fallback: scan (bounded) for triggers without an index entry.
    let want = name.to_ascii_lowercase();
    let batch = db
        .scan_batch(b"sys::trigger::".to_vec(), None, 100_000)
        .await?;
    for (_, v) in &batch {
        if let Ok(t) = bincode::deserialize::<TriggerDef>(v) {
            if t.name.eq_ignore_ascii_case(&want) {
                return Ok(Some(t));
            }
        }
    }
    Ok(None)
}

/// Load a table's statistics, if it has been analyzed.
pub async fn load_stats(db: &Session, table: &str) -> Result<Option<TableStats>> {
    match db.get(stats_key(table)).await? {
        Some(b) => Ok(bincode::deserialize(&b).ok()),
        None => Ok(None),
    }
}

pub fn rowid_key(table: &str) -> Vec<u8> {
    format!("meta::rowid::{table}").into_bytes()
}

/// Key under which a view's SQL definition is stored.
pub fn view_key(name: &str) -> Vec<u8> {
    format!("view::{name}").into_bytes()
}

/// Storage key for a materialized view's defining query (the data itself lives
/// in a normal table of the same name).
pub fn matview_key(name: &str) -> Vec<u8> {
    format!("matview::{name}").into_bytes()
}

/// Storage key for a materialized view's base-table dependencies + the write
/// counters they had at the last refresh (used for auto-refresh on staleness).
pub fn matdep_key(name: &str) -> Vec<u8> {
    format!("matdep::{name}").into_bytes()
}

/// One partition of a partitioned table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionDef {
    pub name: String,
    /// RANGE upper bound (exclusive); `None` = `MAXVALUE`.
    pub less_than: Option<i64>,
    /// LIST membership values.
    #[serde(default)]
    pub list_values: Vec<i64>,
}

/// A table's partitioning scheme (over the primary-key column).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// `RANGE`, `LIST`, or `HASH` (upper-cased).
    pub method: String,
    pub column: String,
    pub parts: Vec<PartitionDef>,
    /// HASH partition count (0 for RANGE/LIST).
    #[serde(default)]
    pub hash_count: u32,
}

pub fn partmeta_key(table: &str) -> Vec<u8> {
    format!("partmeta::{table}").into_bytes()
}

/// Load a table's partitioning scheme, if partitioned.
pub async fn load_partspec(db: &Session, table: &str) -> Result<Option<PartitionSpec>> {
    match db.get(partmeta_key(table)).await? {
        Some(b) => Ok(bincode::deserialize(&b).ok()),
        None => Ok(None),
    }
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
/// Bumped whenever any `catalog::` key is written, invalidating every cached
/// `TableDef` (they carry the epoch they were read at).
static CATALOG_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Invalidate all cached table definitions (called on any catalog write).
pub fn bump_epoch() {
    CATALOG_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Release);
}

// --- Rarely-used-feature existence flags -------------------------------------
//
// Materialized-view auto-refresh and per-column masking otherwise cost a
// storage read on *every* SELECT. These flags let the common path (no matviews,
// no column grants) skip those reads entirely. They default to `true` (safe:
// the feature check runs), are corrected once by a lazy startup scan, and are
// set back to `true` whenever the corresponding key is written.
use std::sync::atomic::{AtomicBool, Ordering};
static MATVIEWS_EXIST: AtomicBool = AtomicBool::new(true);
static COLGRANTS_EXIST: AtomicBool = AtomicBool::new(true);
static MV_INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
static CG_INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

/// Called for every committed write: flip a feature flag on if its key appears.
pub fn note_feature_writes(puts: &[(Vec<u8>, Vec<u8>)], deletes: &[Vec<u8>]) {
    let hit = |p: &[u8]| {
        puts.iter().any(|(k, _)| k.starts_with(p)) || deletes.iter().any(|k| k.starts_with(p))
    };
    if hit(b"matview::") {
        MATVIEWS_EXIST.store(true, Ordering::Release);
    }
    if hit(b"sys::colgrant::") {
        COLGRANTS_EXIST.store(true, Ordering::Release);
    }
}

/// Whether any materialized view exists (lazily scanned once, then cached).
pub async fn matviews_exist(sess: &Session) -> bool {
    MV_INIT
        .get_or_init(|| async {
            let any = sess
                .scan_batch(b"matview::".to_vec(), None, 1)
                .await
                .map(|b| !b.is_empty())
                .unwrap_or(true);
            MATVIEWS_EXIST.store(any, Ordering::Release);
        })
        .await;
    MATVIEWS_EXIST.load(Ordering::Acquire)
}

/// Whether any per-column grant exists (lazily scanned once, then cached).
pub async fn colgrants_exist(sess: &Session) -> bool {
    CG_INIT
        .get_or_init(|| async {
            let any = sess
                .scan_batch(b"sys::colgrant::".to_vec(), None, 1)
                .await
                .map(|b| !b.is_empty())
                .unwrap_or(true);
            COLGRANTS_EXIST.store(any, Ordering::Release);
        })
        .await;
    COLGRANTS_EXIST.load(Ordering::Acquire)
}

#[allow(clippy::type_complexity)]
fn catalog_cache() -> &'static std::sync::RwLock<
    std::collections::HashMap<(u64, String), (u64, std::sync::Arc<TableDef>)>,
> {
    use std::sync::{OnceLock, RwLock};
    static C: OnceLock<
        RwLock<std::collections::HashMap<(u64, String), (u64, std::sync::Arc<TableDef>)>>,
    > = OnceLock::new();
    C.get_or_init(|| RwLock::new(std::collections::HashMap::new()))
}

/// Load a table definition. In autocommit this is served from an in-memory
/// cache keyed by table name and validated against the catalog epoch, so the
/// common query path avoids a storage read + decode entirely. Inside a
/// transaction the definition is always read fresh (through the write overlay),
/// so uncommitted DDL is visible.
pub async fn load(db: &Session, table: &str) -> Result<TableDef> {
    let epoch = CATALOG_EPOCH.load(std::sync::atomic::Ordering::Acquire);
    // Key the process-global cache by (database id, table name) so multiple Dbs
    // in one process never serve each other's schema for a same-named table.
    let ckey = (db.db_id(), table.to_string());
    if !db.in_txn() {
        if let Some((e, def)) = catalog_cache().read().unwrap().get(&ckey) {
            if *e == epoch {
                return Ok((**def).clone());
            }
        }
    }
    let def = match db.get(catalog_key(table)).await? {
        Some(bytes) => TableDef::decode(&bytes)?,
        None => return Err(Error::Catalog(format!("no such table: {table}"))),
    };
    if !db.in_txn() {
        catalog_cache()
            .write()
            .unwrap()
            .insert(ckey, (epoch, std::sync::Arc::new(def.clone())));
    }
    Ok(def)
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
