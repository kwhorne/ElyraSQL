//! Optional in-memory columnar cache for repeated, unfiltered analytical
//! aggregations (OLAP phase 4).
//!
//! When enabled (`ELYRASQL_COLUMN_CACHE_MB > 0`), the numeric base columns of a
//! table are extracted once into contiguous typed arrays and reused by later
//! scalar/grouped aggregations, skipping the storage scan and per-row decode.
//!
//! **Correctness.** Each cache entry is tagged with the storage write sequence
//! (`wseq`) it was built from -- a counter persisted *inside every write
//! transaction*, so it is atomic with data visibility. A cached entry is used
//! only when the current committed `wseq` still matches (checked per query), and
//! is only stored when `wseq` did not change during the build. Any committed
//! write (insert/update/delete, transaction commit, replication, DDL) advances
//! `wseq` and thus invalidates the cache. It is only ever consulted in
//! autocommit (never inside a transaction with an uncommitted overlay).

use crate::rowdec;
use elyra_core::{ColumnType, Result, Schema, Value};
use elyra_olap::{AggFunc, FxHasher};
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::{OnceLock, RwLock};

/// One cached numeric column: values plus a per-row null flag (row-aligned).
pub enum ColArray {
    Int(Vec<i64>, Vec<bool>),
    Float(Vec<f64>, Vec<bool>),
}

impl ColArray {
    fn bytes(&self) -> usize {
        match self {
            ColArray::Int(v, n) => v.len() * 8 + n.len(),
            ColArray::Float(v, n) => v.len() * 8 + n.len(),
        }
    }
    /// Numeric value at row `i`, or `None` if that cell is NULL.
    #[inline]
    fn get_f64(&self, i: usize) -> Option<f64> {
        match self {
            ColArray::Int(v, n) => (!n[i]).then(|| v[i] as f64),
            ColArray::Float(v, n) => (!n[i]).then(|| v[i]),
        }
    }
}

/// A table's numeric columns materialised columnar, valid at `wseq`.
pub struct CachedTable {
    pub wseq: u64,
    pub nrows: usize,
    /// Indexed by base column; `None` for non-numeric (uncached) columns.
    pub cols: Vec<Option<ColArray>>,
    pub bytes: usize,
}

type Map = HashMap<String, std::sync::Arc<CachedTable>>;

fn cache() -> &'static RwLock<Map> {
    static C: OnceLock<RwLock<Map>> = OnceLock::new();
    C.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Configured cache budget in bytes (`ELYRASQL_COLUMN_CACHE_MB`, default 0 = off).
pub fn budget_bytes() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("ELYRASQL_COLUMN_CACHE_MB")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0)
            .saturating_mul(1024 * 1024)
    })
}

pub fn enabled() -> bool {
    budget_bytes() > 0
}

/// Fetch a cached table iff it exists and its `wseq` matches `epoch`.
pub fn get(table: &str, epoch: u64) -> Option<std::sync::Arc<CachedTable>> {
    let g = cache().read().unwrap();
    g.get(table).filter(|t| t.wseq == epoch).cloned()
}

/// Store a freshly built table, evicting others if the budget would be exceeded.
/// If the entry alone exceeds the budget it is not cached (aggregation still ran
/// from the built arrays for the current query).
pub fn store(table: &str, ct: std::sync::Arc<CachedTable>) {
    let budget = budget_bytes();
    if ct.bytes > budget {
        return;
    }
    let mut g = cache().write().unwrap();
    g.remove(table);
    let mut total: usize = g.values().map(|t| t.bytes).sum();
    // Evict arbitrary entries until the newcomer fits (simple bound, not LRU).
    while total + ct.bytes > budget {
        let victim = g.keys().next().cloned();
        match victim {
            Some(k) => {
                if let Some(v) = g.remove(&k) {
                    total -= v.bytes;
                }
            }
            None => break,
        }
    }
    g.insert(table.to_string(), ct);
}

/// Build a [`CachedTable`] from already-scanned rows. `rows` is the raw stored
/// value blobs; each is decoded for the numeric columns only.
pub fn build(schema: &Schema, wseq: u64, blobs: &[Vec<u8>]) -> Result<CachedTable> {
    let ncols = schema.columns.len();
    let numeric: Vec<bool> = schema
        .columns
        .iter()
        .map(|c| matches!(c.ty, ColumnType::Int | ColumnType::Float))
        .collect();
    let mut cols: Vec<Option<ColArray>> = schema
        .columns
        .iter()
        .map(|c| match c.ty {
            ColumnType::Int => Some(ColArray::Int(
                Vec::with_capacity(blobs.len()),
                Vec::with_capacity(blobs.len()),
            )),
            ColumnType::Float => Some(ColArray::Float(
                Vec::with_capacity(blobs.len()),
                Vec::with_capacity(blobs.len()),
            )),
            _ => None,
        })
        .collect();
    let mut buf: Vec<Value> = Vec::with_capacity(ncols);
    for v in blobs {
        rowdec::decode_projected_into(v, ncols, &numeric, &mut buf)?;
        for (c, slot) in cols.iter_mut().enumerate() {
            match slot {
                Some(ColArray::Int(vals, nulls)) => match buf.get(c) {
                    Some(Value::Int(x)) => {
                        vals.push(*x);
                        nulls.push(false);
                    }
                    _ => {
                        vals.push(0);
                        nulls.push(true);
                    }
                },
                Some(ColArray::Float(vals, nulls)) => match buf.get(c) {
                    Some(Value::Float(x)) => {
                        vals.push(*x);
                        nulls.push(false);
                    }
                    Some(Value::Int(x)) => {
                        vals.push(*x as f64);
                        nulls.push(false);
                    }
                    _ => {
                        vals.push(0.0);
                        nulls.push(true);
                    }
                },
                None => {}
            }
        }
    }
    let bytes = cols.iter().flatten().map(|a| a.bytes()).sum();
    Ok(CachedTable {
        wseq,
        nrows: blobs.len(),
        cols,
        bytes,
    })
}

/// Scalar (no GROUP BY) aggregation over cached columns. `specs` are
/// `(func, arg column, is_integer)`; mirrors the scan-based columnar finish.
pub fn scalar_agg(ct: &CachedTable, specs: &[(AggFunc, Option<usize>, bool)]) -> Vec<Value> {
    specs
        .iter()
        .map(|&(func, arg, is_int)| {
            let arr = arg.and_then(|c| ct.cols.get(c).and_then(|o| o.as_ref()));
            match func {
                AggFunc::CountStar => Value::Int(ct.nrows as i64),
                AggFunc::Count => {
                    let mut c = 0i64;
                    if let Some(a) = arr {
                        for i in 0..ct.nrows {
                            if a.get_f64(i).is_some() {
                                c += 1;
                            }
                        }
                    }
                    Value::Int(c)
                }
                AggFunc::Sum | AggFunc::Avg => {
                    let (mut sum, mut cnt) = (0.0f64, 0i64);
                    if let Some(a) = arr {
                        for i in 0..ct.nrows {
                            if let Some(x) = a.get_f64(i) {
                                sum += x;
                                cnt += 1;
                            }
                        }
                    }
                    finish_sum_avg(func, sum, cnt, is_int)
                }
                AggFunc::Min | AggFunc::Max => {
                    let mut ext: Option<f64> = None;
                    if let Some(a) = arr {
                        for i in 0..ct.nrows {
                            if let Some(x) = a.get_f64(i) {
                                ext = Some(match (ext, func) {
                                    (None, _) => x,
                                    (Some(e), AggFunc::Min) => e.min(x),
                                    (Some(e), _) => e.max(x),
                                });
                            }
                        }
                    }
                    finish_ext(ext, is_int)
                }
                AggFunc::GroupConcat => Value::Null,
            }
        })
        .collect()
}

type FxU64Map = HashMap<u64, u32, BuildHasherDefault<FxHasher>>;

/// Grouped aggregation over cached columns. Returns `None` if the distinct-group
/// count would exceed the configured cap (caller falls back to the scan/spill
/// path). Group key kept exactly (integer bits / canonical float bits).
#[allow(clippy::type_complexity)]
pub fn group_agg(
    ct: &CachedTable,
    group_col: usize,
    specs: &[(AggFunc, Option<usize>, bool)],
    base_len: usize,
) -> Option<Vec<(Vec<Value>, Vec<Value>)>> {
    let naggs = specs.len();
    let max_groups = elyra_olap::default_max_groups();
    let gcol = ct.cols.get(group_col).and_then(|o| o.as_ref());
    let mut index: FxU64Map = FxU64Map::default();
    let mut null_gid = u32::MAX;
    let mut keyvals: Vec<Value> = Vec::new();
    let mut count: Vec<i64> = Vec::new();
    let mut sum: Vec<f64> = Vec::new();
    let mut min: Vec<f64> = Vec::new();
    let mut max: Vec<f64> = Vec::new();
    let mut has: Vec<bool> = Vec::new();

    let new_group = |keyvals: &mut Vec<Value>,
                     count: &mut Vec<i64>,
                     sum: &mut Vec<f64>,
                     min: &mut Vec<f64>,
                     max: &mut Vec<f64>,
                     has: &mut Vec<bool>,
                     kv: Value|
     -> Option<u32> {
        if max_groups > 0 && keyvals.len() >= max_groups {
            return None;
        }
        let gid = keyvals.len() as u32;
        keyvals.push(kv);
        count.resize(count.len() + naggs, 0);
        sum.resize(sum.len() + naggs, 0.0);
        min.resize(min.len() + naggs, f64::INFINITY);
        max.resize(max.len() + naggs, f64::NEG_INFINITY);
        has.resize(has.len() + naggs, false);
        Some(gid)
    };

    for i in 0..ct.nrows {
        // Group key value (exact) for this row.
        let (bits, is_null, kv) = match gcol {
            Some(ColArray::Int(v, n)) => {
                if n[i] {
                    (0u64, true, Value::Null)
                } else {
                    (v[i] as u64, false, Value::Int(v[i]))
                }
            }
            Some(ColArray::Float(v, n)) => {
                if n[i] {
                    (0u64, true, Value::Null)
                } else {
                    (
                        elyra_core::canonical_f64_bits(v[i]),
                        false,
                        Value::Float(v[i]),
                    )
                }
            }
            None => (0u64, true, Value::Null),
        };
        let gid = if is_null {
            if null_gid == u32::MAX {
                match new_group(
                    &mut keyvals,
                    &mut count,
                    &mut sum,
                    &mut min,
                    &mut max,
                    &mut has,
                    Value::Null,
                ) {
                    Some(g) => null_gid = g,
                    None => return None,
                }
            }
            null_gid
        } else {
            match index.get(&bits) {
                Some(&g) => g,
                None => match new_group(
                    &mut keyvals,
                    &mut count,
                    &mut sum,
                    &mut min,
                    &mut max,
                    &mut has,
                    kv,
                ) {
                    Some(g) => {
                        index.insert(bits, g);
                        g
                    }
                    None => return None,
                },
            }
        };
        let base = gid as usize * naggs;
        for (a, &(func, arg, _)) in specs.iter().enumerate() {
            match func {
                AggFunc::CountStar => count[base + a] += 1,
                _ => {
                    let x = arg
                        .and_then(|c| ct.cols.get(c).and_then(|o| o.as_ref()))
                        .and_then(|arr| arr.get_f64(i));
                    if let Some(x) = x {
                        match func {
                            AggFunc::Count => count[base + a] += 1,
                            AggFunc::Sum | AggFunc::Avg => {
                                sum[base + a] += x;
                                count[base + a] += 1;
                            }
                            AggFunc::Min => {
                                has[base + a] = true;
                                if x < min[base + a] {
                                    min[base + a] = x;
                                }
                            }
                            AggFunc::Max => {
                                has[base + a] = true;
                                if x > max[base + a] {
                                    max[base + a] = x;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    let mut out = Vec::with_capacity(keyvals.len());
    for (gid, kv) in keyvals.iter().enumerate() {
        let base = gid * naggs;
        let results: Vec<Value> = specs
            .iter()
            .enumerate()
            .map(|(a, &(func, _, is_int))| match func {
                AggFunc::CountStar | AggFunc::Count => Value::Int(count[base + a]),
                AggFunc::Sum | AggFunc::Avg => {
                    finish_sum_avg(func, sum[base + a], count[base + a], is_int)
                }
                AggFunc::Min => finish_ext(has[base + a].then_some(min[base + a]), is_int),
                AggFunc::Max => finish_ext(has[base + a].then_some(max[base + a]), is_int),
                AggFunc::GroupConcat => Value::Null,
            })
            .collect();
        let mut sample = vec![Value::Null; base_len];
        if group_col < base_len {
            sample[group_col] = kv.clone();
        }
        out.push((sample, results));
    }
    Some(out)
}

fn finish_sum_avg(func: AggFunc, sum: f64, count: i64, is_int: bool) -> Value {
    if count == 0 {
        return Value::Null;
    }
    match func {
        AggFunc::Avg => Value::Float(sum / count as f64),
        _ => {
            if is_int && sum.fract() == 0.0 {
                Value::Int(sum as i64)
            } else {
                Value::Float(sum)
            }
        }
    }
}

fn finish_ext(ext: Option<f64>, is_int: bool) -> Value {
    match ext {
        None => Value::Null,
        Some(x) if is_int => Value::Int(x as i64),
        Some(x) => Value::Float(x),
    }
}
