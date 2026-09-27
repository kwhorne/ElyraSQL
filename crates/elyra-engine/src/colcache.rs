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
use elyra_olap::{AggFunc, FxHasher, NumSlot};
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
    /// Fold row `i` into `slot` (nothing for a NULL cell), exactly: an integer
    /// column stays integer. This used to go through `f64`, so aggregates over
    /// `BIGINT` values past 2^53 rounded.
    #[inline]
    fn feed(&self, i: usize, slot: &mut NumSlot) {
        match self {
            ColArray::Int(v, n) => {
                if !n[i] {
                    slot.push_int(v[i]);
                }
            }
            ColArray::Float(v, n) => {
                if !n[i] {
                    // A float array only ever feeds a float slot.
                    let _ = slot.push_float(v[i]);
                }
            }
        }
    }

    /// Fold the whole column into `slot`. A column without NULLs goes to the
    /// slot's batch loops in one slice; otherwise non-NULL values are gathered
    /// into short runs first, so the per-value work is still a tight loop and
    /// not a type dispatch per cell.
    fn feed_all(&self, slot: &mut NumSlot) {
        const RUN: usize = 4096;
        match self {
            ColArray::Int(v, n) => {
                if !n.contains(&true) {
                    return slot.add_ints(v);
                }
                let mut run = Vec::with_capacity(RUN);
                for (vc, nc) in v.chunks(RUN).zip(n.chunks(RUN)) {
                    run.clear();
                    run.extend(
                        vc.iter()
                            .zip(nc)
                            .filter(|(_, &null)| !null)
                            .map(|(x, _)| *x),
                    );
                    slot.add_ints(&run);
                }
            }
            // A float array only ever feeds a float slot.
            ColArray::Float(v, n) => {
                if !n.contains(&true) {
                    let _ = slot.add_floats(v);
                    return;
                }
                let mut run = Vec::with_capacity(RUN);
                for (vc, nc) in v.chunks(RUN).zip(n.chunks(RUN)) {
                    run.clear();
                    run.extend(
                        vc.iter()
                            .zip(nc)
                            .filter(|(_, &null)| !null)
                            .map(|(x, _)| *x),
                    );
                    let _ = slot.add_floats(&run);
                }
            }
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
    /// Monotonic tick of the last access, for approximate-LRU eviction. Updated
    /// atomically on `get` (no lock upgrade needed).
    pub last_used: std::sync::atomic::AtomicU64,
}

/// Next value of the global access clock (drives approximate LRU).
fn next_tick() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static CLOCK: AtomicU64 = AtomicU64::new(1);
    CLOCK.fetch_add(1, Ordering::Relaxed)
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
    let g = cache().read().unwrap_or_else(|e| e.into_inner());
    let hit = g.get(table).filter(|t| t.wseq == epoch).cloned();
    if let Some(t) = &hit {
        // Mark as most-recently-used for approximate-LRU eviction (atomic, so no
        // read->write lock upgrade).
        t.last_used
            .store(next_tick(), std::sync::atomic::Ordering::Relaxed);
    }
    hit
}

/// Store a freshly built table, evicting others if the budget would be exceeded.
/// If the entry alone exceeds the budget it is not cached (aggregation still ran
/// from the built arrays for the current query).
pub fn store(table: &str, ct: std::sync::Arc<CachedTable>) {
    let budget = budget_bytes();
    if ct.bytes > budget {
        return;
    }
    let mut g = cache().write().unwrap_or_else(|e| e.into_inner());
    g.remove(table);
    let mut total: usize = g.values().map(|t| t.bytes).sum();
    // Evict the least-recently-used entries until the newcomer fits.
    while total + ct.bytes > budget {
        let victim = g
            .iter()
            .min_by_key(|(_, t)| t.last_used.load(std::sync::atomic::Ordering::Relaxed))
            .map(|(k, _)| k.clone());
        match victim {
            Some(k) => {
                if let Some(v) = g.remove(&k) {
                    total -= v.bytes;
                }
            }
            None => break,
        }
    }
    ct.last_used
        .store(next_tick(), std::sync::atomic::Ordering::Relaxed);
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
        last_used: std::sync::atomic::AtomicU64::new(0),
    })
}

/// Scalar (no GROUP BY) aggregation over cached columns. `specs` are
/// `(func, arg column, is_integer)`; mirrors the scan-based columnar finish.
pub fn scalar_agg(ct: &CachedTable, specs: &[(AggFunc, Option<usize>, bool)]) -> Vec<Value> {
    // A slot holds count, sum, minimum and maximum together, so aggregates
    // over the same column (`SUM(x), MIN(x), MAX(x)`) share one pass over it.
    let mut folded: Vec<(usize, NumSlot)> = Vec::new();
    specs
        .iter()
        .map(|&(func, arg, is_int)| {
            if func == AggFunc::CountStar {
                return Value::Int(ct.nrows as i64);
            }
            let Some(c) = arg else {
                return NumSlot::new(is_int).finish(func);
            };
            if let Some((_, slot)) = folded.iter().find(|(col, _)| *col == c) {
                return slot.finish(func);
            }
            let mut slot = NumSlot::new(is_int);
            if let Some(a) = ct.cols.get(c).and_then(|o| o.as_ref()) {
                a.feed_all(&mut slot);
            }
            folded.push((c, slot));
            slot.finish(func)
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
    let args: Vec<Option<&ColArray>> = specs
        .iter()
        .map(|&(_, arg, _)| arg.and_then(|c| ct.cols.get(c).and_then(|o| o.as_ref())))
        .collect();
    let blank: Vec<NumSlot> = specs.iter().map(|s| NumSlot::new(s.2)).collect();
    let mut index: FxU64Map = FxU64Map::default();
    let mut null_gid = u32::MAX;
    let mut keyvals: Vec<Value> = Vec::new();
    let mut slots: Vec<NumSlot> = Vec::new();

    let new_group = |keyvals: &mut Vec<Value>, slots: &mut Vec<NumSlot>, kv: Value| {
        if max_groups > 0 && keyvals.len() >= max_groups {
            return None;
        }
        let gid = keyvals.len() as u32;
        keyvals.push(kv);
        slots.extend_from_slice(&blank);
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
                null_gid = new_group(&mut keyvals, &mut slots, Value::Null)?;
            }
            null_gid
        } else {
            match index.get(&bits) {
                Some(&g) => g,
                None => {
                    let g = new_group(&mut keyvals, &mut slots, kv)?;
                    index.insert(bits, g);
                    g
                }
            }
        };
        let base = gid as usize * naggs;
        for (a, &(func, _, _)) in specs.iter().enumerate() {
            let slot = &mut slots[base + a];
            if func == AggFunc::CountStar {
                slot.count_row();
            } else if let Some(arr) = args[a] {
                arr.feed(i, slot);
            }
        }
    }

    let mut out = Vec::with_capacity(keyvals.len());
    for (gid, kv) in keyvals.iter().enumerate() {
        let base = gid * naggs;
        let results: Vec<Value> = specs
            .iter()
            .enumerate()
            .map(|(a, &(func, _, _))| slots[base + a].finish(func))
            .collect();
        let mut sample = vec![Value::Null; base_len];
        if group_col < base_len {
            sample[group_col] = kv.clone();
        }
        out.push((sample, results));
    }
    Some(out)
}
