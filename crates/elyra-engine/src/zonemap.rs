//! Zone maps for data-skipping on filtered scans (inspired by
//! Parquet/InfluxDB-IOx block statistics).
//!
//! The clustered keyspace of a table is divided into fixed-size **chunks** of
//! contiguous rows; for each chunk we keep the min/max of every numeric column.
//! A filter that is a conjunction of `column <op> literal` (see
//! [`cpred`](crate::cpred)) can then skip any chunk whose [min, max] range for a
//! bounded column cannot satisfy the bound -- that chunk provably contains no
//! matching row, so it is never scanned.
//!
//! **Correctness.** Zone maps only *reduce which rows are scanned*; the compiled
//! predicate is still evaluated on every surviving row, so results are always
//! exact. A zone map is tagged with the storage write sequence (`wseq`) it was
//! built from and used only while the current committed `wseq` still matches, so
//! it can never skip a chunk based on stale statistics (same guarantee as the
//! columnar cache). Best suited to data with locality (time-ordered rows,
//! monotonic ids, sorted loads); it is opt-in via `ELYRASQL_ZONE_MAPS`.

use crate::cpred::{BoundOp, ColBound};
use crate::rowdec;
use elyra_core::{ColumnType, Result, Schema, Value};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

/// Rows per chunk. Fine enough to skip meaningfully, coarse enough that the map
/// stays tiny (a few bytes per chunk per column).
const CHUNK_ROWS: usize = 8192;

/// Per-chunk numeric min/max (indexed by base column; `None` = column not
/// numeric, or all-null within the chunk).
struct Chunk {
    start: Vec<u8>,
    min: Vec<Option<f64>>,
    max: Vec<Option<f64>>,
}

pub struct ZoneMap {
    wseq: u64,
    chunks: Vec<Chunk>,
    /// Exclusive upper bound of the whole table keyspace (end of the last chunk).
    upper: Vec<u8>,
}

/// Incremental builder fed row-by-row from a single consistent scan.
pub struct Builder {
    ncols: usize,
    numeric: Vec<bool>,
    buf: Vec<Value>,
    chunks: Vec<Chunk>,
    cur_min: Vec<Option<f64>>,
    cur_max: Vec<Option<f64>>,
    cur_start: Vec<u8>,
    rows_in_chunk: usize,
}

impl Builder {
    pub fn new(schema: &Schema) -> Self {
        let ncols = schema.columns.len();
        let numeric: Vec<bool> = schema
            .columns
            .iter()
            .map(|c| matches!(c.ty, ColumnType::Int | ColumnType::Float))
            .collect();
        Builder {
            ncols,
            numeric,
            buf: Vec::with_capacity(ncols),
            chunks: Vec::new(),
            cur_min: vec![None; ncols],
            cur_max: vec![None; ncols],
            cur_start: Vec::new(),
            rows_in_chunk: 0,
        }
    }

    pub fn feed(&mut self, key: &[u8], row: &[u8]) -> Result<()> {
        if self.rows_in_chunk == 0 {
            self.cur_start = key.to_vec();
        }
        rowdec::decode_projected_into(row, self.ncols, &self.numeric, &mut self.buf)?;
        for c in 0..self.ncols {
            if !self.numeric[c] {
                continue;
            }
            let v = match self.buf.get(c) {
                Some(Value::Int(i)) => *i as f64,
                Some(Value::Float(f)) => *f,
                _ => continue, // NULL / other: does not widen numeric min/max
            };
            self.cur_min[c] = Some(self.cur_min[c].map_or(v, |m| m.min(v)));
            self.cur_max[c] = Some(self.cur_max[c].map_or(v, |m| m.max(v)));
        }
        self.rows_in_chunk += 1;
        if self.rows_in_chunk >= CHUNK_ROWS {
            self.seal_chunk();
        }
        Ok(())
    }

    fn seal_chunk(&mut self) {
        if self.rows_in_chunk == 0 {
            return;
        }
        self.chunks.push(Chunk {
            start: std::mem::take(&mut self.cur_start),
            min: std::mem::replace(&mut self.cur_min, vec![None; self.ncols]),
            max: std::mem::replace(&mut self.cur_max, vec![None; self.ncols]),
        });
        self.rows_in_chunk = 0;
    }

    pub fn finish(mut self, wseq: u64, upper: Vec<u8>) -> ZoneMap {
        self.seal_chunk();
        ZoneMap {
            wseq,
            chunks: self.chunks,
            upper,
        }
    }
}

impl ZoneMap {
    /// Merge the surviving chunks under `bounds` (all must hold -- AND) into a
    /// minimal set of contiguous `[start, end)` key ranges to scan. Returns an
    /// empty vec if every chunk is skipped.
    pub fn surviving_ranges(&self, bounds: &[ColBound]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let n = self.chunks.len();
        let mut ranges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut i = 0;
        while i < n {
            if !self.chunk_survives(i, bounds) {
                i += 1;
                continue;
            }
            let start = self.chunks[i].start.clone();
            // Extend across consecutive surviving chunks.
            let mut j = i;
            while j + 1 < n && self.chunk_survives(j + 1, bounds) {
                j += 1;
            }
            let end = if j + 1 < n {
                self.chunks[j + 1].start.clone()
            } else {
                self.upper.clone()
            };
            ranges.push((start, end));
            i = j + 1;
        }
        ranges
    }

    fn chunk_survives(&self, i: usize, bounds: &[ColBound]) -> bool {
        let ch = &self.chunks[i];
        bounds.iter().all(|b| {
            match (
                ch.min.get(b.col).copied().flatten(),
                ch.max.get(b.col).copied().flatten(),
            ) {
                // All-null (or unknown) column in this chunk: no numeric comparison
                // can match, so the chunk cannot satisfy this bound.
                (None, _) | (_, None) => false,
                (Some(min), Some(max)) => match b.op {
                    BoundOp::Gt => max > b.rhs,
                    BoundOp::Ge => max >= b.rhs,
                    BoundOp::Lt => min < b.rhs,
                    BoundOp::Le => min <= b.rhs,
                    BoundOp::Eq => min <= b.rhs && b.rhs <= max,
                    // Only skippable when the whole chunk is exactly rhs.
                    BoundOp::Ne => !(min == b.rhs && max == b.rhs),
                },
            }
        })
    }
}

type Map = HashMap<String, Arc<ZoneMap>>;

fn cache() -> &'static RwLock<Map> {
    static C: OnceLock<RwLock<Map>> = OnceLock::new();
    C.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn enabled() -> bool {
    static E: OnceLock<bool> = OnceLock::new();
    *E.get_or_init(|| {
        matches!(
            std::env::var("ELYRASQL_ZONE_MAPS").ok().as_deref(),
            Some("on") | Some("1") | Some("true")
        )
    })
}

pub fn get(table: &str, epoch: u64) -> Option<Arc<ZoneMap>> {
    let g = cache().read().unwrap();
    g.get(table).filter(|z| z.wseq == epoch).cloned()
}

pub fn store(table: &str, z: Arc<ZoneMap>) {
    cache().write().unwrap().insert(table.to_string(), z);
}
