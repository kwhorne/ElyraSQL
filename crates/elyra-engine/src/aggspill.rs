//! Partitioned spill for `GROUP BY`.
//!
//! Rows are routed to `N` partitions by a hash of their group key, buffered in
//! memory up to a row budget, then spilled to per-partition temp files. Because
//! every row for a given group lands in the same partition, each partition can
//! be aggregated independently in bounded memory (≈ total_groups / N distinct
//! groups), so a `GROUP BY` with a huge number of distinct groups no longer
//! risks running the server out of memory.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use elyra_core::{Error, Result, Value};

/// FNV-1a hash → partition index.
pub fn partition_of(key: &[u8], n: usize) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % n as u64) as usize
}

fn temp_path(part: usize) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("elyrasql-agg-{pid}-{n}-{part}.tmp"))
}

/// A set of `N` row partitions with bounded-memory spill to temp files.
pub struct Partitions {
    mem: Vec<Vec<Vec<Value>>>,
    files: Vec<Option<PathBuf>>,
    buffered: usize,
    budget: usize,
}

impl Partitions {
    pub fn new(n: usize, budget: usize) -> Self {
        Partitions {
            mem: (0..n).map(|_| Vec::new()).collect(),
            files: (0..n).map(|_| None).collect(),
            buffered: 0,
            budget: budget.max(1),
        }
    }

    pub fn len(&self) -> usize {
        self.mem.len()
    }

    /// Route a row to partition `p`; spill everything if the budget is hit.
    pub fn route(&mut self, p: usize, row: Vec<Value>) -> Result<()> {
        self.mem[p].push(row);
        self.buffered += 1;
        if self.buffered >= self.budget {
            self.spill_all()?;
        }
        Ok(())
    }

    fn spill_all(&mut self) -> Result<()> {
        for p in 0..self.mem.len() {
            if self.mem[p].is_empty() {
                continue;
            }
            let path = self.files[p].clone().unwrap_or_else(|| temp_path(p));
            let mut w = BufWriter::new(File::options().create(true).append(true).open(&path)?);
            for row in self.mem[p].drain(..) {
                let bytes = bincode::serialize(&row).map_err(|e| Error::Storage(e.to_string()))?;
                w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                w.write_all(&bytes)?;
            }
            w.flush()?;
            self.files[p] = Some(path);
        }
        self.buffered = 0;
        Ok(())
    }

    /// Consume partition `p`, yielding every row (in-memory + spilled) to `f`.
    pub fn drain_each<F: FnMut(Vec<Value>) -> Result<()>>(
        &mut self,
        p: usize,
        mut f: F,
    ) -> Result<()> {
        for row in std::mem::take(&mut self.mem[p]) {
            f(row)?;
        }
        if let Some(path) = self.files[p].take() {
            let mut r = BufReader::new(File::open(&path)?);
            loop {
                let mut len = [0u8; 4];
                match r.read_exact(&mut len) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(Error::Io(e)),
                }
                let n = u32::from_le_bytes(len) as usize;
                if n > elyra_core::max_frame_bytes() {
                    return Err(Error::Storage(
                        "aggregation spill record too large (corrupt?)".into(),
                    ));
                }
                let mut buf = vec![0u8; n];
                r.read_exact(&mut buf)?;
                let row: Vec<Value> =
                    bincode::deserialize(&buf).map_err(|e| Error::Storage(e.to_string()))?;
                f(row)?;
            }
            let _ = std::fs::remove_file(&path);
        }
        Ok(())
    }
}

impl Drop for Partitions {
    fn drop(&mut self) {
        for f in self.files.iter().flatten() {
            let _ = std::fs::remove_file(f);
        }
    }
}
