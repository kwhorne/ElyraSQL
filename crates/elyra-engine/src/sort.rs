//! Memory-bounded ORDER BY.
//!
//! Two strategies keep large sorts from exhausting memory:
//!
//! * **Top-N heap** for `ORDER BY ... LIMIT` — only the `offset + limit` best
//!   rows are ever held, so `ORDER BY x LIMIT 50` over a billion rows costs
//!   O(50) memory.
//! * **External merge sort** for unbounded sorts — rows accumulate up to a row
//!   budget, then a sorted run is spilled to a temporary file; at the end the
//!   runs are merged. Peak memory is bounded by the budget regardless of the
//!   result size (OOM safety).

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use elyra_core::{Error, Result, Value};

use crate::aggregate::value_cmp;

/// Largest `offset + limit` that uses the in-memory top-N heap (rather than
/// spilling). Above this, a bounded `LIMIT` still goes through the external
/// sort so a pathological `LIMIT 100000000` cannot blow up memory.
const TOPN_CAP: usize = 1_000_000;

/// The external-sort spill budget in rows, from `ELYRASQL_SORT_MAX_ROWS`
/// (default 1,000,000). Rows beyond this are spilled to a temp file.
pub fn sort_max_rows() -> usize {
    use std::sync::OnceLock;
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("ELYRASQL_SORT_MAX_ROWS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(1_000_000)
    })
}

/// Compare two precomputed key vectors under per-key asc/desc flags.
fn cmp_keys(a: &[Value], b: &[Value], asc: &[bool]) -> Ordering {
    for (i, &asc) in asc.iter().enumerate() {
        let ord = value_cmp(&a[i], &b[i]);
        let ord = if asc { ord } else { ord.reverse() };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// A row plus its precomputed sort keys, ordered as the *worst* (largest under
/// the ORDER BY) so a `BinaryHeap` (max-heap) keeps the best N.
struct Ranked {
    keys: Vec<Value>,
    row: Vec<Value>,
    asc: std::sync::Arc<Vec<bool>>,
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        cmp_keys(&self.keys, &other.keys, &self.asc) == Ordering::Equal
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        cmp_keys(&self.keys, &other.keys, &self.asc)
    }
}

fn temp_path() -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, AtomicOrdering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("elyrasql-sort-{pid}-{n}.tmp"))
}

/// Delete leftover spill/aggregation temp files from ElyraSQL processes that are
/// no longer running (e.g. killed with SIGKILL, which skips `Drop` cleanup).
/// Only removes files whose embedded PID is *confirmed* dead, so concurrently
/// running instances are never disturbed; a no-op where liveness can't be
/// determined (non-Linux) or the temp dir can't be read.
pub fn cleanup_stale_tempfiles() {
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    let me = std::process::id();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(pid) = tempfile_pid(&name) else {
            continue;
        };
        if pid != me && pid_is_dead(pid) {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Extract the owning PID from an `elyrasql-sort-<pid>-...` /
/// `elyrasql-agg-<pid>-...` temp file name.
fn tempfile_pid(name: &str) -> Option<u32> {
    let rest = name
        .strip_prefix("elyrasql-sort-")
        .or_else(|| name.strip_prefix("elyrasql-agg-"))?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// True only when we can *confirm* the process is gone (via Linux `/proc`).
/// When liveness can't be determined, returns `false` so a possibly-live file
/// is never deleted.
fn pid_is_dead(pid: u32) -> bool {
    let proc_root = std::path::Path::new("/proc");
    if !proc_root.exists() {
        return false; // not Linux (e.g. dev on macOS) -> don't risk it
    }
    !proc_root.join(pid.to_string()).exists()
}

/// A spilled, sorted run on disk, read back one length-prefixed frame at a time.
struct RunReader {
    r: BufReader<File>,
}
impl RunReader {
    /// Read back a spilled run from its (already-open, possibly-unlinked) file.
    fn from_file(mut file: File) -> Result<Self> {
        file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
        Ok(RunReader {
            r: BufReader::new(file),
        })
    }
    fn next(&mut self) -> Result<Option<(Vec<Value>, Vec<Value>)>> {
        let mut len = [0u8; 4];
        match self.r.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        }
        let n = u32::from_le_bytes(len) as usize;
        if n > elyra_core::max_frame_bytes() {
            return Err(Error::Storage(
                "sort spill record too large (corrupt?)".into(),
            ));
        }
        let mut buf = vec![0u8; n];
        self.r.read_exact(&mut buf)?;
        let v = bincode::deserialize(&buf).map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Some(v))
    }
}

/// Accumulates rows and returns them sorted, bounding peak memory.
pub struct Sorter {
    asc: std::sync::Arc<Vec<bool>>,
    offset: usize,
    limit: Option<usize>,
    max_rows: usize,
    topn: bool,
    /// Top-N heap of the best `offset + limit` rows (top-N mode).
    heap: BinaryHeap<Ranked>,
    /// Pending in-memory run (external mode).
    buffer: Vec<(Vec<Value>, Vec<Value>)>,
    /// Spilled runs, each an open file handle (unlinked on Unix so a crash never
    /// leaves a temp file behind); the path is retained only for best-effort
    /// cleanup on platforms where unlinking an open file isn't possible.
    runs: Vec<(PathBuf, File)>,
}

impl Sorter {
    /// `asc` gives the direction of each ORDER BY key. `max_rows` is the spill
    /// budget for the external sort.
    pub fn new(asc: Vec<bool>, offset: usize, limit: Option<usize>, max_rows: usize) -> Self {
        let bounded = limit
            .map(|l| offset.saturating_add(l))
            .unwrap_or(usize::MAX);
        let topn = bounded <= TOPN_CAP;
        Sorter {
            asc: std::sync::Arc::new(asc),
            offset,
            limit,
            max_rows: max_rows.max(1),
            topn,
            heap: BinaryHeap::new(),
            buffer: Vec::new(),
            runs: Vec::new(),
        }
    }

    /// Feed one row with its precomputed sort keys.
    pub fn push(&mut self, keys: Vec<Value>, row: Vec<Value>) -> Result<()> {
        if self.topn {
            let n = self.offset.saturating_add(self.limit.unwrap_or(0));
            if n == 0 {
                return Ok(());
            }
            if self.heap.len() < n {
                self.heap.push(Ranked {
                    keys,
                    row,
                    asc: self.asc.clone(),
                });
            } else if let Some(top) = self.heap.peek() {
                // Replace the worst kept row if this one is better.
                if cmp_keys(&keys, &top.keys, &self.asc) == Ordering::Less {
                    self.heap.pop();
                    self.heap.push(Ranked {
                        keys,
                        row,
                        asc: self.asc.clone(),
                    });
                }
            }
        } else {
            self.buffer.push((keys, row));
            if self.buffer.len() >= self.max_rows {
                self.spill()?;
            }
        }
        Ok(())
    }

    fn spill(&mut self) -> Result<()> {
        let asc = self.asc.clone();
        self.buffer.sort_by(|a, b| cmp_keys(&a.0, &b.0, &asc));
        let path = temp_path();
        let file = File::create(&path)?;
        // Unlink immediately: the inode lives on via the open handle and is
        // reclaimed by the OS on close or crash, so no temp file is ever
        // orphaned (best-effort; a no-op-until-close on non-Unix).
        let _ = std::fs::remove_file(&path);
        let mut w = BufWriter::new(file);
        for (k, row) in &self.buffer {
            let bytes = bincode::serialize(&(k, row)).map_err(|e| Error::Storage(e.to_string()))?;
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            w.write_all(&bytes)?;
        }
        w.flush()?;
        let file = w.into_inner().map_err(|e| Error::Storage(e.to_string()))?;
        self.runs.push((path, file));
        self.buffer.clear();
        Ok(())
    }

    /// Finish sorting and return rows in order, with offset/limit applied.
    pub fn finish(&mut self) -> Result<Vec<Vec<Value>>> {
        if self.topn {
            let mut ranked: Vec<Ranked> = self.heap.drain().collect();
            ranked.sort_by(|a, b| cmp_keys(&a.keys, &b.keys, &self.asc));
            let rows: Vec<Vec<Value>> = ranked.into_iter().map(|r| r.row).collect();
            let start = self.offset.min(rows.len());
            return Ok(rows[start..].to_vec());
        }

        if self.runs.is_empty() {
            // Everything fit in memory: a plain sort.
            let asc = self.asc.clone();
            let mut buffer = std::mem::take(&mut self.buffer);
            buffer.sort_by(|a, b| cmp_keys(&a.0, &b.0, &asc));
            let mut out: Vec<Vec<Value>> = buffer.into_iter().map(|(_, r)| r).collect();
            if self.offset > 0 {
                out.drain(0..self.offset.min(out.len()));
            }
            if let Some(l) = self.limit {
                out.truncate(l);
            }
            return Ok(out);
        }

        // Spill the tail, then k-way merge every run.
        if !self.buffer.is_empty() {
            self.spill()?;
        }
        let runs = std::mem::take(&mut self.runs);
        let paths: Vec<PathBuf> = runs.iter().map(|(p, _)| p.clone()).collect();
        let mut readers: Vec<RunReader> = runs
            .into_iter()
            .map(|(_, f)| RunReader::from_file(f))
            .collect::<Result<_>>()?;
        let mut heads: Vec<Option<(Vec<Value>, Vec<Value>)>> = Vec::with_capacity(readers.len());
        for r in &mut readers {
            heads.push(r.next()?);
        }

        let mut out = Vec::new();
        let mut skipped = 0usize;
        loop {
            // Pick the smallest current head across runs.
            let mut best: Option<usize> = None;
            for (i, h) in heads.iter().enumerate() {
                let Some((k, _)) = h else { continue };
                match best {
                    None => best = Some(i),
                    Some(bi) => {
                        let bk = &heads[bi].as_ref().unwrap().0;
                        if cmp_keys(k, bk, &self.asc) == Ordering::Less {
                            best = Some(i);
                        }
                    }
                }
            }
            let Some(bi) = best else { break };
            let (_, row) = heads[bi].take().unwrap();
            heads[bi] = readers[bi].next()?;

            if skipped < self.offset {
                skipped += 1;
            } else {
                out.push(row);
                if let Some(l) = self.limit {
                    if out.len() >= l {
                        break;
                    }
                }
            }
        }
        for p in &paths {
            let _ = std::fs::remove_file(p);
        }
        Ok(out)
    }
}

impl Drop for Sorter {
    fn drop(&mut self) {
        // Best-effort cleanup for any run not consumed by finish() (already
        // unlinked on Unix, so typically a no-op).
        for (p, _) in &self.runs {
            let _ = std::fs::remove_file(p);
        }
    }
}

#[cfg(test)]
mod cleanup_tests {
    use super::*;

    #[test]
    fn parses_owning_pid_from_tempfile_names() {
        assert_eq!(tempfile_pid("elyrasql-sort-1234-7.tmp"), Some(1234));
        assert_eq!(tempfile_pid("elyrasql-agg-999-3-12.tmp"), Some(999));
        assert_eq!(tempfile_pid("elyrasql-other-5.tmp"), None);
        assert_eq!(tempfile_pid("unrelated.tmp"), None);
    }

    #[test]
    fn cleanup_never_removes_live_own_files() {
        // A file tagged with our own live PID must survive cleanup.
        let me = std::process::id();
        let path = std::env::temp_dir().join(format!("elyrasql-agg-{me}-424242-0.tmp"));
        std::fs::write(&path, b"x").unwrap();
        cleanup_stale_tempfiles();
        assert!(
            path.exists(),
            "cleanup must not delete a live process's files"
        );
        let _ = std::fs::remove_file(&path);

        // On Linux we can confirm a clearly-dead PID's file is reclaimed.
        if std::path::Path::new("/proc").exists() {
            let dead = std::env::temp_dir().join("elyrasql-sort-2147480000-1.tmp");
            std::fs::write(&dead, b"x").unwrap();
            assert!(pid_is_dead(2_147_480_000));
            cleanup_stale_tempfiles();
            assert!(
                !dead.exists(),
                "cleanup must reclaim a dead process's files"
            );
        }
    }
}
