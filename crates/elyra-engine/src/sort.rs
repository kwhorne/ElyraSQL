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

use elyra_core::{Collation, Error, Result, Value};

/// Largest `offset + limit` that uses the in-memory top-N heap (rather than
/// spilling). Above this, a bounded `LIMIT` still goes through the external
/// sort so a pathological `LIMIT 100000000` cannot blow up memory.
const TOPN_CAP: usize = 1_000_000;

/// The external-sort spill budget in rows, from `ELYRASQL_SORT_MAX_ROWS`
/// (default 1,000,000). Rows beyond this are spilled to a temp file.
/// Reads the env variable on every call so tests can reconfigure the budget.
pub fn sort_max_rows() -> usize {
    std::env::var("ELYRASQL_SORT_MAX_ROWS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1_000_000)
}

/// Compare two precomputed key vectors under per-key asc/desc flags and text
/// collations (`Bin` sorts text case-sensitively).
fn cmp_keys(a: &[Value], b: &[Value], asc: &[bool], colls: &[Collation]) -> Ordering {
    for (i, &asc) in asc.iter().enumerate() {
        let coll = colls.get(i).copied().unwrap_or(Collation::Ci);
        let ord = a[i].total_cmp_coll(&b[i], coll);
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
    colls: std::sync::Arc<Vec<Collation>>,
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        cmp_keys(&self.keys, &other.keys, &self.asc, &self.colls) == Ordering::Equal
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
        cmp_keys(&self.keys, &other.keys, &self.asc, &self.colls)
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
/// running instances are never disturbed; a no-op where process liveness can't
/// be determined or the temp dir can't be read.
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

/// True only when POSIX `kill(pid, 0)` confirms that the process is gone.
/// `EPERM` means the process exists under another user; every other unexpected
/// error is treated as possibly alive so cleanup cannot delete a live owner's
/// files.
#[cfg(unix)]
fn pid_is_dead(pid: u32) -> bool {
    use rustix::io::Errno;
    use rustix::process::{test_kill_process, Pid};

    let Ok(raw_pid) = i32::try_from(pid) else {
        return false;
    };
    let Some(pid) = Pid::from_raw(raw_pid) else {
        return false;
    };
    match test_kill_process(pid) {
        Err(Errno::SRCH) => true,
        Ok(()) | Err(Errno::PERM) => false,
        Err(_) => false,
    }
}

#[cfg(not(unix))]
fn pid_is_dead(_pid: u32) -> bool {
    false
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
    colls: std::sync::Arc<Vec<Collation>>,
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
    pub fn new(
        asc: Vec<bool>,
        colls: Vec<Collation>,
        offset: usize,
        limit: Option<usize>,
        max_rows: usize,
    ) -> Self {
        let bounded = limit
            .map(|l| offset.saturating_add(l))
            .unwrap_or(usize::MAX);
        let topn = bounded <= TOPN_CAP;
        Sorter {
            asc: std::sync::Arc::new(asc),
            colls: std::sync::Arc::new(colls),
            offset,
            limit,
            max_rows: max_rows.max(1),
            topn,
            heap: BinaryHeap::new(),
            buffer: Vec::new(),
            runs: Vec::new(),
        }
    }

    /// Would a row with these sort keys be kept if pushed right now?
    ///
    /// This is the *admission test* callers use for late materialisation: with
    /// `ORDER BY x LIMIT 100` over 200k rows, ~199,900 rows lose the test, so
    /// building their `keys`/`row` vectors (and decoding the columns behind
    /// them) is pure waste. Compute the keys into a scratch buffer, ask here,
    /// and only materialise on `true`.
    ///
    /// The test is deliberately the *same* comparison [`push`](Self::push)
    /// makes, in the same order, so a row admitted here is exactly a row `push`
    /// would have kept -- an admission test that disagreed with the heap
    /// comparator on even one tie would silently return wrong rows.
    /// Outside top-N mode every row is retained, so it is always `true`.
    pub fn admits(&self, keys: &[Value]) -> bool {
        if !self.topn {
            return true;
        }
        let n = self.offset.saturating_add(self.limit.unwrap_or(0));
        if n == 0 {
            return false;
        }
        if self.heap.len() < n {
            return true;
        }
        match self.heap.peek() {
            Some(top) => cmp_keys(keys, &top.keys, &self.asc, &self.colls) == Ordering::Less,
            // Unreachable (n > 0 and the heap is full), but keeping the row is
            // the safe answer: `push` decides for real.
            None => true,
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
                    colls: self.colls.clone(),
                });
            } else if let Some(top) = self.heap.peek() {
                // Replace the worst kept row if this one is better.
                if cmp_keys(&keys, &top.keys, &self.asc, &self.colls) == Ordering::Less {
                    self.heap.pop();
                    self.heap.push(Ranked {
                        keys,
                        row,
                        asc: self.asc.clone(),
                        colls: self.colls.clone(),
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
        let colls = self.colls.clone();
        self.buffer
            .sort_by(|a, b| cmp_keys(&a.0, &b.0, &asc, &colls));
        let path = temp_path();
        // Read *and* write: the merge phase reads every run back through this
        // same handle after seeking to 0. `File::create` opens write-only, so
        // reading from it failed with EBADF ("Bad file descriptor") -- which
        // meant an unbounded ORDER BY that spilled never worked at all.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
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
            ranked.sort_by(|a, b| cmp_keys(&a.keys, &b.keys, &self.asc, &self.colls));
            let rows: Vec<Vec<Value>> = ranked.into_iter().map(|r| r.row).collect();
            let start = self.offset.min(rows.len());
            return Ok(rows[start..].to_vec());
        }

        if self.runs.is_empty() {
            // Everything fit in memory: a plain sort.
            let asc = self.asc.clone();
            let colls = self.colls.clone();
            let mut buffer = std::mem::take(&mut self.buffer);
            buffer.sort_by(|a, b| cmp_keys(&a.0, &b.0, &asc, &colls));
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
                        if cmp_keys(k, bk, &self.asc, &self.colls) == Ordering::Less {
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
mod admission_tests {
    use super::*;

    /// Deterministic pseudo-random ints with heavy ties, so the admission test
    /// meets the tie cases where a disagreement with the heap comparator would
    /// silently change which rows a `LIMIT` returns.
    fn keys_seq(n: usize, distinct: i64) -> Vec<i64> {
        let mut s = 12345u64;
        (0..n)
            .map(|_| {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((s >> 33) as i64) % distinct
            })
            .collect()
    }

    fn sorter(asc: bool, offset: usize, limit: Option<usize>) -> Sorter {
        Sorter::new(vec![asc], vec![Collation::Ci], offset, limit, 1_000_000)
    }

    /// Skipping every row `admits` rejects must produce byte-identical output to
    /// pushing every row -- that equivalence is the whole safety argument for
    /// late materialisation.
    #[test]
    fn admission_test_agrees_with_push() {
        for (asc, offset, limit) in [
            (true, 0, Some(1)),
            (true, 0, Some(10)),
            (false, 0, Some(10)),
            (true, 5, Some(10)),
            (true, 0, Some(5000)), // limit exceeds the input
            (true, 0, None),       // unbounded: nothing may be rejected
            (true, 3, None),
        ] {
            for distinct in [3, 40, 1000] {
                let mut all = sorter(asc, offset, limit);
                let mut admitted = sorter(asc, offset, limit);
                let mut skipped = 0usize;
                for (i, k) in keys_seq(500, distinct).into_iter().enumerate() {
                    let keys = vec![Value::Int(k)];
                    let row = vec![Value::Int(k), Value::Int(i as i64)];
                    all.push(keys.clone(), row.clone()).unwrap();
                    if admitted.admits(&keys) {
                        admitted.push(keys, row).unwrap();
                    } else {
                        skipped += 1;
                    }
                }
                assert_eq!(
                    all.finish().unwrap(),
                    admitted.finish().unwrap(),
                    "asc={asc} offset={offset} limit={limit:?} distinct={distinct}"
                );
                match limit {
                    None => assert_eq!(skipped, 0, "unbounded sorts must admit every row"),
                    // A limit wider than the input keeps everything.
                    Some(l) if offset + l >= 500 => assert_eq!(skipped, 0),
                    Some(_) => assert!(skipped > 0, "expected the top-N test to reject rows"),
                }
            }
        }
    }

    /// `LIMIT 0` keeps nothing, so nothing should ever be materialised for it.
    #[test]
    fn zero_limit_admits_nothing() {
        let s = sorter(true, 0, Some(0));
        assert!(!s.admits(&[Value::Int(1)]));
    }
}

#[cfg(test)]
mod spill_tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    fn ints(n: usize) -> Vec<i64> {
        let mut state = 99u64;
        (0..n)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((state >> 33) as i64) % 500
            })
            .collect()
    }

    /// Sorting past the spill budget must produce exactly what an in-memory sort
    /// of the same rows produces.
    ///
    /// Nothing exercised this before: the budget defaults to a million rows, so
    /// no test or benchmark ever wrote a run file -- and the merge phase read
    /// runs back through a handle opened by `File::create`, which is write-only,
    /// so every spilling `ORDER BY` failed with EBADF. A regression here has to
    /// force a *small* budget, or it tests the in-memory path again.
    #[test]
    fn spilled_sort_matches_the_in_memory_sort() {
        let keys = ints(2000);

        for (asc, offset, limit) in [
            (true, 0, None),
            (false, 0, None),
            (true, 1990, None),
            (true, 5, Some(20)),
        ] {
            // Small budget: 2000 rows over 200 is ten runs to merge.
            let mut spilling = Sorter::new(vec![asc], vec![Collation::Ci], offset, limit, 200);
            let mut in_memory =
                Sorter::new(vec![asc], vec![Collation::Ci], offset, limit, usize::MAX);
            for (i, k) in keys.iter().enumerate() {
                let key = vec![Value::Int(*k)];
                let row = vec![Value::Int(*k), Value::Int(i as i64)];
                spilling.push(key.clone(), row.clone()).unwrap();
                in_memory.push(key, row).unwrap();
            }
            let spilled = spilling.finish().unwrap();
            let memory = in_memory.finish().unwrap();
            assert_eq!(spilled, memory, "asc={asc} offset={offset} limit={limit:?}");
            assert!(!spilled.is_empty());
        }
    }

    /// A single run still has to be read back -- the one-run case skips the
    /// k-way merge and is easy to get wrong separately.
    #[test]
    fn a_single_spilled_run_is_read_back() {
        let mut sorter = Sorter::new(vec![true], vec![Collation::Ci], 0, None, 10);
        for i in (0..20).rev() {
            sorter
                .push(vec![Value::Int(i)], vec![Value::Int(i)])
                .unwrap();
        }
        let out = sorter.finish().unwrap();
        assert_eq!(out.len(), 20);
        assert_eq!(out[0], vec![Value::Int(0)]);
        assert_eq!(out[19], vec![Value::Int(19)]);
    }

    /// `temp_path()` must return a writable path under the system temp
    /// directory. The scratch Docker image bundles no `/tmp` unless it is
    /// explicitly copied into the runtime stage, so this test documents the
    /// invariant that the spill infrastructure depends on. If `temp_dir()` is
    /// missing or unwritable this test fails, which is by design.
    #[test]
    fn temp_path_is_writable_in_temp_dir() {
        assert!(
            std::env::temp_dir().exists(),
            "temp_dir must exist and be readable"
        );
        let p = temp_path();
        assert!(
            p.starts_with(std::env::temp_dir()),
            "temp_path must live under temp_dir"
        );
        let mut f = File::create(&p).expect("must be able to create a temp file");
        f.write_all(b"hello").unwrap();
        f.flush().unwrap();
        drop(f);
        let _ = fs::remove_file(&p);
    }

    /// The Sorter respects the `ELYRASQL_SORT_MAX_ROWS` spill budget: when the
    /// in-memory buffer fills, it spills a run to disk and clears the buffer,
    /// keeping peak memory bounded.
    #[test]
    fn sorter_spills_when_buffer_exceeds_budget() {
        std::env::set_var("ELYRASQL_SORT_MAX_ROWS", "10");
        let budget = sort_max_rows();
        assert_eq!(budget, 10);

        let mut s = Sorter::new(vec![true], vec![Collation::Ci], 0, None, budget);

        for i in 0..25i64 {
            s.push(vec![Value::Int(i)], vec![Value::Int(i)]).unwrap();
        }

        assert!(
            s.buffer.len() <= 10,
            "buffer must not exceed the spill budget"
        );
        assert!(
            !s.runs.is_empty(),
            "spill should have created at least one run"
        );

        let rows = s.finish().unwrap();
        assert_eq!(rows.len(), 25, "finish must return every pushed row");
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

        // Unix provides kill(pid, 0), so a clearly-dead PID is reclaimed.
        #[cfg(unix)]
        {
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
