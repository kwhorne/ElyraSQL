//! Append-only binary log for point-in-time recovery, stored as rotating
//! segment files in a directory (`binlog.000001`, `binlog.000002`, ...).
//!
//! Every committed write-set is appended as an ordered, length-prefixed record
//! `(lsn, timestamp_ms, puts, deletes)`. Replaying the segments in order onto a
//! base (an empty database or a restored backup) is idempotent and reconstructs
//! the exact state as of any chosen LSN or timestamp. Old segments can be pruned
//! once they are covered by a newer base backup.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use elyra_core::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::Db;

/// One committed write-set as stored in the binlog.
#[derive(Serialize, Deserialize)]
pub struct BinlogRecord {
    pub lsn: u64,
    pub ts_ms: u64,
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
    pub deletes: Vec<Vec<u8>>,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn segment_name(seq: u64) -> String {
    format!("binlog.{seq:06}")
}

/// Parse a `binlog.NNNNNN` file name into its sequence number.
fn parse_seq(name: &str) -> Option<u64> {
    name.strip_prefix("binlog.").and_then(|s| s.parse().ok())
}

/// Sorted sequence numbers of the segments present in `dir`.
fn list_seqs(dir: &Path) -> Result<Vec<u64>> {
    let mut seqs = Vec::new();
    if dir.exists() {
        for entry in std::fs::read_dir(dir).map_err(Error::Io)? {
            let entry = entry.map_err(Error::Io)?;
            if let Some(seq) = entry.file_name().to_str().and_then(parse_seq) {
                seqs.push(seq);
            }
        }
    }
    seqs.sort_unstable();
    Ok(seqs)
}

fn segment_max_bytes() -> u64 {
    use std::sync::OnceLock;
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        let mb = std::env::var("ELYRASQL_BINLOG_SEGMENT_MB")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|m| *m > 0)
            .unwrap_or(128);
        mb * 1024 * 1024
    })
}

/// Buffered append-only binlog writer (owned by the single writer thread). Opens
/// a fresh segment on start and rotates when a segment reaches the size cap.
pub struct BinlogWriter {
    dir: PathBuf,
    seq: u64,
    w: BufWriter<File>,
    size: u64,
    segment_max: u64,
}

impl BinlogWriter {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(Error::Io)?;
        let seq = list_seqs(&dir)?.into_iter().max().unwrap_or(0) + 1;
        let f = File::options()
            .create(true)
            .append(true)
            .open(dir.join(segment_name(seq)))
            .map_err(Error::Io)?;
        Ok(BinlogWriter {
            dir,
            seq,
            w: BufWriter::new(f),
            size: 0,
            segment_max: segment_max_bytes(),
        })
    }

    fn rotate(&mut self) -> Result<()> {
        self.w.flush().map_err(Error::Io)?;
        self.seq += 1;
        let f = File::options()
            .create(true)
            .append(true)
            .open(self.dir.join(segment_name(self.seq)))
            .map_err(Error::Io)?;
        self.w = BufWriter::new(f);
        self.size = 0;
        Ok(())
    }

    /// Append and flush one record, rotating to a new segment past the cap.
    pub fn append(
        &mut self,
        lsn: u64,
        puts: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
    ) -> Result<()> {
        let rec = BinlogRecord {
            lsn,
            ts_ms: now_ms(),
            puts: puts.to_vec(),
            deletes: deletes.to_vec(),
        };
        let bytes = bincode::serialize(&rec).map_err(|e| Error::Storage(e.to_string()))?;
        self.w
            .write_all(&(bytes.len() as u32).to_le_bytes())
            .map_err(Error::Io)?;
        self.w.write_all(&bytes).map_err(Error::Io)?;
        self.w.flush().map_err(Error::Io)?;
        self.size += bytes.len() as u64 + 4;
        if self.size >= self.segment_max {
            self.rotate()?;
        }
        Ok(())
    }
}

/// `(segment_name, size_bytes)` for every segment, in order (for
/// `SHOW BINARY LOGS`).
pub fn list_segments(dir: impl AsRef<Path>) -> Result<Vec<(String, u64)>> {
    let dir = dir.as_ref();
    let mut out = Vec::new();
    for seq in list_seqs(dir)? {
        let name = segment_name(seq);
        let size = std::fs::metadata(dir.join(&name))
            .map(|m| m.len())
            .unwrap_or(0);
        out.push((name, size));
    }
    Ok(out)
}

/// Delete every segment strictly before `to` (e.g. `binlog.000004`). Returns the
/// number of segments removed.
pub fn purge(dir: impl AsRef<Path>, to: &str) -> Result<u64> {
    let dir = dir.as_ref();
    let boundary =
        parse_seq(to).ok_or_else(|| Error::Query(format!("invalid binlog name: {to}")))?;
    let mut removed = 0;
    for seq in list_seqs(dir)? {
        if seq < boundary {
            std::fs::remove_file(dir.join(segment_name(seq))).map_err(Error::Io)?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Read every record from one segment file into memory.
fn read_segment_records(path: &Path) -> Result<Vec<BinlogRecord>> {
    let mut r = BufReader::new(File::open(path).map_err(Error::Io)?);
    let mut out = Vec::new();
    loop {
        let mut len = [0u8; 4];
        match r.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }
        let n = u32::from_le_bytes(len) as usize;
        if n > elyra_core::max_frame_bytes() {
            return Err(Error::Storage("binlog record too large (corrupt?)".into()));
        }
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf).map_err(Error::Io)?;
        out.push(bincode::deserialize(&buf).map_err(|e| Error::Storage(e.to_string()))?);
    }
    Ok(out)
}

/// The smallest LSN still available in the binlog (the start of the oldest
/// segment), or `None` if the binlog is empty.
pub fn earliest_lsn(dir: impl AsRef<Path>) -> Result<Option<u64>> {
    let dir = dir.as_ref();
    for seq in list_seqs(dir)? {
        let recs = read_segment_records(&dir.join(segment_name(seq)))?;
        if let Some(first) = recs.first() {
            return Ok(Some(first.lsn));
        }
    }
    Ok(None)
}

/// The highest LSN recorded in the binlog (0 if empty). Used to make the LSN
/// counter monotonic across primary restarts.
pub fn max_lsn(dir: impl AsRef<Path>) -> Result<u64> {
    let dir = dir.as_ref();
    let mut max = 0;
    if let Some(&last) = list_seqs(dir)?.last() {
        if let Some(rec) = read_segment_records(&dir.join(segment_name(last)))?.last() {
            max = rec.lsn;
        }
    }
    Ok(max)
}

/// Collect binlog records with `after_lsn < lsn <= up_to_lsn`, in order. Used to
/// stream an incremental catch-up to a reconnecting replica.
pub fn read_since(
    dir: impl AsRef<Path>,
    after_lsn: u64,
    up_to_lsn: u64,
) -> Result<Vec<BinlogRecord>> {
    let dir = dir.as_ref();
    let mut out = Vec::new();
    for seq in list_seqs(dir)? {
        for rec in read_segment_records(&dir.join(segment_name(seq)))? {
            if rec.lsn > after_lsn && rec.lsn <= up_to_lsn {
                out.push(rec);
            }
        }
    }
    Ok(out)
}

fn replay_segment(
    path: &Path,
    db: &Db,
    until_lsn: Option<u64>,
    until_ts: Option<u64>,
    applied: &mut u64,
) -> Result<bool> {
    let mut r = BufReader::new(File::open(path).map_err(Error::Io)?);
    loop {
        let mut len = [0u8; 4];
        match r.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(true),
            Err(e) => return Err(Error::Io(e)),
        }
        let n = u32::from_le_bytes(len) as usize;
        if n > elyra_core::max_frame_bytes() {
            return Err(Error::Storage("binlog record too large (corrupt?)".into()));
        }
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf).map_err(Error::Io)?;
        let rec: BinlogRecord =
            bincode::deserialize(&buf).map_err(|e| Error::Storage(e.to_string()))?;
        if until_lsn.is_some_and(|l| rec.lsn > l) || until_ts.is_some_and(|t| rec.ts_ms > t) {
            return Ok(false); // stop
        }
        // Apply synchronously via the writer (blocking bridge for the CLI tool).
        futures_apply(db, rec.puts, rec.deletes)?;
        *applied += 1;
    }
}

/// Apply one write-set, blocking on the async commit (used by the offline replay
/// tool).
fn futures_apply(db: &Db, puts: Vec<(Vec<u8>, Vec<u8>)>, deletes: Vec<Vec<u8>>) -> Result<()> {
    tokio::runtime::Handle::current().block_on(db.commit(puts, deletes))
}

/// Replay all segments in `dir` in order onto `db`, up to `until_lsn` and/or
/// `until_ts`. Returns the number of records applied.
pub async fn replay(
    dir: impl AsRef<Path>,
    db: &Db,
    until_lsn: Option<u64>,
    until_ts: Option<u64>,
) -> Result<u64> {
    let dir = dir.as_ref();
    let mut applied = 0u64;
    let dirp = dir.to_path_buf();
    let db = db.clone();
    // Run the blocking file IO + apply on a blocking thread.
    tokio::task::spawn_blocking(move || -> Result<u64> {
        for seq in list_seqs(&dirp)? {
            let cont = replay_segment(
                &dirp.join(segment_name(seq)),
                &db,
                until_lsn,
                until_ts,
                &mut applied,
            )?;
            if !cont {
                break;
            }
        }
        Ok(applied)
    })
    .await
    .map_err(|e| Error::Storage(format!("replay task failed: {e}")))?
}
