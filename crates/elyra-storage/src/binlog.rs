//! Append-only binary log for point-in-time recovery, stored as rotating
//! segment files in a directory (`binlog.000001`, `binlog.000002`, ...).
//!
//! Every committed write-set is appended as an ordered, length-prefixed record
//! `(lsn, timestamp_ms, puts, deletes)`. Replaying the segments in order onto a
//! base (an empty database or a restored backup) is idempotent and reconstructs
//! the exact state as of any chosen LSN or timestamp. Old segments can be pruned
//! once they are covered by a newer base backup.

use std::fs::File;
use std::io::{Read, Write};
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

/// Starts every segment written since 1.12.18. Its records carry a CRC32 of
/// the payload: `[len u32][crc32 u32][payload]`. A segment without it is the
/// original format, `[len u32][payload]`, and is still read. The magic cannot be
/// mistaken for an original record: read as a length it is far past any frame.
const SEGMENT_MAGIC: &[u8; 8] = b"ELYBLOG2";

/// A record to append, borrowed: `(lsn, puts, deletes)`.
pub type RecordRef<'a> = (u64, &'a [(Vec<u8>, Vec<u8>)], &'a [Vec<u8>]);

/// Where a group of records starts, so a group whose data commit failed can be
/// taken back out of the binlog.
#[derive(Clone, Copy, Debug)]
pub struct GroupMark {
    size: u64,
}

/// Append-only binlog writer, owned by the single writer thread.
///
/// A group of records is written before the database commit that holds the
/// same write-sets (see `Db`'s writer), and is durable no later than it, so
/// the binlog on disk always holds at least what the database does: a crash
/// between the two leaves records the database lacks, which [`crate::Db`]
/// replays at the next open, never the reverse.
///
/// With per-commit durability each group is fsynced before its commit. With
/// relaxed durability the database is made durable only by a periodic flush,
/// so the binlog is fsynced there instead, just before it (see
/// [`BinlogSync`]). Opens a fresh segment on start and moves to a new one
/// once a segment passes the size cap.
pub struct BinlogWriter {
    dir: PathBuf,
    seq: u64,
    file: File,
    size: u64,
    segment_max: u64,
    sync_each_group: bool,
    shared: BinlogSync,
}

/// The binlog's durability point under relaxed durability, shared by the
/// writer and the periodic flush.
///
/// The flush must make the binlog durable before the database: if the writer
/// committed a group in between, the database would hold it durably and the
/// binlog not. So the writer holds [`BinlogSync::hold`] across logging and
/// committing a group, and the flush holds it across syncing the binlog and
/// flushing the database.
#[derive(Clone)]
pub struct BinlogSync {
    current: std::sync::Arc<std::sync::Mutex<File>>,
}

impl BinlogSync {
    /// Exclude the other side (see the type's docs) while the guard lives.
    pub fn hold(&self) -> std::sync::MutexGuard<'_, File> {
        self.current.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Make the binlog durable, then run `make_database_durable`, with the
    /// writer held off in between.
    pub fn durable_then<T>(&self, make_database_durable: impl FnOnce() -> Result<T>) -> Result<T> {
        let file = self.hold();
        file.sync_data().map_err(Error::Io)?;
        make_database_durable()
    }
}

impl BinlogWriter {
    /// Open the binlog in `dir`. `sync_each_group` fsyncs every group before
    /// its commit (per-commit durability); otherwise the owner must make it
    /// durable through [`BinlogWriter::sync_handle`] before the database.
    pub fn open(dir: impl AsRef<Path>, sync_each_group: bool) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(Error::Io)?;
        let seqs = list_seqs(&dir)?;
        // A crash mid-append leaves a partial record at the end of the newest
        // segment; nothing after it was acknowledged, so it is cut off.
        if let Some(&last) = seqs.last() {
            repair_tail(&dir.join(segment_name(last)))?;
        }
        let seq = seqs.last().copied().unwrap_or(0) + 1;
        let (file, size) = new_segment(&dir, seq)?;
        let shared = BinlogSync {
            current: std::sync::Arc::new(std::sync::Mutex::new(
                file.try_clone().map_err(Error::Io)?,
            )),
        };
        Ok(BinlogWriter {
            dir,
            seq,
            file,
            size,
            segment_max: segment_max_bytes(),
            sync_each_group,
            shared,
        })
    }

    /// The durability point for relaxed durability (see [`BinlogSync`]).
    pub fn sync_handle(&self) -> BinlogSync {
        self.shared.clone()
    }

    /// Durably append one group of records (`(lsn, puts, deletes)`, in LSN
    /// order): one write and one fsync for the group. Returns where it started.
    pub fn append_group(&mut self, records: &[RecordRef<'_>]) -> Result<GroupMark> {
        // A segment holds at least one group, however large.
        if self.size > SEGMENT_MAGIC.len() as u64 && self.size >= self.segment_max {
            // The segment being left is made durable first: from here on only
            // the new one is synced.
            self.file.sync_data().map_err(Error::Io)?;
            let (file, size) = new_segment(&self.dir, self.seq + 1)?;
            *self.shared.hold() = file.try_clone().map_err(Error::Io)?;
            self.seq += 1;
            self.file = file;
            self.size = size;
        }
        let ts_ms = now_ms();
        let mut buf = Vec::new();
        for (lsn, puts, deletes) in records {
            let rec = BinlogRecord {
                lsn: *lsn,
                ts_ms,
                puts: puts.to_vec(),
                deletes: deletes.to_vec(),
            };
            let payload = bincode::serialize(&rec).map_err(|e| Error::Storage(e.to_string()))?;
            let len = u32::try_from(payload.len())
                .map_err(|_| Error::Storage("binlog record over 4 GiB".into()))?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
            buf.extend_from_slice(&payload);
        }
        let mark = GroupMark { size: self.size };
        let written = self.file.write_all(&buf).and_then(|()| {
            if self.sync_each_group {
                self.file.sync_data()
            } else {
                Ok(())
            }
        });
        if let Err(e) = written {
            // Leave no partial group behind for a reader to stop at.
            let _ = self.undo(mark);
            return Err(Error::Io(e));
        }
        self.size += buf.len() as u64;
        Ok(mark)
    }

    /// Take the group appended at `mark` back out: its database commit failed,
    /// so it never happened.
    pub fn undo(&mut self, mark: GroupMark) -> Result<()> {
        self.file.set_len(mark.size).map_err(Error::Io)?;
        self.file.sync_data().map_err(Error::Io)?;
        self.size = mark.size;
        Ok(())
    }
}

/// Create segment `seq` with its header, durable together with its directory
/// entry. Returns the file, positioned for appending, and its size.
fn new_segment(dir: &Path, seq: u64) -> Result<(File, u64)> {
    let mut file = File::options()
        .create(true)
        .append(true)
        .open(dir.join(segment_name(seq)))
        .map_err(Error::Io)?;
    file.write_all(SEGMENT_MAGIC).map_err(Error::Io)?;
    file.sync_all().map_err(Error::Io)?;
    sync_dir(dir)?;
    Ok((file, SEGMENT_MAGIC.len() as u64))
}

fn sync_dir(dir: &Path) -> Result<()> {
    // Directories cannot be opened for syncing on every platform; where they
    // can, this makes a new segment's name durable.
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Truncate a segment after its last complete, intact record.
fn repair_tail(path: &Path) -> Result<()> {
    let read = read_segment(path)?;
    if let Some(cut) = read.torn_at {
        let len = std::fs::metadata(path).map_err(Error::Io)?.len();
        tracing::warn!(
            segment = %path.display(),
            dropped_bytes = len - cut,
            "binlog ended in a partial record (an interrupted write); truncating it"
        );
        let f = File::options().write(true).open(path).map_err(Error::Io)?;
        f.set_len(cut).map_err(Error::Io)?;
        f.sync_all().map_err(Error::Io)?;
    }
    Ok(())
}

/// The records of one segment, and where they stop if the segment does not end
/// cleanly.
struct SegmentRead {
    records: Vec<BinlogRecord>,
    /// Offset of a partial or damaged record at the end of the file. Only the
    /// newest segment can have one -- the writer may be mid-append, or crashed
    /// there -- so callers treat it as the end of the log there and as damage
    /// anywhere else.
    torn_at: Option<u64>,
}

/// Read every record of one segment, in either format.
fn read_segment(path: &Path) -> Result<SegmentRead> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(Error::Io)?
        .read_to_end(&mut bytes)
        .map_err(Error::Io)?;
    let checked = bytes.starts_with(SEGMENT_MAGIC);
    let mut pos = if checked { SEGMENT_MAGIC.len() } else { 0 };
    let header = if checked { 8 } else { 4 };
    let mut records = Vec::new();
    while pos < bytes.len() {
        let torn = SegmentRead {
            records: Vec::new(),
            torn_at: Some(pos as u64),
        };
        if bytes.len() - pos < header {
            return Ok(SegmentRead { records, ..torn });
        }
        let n = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        if n > elyra_core::max_frame_bytes() || bytes.len() - pos - header < n {
            return Ok(SegmentRead { records, ..torn });
        }
        let payload = &bytes[pos + header..pos + header + n];
        if checked {
            let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
            if crc32fast::hash(payload) != crc {
                return Ok(SegmentRead { records, ..torn });
            }
        }
        match bincode::deserialize(payload) {
            Ok(rec) => records.push(rec),
            Err(_) => return Ok(SegmentRead { records, ..torn }),
        }
        pos += header + n;
    }
    Ok(SegmentRead {
        records,
        torn_at: None,
    })
}

/// Every record, in order, across all segments. Only the newest segment may
/// end in a partial record (a write in progress); anywhere else it is damage.
fn read_all(dir: &Path) -> Result<Vec<BinlogRecord>> {
    let seqs = list_seqs(dir)?;
    let mut out = Vec::new();
    for (i, seq) in seqs.iter().enumerate() {
        let path = dir.join(segment_name(*seq));
        let read = read_segment(&path)?;
        if let Some(at) = read.torn_at {
            if i + 1 != seqs.len() {
                return Err(Error::Storage(format!(
                    "binlog segment {} is damaged at byte {at}",
                    path.display()
                )));
            }
        }
        out.extend(read.records);
    }
    Ok(out)
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

/// The smallest LSN still available in the binlog (the start of the oldest
/// segment), or `None` if the binlog is empty.
pub fn earliest_lsn(dir: impl AsRef<Path>) -> Result<Option<u64>> {
    let dir = dir.as_ref();
    for seq in list_seqs(dir)? {
        if let Some(first) = read_segment(&dir.join(segment_name(seq)))?.records.first() {
            return Ok(Some(first.lsn));
        }
    }
    Ok(None)
}

/// The highest LSN recorded in the binlog (0 if empty). Used to make the LSN
/// counter monotonic across primary restarts.
///
/// Searches back from the newest segment: every start opens a fresh one, so
/// the newest is often empty, and reading only it put the counter back to 0
/// after two restarts without a write.
pub fn max_lsn(dir: impl AsRef<Path>) -> Result<u64> {
    let dir = dir.as_ref();
    for seq in list_seqs(dir)?.into_iter().rev() {
        if let Some(rec) = read_segment(&dir.join(segment_name(seq)))?.records.last() {
            return Ok(rec.lsn);
        }
    }
    Ok(0)
}

/// Collect binlog records with `after_lsn < lsn <= up_to_lsn`, in order. Used to
/// stream an incremental catch-up to a reconnecting replica, and to recover
/// write-sets the binlog holds and the database does not.
pub fn read_since(
    dir: impl AsRef<Path>,
    after_lsn: u64,
    up_to_lsn: u64,
) -> Result<Vec<BinlogRecord>> {
    Ok(read_all(dir.as_ref())?
        .into_iter()
        .filter(|rec| rec.lsn > after_lsn && rec.lsn <= up_to_lsn)
        .collect())
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
    let dirp = dir.as_ref().to_path_buf();
    let db = db.clone();
    // Run the blocking file IO + apply on a blocking thread.
    tokio::task::spawn_blocking(move || -> Result<u64> {
        let mut applied = 0u64;
        for rec in read_all(&dirp)? {
            if until_lsn.is_some_and(|l| rec.lsn > l) || until_ts.is_some_and(|t| rec.ts_ms > t) {
                break;
            }
            futures_apply(&db, rec.puts, rec.deletes)?;
            applied += 1;
        }
        Ok(applied)
    })
    .await
    .map_err(|e| Error::Storage(format!("replay task failed: {e}")))?
}

#[cfg(test)]
mod tests {
    //! The binlog must hold every committed write-set, in commit order, and
    //! survive a crash at any point of an append. It used to be written after
    //! the database commit and only flushed: a crash could keep a write the
    //! binlog never saw, a partial record at the end of a segment made it
    //! unreadable and reset the LSN counter to 0, and so did two restarts
    //! without a write.
    use super::*;
    use crate::{Db, Validation};

    fn kv(k: &str, v: &str) -> (Vec<u8>, Vec<u8>) {
        (k.as_bytes().to_vec(), v.as_bytes().to_vec())
    }

    fn newest_segment(dir: &Path) -> PathBuf {
        dir.join(segment_name(*list_seqs(dir).unwrap().last().unwrap()))
    }

    /// Open, write, close: what a server start, some commits and a stop leave.
    async fn session(data: &Path, log: &Path, writes: &[(&str, &str)]) -> u64 {
        let db = Db::open_with_binlog(data, Some(log.to_path_buf())).unwrap();
        for (k, v) in writes {
            db.commit(vec![kv(k, v)], vec![]).await.unwrap();
        }
        let lsn = db.current_lsn();
        let closed = db.close_waiter();
        drop(db);
        assert!(closed.wait(std::time::Duration::from_secs(5)));
        lsn
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn every_commit_is_logged_in_order_and_the_lsn_survives_restarts() {
        let tmp = tempfile::tempdir().unwrap();
        let (data, log) = (tmp.path().join("d.edb"), tmp.path().join("binlog"));
        assert_eq!(session(&data, &log, &[("a", "1"), ("b", "2")]).await, 2);
        // Two starts without a write leave two empty newest segments.
        assert_eq!(session(&data, &log, &[]).await, 2);
        assert_eq!(session(&data, &log, &[]).await, 2);
        assert_eq!(session(&data, &log, &[("c", "3")]).await, 3);
        let lsns: Vec<u64> = read_since(&log, 0, u64::MAX)
            .unwrap()
            .iter()
            .map(|r| r.lsn)
            .collect();
        assert_eq!(lsns, [1, 2, 3]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_commits_are_all_logged_once() {
        let tmp = tempfile::tempdir().unwrap();
        let db =
            Db::open_with_binlog(tmp.path().join("d.edb"), Some(tmp.path().join("log"))).unwrap();
        let tasks: Vec<_> = (0..8)
            .map(|t| {
                let db = db.clone();
                tokio::spawn(async move {
                    for i in 0..50 {
                        db.commit(vec![kv(&format!("k{t}-{i}"), "v")], vec![])
                            .await
                            .unwrap();
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        let recs = read_since(tmp.path().join("log"), 0, u64::MAX).unwrap();
        let lsns: Vec<u64> = recs.iter().map(|r| r.lsn).collect();
        assert_eq!(lsns, (1..=400).collect::<Vec<_>>());
        assert_eq!(db.current_lsn(), 400);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_conflicting_commit_is_not_logged() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("log");
        let db = Db::open_with_binlog(tmp.path().join("d.edb"), Some(log.clone())).unwrap();
        db.commit(vec![kv("a", "1")], vec![]).await.unwrap();
        let stale = Validation {
            keys: vec![(b"a".to_vec(), Some(b"0".to_vec()))],
            ranges: Vec::new(),
        };
        assert!(db
            .commit_validated(stale, vec![kv("a", "2")], vec![])
            .await
            .is_err());
        db.commit(vec![kv("b", "1")], vec![]).await.unwrap();
        let recs = read_since(&log, 0, u64::MAX).unwrap();
        assert_eq!(recs.iter().map(|r| r.lsn).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(recs[1].puts, vec![kv("b", "1")]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_partial_record_left_by_a_crash_is_cut_off() {
        let tmp = tempfile::tempdir().unwrap();
        let (data, log) = (tmp.path().join("d.edb"), tmp.path().join("binlog"));
        session(&data, &log, &[("a", "1"), ("b", "2")]).await;
        // A crash mid-append: a length and checksum, then half a payload.
        let seg = newest_segment(&log);
        let full = std::fs::metadata(&seg).unwrap().len();
        let mut f = File::options().append(true).open(&seg).unwrap();
        f.write_all(&500u32.to_le_bytes()).unwrap();
        f.write_all(&[7u8; 4 + 100]).unwrap();
        drop(f);
        assert_eq!(
            max_lsn(&log).unwrap(),
            2,
            "readers stop at the partial record"
        );
        assert_eq!(session(&data, &log, &[("c", "3")]).await, 3);
        assert_eq!(std::fs::metadata(&seg).unwrap().len(), full);
        assert_eq!(read_since(&log, 0, u64::MAX).unwrap().len(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_damaged_record_before_the_newest_segment_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let (data, log) = (tmp.path().join("d.edb"), tmp.path().join("binlog"));
        session(&data, &log, &[("a", "1"), ("b", "2")]).await;
        session(&data, &log, &[("c", "3")]).await;
        let first = log.join(segment_name(list_seqs(&log).unwrap()[0]));
        let mut bytes = std::fs::read(&first).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&first, bytes).unwrap();
        let Err(err) = read_since(&log, 0, u64::MAX) else {
            panic!("a damaged older segment must not read as fine");
        };
        assert!(err.to_string().contains("damaged"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_write_logged_but_not_committed_is_applied_at_open() {
        let tmp = tempfile::tempdir().unwrap();
        let (data, log) = (tmp.path().join("d.edb"), tmp.path().join("binlog"));
        session(&data, &log, &[("a", "1")]).await;
        // A crash after the binlog fsync, before the database commit.
        let puts = vec![kv("b", "2")];
        BinlogWriter::open(&log, true)
            .unwrap()
            .append_group(&[(2, puts.as_slice(), &[])])
            .unwrap();
        let db = Db::open_with_binlog(&data, Some(log.clone())).unwrap();
        assert_eq!(db.get(b"b".to_vec()).await.unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.current_lsn(), 2);
        db.commit(vec![kv("c", "3")], vec![]).await.unwrap();
        assert_eq!(db.current_lsn(), 3);
        let lsns: Vec<u64> = read_since(&log, 0, u64::MAX)
            .unwrap()
            .iter()
            .map(|r| r.lsn)
            .collect();
        assert_eq!(lsns, [1, 2, 3]);
    }

    #[test]
    fn relaxed_durability_syncs_the_binlog_at_the_flush() {
        // Without a per-group fsync the records still reach the file at once
        // (a process crash keeps them), and the flush's sync point holds the
        // writer off while it syncs.
        let tmp = tempfile::tempdir().unwrap();
        let mut w = BinlogWriter::open(tmp.path(), false).unwrap();
        let puts = vec![kv("a", "1")];
        w.append_group(&[(1, puts.as_slice(), &[])]).unwrap();
        assert_eq!(max_lsn(tmp.path()).unwrap(), 1);
        let sync = w.sync_handle();
        let flushed = sync.durable_then(|| Ok(7)).unwrap();
        assert_eq!(flushed, 7);
    }

    #[test]
    fn a_rotated_segment_keeps_its_records_and_the_sync_point_follows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = BinlogWriter::open(tmp.path(), false).unwrap();
        w.segment_max = 1; // every group starts a new segment
        let sync = w.sync_handle();
        for lsn in 1..=3 {
            let puts = vec![kv(&format!("k{lsn}"), "v")];
            w.append_group(&[(lsn, puts.as_slice(), &[])]).unwrap();
            sync.durable_then(|| Ok(())).unwrap();
        }
        assert_eq!(list_seqs(tmp.path()).unwrap().len(), 3);
        let lsns: Vec<u64> = read_since(tmp.path(), 0, u64::MAX)
            .unwrap()
            .iter()
            .map(|r| r.lsn)
            .collect();
        assert_eq!(lsns, [1, 2, 3]);
    }

    #[test]
    fn a_group_whose_commit_failed_is_taken_back_out() {
        let tmp = tempfile::tempdir().unwrap();
        let mut w = BinlogWriter::open(tmp.path(), true).unwrap();
        let one = vec![kv("a", "1")];
        let two = vec![kv("b", "2")];
        w.append_group(&[(1, one.as_slice(), &[])]).unwrap();
        let mark = w.append_group(&[(2, two.as_slice(), &[])]).unwrap();
        w.undo(mark).unwrap();
        assert_eq!(max_lsn(tmp.path()).unwrap(), 1);
        let three = vec![kv("c", "3")];
        w.append_group(&[(2, three.as_slice(), &[])]).unwrap();
        let recs = read_since(tmp.path(), 0, u64::MAX).unwrap();
        assert_eq!(recs.iter().map(|r| r.lsn).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(recs[1].puts, three);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_segment_in_the_original_format_is_still_read() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("binlog");
        std::fs::create_dir_all(&log).unwrap();
        let mut old = Vec::new();
        for lsn in 1..=2u64 {
            let rec = BinlogRecord {
                lsn,
                ts_ms: 0,
                puts: vec![kv(&format!("k{lsn}"), "v")],
                deletes: Vec::new(),
            };
            let payload = bincode::serialize(&rec).unwrap();
            old.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            old.extend_from_slice(&payload);
        }
        std::fs::write(log.join(segment_name(1)), old).unwrap();
        assert_eq!(max_lsn(&log).unwrap(), 2);
        let db = Db::open_with_binlog(tmp.path().join("d.edb"), Some(log.clone())).unwrap();
        assert_eq!(db.current_lsn(), 2);
        db.commit(vec![kv("k3", "v")], vec![]).await.unwrap();
        let restored = Db::in_memory().unwrap();
        assert_eq!(replay(&log, &restored, None, None).await.unwrap(), 3);
        assert_eq!(
            restored.get(b"k1".to_vec()).await.unwrap(),
            Some(b"v".to_vec())
        );
        assert_eq!(
            restored.get(b"k3".to_vec()).await.unwrap(),
            Some(b"v".to_vec())
        );
    }
}
