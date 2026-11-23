//! Raft replicated-log core (consensus foundation).
//!
//! This is the correctness-critical core of full Raft log replication: an
//! ordered, persistent log of `(term, index, data)` entries with the two
//! properties Raft depends on:
//!
//! * **Log matching / consistency + truncation.** `append_entries` accepts new
//!   entries only if the follower's log contains `prev_index`/`prev_term`
//!   (the AppendEntries consistency check), and truncates any conflicting
//!   suffix before appending — so followers converge to the leader's log and an
//!   entry is never applied out of a divergent branch.
//! * **The election restriction (§5.4.1).** `is_up_to_date` decides whether to
//!   grant a vote: a candidate's log must be at least as up-to-date (by last
//!   term, then last index) as the voter's, guaranteeing an elected leader holds
//!   every committed entry.
//!
//! Entries are applied to the state machine only once **committed** (present on
//! a quorum), so uncommitted entries on a minority that later get truncated are
//! never applied — the property that makes pre-commit (2-phase) replication
//! safe.
//!
//! The live cluster write path runs through it (`cluster.rs`): the leader
//! appends each write, replicates it with AppendEntries, commits it once a
//! majority has it, and only then applies it and acknowledges the client;
//! followers apply up to the leader's commit index.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// One replicated log entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

/// A persistent Raft log with commit/apply cursors.
#[derive(Default)]
pub struct RaftLog {
    /// Entries in index order. `entries[0].index` may be > 1 after compaction
    /// (not implemented yet), so always index via `at`/`term_at`.
    entries: Vec<LogEntry>,
    /// Highest index known to be committed (present on a quorum).
    commit_index: u64,
    /// Highest index applied to the state machine.
    last_applied: u64,
    /// Optional backing file for durability (append-only records).
    path: Option<PathBuf>,
    /// Highest index whose entry -- and every entry before it -- is durably in
    /// `path` as this log holds it.
    durable: u64,
    /// `path` is no longer a prefix of this log -- a suffix was truncated, the
    /// log compacted, or a write failed -- so the next persist rewrites it.
    dirty: bool,
    /// Compaction point: entries with index <= this have been discarded (their
    /// state lives in the applied state machine). `term_at(snapshot_index)`
    /// returns `snapshot_term` so the AppendEntries consistency check still works
    /// at the boundary.
    snapshot_index: u64,
    snapshot_term: u64,
}

impl RaftLog {
    pub fn new() -> Self {
        RaftLog::default()
    }

    /// Open a log backed by `path`, loading any persisted entries. The log is an
    /// append-only sequence of length-prefixed entry records. `commit_index` and
    /// `last_applied` are **not** persisted: on restart they start at 0 and the
    /// node re-applies the committed prefix (apply is idempotent for final
    /// state), or re-learns commit from the current leader via AppendEntries.
    ///
    /// Loading stops at the first record that is cut off, cannot be decoded or
    /// does not continue the log: the tail of a write a crash interrupted. It
    /// was never acknowledged, and is dropped. It used to be skipped over
    /// instead, so the next append landed after it and the records read after
    /// a restart were offset into the middle of the new ones. A file that
    /// cannot be read at all is an error: it used to give an empty log, with
    /// which the node voted and acknowledged as if it had never stored a thing.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        let (snapshot_index, snapshot_term) = read_snapshot_meta(&path)?;
        let mut entries: Vec<LogEntry> = Vec::new();
        let mut dirty = false;
        let mut buf = Vec::new();
        match std::fs::File::open(&path) {
            Ok(mut f) => {
                f.read_to_end(&mut buf)?;
                // What a crash left unsynced may still be read back here from
                // the page cache: make it durable before it is counted on.
                f.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let mut pos = 0;
        while pos < buf.len() {
            let Some(e) = decode_at(&buf, pos) else {
                break;
            };
            let next = pos + 4 + bincode::serialized_size(&e).unwrap_or(0) as usize;
            if e.index <= snapshot_index {
                // Already compacted: the file was not rewritten yet when the
                // compaction was interrupted.
                dirty = true;
            } else {
                let expected = entries.last().map_or(snapshot_index + 1, |l| l.index + 1);
                if e.index != expected {
                    break;
                }
                entries.push(e);
            }
            pos = next;
        }
        if pos < buf.len() {
            tracing::warn!(
                log = %path.display(),
                bytes = buf.len() - pos,
                last_index = entries.last().map_or(snapshot_index, |e| e.index),
                "dropping the unreadable tail of the Raft log (a write interrupted by a crash)"
            );
            dirty = true;
        }
        let durable = entries.last().map_or(snapshot_index, |e| e.index);
        Ok(RaftLog {
            entries,
            commit_index: snapshot_index,
            last_applied: snapshot_index,
            path: Some(path),
            durable,
            dirty,
            snapshot_index,
            snapshot_term,
        })
    }

    /// Discard applied log entries up to and including `up_to` (compaction). The
    /// snapshot boundary term is retained so consistency checks at `up_to` still
    /// succeed. Never compacts past what has been applied.
    pub fn compact(&mut self, up_to: u64) -> std::io::Result<()> {
        if up_to <= self.snapshot_index || up_to > self.last_applied {
            return Ok(());
        }
        let Some(term) = self.term_at(up_to) else {
            return Ok(());
        };
        // Persist the new snapshot boundary FIRST, then rewrite the (now shorter)
        // entries file. If we crash between the two, the boundary is ahead of the
        // entries file, which still holds a superset of entries — harmless and
        // consistent. The reverse order could leave a gap (corrupt) on crash,
        // and so could going on after the boundary failed to persist: nothing
        // changes then.
        if let Some(p) = &self.path {
            write_snapshot_meta(p, up_to, term)?;
        }
        self.entries.retain(|e| e.index > up_to);
        self.snapshot_index = up_to;
        self.snapshot_term = term;
        self.durable = self.durable.max(up_to);
        self.dirty = true;
        self.persist()
    }

    /// The compaction (snapshot) index — lowest index still in the log is
    /// `snapshot_index + 1`.
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    /// The highest index this log holds durably, with every entry before it:
    /// all a leader may count itself as having when it counts a quorum.
    pub fn durable_index(&self) -> u64 {
        self.durable.min(self.last_index())
    }

    /// Make every entry durable: append what is not on disk yet (one fsync), or
    /// rewrite the file when it no longer holds a prefix of this log.
    ///
    /// Every write used to be best effort: a failed one was dropped without a
    /// word and the caller went on as if the entries were on disk -- a follower
    /// acknowledged them, a leader counted itself toward their quorum. Now it
    /// is an error, and the next attempt rewrites the whole file: after a failed
    /// fsync the kernel may have dropped the unwritten pages and marked them
    /// clean, so syncing the same file again could succeed without storing them.
    fn persist(&mut self) -> std::io::Result<()> {
        let Some(p) = self.path.clone() else {
            self.durable = self.last_index();
            self.dirty = false;
            return Ok(());
        };
        if !self.dirty && self.durable >= self.last_index() {
            return Ok(());
        }
        let written = if self.dirty {
            self.rewrite(&p)
        } else {
            self.append_new(&p)
        };
        match written {
            Ok(()) => {
                self.durable = self.last_index();
                self.dirty = false;
                Ok(())
            }
            Err(e) => {
                self.dirty = true;
                Err(e)
            }
        }
    }

    /// Append the entries after `durable` to the file; the common, O(new) path.
    fn append_new(&self, p: &Path) -> std::io::Result<()> {
        let from = self.entries.partition_point(|e| e.index <= self.durable);
        let buf = encode(&self.entries[from..])?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)?;
        f.write_all(&buf)?;
        f.sync_all()
    }

    /// Replace the file with the whole log (after a conflicting-suffix
    /// truncation or a compaction, which are rare, or a failed write).
    fn rewrite(&self, p: &Path) -> std::io::Result<()> {
        atomic_write(p, &encode(&self.entries)?)
    }

    /// Index of the last entry (the snapshot index if the log is empty).
    pub fn last_index(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.index)
            .unwrap_or(self.snapshot_index)
    }

    /// Term of the last entry (the snapshot term if the log is empty).
    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// The term of the entry at `index`, if present (0 for index 0 = the empty
    /// sentinel before the first entry).
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term); // 0/0 for an un-compacted log
        }
        if index < self.snapshot_index {
            return None; // compacted away
        }
        self.entries
            .iter()
            .find(|e| e.index == index)
            .map(|e| e.term)
    }

    /// Leader path: append a new entry for `term` in memory, returning its
    /// index. Durability is deferred to [`sync`](Self::sync), so many concurrent
    /// proposals share a single fsync per replication round (batched throughput).
    pub fn leader_append(&mut self, term: u64, data: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry { term, index, data });
        index
    }

    /// Durably persist any not-yet-written entries (one fsync). The leader calls
    /// this before replicating/committing a round's entries.
    pub fn sync(&mut self) -> std::io::Result<()> {
        self.persist()
    }

    /// Follower path: the AppendEntries consistency check + truncation + append.
    ///
    /// Succeeds only if the log contains an entry at `prev_index` with
    /// `prev_term` (or `prev_index == 0`). Any existing entry that conflicts
    /// with a new one (same index, different term) — and everything after it —
    /// is truncated before the new entries are appended. Returns `false` (no
    /// change) if the consistency check fails, and an error if the entries
    /// could not be made durable: they must not be acknowledged then.
    pub fn append_entries(
        &mut self,
        prev_index: u64,
        prev_term: u64,
        new_entries: &[LogEntry],
    ) -> std::io::Result<bool> {
        // Consistency check: our log must match the leader's at prev_index.
        match self.term_at(prev_index) {
            Some(t) if t == prev_term => {}
            _ => return Ok(false),
        }
        // The entries must continue the log right after `prev_index`, without a
        // hole. A leader that has compacted the entries a new node needs used to
        // send just its log's tail with `prev_index` 0: the node accepted it,
        // could apply none of it (it has no entry 1), and still reported the
        // tail as matched -- so the leader counted an empty node toward its
        // quorum, and the node could later be elected with an empty database.
        let contiguous = new_entries
            .iter()
            .zip(prev_index + 1..)
            .all(|(e, expected)| e.index == expected);
        if !contiguous {
            return Ok(false);
        }
        for ne in new_entries {
            match self.entries.iter().position(|e| e.index == ne.index) {
                Some(pos) => {
                    if self.entries[pos].term != ne.term {
                        // Conflict: drop this entry and everything after it.
                        // What the file holds from here on is no longer this
                        // log, so it is rewritten.
                        self.entries.truncate(pos);
                        self.entries.push(ne.clone());
                        self.durable = self.durable.min(ne.index - 1);
                        self.dirty = true;
                    }
                    // Same term at this index: already have it (idempotent).
                }
                None => self.entries.push(ne.clone()),
            }
        }
        self.persist()?;
        Ok(true)
    }

    /// Advance the commit index (followers: to `min(leader_commit, last_index)`).
    /// Never moves backwards.
    pub fn advance_commit(&mut self, leader_commit: u64) {
        let target = leader_commit.min(self.last_index());
        if target > self.commit_index {
            self.commit_index = target; // commit index is not persisted (re-derived)
        }
    }

    /// Leader commit rule: an entry from the current term is committed once it is
    /// present on a quorum. Given the sorted match indexes of all members
    /// (including the leader), advance the commit index to the highest index
    /// replicated to a majority, but only for an entry in `current_term`
    /// (§5.4.2 — a leader never commits a prior term's entry by count alone).
    pub fn maybe_commit(&mut self, match_indexes: &mut [u64], current_term: u64) {
        if match_indexes.is_empty() {
            return;
        }
        match_indexes.sort_unstable();
        // The highest index present on a majority is the median from the top.
        let majority_idx = match_indexes[(match_indexes.len() - 1) / 2];
        if majority_idx > self.commit_index && self.term_at(majority_idx) == Some(current_term) {
            self.commit_index = majority_idx; // not persisted (re-derived)
        }
    }

    /// Take the next batch of committed-but-unapplied entries, advancing
    /// `last_applied`. The caller applies them to the state machine in order.
    pub fn take_applicable(&mut self) -> Vec<LogEntry> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            if let Some(e) = self.entries.iter().find(|e| e.index == next) {
                out.push(e.clone());
                self.last_applied = next;
            } else {
                break;
            }
        }
        out
    }

    /// Election restriction (§5.4.1): may we vote for a candidate whose last log
    /// entry is `(cand_last_term, cand_last_index)`? Yes iff the candidate's log
    /// is at least as up-to-date as ours (higher last term, or equal term and
    /// index >= ours).
    pub fn is_up_to_date(&self, cand_last_term: u64, cand_last_index: u64) -> bool {
        let (my_term, my_index) = (self.last_term(), self.last_index());
        cand_last_term > my_term || (cand_last_term == my_term && cand_last_index >= my_index)
    }

    /// Entries strictly after `index` (what a leader sends a lagging follower).
    pub fn entries_after(&self, index: u64) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.index > index)
            .cloned()
            .collect()
    }
}

/// Path of the snapshot-metadata sidecar for a log file.
fn snap_meta_path(path: &std::path::Path) -> PathBuf {
    path.with_extension("snap")
}

/// The compaction boundary `(index, term)`; `(0, 0)` when nothing was ever
/// compacted. It is written whole (see `atomic_write`), so one that cannot be
/// read is an error: guessing would misplace every entry after it.
fn read_snapshot_meta(path: &Path) -> std::io::Result<(u64, u64)> {
    let meta = snap_meta_path(path);
    let s = match std::fs::read_to_string(&meta) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e),
    };
    let mut fields = s.lines().map(|l| l.trim().parse::<u64>());
    match (fields.next(), fields.next()) {
        (Some(Ok(index)), Some(Ok(term))) => Ok((index, term)),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} is not a Raft compaction boundary", meta.display()),
        )),
    }
}

fn write_snapshot_meta(path: &Path, index: u64, term: u64) -> std::io::Result<()> {
    atomic_write(
        &snap_meta_path(path),
        format!("{index}\n{term}\n").as_bytes(),
    )
}

/// Length-prefixed records for `entries`, as the log file holds them.
fn encode(entries: &[LogEntry]) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    for e in entries {
        let bytes = bincode::serialize(e)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bytes);
    }
    Ok(buf)
}

/// The record at `pos`, if a whole one that decodes is there.
fn decode_at(buf: &[u8], pos: usize) -> Option<LogEntry> {
    let len = u32::from_le_bytes(buf.get(pos..pos + 4)?.try_into().ok()?) as usize;
    let body = buf.get(pos + 4..(pos + 4).checked_add(len)?)?;
    let e: LogEntry = bincode::deserialize(body).ok()?;
    // A record that decodes from fewer bytes than its length says is not one.
    (bincode::serialized_size(&e).ok()? == len as u64).then_some(e)
}

/// Durably replace `path` with `bytes`: write a sibling `<path>.tmp`, fsync it,
/// rename it over `path` and fsync the directory, so a crash leaves either the
/// old file or the new one, never part of one.
pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = PathBuf::from(format!("{}.tmp", path.display()));
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path)?;
    sync_dir(path)
}

/// Make a rename in `path`'s directory durable.
fn sync_dir(path: &Path) -> std::io::Result<()> {
    let dir = match path.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    // Not every platform lets a directory be opened for syncing; where one can
    // be opened, its sync must succeed.
    match std::fs::File::open(dir) {
        Ok(d) => d.sync_all(),
        Err(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(term: u64, index: u64) -> LogEntry {
        LogEntry {
            term,
            index,
            data: vec![index as u8],
        }
    }

    #[test]
    fn leader_append_and_commit_flow() {
        let mut log = RaftLog::new();
        assert_eq!(log.leader_append(1, vec![1]), 1);
        assert_eq!(log.leader_append(1, vec![2]), 2);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 1);
        // Not committed yet -> nothing applicable.
        assert!(log.take_applicable().is_empty());
        // Quorum of 3 with match indexes [2,2,1] commits index 2 (current term).
        log.maybe_commit(&mut [2, 2, 1], 1);
        assert_eq!(log.commit_index(), 2);
        let applied = log.take_applicable();
        assert_eq!(applied.len(), 2);
        assert_eq!(log.last_applied(), 2);
    }

    #[test]
    fn append_entries_consistency_and_truncation() {
        let mut log = RaftLog::new();
        // Follower already has [ (1,1), (1,2), (2,3) ].
        assert!(log
            .append_entries(0, 0, &[e(1, 1), e(1, 2), e(2, 3)])
            .unwrap());
        // A mismatched prev_term is rejected.
        assert!(!log.append_entries(2, 9, &[e(3, 3)]).unwrap());
        // Leader overwrites index 3 with a different term -> truncate + append.
        let mut conflicting = e(3, 3);
        conflicting.data = vec![99];
        assert!(log.append_entries(2, 1, &[conflicting.clone()]).unwrap());
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(3), Some(3));
    }

    #[test]
    fn compaction_discards_applied_entries_and_keeps_boundary() {
        let mut log = RaftLog::new();
        for i in 1..=5 {
            log.leader_append(1, vec![i as u8]);
        }
        log.maybe_commit(&mut [5, 5, 5], 1);
        assert_eq!(log.take_applicable().len(), 5); // applied 1..=5
                                                    // Compact up to 3: entries 1..=3 discarded, boundary term retained.
        log.compact(3).unwrap();
        assert_eq!(log.snapshot_index(), 3);
        assert_eq!(log.term_at(3), Some(1)); // boundary still known
        assert_eq!(log.term_at(2), None); // compacted away
        assert_eq!(log.term_at(4), Some(1)); // still present
        assert_eq!(log.last_index(), 5);
        // entries_after the boundary are intact.
        assert_eq!(log.entries_after(3).len(), 2);
        // Never compacts past what is applied.
        log.leader_append(1, vec![9]); // index 6, not applied
        log.compact(6).unwrap();
        assert_eq!(log.snapshot_index(), 3); // unchanged
    }

    #[test]
    fn compaction_survives_reload() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("elyra-raftlog-compact-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("snap"));
        {
            let mut log = RaftLog::open(path.clone()).unwrap();
            for i in 1..=5 {
                log.leader_append(1, vec![i as u8]);
            }
            log.sync().unwrap();
            log.maybe_commit(&mut [5, 5, 5], 1);
            log.take_applicable();
            log.compact(3).unwrap();
        }
        let log = RaftLog::open(path.clone()).unwrap();
        assert_eq!(log.snapshot_index(), 3);
        assert_eq!(log.term_at(3), Some(1));
        assert_eq!(log.last_index(), 5);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("snap"));
        let _ = std::fs::remove_file(path.with_extension("tmp"));
    }

    #[test]
    fn a_new_node_refuses_a_log_tail_it_cannot_apply() {
        let mut leader = RaftLog::new();
        for i in 1..=10 {
            leader.leader_append(1, vec![i as u8]);
        }
        leader.advance_commit(10);
        assert_eq!(leader.take_applicable().len(), 10);
        leader.compact(8).unwrap();
        // What the leader sends a brand-new follower (next = 1, prev = 0).
        let prev_term = leader.term_at(0).unwrap_or(0);
        let tail = leader.entries_after(0);
        assert_eq!(tail.iter().map(|e| e.index).collect::<Vec<_>>(), [9, 10]);
        let mut follower = RaftLog::new();
        assert!(!follower.append_entries(0, prev_term, &tail).unwrap());
        assert_eq!(follower.last_index(), 0, "nothing to report as matched");
        // A hole inside a batch is refused too.
        let gappy = vec![e(1, 1), e(1, 3)];
        assert!(!RaftLog::new().append_entries(0, 0, &gappy).unwrap());
        // The ordinary case still works.
        assert!(RaftLog::new()
            .append_entries(0, 0, &[e(1, 1), e(1, 2)])
            .unwrap());
    }

    fn temp_log(name: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("elyra-raftlog-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("n.raftlog");
        (dir, path)
    }

    #[test]
    fn a_torn_tail_is_dropped_and_what_is_appended_after_it_survives() {
        let (dir, path) = temp_log("torn");
        {
            let mut log = RaftLog::open(path.clone()).unwrap();
            for i in 1..=3 {
                log.leader_append(1, vec![i as u8]);
            }
            log.sync().unwrap();
        }
        // A crash in the middle of a write: a length prefix and part of a record.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        f.write_all(&200u32.to_le_bytes()).unwrap();
        f.write_all(&[7; 12]).unwrap();
        drop(f);
        {
            let mut log = RaftLog::open(path.clone()).unwrap();
            assert_eq!(log.last_index(), 3);
            for i in 4..=40 {
                log.leader_append(1, vec![i as u8; 8]);
            }
            log.sync().unwrap();
        }
        // The new entries used to be appended after the torn record, whose
        // length then swallowed them on the next start.
        let log = RaftLog::open(path.clone()).unwrap();
        assert_eq!(log.last_index(), 40);
        assert_eq!(log.entries_after(0).len(), 40);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// Make `dir` and `file` unwritable; `false` if writes still work (root).
    #[cfg(unix)]
    fn deny_writes(dir: &Path, file: &Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o444)).unwrap();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let probe = dir.join("probe");
        if std::fs::write(&probe, b"x").is_ok() {
            let _ = std::fs::remove_file(probe);
            allow_writes(dir, file);
            return false;
        }
        true
    }

    #[cfg(unix)]
    fn allow_writes(dir: &Path, file: &Path) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o644)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_write_that_fails_is_an_error_and_is_made_good_later() {
        let (dir, path) = temp_log("unwritable");
        let mut log = RaftLog::open(path.clone()).unwrap();
        log.leader_append(1, vec![1]);
        log.leader_append(1, vec![2]);
        log.sync().unwrap();
        if !deny_writes(&dir, &path) {
            eprintln!("skipped: permissions do not stop this user from writing");
            return std::fs::remove_dir_all(&dir).unwrap();
        }
        // Leader: the entry is in memory, but not durable, and says so.
        log.leader_append(1, vec![3]);
        assert!(log.sync().is_err());
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.durable_index(), 2);
        // Follower: entries it cannot store are not reported as appended.
        let mut follower = RaftLog::open(path.clone()).unwrap();
        assert!(follower.append_entries(2, 1, &[e(1, 3)]).is_err());
        // Compaction leaves everything as it was when its boundary cannot be
        // stored.
        log.maybe_commit(&mut [3, 3, 3], 1);
        log.take_applicable();
        assert!(log.compact(2).is_err());
        assert_eq!(log.snapshot_index(), 0);
        assert_eq!(log.entries_after(0).len(), 3);
        // Once the disk takes writes again the next sync stores everything.
        allow_writes(&dir, &path);
        log.sync().unwrap();
        assert_eq!(log.durable_index(), 3);
        let reopened = RaftLog::open(path.clone()).unwrap();
        assert_eq!(reopened.entries_after(0), log.entries_after(0));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_leader_counts_only_what_is_on_its_disk() {
        let (dir, path) = temp_log("durable");
        let mut log = RaftLog::open(path).unwrap();
        log.leader_append(1, vec![1]);
        log.leader_append(1, vec![2]);
        log.sync().unwrap();
        log.leader_append(1, vec![3]); // proposed since the last sync
        assert_eq!(log.durable_index(), 2);
        log.sync().unwrap();
        assert_eq!(log.durable_index(), 3);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_damaged_compaction_boundary_stops_the_log_from_opening() {
        let (dir, path) = temp_log("badsnap");
        std::fs::write(path.with_extension("snap"), b"not a number\n").unwrap();
        assert!(RaftLog::open(path).is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_log_rewritten_after_a_conflict_survives_a_restart() {
        let (dir, path) = temp_log("conflict");
        {
            let mut log = RaftLog::open(path.clone()).unwrap();
            log.append_entries(0, 0, &[e(1, 1), e(1, 2), e(1, 3)])
                .unwrap();
            // A new leader overwrites 2.. with its own term.
            log.append_entries(1, 1, &[e(2, 2)]).unwrap();
            assert_eq!(log.last_index(), 2);
        }
        let log = RaftLog::open(path).unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.term_at(2), Some(2));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn election_restriction() {
        let mut log = RaftLog::new();
        log.append_entries(0, 0, &[e(1, 1), e(2, 2)]).unwrap();
        // Same last term, higher/equal index: allowed.
        assert!(log.is_up_to_date(2, 2));
        assert!(log.is_up_to_date(2, 5));
        // Same term, lower index: rejected.
        assert!(!log.is_up_to_date(2, 1));
        // Higher last term always wins regardless of index.
        assert!(log.is_up_to_date(3, 1));
        // Lower last term rejected.
        assert!(!log.is_up_to_date(1, 100));
    }

    #[test]
    fn maybe_commit_only_current_term() {
        let mut log = RaftLog::new();
        log.append_entries(0, 0, &[e(1, 1), e(1, 2)]).unwrap();
        // A quorum has index 2, but it's a prior term -> not committed by count.
        log.maybe_commit(&mut [2, 2, 1], 3);
        assert_eq!(log.commit_index(), 0);
        // An entry in the current term does get committed.
        log.leader_append(3, vec![3]);
        log.maybe_commit(&mut [3, 3, 1], 3);
        assert_eq!(log.commit_index(), 3);
    }

    #[test]
    fn persistence_roundtrip() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("elyra-raftlog-test-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);
        {
            let mut log = RaftLog::open(path.clone()).unwrap();
            log.leader_append(1, vec![1]);
            log.leader_append(1, vec![2]);
            log.sync().unwrap(); // durability is deferred to sync()
            log.maybe_commit(&mut [2, 2, 1], 1);
        }
        // Entries are durable (append-only); commit index is re-derived (0).
        let log = RaftLog::open(path.clone()).unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.last_term(), 1);
        assert_eq!(log.commit_index(), 0);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("tmp"));
    }
}
