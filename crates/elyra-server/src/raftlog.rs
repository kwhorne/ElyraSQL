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
//! NOTE: this module is the tested building block. Routing the live cluster
//! write path through it (leader append -> quorum commit -> apply, with
//! followers applying up to the leader's commit index) is the remaining
//! integration work tracked for the full-consensus milestone; today's clusters
//! use asynchronous replication with the LSN-aware election restriction.

use std::io::{Read, Write};
use std::path::PathBuf;

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
    /// Optional backing file for durability.
    path: Option<PathBuf>,
}

impl RaftLog {
    pub fn new() -> Self {
        RaftLog::default()
    }

    /// Open a log backed by `path`, loading any persisted entries.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        let mut log = RaftLog {
            path: Some(path.clone()),
            ..Default::default()
        };
        if path.exists() {
            let mut buf = Vec::new();
            std::fs::File::open(&path)?.read_to_end(&mut buf)?;
            if !buf.is_empty() {
                let (entries, commit): (Vec<LogEntry>, u64) = bincode::deserialize(&buf)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                log.entries = entries;
                log.commit_index = commit;
            }
        }
        Ok(log)
    }

    fn persist(&self) {
        if let Some(p) = &self.path {
            if let Ok(bytes) = bincode::serialize(&(&self.entries, self.commit_index)) {
                // Write to a temp file then rename for atomicity.
                let tmp = p.with_extension("tmp");
                if std::fs::File::create(&tmp)
                    .and_then(|mut f| f.write_all(&bytes).and_then(|_| f.sync_all()))
                    .is_ok()
                {
                    let _ = std::fs::rename(&tmp, p);
                }
            }
        }
    }

    /// Index of the last entry (0 if empty).
    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|e| e.index).unwrap_or(0)
    }

    /// Term of the last entry (0 if empty).
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(0)
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
        if index == 0 {
            return Some(0);
        }
        self.entries
            .iter()
            .find(|e| e.index == index)
            .map(|e| e.term)
    }

    /// Leader path: append a new entry for `term`, returning its index.
    pub fn leader_append(&mut self, term: u64, data: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry { term, index, data });
        self.persist();
        index
    }

    /// Follower path: the AppendEntries consistency check + truncation + append.
    ///
    /// Succeeds only if the log contains an entry at `prev_index` with
    /// `prev_term` (or `prev_index == 0`). Any existing entry that conflicts
    /// with a new one (same index, different term) — and everything after it —
    /// is truncated before the new entries are appended. Returns `false` (no
    /// change) if the consistency check fails.
    pub fn append_entries(
        &mut self,
        prev_index: u64,
        prev_term: u64,
        new_entries: &[LogEntry],
    ) -> bool {
        // Consistency check: our log must match the leader's at prev_index.
        match self.term_at(prev_index) {
            Some(t) if t == prev_term => {}
            _ => return false,
        }
        for ne in new_entries {
            match self.entries.iter().position(|e| e.index == ne.index) {
                Some(pos) => {
                    if self.entries[pos].term != ne.term {
                        // Conflict: drop this entry and everything after it.
                        self.entries.truncate(pos);
                        self.entries.push(ne.clone());
                    }
                    // Same term at this index: already have it (idempotent).
                }
                None => self.entries.push(ne.clone()),
            }
        }
        self.persist();
        true
    }

    /// Advance the commit index (followers: to `min(leader_commit, last_index)`).
    /// Never moves backwards.
    pub fn advance_commit(&mut self, leader_commit: u64) {
        let target = leader_commit.min(self.last_index());
        if target > self.commit_index {
            self.commit_index = target;
            self.persist();
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
            self.commit_index = majority_idx;
            self.persist();
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
        assert!(log.append_entries(0, 0, &[e(1, 1), e(1, 2), e(2, 3)]));
        // A mismatched prev_term is rejected.
        assert!(!log.append_entries(2, 9, &[e(3, 3)]));
        // Leader overwrites index 3 with a different term -> truncate + append.
        let mut conflicting = e(3, 3);
        conflicting.data = vec![99];
        assert!(log.append_entries(2, 1, &[conflicting.clone()]));
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(3), Some(3));
    }

    #[test]
    fn election_restriction() {
        let mut log = RaftLog::new();
        log.append_entries(0, 0, &[e(1, 1), e(2, 2)]);
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
        log.append_entries(0, 0, &[e(1, 1), e(1, 2)]);
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
            log.maybe_commit(&mut [2, 2, 1], 1);
        }
        let log = RaftLog::open(path.clone()).unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.commit_index(), 2);
        let _ = std::fs::remove_file(&path);
    }
}
