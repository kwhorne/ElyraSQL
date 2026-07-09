//! Append-only binary log for point-in-time recovery.
//!
//! When enabled, every committed write-set is appended here as an ordered,
//! length-prefixed record `(lsn, timestamp_ms, puts, deletes)`. Because
//! write-sets are absolute key/value changes, replaying the log in order onto a
//! base (an empty database or a restored backup) is idempotent and reconstructs
//! the exact state as of any chosen LSN or timestamp.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
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

/// Buffered append-only binlog writer (owned by the single writer thread).
pub struct BinlogWriter {
    w: BufWriter<File>,
}

impl BinlogWriter {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let f = File::options()
            .create(true)
            .append(true)
            .open(path)
            .map_err(Error::Io)?;
        Ok(BinlogWriter {
            w: BufWriter::new(f),
        })
    }

    /// Append and flush one record (durable to the OS on return).
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
        Ok(())
    }
}

/// Replay a binlog onto `db`, applying records in order up to (and including)
/// `until_lsn` and/or `until_ts` (whichever bounds are set). Returns the number
/// of records applied.
pub async fn replay(
    path: impl AsRef<Path>,
    db: &Db,
    until_lsn: Option<u64>,
    until_ts: Option<u64>,
) -> Result<u64> {
    let mut r = BufReader::new(File::open(path).map_err(Error::Io)?);
    let mut applied = 0u64;
    loop {
        let mut len = [0u8; 4];
        match r.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }
        let n = u32::from_le_bytes(len) as usize;
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf).map_err(Error::Io)?;
        let rec: BinlogRecord =
            bincode::deserialize(&buf).map_err(|e| Error::Storage(e.to_string()))?;

        if until_lsn.is_some_and(|l| rec.lsn > l) {
            break;
        }
        if until_ts.is_some_and(|t| rec.ts_ms > t) {
            break;
        }
        db.commit(rec.puts, rec.deletes).await?;
        applied += 1;
    }
    Ok(applied)
}
