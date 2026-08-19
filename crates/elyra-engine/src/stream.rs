//! Streaming result sets.
//!
//! A [`RowStream`] never materialises a whole table. Table scans pull rows
//! from storage in bounded batches via a cursor, apply the `WHERE` filter and
//! `LIMIT`/`OFFSET`, then project — all with bounded memory. The server
//! drains batches straight to the wire.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

use elyra_core::{ColumnType, Error, Result, Schema, Value};
use elyra_storage::Db;
use sqlparser::ast::Expr;

use crate::catalog::{data_prefix, TableDef};
use crate::predicate;

/// How many storage rows to pull per underlying scan step.
const SCAN_CHUNK: usize = 1024;

pub struct RowStream {
    pub schema: Schema,
    src: Source,
}

#[allow(clippy::large_enum_variant)]
enum Source {
    /// Small, already-computed rows (literal SELECTs, PK point-lookups, ...).
    Literal(std::vec::IntoIter<Vec<Value>>),
    /// Bounded-memory clustered scan over a table.
    Scan(Scan),
    /// Length-prefixed, bincode-encoded rows owned by this stream.
    Spill(Spill),
}

struct Spill {
    reader: BufReader<File>,
    path: PathBuf,
    done: bool,
}

struct Scan {
    db: Db,
    prefix: Vec<u8>,
    cursor: Option<Vec<u8>>,
    /// Full table schema — needed to evaluate the predicate on whole rows.
    full_schema: Schema,
    /// Row-index for each output column (projection).
    projection: Vec<usize>,
    filter: Option<Expr>,
    offset: usize,
    limit: Option<usize>,
    done: bool,
}

/// Parameters for a streaming table scan.
pub struct ScanSpec {
    pub projection: Vec<usize>,
    pub out_schema: Schema,
    pub filter: Option<Expr>,
    pub offset: usize,
    pub limit: Option<usize>,
}

/// Reconcile declared `Int`/`Float` column types with the actual values:
/// narrow `Float`->`Int` when every non-null value is an integer, and widen
/// `Int`->`Float` when any value is a float/decimal. Non-numeric columns are
/// left untouched.
pub(crate) struct NumericTypeReconciler {
    states: Vec<(bool, bool, bool)>,
}

impl NumericTypeReconciler {
    pub(crate) fn new(columns: usize) -> Self {
        Self {
            states: vec![(false, false, false); columns],
        }
    }

    pub(crate) fn observe(&mut self, row: &[Value]) {
        for (index, state) in self.states.iter_mut().enumerate() {
            match row.get(index) {
                Some(Value::Float(_)) | Some(Value::Decimal(..)) => state.0 = true,
                Some(Value::Int(_)) | Some(Value::Bool(_)) => state.1 = true,
                Some(Value::Null) | None => {}
                Some(_) => state.2 = true,
            }
        }
    }

    pub(crate) fn reconcile(&self, schema: &mut Schema) {
        for (col, &(has_float, has_int, bail)) in schema.columns.iter_mut().zip(&self.states) {
            if !matches!(col.ty, ColumnType::Int | ColumnType::Float) || bail {
                continue;
            }
            if has_float {
                col.ty = ColumnType::Float;
            } else if has_int {
                col.ty = ColumnType::Int;
            }
        }
    }
}

fn reconcile_numeric_types(schema: &mut Schema, rows: &[Vec<Value>]) {
    let mut reconciler = NumericTypeReconciler::new(schema.columns.len());
    for row in rows {
        reconciler.observe(row);
    }
    reconciler.reconcile(schema);
}

impl RowStream {
    /// Wrap already-computed rows. The declared numeric column types are
    /// reconciled with the actual values so computed columns (aggregates,
    /// expressions) report the right wire type (e.g. an integer conditional
    /// SUM is sent as an integer, not a double).
    pub fn literal(mut schema: Schema, rows: Vec<Vec<Value>>) -> Self {
        reconcile_numeric_types(&mut schema, &rows);
        Self {
            schema,
            src: Source::Literal(rows.into_iter()),
        }
    }

    /// Stream a clustered table scan.
    pub fn scan(db: Db, table: &TableDef, spec: ScanSpec) -> Self {
        Self {
            schema: spec.out_schema,
            src: Source::Scan(Scan {
                db,
                prefix: data_prefix(&table.name),
                cursor: None,
                full_schema: table.schema.clone(),
                projection: spec.projection,
                filter: spec.filter,
                offset: spec.offset,
                limit: spec.limit,
                done: false,
            }),
        }
    }

    /// Build a stream over an already-open spill file.
    ///
    /// Rows must be bincode-encoded `Vec<Value>` values, each preceded by a
    /// little-endian `u32` byte length. The stream owns the handle and removes
    /// `path` when it is dropped. Callers may unlink the path before calling
    /// this on platforms that support reading an unlinked open file.
    pub(crate) fn spill(schema: Schema, path: PathBuf, mut file: File) -> Result<Self> {
        if let Err(error) = file.seek(SeekFrom::Start(0)) {
            let _ = std::fs::remove_file(&path);
            return Err(Error::Io(error));
        }
        Ok(Self {
            schema,
            src: Source::Spill(Spill {
                reader: BufReader::new(file),
                path,
                done: false,
            }),
        })
    }

    /// Fetch the next batch of up to `n` output rows. Empty = exhausted.
    pub async fn next_batch(&mut self, n: usize) -> Result<Vec<Vec<Value>>> {
        match &mut self.src {
            Source::Literal(iter) => Ok(iter.by_ref().take(n).collect()),
            Source::Scan(scan) => scan.next_batch(n).await,
            Source::Spill(spill) => spill.next_batch(n),
        }
    }
}

impl Spill {
    fn next_batch(&mut self, n: usize) -> Result<Vec<Vec<Value>>> {
        let mut rows = Vec::with_capacity(n.min(SCAN_CHUNK));
        while !self.done && rows.len() < n {
            match self.next_row()? {
                Some(row) => rows.push(row),
                None => self.done = true,
            }
        }
        Ok(rows)
    }

    fn next_row(&mut self) -> Result<Option<Vec<Value>>> {
        let mut len = [0u8; 4];
        match self.reader.read(&mut len[..1]) {
            Ok(0) => return Ok(None),
            Ok(1) => {}
            Ok(_) => unreachable!("one-byte read returned more than one byte"),
            Err(error) => return Err(Error::Io(error)),
        }
        self.reader.read_exact(&mut len[1..]).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::Storage("truncated spill row frame header".into())
            } else {
                Error::Io(error)
            }
        })?;

        let frame_len = u32::from_le_bytes(len) as usize;
        if frame_len > elyra_core::max_frame_bytes() {
            return Err(Error::Storage(
                "spill row frame too large (corrupt?)".into(),
            ));
        }
        let mut frame = vec![0; frame_len];
        self.reader.read_exact(&mut frame).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                Error::Storage("truncated spill row frame".into())
            } else {
                Error::Io(error)
            }
        })?;
        bincode::deserialize(&frame)
            .map(Some)
            .map_err(|error| Error::Storage(format!("invalid spill row: {error}")))
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

impl Scan {
    async fn next_batch(&mut self, n: usize) -> Result<Vec<Vec<Value>>> {
        let mut out = Vec::new();

        while !self.done && out.len() < n {
            if self.limit == Some(0) {
                self.done = true;
                break;
            }

            let chunk = self
                .db
                .scan_batch(self.prefix.clone(), self.cursor.clone(), SCAN_CHUNK)
                .await?;

            if chunk.len() < SCAN_CHUNK {
                self.done = true;
            }
            if let Some((last_key, _)) = chunk.last() {
                self.cursor = Some(last_key.clone());
            }

            for (_, value) in chunk {
                let row: Vec<Value> = crate::rowdec::decode_row(&value)?;

                // WHERE
                if let Some(f) = &self.filter {
                    if !predicate::matches(f, &self.full_schema, &row)? {
                        continue;
                    }
                }
                // OFFSET
                if self.offset > 0 {
                    self.offset -= 1;
                    continue;
                }

                out.push(self.project(&row));

                // LIMIT
                if let Some(l) = self.limit.as_mut() {
                    *l -= 1;
                    if *l == 0 {
                        self.done = true;
                        return Ok(out);
                    }
                }
            }
        }

        Ok(out)
    }

    fn project(&self, row: &[Value]) -> Vec<Value> {
        self.projection
            .iter()
            .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
            .collect()
    }
}

#[cfg(test)]
mod spill_tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn spill_file(frames: &[Vec<u8>]) -> (PathBuf, File) {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "elyrasql-stream-test-{}-{}.tmp",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        for frame in frames {
            file.write_all(&(frame.len() as u32).to_le_bytes()).unwrap();
            file.write_all(frame).unwrap();
        }
        file.flush().unwrap();
        (path, file)
    }

    fn empty_schema() -> Schema {
        Schema::new(vec![])
    }

    #[tokio::test]
    async fn spill_stream_reads_bounded_batches_and_cleans_up() {
        let expected = [
            vec![Value::Int(1)],
            vec![Value::Text("two".into())],
            vec![Value::Null],
        ];
        let frames: Vec<_> = expected
            .iter()
            .map(|row| bincode::serialize(row).unwrap())
            .collect();
        let (path, file) = spill_file(&frames);
        let mut stream = RowStream::spill(empty_schema(), path.clone(), file).unwrap();

        assert_eq!(stream.next_batch(2).await.unwrap(), expected[..2]);
        assert_eq!(stream.next_batch(2).await.unwrap(), expected[2..]);
        assert!(stream.next_batch(2).await.unwrap().is_empty());
        assert!(path.exists());
        drop(stream);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn spill_stream_rejects_oversized_frames() {
        let (path, mut file) = spill_file(&[]);
        let oversized = u32::try_from(elyra_core::max_frame_bytes())
            .unwrap()
            .checked_add(1)
            .unwrap();
        file.write_all(&oversized.to_le_bytes()).unwrap();
        file.flush().unwrap();
        let mut stream = RowStream::spill(empty_schema(), path, file).unwrap();
        let err = stream.next_batch(1).await.unwrap_err();
        assert!(err.to_string().contains("too large"));
    }

    #[tokio::test]
    async fn spill_stream_rejects_truncated_frames() {
        let (path, mut file) = spill_file(&[]);
        file.write_all(&10u32.to_le_bytes()).unwrap();
        file.write_all(b"short").unwrap();
        file.flush().unwrap();
        let mut stream = RowStream::spill(empty_schema(), path, file).unwrap();
        let err = stream.next_batch(1).await.unwrap_err();
        assert!(err.to_string().contains("truncated spill row frame"));
    }
}
