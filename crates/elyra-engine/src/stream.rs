//! Streaming result sets.
//!
//! A [`RowStream`] never materialises a whole table. Table scans pull rows
//! from storage in bounded batches via a cursor, so a `SELECT` over a
//! billion-row table uses the same small amount of memory as one over ten
//! rows. The server drains batches straight to the wire.

use elyra_core::{Error, Result, Schema, Value};
use elyra_storage::Db;

use crate::catalog::{data_prefix, TableDef};

pub struct RowStream {
    pub schema: Schema,
    src: Source,
}

enum Source {
    /// Small, already-computed rows (literal SELECTs, `@@vars`, etc.).
    Literal(std::vec::IntoIter<Vec<Value>>),
    /// Bounded-memory clustered scan over a table.
    Scan(Scan),
}

struct Scan {
    db: Db,
    prefix: Vec<u8>,
    cursor: Option<Vec<u8>>,
    /// Row-index for each output column (projection).
    projection: Vec<usize>,
    done: bool,
}

impl RowStream {
    /// Wrap already-computed rows.
    pub fn literal(schema: Schema, rows: Vec<Vec<Value>>) -> Self {
        Self { schema, src: Source::Literal(rows.into_iter()) }
    }

    /// Stream a clustered table scan projected to `output` columns (schema
    /// indices). `out_schema` describes the projected columns.
    pub fn scan(db: Db, table: &TableDef, projection: Vec<usize>, out_schema: Schema) -> Self {
        Self {
            schema: out_schema,
            src: Source::Scan(Scan {
                db,
                prefix: data_prefix(&table.name),
                cursor: None,
                projection,
                done: false,
            }),
        }
    }

    /// Fetch the next batch of up to `n` rows. An empty batch means the
    /// stream is exhausted.
    pub async fn next_batch(&mut self, n: usize) -> Result<Vec<Vec<Value>>> {
        match &mut self.src {
            Source::Literal(iter) => Ok(iter.by_ref().take(n).collect()),
            Source::Scan(scan) => scan.next_batch(n).await,
        }
    }
}

impl Scan {
    async fn next_batch(&mut self, n: usize) -> Result<Vec<Vec<Value>>> {
        if self.done {
            return Ok(Vec::new());
        }
        let batch = self
            .db
            .scan_batch(self.prefix.clone(), self.cursor.clone(), n)
            .await?;

        if batch.len() < n {
            self.done = true;
        }
        if let Some((last_key, _)) = batch.last() {
            self.cursor = Some(last_key.clone());
        }

        let mut out = Vec::with_capacity(batch.len());
        for (_, value) in batch {
            let row: Vec<Value> =
                bincode::deserialize(&value).map_err(|e| Error::Storage(e.to_string()))?;
            let projected = self
                .projection
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
            out.push(projected);
        }
        Ok(out)
    }
}
