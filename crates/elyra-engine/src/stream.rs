//! Streaming result sets.
//!
//! A [`RowStream`] never materialises a whole table. Table scans pull rows
//! from storage in bounded batches via a cursor, apply the `WHERE` filter and
//! `LIMIT`/`OFFSET`, then project — all with bounded memory. The server
//! drains batches straight to the wire.

use elyra_core::{Error, Result, Schema, Value};
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

enum Source {
    /// Small, already-computed rows (literal SELECTs, PK point-lookups, ...).
    Literal(std::vec::IntoIter<Vec<Value>>),
    /// Bounded-memory clustered scan over a table.
    Scan(Scan),
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

impl RowStream {
    /// Wrap already-computed rows.
    pub fn literal(schema: Schema, rows: Vec<Vec<Value>>) -> Self {
        Self { schema, src: Source::Literal(rows.into_iter()) }
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

    /// Fetch the next batch of up to `n` output rows. Empty = exhausted.
    pub async fn next_batch(&mut self, n: usize) -> Result<Vec<Vec<Value>>> {
        match &mut self.src {
            Source::Literal(iter) => Ok(iter.by_ref().take(n).collect()),
            Source::Scan(scan) => scan.next_batch(n).await,
        }
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
                let row: Vec<Value> =
                    bincode::deserialize(&value).map_err(|e| Error::Storage(e.to_string()))?;

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
