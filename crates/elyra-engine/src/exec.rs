//! Statement execution over the clustered single-file store.
//!
//! Implements `CREATE TABLE`, `INSERT`, `SELECT ... FROM`, `DROP TABLE`.
//! Inserts are batched into one group-commit; scans stream.

use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use elyra_storage::Db;
use sqlparser::ast::{
    ColumnOption, CreateTable, DataType, Insert, ObjectName, SetExpr, TableConstraint,
    TableFactor, Query as SqlQuery,
};

use crate::catalog::{self, catalog_key, data_key, data_prefix, rowid_key, TableDef};
use crate::eval::eval_expr;
use crate::keyenc;
use crate::stream::{RowStream, ScanSpec};
use crate::QueryResult;
use sqlparser::ast::Expr;

fn table_ident(name: &ObjectName) -> Result<String> {
    name.0
        .last()
        .map(|i| i.value.clone())
        .ok_or_else(|| Error::Catalog("empty table name".into()))
}

fn map_type(dt: &DataType) -> Result<ColumnType> {
    Ok(match dt {
        DataType::TinyInt(_) if is_tinyint_bool(dt) => ColumnType::Bool,
        DataType::Bool | DataType::Boolean => ColumnType::Bool,
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_) => ColumnType::Int,
        DataType::Float(_) | DataType::Real | DataType::Double | DataType::Decimal(_) => {
            ColumnType::Float
        }
        DataType::Text
        | DataType::String(_)
        | DataType::Varchar(_)
        | DataType::Char(_) => ColumnType::Text,
        DataType::Blob(_) | DataType::Bytea => ColumnType::Bytes,
        DataType::Custom(name, args) if name.0.last().map(|i| i.value.eq_ignore_ascii_case("vector")).unwrap_or(false) => {
            let dim = args
                .first()
                .and_then(|s| s.parse::<u32>().ok())
                .ok_or_else(|| Error::Type("VECTOR requires a dimension, e.g. VECTOR(768)".into()))?;
            ColumnType::Vector(dim)
        }
        other => {
            return Err(Error::Unsupported(format!("column type not supported: {other}")))
        }
    })
}

fn is_tinyint_bool(_dt: &DataType) -> bool {
    false
}

pub async fn create_table(db: &Db, ct: CreateTable) -> Result<QueryResult> {
    let name = table_ident(&ct.name)?;

    if catalog::exists(db, &name).await? {
        if ct.if_not_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Catalog(format!("table already exists: {name}")));
    }

    let mut columns = Vec::with_capacity(ct.columns.len());
    let mut pk_col = None;

    for (idx, col) in ct.columns.iter().enumerate() {
        let ty = map_type(&col.data_type)?;
        let mut nullable = true;
        for opt in &col.options {
            match &opt.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Unique { is_primary: true, .. } => {
                    pk_col = Some(idx);
                    nullable = false;
                }
                _ => {}
            }
        }
        columns.push(ColumnDef { name: col.name.value.clone(), ty, nullable });
    }

    // Table-level PRIMARY KEY (single column supported).
    for c in &ct.constraints {
        if let TableConstraint::PrimaryKey { columns: cols, .. } = c {
            if cols.len() != 1 {
                return Err(Error::Unsupported(
                    "composite primary keys are not supported yet".into(),
                ));
            }
            let pk_name = &cols[0].value;
            let i = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(pk_name))
                .ok_or_else(|| Error::Catalog(format!("unknown primary key column: {pk_name}")))?;
            pk_col = Some(i);
            columns[i].nullable = false;
        }
    }

    let def = TableDef { name: name.clone(), schema: Schema::new(columns), pk_col };
    db.commit(vec![(catalog_key(&name), def.encode()?)], vec![]).await?;
    Ok(QueryResult::Affected(0))
}

pub async fn insert(db: &Db, ins: Insert) -> Result<QueryResult> {
    let name = table_ident(&ins.table_name)?;
    let def = catalog::load(db, &name).await?;

    // Resolve target column order.
    let target: Vec<usize> = if ins.columns.is_empty() {
        (0..def.schema.columns.len()).collect()
    } else {
        ins.columns
            .iter()
            .map(|c| {
                def.schema
                    .columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(&c.value))
                    .ok_or_else(|| Error::Catalog(format!("unknown column: {}", c.value)))
            })
            .collect::<Result<_>>()?
    };

    let source = ins
        .source
        .as_ref()
        .ok_or_else(|| Error::Unsupported("INSERT without VALUES is not supported".into()))?;
    let value_rows = match source_rows(source)? {
        Some(rows) => rows,
        None => return Err(Error::Unsupported("only INSERT ... VALUES is supported".into())),
    };

    // Load rowid counter once for tables without a PK.
    let mut next_rowid = if def.pk_col.is_none() {
        read_rowid(db, &name).await?
    } else {
        0
    };

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(value_rows.len());

    for exprs in value_rows {
        if exprs.len() != target.len() {
            return Err(Error::Query(format!(
                "column count mismatch: {} values for {} columns",
                exprs.len(),
                target.len()
            )));
        }

        let mut row = vec![Value::Null; def.schema.columns.len()];
        for (slot, expr) in target.iter().zip(exprs.iter()) {
            let v = eval_expr(expr)?;
            let col = &def.schema.columns[*slot];
            row[*slot] = coerce(v, &col.ty, &col.name)?;
        }

        // Enforce NOT NULL.
        for (i, col) in def.schema.columns.iter().enumerate() {
            if !col.nullable && row[i].is_null() {
                return Err(Error::Query(format!("column '{}' cannot be NULL", col.name)));
            }
        }

        let key = match def.pk_col {
            Some(i) => data_key(&name, &keyenc::encode(&row[i])?),
            None => {
                next_rowid += 1;
                data_key(&name, &keyenc::encode_rowid(next_rowid))
            }
        };
        let encoded = bincode::serialize(&row).map_err(|e| Error::Storage(e.to_string()))?;
        puts.push((key, encoded));
    }

    let affected = puts.len() as u64;

    // Persist the advanced rowid counter in the same atomic commit.
    if def.pk_col.is_none() {
        puts.push((rowid_key(&name), next_rowid.to_le_bytes().to_vec()));
    }

    db.commit(puts, vec![]).await?;
    Ok(QueryResult::Affected(affected))
}

pub async fn select(db: &Db, query: &SqlQuery) -> Result<QueryResult> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(Error::Unsupported("only simple SELECT is supported".into())),
    };
    if select.from.len() != 1 {
        return Err(Error::Unsupported("exactly one table in FROM is supported".into()));
    }
    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => table_ident(name)?,
        _ => return Err(Error::Unsupported("only plain table references are supported".into())),
    };
    let def = catalog::load(db, &table).await?;

    let offset = match &query.offset {
        Some(o) => eval_usize(&o.value)?,
        None => 0,
    };
    let limit = match &query.limit {
        Some(e) => Some(eval_usize(e)?),
        None => None,
    };
    let filter = select.selection.clone();

    // Build projection.
    use sqlparser::ast::SelectItem;
    let (projection, out_cols): (Vec<usize>, Vec<ColumnDef>) = if select
        .projection
        .iter()
        .any(|p| matches!(p, SelectItem::Wildcard(_)))
    {
        (
            (0..def.schema.columns.len()).collect(),
            def.schema.columns.clone(),
        )
    } else {
        let mut idxs = Vec::new();
        let mut cols = Vec::new();
        for item in &select.projection {
            let ident = match item {
                SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(id)) => &id.value,
                SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::Identifier(id),
                    ..
                } => &id.value,
                other => {
                    return Err(Error::Unsupported(format!(
                        "projection not supported over table scan: {other}"
                    )))
                }
            };
            let i = def
                .schema
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(ident))
                .ok_or_else(|| Error::Catalog(format!("unknown column: {ident}")))?;
            idxs.push(i);
            cols.push(def.schema.columns[i].clone());
        }
        (idxs, cols)
    };

    let out_schema = Schema::new(out_cols);

    // Fast path: `WHERE pk = <literal>` becomes an O(log n) point lookup on
    // the clustered key instead of a full table scan.
    if offset == 0 && limit != Some(0) {
        match try_pk_lookup(db, &def, filter.as_ref()).await? {
            PkLookup::Found(row) => {
                let projected = projection.iter().map(|&i| row[i].clone()).collect();
                return Ok(QueryResult::Rows(RowStream::literal(out_schema, vec![projected])));
            }
            PkLookup::NotFound => {
                return Ok(QueryResult::Rows(RowStream::literal(out_schema, vec![])));
            }
            PkLookup::NotApplicable => {}
        }
    }

    Ok(QueryResult::Rows(RowStream::scan(
        db.clone(),
        &def,
        ScanSpec { projection, out_schema, filter, offset, limit },
    )))
}

/// Outcome of attempting the clustered PK point-lookup fast path.
enum PkLookup {
    /// The predicate is not `pk = <literal>`; use a normal scan.
    NotApplicable,
    /// Point lookup ran, no matching row.
    NotFound,
    /// Point lookup ran, here is the row.
    Found(Vec<Value>),
}

/// If `filter` is exactly `pk = <literal>` (either operand order), fetch the
/// single row directly from the clustered key.
async fn try_pk_lookup(db: &Db, def: &TableDef, filter: Option<&Expr>) -> Result<PkLookup> {
    use sqlparser::ast::BinaryOperator;
    let Some(pk) = def.pk_col else { return Ok(PkLookup::NotApplicable) };
    let Some(Expr::BinaryOp { left, op: BinaryOperator::Eq, right }) = filter else {
        return Ok(PkLookup::NotApplicable);
    };
    let pk_name = &def.schema.columns[pk].name;

    // Identify which side is the pk column and which is the literal.
    let lit = match (ident_name(left), ident_name(right)) {
        (Some(n), None) if n.eq_ignore_ascii_case(pk_name) => eval_expr(right)?,
        (None, Some(n)) if n.eq_ignore_ascii_case(pk_name) => eval_expr(left)?,
        _ => return Ok(PkLookup::NotApplicable),
    };
    let key = data_key(&def.name, &keyenc::encode(&lit)?);
    match db.get(key).await? {
        Some(bytes) => {
            let row: Vec<Value> =
                bincode::deserialize(&bytes).map_err(|e| Error::Storage(e.to_string()))?;
            Ok(PkLookup::Found(row))
        }
        None => Ok(PkLookup::NotFound),
    }
}

fn ident_name(e: &Expr) -> Option<&str> {
    match e {
        Expr::Identifier(id) => Some(&id.value),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.as_str()),
        _ => None,
    }
}

fn eval_usize(e: &Expr) -> Result<usize> {
    match eval_expr(e)? {
        Value::Int(i) if i >= 0 => Ok(i as usize),
        other => Err(Error::Query(format!("expected non-negative integer, got {other:?}"))),
    }
}

pub async fn drop_table(db: &Db, name: &str, if_exists: bool) -> Result<QueryResult> {
    if !catalog::exists(db, name).await? {
        if if_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Catalog(format!("no such table: {name}")));
    }

    // Collect the table's data keys in batches to avoid unbounded memory.
    let prefix = data_prefix(name);
    let mut deletes = vec![catalog_key(name), rowid_key(name)];
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if batch.is_empty() {
            break;
        }
        cursor = batch.last().map(|(k, _)| k.clone());
        let last = batch.len() < 4096;
        deletes.extend(batch.into_iter().map(|(k, _)| k));
        if last {
            break;
        }
    }
    db.commit(vec![], deletes).await?;
    Ok(QueryResult::Affected(0))
}

async fn read_rowid(db: &Db, table: &str) -> Result<u64> {
    Ok(match db.get(rowid_key(table)).await? {
        Some(bytes) if bytes.len() == 8 => {
            u64::from_le_bytes(bytes.try_into().expect("checked length"))
        }
        _ => 0,
    })
}

/// Extract literal value rows from an `INSERT ... VALUES` source.
fn source_rows(source: &SqlQuery) -> Result<Option<Vec<Vec<sqlparser::ast::Expr>>>> {
    match source.body.as_ref() {
        SetExpr::Values(values) => Ok(Some(values.rows.clone())),
        _ => Ok(None),
    }
}

/// Coerce a literal value to a column's declared type.
fn coerce(v: Value, ty: &ColumnType, col: &str) -> Result<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    Ok(match (ty, v) {
        (ColumnType::Int, Value::Int(i)) => Value::Int(i),
        (ColumnType::Int, Value::Bool(b)) => Value::Int(b as i64),
        (ColumnType::Float, Value::Int(i)) => Value::Float(i as f64),
        (ColumnType::Float, Value::Float(f)) => Value::Float(f),
        (ColumnType::Bool, Value::Bool(b)) => Value::Bool(b),
        (ColumnType::Bool, Value::Int(i)) => Value::Bool(i != 0),
        (ColumnType::Text, Value::Text(s)) => Value::Text(s),
        (ColumnType::Bytes, Value::Text(s)) => Value::Bytes(s.into_bytes()),
        (ColumnType::Bytes, Value::Bytes(b)) => Value::Bytes(b),
        (ColumnType::Vector(dim), Value::Text(s)) => Value::Vector(parse_vector(&s, *dim)?),
        (want, got) => {
            return Err(Error::Type(format!(
                "value {got:?} is not compatible with column '{col}' of type {}",
                want.display_name()
            )))
        }
    })
}

fn parse_vector(s: &str, dim: u32) -> Result<Vec<f32>> {
    let inner = s.trim().trim_start_matches('[').trim_end_matches(']');
    let vals: Result<Vec<f32>> = inner
        .split(',')
        .filter(|t| !t.trim().is_empty())
        .map(|t| t.trim().parse::<f32>().map_err(|_| Error::Type(format!("bad vector element: {t}"))))
        .collect();
    let vals = vals?;
    if vals.len() as u32 != dim {
        return Err(Error::Type(format!(
            "vector has {} elements, expected {dim}",
            vals.len()
        )));
    }
    Ok(vals)
}
