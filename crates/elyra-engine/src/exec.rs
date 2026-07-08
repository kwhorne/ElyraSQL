//! Statement execution over the clustered single-file store.
//!
//! Implements `CREATE TABLE`, `INSERT`, `SELECT ... FROM`, `DROP TABLE`.
//! Inserts are batched into one group-commit; scans stream.

use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use elyra_storage::Db;
use sqlparser::ast::{
    Assignment, AssignmentTarget, ColumnOption, CreateIndex, CreateTable, DataType, Delete,
    FromTable, Insert, JoinConstraint, JoinOperator, ObjectName, Select, SetExpr, TableConstraint,
    TableFactor, TableWithJoins, Query as SqlQuery,
};

use crate::aggregate;
use crate::index;
use crate::predicate;

use crate::catalog::{
    self, catalog_key, data_key, data_prefix, rowid_key, wcount_key, IndexDef, TableDef,
};
use crate::vindex::{read_wcount, VectorRegistry};
use elyra_vector::Metric;
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

    let def = TableDef {
        name: name.clone(),
        schema: Schema::new(columns),
        pk_col,
        indexes: Vec::new(),
    };
    db.commit(vec![(catalog_key(&name), def.encode()?)], vec![]).await?;
    Ok(QueryResult::Affected(0))
}

pub async fn create_index(db: &Db, ci: CreateIndex) -> Result<QueryResult> {
    let table = table_ident(&ci.table_name)?;
    let mut def = catalog::load(db, &table).await?;

    if ci.columns.len() != 1 {
        return Err(Error::Unsupported("composite indexes are not supported yet".into()));
    }
    let col_expr = &ci.columns[0].expr;
    let col_name = ident_name(col_expr)
        .ok_or_else(|| Error::Unsupported("index column must be a plain column".into()))?;
    let col = def
        .schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(col_name))
        .ok_or_else(|| Error::Catalog(format!("unknown column: {col_name}")))?;

    let name = match &ci.name {
        Some(n) => n.0.last().map(|i| i.value.clone()).unwrap_or_default(),
        None => format!("{table}_{col_name}_idx"),
    };
    if def.indexes.iter().any(|i| i.name.eq_ignore_ascii_case(&name)) {
        if ci.if_not_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Catalog(format!("index already exists: {name}")));
    }

    let is_vector = matches!(def.schema.columns[col].ty, ColumnType::Vector(_));
    def.indexes.push(IndexDef { name, col, unique: ci.unique, vector: is_vector });

    // Persist the new catalog and backfill index entries for existing rows.
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = vec![(catalog_key(&table), def.encode()?)];
    let prefix = data_prefix(&table);
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let chunk = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if chunk.is_empty() {
            break;
        }
        let last = chunk.len() < 4096;
        cursor = chunk.last().map(|(k, _)| k.clone());
        for (k, v) in chunk {
            let row: Vec<Value> =
                bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;
            puts.extend(index::entries_for_row(&def, &row, &k)?);
        }
        if last {
            break;
        }
    }
    db.commit(puts, vec![]).await?;
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
        let idx_entries = index::entries_for_row(&def, &row, &key)?;
        puts.push((key, encoded));
        puts.extend(idx_entries);
    }

    let affected = puts.len() as u64;

    // Persist the advanced rowid counter in the same atomic commit.
    if def.pk_col.is_none() {
        puts.push((rowid_key(&name), next_rowid.to_le_bytes().to_vec()));
    }
    puts.push(bump_wcount(db, &name).await?);

    db.commit(puts, vec![]).await?;
    Ok(QueryResult::Affected(affected))
}

pub async fn select(
    db: &Db,
    vindex: &VectorRegistry,
    query: &SqlQuery,
) -> Result<QueryResult> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(Error::Unsupported("only simple SELECT is supported".into())),
    };
    let offset = match &query.offset {
        Some(o) => eval_usize(&o.value)?,
        None => 0,
    };
    let limit = match &query.limit {
        Some(e) => Some(eval_usize(e)?),
        None => None,
    };
    let filter = select.selection.clone();

    // GROUP BY / ORDER BY.
    let group_by: Vec<Expr> = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
        sqlparser::ast::GroupByExpr::All(_) => {
            return Err(Error::Unsupported("GROUP BY ALL is not supported".into()))
        }
    };
    let order_exprs: Vec<(Expr, bool)> = match &query.order_by {
        Some(ob) => ob.exprs.iter().map(|o| (o.expr.clone(), o.asc.unwrap_or(true))).collect(),
        None => Vec::new(),
    };

    // Multi-table / JOIN queries take a dedicated materialised path.
    let is_join =
        select.from.len() > 1 || select.from.iter().any(|t| !t.joins.is_empty());
    if is_join {
        return join_select(db, select, filter, group_by, order_exprs, offset, limit).await;
    }

    if select.from.len() != 1 {
        return Err(Error::Unsupported("exactly one table in FROM is supported".into()));
    }
    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => table_ident(name)?,
        _ => return Err(Error::Unsupported("only plain table references are supported".into())),
    };
    let def = catalog::load(db, &table).await?;

    // Aggregation / grouping path: materialise, aggregate, order, page.
    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        let rows = scan_rows(db, &def, filter.as_ref()).await?;
        let (schema, mut out_rows) =
            aggregate::run(&def.schema, &select.projection, &group_by, rows)?;
        order_output_rows(&mut out_rows, &schema, &order_exprs)?;
        apply_offset_limit(&mut out_rows, offset, limit);
        return Ok(QueryResult::Rows(RowStream::literal(schema, out_rows)));
    }

    // Materialised path: needed for ORDER BY, or for expression projections
    // such as `VEC_DISTANCE(embedding, '[..]') AS dist`.
    if !order_exprs.is_empty() || !projection_is_simple(&select.projection) {
        // ORDER BY may reference a projection alias (e.g. `ORDER BY dist`);
        // substitute it with the expression it names.
        let resolved = resolve_order_aliases(&order_exprs, &select.projection, &def.schema);

        // Vector ANN fast path: `ORDER BY VEC_DISTANCE(col, q) LIMIT k` with an
        // HNSW index and no WHERE — search the index instead of scanning all.
        if filter.is_none() && offset == 0 {
            if let Some((col, q, k)) = ann_query(&resolved, limit, &def)? {
                if def.indexes.iter().any(|i| i.vector && i.col == col) {
                    let cached = vindex.get(db, &def, col, Metric::L2).await?;
                    let hits = cached.index.search(&q, k, (k * 4).max(64));
                    let mut rows = Vec::with_capacity(hits.len());
                    for (node, _) in hits {
                        if let Some(bytes) = db.get(cached.keys[node as usize].clone()).await? {
                            rows.push(
                                bincode::deserialize::<Vec<Value>>(&bytes)
                                    .map_err(|e| Error::Storage(e.to_string()))?,
                            );
                        }
                    }
                    // Order the candidate set by exact distance for a clean top-k.
                    sort_full_rows(&mut rows, &def.schema, &resolved)?;
                    rows.truncate(k);
                    let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
                    return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
                }
            }
        }

        let mut rows = scan_rows(db, &def, filter.as_ref()).await?;
        if !resolved.is_empty() {
            sort_full_rows(&mut rows, &def.schema, &resolved)?;
        }
        apply_offset_limit(&mut rows, offset, limit);
        let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
        return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
    }

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

    // Secondary-index fast path: `WHERE indexed_col = <literal>` retrieves
    // matching rows via the index instead of scanning the whole table.
    if let Some((col, _)) = eq_col_literal(&def, filter.as_ref())? {
        if def.pk_col != Some(col) && index::index_on(&def, col).is_some() {
            let mut rows: Vec<Vec<Value>> = collect_matches(db, &def, filter.as_ref(), None)
                .await?
                .into_iter()
                .map(|(_, r)| r)
                .collect();
            apply_offset_limit(&mut rows, offset, limit);
            let out: Vec<Vec<Value>> = rows
                .iter()
                .map(|r| projection.iter().map(|&i| r[i].clone()).collect())
                .collect();
            return Ok(QueryResult::Rows(RowStream::literal(out_schema, out)));
        }
    }

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

/// If `filter` is exactly `pk = <literal>` (either operand order), return the
/// literal to look up directly on the clustered key.
/// Execute a multi-table / JOIN SELECT: materialise the joined row set, then
/// apply WHERE, aggregation or ORDER BY, projection and paging.
async fn join_select(
    db: &Db,
    select: &Select,
    filter: Option<Expr>,
    group_by: Vec<Expr>,
    order_exprs: Vec<(Expr, bool)>,
    offset: usize,
    limit: Option<usize>,
) -> Result<QueryResult> {
    // Decompose the WHERE into AND-conjuncts so single-table predicates can be
    // pushed down to the base relations before joining.
    let mut conjuncts = Vec::new();
    if let Some(f) = &filter {
        split_and(f, &mut conjuncts);
    }

    let (cols, mut rows) = build_from(db, &select.from, &conjuncts).await?;
    let schema = Schema::new(cols);

    // WHERE over the joined rows.
    if let Some(f) = &filter {
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows.into_iter() {
            if predicate::matches(f, &schema, &row)? {
                kept.push(row);
            }
        }
        rows = kept;
    }

    // Aggregation / grouping.
    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        let (osch, mut orows) = aggregate::run(&schema, &select.projection, &group_by, rows)?;
        order_output_rows(&mut orows, &osch, &order_exprs)?;
        apply_offset_limit(&mut orows, offset, limit);
        return Ok(QueryResult::Rows(RowStream::literal(osch, orows)));
    }

    // ORDER BY + projection.
    let resolved = resolve_order_aliases(&order_exprs, &select.projection, &schema);
    if !resolved.is_empty() {
        sort_full_rows(&mut rows, &schema, &resolved)?;
    }
    apply_offset_limit(&mut rows, offset, limit);
    let (osch, out) = project_exprs(&select.projection, &schema, &rows)?;
    Ok(QueryResult::Rows(RowStream::literal(osch, out)))
}

/// Load a single FROM relation into `(qualified columns, rows)`. Column names
/// are qualified with the table alias (or name) as "alias.col".
///
/// If a single-table conjunct is `indexed_col = <literal>` (PK or secondary
/// index), the base relation is fetched via the O(log n) fast path instead of
/// a full scan.
/// Resolve a table reference to its definition and qualified ("alias.col")
/// columns, without reading any rows.
async fn resolve_table(db: &Db, tf: &TableFactor) -> Result<(TableDef, Vec<ColumnDef>)> {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let tname = table_ident(name)?;
            let def = catalog::load(db, &tname).await?;
            let a = alias.as_ref().map(|al| al.name.value.clone()).unwrap_or_else(|| tname.clone());
            let cols = def
                .schema
                .columns
                .iter()
                .map(|c| ColumnDef {
                    name: format!("{a}.{}", c.name),
                    ty: c.ty.clone(),
                    nullable: c.nullable,
                })
                .collect();
            Ok((def, cols))
        }
        _ => Err(Error::Unsupported("only plain table references are supported in joins".into())),
    }
}

async fn load_relation(
    db: &Db,
    tf: &TableFactor,
    conjuncts: &[Expr],
) -> Result<(Vec<ColumnDef>, Vec<Vec<Value>>)> {
    let (def, cols) = resolve_table(db, tf).await?;

    // Pick an accelerable conjunct (eq on PK / indexed column) that references
    // only this relation, and route it through the index fast path.
    let rel_schema = Schema::new(cols.clone());
    let accel = conjuncts
        .iter()
        .find(|c| refs_in_schema(c, &rel_schema) && is_accelerable(&def, c).unwrap_or(false));

    let rows = match accel {
        Some(c) => collect_matches(db, &def, Some(c), None)
            .await?
            .into_iter()
            .map(|(_, r)| r)
            .collect(),
        None => scan_rows(db, &def, None).await?,
    };
    Ok((cols, rows))
}

/// Driving-side row count at or below which we prefer an index nested-loop
/// join (probe the partner per row) over materialising the whole partner.
const NLJ_MAX_DRIVING: usize = 2048;

/// Fetch partner rows where `col == value` via PK/point or secondary index.
async fn lookup_rows_by_eq(
    db: &Db,
    def: &TableDef,
    col: usize,
    value: &Value,
) -> Result<Vec<Vec<Value>>> {
    let deser = |b: Vec<u8>| -> Result<Vec<Value>> {
        bincode::deserialize(&b).map_err(|e| Error::Storage(e.to_string()))
    };
    if def.pk_col == Some(col) {
        let key = data_key(&def.name, &keyenc::encode(value)?);
        return Ok(match db.get(key).await? {
            Some(b) => vec![deser(b)?],
            None => vec![],
        });
    }
    if let Some(idx) = index::index_on(def, col) {
        let mut out = Vec::new();
        for dk in index::lookup_eq(db, &def.name, idx, value).await? {
            if let Some(b) = db.get(dk).await? {
                out.push(deser(b)?);
            }
        }
        return Ok(out);
    }
    Err(Error::Query("column is not indexed for nested-loop join".into()))
}

/// If `on` is `A = B` with one operand referencing only the driving side and
/// the other a plain column of the partner, return `(driving_key_expr,
/// partner_col_index)` for an index nested-loop probe.
fn equi_nlj(on: &Expr, driving: &Schema, partner: &Schema) -> Option<(Expr, usize)> {
    let Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Eq, right } = on else {
        return None;
    };
    let plain = |e: &Expr| -> Option<String> {
        match e {
            Expr::Identifier(id) => Some(id.value.clone()),
            Expr::CompoundIdentifier(p) => {
                Some(p.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join("."))
            }
            _ => None,
        }
    };
    if refs_in_schema(left, driving) {
        if let Some(n) = plain(right) {
            if let Ok(i) = predicate::resolve_index(&n, partner) {
                return Some(((**left).clone(), i));
            }
        }
    }
    if refs_in_schema(right, driving) {
        if let Some(n) = plain(left) {
            if let Ok(i) = predicate::resolve_index(&n, partner) {
                return Some(((**right).clone(), i));
            }
        }
    }
    None
}

fn join_on(op: &JoinOperator) -> Result<(Option<Expr>, bool)> {
    Ok(match op {
        JoinOperator::Inner(JoinConstraint::On(e)) => (Some(e.clone()), false),
        JoinOperator::Inner(JoinConstraint::None) | JoinOperator::CrossJoin => (None, false),
        JoinOperator::LeftOuter(JoinConstraint::On(e)) => (Some(e.clone()), true),
        JoinOperator::LeftOuter(JoinConstraint::None) => (None, true),
        other => return Err(Error::Unsupported(format!("join type not supported: {other:?}"))),
    })
}

/// Whether `conjunct` is `col = <literal>` on this table's PK or an index.
fn is_accelerable(def: &TableDef, conjunct: &Expr) -> Result<bool> {
    Ok(match eq_col_literal(def, Some(conjunct))? {
        Some((col, _)) => def.pk_col == Some(col) || index::index_on(def, col).is_some(),
        None => false,
    })
}

/// Build the joined row set from a FROM clause (comma cross-joins + explicit
/// JOINs), pushing single-table `conjuncts` down to each base relation.
async fn build_from(
    db: &Db,
    from: &[TableWithJoins],
    conjuncts: &[Expr],
) -> Result<(Vec<ColumnDef>, Vec<Vec<Value>>)> {
    let mut cur_cols: Vec<ColumnDef> = Vec::new();
    let mut cur_rows: Vec<Vec<Value>> = Vec::new();
    let mut first = true;

    for twj in from {
        let (bc, mut br) = load_relation(db, &twj.relation, conjuncts).await?;
        br = apply_pushdown(br, &bc, conjuncts)?;
        if first {
            cur_cols = bc;
            cur_rows = br;
            first = false;
        } else {
            let (c, r) = combine(&cur_cols, &cur_rows, &bc, &br, &JoinOperator::CrossJoin)?;
            cur_cols = c;
            cur_rows = r;
        }
        for join in &twj.joins {
            // Index nested-loop join: when the driving side is small and the
            // partner is indexed on the join key, probe the partner per row
            // instead of materialising it in full.
            let (pdef, pcols) = resolve_table(db, &join.relation).await?;
            let driving_schema = Schema::new(cur_cols.clone());
            let partner_schema = Schema::new(pcols.clone());
            let (on, left_outer) = join_on(&join.join_operator)?;

            let nlj = on
                .as_ref()
                .and_then(|e| equi_nlj(e, &driving_schema, &partner_schema))
                .filter(|(_, pcol)| {
                    cur_rows.len() <= NLJ_MAX_DRIVING
                        && (pdef.pk_col == Some(*pcol) || index::index_on(&pdef, *pcol).is_some())
                });

            if let Some((driving_key, pcol)) = nlj {
                let plen = pcols.len();
                let mut out = Vec::new();
                for l in &cur_rows {
                    let v = predicate::eval_row(&driving_key, &driving_schema, l)?;
                    let matches = if v.is_null() {
                        Vec::new()
                    } else {
                        lookup_rows_by_eq(db, &pdef, pcol, &v).await?
                    };
                    let mut matched = false;
                    for m in matches {
                        let mut combined = l.clone();
                        combined.extend(m);
                        out.push(combined);
                        matched = true;
                    }
                    if left_outer && !matched {
                        let mut combined = l.clone();
                        combined.extend(std::iter::repeat(Value::Null).take(plen));
                        out.push(combined);
                    }
                }
                cur_cols.extend(pcols);
                cur_rows = out;
                continue;
            }

            // Fallback: materialise the partner (with pushdown) and hash/nested join.
            let (jc, mut jr) = load_relation(db, &join.relation, conjuncts).await?;
            jr = apply_pushdown(jr, &jc, conjuncts)?;
            let (c, r) = combine(&cur_cols, &cur_rows, &jc, &jr, &join.join_operator)?;
            cur_cols = c;
            cur_rows = r;
        }
    }
    Ok((cur_cols, cur_rows))
}

/// Filter `rows` by every conjunct that references only this relation's
/// columns (predicate pushdown).
fn apply_pushdown(
    rows: Vec<Vec<Value>>,
    cols: &[ColumnDef],
    conjuncts: &[Expr],
) -> Result<Vec<Vec<Value>>> {
    let schema = Schema::new(cols.to_vec());
    let applicable: Vec<&Expr> =
        conjuncts.iter().filter(|c| refs_in_schema(c, &schema)).collect();
    if applicable.is_empty() {
        return Ok(rows);
    }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut keep = true;
        for c in &applicable {
            if !predicate::matches(c, &schema, &row)? {
                keep = false;
                break;
            }
        }
        if keep {
            out.push(row);
        }
    }
    Ok(out)
}

/// Combine two materialised relations under a join operator. Uses a hash join
/// for equi-join `ON`s (`l.x = r.y`), falling back to nested-loop otherwise.
fn combine(
    lcols: &[ColumnDef],
    lrows: &[Vec<Value>],
    rcols: &[ColumnDef],
    rrows: &[Vec<Value>],
    op: &JoinOperator,
) -> Result<(Vec<ColumnDef>, Vec<Vec<Value>>)> {
    let (on, left_outer) = match op {
        JoinOperator::Inner(JoinConstraint::On(e)) => (Some(e.clone()), false),
        JoinOperator::Inner(JoinConstraint::None) | JoinOperator::CrossJoin => (None, false),
        JoinOperator::LeftOuter(JoinConstraint::On(e)) => (Some(e.clone()), true),
        JoinOperator::LeftOuter(JoinConstraint::None) => (None, true),
        other => {
            return Err(Error::Unsupported(format!("join type not supported: {other:?}")))
        }
    };

    let mut cols = lcols.to_vec();
    cols.extend_from_slice(rcols);
    let lschema = Schema::new(lcols.to_vec());
    let rschema = Schema::new(rcols.to_vec());

    // Try to plan a hash join from an equi-`ON`.
    if let Some(e) = &on {
        if let Some((lkey, rkey)) = equi_keys(e, &lschema, &rschema) {
            return Ok((cols, hash_join(lrows, rrows, &lschema, &rschema, &lkey, &rkey, left_outer)?));
        }
    }

    // Nested-loop fallback.
    let schema = Schema::new(cols.clone());
    let rlen = rcols.len();
    let mut out = Vec::new();
    for l in lrows {
        let mut matched = false;
        for r in rrows {
            let mut combined = l.clone();
            combined.extend_from_slice(r);
            let keep = match &on {
                Some(e) => predicate::matches(e, &schema, &combined)?,
                None => true,
            };
            if keep {
                out.push(combined);
                matched = true;
            }
        }
        if left_outer && !matched {
            let mut combined = l.clone();
            combined.extend(std::iter::repeat(Value::Null).take(rlen));
            out.push(combined);
        }
    }
    Ok((cols, out))
}

/// Hash join: build a map on the right key, probe with the left key.
#[allow(clippy::too_many_arguments)]
fn hash_join(
    lrows: &[Vec<Value>],
    rrows: &[Vec<Value>],
    lschema: &Schema,
    rschema: &Schema,
    lkey: &Expr,
    rkey: &Expr,
    left_outer: bool,
) -> Result<Vec<Vec<Value>>> {
    use std::collections::HashMap;
    let rlen = rschema.columns.len();
    let mut table: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, r) in rrows.iter().enumerate() {
        let k = key_str(&predicate::eval_row(rkey, rschema, r)?);
        if let Some(k) = k {
            table.entry(k).or_default().push(i);
        }
    }

    let mut out = Vec::new();
    for l in lrows {
        let probe = key_str(&predicate::eval_row(lkey, lschema, l)?);
        let mut matched = false;
        if let Some(k) = probe {
            if let Some(idxs) = table.get(&k) {
                for &i in idxs {
                    let mut combined = l.clone();
                    combined.extend_from_slice(&rrows[i]);
                    out.push(combined);
                    matched = true;
                }
            }
        }
        if left_outer && !matched {
            let mut combined = l.clone();
            combined.extend(std::iter::repeat(Value::Null).take(rlen));
            out.push(combined);
        }
    }
    Ok(out)
}

/// Hash-key string for a value; `None` for NULL (never matches, per SQL).
fn key_str(v: &Value) -> Option<String> {
    if v.is_null() {
        None
    } else {
        Some(format!("{v:?}"))
    }
}

/// If `on` is `A = B` with one operand referencing only the left relation and
/// the other only the right, return `(left_key_expr, right_key_expr)`.
fn equi_keys(on: &Expr, lschema: &Schema, rschema: &Schema) -> Option<(Expr, Expr)> {
    let Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::Eq, right } = on else {
        return None;
    };
    if refs_in_schema(left, lschema) && refs_in_schema(right, rschema) {
        Some(((**left).clone(), (**right).clone()))
    } else if refs_in_schema(right, lschema) && refs_in_schema(left, rschema) {
        Some(((**right).clone(), (**left).clone()))
    } else {
        None
    }
}

/// Split an expression on top-level `AND` into conjuncts.
fn split_and(expr: &Expr, out: &mut Vec<Expr>) {
    if let Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::And, right } = expr {
        split_and(left, out);
        split_and(right, out);
    } else {
        out.push(expr.clone());
    }
}

/// True if every column referenced by `expr` resolves within `schema` (and the
/// expression is fully analysable).
fn refs_in_schema(expr: &Expr, schema: &Schema) -> bool {
    let mut refs = Vec::new();
    if !collect_refs(expr, &mut refs) {
        return false;
    }
    refs.iter().all(|r| predicate::resolve_index(r, schema).is_ok())
}

/// Collect column references from `expr`. Returns false if the expression
/// contains a construct we do not analyse (so callers stay conservative).
fn collect_refs(expr: &Expr, out: &mut Vec<String>) -> bool {
    match expr {
        Expr::Identifier(id) => {
            out.push(id.value.clone());
            true
        }
        Expr::CompoundIdentifier(parts) => {
            out.push(parts.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join("."));
            true
        }
        Expr::Value(_) => true,
        Expr::Nested(e) | Expr::UnaryOp { expr: e, .. } => collect_refs(e, out),
        Expr::IsNull(e) | Expr::IsNotNull(e) => collect_refs(e, out),
        Expr::BinaryOp { left, right, .. } => collect_refs(left, out) && collect_refs(right, out),
        Expr::Between { expr, low, high, .. } => {
            collect_refs(expr, out) && collect_refs(low, out) && collect_refs(high, out)
        }
        Expr::Function(f) => {
            if let sqlparser::ast::FunctionArguments::List(list) = &f.args {
                for a in &list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = a
                    {
                        if !collect_refs(e, out) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            } else {
                false
            }
        }
        _ => false,
    }
}

/// If `filter` is exactly `col = <literal>` (either operand order), return the
/// column index and the literal value.
fn eq_col_literal(def: &TableDef, filter: Option<&Expr>) -> Result<Option<(usize, Value)>> {
    use sqlparser::ast::BinaryOperator;
    let Some(Expr::BinaryOp { left, op: BinaryOperator::Eq, right }) = filter else {
        return Ok(None);
    };
    let (name, lit_expr): (&str, &Expr) = match (ident_name(left), ident_name(right)) {
        (Some(n), None) => (n, right),
        (None, Some(n)) => (n, left),
        _ => return Ok(None),
    };
    let Some(idx) = def
        .schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
    else {
        return Ok(None);
    };
    Ok(Some((idx, eval_expr(lit_expr)?)))
}

fn pk_eq_literal(def: &TableDef, filter: Option<&Expr>) -> Result<Option<Value>> {
    let Some(pk) = def.pk_col else { return Ok(None) };
    Ok(match eq_col_literal(def, filter)? {
        Some((idx, lit)) if idx == pk => Some(lit),
        _ => None,
    })
}

async fn try_pk_lookup(db: &Db, def: &TableDef, filter: Option<&Expr>) -> Result<PkLookup> {
    let Some(lit) = pk_eq_literal(def, filter)? else { return Ok(PkLookup::NotApplicable) };
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

/// Collect `(storage_key, row)` for every row matching `filter`, up to
/// `limit`. Uses the PK point-lookup fast path when possible, otherwise a
/// bounded-batch clustered scan.
async fn collect_matches(
    db: &Db,
    def: &TableDef,
    filter: Option<&Expr>,
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let mut out = Vec::new();

    if pk_eq_literal(def, filter)?.is_some() {
        if let PkLookup::Found(row) = try_pk_lookup(db, def, filter).await? {
            let lit = pk_eq_literal(def, filter)?.expect("checked");
            out.push((data_key(&def.name, &keyenc::encode(&lit)?), row));
        }
        return Ok(out);
    }

    // Secondary-index fast path: `WHERE indexed_col = <literal>`.
    if let Some((col, lit)) = eq_col_literal(def, filter)? {
        if let Some(idx) = index::index_on(def, col) {
            for data_key in index::lookup_eq(db, &def.name, idx, &lit).await? {
                if let Some(bytes) = db.get(data_key.clone()).await? {
                    let row: Vec<Value> =
                        bincode::deserialize(&bytes).map_err(|e| Error::Storage(e.to_string()))?;
                    out.push((data_key, row));
                    if let Some(l) = limit {
                        if out.len() >= l {
                            return Ok(out);
                        }
                    }
                }
            }
            return Ok(out);
        }
    }

    let prefix = data_prefix(&def.name);
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let chunk = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if chunk.is_empty() {
            break;
        }
        let last = chunk.len() < 4096;
        cursor = chunk.last().map(|(k, _)| k.clone());
        for (k, v) in chunk {
            let row: Vec<Value> =
                bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;
            let keep = match filter {
                Some(f) => predicate::matches(f, &def.schema, &row)?,
                None => true,
            };
            if keep {
                out.push((k, row));
                if let Some(l) = limit {
                    if out.len() >= l {
                        return Ok(out);
                    }
                }
            }
        }
        if last {
            break;
        }
    }
    Ok(out)
}

fn table_of(twj: &TableWithJoins) -> Result<String> {
    match &twj.relation {
        TableFactor::Table { name, .. } => table_ident(name),
        _ => Err(Error::Unsupported("only plain table references are supported".into())),
    }
}

pub async fn update(
    db: &Db,
    table: &TableWithJoins,
    assignments: &[Assignment],
    selection: Option<&Expr>,
) -> Result<QueryResult> {
    let name = table_of(table)?;
    let def = catalog::load(db, &name).await?;

    // Resolve assignment targets to column indices.
    let mut sets: Vec<(usize, &Expr)> = Vec::with_capacity(assignments.len());
    for a in assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(n) => n
                .0
                .last()
                .map(|i| i.value.clone())
                .ok_or_else(|| Error::Query("empty assignment target".into()))?,
            AssignmentTarget::Tuple(_) => {
                return Err(Error::Unsupported("tuple assignment is not supported".into()))
            }
        };
        let idx = def
            .schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col))
            .ok_or_else(|| Error::Catalog(format!("unknown column: {col}")))?;
        sets.push((idx, &a.value));
    }

    let matches = collect_matches(db, &def, selection, None).await?;
    let affected = matches.len() as u64;

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();

    for (old_key, old_row) in matches {
        let mut new_row = old_row.clone();
        for (idx, expr) in &sets {
            // Assignment RHS may reference existing column values.
            let v = predicate::eval_row(expr, &def.schema, &old_row)?;
            let col = &def.schema.columns[*idx];
            new_row[*idx] = coerce(v, &col.ty, &col.name)?;
        }
        for (i, col) in def.schema.columns.iter().enumerate() {
            if !col.nullable && new_row[i].is_null() {
                return Err(Error::Query(format!("column '{}' cannot be NULL", col.name)));
            }
        }

        // If the primary key changed, the clustered key moves.
        let new_key = match def.pk_col {
            Some(i) => data_key(&name, &keyenc::encode(&new_row[i])?),
            None => old_key.clone(),
        };

        // Index maintenance: drop old entries, write new ones. Deletes are
        // applied before puts, so unchanged index entries survive.
        deletes.extend(index::entry_keys_for_row(&def, &old_row, &old_key)?);
        let new_index_entries = index::entries_for_row(&def, &new_row, &new_key)?;
        if new_key != old_key {
            deletes.push(old_key);
        }
        let encoded = bincode::serialize(&new_row).map_err(|e| Error::Storage(e.to_string()))?;
        puts.push((new_key, encoded));
        puts.extend(new_index_entries);
    }

    puts.push(bump_wcount(db, &name).await?);
    db.commit(puts, deletes).await?;
    Ok(QueryResult::Affected(affected))
}

pub async fn delete(db: &Db, del: &Delete) -> Result<QueryResult> {
    let relations = match &del.from {
        FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
    };
    if relations.len() != 1 {
        return Err(Error::Unsupported("multi-table DELETE is not supported".into()));
    }
    let name = table_of(&relations[0])?;
    let def = catalog::load(db, &name).await?;

    let limit = match &del.limit {
        Some(e) => Some(eval_usize(e)?),
        None => None,
    };

    let matches = collect_matches(db, &def, del.selection.as_ref(), limit).await?;
    let affected = matches.len() as u64;

    let mut deletes: Vec<Vec<u8>> = Vec::with_capacity(matches.len());
    for (key, row) in matches {
        deletes.extend(index::entry_keys_for_row(&def, &row, &key)?);
        deletes.push(key);
    }

    let wc = bump_wcount(db, &name).await?;
    db.commit(vec![wc], deletes).await?;
    Ok(QueryResult::Affected(affected))
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

/// True when every projection item is `*` or a bare column reference, so it
/// can go through the streaming scan path.
/// Rewrite ORDER BY items that name a projection alias into the aliased
/// expression, so sorting can evaluate them against the table row.
fn resolve_order_aliases(
    order: &[(Expr, bool)],
    projection: &[sqlparser::ast::SelectItem],
    schema: &Schema,
) -> Vec<(Expr, bool)> {
    use sqlparser::ast::SelectItem;
    order
        .iter()
        .map(|(e, asc)| {
            if let Some(name) = ident_name(e) {
                let is_column = schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(name));
                if !is_column {
                    for item in projection {
                        if let SelectItem::ExprWithAlias { expr, alias } = item {
                            if alias.value.eq_ignore_ascii_case(name) {
                                return (expr.clone(), *asc);
                            }
                        }
                    }
                }
            }
            (e.clone(), *asc)
        })
        .collect()
}

fn projection_is_simple(projection: &[sqlparser::ast::SelectItem]) -> bool {
    use sqlparser::ast::SelectItem;
    projection.iter().all(|item| match item {
        SelectItem::Wildcard(_) => true,
        SelectItem::UnnamedExpr(e) => ident_name(e).is_some(),
        SelectItem::ExprWithAlias { expr, .. } => ident_name(expr).is_some(),
        _ => false,
    })
}

/// Project (possibly expression) columns over materialised rows. Supports
/// `*`, bare columns, and scalar expressions like `VEC_DISTANCE(...)`.
fn project_exprs(
    projection: &[sqlparser::ast::SelectItem],
    schema: &Schema,
    rows: &[Vec<Value>],
) -> Result<(Schema, Vec<Vec<Value>>)> {
    use sqlparser::ast::SelectItem;

    enum Proj<'a> {
        Col(usize),
        Expr(&'a Expr),
    }
    let mut names: Vec<String> = Vec::new();
    let mut projs: Vec<Proj> = Vec::new();

    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (i, c) in schema.columns.iter().enumerate() {
                    names.push(c.name.clone());
                    projs.push(Proj::Col(i));
                }
            }
            SelectItem::UnnamedExpr(e) => {
                names.push(ident_name(e).map(|s| s.to_string()).unwrap_or_else(|| e.to_string()));
                projs.push(Proj::Expr(e));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                names.push(alias.value.clone());
                projs.push(Proj::Expr(expr));
            }
            other => {
                return Err(Error::Unsupported(format!("projection item not supported: {other}")))
            }
        }
    }

    let mut out_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut o = Vec::with_capacity(projs.len());
        for p in &projs {
            o.push(match p {
                Proj::Col(i) => row[*i].clone(),
                Proj::Expr(e) => predicate::eval_row(e, schema, row)?,
            });
        }
        out_rows.push(o);
    }

    // Infer output column types (from the source column, else the first row).
    let mut cols = Vec::with_capacity(projs.len());
    for (ci, (name, p)) in names.iter().zip(&projs).enumerate() {
        let ty = match p {
            Proj::Col(i) => schema.columns[*i].ty.clone(),
            Proj::Expr(e) => match ident_name(e).and_then(|n| {
                schema.columns.iter().find(|c| c.name.eq_ignore_ascii_case(n))
            }) {
                Some(c) => c.ty.clone(),
                None => out_rows.first().map(|r| infer_val(&r[ci])).unwrap_or(ColumnType::Text),
            },
        };
        cols.push(ColumnDef { name: name.clone(), ty, nullable: true });
    }

    Ok((Schema::new(cols), out_rows))
}

fn infer_val(v: &Value) -> ColumnType {
    match v {
        Value::Bool(_) => ColumnType::Bool,
        Value::Int(_) => ColumnType::Int,
        Value::Float(_) => ColumnType::Float,
        Value::Bytes(_) => ColumnType::Bytes,
        Value::Vector(x) => ColumnType::Vector(x.len() as u32),
        _ => ColumnType::Text,
    }
}

/// Detect the `ORDER BY VEC_DISTANCE(col, <literal>) ASC LIMIT k` pattern.
/// Returns the vector column index, the query vector, and k.
fn ann_query(
    resolved: &[(Expr, bool)],
    limit: Option<usize>,
    def: &TableDef,
) -> Result<Option<(usize, Vec<f32>, usize)>> {
    let Some(k) = limit else { return Ok(None) };
    if resolved.len() != 1 || !resolved[0].1 {
        return Ok(None);
    }
    let Expr::Function(f) = &resolved[0].0 else { return Ok(None) };
    let name = f.name.0.last().map(|i| i.value.to_ascii_lowercase()).unwrap_or_default();
    // Only the L2 family is accelerated (HNSW is built with L2).
    if !matches!(name.as_str(), "vec_distance" | "vec_l2_distance" | "vec_distance_l2") {
        return Ok(None);
    }
    let args = fn_arg_exprs(f);
    if args.len() != 2 {
        return Ok(None);
    }
    let (col, lit_expr) = match (ident_name(args[0]), ident_name(args[1])) {
        (Some(n), None) => (col_of(def, n), args[1]),
        (None, Some(n)) => (col_of(def, n), args[0]),
        _ => return Ok(None),
    };
    let Some(col) = col else { return Ok(None) };
    if !matches!(def.schema.columns[col].ty, ColumnType::Vector(_)) {
        return Ok(None);
    }
    let q = match eval_expr(lit_expr)? {
        Value::Text(s) => parse_vec_free(&s)?,
        Value::Vector(v) => v,
        _ => return Ok(None),
    };
    Ok(Some((col, q, k)))
}

fn fn_arg_exprs(f: &sqlparser::ast::Function) -> Vec<&Expr> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let FunctionArguments::List(list) = &f.args else { return Vec::new() };
    let mut out = Vec::new();
    for a in &list.args {
        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = a {
            out.push(e);
        } else {
            return Vec::new();
        }
    }
    out
}

fn col_of(def: &TableDef, name: &str) -> Option<usize> {
    def.schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
}

fn parse_vec_free(s: &str) -> Result<Vec<f32>> {
    let inner = s.trim().trim_start_matches('[').trim_end_matches(']');
    inner
        .split(',')
        .filter(|t| !t.trim().is_empty())
        .map(|t| t.trim().parse::<f32>().map_err(|_| Error::Vector(format!("bad vector element: {t}"))))
        .collect()
}

/// Produce the `(key, value)` put that advances a table's write counter.
async fn bump_wcount(db: &Db, table: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let next = read_wcount(db, table).await? + 1;
    Ok((wcount_key(table), next.to_le_bytes().to_vec()))
}

/// Materialise all rows matching `filter` (drops storage keys).
async fn scan_rows(db: &Db, def: &TableDef, filter: Option<&Expr>) -> Result<Vec<Vec<Value>>> {
    Ok(collect_matches(db, def, filter, None)
        .await?
        .into_iter()
        .map(|(_, r)| r)
        .collect())
}

fn apply_offset_limit(rows: &mut Vec<Vec<Value>>, offset: usize, limit: Option<usize>) {
    if offset > 0 {
        rows.drain(0..offset.min(rows.len()));
    }
    if let Some(l) = limit {
        rows.truncate(l);
    }
}

/// Sort full table rows by ORDER BY expressions evaluated against the row.
fn sort_full_rows(
    rows: &mut [Vec<Value>],
    schema: &Schema,
    order: &[(Expr, bool)],
) -> Result<()> {
    // Precompute sort keys once per row.
    let mut keyed: Vec<(Vec<Value>, usize)> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let mut keys = Vec::with_capacity(order.len());
        for (e, _) in order {
            keys.push(predicate::eval_row(e, schema, row)?);
        }
        keyed.push((keys, i));
    }
    sort_keyed(&mut keyed, order);
    reorder(rows, &keyed);
    Ok(())
}

/// Sort already-computed output rows by ORDER BY referencing output columns.
fn order_output_rows(
    rows: &mut [Vec<Value>],
    schema: &Schema,
    order: &[(Expr, bool)],
) -> Result<()> {
    if order.is_empty() {
        return Ok(());
    }
    // Resolve each order expr to an output column index.
    let mut cols = Vec::with_capacity(order.len());
    for (e, _) in order {
        let name = ident_name(e).map(|s| s.to_string()).unwrap_or_else(|| e.to_string());
        let idx = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&name))
            .ok_or_else(|| Error::Query(format!("ORDER BY references unknown output column: {name}")))?;
        cols.push(idx);
    }
    let mut keyed: Vec<(Vec<Value>, usize)> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| (cols.iter().map(|&c| row[c].clone()).collect(), i))
        .collect();
    sort_keyed(&mut keyed, order);
    reorder(rows, &keyed);
    Ok(())
}

fn sort_keyed(keyed: &mut [(Vec<Value>, usize)], order: &[(Expr, bool)]) {
    keyed.sort_by(|a, b| {
        for (i, (_, asc)) in order.iter().enumerate() {
            let ord = aggregate::value_cmp(&a.0[i], &b.0[i]);
            let ord = if *asc { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn reorder(rows: &mut [Vec<Value>], keyed: &[(Vec<Value>, usize)]) {
    let snapshot: Vec<Vec<Value>> = keyed.iter().map(|(_, i)| rows[*i].clone()).collect();
    for (slot, row) in rows.iter_mut().zip(snapshot) {
        *slot = row;
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
