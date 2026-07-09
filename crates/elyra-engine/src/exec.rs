//! Statement execution over the clustered single-file store.
//!
//! Implements `CREATE TABLE`, `INSERT`, `SELECT ... FROM`, `DROP TABLE`.
//! Inserts are batched into one group-commit; scans stream.

use crate::session::Session;
use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, Assignment, AssignmentTarget, ColumnOption,
    CreateIndex, CreateTable, DataType, Delete, FromTable, Insert, JoinConstraint, JoinOperator,
    ObjectName, Query as SqlQuery, Select, SetExpr, TableConstraint, TableFactor, TableWithJoins,
};

use crate::aggregate;
use crate::aggregate::AggPlan;
use crate::index;
use crate::predicate;
use elyra_olap::GroupAggregator;

use crate::catalog::{
    self, autoinc_key, catalog_key, data_key, data_prefix, index_table_prefix, rowid_key,
    wcount_key, ColMeta, ForeignKey, IndexDef, RefAction, TableDef,
};
use crate::eval::eval_expr;
use crate::keyenc;
use crate::stream::{RowStream, ScanSpec};
use crate::vindex::{read_wcount, VectorRegistry};
use crate::QueryResult;
use elyra_vector::Metric;
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
        DataType::Float(_) | DataType::Real | DataType::Double => ColumnType::Float,
        DataType::Text | DataType::String(_) | DataType::Varchar(_) | DataType::Char(_) => {
            ColumnType::Text
        }
        // ENUM/SET are stored as their string value.
        DataType::Enum(..) | DataType::Set(_) => ColumnType::Text,
        DataType::Blob(_) | DataType::Bytea | DataType::Binary(_) | DataType::Varbinary(_) => {
            ColumnType::Bytes
        }
        // BIT(n) is stored as an integer.
        DataType::Bit(_) | DataType::BitVarying(_) => ColumnType::Int,
        DataType::Date => ColumnType::Date,
        DataType::Datetime(_) | DataType::Timestamp(_, _) => ColumnType::DateTime,
        DataType::Time(_, _) => ColumnType::Time,
        DataType::JSON | DataType::JSONB => ColumnType::Json,
        DataType::Decimal(info) | DataType::Numeric(info) => {
            let (p, s) = match info {
                sqlparser::ast::ExactNumberInfo::None => (10, 0),
                sqlparser::ast::ExactNumberInfo::Precision(p) => (*p as u8, 0),
                sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as u8),
            };
            ColumnType::Decimal(p, s)
        }
        DataType::Custom(name, args)
            if name
                .0
                .last()
                .map(|i| i.value.eq_ignore_ascii_case("vector"))
                .unwrap_or(false) =>
        {
            let dim = args
                .first()
                .and_then(|s| s.parse::<u32>().ok())
                .ok_or_else(|| {
                    Error::Type("VECTOR requires a dimension, e.g. VECTOR(768)".into())
                })?;
            ColumnType::Vector(dim)
        }
        // Spatial geometry columns are stored as WKT text.
        DataType::Custom(name, _)
            if name
                .0
                .last()
                .map(|i| {
                    matches!(
                        i.value.to_ascii_lowercase().as_str(),
                        "point"
                            | "geometry"
                            | "linestring"
                            | "polygon"
                            | "geometrycollection"
                            | "multipoint"
                            | "multilinestring"
                            | "multipolygon"
                    )
                })
                .unwrap_or(false) =>
        {
            ColumnType::Text
        }
        other => {
            return Err(Error::Unsupported(format!(
                "column type not supported: {other}"
            )))
        }
    })
}

fn is_tinyint_bool(_dt: &DataType) -> bool {
    false
}

pub async fn create_table(
    db: &Session,
    vindex: &VectorRegistry,
    ct: CreateTable,
) -> Result<QueryResult> {
    let name = table_ident(&ct.name)?;

    if catalog::exists(db, &name).await? {
        if ct.if_not_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Catalog(format!("table already exists: {name}")));
    }

    // CREATE TABLE ... LIKE source: copy the structure, no data.
    if let Some(src) = &ct.like {
        let sname = src
            .0
            .last()
            .map(|i| i.value.clone())
            .ok_or_else(|| Error::Catalog("empty source table".into()))?;
        let mut def = catalog::load(db, &sname).await?;
        def.name = name.clone();
        db.commit_write(vec![(catalog_key(&name), def.encode()?)], vec![])
            .await?;
        return Ok(QueryResult::Affected(0));
    }

    // CREATE TABLE ... AS SELECT: derive structure from the query, copy rows.
    if let Some(q) = &ct.query {
        return create_table_as(db, vindex, &name, &ct, q).await;
    }

    let mut columns = Vec::with_capacity(ct.columns.len());
    let mut col_meta: Vec<ColMeta> = Vec::with_capacity(ct.columns.len());
    let mut pk_cols: Vec<usize> = Vec::new();
    let mut indexes: Vec<IndexDef> = Vec::new();
    let mut checks: Vec<String> = Vec::new();
    let mut foreign_keys: Vec<ForeignKey> = Vec::new();

    for (idx, col) in ct.columns.iter().enumerate() {
        let ty = map_type(&col.data_type)?;
        let mut nullable = true;
        let mut meta = ColMeta::default();
        let collation = col
            .collation
            .as_ref()
            .map(|c| elyra_core::Collation::from_name(&c.to_string()))
            .unwrap_or_default();
        for opt in &col.options {
            match &opt.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Unique { is_primary, .. } => {
                    if *is_primary {
                        pk_cols.push(idx);
                        nullable = false;
                    } else {
                        indexes.push(IndexDef {
                            name: format!("uniq_{}", col.name.value),
                            cols: vec![idx],
                            unique: true,
                            vector: false,
                            fulltext: false,
                            col_collations: vec![collation],
                        });
                    }
                }
                ColumnOption::Default(e) => meta.default = Some(e.to_string()),
                ColumnOption::Generated {
                    generation_expr: Some(e),
                    ..
                } => meta.generated = Some(e.to_string()),
                ColumnOption::DialectSpecific(tokens)
                    if tokens
                        .iter()
                        .any(|t| t.to_string().eq_ignore_ascii_case("AUTO_INCREMENT")) =>
                {
                    meta.auto_increment = true;
                }
                ColumnOption::Check(e) => checks.push(e.to_string()),
                _ => {}
            }
        }
        columns.push(ColumnDef {
            name: col.name.value.clone(),
            ty,
            nullable,
            collation,
        });
        col_meta.push(meta);
    }

    // Table-level PRIMARY KEY / UNIQUE (single or composite).
    for c in &ct.constraints {
        match c {
            TableConstraint::PrimaryKey { columns: cols, .. } => {
                pk_cols.clear();
                for ident in cols {
                    let i = columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                        .ok_or_else(|| {
                            Error::Catalog(format!("unknown primary key column: {}", ident.value))
                        })?;
                    columns[i].nullable = false;
                    pk_cols.push(i);
                }
            }
            TableConstraint::Unique {
                name: cname,
                columns: cols,
                ..
            } => {
                let mut idxs = Vec::new();
                for ident in cols {
                    let i = columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                        .ok_or_else(|| {
                            Error::Catalog(format!("unknown unique column: {}", ident.value))
                        })?;
                    idxs.push(i);
                }
                let iname = cname.as_ref().map(|n| n.value.clone()).unwrap_or_else(|| {
                    format!(
                        "uniq_{}",
                        idxs.iter()
                            .map(|&i| columns[i].name.clone())
                            .collect::<Vec<_>>()
                            .join("_")
                    )
                });
                let ucolls: Vec<elyra_core::Collation> =
                    idxs.iter().map(|&i| columns[i].collation).collect();
                indexes.push(IndexDef {
                    name: iname,
                    cols: idxs,
                    unique: true,
                    vector: false,
                    fulltext: false,
                    col_collations: ucolls,
                });
            }
            TableConstraint::Check { expr, .. } => checks.push(expr.to_string()),
            TableConstraint::ForeignKey {
                name: fname,
                columns: cols,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                let mut fk_cols = Vec::new();
                for ident in cols {
                    let i = columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                        .ok_or_else(|| {
                            Error::Catalog(format!("unknown foreign key column: {}", ident.value))
                        })?;
                    fk_cols.push(i);
                }
                // Index the referencing columns so parent-side checks (RESTRICT
                // / CASCADE / SET NULL) can find child rows efficiently.
                if !indexes.iter().any(|ix| ix.cols == fk_cols) && pk_cols != fk_cols {
                    let fkcolls: Vec<elyra_core::Collation> =
                        fk_cols.iter().map(|&i| columns[i].collation).collect();
                    indexes.push(IndexDef {
                        name: format!("fk_{name}_{}", foreign_keys.len()),
                        cols: fk_cols.clone(),
                        unique: false,
                        vector: false,
                        fulltext: false,
                        col_collations: fkcolls,
                    });
                }
                foreign_keys.push(ForeignKey {
                    name: fname
                        .as_ref()
                        .map(|n| n.value.clone())
                        .unwrap_or_else(|| format!("fk_{name}_{}", foreign_keys.len())),
                    columns: fk_cols,
                    ref_table: foreign_table
                        .0
                        .last()
                        .map(|i| i.value.clone())
                        .unwrap_or_default(),
                    ref_columns: referred_columns.iter().map(|i| i.value.clone()).collect(),
                    on_delete: map_ref_action(on_delete),
                    on_update: map_ref_action(on_update),
                });
            }
            _ => {}
        }
    }

    let def = TableDef {
        name: name.clone(),
        schema: Schema::new(columns),
        pk_cols,
        indexes,
        col_meta,
        checks,
        foreign_keys,
    };
    db.commit_write(vec![(catalog_key(&name), def.encode()?)], vec![])
        .await?;
    Ok(QueryResult::Affected(0))
}

/// SHOW TABLES: one column of user table names.
pub async fn show_tables(db: &Session) -> Result<QueryResult> {
    let names = catalog::list_tables(db).await?;
    let schema = Schema::new(vec![ColumnDef {
        name: "Tables_in_elyra".into(),
        ty: ColumnType::Text,
        nullable: false,
        collation: elyra_core::Collation::Ci,
    }]);
    let rows = names.into_iter().map(|n| vec![Value::Text(n)]).collect();
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// SHOW COLUMNS / DESCRIBE: column metadata (Field/Type/Null/Key/Default/Extra).
pub async fn show_columns(db: &Session, table: &str) -> Result<QueryResult> {
    let def = catalog::load(db, table).await?;
    let head = ["Field", "Type", "Null", "Key", "Default", "Extra"];
    let schema = Schema::new(
        head.iter()
            .map(|n| ColumnDef {
                name: (*n).to_string(),
                ty: ColumnType::Text,
                nullable: *n == "Default",
                collation: elyra_core::Collation::Ci,
            })
            .collect(),
    );
    let mut rows = Vec::with_capacity(def.schema.columns.len());
    for (i, c) in def.schema.columns.iter().enumerate() {
        let meta = def.meta(i);
        let key = if def.pk_cols.contains(&i) {
            "PRI"
        } else if def.indexes.iter().any(|idx| idx.unique && idx.cols == [i]) {
            "UNI"
        } else if def.indexes.iter().any(|idx| idx.cols.first() == Some(&i)) {
            "MUL"
        } else {
            ""
        };
        let default = match &meta.default {
            Some(d) => Value::Text(d.clone()),
            None => Value::Null,
        };
        let extra = if meta.auto_increment {
            "auto_increment"
        } else if meta.generated.is_some() {
            "STORED GENERATED"
        } else {
            ""
        };
        rows.push(vec![
            Value::Text(c.name.clone()),
            Value::Text(c.ty.display_name()),
            Value::Text(if c.nullable { "YES" } else { "NO" }.to_string()),
            Value::Text(key.to_string()),
            default,
            Value::Text(extra.to_string()),
        ]);
    }
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// SHOW CREATE TABLE: reconstruct the DDL from the catalog definition.
pub async fn show_create_table(db: &Session, name: &str) -> Result<QueryResult> {
    let def = catalog::load(db, name).await?;
    let mut lines: Vec<String> = Vec::new();
    for (i, c) in def.schema.columns.iter().enumerate() {
        let meta = def.meta(i);
        let mut s = format!("  `{}` {}", c.name, c.ty.display_name());
        if !c.nullable {
            s.push_str(" NOT NULL");
        }
        if let Some(d) = &meta.default {
            s.push_str(&format!(" DEFAULT {d}"));
        }
        if meta.auto_increment {
            s.push_str(" AUTO_INCREMENT");
        }
        if let Some(g) = &meta.generated {
            s.push_str(&format!(" GENERATED ALWAYS AS ({g}) STORED"));
        }
        lines.push(s);
    }
    if !def.pk_cols.is_empty() {
        let cols = def
            .pk_cols
            .iter()
            .map(|&i| format!("`{}`", def.schema.columns[i].name))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("  PRIMARY KEY ({cols})"));
    }
    for idx in &def.indexes {
        let kind = if idx.vector {
            "VECTOR KEY"
        } else if idx.unique {
            "UNIQUE KEY"
        } else {
            "KEY"
        };
        let cols = idx
            .cols
            .iter()
            .map(|&i| format!("`{}`", def.schema.columns[i].name))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("  {kind} `{}` ({cols})", idx.name));
    }
    let ddl = format!("CREATE TABLE `{name}` (\n{}\n)", lines.join(",\n"));
    let schema = Schema::new(vec![
        ColumnDef {
            name: "Table".into(),
            ty: ColumnType::Text,
            nullable: false,
            collation: elyra_core::Collation::Ci,
        },
        ColumnDef {
            name: "Create Table".into(),
            ty: ColumnType::Text,
            nullable: false,
            collation: elyra_core::Collation::Ci,
        },
    ]);
    let rows = vec![vec![Value::Text(name.to_string()), Value::Text(ddl)]];
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// SHOW INDEX FROM table: one row per index column.
pub async fn show_index(db: &Session, name: &str) -> Result<QueryResult> {
    let def = catalog::load(db, name).await?;
    let head = [
        "Table",
        "Non_unique",
        "Key_name",
        "Seq_in_index",
        "Column_name",
        "Collation",
        "Cardinality",
        "Null",
        "Index_type",
    ];
    let schema = Schema::new(
        head.iter()
            .map(|n| ColumnDef {
                name: (*n).to_string(),
                ty: if matches!(*n, "Non_unique" | "Seq_in_index" | "Cardinality") {
                    ColumnType::Int
                } else {
                    ColumnType::Text
                },
                nullable: true,
                collation: elyra_core::Collation::Ci,
            })
            .collect(),
    );
    let mk = |non_unique: i64, key: &str, seq: usize, ci: usize, itype: &str| -> Vec<Value> {
        let c = &def.schema.columns[ci];
        vec![
            Value::Text(name.to_string()),
            Value::Int(non_unique),
            Value::Text(key.to_string()),
            Value::Int(seq as i64),
            Value::Text(c.name.clone()),
            Value::Text("A".into()),
            Value::Null,
            Value::Text(if c.nullable { "YES" } else { "" }.into()),
            Value::Text(itype.to_string()),
        ]
    };
    let mut rows = Vec::new();
    for (seq, &ci) in def.pk_cols.iter().enumerate() {
        rows.push(mk(0, "PRIMARY", seq + 1, ci, "BTREE"));
    }
    for idx in &def.indexes {
        let non_unique = if idx.unique { 0 } else { 1 };
        let itype = if idx.vector { "HNSW" } else { "BTREE" };
        for (seq, &ci) in idx.cols.iter().enumerate() {
            rows.push(mk(non_unique, &idx.name, seq + 1, ci, itype));
        }
    }
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// If `tf` is `information_schema.<view>`, return the lowercase view name.
fn information_schema_view(tf: &TableFactor) -> Option<String> {
    if let TableFactor::Table { name, .. } = tf {
        if name.0.len() >= 2
            && name.0[name.0.len() - 2]
                .value
                .eq_ignore_ascii_case("information_schema")
        {
            return name.0.last().map(|i| i.value.to_ascii_lowercase());
        }
    }
    None
}

/// The `Key` letter (PRI/UNI/MUL/empty) for column `i` of a table.
fn column_key(def: &TableDef, i: usize) -> &'static str {
    if def.pk_cols.contains(&i) {
        "PRI"
    } else if def.indexes.iter().any(|idx| idx.unique && idx.cols == [i]) {
        "UNI"
    } else if def.indexes.iter().any(|idx| idx.cols.first() == Some(&i)) {
        "MUL"
    } else {
        ""
    }
}

fn column_extra(meta: &ColMeta) -> &'static str {
    if meta.auto_increment {
        "auto_increment"
    } else if meta.generated.is_some() {
        "STORED GENERATED"
    } else {
        ""
    }
}

/// Build the rows of an `information_schema` view (`tables` or `columns`).
async fn information_schema(db: &Session, view: &str) -> Result<(Schema, Vec<Vec<Value>>)> {
    let text = |n: &str| ColumnDef {
        name: n.to_string(),
        ty: ColumnType::Text,
        nullable: true,
        collation: elyra_core::Collation::Ci,
    };
    let int = |n: &str| ColumnDef {
        name: n.to_string(),
        ty: ColumnType::Int,
        nullable: true,
        collation: elyra_core::Collation::Ci,
    };
    let names = catalog::list_tables(db).await?;
    match view {
        "tables" => {
            let schema = Schema::new(vec![
                text("TABLE_SCHEMA"),
                text("TABLE_NAME"),
                text("TABLE_TYPE"),
                text("ENGINE"),
                int("TABLE_ROWS"),
            ]);
            let mut rows = Vec::with_capacity(names.len());
            for n in names {
                let table_rows = match catalog::load_stats(db, &n).await? {
                    Some(s) => Value::Int(s.rows as i64),
                    None => Value::Null,
                };
                rows.push(vec![
                    Value::Text("elyra".into()),
                    Value::Text(n),
                    Value::Text("BASE TABLE".into()),
                    Value::Text("ElyraSQL".into()),
                    table_rows,
                ]);
            }
            Ok((schema, rows))
        }
        "columns" => {
            let schema = Schema::new(vec![
                text("TABLE_SCHEMA"),
                text("TABLE_NAME"),
                text("COLUMN_NAME"),
                int("ORDINAL_POSITION"),
                text("COLUMN_DEFAULT"),
                text("IS_NULLABLE"),
                text("DATA_TYPE"),
                text("COLUMN_TYPE"),
                text("COLUMN_KEY"),
                text("EXTRA"),
            ]);
            let mut rows = Vec::new();
            for tname in names {
                let def = catalog::load(db, &tname).await?;
                for (i, c) in def.schema.columns.iter().enumerate() {
                    let meta = def.meta(i);
                    let ty = c.ty.display_name();
                    rows.push(vec![
                        Value::Text("elyra".into()),
                        Value::Text(tname.clone()),
                        Value::Text(c.name.clone()),
                        Value::Int(i as i64 + 1),
                        match &meta.default {
                            Some(d) => Value::Text(d.clone()),
                            None => Value::Null,
                        },
                        Value::Text(if c.nullable { "YES" } else { "NO" }.into()),
                        Value::Text(ty.clone()),
                        Value::Text(ty),
                        Value::Text(column_key(&def, i).into()),
                        Value::Text(column_extra(&meta).into()),
                    ]);
                }
            }
            Ok((schema, rows))
        }
        "statistics" => {
            let schema = Schema::new(vec![
                text("TABLE_SCHEMA"),
                text("TABLE_NAME"),
                int("NON_UNIQUE"),
                text("INDEX_NAME"),
                int("SEQ_IN_INDEX"),
                text("COLUMN_NAME"),
                text("COLLATION"),
                int("CARDINALITY"),
                text("NULLABLE"),
                text("INDEX_TYPE"),
            ]);
            let mut rows = Vec::new();
            for tname in names {
                let def = catalog::load(db, &tname).await?;
                let mut push =
                    |non_unique: i64, iname: &str, seq: usize, ci: usize, itype: &str| {
                        let c = &def.schema.columns[ci];
                        rows.push(vec![
                            Value::Text("elyra".into()),
                            Value::Text(tname.clone()),
                            Value::Int(non_unique),
                            Value::Text(iname.to_string()),
                            Value::Int(seq as i64 + 1),
                            Value::Text(c.name.clone()),
                            Value::Text("A".into()),
                            Value::Null,
                            Value::Text(if c.nullable { "YES" } else { "" }.into()),
                            Value::Text(itype.to_string()),
                        ]);
                    };
                for (seq, &ci) in def.pk_cols.iter().enumerate() {
                    push(0, "PRIMARY", seq, ci, "BTREE");
                }
                for idx in &def.indexes {
                    let nu = if idx.unique { 0 } else { 1 };
                    let itype = if idx.vector { "HNSW" } else { "BTREE" };
                    let iname = idx.name.clone();
                    for (seq, &ci) in idx.cols.iter().enumerate() {
                        push(nu, &iname, seq, ci, itype);
                    }
                }
            }
            Ok((schema, rows))
        }
        "key_column_usage" => {
            let schema = Schema::new(vec![
                text("CONSTRAINT_SCHEMA"),
                text("CONSTRAINT_NAME"),
                text("TABLE_SCHEMA"),
                text("TABLE_NAME"),
                text("COLUMN_NAME"),
                int("ORDINAL_POSITION"),
            ]);
            let mut rows = Vec::new();
            for tname in names {
                let def = catalog::load(db, &tname).await?;
                let mut push = |cname: &str, seq: usize, ci: usize| {
                    rows.push(vec![
                        Value::Text("elyra".into()),
                        Value::Text(cname.to_string()),
                        Value::Text("elyra".into()),
                        Value::Text(tname.clone()),
                        Value::Text(def.schema.columns[ci].name.clone()),
                        Value::Int(seq as i64 + 1),
                    ]);
                };
                for (seq, &ci) in def.pk_cols.iter().enumerate() {
                    push("PRIMARY", seq, ci);
                }
                for idx in def.indexes.iter().filter(|i| i.unique) {
                    for (seq, &ci) in idx.cols.iter().enumerate() {
                        push(&idx.name, seq, ci);
                    }
                }
            }
            Ok((schema, rows))
        }
        "column_statistics" => {
            let schema = Schema::new(vec![
                text("TABLE_NAME"),
                text("COLUMN_NAME"),
                int("NDV"),
                int("NULLS"),
                text("MIN_VALUE"),
                text("MAX_VALUE"),
            ]);
            let mut rows = Vec::new();
            for tname in names {
                let Some(stats) = catalog::load_stats(db, &tname).await? else {
                    continue;
                };
                for c in &stats.columns {
                    rows.push(vec![
                        Value::Text(tname.clone()),
                        Value::Text(c.name.clone()),
                        Value::Int(c.ndv as i64),
                        Value::Int(c.nulls as i64),
                        c.min.clone().map(Value::Text).unwrap_or(Value::Null),
                        c.max.clone().map(Value::Text).unwrap_or(Value::Null),
                    ]);
                }
            }
            Ok((schema, rows))
        }
        other => Err(Error::Unsupported(format!(
            "information_schema.{other} is not available"
        ))),
    }
}

/// Filter / aggregate / project / order a pre-materialised relation (used by
/// information_schema virtual tables).
#[allow(clippy::too_many_arguments)]
async fn run_virtual_select(
    db: &Session,
    vindex: &VectorRegistry,
    select: &Select,
    schema: Schema,
    mut rows: Vec<Vec<Value>>,
    group_by: &[Expr],
    order_exprs: &[(Expr, bool)],
    offset: usize,
    limit: Option<usize>,
) -> Result<QueryResult> {
    if let Some(f) = &select.selection {
        let rf = resolve_subqueries(db, vindex, f.clone()).await?;
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            if predicate::matches(&rf, &schema, &r)? {
                kept.push(r);
            }
        }
        rows = kept;
    }

    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        let (osch, orows) = aggregate::run(&schema, &select.projection, group_by, rows)?;
        let mut orows = apply_having(select.having.as_ref(), &select.projection, &osch, orows)?;
        order_output_rows(&mut orows, &osch, order_exprs)?;
        apply_offset_limit(&mut orows, offset, limit);
        return Ok(QueryResult::Rows(RowStream::literal(osch, orows)));
    }

    let resolved = resolve_order_aliases(order_exprs, &select.projection, &schema);
    if !resolved.is_empty() {
        sort_full_rows(&mut rows, &schema, &resolved)?;
    }
    apply_offset_limit(&mut rows, offset, limit);
    let (osch, out) = project_exprs(&select.projection, &schema, &rows)?;
    Ok(QueryResult::Rows(RowStream::literal(osch, out)))
}

/// CREATE TABLE ... AS SELECT: build a rowid table from the query's output
/// schema (or an explicit column list) and copy the result rows.
async fn create_table_as(
    db: &Session,
    vindex: &VectorRegistry,
    name: &str,
    ct: &CreateTable,
    q: &SqlQuery,
) -> Result<QueryResult> {
    let (qschema, rows) = run_subquery_schema(db, vindex, q).await?;
    let columns: Vec<ColumnDef> = if ct.columns.is_empty() {
        qschema
            .columns
            .iter()
            .map(|c| ColumnDef {
                name: c.name.rsplit('.').next().unwrap_or(&c.name).to_string(),
                ty: c.ty.clone(),
                nullable: true,
                collation: elyra_core::Collation::Ci,
            })
            .collect()
    } else {
        let mut v = Vec::with_capacity(ct.columns.len());
        for c in &ct.columns {
            v.push(ColumnDef {
                name: c.name.value.clone(),
                ty: map_type(&c.data_type)?,
                nullable: true,
                collation: elyra_core::Collation::Ci,
            });
        }
        v
    };
    if columns.len() != qschema.columns.len() {
        return Err(Error::Query(
            "CREATE TABLE AS: column count does not match the query".into(),
        ));
    }

    let def = TableDef {
        name: name.to_string(),
        schema: Schema::new(columns),
        pk_cols: Vec::new(),
        indexes: Vec::new(),
        col_meta: Vec::new(),
        checks: Vec::new(),
        foreign_keys: Vec::new(),
    };
    let mut puts = vec![(catalog_key(name), def.encode()?)];
    let mut rowid = 0u64;
    for row in &rows {
        rowid += 1;
        let mut r = vec![Value::Null; def.schema.columns.len()];
        for (i, col) in def.schema.columns.iter().enumerate() {
            if let Some(v) = row.get(i) {
                r[i] = coerce(v.clone(), &col.ty, &col.name)?;
            }
        }
        let enc = bincode::serialize(&r).map_err(|e| Error::Storage(e.to_string()))?;
        puts.push((data_key(name, &keyenc::encode_rowid(rowid)), enc));
    }
    if rowid > 0 {
        puts.push((rowid_key(name), rowid.to_le_bytes().to_vec()));
    }
    let affected = rows.len() as u64;
    db.commit_write(puts, vec![]).await?;
    Ok(QueryResult::Affected(affected))
}

/// TRUNCATE TABLE: remove all rows and index entries, reset counters.
pub async fn truncate(db: &Session, name: &str) -> Result<QueryResult> {
    if !catalog::exists(db, name).await? {
        return Err(Error::Catalog(format!("no such table: {name}")));
    }
    let mut deletes = vec![rowid_key(name), autoinc_key(name)];
    for prefix in [data_prefix(name), index_table_prefix(name)] {
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
    }
    let wc = bump_wcount(db, name).await?;
    db.commit_write(vec![wc], deletes).await?;
    Ok(QueryResult::Affected(0))
}

pub async fn alter_table(
    db: &Session,
    name: &ObjectName,
    ops: &[AlterTableOperation],
) -> Result<QueryResult> {
    let tname = table_ident(name)?;
    let mut def = catalog::load(db, &tname).await?;
    let mut persist_catalog = true;

    for op in ops {
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                alter_add_column(db, &mut def, column_def).await?
            }
            AlterTableOperation::DropColumn { column_name, .. } => {
                alter_drop_column(db, &mut def, &column_name.value).await?
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let i = def
                    .schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&old_column_name.value))
                    .ok_or_else(|| Error::Catalog(format!("unknown column: {old_column_name}")))?;
                def.schema.columns[i].name = new_column_name.value.clone();
            }
            AlterTableOperation::RenameTable { table_name } => {
                let new = table_ident(table_name)?;
                alter_rename_table(db, &mut def, &new).await?;
                persist_catalog = false;
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                ..
            } => {
                alter_change_column(
                    db,
                    &mut def,
                    &old_name.value,
                    Some(&new_name.value),
                    data_type,
                    options,
                )
                .await?;
            }
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                ..
            } => {
                alter_change_column(db, &mut def, &col_name.value, None, data_type, options)
                    .await?;
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                alter_column_op(db, &mut def, &column_name.value, op).await?;
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "ALTER operation not supported: {other}"
                )))
            }
        }
    }

    if persist_catalog {
        db.commit_write(vec![(catalog_key(&def.name), def.encode()?)], vec![])
            .await?;
    }
    Ok(QueryResult::Affected(0))
}

fn ensure_col_meta(def: &mut TableDef) {
    if def.col_meta.len() < def.schema.columns.len() {
        def.col_meta
            .resize(def.schema.columns.len(), ColMeta::default());
    }
}

/// Build a column's nullability and metadata from its options.
fn options_to_meta(options: &[ColumnOption]) -> (bool, ColMeta) {
    let mut nullable = true;
    let mut meta = ColMeta::default();
    for opt in options {
        match opt {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Unique {
                is_primary: true, ..
            } => nullable = false,
            ColumnOption::Default(e) => meta.default = Some(e.to_string()),
            ColumnOption::Generated {
                generation_expr: Some(e),
                ..
            } => meta.generated = Some(e.to_string()),
            ColumnOption::DialectSpecific(tokens)
                if tokens
                    .iter()
                    .any(|t| t.to_string().eq_ignore_ascii_case("AUTO_INCREMENT")) =>
            {
                meta.auto_increment = true;
            }
            _ => {}
        }
    }
    (nullable, meta)
}

/// `MODIFY COLUMN` / `CHANGE COLUMN`: retype, rename, and reset options.
async fn alter_change_column(
    db: &Session,
    def: &mut TableDef,
    old: &str,
    new_name: Option<&str>,
    data_type: &DataType,
    options: &[ColumnOption],
) -> Result<()> {
    ensure_col_meta(def);
    let i = def
        .schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(old))
        .ok_or_else(|| Error::Catalog(format!("unknown column: {old}")))?;

    let new_ty = map_type(data_type)?;
    let old_ty = def.schema.columns[i].ty.clone();
    if def.pk_cols.contains(&i) && new_ty != old_ty {
        return Err(Error::Unsupported(
            "cannot change the type of a primary key column".into(),
        ));
    }
    if let Some(nn) = new_name {
        def.schema.columns[i].name = nn.to_string();
    }
    let (nullable, meta) = options_to_meta(options);
    def.schema.columns[i].nullable = nullable;
    def.schema.columns[i].ty = new_ty.clone();
    def.col_meta[i] = meta;
    if new_ty != old_ty {
        recoerce_column(db, def, i).await?;
    }
    Ok(())
}

/// `ALTER COLUMN ... SET/DROP DEFAULT | SET/DROP NOT NULL | SET DATA TYPE`.
async fn alter_column_op(
    db: &Session,
    def: &mut TableDef,
    name: &str,
    op: &AlterColumnOperation,
) -> Result<()> {
    ensure_col_meta(def);
    let i = def
        .schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Catalog(format!("unknown column: {name}")))?;
    match op {
        AlterColumnOperation::SetDefault { value } => {
            def.col_meta[i].default = Some(value.to_string())
        }
        AlterColumnOperation::DropDefault => def.col_meta[i].default = None,
        AlterColumnOperation::SetNotNull => def.schema.columns[i].nullable = false,
        AlterColumnOperation::DropNotNull => def.schema.columns[i].nullable = true,
        AlterColumnOperation::SetDataType { data_type, .. } => {
            let new_ty = map_type(data_type)?;
            let old_ty = def.schema.columns[i].ty.clone();
            if def.pk_cols.contains(&i) && new_ty != old_ty {
                return Err(Error::Unsupported(
                    "cannot change the type of a primary key column".into(),
                ));
            }
            def.schema.columns[i].ty = new_ty.clone();
            if new_ty != old_ty {
                recoerce_column(db, def, i).await?;
            }
        }
        other => {
            return Err(Error::Unsupported(format!(
                "ALTER COLUMN operation not supported: {other}"
            )))
        }
    }
    Ok(())
}

/// Re-coerce column `i` of every row to its (new) type, maintaining indexes.
async fn recoerce_column(db: &Session, def: &TableDef, i: usize) -> Result<()> {
    let all = collect_matches(db, def, None, None).await?;
    let col = &def.schema.columns[i];
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();
    for (key, old_row) in all {
        let coerced = coerce(old_row[i].clone(), &col.ty, &col.name)?;
        if coerced == old_row[i] {
            continue;
        }
        let mut new_row = old_row.clone();
        new_row[i] = coerced;
        deletes.extend(index::entry_keys_for_row(def, &old_row, &key)?);
        let entries = index::entries_for_row(def, &new_row, &key)?;
        let enc = bincode::serialize(&new_row).map_err(|e| Error::Storage(e.to_string()))?;
        puts.push((key, enc));
        puts.extend(entries);
    }
    if !puts.is_empty() {
        puts.push(bump_wcount(db, &def.name).await?);
        db.commit_write(puts, deletes).await?;
    }
    Ok(())
}

async fn alter_add_column(
    db: &Session,
    def: &mut TableDef,
    col: &sqlparser::ast::ColumnDef,
) -> Result<()> {
    let ty = map_type(&col.data_type)?;
    let mut nullable = true;
    let mut default = Value::Null;
    for opt in &col.options {
        match &opt.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Default(e) => default = coerce(eval_expr(e)?, &ty, &col.name.value)?,
            _ => {}
        }
    }
    if !nullable && default.is_null() {
        return Err(Error::Query(format!(
            "ADD COLUMN '{}' is NOT NULL and needs a DEFAULT",
            col.name.value
        )));
    }

    // Append the default to every existing row.
    let prefix = data_prefix(&def.name);
    let mut cursor: Option<Vec<u8>> = None;
    let mut puts = Vec::new();
    loop {
        let chunk = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if chunk.is_empty() {
            break;
        }
        let last = chunk.len() < 4096;
        cursor = chunk.last().map(|(k, _)| k.clone());
        for (k, v) in chunk {
            let mut row: Vec<Value> =
                bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;
            row.push(default.clone());
            puts.push((
                k,
                bincode::serialize(&row).map_err(|e| Error::Storage(e.to_string()))?,
            ));
        }
        if last {
            break;
        }
    }
    def.schema.columns.push(ColumnDef {
        name: col.name.value.clone(),
        ty,
        nullable,
        collation: col
            .collation
            .as_ref()
            .map(|c| elyra_core::Collation::from_name(&c.to_string()))
            .unwrap_or_default(),
    });
    if !puts.is_empty() {
        db.commit_write(puts, vec![]).await?;
    }
    Ok(())
}

async fn alter_drop_column(db: &Session, def: &mut TableDef, name: &str) -> Result<()> {
    let idx = def
        .schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Catalog(format!("unknown column: {name}")))?;
    if def.pk_cols.contains(&idx) {
        return Err(Error::Unsupported(
            "cannot drop a primary key column".into(),
        ));
    }
    if def.indexes.iter().any(|i| i.cols.contains(&idx)) {
        return Err(Error::Unsupported(
            "cannot drop an indexed column; drop the index first".into(),
        ));
    }
    if idx < def.col_meta.len() {
        def.col_meta.remove(idx);
    }

    // Rewrite rows without the dropped position.
    let prefix = data_prefix(&def.name);
    let mut cursor: Option<Vec<u8>> = None;
    let mut puts = Vec::new();
    loop {
        let chunk = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if chunk.is_empty() {
            break;
        }
        let last = chunk.len() < 4096;
        cursor = chunk.last().map(|(k, _)| k.clone());
        for (k, v) in chunk {
            let mut row: Vec<Value> =
                bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;
            if idx < row.len() {
                row.remove(idx);
            }
            puts.push((
                k,
                bincode::serialize(&row).map_err(|e| Error::Storage(e.to_string()))?,
            ));
        }
        if last {
            break;
        }
    }
    def.schema.columns.remove(idx);
    // Shift key/index column positions above the removed one.
    let shift = |c: &mut usize| {
        if *c > idx {
            *c -= 1;
        }
    };
    def.pk_cols.iter_mut().for_each(shift);
    for i in &mut def.indexes {
        i.cols.iter_mut().for_each(shift);
    }
    if !puts.is_empty() {
        db.commit_write(puts, vec![]).await?;
    }
    Ok(())
}

async fn alter_rename_table(db: &Session, def: &mut TableDef, new: &str) -> Result<()> {
    if catalog::exists(db, new).await? {
        return Err(Error::Catalog(format!("table already exists: {new}")));
    }
    let old = def.name.clone();
    def.name = new.to_string();

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();

    // Re-key all data rows and rebuild their index entries under the new name.
    let old_prefix = data_prefix(&old);
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let chunk = db
            .scan_batch(old_prefix.clone(), cursor.clone(), 4096)
            .await?;
        if chunk.is_empty() {
            break;
        }
        let last = chunk.len() < 4096;
        cursor = chunk.last().map(|(k, _)| k.clone());
        for (old_key, v) in chunk {
            let clustered = &old_key[old_prefix.len()..];
            let new_key = data_key(new, clustered);
            let row: Vec<Value> =
                bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;
            deletes.push(old_key);
            puts.push((new_key.clone(), v));
            puts.extend(index::entries_for_row(def, &row, &new_key)?);
        }
        if last {
            break;
        }
    }

    // Delete all old index entries (keyed under the old table name).
    let old_index_prefix = format!("index::{old}::").into_bytes();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let chunk = db
            .scan_batch(old_index_prefix.clone(), cursor.clone(), 4096)
            .await?;
        if chunk.is_empty() {
            break;
        }
        let last = chunk.len() < 4096;
        cursor = chunk.last().map(|(k, _)| k.clone());
        for (k, _) in chunk {
            deletes.push(k);
        }
        if last {
            break;
        }
    }

    // Move catalog + meta counters.
    deletes.push(catalog_key(&old));
    puts.push((catalog_key(new), def.encode()?));
    if let Some(rc) = db.get(rowid_key(&old)).await? {
        deletes.push(rowid_key(&old));
        puts.push((rowid_key(new), rc));
    }
    if let Some(wc) = db.get(wcount_key(&old)).await? {
        deletes.push(wcount_key(&old));
        puts.push((wcount_key(new), wc));
    }
    db.commit_write(puts, deletes).await?;
    Ok(())
}

/// `CREATE FULLTEXT INDEX name ON table(col, ...)` — builds an inverted,
/// tokenized index (maintained thereafter via the normal index machinery).
pub async fn create_fulltext_index(
    db: &Session,
    name: &str,
    table: &str,
    cols: &[String],
) -> Result<QueryResult> {
    let mut def = catalog::load(db, table).await?;
    if def
        .indexes
        .iter()
        .any(|i| i.name.eq_ignore_ascii_case(name))
    {
        return Err(Error::Catalog(format!("index already exists: {name}")));
    }
    let col_idx: Vec<usize> = cols
        .iter()
        .map(|c| {
            def.schema
                .columns
                .iter()
                .position(|d| d.name.eq_ignore_ascii_case(c))
                .ok_or_else(|| Error::Catalog(format!("unknown column: {c}")))
        })
        .collect::<Result<_>>()?;
    def.indexes.push(IndexDef {
        name: name.to_string(),
        cols: col_idx,
        unique: false,
        vector: false,
        fulltext: true,
        col_collations: Vec::new(),
    });
    let idx = def.indexes.last().unwrap().clone();

    // Persist the catalog and backfill index entries for existing rows.
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = vec![(catalog_key(table), def.encode()?)];
    let prefix = data_prefix(table);
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
            for (ek, ev) in index::entries_for_row(
                &TableDef {
                    indexes: vec![idx.clone()],
                    ..def.clone()
                },
                &row,
                &k,
            )? {
                puts.push((ek, ev));
            }
        }
        if last {
            break;
        }
    }
    db.commit_write(puts, vec![]).await?;
    Ok(QueryResult::Affected(0))
}

pub async fn create_index(db: &Session, ci: CreateIndex) -> Result<QueryResult> {
    let table = table_ident(&ci.table_name)?;
    let mut def = catalog::load(db, &table).await?;

    if ci.columns.is_empty() {
        return Err(Error::Query(
            "CREATE INDEX requires at least one column".into(),
        ));
    }
    let mut cols = Vec::with_capacity(ci.columns.len());
    let mut col_names = Vec::new();
    for oc in &ci.columns {
        let col_name = ident_name(&oc.expr)
            .ok_or_else(|| Error::Unsupported("index column must be a plain column".into()))?;
        let col = def
            .schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col_name))
            .ok_or_else(|| Error::Catalog(format!("unknown column: {col_name}")))?;
        cols.push(col);
        col_names.push(col_name.to_string());
    }

    let name = match &ci.name {
        Some(n) => n.0.last().map(|i| i.value.clone()).unwrap_or_default(),
        None => format!("{table}_{}_idx", col_names.join("_")),
    };
    if def
        .indexes
        .iter()
        .any(|i| i.name.eq_ignore_ascii_case(&name))
    {
        if ci.if_not_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Catalog(format!("index already exists: {name}")));
    }

    // A vector (HNSW) index is a single VECTOR column; composite must be B-tree.
    let is_vector =
        cols.len() == 1 && matches!(def.schema.columns[cols[0]].ty, ColumnType::Vector(_));
    let col_collations: Vec<elyra_core::Collation> =
        cols.iter().map(|&c| def.collation_of(c)).collect();
    def.indexes.push(IndexDef {
        name,
        cols,
        unique: ci.unique,
        vector: is_vector,
        fulltext: false,
        col_collations,
    });

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
    db.commit_write(puts, vec![]).await?;
    Ok(QueryResult::Affected(0))
}

pub async fn insert(db: &Session, vindex: &VectorRegistry, ins: Insert) -> Result<QueryResult> {
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
    // Rows come either from `VALUES (...)` (literal expressions, evaluated
    // here) or from `INSERT ... SELECT` (executed through the query engine).
    let rows: Vec<Vec<Value>> = match source_rows(source)? {
        Some(expr_rows) => {
            let mut out = Vec::with_capacity(expr_rows.len());
            for exprs in expr_rows {
                out.push(exprs.iter().map(eval_expr).collect::<Result<Vec<_>>>()?);
            }
            out
        }
        None => run_subquery(db, vindex, source).await?,
    };

    // Upsert mode: REPLACE INTO, INSERT IGNORE, ON DUPLICATE KEY UPDATE.
    let replace = ins.replace_into;
    let ignore = ins.ignore;
    let dup_sets: Vec<(usize, Expr)> = match &ins.on {
        Some(sqlparser::ast::OnInsert::DuplicateKeyUpdate(assigns)) => {
            let mut v = Vec::with_capacity(assigns.len());
            for a in assigns {
                let col = match &a.target {
                    AssignmentTarget::ColumnName(n) => {
                        n.0.last()
                            .map(|i| i.value.clone())
                            .ok_or_else(|| Error::Query("empty assignment target".into()))?
                    }
                    AssignmentTarget::Tuple(_) => {
                        return Err(Error::Unsupported(
                            "tuple assignment is not supported".into(),
                        ))
                    }
                };
                let idx = def
                    .schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&col))
                    .ok_or_else(|| Error::Catalog(format!("unknown column: {col}")))?;
                v.push((idx, a.value.clone()));
            }
            v
        }
        Some(other) => {
            return Err(Error::Unsupported(format!(
                "unsupported ON clause: {other:?}"
            )))
        }
        None => Vec::new(),
    };
    let on_dup = !dup_sets.is_empty();
    let has_pk = def.has_pk();
    let pk_colls = def.pk_collations();

    // Load rowid counter once for tables without a PK.
    let mut next_rowid = if has_pk {
        0
    } else {
        read_rowid(db, &name).await?
    };

    // Column defaults, AUTO_INCREMENT, and (stored) generated columns.
    let ncols = def.schema.columns.len();
    let has_meta = def.has_col_meta();
    let mut provided = vec![false; ncols];
    for &s in &target {
        provided[s] = true;
    }
    let mut default_exprs: Vec<Option<Expr>> = vec![None; ncols];
    let mut generated_exprs: Vec<Option<Expr>> = vec![None; ncols];
    let mut auto_col: Option<usize> = None;
    if has_meta {
        for i in 0..ncols {
            let m = def.meta(i);
            if let Some(d) = &m.default {
                default_exprs[i] = Some(parse_scalar_expr(d)?);
            }
            if let Some(g) = &m.generated {
                generated_exprs[i] = Some(parse_scalar_expr(g)?);
            }
            if m.auto_increment {
                auto_col = Some(i);
            }
        }
    }
    let mut autoinc: i64 = if auto_col.is_some() {
        read_autoinc(db, &name).await?
    } else {
        0
    };

    let mut deletes: Vec<Vec<u8>> = Vec::new();
    let mut affected: u64 = 0;
    // PK rows coalesce by clustered key so within-statement duplicates merge;
    // rowid rows are always fresh inserts.
    let mut batch: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    let mut pos_of: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();

    let apply_dup = |old: &[Value], insert: &[Value]| -> Result<Vec<Value>> {
        let mut merged = old.to_vec();
        for (idx, expr) in &dup_sets {
            let bound = bind_values(expr, insert, &def.schema);
            let v = predicate::eval_row(&bound, &def.schema, &merged)?;
            let col = &def.schema.columns[*idx];
            merged[*idx] = coerce(v, &col.ty, &col.name)?;
        }
        Ok(merged)
    };

    // Pass 1: build every row (coerce, defaults, AUTO_INCREMENT, generated,
    // NOT NULL) and its clustered key — no per-row storage reads.
    let mut built: Vec<(Vec<u8>, Vec<Value>)> = Vec::with_capacity(rows.len());
    let checks = parse_checks(&def)?;
    let trigs = catalog::load_triggers(db, &name).await?;
    let before_ins: Vec<catalog::TriggerDef> = trigs
        .iter()
        .filter(|t| t.before && t.event == catalog::TrigEvent::Insert)
        .cloned()
        .collect();
    let after_ins: Vec<catalog::TriggerDef> = trigs
        .iter()
        .filter(|t| !t.before && t.event == catalog::TrigEvent::Insert)
        .cloned()
        .collect();
    for vals in rows {
        if vals.len() != target.len() {
            return Err(Error::Query(format!(
                "column count mismatch: {} values for {} columns",
                vals.len(),
                target.len()
            )));
        }

        let mut row = vec![Value::Null; def.schema.columns.len()];
        for (slot, v) in target.iter().zip(vals) {
            let col = &def.schema.columns[*slot];
            row[*slot] = coerce(v, &col.ty, &col.name)?;
        }

        if has_meta {
            for i in 0..ncols {
                if !provided[i] && generated_exprs[i].is_none() {
                    if let Some(de) = &default_exprs[i] {
                        let col = &def.schema.columns[i];
                        row[i] = coerce(eval_expr(de)?, &col.ty, &col.name)?;
                    }
                }
            }
            if let Some(ai) = auto_col {
                let need = !provided[ai] || row[ai].is_null() || matches!(row[ai], Value::Int(0));
                if need {
                    autoinc += 1;
                    row[ai] = Value::Int(autoinc);
                } else if let Value::Int(n) = row[ai] {
                    if n > autoinc {
                        autoinc = n;
                    }
                }
            }
            for i in 0..ncols {
                if let Some(ge) = &generated_exprs[i] {
                    let col = &def.schema.columns[i];
                    row[i] = coerce(
                        predicate::eval_row(ge, &def.schema, &row)?,
                        &col.ty,
                        &col.name,
                    )?;
                }
            }
        }

        for t in &before_ins {
            apply_before_trigger(t, &def.schema, &mut row, None)?;
        }

        for (i, col) in def.schema.columns.iter().enumerate() {
            if !col.nullable && row[i].is_null() {
                return Err(Error::Query(format!(
                    "column '{}' cannot be NULL",
                    col.name
                )));
            }
        }
        check_row(&def, &checks, &row)?;

        let key = if has_pk {
            let pk_vals: Vec<Value> = def.pk_cols.iter().map(|&i| row[i].clone()).collect();
            data_key(&name, &keyenc::encode_key_coll(&pk_vals, &pk_colls)?)
        } else {
            next_rowid += 1;
            data_key(&name, &keyenc::encode_rowid(next_rowid))
        };
        built.push((key, row));
    }

    // Fast path: a plain INSERT (no IGNORE/REPLACE/ON DUPLICATE) into a PK
    // table outside a transaction detects duplicates inside the write
    // transaction itself (redb returns the previous value), avoiding any
    // existence read. This is the bulk-load hot path.
    if !replace && !on_dup && !ignore && has_pk && !db.in_txn() {
        if !def.foreign_keys.is_empty() {
            check_fk_batch(db, &def, &built).await?;
        }
        let mut new_puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(built.len());
        let mut aux_puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (key, row) in &built {
            let enc = bincode::serialize(row).map_err(|e| Error::Storage(e.to_string()))?;
            // Non-unique index entries may coexist (aux); the data key and any
            // unique index entries must be new, so a duplicate PK or unique
            // value is caught inside the write transaction.
            let (nonuniq, uniq) = index::partition_entries_for_row(&def, row, key)?;
            aux_puts.extend(nonuniq);
            new_puts.push((key.clone(), enc));
            new_puts.extend(uniq);
        }
        aux_puts.push(bump_wcount(db, &name).await?);
        // Persist the advanced AUTO_INCREMENT counter (otherwise a later insert
        // would reuse ids).
        if auto_col.is_some() {
            aux_puts.push((autoinc_key(&name), autoinc.to_le_bytes().to_vec()));
        }
        let affected = built.len() as u64;
        db.raw_db()
            .commit_insert(new_puts, aux_puts, Vec::new())
            .await?;
        if !after_ins.is_empty() {
            for (_, row) in &built {
                queue_after(db, &after_ins, &def.schema, Some(row), None)?;
            }
        }
        return Ok(QueryResult::Affected(affected));
    }

    // One batched existence read for the whole statement (PK tables) instead of
    // a read per row — the bulk-insert hot path.
    let existing: Vec<Option<Vec<u8>>> = if has_pk {
        let keys: Vec<Vec<u8>> = built.iter().map(|(k, _)| k.clone()).collect();
        db.multi_get(keys).await?
    } else {
        Vec::new()
    };

    // Pass 2: apply INSERT / upsert semantics using the batched existence info.
    for (i, (key, row)) in built.into_iter().enumerate() {
        if !has_pk {
            batch.push((key, row));
            affected += 1;
            continue;
        }

        // Coalesce with an earlier row in the same statement.
        if let Some(&pos) = pos_of.get(&key) {
            if replace {
                batch[pos].1 = row;
                affected += 1;
            } else if on_dup {
                batch[pos].1 = apply_dup(&batch[pos].1.clone(), &row)?;
                affected += 1;
            } else if !ignore {
                return Err(Error::Duplicate(format!(
                    "Duplicate entry for key 'PRIMARY' on '{name}'"
                )));
            }
            continue;
        }

        // Coalesce with an existing row in storage.
        if let Some(old_enc) = existing.get(i).and_then(|o| o.as_ref()) {
            if !replace && !on_dup {
                if ignore {
                    continue;
                }
                return Err(Error::Duplicate(format!(
                    "Duplicate entry for key 'PRIMARY' on '{name}'"
                )));
            }
            let old_row: Vec<Value> =
                bincode::deserialize(old_enc).map_err(|e| Error::Storage(e.to_string()))?;
            deletes.extend(index::entry_keys_for_row(&def, &old_row, &key)?);
            let new_row = if replace {
                row
            } else {
                apply_dup(&old_row, &row)?
            };
            pos_of.insert(key.clone(), batch.len());
            batch.push((key, new_row));
            affected += 1;
        } else {
            pos_of.insert(key.clone(), batch.len());
            batch.push((key, row));
            affected += 1;
        }
    }
    // Enforce unique secondary indexes for plain INSERT on the slow path
    // (transactions, rowid tables) where writer-side detection is not used.
    if !replace && !on_dup && !ignore && index::has_unique(&def) {
        check_unique_batch(db, &def, &batch).await?;
    }
    if !def.foreign_keys.is_empty() {
        check_fk_batch(db, &def, &batch).await?;
    }

    // Materialise the batch into data + index puts.
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(batch.len() + 2);
    for (key, row) in &batch {
        let encoded = bincode::serialize(row).map_err(|e| Error::Storage(e.to_string()))?;
        puts.push((key.clone(), encoded));
        puts.extend(index::entries_for_row(&def, row, key)?);
    }

    // Persist the advanced rowid / auto-increment counters in the same commit.
    if !def.has_pk() {
        puts.push((rowid_key(&name), next_rowid.to_le_bytes().to_vec()));
    }
    if auto_col.is_some() {
        puts.push((autoinc_key(&name), autoinc.to_le_bytes().to_vec()));
    }
    puts.push(bump_wcount(db, &name).await?);

    db.commit_write(puts, deletes).await?;
    if !after_ins.is_empty() {
        for (_, row) in &batch {
            queue_after(db, &after_ins, &def.schema, Some(row), None)?;
        }
    }
    Ok(QueryResult::Affected(affected))
}

/// Replace `VALUES(col)` references (MySQL ON DUPLICATE KEY UPDATE) with the
/// value that would have been inserted.
fn bind_values(expr: &Expr, insert_row: &[Value], schema: &Schema) -> Expr {
    map_expr(expr, &|e| {
        if let Expr::Function(f) = e {
            let is_values = f
                .name
                .0
                .last()
                .is_some_and(|i| i.value.eq_ignore_ascii_case("values"));
            if is_values {
                if let Some(col) = fn_arg_exprs(f).first().and_then(|a| ident_name(a)) {
                    if let Some(i) = schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(col))
                    {
                        return Some(value_to_expr(&insert_row[i]));
                    }
                }
            }
        }
        None
    })
}

fn map_ref_action(a: &Option<sqlparser::ast::ReferentialAction>) -> RefAction {
    use sqlparser::ast::ReferentialAction as RA;
    match a {
        Some(RA::Cascade) => RefAction::Cascade,
        Some(RA::SetNull) => RefAction::SetNull,
        Some(RA::Restrict) => RefAction::Restrict,
        _ => RefAction::NoAction,
    }
}

/// Evaluate a scalar SQL expression string (no FROM) to a value.
pub(crate) fn eval_scalar(sql: &str) -> Result<Value> {
    let e = parse_scalar_expr(sql)?;
    predicate::eval_row(&e, &Schema::new(vec![]), &[])
}

/// Replace bare identifiers that name a procedure variable with the variable's
/// SQL literal, leaving string literals and qualified names untouched.
pub(crate) fn substitute_vars(sql: &str, env: &std::collections::HashMap<String, Value>) -> String {
    if env.is_empty() {
        return sql.to_string();
    }
    let cs: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if c == '\'' {
            out.push(c);
            i += 1;
            while i < cs.len() {
                out.push(cs[i]);
                if cs[i] == '\'' {
                    if i + 1 < cs.len() && cs[i + 1] == '\'' {
                        out.push(cs[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < cs.len() && (cs[i].is_ascii_alphanumeric() || cs[i] == '_') {
                i += 1;
            }
            let word: String = cs[start..i].iter().collect();
            let prev_dot = start > 0 && cs[start - 1] == '.';
            let next_dot = i < cs.len() && cs[i] == '.';
            let lw = word.to_ascii_lowercase();
            if !prev_dot && !next_dot && env.contains_key(&lw) {
                out.push_str(&value_sql_literal(&env[&lw]));
            } else {
                out.push_str(&word);
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Replace `@user` variable references with their SQL literals (unset = NULL),
/// leaving `@@system` variables and string literals untouched.
pub(crate) fn substitute_uvars(
    sql: &str,
    vars: &std::collections::HashMap<String, Value>,
) -> String {
    let cs: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if c == '\'' {
            out.push(c);
            i += 1;
            while i < cs.len() {
                out.push(cs[i]);
                if cs[i] == '\'' {
                    if i + 1 < cs.len() && cs[i + 1] == '\'' {
                        out.push(cs[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == '@' {
            if i + 1 < cs.len() && cs[i + 1] == '@' {
                out.push('@');
                out.push('@');
                i += 2;
                continue;
            }
            let start = i + 1;
            let mut j = start;
            while j < cs.len() && (cs[j].is_ascii_alphanumeric() || cs[j] == '_') {
                j += 1;
            }
            if j > start {
                let name = cs[start..j].iter().collect::<String>().to_ascii_lowercase();
                let v = vars.get(&name).cloned().unwrap_or(Value::Null);
                out.push_str(&value_sql_literal(&v));
                i = j;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Render a value as a SQL literal (for splicing NEW/OLD into trigger bodies).
pub(crate) fn value_sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Bool(b) => {
            if *b {
                "1".into()
            } else {
                "0".into()
            }
        }
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) | Value::Json(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Bytes(b) => format!(
            "x'{}'",
            b.iter().map(|x| format!("{x:02x}")).collect::<String>()
        ),
        Value::Vector(_) => "NULL".into(),
        other => match other.to_wire_string() {
            Some(s) => format!("'{}'", s.replace('\'', "''")),
            None => "NULL".into(),
        },
    }
}

/// Strip an optional `BEGIN ... END` wrapper from a trigger body.
fn strip_begin_end(body: &str) -> String {
    let t = body.trim().trim_end_matches(';').trim();
    let low = t.to_ascii_lowercase();
    if low.starts_with("begin") && low.ends_with("end") {
        t[5..t.len() - 3].trim().to_string()
    } else {
        t.to_string()
    }
}

/// Replace `NEW.col` / `OLD.col` references with SQL literals of the row values,
/// leaving string literals untouched.
fn substitute_newold(
    body: &str,
    schema: &Schema,
    new: Option<&[Value]>,
    old: Option<&[Value]>,
) -> Result<String> {
    let lookup = |is_new: bool, col: &str| -> Result<String> {
        let row = if is_new { new } else { old };
        let row = row.ok_or_else(|| {
            Error::Query(format!(
                "trigger references {}.{} which is not available for this event",
                if is_new { "NEW" } else { "OLD" },
                col
            ))
        })?;
        let i = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(col))
            .ok_or_else(|| Error::Query(format!("trigger references unknown column: {col}")))?;
        Ok(value_sql_literal(row.get(i).unwrap_or(&Value::Null)))
    };
    let cs: Vec<char> = body.chars().collect();
    let mut out = String::with_capacity(body.len());
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if c == '\'' {
            out.push(c);
            i += 1;
            while i < cs.len() {
                out.push(cs[i]);
                if cs[i] == '\'' {
                    if i + 1 < cs.len() && cs[i + 1] == '\'' {
                        out.push(cs[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < cs.len() && (cs[i].is_ascii_alphanumeric() || cs[i] == '_') {
                i += 1;
            }
            let word: String = cs[start..i].iter().collect();
            let is_new = word.eq_ignore_ascii_case("new");
            let is_old = word.eq_ignore_ascii_case("old");
            if (is_new || is_old) && i < cs.len() && cs[i] == '.' {
                let cstart = i + 1;
                let mut j = cstart;
                while j < cs.len() && (cs[j].is_ascii_alphanumeric() || cs[j] == '_') {
                    j += 1;
                }
                let col: String = cs[cstart..j].iter().collect();
                out.push_str(&lookup(is_new, &col)?);
                i = j;
            } else {
                out.push_str(&word);
            }
            continue;
        }
        out.push(c);
        i += 1;
    }
    Ok(out)
}

/// Apply a BEFORE trigger (supports `SET NEW.col = expr` statements) to `row`.
fn apply_before_trigger(
    t: &catalog::TriggerDef,
    schema: &Schema,
    row: &mut [Value],
    old: Option<&[Value]>,
) -> Result<()> {
    let empty = Schema::new(vec![]);
    for stmt in strip_begin_end(&t.body).split(';') {
        let s = stmt.trim();
        if s.is_empty() {
            continue;
        }
        let low = s.to_ascii_lowercase();
        if !low.starts_with("set ") {
            return Err(Error::Unsupported(
                "BEFORE triggers support only SET NEW.col = expr".into(),
            ));
        }
        let rest = s[4..].trim();
        let eq = rest
            .find('=')
            .ok_or_else(|| Error::Parse("malformed SET in trigger".into()))?;
        let lhs = rest[..eq].trim();
        let col = lhs
            .to_ascii_lowercase()
            .strip_prefix("new.")
            .map(|_| lhs[4..].to_string())
            .ok_or_else(|| {
                Error::Unsupported("BEFORE trigger SET target must be NEW.col".into())
            })?;
        let ci = schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&col))
            .ok_or_else(|| Error::Query(format!("unknown column in trigger: {col}")))?;
        let sub = substitute_newold(rest[eq + 1..].trim(), schema, Some(row), old)?;
        let expr = parse_scalar_expr(&sub)?;
        let val = predicate::eval_row(&expr, &empty, &[])?;
        row[ci] = coerce(val, &schema.columns[ci].ty, &schema.columns[ci].name)?;
    }
    Ok(())
}

/// Queue AFTER-trigger bodies (rendered to concrete SQL) for a set of rows.
fn queue_after(
    db: &Session,
    trigs: &[catalog::TriggerDef],
    schema: &Schema,
    new: Option<&[Value]>,
    old: Option<&[Value]>,
) -> Result<()> {
    for t in trigs {
        let sql = substitute_newold(&strip_begin_end(&t.body), schema, new, old)?;
        db.queue_trigger(sql);
    }
    Ok(())
}

/// Parse a table's CHECK expressions once (for a whole statement).
fn parse_checks(def: &TableDef) -> Result<Vec<Expr>> {
    def.checks.iter().map(|s| parse_scalar_expr(s)).collect()
}

/// A CHECK is satisfied unless it evaluates to FALSE (NULL/UNKNOWN passes).
fn check_row(def: &TableDef, checks: &[Expr], row: &[Value]) -> Result<()> {
    for c in checks {
        let fails = match predicate::eval_row(c, &def.schema, row)? {
            Value::Null => false,
            Value::Bool(b) => !b,
            Value::Int(i) => i == 0,
            Value::Float(f) => f == 0.0,
            _ => false,
        };
        if fails {
            return Err(Error::Query(format!(
                "CHECK constraint violated for '{}'",
                def.name
            )));
        }
    }
    Ok(())
}

/// The parent-table storage key to probe for a referenced-key's existence.
/// Foreign keys must reference the parent's primary key or a unique index.
fn fk_probe_key(parent: &TableDef, ref_cols: &[String], vals: &[Value]) -> Result<Vec<u8>> {
    let name_match = |cols: &[usize]| {
        cols.len() == ref_cols.len()
            && cols
                .iter()
                .zip(ref_cols)
                .all(|(&i, rc)| parent.schema.columns[i].name.eq_ignore_ascii_case(rc))
    };
    if !parent.pk_cols.is_empty() && name_match(&parent.pk_cols) {
        return Ok(data_key(
            &parent.name,
            &keyenc::encode_key_coll(vals, &parent.pk_collations())?,
        ));
    }
    for idx in &parent.indexes {
        if idx.unique && !idx.vector && name_match(&idx.cols) {
            return index::unique_probe_key(&parent.name, &idx.name, vals, &idx.col_collations);
        }
    }
    Err(Error::Query(format!(
        "foreign key must reference the primary key or a unique index of '{}'",
        parent.name
    )))
}

/// Verify every foreign key of `def` for the rows in `batch`: each non-NULL
/// referencing tuple must exist in the parent (error 1452 otherwise).
async fn check_fk_batch(
    db: &Session,
    def: &TableDef,
    batch: &[(Vec<u8>, Vec<Value>)],
) -> Result<()> {
    for fk in &def.foreign_keys {
        let parent = catalog::load(db, &fk.ref_table).await?;
        let mut probes = Vec::new();
        for (_, row) in batch {
            let vals: Vec<Value> = fk.columns.iter().map(|&i| row[i].clone()).collect();
            if vals.iter().any(|v| v.is_null()) {
                continue; // a NULL in the referencing tuple is allowed
            }
            probes.push(fk_probe_key(&parent, &fk.ref_columns, &vals)?);
        }
        if probes.is_empty() {
            continue;
        }
        for found in db.multi_get(probes).await? {
            if found.is_none() {
                return Err(Error::ForeignKey(format!(
                    "a row in '{}' has no matching parent in '{}' (constraint '{}')",
                    def.name, fk.ref_table, fk.name
                )));
            }
        }
    }
    Ok(())
}

/// Verify that no row in `batch` collides on a unique index, either with
/// another row in the batch or an existing row owned by a different key.
async fn check_unique_batch(
    db: &Session,
    def: &TableDef,
    batch: &[(Vec<u8>, Vec<Value>)],
) -> Result<()> {
    let mut probes: Vec<(Vec<u8>, usize)> = Vec::new();
    for (i, (_, row)) in batch.iter().enumerate() {
        for pk in index::unique_probe_keys(def, row)? {
            probes.push((pk, i));
        }
    }
    if probes.is_empty() {
        return Ok(());
    }
    // Two batch rows sharing a probe key violate uniqueness.
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for (pk, _) in &probes {
        if !seen.insert(pk.clone()) {
            return Err(Error::Duplicate("Duplicate entry for a unique key".into()));
        }
    }
    // A stored value under the probe key that belongs to a different row.
    let keys: Vec<Vec<u8>> = probes.iter().map(|(k, _)| k.clone()).collect();
    let existing = db.multi_get(keys).await?;
    for ((_, i), owner) in probes.iter().zip(existing) {
        if let Some(owner_key) = owner {
            if owner_key != batch[*i].0 {
                return Err(Error::Duplicate("Duplicate entry for a unique key".into()));
            }
        }
    }
    Ok(())
}

pub async fn select(
    db: &Session,
    vindex: &VectorRegistry,
    query: &SqlQuery,
) -> Result<QueryResult> {
    // Expand CTEs (WITH ...) into derived tables, then execute. Recursive CTEs
    // take a fixpoint-materialisation path via temporary relations.
    if let Some(w) = &query.with {
        if w.recursive {
            return Box::pin(execute_recursive_cte(db, vindex, query)).await;
        }
        let expanded = expand_ctes(query)?;
        return Box::pin(select(db, vindex, &expanded)).await;
    }

    // Expand view references in FROM into derived tables.
    let view_expanded;
    let query = if from_has_plain_table(query) {
        view_expanded = expand_views(db, query).await?;
        &view_expanded
    } else {
        query
    };

    // Top-level set operations (UNION / INTERSECT / EXCEPT).
    if matches!(query.body.as_ref(), SetExpr::SetOperation { .. }) {
        return Box::pin(execute_set_query(db, vindex, query)).await;
    }
    // A parenthesised subquery as the whole body.
    if let SetExpr::Query(inner) = query.body.as_ref() {
        return Box::pin(select(db, vindex, inner)).await;
    }

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
    // Resolve uncorrelated WHERE subqueries into literals / value lists
    // (IN / scalar / EXISTS). A subquery that references an outer column fails
    // to resolve standalone; that marks the filter as correlated, handled
    // per-row after the table is loaded.
    let raw_filter = select.selection.clone();

    // GROUP BY / ORDER BY.
    let group_by: Vec<Expr> = match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
        sqlparser::ast::GroupByExpr::All(_) => {
            return Err(Error::Unsupported("GROUP BY ALL is not supported".into()))
        }
    };
    let order_exprs: Vec<(Expr, bool)> = match &query.order_by {
        Some(ob) => ob
            .exprs
            .iter()
            .map(|o| (o.expr.clone(), o.asc.unwrap_or(true)))
            .collect(),
        None => Vec::new(),
    };

    // Multi-table / JOIN queries, and any query over a derived table
    // (FROM (SELECT ...)), take the materialised path.
    let is_join = select.from.len() > 1
        || select
            .from
            .iter()
            .any(|t| !t.joins.is_empty() || matches!(t.relation, TableFactor::Derived { .. }));
    if is_join {
        // A subquery that references one of the join's tables is correlated;
        // evaluate it per joined row.
        let quals = join_qualifiers(&select.from);
        let correlated = raw_filter
            .as_ref()
            .is_some_and(|f| filter_correlated_any(f, &quals))
            || projection_correlated_any(&select.projection, &quals);
        if correlated {
            return join_correlated_select(
                db,
                vindex,
                select,
                raw_filter.clone(),
                group_by,
                order_exprs,
                offset,
                limit,
            )
            .await;
        }
        let filter = match raw_filter {
            Some(f) => Some(resolve_subqueries(db, vindex, f).await?),
            None => None,
        };
        return join_select(
            db,
            vindex,
            select,
            filter,
            group_by,
            order_exprs,
            offset,
            limit,
        )
        .await;
    }

    // FROM-less SELECT (e.g. `SELECT 1`, recursive-CTE anchors): one row.
    if select.from.is_empty() {
        use sqlparser::ast::SelectItem;
        let empty = Schema::new(Vec::new());
        let empty_row: Vec<Value> = Vec::new();
        let pass = match &raw_filter {
            Some(f) => {
                let rf = resolve_subqueries(db, vindex, f.clone()).await?;
                predicate::matches(&rf, &empty, &empty_row)?
            }
            None => true,
        };
        let mut cols = Vec::with_capacity(select.projection.len());
        let mut vals = Vec::with_capacity(select.projection.len());
        for (ci, item) in select.projection.iter().enumerate() {
            let expr = match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                other => {
                    return Err(Error::Unsupported(format!(
                        "projection item not supported without FROM: {other}"
                    )))
                }
            };
            let e = resolve_subqueries(db, vindex, expr.clone()).await?;
            let v = predicate::eval_row(&e, &empty, &empty_row)?;
            let name = match item {
                SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                SelectItem::UnnamedExpr(e) => ident_name(e)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| e.to_string()),
                _ => format!("col{ci}"),
            };
            cols.push(ColumnDef {
                name,
                ty: infer_val(&v),
                nullable: true,
                collation: elyra_core::Collation::Ci,
            });
            vals.push(v);
        }
        let rows = if pass { vec![vals] } else { Vec::new() };
        return Ok(QueryResult::Rows(RowStream::literal(
            Schema::new(cols),
            rows,
        )));
    }

    if select.from.len() != 1 {
        return Err(Error::Unsupported(
            "exactly one table in FROM is supported".into(),
        ));
    }

    // information_schema.<view> as a single virtual relation.
    if let Some(view) = information_schema_view(&select.from[0].relation) {
        let (schema, rows) = information_schema(db, &view).await?;
        return run_virtual_select(
            db,
            vindex,
            select,
            schema,
            rows,
            &group_by,
            &order_exprs,
            offset,
            limit,
        )
        .await;
    }

    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => table_ident(name)?,
        _ => {
            return Err(Error::Unsupported(
                "only plain table references are supported".into(),
            ))
        }
    };
    let def = catalog::load(db, &table).await?;

    // Outer table name/alias, used to detect and bind correlated subqueries.
    let outer = match &select.from[0].relation {
        TableFactor::Table { alias, .. } => alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| table.clone()),
        _ => table.clone(),
    };

    // A WHERE or SELECT-list subquery that references `outer.<col>` is
    // correlated: evaluate per outer row with the outer columns bound.
    let correlated = raw_filter
        .as_ref()
        .is_some_and(|f| filter_correlated(f, &outer))
        || projection_correlated(&select.projection, &outer);
    if correlated {
        let corr_filter = raw_filter
            .clone()
            .unwrap_or(Expr::Value(sqlparser::ast::Value::Boolean(true)));
        return correlated_select(
            db,
            vindex,
            select,
            &def,
            &outer,
            &corr_filter,
            &group_by,
            &order_exprs,
            offset,
            limit,
        )
        .await;
    }

    // Otherwise resolve uncorrelated WHERE subqueries into literals.
    let filter = match raw_filter {
        Some(f) => Some(resolve_subqueries(db, vindex, f).await?),
        None => None,
    };

    // SELECT ... FOR UPDATE / FOR SHARE inside a transaction: record the matched
    // rows so a concurrent change to them aborts this transaction at commit
    // (optimistic row locking).
    if !query.locks.is_empty() && db.in_txn() {
        let matched = collect_matches(db, &def, filter.as_ref(), None).await?;
        let keys: Vec<Vec<u8>> = matched.into_iter().map(|(k, _)| k).collect();
        db.lock_keys(&keys);
    }

    // Resolve uncorrelated subqueries in the SELECT list; work with the
    // resolved projection thereafter.
    let resolved_select;
    let select = if projection_has_subquery(&select.projection) {
        let mut s = select.clone();
        for item in &mut s.projection {
            *item = resolve_item(db, vindex, item).await?;
        }
        resolved_select = s;
        &resolved_select
    } else {
        select
    };

    // Window functions in the projection take a dedicated materialised path.
    if projection_has_window(&select.projection) {
        return window_select(
            db,
            &def,
            select,
            filter.as_ref(),
            &order_exprs,
            offset,
            limit,
        )
        .await;
    }

    // Aggregation / grouping path: parallel streaming aggregation (OLAP).
    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        let plan = aggregate::build_plan(&def.schema, &select.projection, &group_by)?;
        let agg = olap_aggregate(db, &def, filter.clone(), &plan).await?;
        let (schema, out_rows) = if agg.overflowed() {
            // Too many distinct groups for memory: fall back to partitioned
            // spill aggregation (bounded memory).
            partitioned_aggregate(db, &def, filter.clone(), &plan).await?
        } else {
            plan.finalize(agg)
        };
        let mut out_rows = apply_having(
            select.having.as_ref(),
            &select.projection,
            &schema,
            out_rows,
        )?;
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
                if def
                    .indexes
                    .iter()
                    .any(|i| i.vector && i.single_col() == Some(col))
                {
                    let cached = vindex.get(db, &def, col, Metric::L2).await?;
                    let hits = cached.index.search(&q, k, (k * 4).max(64));
                    let keys: Vec<Vec<u8>> = hits
                        .iter()
                        .map(|(node, _)| cached.keys[*node as usize].clone())
                        .collect();
                    let blobs = db.multi_get(keys).await?;
                    let mut rows = Vec::with_capacity(blobs.len());
                    for bytes in blobs.into_iter().flatten() {
                        rows.push(
                            bincode::deserialize::<Vec<Value>>(&bytes)
                                .map_err(|e| Error::Storage(e.to_string()))?,
                        );
                    }
                    // Order the candidate set by exact distance for a clean top-k.
                    sort_full_rows(&mut rows, &def.schema, &resolved)?;
                    rows.truncate(k);
                    let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
                    return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
                }
            }
        }

        // Memory-bounded ORDER BY for the non-accelerable autocommit case:
        // stream the filtered rows and sort with a top-N heap (when LIMIT is
        // small) or an external merge sort that spills to disk (OOM safety),
        // instead of materialising the whole result set.
        if !resolved.is_empty() && !db.in_txn() && !accelerable(&def, filter.as_ref())? {
            let ncols = def.schema.columns.len();
            let spec = ScanSpec {
                projection: (0..ncols).collect(),
                out_schema: def.schema.clone(),
                filter: filter.clone(),
                offset: 0,
                limit: None,
            };
            let mut stream = RowStream::scan(db.raw_db(), &def, spec);
            let asc: Vec<bool> = resolved.iter().map(|(_, a)| *a).collect();
            let mut sorter =
                crate::sort::Sorter::new(asc, offset, limit, crate::sort::sort_max_rows());
            loop {
                let batch = stream.next_batch(4096).await?;
                if batch.is_empty() {
                    break;
                }
                for row in batch {
                    let mut keys = Vec::with_capacity(resolved.len());
                    for (e, _) in &resolved {
                        keys.push(predicate::eval_row(e, &def.schema, &row)?);
                    }
                    sorter.push(keys, row)?;
                }
            }
            let rows = sorter.finish()?;
            let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
            return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
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
            let (ident, alias) = match item {
                SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(id)) => (&id.value, None),
                SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::Identifier(id),
                    alias,
                } => (&id.value, Some(alias.value.clone())),
                SelectItem::UnnamedExpr(sqlparser::ast::Expr::CompoundIdentifier(parts))
                    if !parts.is_empty() =>
                {
                    (&parts.last().unwrap().value, None)
                }
                SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::CompoundIdentifier(parts),
                    alias,
                } if !parts.is_empty() => (&parts.last().unwrap().value, Some(alias.value.clone())),
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
            let mut col = def.schema.columns[i].clone();
            if let Some(a) = alias {
                col.name = a; // honor `col AS alias` in the output schema
            }
            cols.push(col);
        }
        (idxs, cols)
    };

    let out_schema = Schema::new(out_cols);

    // Fast path: PK/index equality (single or composite) or a range on a
    // PK/indexed column -> fetch via the index and project, instead of a scan.
    if accelerable(&def, filter.as_ref())? {
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

    // Inside a transaction, reads must observe the snapshot + buffered writes,
    // so materialise through the session rather than streaming from committed
    // storage. Autocommit reads stream directly for bounded memory.
    if db.in_txn() {
        let mut rows = scan_rows(db, &def, filter.as_ref()).await?;
        apply_offset_limit(&mut rows, offset, limit);
        let out: Vec<Vec<Value>> = rows
            .iter()
            .map(|r| projection.iter().map(|&i| r[i].clone()).collect())
            .collect();
        return Ok(QueryResult::Rows(RowStream::literal(out_schema, out)));
    }

    Ok(QueryResult::Rows(RowStream::scan(
        db.raw_db(),
        &def,
        ScanSpec {
            projection,
            out_schema,
            filter,
            offset,
            limit,
        },
    )))
}

/// Execute a multi-table / JOIN SELECT: materialise the joined row set, then
/// apply WHERE, aggregation or ORDER BY, projection and paging.
#[allow(clippy::too_many_arguments)]
async fn join_select(
    db: &Session,
    vindex: &VectorRegistry,
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

    let (cols, mut rows) = build_from(db, vindex, &select.from, &conjuncts).await?;
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
        let (osch, orows) = aggregate::run(&schema, &select.projection, &group_by, rows)?;
        let mut orows = apply_having(select.having.as_ref(), &select.projection, &osch, orows)?;
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

/// The table qualifiers (alias or name) of every relation in a FROM clause.
fn join_qualifiers(from: &[TableWithJoins]) -> Vec<String> {
    let mut q = Vec::new();
    for twj in from {
        if let Some(n) = factor_qualifier(&twj.relation) {
            q.push(n);
        }
        for j in &twj.joins {
            if let Some(n) = factor_qualifier(&j.relation) {
                q.push(n);
            }
        }
    }
    q
}

fn factor_qualifier(tf: &TableFactor) -> Option<String> {
    match tf {
        TableFactor::Table { name, alias, .. } => alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .or_else(|| name.0.last().map(|i| i.value.clone())),
        TableFactor::Derived { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
        _ => None,
    }
}

fn filter_correlated_any(f: &Expr, quals: &[String]) -> bool {
    quals.iter().any(|q| filter_correlated(f, q))
}

fn projection_correlated_any(projection: &[sqlparser::ast::SelectItem], quals: &[String]) -> bool {
    quals.iter().any(|q| projection_correlated(projection, q))
}

/// Bind every qualified column reference (`alias.col`) that resolves in the
/// joined `schema` to its literal value from `row`, including inside
/// subqueries. Outer references in correlated subqueries become literals; the
/// subquery's own columns are left untouched.
fn bind_row(expr: &Expr, schema: &Schema, row: &[Value]) -> Expr {
    map_expr(expr, &|e| {
        if let Expr::CompoundIdentifier(parts) = e {
            if parts.len() >= 2 {
                let qual = format!(
                    "{}.{}",
                    parts[parts.len() - 2].value,
                    parts[parts.len() - 1].value
                );
                if let Some(i) = schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&qual))
                {
                    return Some(value_to_expr(&row[i]));
                }
            }
        }
        None
    })
}

/// Execute a join whose WHERE or SELECT list has a correlated subquery: build
/// the joined rows, then bind outer references and resolve the subqueries per
/// row for both the filter and the projection.
#[allow(clippy::too_many_arguments)]
async fn join_correlated_select(
    db: &Session,
    vindex: &VectorRegistry,
    select: &Select,
    raw_filter: Option<Expr>,
    group_by: Vec<Expr>,
    order_exprs: Vec<(Expr, bool)>,
    offset: usize,
    limit: Option<usize>,
) -> Result<QueryResult> {
    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        return Err(Error::Unsupported(
            "correlated subqueries combined with aggregation over joins are not supported".into(),
        ));
    }

    let (cols, rows) = build_from(db, vindex, &select.from, &[]).await?;
    let schema = Schema::new(cols);

    let mut kept: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        if let Some(f) = &raw_filter {
            let bound = bind_row(f, &schema, &row);
            let resolved = resolve_subqueries(db, vindex, bound).await?;
            if !predicate::matches(&resolved, &schema, &row)? {
                continue;
            }
        }
        kept.push(row);
    }

    let resolved_order = resolve_order_aliases(&order_exprs, &select.projection, &schema);
    if !resolved_order.is_empty() {
        sort_full_rows(&mut kept, &schema, &resolved_order)?;
    }
    apply_offset_limit(&mut kept, offset, limit);

    // Plain projection when no SELECT-list subqueries.
    if !projection_has_subquery(&select.projection) {
        let (osch, out) = project_exprs(&select.projection, &schema, &kept)?;
        return Ok(QueryResult::Rows(RowStream::literal(osch, out)));
    }

    // Per-row projection with correlated SELECT-list subqueries.
    use sqlparser::ast::SelectItem;
    let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(kept.len());
    for row in &kept {
        let mut vals = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            let expr = match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                other => {
                    return Err(Error::Unsupported(format!(
                        "projection item not supported with correlated subquery: {other}"
                    )))
                }
            };
            let bound = bind_row(expr, &schema, row);
            let resolved = resolve_subqueries(db, vindex, bound).await?;
            vals.push(predicate::eval_row(&resolved, &schema, row)?);
        }
        out_rows.push(vals);
    }

    let mut outcols = Vec::with_capacity(select.projection.len());
    for (ci, item) in select.projection.iter().enumerate() {
        let name = match item {
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            SelectItem::UnnamedExpr(e) => ident_name(e)
                .map(|s| s.to_string())
                .unwrap_or_else(|| e.to_string()),
            _ => format!("col{ci}"),
        };
        let ty = out_rows
            .iter()
            .map(|r| &r[ci])
            .find(|v| !v.is_null())
            .map(infer_val)
            .unwrap_or(ColumnType::Text);
        outcols.push(ColumnDef {
            name,
            ty,
            nullable: true,
            collation: elyra_core::Collation::Ci,
        });
    }
    Ok(QueryResult::Rows(RowStream::literal(
        Schema::new(outcols),
        out_rows,
    )))
}

/// Load a single FROM relation into `(qualified columns, rows)`. Column names
/// are qualified with the table alias (or name) as "alias.col".
///
/// If a single-table conjunct is `indexed_col = <literal>` (PK or secondary
/// index), the base relation is fetched via the O(log n) fast path instead of
/// a full scan.
/// Resolve a table reference to its definition and qualified ("alias.col")
/// columns, without reading any rows.
async fn resolve_table(db: &Session, tf: &TableFactor) -> Result<(TableDef, Vec<ColumnDef>)> {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let tname = table_ident(name)?;
            let def = catalog::load(db, &tname).await?;
            let a = alias
                .as_ref()
                .map(|al| al.name.value.clone())
                .unwrap_or_else(|| tname.clone());
            let cols = def
                .schema
                .columns
                .iter()
                .map(|c| ColumnDef {
                    name: format!("{a}.{}", c.name),
                    ty: c.ty.clone(),
                    nullable: c.nullable,
                    collation: elyra_core::Collation::Ci,
                })
                .collect();
            Ok((def, cols))
        }
        _ => Err(Error::Unsupported(
            "only plain table references are supported in joins".into(),
        )),
    }
}

async fn load_relation(
    db: &Session,
    vindex: &VectorRegistry,
    tf: &TableFactor,
    conjuncts: &[Expr],
) -> Result<(Vec<ColumnDef>, Vec<Vec<Value>>)> {
    // information_schema.<view>: synthesize a virtual relation.
    if let Some(view) = information_schema_view(tf) {
        let (schema, rows) = information_schema(db, &view).await?;
        let qual = factor_qualifier(tf).unwrap_or(view);
        let cols = schema
            .columns
            .iter()
            .map(|c| ColumnDef {
                name: format!("{qual}.{}", c.name),
                ty: c.ty.clone(),
                nullable: c.nullable,
                collation: elyra_core::Collation::Ci,
            })
            .collect();
        return Ok((cols, rows));
    }

    // Derived table: materialise the subquery and qualify its columns.
    if let TableFactor::Derived {
        subquery, alias, ..
    } = tf
    {
        let alias = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .ok_or_else(|| {
                Error::Query("a derived table (FROM (SELECT ...)) needs an alias".into())
            })?;
        let (schema, rows) = run_subquery_schema(db, vindex, subquery).await?;
        let cols = schema
            .columns
            .iter()
            .map(|c| ColumnDef {
                name: format!("{alias}.{}", c.name),
                ty: c.ty.clone(),
                nullable: c.nullable,
                collation: elyra_core::Collation::Ci,
            })
            .collect();
        return Ok((cols, rows));
    }

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
    db: &Session,
    def: &TableDef,
    col: usize,
    value: &Value,
) -> Result<Vec<Vec<Value>>> {
    let deser = |b: Vec<u8>| -> Result<Vec<Value>> {
        bincode::deserialize(&b).map_err(|e| Error::Storage(e.to_string()))
    };
    if def.pk_cols == [col] {
        let key = data_key(
            &def.name,
            &keyenc::encode_coll(value, def.collation_of(col))?,
        );
        return Ok(match db.get(key).await? {
            Some(b) => vec![deser(b)?],
            None => vec![],
        });
    }
    if let Some(idx) = index::index_on(def, col) {
        let dks = index::lookup_eq(db, &def.name, idx, std::slice::from_ref(value)).await?;
        let blobs = db.multi_get(dks).await?;
        let mut out = Vec::new();
        for b in blobs.into_iter().flatten() {
            out.push(deser(b)?);
        }
        return Ok(out);
    }
    Err(Error::Query(
        "column is not indexed for nested-loop join".into(),
    ))
}

/// If `on` is `A = B` with one operand referencing only the driving side and
/// the other a plain column of the partner, return `(driving_key_expr,
/// partner_col_index)` for an index nested-loop probe.
fn equi_nlj(on: &Expr, driving: &Schema, partner: &Schema) -> Option<(Expr, usize)> {
    let Expr::BinaryOp {
        left,
        op: sqlparser::ast::BinaryOperator::Eq,
        right,
    } = on
    else {
        return None;
    };
    let plain = |e: &Expr| -> Option<String> {
        match e {
            Expr::Identifier(id) => Some(id.value.clone()),
            Expr::CompoundIdentifier(p) => Some(
                p.iter()
                    .map(|i| i.value.as_str())
                    .collect::<Vec<_>>()
                    .join("."),
            ),
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

/// Whether `conjunct` is `col = <literal>` on this table's PK or an index.
fn is_accelerable(def: &TableDef, conjunct: &Expr) -> Result<bool> {
    Ok(match eq_col_literal(def, Some(conjunct))? {
        Some((col, _)) => def.pk_cols == [col] || index::index_on(def, col).is_some(),
        None => false,
    })
}

/// A range constraint on one column, `(value, inclusive)` bounds.
struct RangeQuery {
    col: usize,
    lo: Option<(Value, bool)>,
    hi: Option<(Value, bool)>,
}

/// Detect a range over a PK/indexed column from the filter's AND-conjuncts
/// (`col >|>=|<|<= lit`, `col BETWEEN a AND b`). Only columns with
/// order-encodable bound values qualify.
fn range_bounds(def: &TableDef, filter: Option<&Expr>) -> Result<Option<RangeQuery>> {
    use std::collections::HashMap;
    let Some(f) = filter else { return Ok(None) };
    let mut conj = Vec::new();
    split_and(f, &mut conj);

    type Bounds = (Option<(Value, bool)>, Option<(Value, bool)>);
    let mut map: HashMap<usize, Bounds> = HashMap::new();

    for c in &conj {
        if let Some((col, op, val)) = as_range(def, c)? {
            let e = map.entry(col).or_default();
            use sqlparser::ast::BinaryOperator::*;
            match op {
                Gt => e.0 = Some((val, false)),
                GtEq => e.0 = Some((val, true)),
                Lt => e.1 = Some((val, false)),
                LtEq => e.1 = Some((val, true)),
                _ => {}
            }
        } else if let Some((col, lo, hi)) = as_between(def, c)? {
            map.insert(col, (Some((lo, true)), Some((hi, true))));
        }
    }

    for (col, (lo, hi)) in map {
        if lo.is_none() && hi.is_none() {
            continue;
        }
        let indexed = def.pk_cols == [col] || index::index_on(def, col).is_some();
        let encodable = lo
            .as_ref()
            .map(|(v, _)| keyenc::encode(v).is_ok())
            .unwrap_or(true)
            && hi
                .as_ref()
                .map(|(v, _)| keyenc::encode(v).is_ok())
                .unwrap_or(true);
        if indexed && encodable {
            return Ok(Some(RangeQuery { col, lo, hi }));
        }
    }
    Ok(None)
}

/// `col OP literal` (or `literal OP col`) -> `(col, op-relative-to-col, value)`.
fn as_range(
    def: &TableDef,
    expr: &Expr,
) -> Result<Option<(usize, sqlparser::ast::BinaryOperator, Value)>> {
    use sqlparser::ast::BinaryOperator::*;
    let Expr::BinaryOp { left, op, right } = expr else {
        return Ok(None);
    };
    if !matches!(op, Gt | GtEq | Lt | LtEq) {
        return Ok(None);
    }
    let col_of = |n: &str| {
        def.schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(n))
    };
    let coerce_col = |col: usize, v: Value| {
        let c = &def.schema.columns[col];
        coerce(v, &c.ty, &c.name).ok()
    };
    if let Some(col) = ident_name(left).and_then(col_of) {
        if let Ok(v) = eval_expr(right) {
            if let Some(cv) = coerce_col(col, v) {
                return Ok(Some((col, op.clone(), cv)));
            }
        }
    }
    if let Some(col) = ident_name(right).and_then(col_of) {
        if let Ok(v) = eval_expr(left) {
            if let Some(cv) = coerce_col(col, v) {
                let flipped = match op {
                    Gt => Lt,
                    GtEq => LtEq,
                    Lt => Gt,
                    LtEq => GtEq,
                    _ => unreachable!(),
                };
                return Ok(Some((col, flipped, cv)));
            }
        }
    }
    Ok(None)
}

fn as_between(def: &TableDef, expr: &Expr) -> Result<Option<(usize, Value, Value)>> {
    let Expr::Between {
        expr: e,
        negated: false,
        low,
        high,
    } = expr
    else {
        return Ok(None);
    };
    let col_of = |n: &str| {
        def.schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(n))
    };
    let Some(col) = ident_name(e).and_then(col_of) else {
        return Ok(None);
    };
    let c = &def.schema.columns[col];
    match (eval_expr(low), eval_expr(high)) {
        (Ok(lo), Ok(hi)) => match (coerce(lo, &c.ty, &c.name), coerce(hi, &c.ty, &c.name)) {
            (Ok(lo), Ok(hi)) => Ok(Some((col, lo, hi))),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

/// Range scan over the clustered (PK) data keyspace.
async fn clustered_range(
    db: &Session,
    def: &TableDef,
    rq: &RangeQuery,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let prefix = data_prefix(&def.name);
    let coll = def.pk_collations().first().copied().unwrap_or_default();
    let mut start = match &rq.lo {
        Some((v, incl)) => {
            let mut b = data_key(&def.name, &keyenc::encode_coll(v, coll)?);
            if !*incl {
                b.push(0x00); // strictly after the row with pk == v
            }
            b
        }
        None => prefix.clone(),
    };
    let end = match &rq.hi {
        Some((v, incl)) => {
            let mut b = data_key(&def.name, &keyenc::encode_coll(v, coll)?);
            if *incl {
                b.push(0x00); // include the row with pk == v
            }
            b
        }
        None => index::prefix_upper_bound(&prefix),
    };

    let mut out = Vec::new();
    loop {
        let batch = db
            .scan_range(start.clone(), Some(end.clone()), 4096)
            .await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 4096;
        start = batch
            .last()
            .map(|(k, _)| {
                let mut n = k.clone();
                n.push(0);
                n
            })
            .unwrap();
        for (k, v) in batch {
            let row = bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;
            out.push((k, row));
        }
        if last {
            break;
        }
    }
    Ok(out)
}

/// Range scan via a secondary index, then batch-fetch the rows.
async fn index_range(
    db: &Session,
    def: &TableDef,
    idx: &IndexDef,
    rq: &RangeQuery,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let lo = rq.lo.as_ref().map(|(v, i)| (v, *i));
    let hi = rq.hi.as_ref().map(|(v, i)| (v, *i));
    let data_keys = index::lookup_range(db, &def.name, idx, lo, hi).await?;
    let blobs = db.multi_get(data_keys.clone()).await?;
    let mut out = Vec::new();
    for (k, blob) in data_keys.into_iter().zip(blobs) {
        if let Some(b) = blob {
            out.push((
                k,
                bincode::deserialize(&b).map_err(|e| Error::Storage(e.to_string()))?,
            ));
        }
    }
    Ok(out)
}

/// Build the joined row set from a FROM clause (comma cross-joins + explicit
/// JOINs), pushing single-table `conjuncts` down to each base relation.
async fn build_from(
    db: &Session,
    vindex: &VectorRegistry,
    from: &[TableWithJoins],
    conjuncts: &[Expr],
) -> Result<(Vec<ColumnDef>, Vec<Vec<Value>>)> {
    // Cost-based reordering of an explicit INNER-join chain over base tables:
    // build from the smallest tables and always extend along a join predicate,
    // keeping intermediate results small.
    if from.len() == 1 {
        let twj = &from[0];
        let all_inner = twj.joins.iter().all(|j| {
            matches!(
                j.join_operator,
                JoinOperator::Inner(_) | JoinOperator::CrossJoin
            )
        });
        let all_tables = matches!(twj.relation, TableFactor::Table { .. })
            && twj
                .joins
                .iter()
                .all(|j| matches!(j.relation, TableFactor::Table { .. }));
        if !twj.joins.is_empty() && all_inner && all_tables {
            if let Some(res) = build_inner_join_reordered(db, vindex, twj, conjuncts).await? {
                return Ok(res);
            }
            // Fell back (a predicate wasn't a clean equi-connector): use the
            // sequential left-to-right plan below, which applies each ON at its
            // own two-relation step.
        }
    }

    let mut cur_cols: Vec<ColumnDef> = Vec::new();
    let mut cur_rows: Vec<Vec<Value>> = Vec::new();
    let mut first = true;

    // Cost-based ordering for a pure comma cross-join (every entry is a plain
    // base table with no explicit JOINs): drive from the smallest analyzed
    // table. This is safe because cross-join + global WHERE is commutative.
    let ordered: Vec<&TableWithJoins> = if from.len() > 1
        && from
            .iter()
            .all(|t| t.joins.is_empty() && matches!(t.relation, TableFactor::Table { .. }))
    {
        let mut idx: Vec<(&TableWithJoins, u64)> = Vec::with_capacity(from.len());
        for t in from {
            let est = match &t.relation {
                TableFactor::Table { name, .. } => {
                    let n = name.0.last().map(|i| i.value.clone()).unwrap_or_default();
                    catalog::load_stats(db, &n)
                        .await?
                        .map(|s| s.rows)
                        .unwrap_or(u64::MAX)
                }
                _ => u64::MAX,
            };
            idx.push((t, est));
        }
        idx.sort_by_key(|(_, est)| *est);
        idx.into_iter().map(|(t, _)| t).collect()
    } else {
        from.iter().collect()
    };

    for twj in ordered {
        let (bc, mut br) = load_relation(db, vindex, &twj.relation, conjuncts).await?;
        br = apply_pushdown(br, &bc, conjuncts)?;
        if first {
            cur_cols = bc;
            cur_rows = br;
            first = false;
        } else {
            let (c, r) = combine(&cur_cols, &cur_rows, &bc, &br, JoinKind::Inner, None)?;
            cur_cols = c;
            cur_rows = r;
        }
        for join in &twj.joins {
            // Index nested-loop join: when the driving side is small and the
            // partner is indexed on the join key, probe the partner per row
            // instead of materialising it in full.
            let driving_schema = Schema::new(cur_cols.clone());
            let (kind, on) = join_kind(&join.join_operator)?;
            let left_outer = kind == JoinKind::Left;

            // Index nested-loop join only applies to a plain (indexed) table
            // partner, not a derived table.
            let nlj = if let TableFactor::Table { .. } = &join.relation {
                let (pdef, pcols) = resolve_table(db, &join.relation).await?;
                let partner_schema = Schema::new(pcols.clone());
                on.as_ref()
                    .filter(|_| matches!(kind, JoinKind::Inner | JoinKind::Left))
                    .and_then(|e| equi_nlj(e, &driving_schema, &partner_schema))
                    .filter(|(_, pcol)| {
                        cur_rows.len() <= NLJ_MAX_DRIVING
                            && (pdef.pk_cols == [*pcol] || index::index_on(&pdef, *pcol).is_some())
                    })
                    .map(|(k, pcol)| (k, pcol, pdef, pcols))
            } else {
                None
            };

            if let Some((driving_key, pcol, pdef, pcols)) = nlj {
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
                        combined.extend(std::iter::repeat_n(Value::Null, plen));
                        out.push(combined);
                    }
                }
                cur_cols.extend(pcols);
                cur_rows = out;
                continue;
            }

            // Fallback: materialise the partner (with pushdown) and hash/nested join.
            let (jc, mut jr) = load_relation(db, vindex, &join.relation, conjuncts).await?;
            jr = apply_pushdown(jr, &jc, conjuncts)?;
            let (c, r) = combine(&cur_cols, &cur_rows, &jc, &jr, kind, on.as_ref())?;
            cur_cols = c;
            cur_rows = r;
        }
    }
    Ok((cur_cols, cur_rows))
}

/// Execute an all-INNER join chain over base tables in a cost-based order.
/// Loads each table (with predicate pushdown), then greedily joins starting from
/// the smallest, always extending along an available equi-join predicate.
async fn build_inner_join_reordered(
    db: &Session,
    vindex: &VectorRegistry,
    twj: &TableWithJoins,
    conjuncts: &[Expr],
) -> Result<Option<(Vec<ColumnDef>, Vec<Vec<Value>>)>> {
    // Collect relations and ON predicates. A CROSS JOIN contributes no predicate.
    let mut relations: Vec<&TableFactor> = vec![&twj.relation];
    let mut on_preds: Vec<Expr> = Vec::new();
    for j in &twj.joins {
        relations.push(&j.relation);
        if let (_, Some(e)) = join_kind(&j.join_operator)? {
            on_preds.push(e);
        }
    }
    // Only reorder when every join is a single equi-condition connector (the
    // common case). Anything else (multi-condition/non-equi ON) falls back to
    // the sequential plan, which applies each ON at its own two-relation step.
    for p in &on_preds {
        if !matches!(
            p,
            Expr::BinaryOp {
                op: sqlparser::ast::BinaryOperator::Eq,
                ..
            }
        ) {
            return Ok(None);
        }
    }
    // As many equi connectors as (tables - 1) are needed to connect the graph.
    if on_preds.len() + 1 < relations.len() {
        return Ok(None);
    }

    // Load each relation (materialize + pushdown) and estimate its size.
    struct Loaded {
        cols: Vec<ColumnDef>,
        rows: Vec<Vec<Value>>,
        est: u64,
    }
    let mut loaded: Vec<Loaded> = Vec::with_capacity(relations.len());
    for rel in &relations {
        let (cols, mut rows) = load_relation(db, vindex, rel, conjuncts).await?;
        rows = apply_pushdown(rows, &cols, conjuncts)?;
        // Prefer the actual loaded size (already filtered) as the cost estimate.
        let est = rows.len() as u64;
        loaded.push(Loaded { cols, rows, est });
    }

    // Start from the smallest relation.
    let mut remaining: Vec<usize> = (0..loaded.len()).collect();
    remaining.sort_by_key(|&i| loaded[i].est);
    let start = remaining.remove(0);
    let mut cur_cols = std::mem::take(&mut loaded[start].cols);
    let mut cur_rows = std::mem::take(&mut loaded[start].rows);

    while !remaining.is_empty() {
        // Among the remaining tables, pick the smallest one connected to what
        // we've built by an equi-join predicate whose two sides' *aliases* span
        // the built set and that table. (Alias-aware, so `c.id` never falsely
        // matches another table's `id` column.) If none connects, fall back to
        // the sequential plan.
        let cur_aliases = relation_aliases(&cur_cols);
        let mut best: Option<(usize, Expr)> = None; // (pos in remaining, connecting pred)
        let mut best_est = u64::MAX;
        for (pos, &i) in remaining.iter().enumerate() {
            let t_aliases = relation_aliases(&loaded[i].cols);
            for pred in &on_preds {
                if let Some((lq, rq)) = equi_qualifiers(pred) {
                    let connects = (cur_aliases.contains(&lq) && t_aliases.contains(&rq))
                        || (cur_aliases.contains(&rq) && t_aliases.contains(&lq));
                    if connects && loaded[i].est < best_est {
                        best_est = loaded[i].est;
                        best = Some((pos, pred.clone()));
                        break;
                    }
                }
            }
        }
        let Some((pos, pred)) = best else {
            return Ok(None);
        };
        let idx = remaining.remove(pos);
        let rcols = std::mem::take(&mut loaded[idx].cols);
        let rrows = std::mem::take(&mut loaded[idx].rows);
        let (c, r) = combine(
            &cur_cols,
            &cur_rows,
            &rcols,
            &rrows,
            JoinKind::Inner,
            Some(&pred),
        )?;
        cur_cols = c;
        cur_rows = r;
    }
    Ok(Some((cur_cols, cur_rows)))
}

/// The set of alias/table qualifiers present in a relation's column names
/// (`"o.cust"` -> `"o"`).
fn relation_aliases(cols: &[ColumnDef]) -> std::collections::HashSet<String> {
    cols.iter()
        .filter_map(|c| c.name.split_once('.').map(|(q, _)| q.to_ascii_lowercase()))
        .collect()
}

/// For an equi predicate `A.x = B.y`, the two operand qualifiers `(a, b)`.
fn equi_qualifiers(pred: &Expr) -> Option<(String, String)> {
    let Expr::BinaryOp {
        left,
        op: sqlparser::ast::BinaryOperator::Eq,
        right,
    } = pred
    else {
        return None;
    };
    Some((expr_qualifier(left)?, expr_qualifier(right)?))
}

fn expr_qualifier(e: &Expr) -> Option<String> {
    match e {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            Some(parts[parts.len() - 2].value.to_ascii_lowercase())
        }
        _ => None,
    }
}

/// Filter `rows` by every conjunct that references only this relation's
/// columns (predicate pushdown).
fn apply_pushdown(
    rows: Vec<Vec<Value>>,
    cols: &[ColumnDef],
    conjuncts: &[Expr],
) -> Result<Vec<Vec<Value>>> {
    let schema = Schema::new(cols.to_vec());
    let applicable: Vec<&Expr> = conjuncts
        .iter()
        .filter(|c| refs_in_schema(c, &schema))
        .collect();
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

/// The four supported join kinds.
#[derive(Clone, Copy, PartialEq)]
enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

fn join_kind(op: &JoinOperator) -> Result<(JoinKind, Option<Expr>)> {
    let on = |c: &JoinConstraint| match c {
        JoinConstraint::On(e) => Some(e.clone()),
        _ => None,
    };
    Ok(match op {
        JoinOperator::Inner(c) => (JoinKind::Inner, on(c)),
        JoinOperator::CrossJoin => (JoinKind::Inner, None),
        JoinOperator::LeftOuter(c) => (JoinKind::Left, on(c)),
        JoinOperator::RightOuter(c) => (JoinKind::Right, on(c)),
        JoinOperator::FullOuter(c) => (JoinKind::Full, on(c)),
        other => {
            return Err(Error::Unsupported(format!(
                "join type not supported: {other:?}"
            )))
        }
    })
}

/// Combine two materialised relations under a join kind. Equi-`ON` INNER/LEFT
/// use a hash join; everything else (RIGHT/FULL, non-equi) is nested-loop.
fn combine(
    lcols: &[ColumnDef],
    lrows: &[Vec<Value>],
    rcols: &[ColumnDef],
    rrows: &[Vec<Value>],
    kind: JoinKind,
    on: Option<&Expr>,
) -> Result<(Vec<ColumnDef>, Vec<Vec<Value>>)> {
    let mut cols = lcols.to_vec();
    cols.extend_from_slice(rcols);
    let lschema = Schema::new(lcols.to_vec());
    let rschema = Schema::new(rcols.to_vec());

    // Hash join for equi INNER/LEFT/RIGHT (cost-based build side). For large
    // INNER equi-joins whose inputs are already sorted on the join key (e.g.
    // clustered primary-key scans), use a streaming merge join instead — no hash
    // table, and the output stays ordered.
    if matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Right) {
        if let Some(e) = on {
            if let Some((lkey, rkey)) = equi_keys(e, &lschema, &rschema) {
                const MERGE_MIN: usize = 2048;
                if kind == JoinKind::Inner && lrows.len() >= MERGE_MIN && rrows.len() >= MERGE_MIN {
                    if let (Some(lk), Some(rk)) = (
                        sorted_keyed(lrows, &lschema, &lkey)?,
                        sorted_keyed(rrows, &rschema, &rkey)?,
                    ) {
                        if let Some(out) = merge_join_inner(lk, rk) {
                            return Ok((cols, out));
                        }
                    }
                }
                let rows = hash_join(lrows, rrows, &lschema, &rschema, &lkey, &rkey, kind)?;
                return Ok((cols, rows));
            }
        }
    }

    let schema = Schema::new(cols.clone());
    let llen = lcols.len();
    let rlen = rcols.len();
    let mut out = Vec::new();
    let mut right_matched = vec![false; rrows.len()];

    for l in lrows {
        let mut matched = false;
        for (ri, r) in rrows.iter().enumerate() {
            let mut combined = l.clone();
            combined.extend_from_slice(r);
            let keep = match on {
                Some(e) => predicate::matches(e, &schema, &combined)?,
                None => true,
            };
            if keep {
                out.push(combined);
                matched = true;
                right_matched[ri] = true;
            }
        }
        if matches!(kind, JoinKind::Left | JoinKind::Full) && !matched {
            let mut combined = l.clone();
            combined.extend(std::iter::repeat_n(Value::Null, rlen));
            out.push(combined);
        }
    }

    // RIGHT/FULL: emit right rows that matched nothing, left side NULL-filled.
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for (ri, r) in rrows.iter().enumerate() {
            if !right_matched[ri] {
                let mut combined = vec![Value::Null; llen];
                combined.extend_from_slice(r);
                out.push(combined);
            }
        }
    }
    Ok((cols, out))
}

/// Equi hash join for INNER / LEFT / RIGHT, always emitting `[left.., right..]`.
/// The build side is chosen by cost: an outer join must build on its
/// non-preserved side; an INNER join builds on the smaller relation.
#[allow(clippy::too_many_arguments)]
fn hash_join(
    lrows: &[Vec<Value>],
    rrows: &[Vec<Value>],
    lschema: &Schema,
    rschema: &Schema,
    lkey: &Expr,
    rkey: &Expr,
    kind: JoinKind,
) -> Result<Vec<Vec<Value>>> {
    use std::collections::HashMap;
    let llen = lschema.columns.len();
    let rlen = rschema.columns.len();

    // Which side to build the hash table on:
    //   LEFT  → build right, probe left  (emit every left row)
    //   RIGHT → build left,  probe right (emit every right row)
    //   INNER → build the smaller side
    let build_left = match kind {
        JoinKind::Left => false,
        JoinKind::Right => true,
        _ => lrows.len() <= rrows.len(),
    };
    let outer = !matches!(kind, JoinKind::Inner);
    let mut out = Vec::new();

    if build_left {
        let mut table: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, l) in lrows.iter().enumerate() {
            if let Some(k) = key_str(&predicate::eval_row(lkey, lschema, l)?) {
                table.entry(k).or_default().push(i);
            }
        }
        for r in rrows {
            let probe = key_str(&predicate::eval_row(rkey, rschema, r)?);
            let mut matched = false;
            if let Some(k) = probe {
                if let Some(idxs) = table.get(&k) {
                    for &i in idxs {
                        let mut combined = lrows[i].clone();
                        combined.extend_from_slice(r);
                        out.push(combined);
                        matched = true;
                    }
                }
            }
            // RIGHT outer: unmatched right row, left side NULL-filled.
            if outer && !matched {
                let mut combined = vec![Value::Null; llen];
                combined.extend_from_slice(r);
                out.push(combined);
            }
        }
    } else {
        let mut table: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, r) in rrows.iter().enumerate() {
            if let Some(k) = key_str(&predicate::eval_row(rkey, rschema, r)?) {
                table.entry(k).or_default().push(i);
            }
        }
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
            // LEFT outer: unmatched left row, right side NULL-filled.
            if outer && !matched {
                let mut combined = l.clone();
                combined.extend(std::iter::repeat_n(Value::Null, rlen));
                out.push(combined);
            }
        }
    }
    Ok(out)
}

/// A join key paired with its source row (for merge join).
type KeyedRows<'a> = Vec<(Value, &'a Vec<Value>)>;

/// If every non-NULL key is non-decreasing, return the (key, row) pairs with
/// NULL-key rows dropped (they never match an equi-join), ready for a merge
/// join; otherwise `None` (the input is not sorted on the key).
fn sorted_keyed<'a>(
    rows: &'a [Vec<Value>],
    schema: &Schema,
    key: &Expr,
) -> Result<Option<KeyedRows<'a>>> {
    let mut out: Vec<(Value, &Vec<Value>)> = Vec::with_capacity(rows.len());
    for r in rows {
        let k = predicate::eval_row(key, schema, r)?;
        if k.is_null() {
            continue;
        }
        if let Some((prev, _)) = out.last() {
            match prev.compare(&k) {
                Some(std::cmp::Ordering::Greater) | None => return Ok(None),
                _ => {}
            }
        }
        out.push((k, r));
    }
    Ok(Some(out))
}

/// Streaming merge join of two key-sorted, NULL-free inputs (INNER equi-join).
/// Returns `None` if two keys are incomparable (mixed types), so the caller can
/// fall back to a hash join.
fn merge_join_inner(l: KeyedRows, r: KeyedRows) -> Option<Vec<Vec<Value>>> {
    use std::cmp::Ordering;
    let mut out = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < l.len() && j < r.len() {
        match l[i].0.compare(&r[j].0)? {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                // Emit the cartesian product of the equal-key blocks on both sides.
                let mut ie = i;
                while ie < l.len() && l[ie].0.compare(&l[i].0)? == Ordering::Equal {
                    ie += 1;
                }
                let mut je = j;
                while je < r.len() && r[je].0.compare(&r[j].0)? == Ordering::Equal {
                    je += 1;
                }
                for a in &l[i..ie] {
                    for b in &r[j..je] {
                        let mut combined = a.1.clone();
                        combined.extend_from_slice(b.1);
                        out.push(combined);
                    }
                }
                i = ie;
                j = je;
            }
        }
    }
    Some(out)
}

/// Hash-key string for a value; `None` for NULL (never matches, per SQL).
fn key_str(v: &Value) -> Option<String> {
    if v.is_null() {
        None
    } else {
        // Collation-folded so equi-joins match the default collation.
        Some(String::from_utf8_lossy(&v.collation_key()).into_owned())
    }
}

/// If `on` is `A = B` with one operand referencing only the left relation and
/// the other only the right, return `(left_key_expr, right_key_expr)`.
fn equi_keys(on: &Expr, lschema: &Schema, rschema: &Schema) -> Option<(Expr, Expr)> {
    let Expr::BinaryOp {
        left,
        op: sqlparser::ast::BinaryOperator::Eq,
        right,
    } = on
    else {
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
    if let Expr::BinaryOp {
        left,
        op: sqlparser::ast::BinaryOperator::And,
        right,
    } = expr
    {
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
    refs.iter()
        .all(|r| predicate::resolve_index(r, schema).is_ok())
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
            out.push(
                parts
                    .iter()
                    .map(|i| i.value.as_str())
                    .collect::<Vec<_>>()
                    .join("."),
            );
            true
        }
        Expr::Value(_) => true,
        Expr::Nested(e) | Expr::UnaryOp { expr: e, .. } => collect_refs(e, out),
        Expr::IsNull(e) | Expr::IsNotNull(e) => collect_refs(e, out),
        Expr::BinaryOp { left, right, .. } => collect_refs(left, out) && collect_refs(right, out),
        Expr::Between {
            expr, low, high, ..
        } => collect_refs(expr, out) && collect_refs(low, out) && collect_refs(high, out),
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
    let Some(Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    }) = filter
    else {
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
    // Coerce the literal to the column's type so index/PK key encoding matches
    // the stored entries (e.g. a DATE column vs a '2024-01-01' text literal).
    let col = &def.schema.columns[idx];
    match coerce(eval_expr(lit_expr)?, &col.ty, &col.name) {
        Ok(v) => Ok(Some((idx, v))),
        Err(_) => Ok(None),
    }
}

/// Extract equality values for every column in `key_cols` from the filter's
/// AND-conjuncts (coerced to column type). `None` if any key column lacks an
/// equality — i.e. the key is not fully specified.
fn key_eq_values(
    def: &TableDef,
    filter: Option<&Expr>,
    key_cols: &[usize],
) -> Result<Option<Vec<Value>>> {
    use std::collections::HashMap;
    if key_cols.is_empty() {
        return Ok(None);
    }
    let Some(f) = filter else { return Ok(None) };
    let mut conj = Vec::new();
    split_and(f, &mut conj);
    let mut found: HashMap<usize, Value> = HashMap::new();
    for c in &conj {
        if let Some((col, val)) = eq_col_literal(def, Some(c))? {
            found.entry(col).or_insert(val);
        }
    }
    let mut vals = Vec::with_capacity(key_cols.len());
    for &kc in key_cols {
        match found.get(&kc) {
            Some(v) => vals.push(v.clone()),
            None => return Ok(None),
        }
    }
    Ok(Some(vals))
}

/// Whether the filter can be served by a PK/index equality (single or
/// composite) or a single-column range.
fn accelerable(def: &TableDef, filter: Option<&Expr>) -> Result<bool> {
    let Some(f) = filter else {
        return Ok(false);
    };
    // A MATCH on a FULLTEXT-indexed column can use the inverted index.
    if let Some((mcols, _, _)) = match_conjunct(f) {
        let cidx: Option<Vec<usize>> = mcols
            .iter()
            .map(|n| {
                def.schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(n))
            })
            .collect();
        if let Some(mut cidx) = cidx {
            cidx.sort_unstable();
            if def.indexes.iter().any(|i| {
                i.fulltext && {
                    let mut ic = i.cols.clone();
                    ic.sort_unstable();
                    ic == cidx
                }
            }) {
                return Ok(true);
            }
        }
    }
    if def.has_pk() && key_eq_values(def, filter, &def.pk_cols)?.is_some() {
        return Ok(true);
    }
    for idx in &def.indexes {
        if !idx.vector && key_eq_values(def, filter, &idx.cols)?.is_some() {
            return Ok(true);
        }
    }
    Ok(range_bounds(def, filter)?.is_some())
}

/// Collect `(storage_key, row)` for every row matching `filter`, up to
/// `limit`. Uses the PK point-lookup fast path when possible, otherwise a
/// bounded-batch clustered scan.
/// Extract a `MATCH(cols) AGAINST('query' [boolean])` conjunct from a WHERE.
fn match_conjunct(f: &Expr) -> Option<(Vec<String>, String, bool)> {
    use sqlparser::ast::{SearchModifier, Value as SqlValue};
    let mut cs = Vec::new();
    split_and(f, &mut cs);
    for c in &cs {
        if let Expr::MatchAgainst {
            columns,
            match_value,
            opt_search_modifier,
        } = c
        {
            let cols = columns.iter().map(|i| i.value.clone()).collect();
            let query = match match_value {
                SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.clone(),
                other => other.to_string(),
            };
            let boolean = matches!(opt_search_modifier, Some(SearchModifier::InBooleanMode));
            return Some((cols, query, boolean));
        }
    }
    None
}

async fn collect_matches(
    db: &Session,
    def: &TableDef,
    filter: Option<&Expr>,
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let mut out = Vec::new();

    let recheck = |row: &[Value]| -> Result<bool> {
        match filter {
            Some(f) => predicate::matches(f, &def.schema, row),
            None => Ok(true),
        }
    };

    // Full-text fast path: a MATCH(col) AGAINST(...) conjunct on a column with a
    // FULLTEXT index -> fetch candidates from the inverted index (union of the
    // stemmed query terms' postings), then re-check the full predicate.
    if let Some(f) = filter {
        if let Some((mcols, query, boolean)) = match_conjunct(f) {
            let cidx: Option<Vec<usize>> = mcols
                .iter()
                .map(|n| {
                    def.schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(n))
                })
                .collect();
            if let Some(mut cidx) = cidx {
                cidx.sort_unstable();
                if let Some(idx) = def.indexes.iter().find(|i| {
                    i.fulltext && {
                        let mut ic = i.cols.clone();
                        ic.sort_unstable();
                        ic == cidx
                    }
                }) {
                    let mut seen = std::collections::HashSet::new();
                    let mut cand = Vec::new();
                    for raw in query.split_whitespace() {
                        if boolean && raw.starts_with('-') {
                            continue;
                        }
                        let cleaned: String = raw
                            .trim_start_matches(['+', '-'])
                            .chars()
                            .filter(|c| c.is_alphanumeric())
                            .collect();
                        if cleaned.is_empty() {
                            continue;
                        }
                        let stem = crate::ft::stem(&cleaned);
                        for dk in index::fulltext_lookup(db, &def.name, &idx.name, &stem).await? {
                            if seen.insert(dk.clone()) {
                                cand.push(dk);
                            }
                        }
                    }
                    let blobs = db.multi_get(cand.clone()).await?;
                    for (k, b) in cand.into_iter().zip(blobs) {
                        if let Some(bytes) = b {
                            let row: Vec<Value> = bincode::deserialize(&bytes)
                                .map_err(|e| Error::Storage(e.to_string()))?;
                            if recheck(&row)? {
                                out.push((k, row));
                                if let Some(l) = limit {
                                    if out.len() >= l {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    return Ok(out);
                }
            }
        }
    }

    // PK equality (single or composite): direct clustered-key lookup.
    if def.has_pk() {
        if let Some(vals) = key_eq_values(def, filter, &def.pk_cols)? {
            let key = data_key(
                &def.name,
                &keyenc::encode_key_coll(&vals, &def.pk_collations())?,
            );
            if let Some(bytes) = db.get(key.clone()).await? {
                let row: Vec<Value> =
                    bincode::deserialize(&bytes).map_err(|e| Error::Storage(e.to_string()))?;
                if recheck(&row)? {
                    out.push((key, row));
                }
            }
            return Ok(out);
        }
    }

    // Secondary-index equality (single or composite), full filter re-applied.
    for idx in &def.indexes {
        if idx.vector {
            continue;
        }
        if let Some(vals) = key_eq_values(def, filter, &idx.cols)? {
            let data_keys = index::lookup_eq(db, &def.name, idx, &vals).await?;
            let blobs = db.multi_get(data_keys.clone()).await?;
            for (data_key, blob) in data_keys.into_iter().zip(blobs) {
                if let Some(bytes) = blob {
                    let row: Vec<Value> =
                        bincode::deserialize(&bytes).map_err(|e| Error::Storage(e.to_string()))?;
                    if recheck(&row)? {
                        out.push((data_key, row));
                        if let Some(l) = limit {
                            if out.len() >= l {
                                return Ok(out);
                            }
                        }
                    }
                }
            }
            return Ok(out);
        }
    }

    // Range fast path: `col > x` / `BETWEEN` on a PK or indexed column uses an
    // ordered range scan, then re-applies the full filter.
    if let Some(rq) = range_bounds(def, filter)? {
        let candidates = if def.pk_cols == [rq.col] {
            clustered_range(db, def, &rq).await?
        } else {
            let idx = index::index_on(def, rq.col).expect("range_bounds checked index");
            index_range(db, def, idx, &rq).await?
        };
        for (k, row) in candidates {
            if let Some(f) = filter {
                if !predicate::matches(f, &def.schema, &row)? {
                    continue;
                }
            }
            out.push((k, row));
            if let Some(l) = limit {
                if out.len() >= l {
                    return Ok(out);
                }
            }
        }
        return Ok(out);
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
        _ => Err(Error::Unsupported(
            "only plain table references are supported".into(),
        )),
    }
}

/// Collect the rows a mutation should touch, resolving any subquery in the
/// WHERE. Uncorrelated subqueries resolve once; a subquery correlated with the
/// target table is evaluated per row.
async fn mutation_matches(
    db: &Session,
    vindex: &VectorRegistry,
    def: &TableDef,
    qualifier: &str,
    selection: Option<&Expr>,
    limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    let Some(f) = selection else {
        return collect_matches(db, def, None, limit).await;
    };
    if filter_correlated(f, qualifier) {
        let all = collect_matches(db, def, None, None).await?;
        let mut out = Vec::new();
        for (key, row) in all {
            let bound = bind_outer(f, qualifier, &def.schema, &row);
            let resolved = resolve_subqueries(db, vindex, bound).await?;
            if predicate::matches(&resolved, &def.schema, &row)? {
                out.push((key, row));
                if let Some(l) = limit {
                    if out.len() >= l {
                        break;
                    }
                }
            }
        }
        Ok(out)
    } else {
        let resolved = resolve_subqueries(db, vindex, f.clone()).await?;
        collect_matches(db, def, Some(&resolved), limit).await
    }
}

pub async fn update(
    db: &Session,
    vindex: &VectorRegistry,
    table: &TableWithJoins,
    assignments: &[Assignment],
    selection: Option<&Expr>,
) -> Result<QueryResult> {
    if !table.joins.is_empty() {
        return multi_update(db, vindex, table, assignments, selection).await;
    }
    let name = table_of(table)?;
    let def = catalog::load(db, &name).await?;
    let qualifier = factor_qualifier(&table.relation).unwrap_or_else(|| name.clone());

    // Resolve assignment targets to column indices.
    let mut sets: Vec<(usize, &Expr)> = Vec::with_capacity(assignments.len());
    for a in assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(n) => {
                n.0.last()
                    .map(|i| i.value.clone())
                    .ok_or_else(|| Error::Query("empty assignment target".into()))?
            }
            AssignmentTarget::Tuple(_) => {
                return Err(Error::Unsupported(
                    "tuple assignment is not supported".into(),
                ))
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

    let matches = mutation_matches(db, vindex, &def, &qualifier, selection, None).await?;
    let affected = matches.len() as u64;

    // Stored generated columns are recomputed after each update.
    let generated: Vec<(usize, Expr)> = if def.has_col_meta() {
        let mut v = Vec::new();
        for i in 0..def.schema.columns.len() {
            if let Some(g) = def.meta(i).generated {
                v.push((i, parse_scalar_expr(&g)?));
            }
        }
        v
    } else {
        Vec::new()
    };

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();
    let mut fk_parent_changes: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
    let check_uniq = index::has_unique(&def);
    let check_fk = !def.foreign_keys.is_empty();
    let mut uniq_batch: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
    let checks = parse_checks(&def)?;

    let utrigs = catalog::load_triggers(db, &name).await?;
    let before_upd: Vec<catalog::TriggerDef> = utrigs
        .iter()
        .filter(|t| t.before && t.event == catalog::TrigEvent::Update)
        .cloned()
        .collect();
    let after_upd: Vec<catalog::TriggerDef> = utrigs
        .iter()
        .filter(|t| !t.before && t.event == catalog::TrigEvent::Update)
        .cloned()
        .collect();
    for (old_key, old_row) in matches {
        let mut new_row = old_row.clone();
        for (idx, expr) in &sets {
            // Assignment RHS may reference existing column values.
            let v = predicate::eval_row(expr, &def.schema, &old_row)?;
            let col = &def.schema.columns[*idx];
            new_row[*idx] = coerce(v, &col.ty, &col.name)?;
        }
        for (i, ge) in &generated {
            let v = predicate::eval_row(ge, &def.schema, &new_row)?;
            let col = &def.schema.columns[*i];
            new_row[*i] = coerce(v, &col.ty, &col.name)?;
        }
        for t in &before_upd {
            apply_before_trigger(t, &def.schema, &mut new_row, Some(&old_row))?;
        }

        for (i, col) in def.schema.columns.iter().enumerate() {
            if !col.nullable && new_row[i].is_null() {
                return Err(Error::Query(format!(
                    "column '{}' cannot be NULL",
                    col.name
                )));
            }
        }
        check_row(&def, &checks, &new_row)?;

        // If the primary key changed, the clustered key moves.
        let new_key = if def.has_pk() {
            let pk_vals: Vec<Value> = def.pk_cols.iter().map(|&i| new_row[i].clone()).collect();
            data_key(
                &name,
                &keyenc::encode_key_coll(&pk_vals, &def.pk_collations())?,
            )
        } else {
            old_key.clone()
        };

        if check_uniq || check_fk {
            uniq_batch.push((new_key.clone(), new_row.clone()));
        }

        // Index maintenance: drop old entries, write new ones. Deletes are
        // applied before puts, so unchanged index entries survive.
        deletes.extend(index::entry_keys_for_row(&def, &old_row, &old_key)?);
        let new_index_entries = index::entries_for_row(&def, &new_row, &new_key)?;
        if new_key != old_key {
            deletes.push(old_key);
        }
        let encoded = bincode::serialize(&new_row).map_err(|e| Error::Storage(e.to_string()))?;
        fk_parent_changes.push((old_row, new_row.clone()));
        puts.push((new_key, encoded));
        puts.extend(new_index_entries);
    }

    if check_uniq {
        check_unique_batch(db, &def, &uniq_batch).await?;
    }
    if check_fk {
        check_fk_batch(db, &def, &uniq_batch).await?;
    }

    // Parent-side ON UPDATE referential actions for children referencing a
    // changed key (RESTRICT/CASCADE/SET NULL, single level).
    let mut wcounts: Vec<String> = vec![name.clone()];
    cascade_parent_update(
        db,
        &def,
        &fk_parent_changes,
        &mut puts,
        &mut deletes,
        &mut wcounts,
    )
    .await?;
    for t in wcounts {
        puts.push(bump_wcount(db, &t).await?);
    }
    db.commit_write(puts, deletes).await?;
    if !after_upd.is_empty() {
        for (old_row, new_row) in &fk_parent_changes {
            queue_after(db, &after_upd, &def.schema, Some(new_row), Some(old_row))?;
        }
    }
    Ok(QueryResult::Affected(affected))
}

pub async fn delete(db: &Session, vindex: &VectorRegistry, del: &Delete) -> Result<QueryResult> {
    let relations = match &del.from {
        FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
    };
    // Multi-table DELETE: a join in FROM, or explicit target tables.
    if relations.len() != 1 || !relations[0].joins.is_empty() || !del.tables.is_empty() {
        return multi_delete(db, vindex, del, relations).await;
    }
    let name = table_of(&relations[0])?;
    let def = catalog::load(db, &name).await?;
    let qualifier = factor_qualifier(&relations[0].relation).unwrap_or_else(|| name.clone());

    let limit = match &del.limit {
        Some(e) => Some(eval_usize(e)?),
        None => None,
    };

    let matches =
        mutation_matches(db, vindex, &def, &qualifier, del.selection.as_ref(), limit).await?;
    let affected = matches.len() as u64;

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();
    let mut wcounts: Vec<String> = vec![name.clone()];

    // Foreign keys referencing this table: RESTRICT / CASCADE / SET NULL.
    cascade_parent_delete(db, &def, &matches, &mut puts, &mut deletes, &mut wcounts).await?;

    let after_del: Vec<catalog::TriggerDef> = catalog::load_triggers(db, &name)
        .await?
        .into_iter()
        .filter(|t| !t.before && t.event == catalog::TrigEvent::Delete)
        .collect();
    let mut deleted_rows: Vec<Vec<Value>> = Vec::new();
    for (key, row) in matches {
        if !after_del.is_empty() {
            deleted_rows.push(row.clone());
        }
        deletes.extend(index::entry_keys_for_row(&def, &row, &key)?);
        deletes.push(key);
    }
    for t in wcounts {
        puts.push(bump_wcount(db, &t).await?);
    }
    db.commit_write(puts, deletes).await?;
    for row in &deleted_rows {
        queue_after(db, &after_del, &def.schema, None, Some(row))?;
    }
    Ok(QueryResult::Affected(affected))
}

/// Tables (other than `parent`) that declare a foreign key referencing it.
async fn referencing_children(db: &Session, parent: &str) -> Result<Vec<TableDef>> {
    let mut out = Vec::new();
    for t in catalog::list_tables(db).await? {
        if t.eq_ignore_ascii_case(parent) {
            continue;
        }
        let def = catalog::load(db, &t).await?;
        if def
            .foreign_keys
            .iter()
            .any(|fk| fk.ref_table.eq_ignore_ascii_case(parent))
        {
            out.push(def);
        }
    }
    Ok(out)
}

/// Apply referential actions for deleted `parent` rows: block on RESTRICT/NO
/// ACTION, delete child rows on CASCADE, or null their FK columns on SET NULL.
/// (Single level — cascades do not currently recurse into grandchildren.)
async fn cascade_parent_delete(
    db: &Session,
    parent: &TableDef,
    matches: &[(Vec<u8>, Vec<Value>)],
    puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    deletes: &mut Vec<Vec<u8>>,
    wcounts: &mut Vec<String>,
) -> Result<()> {
    let children = referencing_children(db, &parent.name).await?;
    if children.is_empty() || matches.is_empty() {
        return Ok(());
    }
    for child in &children {
        let mut touched = false;
        for fk in &child
            .foreign_keys
            .iter()
            .filter(|fk| fk.ref_table.eq_ignore_ascii_case(&parent.name))
            .cloned()
            .collect::<Vec<_>>()
        {
            for (_, prow) in matches {
                let refvals: Vec<Value> = fk
                    .ref_columns
                    .iter()
                    .filter_map(|rc| {
                        parent
                            .schema
                            .columns
                            .iter()
                            .position(|c| c.name.eq_ignore_ascii_case(rc))
                            .map(|i| prow[i].clone())
                    })
                    .collect();
                if refvals.len() != fk.ref_columns.len() || refvals.iter().any(|v| v.is_null()) {
                    continue;
                }
                let child_rows = lookup_child_rows(db, child, &fk.columns, &refvals).await?;
                if child_rows.is_empty() {
                    continue;
                }
                match fk.on_delete {
                    RefAction::Cascade => {
                        for (ck, crow) in child_rows {
                            deletes.extend(index::entry_keys_for_row(child, &crow, &ck)?);
                            deletes.push(ck);
                        }
                        touched = true;
                    }
                    RefAction::SetNull => {
                        for (ck, crow) in child_rows {
                            let mut nrow = crow.clone();
                            for &fc in &fk.columns {
                                nrow[fc] = Value::Null;
                            }
                            deletes.extend(index::entry_keys_for_row(child, &crow, &ck)?);
                            let enc = bincode::serialize(&nrow)
                                .map_err(|e| Error::Storage(e.to_string()))?;
                            puts.push((ck.clone(), enc));
                            puts.extend(index::entries_for_row(child, &nrow, &ck)?);
                        }
                        touched = true;
                    }
                    _ => {
                        return Err(Error::ForeignKey(format!(
                            "cannot delete from '{}': rows in '{}' reference it (constraint '{}')",
                            parent.name, child.name, fk.name
                        )));
                    }
                }
            }
        }
        if touched {
            wcounts.push(child.name.clone());
        }
    }
    Ok(())
}

/// Apply ON UPDATE referential actions when a parent's referenced key changes.
/// `changes` are `(old_row, new_row)` pairs for the updated parent rows.
/// (Single level — does not recurse into grandchildren.)
async fn cascade_parent_update(
    db: &Session,
    parent: &TableDef,
    changes: &[(Vec<Value>, Vec<Value>)],
    puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    deletes: &mut Vec<Vec<u8>>,
    wcounts: &mut Vec<String>,
) -> Result<()> {
    let children = referencing_children(db, &parent.name).await?;
    if children.is_empty() || changes.is_empty() {
        return Ok(());
    }
    let refvals = |row: &[Value], fk: &ForeignKey| -> Option<Vec<Value>> {
        let vals: Vec<Value> = fk
            .ref_columns
            .iter()
            .filter_map(|rc| {
                parent
                    .schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(rc))
                    .map(|i| row[i].clone())
            })
            .collect();
        (vals.len() == fk.ref_columns.len()).then_some(vals)
    };
    for child in &children {
        let mut touched = false;
        for fk in child
            .foreign_keys
            .iter()
            .filter(|fk| fk.ref_table.eq_ignore_ascii_case(&parent.name))
        {
            for (old_row, new_row) in changes {
                let (Some(oldv), Some(newv)) = (refvals(old_row, fk), refvals(new_row, fk)) else {
                    continue;
                };
                // Only act when the referenced key actually changed.
                if oldv.iter().zip(&newv).all(|(a, b)| a == b) || oldv.iter().any(|v| v.is_null()) {
                    continue;
                }
                let child_rows = lookup_child_rows(db, child, &fk.columns, &oldv).await?;
                if child_rows.is_empty() {
                    continue;
                }
                match fk.on_update {
                    RefAction::Cascade | RefAction::SetNull => {
                        let set_null = matches!(fk.on_update, RefAction::SetNull);
                        for (ck, crow) in child_rows {
                            let mut nrow = crow.clone();
                            for (k, &fc) in fk.columns.iter().enumerate() {
                                nrow[fc] = if set_null {
                                    Value::Null
                                } else {
                                    newv[k].clone()
                                };
                            }
                            deletes.extend(index::entry_keys_for_row(child, &crow, &ck)?);
                            let enc = bincode::serialize(&nrow)
                                .map_err(|e| Error::Storage(e.to_string()))?;
                            puts.push((ck.clone(), enc));
                            puts.extend(index::entries_for_row(child, &nrow, &ck)?);
                        }
                        touched = true;
                    }
                    _ => {
                        return Err(Error::ForeignKey(format!(
                            "cannot update '{}': rows in '{}' reference it (constraint '{}')",
                            parent.name, child.name, fk.name
                        )));
                    }
                }
            }
        }
        if touched {
            wcounts.push(child.name.clone());
        }
    }
    Ok(())
}

/// Child rows whose columns `cols` equal `vals` (via an index if present, else
/// a scan).
async fn lookup_child_rows(
    db: &Session,
    child: &TableDef,
    cols: &[usize],
    vals: &[Value],
) -> Result<Vec<(Vec<u8>, Vec<Value>)>> {
    // Prefer an index on exactly these columns.
    if let Some(idx) = child
        .indexes
        .iter()
        .find(|ix| !ix.vector && ix.cols == cols)
    {
        let data_keys = index::lookup_eq(db, &child.name, idx, vals).await?;
        let blobs = db.multi_get(data_keys.clone()).await?;
        let mut out = Vec::new();
        for (k, b) in data_keys.into_iter().zip(blobs) {
            if let Some(bytes) = b {
                out.push((
                    k,
                    bincode::deserialize(&bytes).map_err(|e| Error::Storage(e.to_string()))?,
                ));
            }
        }
        return Ok(out);
    }
    // Fallback: scan the child and filter.
    let all = collect_matches(db, child, None, None).await?;
    Ok(all
        .into_iter()
        .filter(|(_, row)| {
            cols.iter()
                .zip(vals)
                .all(|(&c, v)| row[c].compare(v) == Some(std::cmp::Ordering::Equal))
        })
        .collect())
}

/// A plain table participating in a multi-table mutation, plus the combined
/// schema indices of its columns (in base-table order).
struct TargetInfo {
    name: String,
    def: TableDef,
    col_idx: Vec<usize>,
}

/// Map each plain table in `from` (by qualifier) to its base definition and the
/// combined-schema indices of its columns.
async fn collect_targets(
    db: &Session,
    from: &[TableWithJoins],
    schema: &Schema,
) -> Result<std::collections::HashMap<String, TargetInfo>> {
    let mut factors: Vec<&TableFactor> = Vec::new();
    for twj in from {
        factors.push(&twj.relation);
        for j in &twj.joins {
            factors.push(&j.relation);
        }
    }
    let mut map = std::collections::HashMap::new();
    for tf in factors {
        if let TableFactor::Table { name, alias, .. } = tf {
            let tname = name
                .0
                .last()
                .map(|i| i.value.clone())
                .ok_or_else(|| Error::Catalog("empty table name".into()))?;
            let qual = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| tname.clone());
            let def = catalog::load(db, &tname).await?;
            let mut col_idx = Vec::with_capacity(def.schema.columns.len());
            for c in &def.schema.columns {
                let qn = format!("{qual}.{}", c.name);
                let i = schema
                    .columns
                    .iter()
                    .position(|sc| sc.name.eq_ignore_ascii_case(&qn))
                    .ok_or_else(|| Error::Query(format!("column {qn} not found in join output")))?;
                col_idx.push(i);
            }
            map.insert(
                qual.to_ascii_lowercase(),
                TargetInfo {
                    name: tname,
                    def,
                    col_idx,
                },
            );
        }
    }
    Ok(map)
}

fn extract_base_row(joined: &[Value], col_idx: &[usize]) -> Vec<Value> {
    col_idx.iter().map(|&i| joined[i].clone()).collect()
}

/// Multi-table UPDATE: `UPDATE t1 JOIN t2 ON ... SET t1.c = ... WHERE ...`.
async fn multi_update(
    db: &Session,
    vindex: &VectorRegistry,
    table: &TableWithJoins,
    assignments: &[Assignment],
    selection: Option<&Expr>,
) -> Result<QueryResult> {
    let from = std::slice::from_ref(table);
    let (cols, rows) = build_from(db, vindex, from, &[]).await?;
    let schema = Schema::new(cols);
    let filter = match selection {
        Some(f) => Some(resolve_subqueries(db, vindex, f.clone()).await?),
        None => None,
    };
    let targets = collect_targets(db, from, &schema).await?;
    let primary = factor_qualifier(&table.relation).map(|q| q.to_ascii_lowercase());

    struct SetOp<'a> {
        qual: String,
        col: usize,
        expr: &'a Expr,
    }
    let mut sets: Vec<SetOp> = Vec::new();
    for a in assignments {
        let n = match &a.target {
            AssignmentTarget::ColumnName(n) => n,
            AssignmentTarget::Tuple(_) => {
                return Err(Error::Unsupported(
                    "tuple assignment is not supported".into(),
                ))
            }
        };
        let (qual, colname) = if n.0.len() >= 2 {
            (
                n.0[n.0.len() - 2].value.to_ascii_lowercase(),
                n.0.last().unwrap().value.clone(),
            )
        } else {
            (
                primary.clone().ok_or_else(|| {
                    Error::Query("cannot resolve target table for assignment".into())
                })?,
                n.0.last().unwrap().value.clone(),
            )
        };
        let info = targets
            .get(&qual)
            .ok_or_else(|| Error::Catalog(format!("unknown table in UPDATE: {qual}")))?;
        if !info.def.has_pk() {
            return Err(Error::Unsupported(
                "multi-table UPDATE requires a primary key on the target table".into(),
            ));
        }
        let col = info
            .def
            .schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&colname))
            .ok_or_else(|| Error::Catalog(format!("unknown column: {colname}")))?;
        sets.push(SetOp {
            qual,
            col,
            expr: &a.value,
        });
    }

    // Per target table: pk -> (old base row, new base row). A base row hit by
    // multiple joined rows is updated once (first match).
    type RowMap = std::collections::HashMap<Vec<u8>, (Vec<Value>, Vec<Value>)>;
    let mut updated: std::collections::HashMap<String, RowMap> = std::collections::HashMap::new();
    let mut affected = 0u64;
    for joined in rows {
        if let Some(f) = &filter {
            if !predicate::matches(f, &schema, &joined)? {
                continue;
            }
        }
        for (qual, info) in &targets {
            if !sets.iter().any(|s| &s.qual == qual) {
                continue;
            }
            let base = extract_base_row(&joined, &info.col_idx);
            let pk_vals: Vec<Value> = info.def.pk_cols.iter().map(|&i| base[i].clone()).collect();
            let pk_key = data_key(
                &info.name,
                &keyenc::encode_key_coll(&pk_vals, &info.def.pk_collations())?,
            );
            let entry = updated.entry(qual.clone()).or_default();
            if entry.contains_key(&pk_key) {
                continue;
            }
            let mut new_base = base.clone();
            for s in &sets {
                if &s.qual == qual {
                    let v = predicate::eval_row(s.expr, &schema, &joined)?;
                    let col = &info.def.schema.columns[s.col];
                    new_base[s.col] = coerce(v, &col.ty, &col.name)?;
                }
            }
            for (i, col) in info.def.schema.columns.iter().enumerate() {
                if !col.nullable && new_base[i].is_null() {
                    return Err(Error::Query(format!(
                        "column '{}' cannot be NULL",
                        col.name
                    )));
                }
            }
            entry.insert(pk_key, (base, new_base));
            affected += 1;
        }
    }

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();
    for (qual, rowsmap) in &updated {
        let info = &targets[qual];
        for (pk_key, (old_base, new_base)) in rowsmap {
            let new_pk: Vec<Value> = info
                .def
                .pk_cols
                .iter()
                .map(|&i| new_base[i].clone())
                .collect();
            let new_key = data_key(
                &info.name,
                &keyenc::encode_key_coll(&new_pk, &info.def.pk_collations())?,
            );
            deletes.extend(index::entry_keys_for_row(&info.def, old_base, pk_key)?);
            let new_entries = index::entries_for_row(&info.def, new_base, &new_key)?;
            if &new_key != pk_key {
                deletes.push(pk_key.clone());
            }
            let enc = bincode::serialize(new_base).map_err(|e| Error::Storage(e.to_string()))?;
            puts.push((new_key, enc));
            puts.extend(new_entries);
        }
        puts.push(bump_wcount(db, &info.name).await?);
    }
    db.commit_write(puts, deletes).await?;
    Ok(QueryResult::Affected(affected))
}

/// Multi-table DELETE: `DELETE t1 FROM t1 JOIN t2 ON ... WHERE ...`.
async fn multi_delete(
    db: &Session,
    vindex: &VectorRegistry,
    del: &Delete,
    relations: &[TableWithJoins],
) -> Result<QueryResult> {
    let mut from_all: Vec<TableWithJoins> = relations.to_vec();
    if let Some(using) = &del.using {
        from_all.extend(using.clone());
    }
    let (cols, rows) = build_from(db, vindex, &from_all, &[]).await?;
    let schema = Schema::new(cols);
    let filter = match &del.selection {
        Some(f) => Some(resolve_subqueries(db, vindex, f.clone()).await?),
        None => None,
    };
    let targets = collect_targets(db, &from_all, &schema).await?;

    let del_quals: Vec<String> = if del.tables.is_empty() {
        vec![factor_qualifier(&relations[0].relation)
            .map(|q| q.to_ascii_lowercase())
            .ok_or_else(|| Error::Query("no target table for DELETE".into()))?]
    } else {
        del.tables
            .iter()
            .filter_map(|t| t.0.last().map(|i| i.value.to_ascii_lowercase()))
            .collect()
    };
    for q in &del_quals {
        let info = targets
            .get(q)
            .ok_or_else(|| Error::Catalog(format!("unknown table in DELETE: {q}")))?;
        if !info.def.has_pk() {
            return Err(Error::Unsupported(
                "multi-table DELETE requires a primary key on the target table".into(),
            ));
        }
    }

    let mut per_table: std::collections::HashMap<
        String,
        std::collections::HashMap<Vec<u8>, Vec<Value>>,
    > = std::collections::HashMap::new();
    let mut affected = 0u64;
    for joined in rows {
        if let Some(f) = &filter {
            if !predicate::matches(f, &schema, &joined)? {
                continue;
            }
        }
        for q in &del_quals {
            let info = &targets[q];
            let base = extract_base_row(&joined, &info.col_idx);
            let pk_vals: Vec<Value> = info.def.pk_cols.iter().map(|&i| base[i].clone()).collect();
            let pk_key = data_key(
                &info.name,
                &keyenc::encode_key_coll(&pk_vals, &info.def.pk_collations())?,
            );
            let entry = per_table.entry(q.clone()).or_default();
            if entry.insert(pk_key, base).is_none() {
                affected += 1;
            }
        }
    }

    let mut deletes: Vec<Vec<u8>> = Vec::new();
    let mut wcs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for (q, rowsmap) in &per_table {
        let info = &targets[q];
        for (pk_key, base) in rowsmap {
            deletes.extend(index::entry_keys_for_row(&info.def, base, pk_key)?);
            deletes.push(pk_key.clone());
        }
        wcs.push(bump_wcount(db, &info.name).await?);
    }
    db.commit_write(wcs, deletes).await?;
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
        other => Err(Error::Query(format!(
            "expected non-negative integer, got {other:?}"
        ))),
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
                let is_column = schema
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(name));
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
                names.push(
                    ident_name(e)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| e.to_string()),
                );
                projs.push(Proj::Expr(e));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                names.push(alias.value.clone());
                projs.push(Proj::Expr(expr));
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "projection item not supported: {other}"
                )))
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

    // Infer output column types: from the source column when the projection is
    // a column reference (join-aware), else from the first non-NULL value.
    let mut cols = Vec::with_capacity(projs.len());
    for (ci, (name, p)) in names.iter().zip(&projs).enumerate() {
        let ty = match p {
            Proj::Col(i) => schema.columns[*i].ty.clone(),
            Proj::Expr(e) => {
                match col_ref_name(e).and_then(|n| predicate::resolve_index(&n, schema).ok()) {
                    Some(idx) => schema.columns[idx].ty.clone(),
                    None => out_rows
                        .iter()
                        .map(|r| &r[ci])
                        .find(|v| !v.is_null())
                        .map(infer_val)
                        .unwrap_or(ColumnType::Text),
                }
            }
        };
        cols.push(ColumnDef {
            name: name.clone(),
            ty,
            nullable: true,
            collation: elyra_core::Collation::Ci,
        });
    }

    Ok((Schema::new(cols), out_rows))
}

/// The (qualified) name of a plain column reference, if `e` is one.
fn col_ref_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => Some(
            parts
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => None,
    }
}

fn infer_val(v: &Value) -> ColumnType {
    match v {
        Value::Bool(_) => ColumnType::Bool,
        Value::Int(_) => ColumnType::Int,
        Value::Float(_) => ColumnType::Float,
        Value::Bytes(_) => ColumnType::Bytes,
        Value::Vector(x) => ColumnType::Vector(x.len() as u32),
        Value::Date(_) => ColumnType::Date,
        Value::DateTime(_) => ColumnType::DateTime,
        Value::Decimal(_, s) => ColumnType::Decimal(38, *s),
        Value::Time(_) => ColumnType::Time,
        Value::Json(_) => ColumnType::Json,
        _ => ColumnType::Text,
    }
}

/// Apply a `HAVING` clause to aggregated output rows. Aggregate expressions
/// and columns in `HAVING` are matched to output columns by their SELECT-list
/// text or alias, then evaluated against each output row.
fn apply_having(
    having: Option<&Expr>,
    projection: &[sqlparser::ast::SelectItem],
    schema: &Schema,
    rows: Vec<Vec<Value>>,
) -> Result<Vec<Vec<Value>>> {
    use sqlparser::ast::SelectItem;
    let Some(h) = having else { return Ok(rows) };

    // Map each SELECT-list expression's text to its output column name.
    let mut map: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for (item, col) in projection.iter().zip(&schema.columns) {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => continue,
        };
        map.insert(expr.to_string(), col.name.clone());
    }

    // Rewrite HAVING so aggregate/column expressions reference output columns.
    let rewritten = map_expr(h, &|e| {
        map.get(&e.to_string())
            .map(|n| Expr::Identifier(sqlparser::ast::Ident::new(n.clone())))
    });

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if predicate::matches(&rewritten, schema, &row)? {
            out.push(row);
        }
    }
    Ok(out)
}

/// True if any subquery in `filter` references `outer.<col>` (i.e. correlates
/// with the outer query). Correlated references must be qualified with the
/// outer table name/alias.
fn filter_correlated(filter: &Expr, outer: &str) -> bool {
    let found = std::cell::Cell::new(false);
    let check = |e: &Expr| -> Option<Expr> {
        if let Expr::Subquery(q)
        | Expr::InSubquery { subquery: q, .. }
        | Expr::Exists { subquery: q, .. } = e
        {
            if query_refs_qualifier(q, outer) {
                found.set(true);
            }
        }
        None
    };
    let _ = map_expr(filter, &check);
    found.get()
}

/// True if any expression in `q` (recursively) is a `qualifier.<col>` reference.
fn query_refs_qualifier(q: &SqlQuery, qualifier: &str) -> bool {
    let found = std::cell::Cell::new(false);
    let check = |e: &Expr| -> Option<Expr> {
        if let Expr::CompoundIdentifier(parts) = e {
            if parts
                .first()
                .map(|i| i.value.eq_ignore_ascii_case(qualifier))
                .unwrap_or(false)
            {
                found.set(true);
            }
        }
        None
    };
    let _ = rewrite_query(q, &check);
    found.get()
}

/// Expand a query's `WITH` clause by inlining each CTE as a derived table
/// wherever it is referenced in a top-level `FROM`. CTEs may reference earlier
/// CTEs in the same `WITH`. `WITH RECURSIVE` is not supported.
fn expand_ctes(query: &SqlQuery) -> Result<SqlQuery> {
    use std::collections::HashMap;
    let Some(with) = &query.with else {
        return Ok(query.clone());
    };

    let mut map: HashMap<String, SqlQuery> = HashMap::new();
    for cte in &with.cte_tables {
        // Expand this CTE's body against the CTEs defined before it.
        let body = replace_from_ctes((*cte.query).clone(), &map);
        map.insert(cte.alias.name.value.to_ascii_lowercase(), body);
    }

    let mut q = query.clone();
    q.with = None;
    Ok(replace_from_ctes(q, &map))
}

fn replace_from_ctes(
    mut query: SqlQuery,
    map: &std::collections::HashMap<String, SqlQuery>,
) -> SqlQuery {
    if let SetExpr::Select(select) = query.body.as_mut() {
        for twj in &mut select.from {
            twj.relation = replace_cte_relation(&twj.relation, map);
            for join in &mut twj.joins {
                join.relation = replace_cte_relation(&join.relation, map);
            }
        }
    }
    query
}

fn replace_cte_relation(
    tf: &TableFactor,
    map: &std::collections::HashMap<String, SqlQuery>,
) -> TableFactor {
    if let TableFactor::Table { name, alias, .. } = tf {
        if let Some(tname) = name.0.last() {
            if let Some(body) = map.get(&tname.value.to_ascii_lowercase()) {
                let al = alias.clone().unwrap_or_else(|| sqlparser::ast::TableAlias {
                    name: sqlparser::ast::Ident::new(tname.value.clone()),
                    columns: Vec::new(),
                });
                return TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(body.clone()),
                    alias: Some(al),
                };
            }
        }
    }
    tf.clone()
}

/// True if any projection item contains a window function (`f(...) OVER (...)`).
fn projection_has_window(projection: &[sqlparser::ast::SelectItem]) -> bool {
    use sqlparser::ast::SelectItem;
    projection.iter().any(|it| match it {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            expr_has_window(e)
        }
        _ => false,
    })
}

fn expr_has_window(e: &Expr) -> bool {
    let found = std::cell::Cell::new(false);
    let _ = map_expr(e, &|x| {
        if let Expr::Function(f) = x {
            if f.over.is_some() {
                found.set(true);
            }
        }
        None
    });
    found.get()
}

fn collect_window_exprs(e: &Expr, out: &mut Vec<Expr>) {
    let acc = std::cell::RefCell::new(Vec::new());
    let _ = map_expr(e, &|x| {
        if let Expr::Function(f) = x {
            if f.over.is_some() {
                acc.borrow_mut().push(x.clone());
            }
        }
        None
    });
    out.extend(acc.into_inner());
}

/// Execute a query with window functions in its projection. Materialises the
/// filtered rows, computes each window function, substitutes the results into
/// the projection, then orders/pages.
#[allow(clippy::too_many_arguments)]
async fn window_select(
    db: &Session,
    def: &TableDef,
    select: &Select,
    filter: Option<&Expr>,
    order_exprs: &[(Expr, bool)],
    offset: usize,
    limit: Option<usize>,
) -> Result<QueryResult> {
    use sqlparser::ast::SelectItem;
    let rows = scan_rows(db, def, filter).await?;
    let schema = &def.schema;

    // Precompute each window function's value per row.
    let mut win_exprs: Vec<Expr> = Vec::new();
    for item in &select.projection {
        if let SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } = item {
            collect_window_exprs(e, &mut win_exprs);
        }
    }
    let mut win_values: Vec<(Expr, Vec<Value>)> = Vec::new();
    for we in &win_exprs {
        let vals = compute_window(&rows, schema, we)?;
        win_values.push((we.clone(), vals));
    }

    // Build output rows: substitute each window result, then evaluate.
    let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let subst = |e: &Expr| -> Option<Expr> {
            win_values
                .iter()
                .find(|(we, _)| we == e)
                .map(|(_, vals)| value_to_expr(&vals[i]))
        };
        let mut vals = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            let expr = match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                other => {
                    return Err(Error::Unsupported(format!(
                        "projection item not supported with window functions: {other}"
                    )))
                }
            };
            let bound = map_expr(expr, &subst);
            vals.push(predicate::eval_row(&bound, schema, row)?);
        }
        out_rows.push(vals);
    }

    // Output schema (names + inferred types).
    let mut cols = Vec::with_capacity(select.projection.len());
    for (ci, item) in select.projection.iter().enumerate() {
        let name = match item {
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            SelectItem::UnnamedExpr(e) => ident_name(e)
                .map(|s| s.to_string())
                .unwrap_or_else(|| e.to_string()),
            _ => format!("col{ci}"),
        };
        let ty = out_rows
            .iter()
            .map(|r| &r[ci])
            .find(|v| !v.is_null())
            .map(infer_val)
            .unwrap_or(ColumnType::Text);
        cols.push(ColumnDef {
            name,
            ty,
            nullable: true,
            collation: elyra_core::Collation::Ci,
        });
    }
    let out_schema = Schema::new(cols);
    order_output_rows(&mut out_rows, &out_schema, order_exprs)?;
    apply_offset_limit(&mut out_rows, offset, limit);
    Ok(QueryResult::Rows(RowStream::literal(out_schema, out_rows)))
}

/// Compute a window function's value for every input row (indexed by original
/// position). Supports ROW_NUMBER/RANK/DENSE_RANK, SUM/COUNT/AVG/MIN/MAX (as
/// running aggregates when ordered, else over the whole partition), and
/// LAG/LEAD.
fn compute_window(rows: &[Vec<Value>], schema: &Schema, func: &Expr) -> Result<Vec<Value>> {
    let Expr::Function(f) = func else {
        return Err(Error::Unsupported("expected a window function".into()));
    };
    let name = f
        .name
        .0
        .last()
        .map(|i| i.value.to_ascii_lowercase())
        .unwrap_or_default();
    let spec = match &f.over {
        Some(sqlparser::ast::WindowType::WindowSpec(s)) => s,
        _ => return Err(Error::Unsupported("named windows are not supported".into())),
    };
    let args = fn_arg_exprs(f);

    // Partition rows (preserving first-seen order), then sort each partition.
    let mut partitions: Vec<(String, Vec<usize>)> = Vec::new();
    let mut index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let mut key = String::new();
        for p in &spec.partition_by {
            key.extend(
                predicate::eval_row(p, schema, row)?
                    .collation_key()
                    .iter()
                    .map(|b| *b as char),
            );
            key.push('\u{1}');
        }
        let slot = *index.entry(key.clone()).or_insert_with(|| {
            partitions.push((key, Vec::new()));
            partitions.len() - 1
        });
        partitions[slot].1.push(i);
    }

    let order: Vec<(Expr, bool)> = spec
        .order_by
        .iter()
        .map(|o| (o.expr.clone(), o.asc.unwrap_or(true)))
        .collect();
    let ordered = !order.is_empty();

    let mut result = vec![Value::Null; rows.len()];
    for (_, mut idxs) in partitions {
        if ordered {
            let key_of = |i: usize| -> Result<Vec<Value>> {
                order
                    .iter()
                    .map(|(e, _)| predicate::eval_row(e, schema, &rows[i]))
                    .collect()
            };
            let mut keyed: Vec<(Vec<Value>, usize)> = idxs
                .iter()
                .map(|&i| Ok((key_of(i)?, i)))
                .collect::<Result<_>>()?;
            keyed.sort_by(|a, b| cmp_order_keys(&a.0, &b.0, &order));
            idxs = keyed.iter().map(|(_, i)| *i).collect();
        }

        compute_partition(
            &name,
            &args,
            rows,
            schema,
            &idxs,
            ordered,
            &order,
            spec.window_frame.as_ref(),
            &mut result,
        )?;
    }
    Ok(result)
}

fn cmp_order_keys(a: &[Value], b: &[Value], order: &[(Expr, bool)]) -> std::cmp::Ordering {
    for (i, (_, asc)) in order.iter().enumerate() {
        let o = a[i].total_cmp(&b[i]);
        let o = if *asc { o } else { o.reverse() };
        if o != std::cmp::Ordering::Equal {
            return o;
        }
    }
    std::cmp::Ordering::Equal
}

#[allow(clippy::too_many_arguments)]
fn compute_partition(
    name: &str,
    args: &[&Expr],
    rows: &[Vec<Value>],
    schema: &Schema,
    idxs: &[usize],
    ordered: bool,
    order: &[(Expr, bool)],
    frame: Option<&sqlparser::ast::WindowFrame>,
    result: &mut [Value],
) -> Result<()> {
    let order_key = |i: usize| -> Result<Vec<Value>> {
        order
            .iter()
            .map(|(e, _)| predicate::eval_row(e, schema, &rows[i]))
            .collect()
    };
    let arg_val = |i: usize| -> Result<Value> {
        match args.first() {
            Some(e) => predicate::eval_row(e, schema, &rows[i]),
            None => Ok(Value::Null),
        }
    };

    match name {
        "row_number" => {
            for (pos, &i) in idxs.iter().enumerate() {
                result[i] = Value::Int(pos as i64 + 1);
            }
        }
        "rank" | "dense_rank" => {
            let dense = name == "dense_rank";
            let mut rank = 0i64;
            let mut prev: Option<Vec<Value>> = None;
            for (pos, &i) in idxs.iter().enumerate() {
                let key = order_key(i)?;
                if prev.as_ref() != Some(&key) {
                    rank = if dense { rank + 1 } else { pos as i64 + 1 };
                    prev = Some(key);
                }
                result[i] = Value::Int(rank);
            }
        }
        "lag" | "lead" => {
            let off = args
                .get(1)
                .and_then(|e| predicate::eval_row(e, schema, &rows[idxs[0]]).ok())
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0) as isize;
            let default = match args.get(2) {
                Some(e) => predicate::eval_row(e, schema, &rows[idxs[0]])?,
                None => Value::Null,
            };
            for (pos, &i) in idxs.iter().enumerate() {
                let target = if name == "lag" {
                    pos as isize - off
                } else {
                    pos as isize + off
                };
                result[i] = if target >= 0 && (target as usize) < idxs.len() {
                    arg_val(idxs[target as usize])?
                } else {
                    default.clone()
                };
            }
        }
        "sum" | "count" | "avg" | "min" | "max" => {
            let count_star = name == "count" && args.is_empty();
            let arg0 = args.first().copied();
            let n = idxs.len();

            match frame_mode(frame, ordered)? {
                FrameMode::Rows => {
                    let f = frame.expect("rows frame present");
                    for (p, &i) in idxs.iter().enumerate() {
                        let (lo, hi) = rows_bounds(f, p, n, schema, rows, idxs)?;
                        let members: &[usize] = if lo <= hi { &idxs[lo..=hi] } else { &[] };
                        result[i] = window_agg(name, count_star, members, arg0, rows, schema)?;
                    }
                }
                FrameMode::Whole => {
                    let agg = window_agg(name, count_star, idxs, arg0, rows, schema)?;
                    for &i in idxs {
                        result[i] = agg.clone();
                    }
                }
                FrameMode::PeerRunning => {
                    let mut p = 0;
                    while p < n {
                        let key = order_key(idxs[p])?;
                        let mut q = p;
                        while q < n && order_key(idxs[q])? == key {
                            q += 1;
                        }
                        let agg = window_agg(name, count_star, &idxs[0..q], arg0, rows, schema)?;
                        for &i in &idxs[p..q] {
                            result[i] = agg.clone();
                        }
                        p = q;
                    }
                }
            }
        }
        other => {
            return Err(Error::Unsupported(format!(
                "window function not supported: {other}"
            )))
        }
    }
    Ok(())
}

enum FrameMode {
    Rows,
    Whole,
    PeerRunning,
}

/// Decide how to evaluate a framed aggregate. Explicit `ROWS` frames use
/// physical offsets; `RANGE` supports whole-partition and running (peer) forms;
/// the default frame is running when ordered, else whole partition.
fn frame_mode(frame: Option<&sqlparser::ast::WindowFrame>, ordered: bool) -> Result<FrameMode> {
    use sqlparser::ast::{WindowFrameBound as B, WindowFrameUnits as U};
    let Some(f) = frame else {
        return Ok(if ordered {
            FrameMode::PeerRunning
        } else {
            FrameMode::Whole
        });
    };
    match f.units {
        U::Rows => Ok(FrameMode::Rows),
        U::Range | U::Groups => {
            let whole = matches!(f.start_bound, B::Preceding(None))
                && matches!(f.end_bound, Some(B::Following(None)));
            let running = matches!(f.start_bound, B::Preceding(None))
                && matches!(f.end_bound, None | Some(B::CurrentRow));
            if whole {
                Ok(FrameMode::Whole)
            } else if running && ordered {
                Ok(FrameMode::PeerRunning)
            } else if !ordered {
                Ok(FrameMode::Whole)
            } else {
                Err(Error::Unsupported(
                    "only RANGE UNBOUNDED PRECEDING .. CURRENT ROW / UNBOUNDED FOLLOWING frames are supported"
                        .into(),
                ))
            }
        }
    }
}

/// Physical `[lo, hi]` bounds (inclusive, clamped) for a `ROWS` frame at sorted
/// position `p`. Returns `lo > hi` for an empty frame.
fn rows_bounds(
    frame: &sqlparser::ast::WindowFrame,
    p: usize,
    n: usize,
    schema: &Schema,
    rows: &[Vec<Value>],
    idxs: &[usize],
) -> Result<(usize, usize)> {
    use sqlparser::ast::WindowFrameBound as B;
    let off = |b: &B| -> Result<isize> {
        Ok(match b {
            B::CurrentRow => p as isize,
            B::Preceding(None) => 0,
            B::Preceding(Some(e)) => p as isize - const_isize(e, schema, rows, idxs)?,
            B::Following(None) => n as isize - 1,
            B::Following(Some(e)) => p as isize + const_isize(e, schema, rows, idxs)?,
        })
    };
    let lo = off(&frame.start_bound)?.max(0) as usize;
    let hi_raw = match frame.end_bound.as_ref() {
        Some(b) => off(b)?,
        None => p as isize,
    };
    let hi = hi_raw.min(n as isize - 1);
    if hi < 0 || lo as isize > hi {
        return Ok((1, 0)); // empty
    }
    Ok((lo, hi as usize))
}

fn const_isize(e: &Expr, schema: &Schema, rows: &[Vec<Value>], idxs: &[usize]) -> Result<isize> {
    let v = predicate::eval_row(e, schema, &rows[idxs[0]])?;
    Ok(v.as_f64().unwrap_or(0.0) as isize)
}

/// Aggregate `name` over the given member rows (evaluating `arg` per row).
fn window_agg(
    name: &str,
    count_star: bool,
    members: &[usize],
    arg: Option<&Expr>,
    rows: &[Vec<Value>],
    schema: &Schema,
) -> Result<Value> {
    if count_star {
        return Ok(Value::Int(members.len() as i64));
    }
    let vals: Vec<Value> = match arg {
        Some(e) => members
            .iter()
            .map(|&i| predicate::eval_row(e, schema, &rows[i]))
            .collect::<Result<_>>()?,
        None => Vec::new(),
    };
    Ok(agg_over(name, &vals, members.len()))
}

fn agg_over(name: &str, vals: &[Value], count_star: usize) -> Value {
    match name {
        "count" => Value::Int(vals.iter().filter(|v| !v.is_null()).count() as i64),
        "sum" | "avg" => {
            let nums: Vec<f64> = vals.iter().filter_map(|v| v.as_f64()).collect();
            if nums.is_empty() {
                return Value::Null;
            }
            let sum: f64 = nums.iter().sum();
            if name == "avg" {
                Value::Float(sum / nums.len() as f64)
            } else if vals
                .iter()
                .all(|v| matches!(v, Value::Int(_) | Value::Null))
            {
                Value::Int(sum as i64)
            } else {
                Value::Float(sum)
            }
        }
        "min" => vals
            .iter()
            .filter(|v| !v.is_null())
            .min_by(|a, b| a.total_cmp(b))
            .cloned()
            .unwrap_or(Value::Null),
        "max" => vals
            .iter()
            .filter(|v| !v.is_null())
            .max_by(|a, b| a.total_cmp(b))
            .cloned()
            .unwrap_or(Value::Null),
        _ => {
            let _ = count_star;
            Value::Null
        }
    }
}

/// `CREATE VIEW name [(cols)] AS SELECT ...` — store the view's SELECT text.
pub async fn create_view(
    db: &Session,
    name: &ObjectName,
    columns: &[sqlparser::ast::ViewColumnDef],
    query: &SqlQuery,
    or_replace: bool,
) -> Result<QueryResult> {
    let name = table_ident(name)?;
    if catalog::exists(db, &name).await? {
        return Err(Error::Catalog(format!(
            "cannot create view: a table named '{name}' exists"
        )));
    }
    if !or_replace && catalog::load_view(db, &name).await?.is_some() {
        return Err(Error::Catalog(format!("view already exists: {name}")));
    }

    // Apply an explicit column list by aliasing the projection.
    let mut q = query.clone();
    if !columns.is_empty() {
        if let SetExpr::Select(select) = q.body.as_mut() {
            use sqlparser::ast::SelectItem;
            if select.projection.len() != columns.len() {
                return Err(Error::Query(
                    "view column count does not match the query".into(),
                ));
            }
            for (item, col) in select.projection.iter_mut().zip(columns.iter()) {
                let expr = match item {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        e.clone()
                    }
                    _ => {
                        return Err(Error::Unsupported(
                            "view column list requires explicit projection expressions".into(),
                        ))
                    }
                };
                *item = SelectItem::ExprWithAlias {
                    expr,
                    alias: col.name.clone(),
                };
            }
        }
    }

    db.commit_write(
        vec![(catalog::view_key(&name), q.to_string().into_bytes())],
        vec![],
    )
    .await?;
    Ok(QueryResult::empty_ok())
}

pub async fn drop_view(db: &Session, name: &str, if_exists: bool) -> Result<QueryResult> {
    if catalog::load_view(db, name).await?.is_none() {
        if if_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Catalog(format!("no such view: {name}")));
    }
    db.commit_write(vec![], vec![catalog::view_key(name)])
        .await?;
    Ok(QueryResult::empty_ok())
}

/// Replace references to views in a query's `FROM` with derived tables backed
/// by the view's stored SELECT. Nested views expand when the derived subquery
/// is itself executed.
async fn expand_views(db: &Session, query: &SqlQuery) -> Result<SqlQuery> {
    let mut q = query.clone();
    if let SetExpr::Select(select) = q.body.as_mut() {
        for twj in &mut select.from {
            expand_view_factor(db, &mut twj.relation).await?;
            for j in &mut twj.joins {
                expand_view_factor(db, &mut j.relation).await?;
            }
        }
    }
    Ok(q)
}

async fn expand_view_factor(db: &Session, tf: &mut TableFactor) -> Result<()> {
    if let TableFactor::Table { name, alias, .. } = tf {
        if let Some(last) = name.0.last() {
            if let Some(sql) = catalog::load_view(db, &last.value).await? {
                let vq = parse_query(&sql)?;
                let al = alias.clone().unwrap_or_else(|| sqlparser::ast::TableAlias {
                    name: sqlparser::ast::Ident::new(last.value.clone()),
                    columns: Vec::new(),
                });
                *tf = TableFactor::Derived {
                    lateral: false,
                    subquery: Box::new(vq),
                    alias: Some(al),
                };
            }
        }
    }
    Ok(())
}

/// True if the query's top-level FROM has any plain table reference (a possible
/// view). Cheap gate to avoid catalog lookups on view-free queries.
fn from_has_plain_table(query: &SqlQuery) -> bool {
    if let SetExpr::Select(select) = query.body.as_ref() {
        select.from.iter().any(|twj| {
            matches!(twj.relation, TableFactor::Table { .. })
                || twj
                    .joins
                    .iter()
                    .any(|j| matches!(j.relation, TableFactor::Table { .. }))
        })
    } else {
        false
    }
}

fn parse_query(sql: &str) -> Result<SqlQuery> {
    let dialect = sqlparser::dialect::MySqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, sql)
        .map_err(|e| Error::Parse(e.to_string()))?;
    match stmts.into_iter().next() {
        Some(sqlparser::ast::Statement::Query(q)) => Ok(*q),
        _ => Err(Error::Query("view definition is not a query".into())),
    }
}

/// Parse a scalar expression (for stored defaults / generated columns).
fn parse_scalar_expr(sql: &str) -> Result<Expr> {
    use sqlparser::ast::SelectItem;
    let q = parse_query(&format!("SELECT {sql}"))?;
    if let SetExpr::Select(sel) = q.body.as_ref() {
        match sel.projection.first() {
            Some(SelectItem::UnnamedExpr(e)) | Some(SelectItem::ExprWithAlias { expr: e, .. }) => {
                return Ok(e.clone())
            }
            _ => {}
        }
    }
    Err(Error::Query(format!("cannot parse expression: {sql}")))
}

async fn read_autoinc(db: &Session, table: &str) -> Result<i64> {
    Ok(match db.get(autoinc_key(table)).await? {
        Some(bytes) if bytes.len() == 8 => {
            i64::from_le_bytes(bytes.try_into().expect("checked length"))
        }
        _ => 0,
    })
}

static TEMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn unique_temp_name(base: &str) -> String {
    let n = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let clean: String = base.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    format!("__cte_{n}_{clean}")
}

/// Execute a `WITH RECURSIVE` query. Each CTE is materialised into a temporary
/// relation (recursive ones by fixpoint iteration); references are rewritten to
/// the temp relations; the outer query is then run and the temps dropped.
async fn execute_recursive_cte(
    db: &Session,
    vindex: &VectorRegistry,
    query: &SqlQuery,
) -> Result<QueryResult> {
    let with = query.with.as_ref().expect("with present");
    let mut temp_names: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut created: Vec<String> = Vec::new();

    let result = async {
        for cte in &with.cte_tables {
            let cname = cte.alias.name.value.clone();
            let temp = unique_temp_name(&cname);
            // Rewrite references to earlier CTEs in this body.
            let body = rewrite_table_refs((*cte.query).clone(), &temp_names);
            let alias_cols: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|c| c.name.value.clone())
                .collect();

            if query_refs_table(&body, &cname) {
                materialize_recursive(db, vindex, &temp, &cname, &body, &alias_cols).await?;
            } else {
                let (schema, rows) = run_subquery_schema(db, vindex, &body).await?;
                let schema = apply_col_aliases(schema, &alias_cols);
                create_temp_table(db, &temp, &schema).await?;
                fill_table(db, &temp, &schema, &rows).await?;
            }
            created.push(temp.clone());
            temp_names.insert(cname.to_ascii_lowercase(), temp);
        }

        // Run the outer query against the temp relations, fully materialised.
        let mut outer = query.clone();
        outer.with = None;
        let outer = rewrite_table_refs(outer, &temp_names);
        run_subquery_schema(db, vindex, &outer).await
    }
    .await;

    // Always drop the temporary relations.
    for t in &created {
        let _ = drop_table(db, t, true).await;
    }

    let (schema, rows) = result?;
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// Fixpoint materialisation of a recursive CTE into temp table `temp`.
async fn materialize_recursive(
    db: &Session,
    vindex: &VectorRegistry,
    temp: &str,
    cname: &str,
    body: &SqlQuery,
    alias_cols: &[String],
) -> Result<()> {
    const MAX_ITERS: usize = 1000;
    let (distinct, anchor_q, rec_q) = split_recursive(body, cname)?;

    let (schema, anchor_rows) = run_subquery_schema(db, vindex, &anchor_q).await?;
    let schema = apply_col_aliases(schema, alias_cols);
    create_temp_table(db, temp, &schema).await?;

    let row_key = |r: &[Value]| -> Vec<u8> { Value::row_collation_key(r) };
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    let mut frontier: Vec<Vec<Value>> = Vec::new();
    for r in anchor_rows {
        if !distinct || seen.insert(row_key(&r)) {
            all_rows.push(r.clone());
            frontier.push(r);
        }
    }

    // Rewrite the recursive term's self-reference to the temp relation.
    let mut self_map = std::collections::HashMap::new();
    self_map.insert(cname.to_ascii_lowercase(), temp.to_string());
    let rec_q = rewrite_table_refs(rec_q, &self_map);

    let mut iters = 0;
    while !frontier.is_empty() {
        iters += 1;
        if iters > MAX_ITERS {
            return Err(Error::Query(format!(
                "recursive CTE '{cname}' exceeded {MAX_ITERS} iterations"
            )));
        }
        // The recursive term sees only the previous iteration's rows.
        clear_table(db, temp).await?;
        fill_table(db, temp, &schema, &frontier).await?;

        let new_rows = run_subquery(db, vindex, &rec_q).await?;
        let mut fresh: Vec<Vec<Value>> = Vec::new();
        for r in new_rows {
            if !distinct || seen.insert(row_key(&r)) {
                fresh.push(r);
            }
        }
        if fresh.is_empty() {
            break;
        }
        all_rows.extend(fresh.iter().cloned());
        frontier = fresh;
    }

    // Final contents: the full accumulated set.
    clear_table(db, temp).await?;
    fill_table(db, temp, &schema, &all_rows).await?;
    Ok(())
}

/// Split a recursive CTE body `anchor UNION [ALL] recursive` into its parts.
/// Returns `(distinct, anchor_query, recursive_query)`.
fn split_recursive(body: &SqlQuery, cname: &str) -> Result<(bool, SqlQuery, SqlQuery)> {
    use sqlparser::ast::{SetOperator, SetQuantifier};
    let SetExpr::SetOperation {
        op: SetOperator::Union,
        set_quantifier,
        left,
        right,
    } = body.body.as_ref()
    else {
        return Err(Error::Unsupported(
            "recursive CTE must be an anchor UNION [ALL] recursive query".into(),
        ));
    };
    let distinct = !matches!(
        set_quantifier,
        SetQuantifier::All | SetQuantifier::AllByName
    );

    let wrap = |b: &SetExpr| -> SqlQuery {
        let mut q = body.clone();
        q.body = Box::new(b.clone());
        q.with = None;
        q
    };
    let left_rec = setexpr_refs_table(left, cname);
    let right_rec = setexpr_refs_table(right, cname);
    match (left_rec, right_rec) {
        (false, true) => Ok((distinct, wrap(left), wrap(right))),
        (true, false) => Ok((distinct, wrap(right), wrap(left))),
        _ => Err(Error::Unsupported(
            "recursive CTE must have exactly one self-referencing branch".into(),
        )),
    }
}

/// Rename plain table references matching a CTE name to its temp relation,
/// aliased back to the CTE name so `cte.col` references keep resolving.
fn rewrite_table_refs(
    mut query: SqlQuery,
    map: &std::collections::HashMap<String, String>,
) -> SqlQuery {
    fn fix(tf: &mut TableFactor, map: &std::collections::HashMap<String, String>) {
        if let TableFactor::Table { name, alias, .. } = tf {
            if let Some(last) = name.0.last() {
                if let Some(temp) = map.get(&last.value.to_ascii_lowercase()) {
                    let orig = last.value.clone();
                    *name = ObjectName(vec![sqlparser::ast::Ident::new(temp.clone())]);
                    if alias.is_none() {
                        *alias = Some(sqlparser::ast::TableAlias {
                            name: sqlparser::ast::Ident::new(orig),
                            columns: Vec::new(),
                        });
                    }
                }
            }
        }
    }
    fn walk(body: &mut SetExpr, map: &std::collections::HashMap<String, String>) {
        match body {
            SetExpr::Select(s) => {
                for twj in &mut s.from {
                    fix(&mut twj.relation, map);
                    for j in &mut twj.joins {
                        fix(&mut j.relation, map);
                    }
                }
            }
            SetExpr::SetOperation { left, right, .. } => {
                walk(left, map);
                walk(right, map);
            }
            SetExpr::Query(q) => walk(&mut q.body, map),
            _ => {}
        }
    }
    walk(&mut query.body, map);
    query
}

fn query_refs_table(query: &SqlQuery, name: &str) -> bool {
    setexpr_refs_table(&query.body, name)
}

fn setexpr_refs_table(body: &SetExpr, name: &str) -> bool {
    match body {
        SetExpr::Select(s) => s.from.iter().any(|twj| {
            factor_refs(&twj.relation, name)
                || twj.joins.iter().any(|j| factor_refs(&j.relation, name))
        }),
        SetExpr::SetOperation { left, right, .. } => {
            setexpr_refs_table(left, name) || setexpr_refs_table(right, name)
        }
        SetExpr::Query(q) => setexpr_refs_table(&q.body, name),
        _ => false,
    }
}

fn factor_refs(tf: &TableFactor, name: &str) -> bool {
    matches!(tf, TableFactor::Table { name: n, .. }
        if n.0.last().is_some_and(|i| i.value.eq_ignore_ascii_case(name)))
}

fn apply_col_aliases(mut schema: Schema, alias_cols: &[String]) -> Schema {
    if !alias_cols.is_empty() {
        for (col, new) in schema.columns.iter_mut().zip(alias_cols.iter()) {
            col.name = new.clone();
        }
    }
    schema
}

async fn create_temp_table(db: &Session, name: &str, schema: &Schema) -> Result<()> {
    let def = TableDef {
        name: name.to_string(),
        schema: schema.clone(),
        pk_cols: Vec::new(),
        indexes: Vec::new(),
        col_meta: Vec::new(),
        checks: Vec::new(),
        foreign_keys: Vec::new(),
    };
    db.commit_write(vec![(catalog_key(name), def.encode()?)], vec![])
        .await?;
    Ok(())
}

async fn fill_table(db: &Session, name: &str, schema: &Schema, rows: &[Vec<Value>]) -> Result<()> {
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(rows.len() + 1);
    let mut rowid = read_rowid(db, name).await?;
    for r in rows {
        rowid += 1;
        let mut row = vec![Value::Null; schema.columns.len()];
        for (i, col) in schema.columns.iter().enumerate() {
            if let Some(v) = r.get(i) {
                row[i] = coerce(v.clone(), &col.ty, &col.name)?;
            }
        }
        let encoded = bincode::serialize(&row).map_err(|e| Error::Storage(e.to_string()))?;
        puts.push((data_key(name, &keyenc::encode_rowid(rowid)), encoded));
    }
    puts.push((rowid_key(name), rowid.to_le_bytes().to_vec()));
    db.commit_write(puts, vec![]).await?;
    Ok(())
}

async fn clear_table(db: &Session, name: &str) -> Result<()> {
    let prefix = data_prefix(name);
    let mut deletes = vec![rowid_key(name)];
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
    db.commit_write(vec![], deletes).await?;
    Ok(())
}

/// True if a projection contains any subquery (scalar/IN/EXISTS).
fn projection_has_subquery(projection: &[sqlparser::ast::SelectItem]) -> bool {
    use sqlparser::ast::SelectItem;
    projection.iter().any(|it| match it {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            expr_has_subquery(e)
        }
        _ => false,
    })
}

fn expr_has_subquery(e: &Expr) -> bool {
    let found = std::cell::Cell::new(false);
    let _ = map_expr(e, &|x| {
        if matches!(
            x,
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. }
        ) {
            found.set(true);
        }
        None
    });
    found.get()
}

/// True if a projection item references `outer.<col>` inside a subquery.
fn projection_correlated(projection: &[sqlparser::ast::SelectItem], outer: &str) -> bool {
    use sqlparser::ast::SelectItem;
    projection.iter().any(|it| match it {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            filter_correlated(e, outer)
        }
        _ => false,
    })
}

/// Resolve the subqueries in a projection item's expression (uncorrelated).
async fn resolve_item(
    db: &Session,
    vindex: &VectorRegistry,
    item: &sqlparser::ast::SelectItem,
) -> Result<sqlparser::ast::SelectItem> {
    use sqlparser::ast::SelectItem;
    Ok(match item {
        SelectItem::UnnamedExpr(e) => {
            SelectItem::UnnamedExpr(resolve_subqueries(db, vindex, e.clone()).await?)
        }
        SelectItem::ExprWithAlias { expr, alias } => SelectItem::ExprWithAlias {
            expr: resolve_subqueries(db, vindex, expr.clone()).await?,
            alias: alias.clone(),
        },
        other => other.clone(),
    })
}

/// Evaluate a query whose WHERE has a correlated subquery: materialise the
/// outer rows, and for each row bind outer column references (qualified with
/// `outer`, or bare columns of the outer table) into the subqueries, resolve
/// them, and test the predicate.
#[allow(clippy::too_many_arguments)]
async fn correlated_select(
    db: &Session,
    vindex: &VectorRegistry,
    select: &Select,
    def: &TableDef,
    outer: &str,
    corr_filter: &Expr,
    group_by: &[Expr],
    order_exprs: &[(Expr, bool)],
    offset: usize,
    limit: Option<usize>,
) -> Result<QueryResult> {
    let all = scan_rows(db, def, None).await?;
    let mut matched: Vec<Vec<Value>> = Vec::new();

    for row in all {
        let bound = bind_outer(corr_filter, outer, &def.schema, &row);
        let resolved = resolve_subqueries(db, vindex, bound).await?;
        if predicate::matches(&resolved, &def.schema, &row)? {
            matched.push(row);
        }
    }

    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        let (schema, out) = aggregate::run(&def.schema, &select.projection, group_by, matched)?;
        let mut out = apply_having(select.having.as_ref(), &select.projection, &schema, out)?;
        order_output_rows(&mut out, &schema, order_exprs)?;
        apply_offset_limit(&mut out, offset, limit);
        return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
    }

    let resolved = resolve_order_aliases(order_exprs, &select.projection, &def.schema);
    if !resolved.is_empty() {
        sort_full_rows(&mut matched, &def.schema, &resolved)?;
    }
    apply_offset_limit(&mut matched, offset, limit);

    // No SELECT-list subqueries: plain projection.
    if !projection_has_subquery(&select.projection) {
        let (schema, out) = project_exprs(&select.projection, &def.schema, &matched)?;
        return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
    }

    // Correlated SELECT-list subqueries: resolve the projection per row.
    use sqlparser::ast::SelectItem;
    let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(matched.len());
    for row in &matched {
        let mut vals = Vec::with_capacity(select.projection.len());
        for item in &select.projection {
            let expr = match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                other => {
                    return Err(Error::Unsupported(format!(
                        "projection item not supported with correlated subquery: {other}"
                    )))
                }
            };
            let bound = bind_outer(expr, outer, &def.schema, row);
            let resolved = resolve_subqueries(db, vindex, bound).await?;
            vals.push(predicate::eval_row(&resolved, &def.schema, row)?);
        }
        out_rows.push(vals);
    }

    // Output schema: names from the projection, types from the first row.
    let mut cols = Vec::with_capacity(select.projection.len());
    for (ci, item) in select.projection.iter().enumerate() {
        let name = match item {
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            SelectItem::UnnamedExpr(e) => ident_name(e)
                .map(|s| s.to_string())
                .unwrap_or_else(|| e.to_string()),
            _ => format!("col{ci}"),
        };
        let ty = out_rows
            .first()
            .map(|r| infer_val(&r[ci]))
            .unwrap_or(ColumnType::Text);
        cols.push(ColumnDef {
            name,
            ty,
            nullable: true,
            collation: elyra_core::Collation::Ci,
        });
    }
    Ok(QueryResult::Rows(RowStream::literal(
        Schema::new(cols),
        out_rows,
    )))
}

/// Rewrite outer column references (`outer.col`, or a bare outer column) in
/// `expr` to literals from `row`, including inside subqueries.
fn bind_outer(expr: &Expr, outer: &str, schema: &Schema, row: &[Value]) -> Expr {
    map_expr(expr, &|e| match e {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let tbl = &parts[parts.len() - 2].value;
            let col = &parts[parts.len() - 1].value;
            if tbl.eq_ignore_ascii_case(outer) {
                schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col))
                    .map(|i| value_to_expr(&row[i]))
            } else {
                None
            }
        }
        Expr::Identifier(id) => schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&id.value))
            .map(|i| value_to_expr(&row[i])),
        _ => None,
    })
}

/// Materialise a subquery's rows by executing it through the query engine.
async fn run_subquery(
    db: &Session,
    vindex: &VectorRegistry,
    q: &SqlQuery,
) -> Result<Vec<Vec<Value>>> {
    // Boxed to break the select -> resolve -> run -> select async cycle.
    match Box::pin(select(db, vindex, q)).await? {
        QueryResult::Rows(mut stream) => {
            let mut rows = Vec::new();
            loop {
                let batch = stream.next_batch(4096).await?;
                if batch.is_empty() {
                    break;
                }
                rows.extend(batch);
            }
            Ok(rows)
        }
        QueryResult::Affected(_) => Ok(Vec::new()),
    }
}

/// Execute a top-level set operation (`UNION`/`INTERSECT`/`EXCEPT`), applying
/// the outer query's `ORDER BY` and `LIMIT`/`OFFSET` to the combined result.
async fn execute_set_query(
    db: &Session,
    vindex: &VectorRegistry,
    query: &SqlQuery,
) -> Result<QueryResult> {
    use sqlparser::ast::{SetOperator, SetQuantifier};
    let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = query.body.as_ref()
    else {
        return Err(Error::Unsupported("expected a set operation".into()));
    };

    let wrap = |b: &SetExpr| -> SqlQuery {
        let mut q = query.clone();
        q.body = Box::new(b.clone());
        q.with = None;
        q.order_by = None;
        q.limit = None;
        q.offset = None;
        q
    };

    let (schema, mut left_rows) = run_subquery_schema(db, vindex, &wrap(left)).await?;
    let right_rows = run_subquery(db, vindex, &wrap(right)).await?;

    let all = matches!(
        set_quantifier,
        SetQuantifier::All | SetQuantifier::AllByName
    );
    let key = |r: &[Value]| -> Vec<u8> { Value::row_collation_key(r) };

    let mut out: Vec<Vec<Value>> = Vec::new();
    match op {
        SetOperator::Union => {
            if all {
                out = left_rows;
                out.extend(right_rows);
            } else {
                let mut seen = std::collections::HashSet::new();
                for r in left_rows.into_iter().chain(right_rows) {
                    if seen.insert(key(&r)) {
                        out.push(r);
                    }
                }
            }
        }
        SetOperator::Intersect => {
            let rset: std::collections::HashSet<Vec<u8>> =
                right_rows.iter().map(|r| key(r)).collect();
            let mut seen = std::collections::HashSet::new();
            for r in left_rows {
                let k = key(&r);
                if rset.contains(&k) && (all || seen.insert(k)) {
                    out.push(r);
                }
            }
        }
        SetOperator::Except => {
            let rset: std::collections::HashSet<Vec<u8>> =
                right_rows.iter().map(|r| key(r)).collect();
            let mut seen = std::collections::HashSet::new();
            for r in std::mem::take(&mut left_rows) {
                let k = key(&r);
                if !rset.contains(&k) && (all || seen.insert(k)) {
                    out.push(r);
                }
            }
        }
    }

    // Outer ORDER BY / LIMIT / OFFSET over the combined result.
    let order_exprs: Vec<(Expr, bool)> = match &query.order_by {
        Some(ob) => ob
            .exprs
            .iter()
            .map(|o| (o.expr.clone(), o.asc.unwrap_or(true)))
            .collect(),
        None => Vec::new(),
    };
    order_output_rows(&mut out, &schema, &order_exprs)?;
    let offset = match &query.offset {
        Some(o) => eval_usize(&o.value)?,
        None => 0,
    };
    let limit = match &query.limit {
        Some(e) => Some(eval_usize(e)?),
        None => None,
    };
    apply_offset_limit(&mut out, offset, limit);
    Ok(QueryResult::Rows(RowStream::literal(schema, out)))
}

/// Execute a subquery and return both its schema and rows (for derived tables).
async fn run_subquery_schema(
    db: &Session,
    vindex: &VectorRegistry,
    q: &SqlQuery,
) -> Result<(Schema, Vec<Vec<Value>>)> {
    match Box::pin(select(db, vindex, q)).await? {
        QueryResult::Rows(mut stream) => {
            let schema = stream.schema.clone();
            let mut rows = Vec::new();
            loop {
                let batch = stream.next_batch(4096).await?;
                if batch.is_empty() {
                    break;
                }
                rows.extend(batch);
            }
            Ok((schema, rows))
        }
        QueryResult::Affected(_) => Ok((Schema::new(Vec::new()), Vec::new())),
    }
}

fn value_to_expr(v: &Value) -> Expr {
    use sqlparser::ast::Value as V;
    let lit = match v {
        Value::Null => V::Null,
        Value::Bool(b) => V::Boolean(*b),
        Value::Int(i) => V::Number(i.to_string(), false),
        Value::Float(f) => V::Number(f.to_string(), false),
        Value::Decimal(..) | Value::Date(_) | Value::DateTime(_) | Value::Time(_) => {
            V::SingleQuotedString(v.to_wire_string().unwrap_or_default())
        }
        Value::Text(s) | Value::Json(s) => V::SingleQuotedString(s.clone()),
        Value::Bytes(_) | Value::Vector(_) => {
            V::SingleQuotedString(v.to_wire_string().unwrap_or_default())
        }
    };
    Expr::Value(lit)
}

/// Recursively replace uncorrelated subqueries in `expr` with literals:
/// scalar `(SELECT ...)` -> value, `x IN (SELECT ...)` -> `x IN (list)`,
/// `EXISTS (SELECT ...)` -> boolean. Correlated subqueries are not supported
/// (the inner query is executed standalone).
fn resolve_subqueries<'a>(
    db: &'a Session,
    vindex: &'a VectorRegistry,
    expr: Expr,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Expr>> + Send + 'a>> {
    Box::pin(async move {
        Ok(match expr {
            Expr::Subquery(q) => {
                let rows = run_subquery(db, vindex, &q).await?;
                let v = rows
                    .first()
                    .and_then(|r| r.first())
                    .cloned()
                    .unwrap_or(Value::Null);
                value_to_expr(&v)
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let inner = resolve_subqueries(db, vindex, *expr).await?;
                let rows = run_subquery(db, vindex, &subquery).await?;
                let list = rows
                    .iter()
                    .filter_map(|r| r.first())
                    .map(value_to_expr)
                    .collect();
                Expr::InList {
                    expr: Box::new(inner),
                    list,
                    negated,
                }
            }
            Expr::Exists { subquery, negated } => {
                let rows = run_subquery(db, vindex, &subquery).await?;
                Expr::Value(sqlparser::ast::Value::Boolean(rows.is_empty() == negated))
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(resolve_subqueries(db, vindex, *left).await?),
                op,
                right: Box::new(resolve_subqueries(db, vindex, *right).await?),
            },
            Expr::UnaryOp { op, expr } => Expr::UnaryOp {
                op,
                expr: Box::new(resolve_subqueries(db, vindex, *expr).await?),
            },
            Expr::Nested(e) => Expr::Nested(Box::new(resolve_subqueries(db, vindex, *e).await?)),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(resolve_subqueries(db, vindex, *expr).await?),
                negated,
                low: Box::new(resolve_subqueries(db, vindex, *low).await?),
                high: Box::new(resolve_subqueries(db, vindex, *high).await?),
            },
            other => other,
        })
    })
}

/// Rewrite every expression in `expr`, including those nested inside
/// subqueries, by applying `f` (which may replace a node). Used to bind outer
/// column references for correlated subqueries.
fn map_expr(expr: &Expr, f: &dyn Fn(&Expr) -> Option<Expr>) -> Expr {
    if let Some(r) = f(expr) {
        return r;
    }
    match expr {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(map_expr(left, f)),
            op: op.clone(),
            right: Box::new(map_expr(right, f)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(map_expr(expr, f)),
        },
        Expr::Nested(e) => Expr::Nested(Box::new(map_expr(e, f))),
        Expr::IsNull(e) => Expr::IsNull(Box::new(map_expr(e, f))),
        Expr::IsNotNull(e) => Expr::IsNotNull(Box::new(map_expr(e, f))),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(map_expr(expr, f)),
            negated: *negated,
            low: Box::new(map_expr(low, f)),
            high: Box::new(map_expr(high, f)),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(map_expr(expr, f)),
            list: list.iter().map(|e| map_expr(e, f)).collect(),
            negated: *negated,
        },
        Expr::Subquery(q) => Expr::Subquery(Box::new(rewrite_query(q, f))),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(map_expr(expr, f)),
            subquery: Box::new(rewrite_query(subquery, f)),
            negated: *negated,
        },
        Expr::Exists { subquery, negated } => Expr::Exists {
            subquery: Box::new(rewrite_query(subquery, f)),
            negated: *negated,
        },
        Expr::Function(func) => {
            let mut func = func.clone();
            if let sqlparser::ast::FunctionArguments::List(list) = &mut func.args {
                for arg in &mut list.args {
                    if let sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(e),
                    ) = arg
                    {
                        *e = map_expr(e, f);
                    }
                }
            }
            Expr::Function(func)
        }
        other => other.clone(),
    }
}

/// Apply `map_expr` to the expression positions of a query (projection, WHERE,
/// JOIN conditions, GROUP BY, HAVING, ORDER BY), recursing into subqueries.
fn rewrite_query(q: &SqlQuery, f: &dyn Fn(&Expr) -> Option<Expr>) -> SqlQuery {
    let mut q = q.clone();
    if let SetExpr::Select(select) = q.body.as_mut() {
        for item in &mut select.projection {
            match item {
                sqlparser::ast::SelectItem::UnnamedExpr(e)
                | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                    *e = map_expr(e, f);
                }
                _ => {}
            }
        }
        if let Some(sel) = &select.selection {
            select.selection = Some(map_expr(sel, f));
        }
        if let Some(h) = &select.having {
            select.having = Some(map_expr(h, f));
        }
        for twj in &mut select.from {
            for join in &mut twj.joins {
                if let sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(e))
                | sqlparser::ast::JoinOperator::LeftOuter(
                    sqlparser::ast::JoinConstraint::On(e),
                )
                | sqlparser::ast::JoinOperator::RightOuter(
                    sqlparser::ast::JoinConstraint::On(e),
                )
                | sqlparser::ast::JoinOperator::FullOuter(
                    sqlparser::ast::JoinConstraint::On(e),
                ) = &mut join.join_operator
                {
                    *e = map_expr(e, f);
                }
            }
        }
    }
    q
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
    let Expr::Function(f) = &resolved[0].0 else {
        return Ok(None);
    };
    let name = f
        .name
        .0
        .last()
        .map(|i| i.value.to_ascii_lowercase())
        .unwrap_or_default();
    // Only the L2 family is accelerated (HNSW is built with L2).
    if !matches!(
        name.as_str(),
        "vec_distance" | "vec_l2_distance" | "vec_distance_l2"
    ) {
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
    let FunctionArguments::List(list) = &f.args else {
        return Vec::new();
    };
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
    def.schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}

fn parse_vec_free(s: &str) -> Result<Vec<f32>> {
    let inner = s.trim().trim_start_matches('[').trim_end_matches(']');
    inner
        .split(',')
        .filter(|t| !t.trim().is_empty())
        .map(|t| {
            t.trim()
                .parse::<f32>()
                .map_err(|_| Error::Vector(format!("bad vector element: {t}")))
        })
        .collect()
}

/// Produce the `(key, value)` put that advances a table's write counter.
async fn bump_wcount(db: &Session, table: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let next = read_wcount(db, table).await? + 1;
    Ok((wcount_key(table), next.to_le_bytes().to_vec()))
}

/// Aggregate a single table into a [`GroupAggregator`]. Uses the index fast
/// path for an accelerable equality filter, otherwise parallel streaming.
async fn olap_aggregate(
    db: &Session,
    def: &TableDef,
    filter: Option<Expr>,
    plan: &AggPlan,
) -> Result<GroupAggregator> {
    if let Some(f) = &filter {
        // Equality or range on a PK/indexed column: aggregate just the matching
        // rows fetched via the index, rather than scanning the whole table.
        if accelerable(def, Some(f))? {
            let rows = collect_matches(db, def, Some(f), None).await?;
            let mut agg = plan.new_aggregator();
            let extend = !plan.arg_exprs().is_empty();
            for (_, row) in rows {
                if extend {
                    agg.feed(&plan.extend_row(&row)?);
                } else {
                    agg.feed(&row);
                }
            }
            return Ok(agg);
        }
    }
    parallel_aggregate(db, def, filter, plan).await
}

/// Partitioned, spill-to-disk aggregation used when the in-memory aggregation
/// overflows the group cap. Rows are routed to partitions by group-key hash and
/// spilled to temp files; each partition is then aggregated independently in
/// bounded memory. Returns finalized output rows.
async fn partitioned_aggregate(
    db: &Session,
    def: &TableDef,
    filter: Option<Expr>,
    plan: &AggPlan,
) -> Result<(Schema, Vec<Vec<Value>>)> {
    const PARTS: usize = 256;
    let budget = crate::sort::sort_max_rows();
    let group_cols = plan.group_cols().to_vec();
    let mut parts = crate::aggspill::Partitions::new(PARTS, budget);

    // Phase 1: scan and route rows to partitions (spilling past the budget).
    let prefix = data_prefix(&def.name);
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 8192).await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 8192;
        cursor = batch.last().map(|(k, _)| k.clone());
        for (_, v) in batch {
            let row: Vec<Value> =
                bincode::deserialize(&v).map_err(|e| Error::Storage(e.to_string()))?;
            if let Some(f) = &filter {
                if !predicate::matches(f, &def.schema, &row)? {
                    continue;
                }
            }
            let gk: Vec<Value> = group_cols
                .iter()
                .map(|&c| row.get(c).cloned().unwrap_or(Value::Null))
                .collect();
            let p = crate::aggspill::partition_of(&Value::row_collation_key(&gk), PARTS);
            parts.route(p, row)?;
        }
        if last {
            break;
        }
    }

    // Phase 2: aggregate each partition independently and concatenate.
    let extend = !plan.arg_exprs().is_empty();
    let mut out_rows: Vec<Vec<Value>> = Vec::new();
    let mut schema: Option<Schema> = None;
    for p in 0..parts.len() {
        let mut agg = plan.new_aggregator();
        parts.drain_each(p, |row| {
            if extend {
                agg.feed(&plan.extend_row(&row)?);
            } else {
                agg.feed(&row);
            }
            Ok(())
        })?;
        if agg.overflowed() {
            return Err(Error::Query(format!(
                "GROUP BY partition still exceeds the group limit ({}); raise \
                 ELYRASQL_GROUP_MAX_GROUPS",
                elyra_olap::default_max_groups()
            )));
        }
        let (s, rows) = plan.finalize(agg);
        schema = Some(s);
        out_rows.extend(rows);
    }
    let schema = schema.unwrap_or_else(|| plan.finalize(plan.new_aggregator()).0);
    Ok((schema, out_rows))
}

/// Scan the table in batches and aggregate them across worker threads, merging
/// partial aggregators. Memory is bounded by (workers x batch), independent of
/// table size — the core OLAP property.
async fn parallel_aggregate(
    db: &Session,
    def: &TableDef,
    filter: Option<Expr>,
    plan: &AggPlan,
) -> Result<GroupAggregator> {
    const BATCH: usize = 8192;
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let prefix = data_prefix(&def.name);
    let schema = def.schema.clone();

    let mut cursor: Option<Vec<u8>> = None;
    let mut result = plan.new_aggregator();
    let mut handles = Vec::new();

    loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), BATCH).await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < BATCH;
        cursor = batch.last().map(|(k, _)| k.clone());
        let blobs: Vec<Vec<u8>> = batch.into_iter().map(|(_, v)| v).collect();

        let mut worker = plan.new_aggregator();
        let f = filter.clone();
        let sch = schema.clone();
        let arg_exprs = plan.arg_exprs().to_vec();
        handles.push(tokio::task::spawn_blocking(
            move || -> Result<GroupAggregator> {
                for b in &blobs {
                    let row: Vec<Value> =
                        bincode::deserialize(b).map_err(|e| Error::Storage(e.to_string()))?;
                    let keep = match &f {
                        Some(e) => predicate::matches(e, &sch, &row)?,
                        None => true,
                    };
                    if keep {
                        if arg_exprs.is_empty() {
                            worker.feed(&row);
                        } else {
                            let mut r = row.clone();
                            for e in &arg_exprs {
                                r.push(predicate::eval_row(e, &sch, &row)?);
                            }
                            worker.feed(&r);
                        }
                    }
                }
                Ok(worker)
            },
        ));

        if handles.len() >= workers || last {
            for h in handles.drain(..) {
                let part = h
                    .await
                    .map_err(|e| Error::Analytics(format!("worker failed: {e}")))??;
                result.merge(part);
            }
        }
        if last {
            break;
        }
    }
    Ok(result)
}

/// Materialise all rows matching `filter` (drops storage keys).
async fn scan_rows(db: &Session, def: &TableDef, filter: Option<&Expr>) -> Result<Vec<Vec<Value>>> {
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
fn sort_full_rows(rows: &mut [Vec<Value>], schema: &Schema, order: &[(Expr, bool)]) -> Result<()> {
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
        let name = ident_name(e)
            .map(|s| s.to_string())
            .unwrap_or_else(|| e.to_string());
        let want = name.rsplit('.').next().unwrap_or(&name);
        let idx = schema
            .columns
            .iter()
            .position(|c| {
                let have = c.name.rsplit('.').next().unwrap_or(&c.name);
                c.name.eq_ignore_ascii_case(&name) || have.eq_ignore_ascii_case(want)
            })
            .ok_or_else(|| {
                Error::Query(format!("ORDER BY references unknown output column: {name}"))
            })?;
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

/// `ANALYZE TABLE`: count rows and persist statistics used for reporting
/// (`information_schema.tables.TABLE_ROWS`) and planning.
pub async fn analyze_table(db: &Session, name: &str) -> Result<QueryResult> {
    if !catalog::exists(db, name).await? {
        return Err(Error::Catalog(format!("no such table: {name}")));
    }
    let def = catalog::load(db, name).await?;
    let ncols = def.schema.columns.len();
    const NDV_CAP: usize = 100_000;
    let mut distinct: Vec<std::collections::HashSet<Vec<u8>>> = vec![Default::default(); ncols];
    let mut capped = vec![false; ncols];
    let mut nulls = vec![0u64; ncols];
    let mut mins: Vec<Option<Value>> = vec![None; ncols];
    let mut maxs: Vec<Option<Value>> = vec![None; ncols];

    let prefix = data_prefix(name);
    let mut cursor: Option<Vec<u8>> = None;
    let mut rows = 0u64;
    loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 8192).await?;
        if batch.is_empty() {
            break;
        }
        rows += batch.len() as u64;
        let last = batch.len() < 8192;
        cursor = batch.last().map(|(k, _)| k.clone());
        for (_, v) in &batch {
            let row: Vec<Value> =
                bincode::deserialize(v).map_err(|e| Error::Storage(e.to_string()))?;
            for (i, val) in row.iter().enumerate().take(ncols) {
                if val.is_null() {
                    nulls[i] += 1;
                    continue;
                }
                if !capped[i] {
                    if distinct[i].len() < NDV_CAP {
                        distinct[i].insert(val.collation_key());
                    } else {
                        capped[i] = true;
                    }
                }
                if mins[i]
                    .as_ref()
                    .is_none_or(|m| val.compare(m) == Some(std::cmp::Ordering::Less))
                {
                    mins[i] = Some(val.clone());
                }
                if maxs[i]
                    .as_ref()
                    .is_none_or(|m| val.compare(m) == Some(std::cmp::Ordering::Greater))
                {
                    maxs[i] = Some(val.clone());
                }
            }
        }
        if last {
            break;
        }
    }
    let columns = (0..ncols)
        .map(|i| catalog::ColStat {
            name: def.schema.columns[i].name.clone(),
            ndv: distinct[i].len() as u64,
            ndv_capped: capped[i],
            nulls: nulls[i],
            min: mins[i].as_ref().and_then(|v| v.to_wire_string()),
            max: maxs[i].as_ref().and_then(|v| v.to_wire_string()),
        })
        .collect();
    let stats = catalog::TableStats { rows, columns };
    let enc = bincode::serialize(&stats).map_err(|e| Error::Storage(e.to_string()))?;
    db.commit_write(vec![(catalog::stats_key(name), enc)], vec![])
        .await?;

    // MySQL-style ANALYZE result set.
    let schema = Schema::new(vec![
        ColumnDef::new("Table", ColumnType::Text, false),
        ColumnDef::new("Op", ColumnType::Text, false),
        ColumnDef::new("Msg_type", ColumnType::Text, false),
        ColumnDef::new("Msg_text", ColumnType::Text, false),
    ]);
    let row = vec![
        Value::Text(name.to_string()),
        Value::Text("analyze".into()),
        Value::Text("status".into()),
        Value::Text("OK".into()),
    ];
    Ok(QueryResult::Rows(RowStream::literal(schema, vec![row])))
}

/// `SHOW BINARY LOGS`: list binlog segments and their sizes.
pub async fn show_binary_logs(db: &Session) -> Result<QueryResult> {
    let handle = db.raw_db();
    let schema = Schema::new(vec![
        ColumnDef::new("Log_name", ColumnType::Text, false),
        ColumnDef::new("File_size", ColumnType::Int, false),
    ]);
    let rows = match handle.binlog_dir() {
        Some(dir) => elyra_storage::binlog::list_segments(dir)?
            .into_iter()
            .map(|(name, size)| vec![Value::Text(name), Value::Int(size as i64)])
            .collect(),
        None => Vec::new(),
    };
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// `PURGE BINARY LOGS TO '<name>'`: delete segments before `name`.
pub async fn purge_binary_logs(db: &Session, to: &str) -> Result<QueryResult> {
    let handle = db.raw_db();
    let dir = handle
        .binlog_dir()
        .ok_or_else(|| Error::Query("binary logging is not enabled".into()))?;
    let n = elyra_storage::binlog::purge(dir, to)?;
    Ok(QueryResult::Affected(n))
}

pub async fn drop_table(db: &Session, name: &str, if_exists: bool) -> Result<QueryResult> {
    if !catalog::exists(db, name).await? {
        if if_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Catalog(format!("no such table: {name}")));
    }

    // Collect the table's data and index keys in batches.
    let mut deletes = vec![catalog_key(name), rowid_key(name), autoinc_key(name)];
    for prefix in [data_prefix(name), index_table_prefix(name)] {
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
    }
    db.commit_write(vec![], deletes).await?;
    Ok(QueryResult::Affected(0))
}

async fn read_rowid(db: &Session, table: &str) -> Result<u64> {
    Ok(match db.get(rowid_key(table)).await? {
        Some(bytes) if bytes.len() == 8 => {
            u64::from_le_bytes(bytes.try_into().expect("checked length"))
        }
        _ => 0,
    })
}

/// Extract literal value rows from an `INSERT ... VALUES` source.
fn source_rows(source: &SqlQuery) -> Result<Option<&[Vec<sqlparser::ast::Expr>]>> {
    match source.body.as_ref() {
        SetExpr::Values(values) => Ok(Some(&values.rows)),
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
        (ColumnType::Date, Value::Date(d)) => Value::Date(d),
        (ColumnType::Date, Value::Text(s)) => elyra_core::datetime::parse_date(&s)
            .map(Value::Date)
            .ok_or_else(|| Error::Type(format!("invalid DATE literal: {s}")))?,
        (ColumnType::DateTime, Value::DateTime(t)) => Value::DateTime(t),
        (ColumnType::DateTime, Value::Text(s)) => elyra_core::datetime::parse_datetime(&s)
            .map(Value::DateTime)
            .ok_or_else(|| Error::Type(format!("invalid DATETIME literal: {s}")))?,
        (ColumnType::Decimal(_, sc), Value::Text(s)) => elyra_core::value::parse_decimal(&s, *sc)
            .map(|(u, s)| Value::Decimal(u, s))
            .ok_or_else(|| Error::Type(format!("invalid DECIMAL literal: {s}")))?,
        (ColumnType::Decimal(_, sc), Value::Int(i)) => {
            Value::Decimal(i as i128 * 10i128.pow(*sc as u32), *sc)
        }
        (ColumnType::Decimal(_, sc), Value::Float(f)) => {
            elyra_core::value::parse_decimal(&f.to_string(), *sc)
                .map(|(u, s)| Value::Decimal(u, s))
                .ok_or_else(|| Error::Type(format!("invalid DECIMAL value: {f}")))?
        }
        (ColumnType::Decimal(_, sc), Value::Decimal(u, s)) => {
            // Rescale to the column's declared scale.
            let v = if s <= *sc {
                u * 10i128.pow((*sc - s) as u32)
            } else {
                u / 10i128.pow((s - *sc) as u32)
            };
            Value::Decimal(v, *sc)
        }
        (ColumnType::Time, Value::Time(t)) => Value::Time(t),
        (ColumnType::Time, Value::Text(s)) => elyra_core::datetime::parse_time(&s)
            .map(Value::Time)
            .ok_or_else(|| Error::Type(format!("invalid TIME literal: {s}")))?,
        (ColumnType::Json, Value::Json(s)) => Value::Json(s),
        (ColumnType::Json, Value::Text(s)) => {
            if elyra_core::value::is_valid_json(&s) {
                Value::Json(s)
            } else {
                return Err(Error::Type(format!("invalid JSON literal: {s}")));
            }
        }
        (ColumnType::Vector(dim), Value::Text(s)) => Value::Vector(parse_vector(&s, *dim)?),
        // Lenient (MySQL-style) conversions.
        (ColumnType::Int, Value::Float(f)) => Value::Int(f as i64),
        (ColumnType::Int, Value::Text(s)) => s
            .trim()
            .parse::<i64>()
            .or_else(|_| s.trim().parse::<f64>().map(|f| f as i64))
            .map(Value::Int)
            .map_err(|_| Error::Type(format!("invalid INTEGER value: {s}")))?,
        (ColumnType::Float, Value::Text(s)) => s
            .trim()
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|_| Error::Type(format!("invalid FLOAT value: {s}")))?,
        (ColumnType::Date, Value::DateTime(m)) => Value::Date(m.div_euclid(86_400_000_000) as i32),
        (ColumnType::DateTime, Value::Date(d)) => Value::DateTime(d as i64 * 86_400_000_000),
        (ColumnType::Text, other) => Value::Text(other.to_wire_string().unwrap_or_default()),
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
        .map(|t| {
            t.trim()
                .parse::<f32>()
                .map_err(|_| Error::Type(format!("bad vector element: {t}")))
        })
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
