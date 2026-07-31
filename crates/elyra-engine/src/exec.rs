//! Statement execution over the clustered single-file store.
//!
//! Implements `CREATE TABLE`, `INSERT`, `SELECT ... FROM`, `DROP TABLE`.
//! Inserts are batched into one group-commit; scans stream.

use crate::session::Session;
use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, Assignment, AssignmentTarget, ColumnOption,
    CreateIndex, CreateTable, DataType, Delete, FromTable, Insert, JoinConstraint, JoinOperator,
    ObjectName, OrderByExpr, Query as SqlQuery, Select, SetExpr, Statement, TableConstraint,
    TableFactor, TableWithJoins,
};

use crate::aggregate;
use crate::aggregate::AggPlan;
use crate::colcache;
use crate::cpred;
use crate::index;
use crate::predicate;
use crate::rowdec;
use crate::zonemap;
use elyra_olap::GroupAggregator;

use crate::catalog::{
    self, autoinc_key, catalog_key, data_key, data_prefix, index_table_prefix,
    indexnull_table_prefix, partmeta_key, rowid_key, stats_key, wcount_key, ColMeta, ForeignKey,
    IndexDef, RefAction, TableDef,
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

fn map_collation(name: &ObjectName) -> elyra_core::Collation {
    name.0
        .last()
        .map(|identifier| elyra_core::Collation::from_name(&identifier.value))
        .unwrap_or_default()
}

/// Escape regex metacharacters so a literal string can be embedded in a pattern
/// (used to build the SET-membership CHECK).
fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if matches!(
            ch,
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
        ) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

fn map_type(dt: &DataType) -> Result<ColumnType> {
    Ok(match dt {
        DataType::TinyInt(_) if is_tinyint_bool(dt) => ColumnType::Bool,
        DataType::Bool | DataType::Boolean => ColumnType::Bool,
        DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::MediumInt(_)
        | DataType::Int(_)
        | DataType::Integer(_)
        | DataType::BigInt(_)
        | DataType::UnsignedTinyInt(_)
        | DataType::UnsignedSmallInt(_)
        | DataType::UnsignedMediumInt(_)
        | DataType::UnsignedInt(_)
        | DataType::UnsignedInteger(_) => ColumnType::Int,
        // BIGINT UNSIGNED can exceed i64::MAX, so it needs the unsigned type.
        DataType::UnsignedBigInt(_) => ColumnType::UInt,
        DataType::Float(_)
        | DataType::Real
        | DataType::Double
        | DataType::DoublePrecision
        | DataType::Float4
        | DataType::Float8 => ColumnType::Float,
        DataType::Text
        | DataType::TinyText
        | DataType::MediumText
        | DataType::LongText
        | DataType::String(_)
        | DataType::Varchar(_)
        | DataType::Nvarchar(_)
        | DataType::CharacterVarying(_)
        | DataType::Char(_) => ColumnType::Text,
        // ENUM/SET are stored as their string value.
        DataType::Enum(..) | DataType::Set(_) => ColumnType::Text,
        DataType::Blob(_)
        | DataType::TinyBlob
        | DataType::MediumBlob
        | DataType::LongBlob
        | DataType::Bytea
        | DataType::Binary(_)
        | DataType::Varbinary(_) => ColumnType::Bytes,
        // BIT(n) is stored as an integer.
        DataType::Bit(_) | DataType::BitVarying(_) => ColumnType::Int,
        DataType::Date => ColumnType::Date,
        DataType::Datetime(_) | DataType::Timestamp(_, _) => ColumnType::DateTime,
        DataType::Time(_, _) => ColumnType::Time,
        DataType::JSON | DataType::JSONB => ColumnType::Json,
        DataType::Decimal(info) | DataType::Numeric(info) | DataType::Dec(info) => {
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
        // ENUM columns are constrained to their declared members via a synthesized
        // CHECK (`col IN ('a','b',...)`), reusing the existing CHECK enforcement.
        // No on-disk format change (checks already live in TableDef); NULL passes
        // the CHECK, matching a nullable ENUM. (SET subset-membership is not yet
        // validated.)
        if let sqlparser::ast::DataType::Enum(members, _) = &col.data_type {
            let vals: Vec<String> = members
                .iter()
                .map(|m| match m {
                    sqlparser::ast::EnumMember::Name(s) => s.clone(),
                    sqlparser::ast::EnumMember::NamedValue(s, _) => s.clone(),
                })
                .collect();
            if !vals.is_empty() {
                let list = vals
                    .iter()
                    .map(|v| format!("'{}'", v.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ");
                checks.push(format!("`{}` IN ({list})", col.name.value));
            }
        }
        // SET: a value is a comma-separated subset of the declared members (or
        // empty). Validate with a synthesized REGEXP CHECK `^(m1|m2|...)(,(...))*$`
        // (plus the empty string). NULL passes the CHECK automatically.
        if let sqlparser::ast::DataType::Set(members) = &col.data_type {
            let alts: Vec<String> = members.iter().map(|m| regex_escape(m)).collect();
            if !alts.is_empty() {
                let group = alts.join("|");
                let pattern = format!("^({group})(,({group}))*$");
                let pat_sql = pattern.replace('\'', "''");
                let cn = &col.name.value;
                checks.push(format!(
                    "`{cn}` IS NULL OR `{cn}` = '' OR `{cn}` REGEXP '{pat_sql}'"
                ));
            }
        }
        let mut nullable = true;
        let mut meta = ColMeta::default();
        let collation = col
            .collation
            .as_ref()
            .map(map_collation)
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
                            indexes_nulls: true,
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
                let single = idxs.len() == 1;
                indexes.push(IndexDef {
                    name: iname,
                    cols: idxs,
                    unique: true,
                    vector: false,
                    fulltext: false,
                    col_collations: ucolls,
                    indexes_nulls: single,
                });
            }
            TableConstraint::Index {
                name: index_name,
                columns: cols,
                ..
            } => {
                let mut idxs = Vec::new();
                for ident in cols {
                    let i = columns
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(&ident.value))
                        .ok_or_else(|| {
                            Error::Catalog(format!("unknown index column: {}", ident.value))
                        })?;
                    idxs.push(i);
                }
                let name = index_name
                    .as_ref()
                    .map(|name| name.value.clone())
                    .unwrap_or_else(|| {
                        format!(
                            "{name}_{}_idx",
                            idxs.iter()
                                .map(|&i| columns[i].name.clone())
                                .collect::<Vec<_>>()
                                .join("_")
                        )
                    });
                let col_collations = idxs
                    .iter()
                    .map(|&i| columns[i].collation)
                    .collect::<Vec<_>>();
                indexes.push(IndexDef {
                    name,
                    indexes_nulls: idxs.len() == 1,
                    cols: idxs,
                    unique: false,
                    vector: false,
                    fulltext: false,
                    col_collations,
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
                        indexes_nulls: fk_cols.len() == 1,
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
        name: format!("Tables_in_{}", db.database()),
        ty: ColumnType::Text,
        nullable: false,
        collation: elyra_core::Collation::Ci,
    }]);
    let rows = names.into_iter().map(|n| vec![Value::Text(n)]).collect();
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// Build a schema of all-Text columns (for SHOW-style tabular results).
fn text_schema(names: &[&str]) -> Schema {
    Schema::new(
        names
            .iter()
            .map(|n| ColumnDef {
                name: (*n).to_string(),
                ty: ColumnType::Text,
                nullable: true,
                collation: elyra_core::Collation::Ci,
            })
            .collect(),
    )
}

/// The first base table named in a query's `FROM`, if simple.
fn explain_first_table(stmt: &sqlparser::ast::Statement) -> Option<String> {
    use sqlparser::ast::{SetExpr, Statement};
    if let Statement::Query(q) = stmt {
        if let SetExpr::Select(sel) = q.body.as_ref() {
            if let Some(t) = sel.from.first() {
                if let TableFactor::Table { name, .. } = &t.relation {
                    return name.0.last().map(|i| i.value.clone());
                }
            }
        }
    }
    None
}

/// `EXPLAIN <statement>` — a best-effort, MySQL-shaped plan row (names the first
/// base table and its estimated row count). Not a full optimizer trace.
pub async fn explain(db: &Session, stmt: &sqlparser::ast::Statement) -> Result<QueryResult> {
    let schema = text_schema(&[
        "id",
        "select_type",
        "table",
        "partitions",
        "type",
        "possible_keys",
        "key",
        "key_len",
        "ref",
        "rows",
        "filtered",
        "Extra",
    ]);
    let table = explain_first_table(stmt);
    let rows_est = match &table {
        Some(t) => catalog::load_stats(db, t)
            .await?
            .map(|s| s.rows.to_string())
            .unwrap_or_else(|| "0".into()),
        None => "0".into(),
    };
    let row = vec![
        Value::Text("1".into()),
        Value::Text("SIMPLE".into()),
        table.clone().map(Value::Text).unwrap_or(Value::Null),
        Value::Null,
        Value::Text(if table.is_some() { "ALL" } else { "" }.into()),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Text(rows_est),
        Value::Text("100.00".into()),
        Value::Text(String::new()),
    ];
    Ok(QueryResult::Rows(RowStream::literal(schema, vec![row])))
}

/// MySQL-compatible system variables reported by `SHOW VARIABLES`. ElyraSQL
/// presents as MySQL 8.0, so GUI tools and ORMs that read these on connect
/// behave (character sets, timeouts, case sensitivity, packet size, ...).
fn system_variables() -> Vec<(&'static str, String)> {
    vec![
        ("auto_increment_increment", "1".into()),
        ("autocommit", "ON".into()),
        ("character_set_client", "utf8mb4".into()),
        ("character_set_connection", "utf8mb4".into()),
        ("character_set_database", "utf8mb4".into()),
        ("character_set_results", "utf8mb4".into()),
        ("character_set_server", "utf8mb4".into()),
        ("character_set_system", "utf8mb3".into()),
        ("collation_connection", "utf8mb4_0900_ai_ci".into()),
        ("collation_database", "utf8mb4_0900_ai_ci".into()),
        ("collation_server", "utf8mb4_0900_ai_ci".into()),
        ("default_storage_engine", "InnoDB".into()),
        ("event_scheduler", "OFF".into()),
        ("foreign_key_checks", "ON".into()),
        ("have_query_cache", "NO".into()),
        ("hostname", "elyrasql".into()),
        ("init_connect", String::new()),
        ("interactive_timeout", "28800".into()),
        ("license", "MIT".into()),
        ("lower_case_file_system", "OFF".into()),
        ("lower_case_table_names", "0".into()),
        ("max_allowed_packet", "67108864".into()),
        ("max_connections", "151".into()),
        ("net_buffer_length", "16384".into()),
        ("net_read_timeout", "30".into()),
        ("net_write_timeout", "60".into()),
        ("performance_schema", "OFF".into()),
        ("protocol_version", "10".into()),
        (
            "sql_mode",
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
             ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
                .into(),
        ),
        ("system_time_zone", "UTC".into()),
        ("time_zone", "SYSTEM".into()),
        ("transaction_isolation", "REPEATABLE-READ".into()),
        ("tx_isolation", "REPEATABLE-READ".into()),
        ("version", elyra_core::SERVER_VERSION.into()),
        ("version_comment", "ElyraSQL \u{2014} MIT licensed".into()),
        (
            "version_compile_machine",
            predicate::compile_machine().into(),
        ),
        ("version_compile_os", predicate::compile_os().into()),
        ("wait_timeout", "28800".into()),
    ]
}

/// Case-insensitive SQL LIKE (`%` = any run, `_` = one char) for SHOW filters.
fn show_like(name: &str, pattern: &str) -> bool {
    let n: Vec<char> = name.to_ascii_lowercase().chars().collect();
    let p: Vec<char> = pattern.to_ascii_lowercase().chars().collect();
    fn m(n: &[char], p: &[char]) -> bool {
        if p.is_empty() {
            return n.is_empty();
        }
        match p[0] {
            '%' => m(n, &p[1..]) || (!n.is_empty() && m(&n[1..], p)),
            '_' => !n.is_empty() && m(&n[1..], &p[1..]),
            c => !n.is_empty() && n[0] == c && m(&n[1..], &p[1..]),
        }
    }
    m(&n, &p)
}

/// The LIKE/NoKeyword pattern of a SHOW filter, if any (WHERE returns all).
fn show_filter_pattern(filter: Option<&sqlparser::ast::ShowStatementFilter>) -> Option<String> {
    use sqlparser::ast::ShowStatementFilter::*;
    match filter {
        Some(Like(p)) | Some(ILike(p)) | Some(NoKeyword(p)) => Some(p.clone()),
        _ => None,
    }
}

/// `SHOW [GLOBAL|SESSION] VARIABLES [LIKE ...]`.
pub fn show_variables(filter: Option<&sqlparser::ast::ShowStatementFilter>) -> Result<QueryResult> {
    let pat = show_filter_pattern(filter);
    let rows: Vec<Vec<Value>> = system_variables()
        .into_iter()
        .filter(|(name, _)| pat.as_deref().is_none_or(|p| show_like(name, p)))
        .map(|(name, val)| vec![Value::Text(name.to_string()), Value::Text(val)])
        .collect();
    Ok(QueryResult::Rows(RowStream::literal(
        text_schema(&["Variable_name", "Value"]),
        rows,
    )))
}

#[cfg(test)]
mod system_variable_tests {
    use super::system_variables;
    use crate::predicate;
    use std::collections::HashMap;

    #[test]
    fn show_variables_reports_the_build_target() {
        let variables: HashMap<_, _> = system_variables().into_iter().collect();
        assert_eq!(
            variables.get("version_compile_machine").map(String::as_str),
            Some(predicate::compile_machine())
        );
        assert_eq!(
            variables.get("version_compile_os").map(String::as_str),
            Some(predicate::compile_os())
        );
    }
}

/// `SHOW [GLOBAL|SESSION] STATUS [LIKE ...]` — minimal counters.
pub fn show_status(filter: Option<&sqlparser::ast::ShowStatementFilter>) -> Result<QueryResult> {
    let pat = show_filter_pattern(filter);
    let rows: Vec<Vec<Value>> = [
        ("Uptime", "0"),
        ("Threads_connected", "1"),
        ("Threads_running", "1"),
        ("Queries", "0"),
    ]
    .into_iter()
    .filter(|(name, _)| pat.as_deref().is_none_or(|p| show_like(name, p)))
    .map(|(name, val)| vec![Value::Text(name.to_string()), Value::Text(val.to_string())])
    .collect();
    Ok(QueryResult::Rows(RowStream::literal(
        text_schema(&["Variable_name", "Value"]),
        rows,
    )))
}

/// `SHOW COLLATION [LIKE ...]` — the collations ElyraSQL supports.
pub fn show_collation(filter: Option<&sqlparser::ast::ShowStatementFilter>) -> Result<QueryResult> {
    let pat = show_filter_pattern(filter);
    let rows: Vec<Vec<Value>> = [
        ("utf8mb4_0900_ai_ci", "utf8mb4", "255", "Yes"),
        ("utf8mb4_general_ci", "utf8mb4", "45", ""),
        ("utf8mb4_bin", "utf8mb4", "46", ""),
    ]
    .into_iter()
    .filter(|(name, ..)| pat.as_deref().is_none_or(|p| show_like(name, p)))
    .map(|(coll, cs, id, def)| {
        vec![
            Value::Text(coll.to_string()),
            Value::Text(cs.to_string()),
            Value::Text(id.to_string()),
            Value::Text(def.to_string()),
            Value::Text("Yes".to_string()),
            Value::Text("1".to_string()),
            Value::Text("PAD SPACE".to_string()),
        ]
    })
    .collect();
    Ok(QueryResult::Rows(RowStream::literal(
        text_schema(&[
            "Collation",
            "Charset",
            "Id",
            "Default",
            "Compiled",
            "Sortlen",
            "Pad_attribute",
        ]),
        rows,
    )))
}

/// `SHOW DATABASES` / `SHOW SCHEMAS`.
pub fn show_databases(db: &Session) -> Result<QueryResult> {
    let rows = vec![
        vec![Value::Text("information_schema".into())],
        vec![Value::Text(db.database())],
    ];
    Ok(QueryResult::Rows(RowStream::literal(
        text_schema(&["Database"]),
        rows,
    )))
}

/// `SHOW [FULL] PROCESSLIST` — a single representative row (the engine does not
/// track a live connection table); handled in-engine so it works over both the
/// text and prepared-statement paths.
pub fn show_processlist(db: &Session) -> Result<QueryResult> {
    let row = vec![
        Value::Text("1".into()),
        Value::Text("root".into()),
        Value::Text("localhost".into()),
        Value::Text(db.database()),
        Value::Text("Query".into()),
        Value::Text("0".into()),
        Value::Text(String::new()),
        Value::Null,
    ];
    Ok(QueryResult::Rows(RowStream::literal(
        text_schema(&[
            "Id", "User", "Host", "db", "Command", "Time", "State", "Info",
        ]),
        vec![row],
    )))
}

/// `SHOW WARNINGS` / `SHOW ERRORS` — always empty (errors surface inline).
pub fn show_warnings() -> Result<QueryResult> {
    Ok(QueryResult::Rows(RowStream::literal(
        text_schema(&["Level", "Code", "Message"]),
        Vec::new(),
    )))
}

/// `SHOW {FUNCTION|PROCEDURE} STATUS [WHERE ...|LIKE ...]` — always empty
/// (ElyraSQL exposes no stored functions here). Handled pre-parse because the
/// `WHERE` form doesn't parse.
pub fn show_routine_status() -> Result<QueryResult> {
    Ok(QueryResult::Rows(RowStream::literal(
        text_schema(&[
            "Db",
            "Name",
            "Type",
            "Definer",
            "Modified",
            "Created",
            "Security_type",
            "Comment",
            "character_set_client",
            "collation_connection",
            "Database Collation",
        ]),
        Vec::new(),
    )))
}

/// `SHOW TABLE STATUS [FROM db] [LIKE ...]` — one metadata row per table.
/// Any FROM/LIKE clause is ignored (all tables are returned; tools tolerate it).
pub async fn show_table_status(db: &Session) -> Result<QueryResult> {
    let schema = text_schema(&[
        "Name",
        "Engine",
        "Version",
        "Row_format",
        "Rows",
        "Avg_row_length",
        "Data_length",
        "Max_data_length",
        "Index_length",
        "Data_free",
        "Auto_increment",
        "Create_time",
        "Update_time",
        "Check_time",
        "Collation",
        "Checksum",
        "Create_options",
        "Comment",
    ]);
    let names = catalog::list_tables(db).await?;
    let mut rows = Vec::with_capacity(names.len());
    for n in names {
        let nrows = match catalog::load_stats(db, &n).await? {
            Some(s) => s.rows.to_string(),
            None => "0".to_string(),
        };
        rows.push(vec![
            Value::Text(n),                           // Name
            Value::Text("InnoDB".into()),             // Engine
            Value::Text("10".into()),                 // Version
            Value::Text("Dynamic".into()),            // Row_format
            Value::Text(nrows),                       // Rows
            Value::Text("0".into()),                  // Avg_row_length
            Value::Text("0".into()),                  // Data_length
            Value::Text("0".into()),                  // Max_data_length
            Value::Text("0".into()),                  // Index_length
            Value::Text("0".into()),                  // Data_free
            Value::Null,                              // Auto_increment
            Value::Null,                              // Create_time
            Value::Null,                              // Update_time
            Value::Null,                              // Check_time
            Value::Text("utf8mb4_0900_ai_ci".into()), // Collation
            Value::Null,                              // Checksum
            Value::Text(String::new()),               // Create_options
            Value::Text(String::new()),               // Comment
        ]);
    }
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
        if name.0.len() >= 2 {
            let schema = name.0[name.0.len() - 2].value.to_ascii_lowercase();
            let table = name.0.last()?.value.to_ascii_lowercase();
            if schema == "information_schema" {
                return Some(table);
            }
            // Expose a few `mysql.*` catalog tables (prefixed so the virtual-
            // table provider can tell them apart).
            if schema == "mysql" {
                return Some(format!("mysql.{table}"));
            }
        }
    }
    None
}

/// Whether a table factor names a persisted ElyraSQL table rather than one of
/// the virtual information_schema/mysql relations. Streaming join plans need a
/// real [`TableDef`]; virtual relations must use the materialising path, which
/// obtains their schema and rows from [`information_schema`].
fn stored_table_factor(tf: &TableFactor) -> bool {
    matches!(tf, TableFactor::Table { .. }) && information_schema_view(tf).is_none()
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

fn ref_action_name(action: RefAction) -> &'static str {
    match action {
        RefAction::NoAction => "NO ACTION",
        RefAction::Restrict => "RESTRICT",
        RefAction::Cascade => "CASCADE",
        RefAction::SetNull => "SET NULL",
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
    let database = db.database();
    match view {
        "tables" => {
            let schema = Schema::new(vec![
                text("TABLE_SCHEMA"),
                text("TABLE_NAME"),
                text("TABLE_TYPE"),
                text("ENGINE"),
                int("TABLE_ROWS"),
                int("DATA_LENGTH"),
                int("INDEX_LENGTH"),
                text("TABLE_COMMENT"),
                text("TABLE_COLLATION"),
            ]);
            let mut rows = Vec::with_capacity(names.len());
            for n in names {
                let table_rows = match catalog::load_stats(db, &n).await? {
                    Some(s) => Value::Int(s.rows as i64),
                    None => Value::Null,
                };
                rows.push(vec![
                    Value::Text(database.clone()),
                    Value::Text(n),
                    Value::Text("BASE TABLE".into()),
                    Value::Text("ElyraSQL".into()),
                    table_rows,
                    // ElyraSQL does not currently maintain MySQL's physical
                    // per-table byte estimates or table comments. Expose the
                    // columns with stable best-effort values so schema tools
                    // can consume the standard information_schema shape.
                    Value::Int(0),
                    Value::Int(0),
                    Value::Text(String::new()),
                    Value::Text("utf8mb4_0900_ai_ci".into()),
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
                text("COLLATION_NAME"),
                text("COLUMN_COMMENT"),
                text("GENERATION_EXPRESSION"),
                text("CHARACTER_SET_NAME"),
            ]);
            let mut rows = Vec::new();
            for tname in names {
                let def = catalog::load(db, &tname).await?;
                for (i, c) in def.schema.columns.iter().enumerate() {
                    let meta = def.meta(i);
                    let ty = c.ty.display_name();
                    let is_text = matches!(c.ty, ColumnType::Text | ColumnType::Json);
                    let collation = match (is_text, c.collation) {
                        (true, elyra_core::Collation::Bin) => Value::Text("utf8mb4_bin".into()),
                        (true, _) => Value::Text("utf8mb4_0900_ai_ci".into()),
                        (false, _) => Value::Null,
                    };
                    let charset = if is_text {
                        Value::Text("utf8mb4".into())
                    } else {
                        Value::Null
                    };
                    rows.push(vec![
                        Value::Text(database.clone()),
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
                        collation,
                        Value::Text(String::new()),
                        match &meta.generated {
                            Some(g) => Value::Text(g.clone()),
                            None => Value::Text(String::new()),
                        },
                        charset,
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
                            Value::Text(database.clone()),
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
                int("POSITION_IN_UNIQUE_CONSTRAINT"),
                text("REFERENCED_TABLE_SCHEMA"),
                text("REFERENCED_TABLE_NAME"),
                text("REFERENCED_COLUMN_NAME"),
            ]);
            let mut rows = Vec::new();
            for tname in names {
                let def = catalog::load(db, &tname).await?;
                // PRIMARY KEY and UNIQUE constraints: no referenced table.
                let mut push_key = |cname: &str, seq: usize, ci: usize| {
                    rows.push(vec![
                        Value::Text(database.clone()),
                        Value::Text(cname.to_string()),
                        Value::Text(database.clone()),
                        Value::Text(tname.clone()),
                        Value::Text(def.schema.columns[ci].name.clone()),
                        Value::Int(seq as i64 + 1),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ]);
                };
                for (seq, &ci) in def.pk_cols.iter().enumerate() {
                    push_key("PRIMARY", seq, ci);
                }
                for idx in def.indexes.iter().filter(|i| i.unique) {
                    for (seq, &ci) in idx.cols.iter().enumerate() {
                        push_key(&idx.name, seq, ci);
                    }
                }
                // FOREIGN KEY constraints: fill the REFERENCED_* columns so tools
                // can discover relationships.
                for fk in &def.foreign_keys {
                    for (seq, (&ci, rc)) in fk.columns.iter().zip(fk.ref_columns.iter()).enumerate()
                    {
                        rows.push(vec![
                            Value::Text(database.clone()),
                            Value::Text(fk.name.clone()),
                            Value::Text(database.clone()),
                            Value::Text(tname.clone()),
                            Value::Text(def.schema.columns[ci].name.clone()),
                            Value::Int(seq as i64 + 1),
                            Value::Int(seq as i64 + 1),
                            Value::Text(database.clone()),
                            Value::Text(fk.ref_table.clone()),
                            Value::Text(rc.clone()),
                        ]);
                    }
                }
            }
            Ok((schema, rows))
        }
        "referential_constraints" => {
            let schema = Schema::new(vec![
                text("CONSTRAINT_SCHEMA"),
                text("CONSTRAINT_NAME"),
                text("UPDATE_RULE"),
                text("DELETE_RULE"),
            ]);
            let mut rows = Vec::new();
            for table_name in names {
                let def = catalog::load(db, &table_name).await?;
                rows.extend(def.foreign_keys.iter().map(|foreign_key| {
                    vec![
                        Value::Text(database.clone()),
                        Value::Text(foreign_key.name.clone()),
                        Value::Text(ref_action_name(foreign_key.on_update).into()),
                        Value::Text(ref_action_name(foreign_key.on_delete).into()),
                    ]
                }));
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
                text("HISTOGRAM"),
            ]);
            let mut rows = Vec::new();
            for tname in names {
                let Some(stats) = catalog::load_stats(db, &tname).await? else {
                    continue;
                };
                for c in &stats.columns {
                    let hist = if c.hist.is_empty() {
                        Value::Null
                    } else {
                        // MySQL-style: buckets as a JSON array of boundaries.
                        let items: Vec<String> = c
                            .hist
                            .iter()
                            .map(|b| format!("\"{}\"", b.replace('"', "\\\"")))
                            .collect();
                        Value::Text(format!("{{\"buckets\":[{}]}}", items.join(",")))
                    };
                    rows.push(vec![
                        Value::Text(tname.clone()),
                        Value::Text(c.name.clone()),
                        Value::Int(c.ndv as i64),
                        Value::Int(c.nulls as i64),
                        c.min.clone().map(Value::Text).unwrap_or(Value::Null),
                        c.max.clone().map(Value::Text).unwrap_or(Value::Null),
                        hist,
                    ]);
                }
            }
            Ok((schema, rows))
        }
        "partitions" => {
            let schema = Schema::new(vec![
                text("TABLE_NAME"),
                text("PARTITION_NAME"),
                text("PARTITION_METHOD"),
                text("PARTITION_EXPRESSION"),
                text("PARTITION_DESCRIPTION"),
            ]);
            let mut rows = Vec::new();
            for tname in names {
                let Some(spec) = catalog::load_partspec(db, &tname).await? else {
                    continue;
                };
                if spec.parts.is_empty() && spec.method == "HASH" {
                    for i in 0..spec.hash_count {
                        rows.push(vec![
                            Value::Text(tname.clone()),
                            Value::Text(format!("p{i}")),
                            Value::Text(spec.method.clone()),
                            Value::Text(spec.column.clone()),
                            Value::Null,
                        ]);
                    }
                }
                for p in &spec.parts {
                    let desc = if let Some(v) = p.less_than {
                        v.to_string()
                    } else if !p.list_values.is_empty() {
                        p.list_values
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(",")
                    } else {
                        "MAXVALUE".to_string()
                    };
                    rows.push(vec![
                        Value::Text(tname.clone()),
                        Value::Text(p.name.clone()),
                        Value::Text(spec.method.clone()),
                        Value::Text(spec.column.clone()),
                        Value::Text(desc),
                    ]);
                }
            }
            Ok((schema, rows))
        }
        "mysql.user" => {
            let cols = [
                "Host",
                "User",
                "Select_priv",
                "Insert_priv",
                "Update_priv",
                "Delete_priv",
                "Create_priv",
                "Drop_priv",
                "Super_priv",
                "plugin",
                "authentication_string",
                "account_locked",
                "password_expired",
            ];
            let schema = Schema::new(cols.iter().map(|n| text(n)).collect());
            let prefix = elyra_core::users::USER_PREFIX.to_vec();
            // Always include the built-in admin account (configured via
            // --user/--password, not stored in the catalog) so the user list is
            // never empty.
            let y = Value::Text("Y".into());
            let mut rows = vec![vec![
                Value::Text("%".into()),
                Value::Text("root".into()),
                y.clone(),
                y.clone(),
                y.clone(),
                y.clone(),
                y.clone(),
                y.clone(),
                y.clone(),
                Value::Text("mysql_native_password".into()),
                Value::Text(String::new()),
                Value::Text("N".into()),
                Value::Text("N".into()),
            ]];
            let mut after: Option<Vec<u8>> = None;
            loop {
                let batch = db.scan_batch(prefix.clone(), after.clone(), 512).await?;
                if batch.is_empty() {
                    break;
                }
                for (k, v) in &batch {
                    let name = String::from_utf8_lossy(&k[prefix.len()..]).to_string();
                    let tier = elyra_core::users::decode_user(v)
                        .map(|u| u.privilege)
                        .unwrap_or(elyra_core::Privilege::Read);
                    let y = |on: bool| Value::Text(if on { "Y" } else { "N" }.into());
                    let write = tier >= elyra_core::Privilege::Write;
                    let admin = tier >= elyra_core::Privilege::Admin;
                    rows.push(vec![
                        Value::Text("%".into()),
                        Value::Text(name),
                        y(true),
                        y(write),
                        y(write),
                        y(write),
                        y(admin),
                        y(admin),
                        y(admin),
                        Value::Text("mysql_native_password".into()),
                        Value::Text(String::new()),
                        y(false),
                        y(false),
                    ]);
                }
                after = batch.last().map(|(k, _)| k.clone());
                if batch.len() < 512 {
                    break;
                }
            }
            Ok((schema, rows))
        }
        "mysql.db" => {
            // No per-database grant table; report an empty, shaped result.
            let schema = Schema::new(
                ["Host", "Db", "User", "Select_priv", "Insert_priv"]
                    .iter()
                    .map(|n| text(n))
                    .collect(),
            );
            Ok((schema, Vec::new()))
        }
        "engines" => {
            let schema = Schema::new(vec![
                text("ENGINE"),
                text("SUPPORT"),
                text("COMMENT"),
                text("TRANSACTIONS"),
                text("XA"),
                text("SAVEPOINTS"),
            ]);
            let rows = vec![vec![
                Value::Text("InnoDB".into()),
                Value::Text("DEFAULT".into()),
                Value::Text("ElyraSQL storage engine (single-file, ACID, MVCC)".into()),
                Value::Text("YES".into()),
                Value::Text("NO".into()),
                Value::Text("YES".into()),
            ]];
            Ok((schema, rows))
        }
        "triggers" => {
            let schema = Schema::new(
                [
                    "TRIGGER_CATALOG",
                    "TRIGGER_SCHEMA",
                    "TRIGGER_NAME",
                    "EVENT_MANIPULATION",
                    "EVENT_OBJECT_CATALOG",
                    "EVENT_OBJECT_SCHEMA",
                    "EVENT_OBJECT_TABLE",
                    "ACTION_ORDER",
                    "ACTION_CONDITION",
                    "ACTION_STATEMENT",
                    "ACTION_ORIENTATION",
                    "ACTION_TIMING",
                    "CREATED",
                    "SQL_MODE",
                    "DEFINER",
                    "CHARACTER_SET_CLIENT",
                    "COLLATION_CONNECTION",
                    "DATABASE_COLLATION",
                ]
                .iter()
                .map(|n| text(n))
                .collect(),
            );
            let prefix = b"sys::trigger::".to_vec();
            let mut rows = Vec::new();
            let mut after: Option<Vec<u8>> = None;
            loop {
                let batch = db.scan_batch(prefix.clone(), after.clone(), 512).await?;
                if batch.is_empty() {
                    break;
                }
                for (_, v) in &batch {
                    let Ok(t) = bincode::deserialize::<catalog::TriggerDef>(v) else {
                        continue;
                    };
                    let event = match t.event {
                        catalog::TrigEvent::Insert => "INSERT",
                        catalog::TrigEvent::Update => "UPDATE",
                        catalog::TrigEvent::Delete => "DELETE",
                    };
                    rows.push(vec![
                        Value::Text("def".into()),
                        Value::Text(database.clone()),
                        Value::Text(t.name),
                        Value::Text(event.into()),
                        Value::Text("def".into()),
                        Value::Text(database.clone()),
                        Value::Text(t.table),
                        Value::Text("1".into()),
                        Value::Null,
                        Value::Text(t.body),
                        Value::Text("ROW".into()),
                        Value::Text(if t.before { "BEFORE" } else { "AFTER" }.into()),
                        Value::Null,
                        Value::Text(String::new()),
                        Value::Text("root@%".into()),
                        Value::Text("utf8mb4".into()),
                        Value::Text("utf8mb4_0900_ai_ci".into()),
                        Value::Text("utf8mb4_0900_ai_ci".into()),
                    ]);
                }
                after = batch.last().map(|(k, _)| k.clone());
                if batch.len() < 512 {
                    break;
                }
            }
            Ok((schema, rows))
        }
        "routines" => {
            let schema = Schema::new(
                [
                    "SPECIFIC_NAME",
                    "ROUTINE_CATALOG",
                    "ROUTINE_SCHEMA",
                    "ROUTINE_NAME",
                    "ROUTINE_TYPE",
                    "DATA_TYPE",
                    "ROUTINE_BODY",
                    "ROUTINE_DEFINITION",
                    "SQL_DATA_ACCESS",
                    "SECURITY_TYPE",
                    "CREATED",
                    "LAST_ALTERED",
                    "SQL_MODE",
                    "ROUTINE_COMMENT",
                    "DEFINER",
                    "CHARACTER_SET_CLIENT",
                    "COLLATION_CONNECTION",
                    "DATABASE_COLLATION",
                ]
                .iter()
                .map(|n| text(n))
                .collect(),
            );
            let prefix = b"sys::proc::".to_vec();
            let mut rows = Vec::new();
            let mut after: Option<Vec<u8>> = None;
            loop {
                let batch = db.scan_batch(prefix.clone(), after.clone(), 512).await?;
                if batch.is_empty() {
                    break;
                }
                for (k, _) in &batch {
                    let name = String::from_utf8_lossy(&k[prefix.len()..]).to_string();
                    rows.push(vec![
                        Value::Text(name.clone()),
                        Value::Text("def".into()),
                        Value::Text(database.clone()),
                        Value::Text(name),
                        Value::Text("PROCEDURE".into()),
                        Value::Text(String::new()),
                        Value::Text("SQL".into()),
                        Value::Null,
                        Value::Text("CONTAINS SQL".into()),
                        Value::Text("DEFINER".into()),
                        Value::Null,
                        Value::Null,
                        Value::Text(String::new()),
                        Value::Text(String::new()),
                        Value::Text("root@%".into()),
                        Value::Text("utf8mb4".into()),
                        Value::Text("utf8mb4_0900_ai_ci".into()),
                        Value::Text("utf8mb4_0900_ai_ci".into()),
                    ]);
                }
                after = batch.last().map(|(k, _)| k.clone());
                if batch.len() < 512 {
                    break;
                }
            }
            Ok((schema, rows))
        }
        "views" => {
            let schema = Schema::new(vec![
                text("TABLE_CATALOG"),
                text("TABLE_SCHEMA"),
                text("TABLE_NAME"),
                text("VIEW_DEFINITION"),
                text("CHECK_OPTION"),
                text("IS_UPDATABLE"),
                text("DEFINER"),
                text("SECURITY_TYPE"),
                text("CHARACTER_SET_CLIENT"),
                text("COLLATION_CONNECTION"),
            ]);
            let prefix = b"view::".to_vec();
            let mut rows = Vec::new();
            let mut after: Option<Vec<u8>> = None;
            loop {
                let batch = db.scan_batch(prefix.clone(), after.clone(), 512).await?;
                if batch.is_empty() {
                    break;
                }
                for (k, v) in &batch {
                    let name = String::from_utf8_lossy(&k[prefix.len()..]).to_string();
                    let def = String::from_utf8_lossy(v).to_string();
                    rows.push(vec![
                        Value::Text("def".into()),
                        Value::Text(database.clone()),
                        Value::Text(name),
                        Value::Text(def),
                        Value::Text("NONE".into()),
                        Value::Text("NO".into()),
                        Value::Text("root@%".into()),
                        Value::Text("DEFINER".into()),
                        Value::Text("utf8mb4".into()),
                        Value::Text("utf8mb4_0900_ai_ci".into()),
                    ]);
                }
                after = batch.last().map(|(k, _)| k.clone());
                if batch.len() < 512 {
                    break;
                }
            }
            Ok((schema, rows))
        }
        "events" => {
            // ElyraSQL has no scheduled events; report an empty, correctly-shaped
            // table so tools that introspect events don't error.
            let schema = Schema::new(
                [
                    "EVENT_CATALOG",
                    "EVENT_SCHEMA",
                    "EVENT_NAME",
                    "DEFINER",
                    "TIME_ZONE",
                    "EVENT_BODY",
                    "EVENT_DEFINITION",
                    "EVENT_TYPE",
                    "EXECUTE_AT",
                    "INTERVAL_VALUE",
                    "INTERVAL_FIELD",
                    "SQL_MODE",
                    "STARTS",
                    "ENDS",
                    "STATUS",
                    "ON_COMPLETION",
                    "CREATED",
                    "LAST_ALTERED",
                    "LAST_EXECUTED",
                    "EVENT_COMMENT",
                    "ORIGINATOR",
                    "CHARACTER_SET_CLIENT",
                    "COLLATION_CONNECTION",
                    "DATABASE_COLLATION",
                ]
                .iter()
                .map(|n| text(n))
                .collect(),
            );
            Ok((schema, Vec::new()))
        }
        "schemata" => {
            let schema = Schema::new(vec![
                text("CATALOG_NAME"),
                text("SCHEMA_NAME"),
                text("DEFAULT_CHARACTER_SET_NAME"),
                text("DEFAULT_COLLATION_NAME"),
                text("SQL_PATH"),
            ]);
            let rows = ["information_schema".to_string(), database]
                .into_iter()
                .map(|schema_name| {
                    vec![
                        Value::Text("def".into()),
                        Value::Text(schema_name),
                        Value::Text("utf8mb4".into()),
                        Value::Text("utf8mb4_0900_ai_ci".into()),
                        Value::Null,
                    ]
                })
                .collect();
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
        let (projection, hidden) = aggregate_projection_with_hidden(
            &select.projection,
            select.having.as_ref(),
            order_exprs,
            &schema,
        );
        let (mut osch, orows) = aggregate::run(&schema, &projection, group_by, rows)?;
        let mut orows = apply_having(select.having.as_ref(), &projection, &osch, orows)?;
        order_output_rows(&mut orows, &osch, order_exprs)?;
        truncate_hidden_columns(&mut osch, &mut orows, hidden);
        apply_offset_limit(&mut orows, offset, limit);
        return Ok(QueryResult::Rows(RowStream::literal(osch, orows)));
    }

    let resolved = resolve_order_aliases(order_exprs, &select.projection, &schema);
    if !resolved.is_empty() {
        sort_full_rows(&mut rows, &schema, &resolved, &db.cancel_token())?;
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
    for prefix in [
        data_prefix(name),
        index_table_prefix(name),
        indexnull_table_prefix(name),
    ] {
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
            // ADD INDEX / KEY / UNIQUE: build the equivalent index (with backfill)
            // via the CREATE INDEX path, then refresh the working definition.
            AlterTableOperation::AddConstraint(tc) => {
                use sqlparser::ast::TableConstraint as TC;
                // Foreign key: index the referencing columns (with backfill),
                // then register the constraint.
                if let TC::ForeignKey {
                    name: fname,
                    columns: cols,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                } = tc
                {
                    let mut fk_cols = Vec::new();
                    for ident in cols {
                        let i = def
                            .schema
                            .columns
                            .iter()
                            .position(|c| c.name.eq_ignore_ascii_case(&ident.value))
                            .ok_or_else(|| {
                                Error::Catalog(format!(
                                    "unknown foreign key column: {}",
                                    ident.value
                                ))
                            })?;
                        fk_cols.push(i);
                    }
                    if !def.indexes.iter().any(|ix| ix.cols == fk_cols) && def.pk_cols != fk_cols {
                        let ci = CreateIndex {
                            name: None,
                            table_name: name.clone(),
                            using: None,
                            columns: cols
                                .iter()
                                .map(|id| sqlparser::ast::OrderByExpr {
                                    expr: Expr::Identifier(id.clone()),
                                    asc: None,
                                    nulls_first: None,
                                    with_fill: None,
                                })
                                .collect(),
                            unique: false,
                            concurrently: false,
                            if_not_exists: false,
                            include: Vec::new(),
                            nulls_distinct: None,
                            with: Vec::new(),
                            predicate: None,
                        };
                        create_index(db, ci).await?;
                        def = catalog::load(db, &tname).await?;
                    }
                    def.foreign_keys.push(ForeignKey {
                        name: fname
                            .as_ref()
                            .map(|n| n.value.clone())
                            .unwrap_or_else(|| format!("fk_{tname}_{}", def.foreign_keys.len())),
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
                    continue;
                }
                let (idx_name, columns, unique) =
                    match tc {
                        TC::Index { name, columns, .. } => (name.clone(), columns.clone(), false),
                        TC::Unique {
                            name,
                            index_name,
                            columns,
                            ..
                        } => (
                            name.clone().or_else(|| index_name.clone()),
                            columns.clone(),
                            true,
                        ),
                        TC::PrimaryKey { .. } => return Err(Error::Unsupported(
                            "ALTER TABLE ADD PRIMARY KEY on an existing table is not supported; \
                             declare the primary key in CREATE TABLE"
                                .into(),
                        )),
                        other => {
                            return Err(Error::Unsupported(format!(
                                "ALTER ADD constraint not supported: {other}"
                            )))
                        }
                    };
                let ci = CreateIndex {
                    name: idx_name.map(|i| ObjectName(vec![i])),
                    table_name: name.clone(),
                    using: None,
                    columns: columns
                        .into_iter()
                        .map(|id| sqlparser::ast::OrderByExpr {
                            expr: Expr::Identifier(id),
                            asc: None,
                            nulls_first: None,
                            with_fill: None,
                        })
                        .collect(),
                    unique,
                    concurrently: false,
                    if_not_exists: false,
                    include: Vec::new(),
                    nulls_distinct: None,
                    with: Vec::new(),
                    predicate: None,
                };
                create_index(db, ci).await?;
                def = catalog::load(db, &tname).await?;
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

fn option_collation(option: &ColumnOption) -> Option<elyra_core::Collation> {
    match option {
        // sqlparser 0.53's CHANGE/MODIFY path retains CHARACTER SET but not
        // COLLATE. The frontend rewrites the latter to this AST shape.
        ColumnOption::CharacterSet(name) => Some(map_collation(name)),
        _ => None,
    }
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
    let old_collation = def.schema.columns[i].collation;
    let new_collation = options
        .iter()
        .rev()
        .find_map(option_collation)
        .unwrap_or(old_collation);
    if def.pk_cols.contains(&i) && new_ty != old_ty {
        return Err(Error::Unsupported(
            "cannot change the type of a primary key column".into(),
        ));
    }
    if def.pk_cols.contains(&i) && new_collation != old_collation {
        return Err(Error::Unsupported(
            "cannot change the collation of a primary key column".into(),
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
    if new_collation != old_collation {
        def.schema.columns[i].collation = new_collation;
        let schema_collations = def
            .schema
            .columns
            .iter()
            .map(|column| column.collation)
            .collect::<Vec<_>>();
        for index in &mut def.indexes {
            if index.cols.contains(&i) {
                index.col_collations = index
                    .cols
                    .iter()
                    .map(|&column| schema_collations[column])
                    .collect();
            }
        }
        rebuild_indexes_for_column(db, def, i).await?;
    }
    Ok(())
}

async fn rebuild_indexes_for_column(db: &Session, def: &TableDef, column: usize) -> Result<()> {
    let indexes = def
        .indexes
        .iter()
        .filter(|index| index.cols.contains(&column))
        .cloned()
        .collect::<Vec<_>>();
    if indexes.is_empty() {
        return Ok(());
    }

    let index_names = indexes
        .iter()
        .map(|index| index.name.as_str())
        .collect::<Vec<_>>();
    let deletes = collect_index_entry_keys(db, &def.name, &index_names).await?;

    let indexed_def = TableDef {
        indexes,
        ..def.clone()
    };
    let rows = collect_matches(db, def, None, None).await?;
    let mut puts = vec![(catalog_key(&def.name), def.encode()?)];
    for (key, row) in rows {
        puts.extend(index::entries_for_row(&indexed_def, &row, &key)?);
    }
    db.commit_write(puts, deletes).await
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
    let options = col
        .options
        .iter()
        .map(|option| option.option.clone())
        .collect::<Vec<_>>();
    let (nullable, meta) = options_to_meta(&options);
    let is_primary = options.iter().any(|option| {
        matches!(
            option,
            ColumnOption::Unique {
                is_primary: true,
                ..
            }
        )
    });
    if is_primary && def.has_pk() {
        return Err(Error::Query("multiple primary keys are not allowed".into()));
    }
    if meta.auto_increment && !is_primary {
        return Err(Error::Unsupported(
            "ADD COLUMN AUTO_INCREMENT currently requires PRIMARY KEY".into(),
        ));
    }
    let explicit_default = options
        .iter()
        .find_map(|option| match option {
            ColumnOption::Default(expression) => Some(expression),
            _ => None,
        })
        .map(|expression| coerce(eval_expr(expression)?, &ty, &col.name.value))
        .transpose()?;
    if !nullable && explicit_default.as_ref().is_some_and(Value::is_null) && !meta.auto_increment {
        return Err(Error::Query(format!(
            "ADD COLUMN '{}' is NOT NULL and needs a DEFAULT",
            col.name.value
        )));
    }

    let old_def = def.clone();
    let rows = collect_matches(db, def, None, None).await?;
    let default = match explicit_default {
        Some(default) => default,
        None if nullable || meta.auto_increment || rows.is_empty() => Value::Null,
        None => mysql_implicit_alter_value(&ty).ok_or_else(|| {
            Error::Query(format!(
                "ADD COLUMN '{}' cannot backfill existing rows without a DEFAULT",
                col.name.value
            ))
        })?,
    };
    ensure_col_meta(def);
    let new_column = def.schema.columns.len();
    def.schema.columns.push(ColumnDef {
        name: col.name.value.clone(),
        ty: ty.clone(),
        nullable,
        collation: col
            .collation
            .as_ref()
            .map(map_collation)
            .or_else(|| {
                col.options
                    .iter()
                    .rev()
                    .find_map(|option| option_collation(&option.option))
            })
            .unwrap_or_default(),
    });
    def.col_meta.push(meta.clone());
    if is_primary {
        def.pk_cols = vec![new_column];
    }

    let mut puts = vec![(catalog_key(&def.name), def.encode()?)];
    let mut deletes = Vec::new();
    let mut auto_increment = 0i64;
    for (old_key, mut row) in rows {
        if is_primary {
            deletes.extend(index::entry_keys_for_row(&old_def, &row, &old_key)?);
        }
        let value = if meta.auto_increment {
            auto_increment += 1;
            coerce(Value::Int(auto_increment), &ty, &col.name.value)?
        } else {
            default.clone()
        };
        row.push(value);
        let new_key = if is_primary {
            let primary_values = def
                .pk_cols
                .iter()
                .map(|&position| row[position].clone())
                .collect::<Vec<_>>();
            data_key(
                &def.name,
                &keyenc::encode_key_coll(&primary_values, &def.pk_collations())?,
            )
        } else {
            old_key.clone()
        };
        if new_key != old_key {
            deletes.push(old_key);
        }
        puts.push((
            new_key.clone(),
            bincode::serialize(&row).map_err(|error| Error::Storage(error.to_string()))?,
        ));
        if is_primary {
            puts.extend(index::entries_for_row(def, &row, &new_key)?);
        }
    }
    if is_primary {
        deletes.push(rowid_key(&def.name));
    }
    if meta.auto_increment {
        puts.push((
            autoinc_key(&def.name),
            auto_increment.to_le_bytes().to_vec(),
        ));
    }
    puts.push(bump_wcount(db, &def.name).await?);
    db.commit_write(puts, deletes).await?;
    Ok(())
}

fn mysql_implicit_alter_value(ty: &ColumnType) -> Option<Value> {
    match ty {
        ColumnType::Bool => Some(Value::Bool(false)),
        ColumnType::Int => Some(Value::Int(0)),
        ColumnType::UInt => Some(Value::UInt(0)),
        ColumnType::Float => Some(Value::Float(0.0)),
        ColumnType::Text => Some(Value::Text(String::new())),
        ColumnType::Bytes => Some(Value::Bytes(Vec::new())),
        ColumnType::Decimal(_, scale) => Some(Value::Decimal(0, *scale)),
        ColumnType::Time => Some(Value::Time(0)),
        ColumnType::Vector(_) | ColumnType::Date | ColumnType::DateTime | ColumnType::Json => None,
    }
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
    if let Some(foreign_key) = def
        .foreign_keys
        .iter()
        .find(|foreign_key| foreign_key.columns.contains(&idx))
    {
        return Err(Error::Unsupported(format!(
            "cannot drop column `{name}`: needed in foreign key constraint `{}`",
            foreign_key.name
        )));
    }

    let dropped_indexes = def
        .indexes
        .iter()
        .filter(|index| index.cols.contains(&idx))
        .cloned()
        .collect::<Vec<_>>();
    let dropped_index_names = dropped_indexes
        .iter()
        .map(|index| index.name.as_str())
        .collect::<Vec<_>>();
    let deletes = collect_index_entry_keys(db, &def.name, &dropped_index_names).await?;
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
            let mut row: Vec<Value> = rowdec::decode_row(&v)?;
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
    def.indexes.retain(|index| !index.cols.contains(&idx));
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
    for foreign_key in &mut def.foreign_keys {
        foreign_key.columns.iter_mut().for_each(shift);
    }
    puts.push(bump_wcount(db, &def.name).await?);
    db.commit_write(puts, deletes).await?;
    Ok(())
}

pub async fn rename_table(db: &Session, old: &str, new: &str) -> Result<QueryResult> {
    let mut def = catalog::load(db, old).await?;
    alter_rename_table(db, &mut def, new).await?;
    Ok(QueryResult::Affected(0))
}

async fn alter_rename_table(db: &Session, def: &mut TableDef, new: &str) -> Result<()> {
    if catalog::exists(db, new).await? {
        return Err(Error::Catalog(format!("table already exists: {new}")));
    }
    let old = def.name.clone();
    def.name = new.to_string();
    for foreign_key in &mut def.foreign_keys {
        if foreign_key.ref_table.eq_ignore_ascii_case(&old) {
            foreign_key.ref_table = new.to_string();
        }
    }

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
            let row: Vec<Value> = rowdec::decode_row(&v)?;
            deletes.push(old_key);
            puts.push((new_key.clone(), v));
            puts.extend(index::entries_for_row(def, &row, &new_key)?);
        }
        if last {
            break;
        }
    }

    // Delete all old index entries (keyed under the old table name), including
    // the NULL-keyed entries under `indexnull::` (rebuilt under the new name by
    // `entries_for_row` above).
    for old_index_prefix in [
        format!("index::{old}::").into_bytes(),
        format!("indexnull::{old}::").into_bytes(),
    ] {
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
    }

    // Move catalog + table-scoped metadata.
    deletes.push(catalog_key(&old));
    puts.push((catalog_key(new), def.encode()?));
    // MySQL carries referencing foreign keys across a table rename. Update
    // every child catalog in the same write so later DML never probes the old
    // table name.
    for table in catalog::list_tables(db).await? {
        if table.eq_ignore_ascii_case(&old) {
            continue;
        }
        let mut referencing = catalog::load(db, &table).await?;
        let mut changed = false;
        for foreign_key in &mut referencing.foreign_keys {
            if foreign_key.ref_table.eq_ignore_ascii_case(&old) {
                foreign_key.ref_table = new.to_string();
                changed = true;
            }
        }
        if changed {
            puts.push((catalog_key(&table), referencing.encode()?));
        }
    }
    for key in [
        rowid_key as fn(&str) -> Vec<u8>,
        autoinc_key,
        wcount_key,
        stats_key,
        partmeta_key,
    ] {
        let old_key = key(&old);
        if let Some(value) = db.get(old_key.clone()).await? {
            deletes.push(old_key);
            puts.push((key(new), value));
        }
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
        indexes_nulls: false,
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
            let row: Vec<Value> = rowdec::decode_row(&v)?;
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
    // Single-column B-tree indexes maintain NULL-keyed entries so ordered
    // `ORDER BY <col> LIMIT` walks are complete without a NULL scan.
    let indexes_nulls = cols.len() == 1 && !is_vector;
    def.indexes.push(IndexDef {
        name,
        cols,
        unique: ci.unique,
        vector: is_vector,
        fulltext: false,
        col_collations,
        indexes_nulls,
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
            let row: Vec<Value> = rowdec::decode_row(&v)?;
            puts.extend(index::entries_for_row(&def, &row, &k)?);
        }
        if last {
            break;
        }
    }
    db.commit_write(puts, vec![]).await?;
    Ok(QueryResult::Affected(0))
}

/// Rename a secondary index and rebuild its persisted keys under the new name.
pub async fn rename_index(
    db: &Session,
    table: &str,
    old_name: &str,
    new_name: &str,
) -> Result<QueryResult> {
    let mut def = catalog::load(db, table).await?;
    if def
        .indexes
        .iter()
        .any(|index| index.name.eq_ignore_ascii_case(new_name))
    {
        return Err(Error::Catalog(format!("index already exists: {new_name}")));
    }
    let position = def
        .indexes
        .iter()
        .position(|index| index.name.eq_ignore_ascii_case(old_name))
        .ok_or_else(|| Error::Catalog(format!("unknown index: {old_name}")))?;
    def.indexes[position].name = new_name.to_string();
    let renamed = def.indexes[position].clone();

    let deletes = collect_index_entry_keys(db, table, &[old_name]).await?;

    let indexed_def = TableDef {
        indexes: vec![renamed],
        ..def.clone()
    };
    let rows = collect_matches(db, &def, None, None).await?;
    let mut puts = vec![(catalog_key(table), def.encode()?)];
    for (key, row) in rows {
        puts.extend(index::entries_for_row(&indexed_def, &row, &key)?);
    }
    db.commit_write(puts, deletes).await?;
    Ok(QueryResult::Affected(0))
}

/// Remove a secondary index definition and all of its persisted entries.
pub async fn drop_index(db: &Session, table: &str, name: &str) -> Result<QueryResult> {
    let mut def = catalog::load(db, table).await?;
    let position = def
        .indexes
        .iter()
        .position(|index| index.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Catalog(format!("unknown index: {name}")))?;
    let removed = def.indexes.remove(position);

    let deletes = collect_index_entry_keys(db, table, &[removed.name.as_str()]).await?;

    db.commit_write(vec![(catalog_key(table), def.encode()?)], deletes)
        .await?;
    Ok(QueryResult::Affected(0))
}

async fn collect_index_entry_keys(
    db: &Session,
    table: &str,
    index_names: &[&str],
) -> Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    for index_name in index_names {
        for prefix in [
            index::index_scan_prefix(table, index_name),
            index::indexnull_scan_prefix(table, index_name),
        ] {
            let mut cursor = None;
            loop {
                let batch = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
                if batch.is_empty() {
                    break;
                }
                let is_last = batch.len() < 4096;
                cursor = batch.last().map(|(key, _)| key.clone());
                keys.extend(batch.into_iter().map(|(key, _)| key));
                if is_last {
                    break;
                }
            }
        }
    }
    Ok(keys)
}

/// Remove a named foreign-key constraint without removing its supporting index.
pub async fn drop_foreign_key(db: &Session, table: &str, name: &str) -> Result<QueryResult> {
    let mut def = catalog::load(db, table).await?;
    let position = def
        .foreign_keys
        .iter()
        .position(|foreign_key| foreign_key.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Catalog(format!("unknown foreign key: {name}")))?;
    def.foreign_keys.remove(position);
    db.commit_write(vec![(catalog_key(table), def.encode()?)], Vec::new())
        .await?;
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
                let mut row = Vec::with_capacity(exprs.len());
                for expr in exprs {
                    if expr_has_subquery(expr) {
                        let resolved = resolve_subqueries(db, vindex, expr.clone()).await?;
                        row.push(eval_expr(&resolved)?);
                    } else {
                        row.push(eval_expr(expr)?);
                    }
                }
                out.push(row);
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
    let stored_auto_id = |row: &[Value]| {
        let value = row.get(auto_col?)?;
        match value {
            Value::Int(id) => u64::try_from(*id).ok(),
            Value::UInt(id) => Some(*id),
            _ => None,
        }
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
            merged[*idx] = coerce_for_session(db, v, &col.ty, &col.name)?;
        }
        Ok(merged)
    };

    // Pass 1: build every row (coerce, defaults, AUTO_INCREMENT, generated,
    // NOT NULL) and its clustered key — no per-row storage reads.
    let mut built: Vec<(Vec<u8>, Vec<Value>)> = Vec::with_capacity(rows.len());
    // LAST_INSERT_ID() retains the first generated value. The wire OK packet
    // follows mysql_insert_id(): it prefers that generated value, but when no
    // value was generated it reports the last explicit nonzero value stored in
    // the AUTO_INCREMENT column.
    let mut first_generated_id: i64 = 0;
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
            row[*slot] = coerce_for_session(db, v, &col.ty, &col.name)?;
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
                let is_zero = matches!(row[ai], Value::Int(0)) || matches!(row[ai], Value::UInt(0));
                let need = !provided[ai] || row[ai].is_null() || is_zero;
                let col = &def.schema.columns[ai];
                if need {
                    autoinc += 1;
                    // Coerce to the column type so a UInt (BIGINT UNSIGNED) PK
                    // stores/looks up with the same key encoding as the value.
                    row[ai] = coerce(Value::Int(autoinc), &col.ty, &col.name)?;
                    if first_generated_id == 0 {
                        first_generated_id = autoinc;
                    }
                } else {
                    let explicit_id = match &row[ai] {
                        Value::Int(n) => u64::try_from(*n).ok(),
                        Value::UInt(u) => Some(*u),
                        _ => None,
                    };
                    if let Some(explicit_id) = explicit_id {
                        if let Ok(n) = i64::try_from(explicit_id) {
                            if n > autoinc {
                                autoinc = n;
                            }
                        }
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

    if on_dup && index::has_unique(&def) {
        remap_unique_upsert_conflicts(db, &def, &mut built).await?;
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
        db.set_last_insert_id(first_generated_id);
        return Ok(QueryResult::Insert {
            affected_rows: affected,
            last_insert_id: if first_generated_id != 0 {
                first_generated_id as u64
            } else {
                built
                    .iter()
                    .rev()
                    .find_map(|(_, row)| stored_auto_id(row))
                    .unwrap_or(0)
            },
        });
    }

    // One batched existence read for the whole statement (PK tables) instead of
    // a read per row — the bulk-insert hot path.
    let clustered_conflicts = has_pk || (on_dup && index::has_unique(&def));
    let existing: Vec<Option<Vec<u8>>> = if clustered_conflicts {
        let keys: Vec<Vec<u8>> = built.iter().map(|(k, _)| k.clone()).collect();
        db.multi_get(keys).await?
    } else {
        Vec::new()
    };

    // Pass 2: apply INSERT / upsert semantics using the batched existence info.
    for (i, (key, row)) in built.into_iter().enumerate() {
        if !clustered_conflicts {
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
            let old_row: Vec<Value> = rowdec::decode_row(old_enc)?;
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
    db.set_last_insert_id(first_generated_id);
    if !after_ins.is_empty() {
        for (_, row) in &batch {
            queue_after(db, &after_ins, &def.schema, Some(row), None)?;
        }
    }
    Ok(QueryResult::Insert {
        affected_rows: affected,
        last_insert_id: if first_generated_id != 0 {
            first_generated_id as u64
        } else {
            batch
                .iter()
                .rev()
                .find_map(|(_, row)| stored_auto_id(row))
                .unwrap_or(0)
        },
    })
}

/// Point rows that collide with a unique secondary index at the owning
/// clustered row. The normal duplicate-update pass can then merge them exactly
/// like primary-key conflicts. The ownership map also covers collisions among
/// rows in the same statement.
async fn remap_unique_upsert_conflicts(
    db: &Session,
    def: &TableDef,
    rows: &mut [(Vec<u8>, Vec<Value>)],
) -> Result<()> {
    let probes = rows
        .iter()
        .map(|(_, row)| index::unique_probe_keys(def, row))
        .collect::<Result<Vec<_>>>()?;
    let flat = probes.iter().flatten().cloned().collect::<Vec<Vec<u8>>>();
    let stored = db.multi_get(flat.clone()).await?;
    let mut owners = std::collections::HashMap::<Vec<u8>, Vec<u8>>::new();
    for (probe, owner) in flat.into_iter().zip(stored) {
        if let Some(owner) = owner {
            owners.insert(probe, owner);
        }
    }

    let clustered = if def.has_pk() {
        db.multi_get(rows.iter().map(|(key, _)| key.clone()).collect())
            .await?
    } else {
        vec![None; rows.len()]
    };

    for (((key, _), row_probes), stored_row) in rows.iter_mut().zip(probes).zip(clustered) {
        let mut conflict = stored_row.map(|_| key.clone());
        for probe in &row_probes {
            let Some(owner) = owners.get(probe) else {
                continue;
            };
            match &conflict {
                Some(existing) if existing != owner => {
                    return Err(Error::Duplicate(
                        "upsert conflicts with more than one unique row".into(),
                    ));
                }
                None => conflict = Some(owner.clone()),
                Some(_) => {}
            }
        }
        if let Some(owner) = conflict {
            *key = owner;
        }
        for probe in row_probes {
            owners.entry(probe).or_insert_with(|| key.clone());
        }
    }
    Ok(())
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
        let mut referenced_values = Vec::new();
        for (_, row) in batch {
            let vals: Vec<Value> = fk.columns.iter().map(|&i| row[i].clone()).collect();
            if vals.iter().any(|v| v.is_null()) {
                continue; // a NULL in the referencing tuple is allowed
            }
            referenced_values.push(vals);
        }
        if referenced_values.is_empty() {
            continue;
        }
        let parent = catalog::load(db, &fk.ref_table).await?;
        let probes = referenced_values
            .iter()
            .map(|values| fk_probe_key(&parent, &fk.ref_columns, values))
            .collect::<Result<Vec<_>>>()?;
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

/// Execute `GROUP BY ... WITH ROLLUP` by running the aggregation once per
/// grouping prefix -- full detail (all N columns), then N-1, ..., down to the
/// grand total (0 columns) -- and concatenating. At level k the dropped group
/// columns (positions >= k) are projected as NULL, matching MySQL's rollup
/// super-aggregate rows. Re-aggregating from the base rows at each level keeps
/// AVG/MIN/MAX correct (they can't be derived from finer groups). ORDER BY and
/// OFFSET/LIMIT apply to the combined result.
#[allow(clippy::too_many_arguments)]
async fn execute_rollup(
    db: &Session,
    vindex: &VectorRegistry,
    query: &SqlQuery,
    group_by: &[Expr],
    order_exprs: &[(Expr, bool)],
    offset: usize,
    limit: Option<usize>,
) -> Result<QueryResult> {
    use sqlparser::ast::{GroupByExpr, SelectItem};
    let n = group_by.len();
    let group_texts: Vec<String> = group_by.iter().map(|e| e.to_string()).collect();

    let mut out_schema: Option<Schema> = None;
    let mut all_rows: Vec<Vec<Value>> = Vec::new();

    // Full detail (k = n) down to the grand total (k = 0).
    for k in (0..=n).rev() {
        let mut lq = query.clone();
        lq.order_by = None;
        lq.limit = None;
        lq.offset = None;
        if let SetExpr::Select(s) = lq.body.as_mut() {
            // Group by the first k columns, dropping the ROLLUP modifier.
            s.group_by = GroupByExpr::Expressions(group_by[..k].to_vec(), vec![]);
            // Replace references to the dropped group columns (positions >= k)
            // in the projection with NULL, so this level's rows carry NULL there.
            let dropped = &group_texts[k..];
            for item in &mut s.projection {
                let expr = match item {
                    SelectItem::UnnamedExpr(e) => Some(e),
                    SelectItem::ExprWithAlias { expr, .. } => Some(expr),
                    _ => None,
                };
                if let Some(e) = expr {
                    if dropped.iter().any(|d| d == &e.to_string()) {
                        *e = Expr::Value(sqlparser::ast::Value::Null);
                    }
                }
            }
        }
        let res = Box::pin(select(db, vindex, &lq)).await?;
        if let QueryResult::Rows(mut stream) = res {
            if out_schema.is_none() {
                out_schema = Some(stream.schema.clone());
            }
            loop {
                let batch = stream.next_batch(8192).await?;
                if batch.is_empty() {
                    break;
                }
                all_rows.extend(batch);
            }
        }
    }

    let schema = out_schema.unwrap_or_else(|| Schema::new(Vec::new()));
    order_output_rows(&mut all_rows, &schema, order_exprs)?;
    apply_offset_limit(&mut all_rows, offset, limit);
    Ok(QueryResult::Rows(RowStream::literal(schema, all_rows)))
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

    // SELECT DISTINCT: applied after projection and before OFFSET/LIMIT. Run the
    // inner query without DISTINCT and without offset/limit (but keeping ORDER BY,
    // so the output stays ordered and duplicates are adjacent), dedup the
    // projected rows by a collation-aware key (so a `_bin` column distinguishes
    // case), then apply offset/limit. This covers every underlying path (scan,
    // join, aggregate) uniformly via one recursive call. Done before the `select`
    // local shadows the function name below.
    if let SetExpr::Select(s) = query.body.as_ref() {
        if matches!(s.distinct, Some(sqlparser::ast::Distinct::Distinct)) {
            let d_offset = match &query.offset {
                Some(o) => eval_usize(&o.value)?,
                None => 0,
            };
            let d_limit = match &query.limit {
                Some(e) => Some(eval_usize(e)?),
                None => None,
            };
            let mut inner_q = query.clone();
            inner_q.limit = None;
            inner_q.offset = None;
            if let SetExpr::Select(si) = inner_q.body.as_mut() {
                si.distinct = None;
            }
            let res = Box::pin(select(db, vindex, &inner_q)).await?;
            let QueryResult::Rows(mut stream) = res else {
                return Ok(res);
            };
            let schema = stream.schema.clone();
            let colls: Vec<elyra_core::Collation> =
                schema.columns.iter().map(|c| c.collation).collect();
            let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
            let mut out: Vec<Vec<Value>> = Vec::new();
            let cap = distinct_max();
            loop {
                let batch = stream.next_batch(8192).await?;
                if batch.is_empty() {
                    break;
                }
                for row in batch {
                    if seen.insert(Value::row_collation_key_coll(&row, &colls)) {
                        out.push(row);
                        if out.len() > cap {
                            return Err(Error::Query(format!(
                                "SELECT DISTINCT exceeded {cap} distinct rows; narrow the query \
                                 or raise ELYRASQL_DISTINCT_MAX"
                            )));
                        }
                    }
                }
            }
            apply_offset_limit(&mut out, d_offset, d_limit);
            return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
        }
    }

    // Normalise an INNER comma-join (`FROM a, b WHERE a.k = b.k`) into an explicit
    // JOIN chain so it gets cost-based reordering and streaming. Done before the
    // `select` local shadows the function name below.
    if let SetExpr::Select(s) = query.body.as_ref() {
        if s.from.len() > 1 && s.from.iter().all(|t| t.joins.is_empty()) {
            if let Some(chain) = comma_join_chain(&s.from, s.selection.as_ref()) {
                let mut q2 = query.clone();
                if let SetExpr::Select(sm) = q2.body.as_mut() {
                    sm.from = vec![chain];
                }
                return Box::pin(select(db, vindex, &q2)).await;
            }
        }
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
    // GROUP BY ... WITH ROLLUP: super-aggregate (subtotal + grand-total) rows.
    let rollup = matches!(
        &select.group_by,
        sqlparser::ast::GroupByExpr::Expressions(_, mods)
            if mods.iter().any(|m| matches!(m, sqlparser::ast::GroupByWithModifier::Rollup))
    );
    let order_exprs: Vec<(Expr, bool)> = match &query.order_by {
        Some(ob) => ob
            .exprs
            .iter()
            .map(|o| (o.expr.clone(), o.asc.unwrap_or(true)))
            .collect(),
        None => Vec::new(),
    };

    if rollup && !group_by.is_empty() {
        return Box::pin(execute_rollup(
            db,
            vindex,
            query,
            &group_by,
            &order_exprs,
            offset,
            limit,
        ))
        .await;
    }

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
        // Streaming index nested-loop fast path for
        // `FROM a JOIN b ON a.k=b.<indexed> [WHERE ...] LIMIT n` (no GROUP BY,
        // aggregate, ORDER BY or DISTINCT): stops after enough rows instead of
        // materialising the whole join. Falls back to join_select otherwise.
        if group_by.is_empty() && !aggregate::projection_has_aggregate(&select.projection) {
            if order_exprs.is_empty() {
                // No ORDER BY: early-stop streaming index nested-loop for LIMIT n.
                if let Some(lim) = limit {
                    if let Some(res) =
                        streaming_nlj_select(db, select, filter.as_ref(), offset, lim).await?
                    {
                        return Ok(res);
                    }
                }
            } else if let Some(res) =
                streaming_join_order(db, select, filter.as_ref(), &order_exprs, offset, limit)
                    .await?
            {
                // ORDER BY (no aggregate): build the partner hash table and stream
                // the driving table into the spilling sorter, so the join output
                // is never fully materialised. Falls back to join_select otherwise.
                return Ok(res);
            }
        } else if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
            // Streaming index nested-loop aggregation: stream the driving table
            // and feed the spilling aggregator so a large join + GROUP BY is
            // bounded by group state, not the join output size. Falls back to
            // join_select otherwise.
            if let Some(res) = streaming_join_aggregate(
                db,
                select,
                filter.as_ref(),
                &group_by,
                &order_exprs,
                offset,
                limit,
            )
            .await?
            {
                return Ok(res);
            }
        }
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

    // Hybrid-search primitive: `SELECT ..., HYBRID(text_col, 'q', vec_col, vec)
    // ... FROM t [WHERE ...]` fuses full-text + vector rankings with RRF.
    if let Some(item) = select.projection.iter().find(|it| {
        matches!(
            it,
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. }
                if hybrid_call(e).is_some()
        )
    }) {
        let e = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => unreachable!(),
        };
        let (tcol, tq, vcol, vexpr) = hybrid_call(e).unwrap();
        return hybrid_select(
            db,
            vindex,
            select,
            &def,
            filter.as_ref(),
            &tcol,
            &tq,
            &vcol,
            vexpr,
            offset,
            limit,
        )
        .await;
    }

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
        // HAVING and ORDER BY may read grouped values that are not returned.
        // Compute them as hidden output columns, then drop them before returning.
        let (projection, hidden) = aggregate_projection_with_hidden(
            &select.projection,
            select.having.as_ref(),
            &order_exprs,
            &def.schema,
        );
        let proj = projection.as_slice();
        let plan = aggregate::build_plan(&def.schema, proj, &group_by)?;
        // If statistics predict more distinct groups than fit in memory, go
        // straight to the spilling partitioned aggregation instead of running the
        // in-memory pass, hitting the cap, and re-scanning from scratch (which
        // cost two full table scans).
        let est_groups = estimate_group_count(db, &def, plan.group_cols()).await?;
        let cap = elyra_olap::default_max_groups() as u64;
        // Vectorised (columnar) scalar-aggregate fast path: no GROUP BY, no
        // filter, numeric aggregates. Extracts columns into f64 arrays and
        // aggregates with tight SIMD-friendly loops.
        // Only when at least two aggregates share the scan (e.g. SUM+AVG+MIN+MAX
        // or SUM+COUNT): vectorising then amortises the columnar extraction over
        // several tight aggregation loops. A single aggregate stays on the
        // streaming path, which is as fast for one accumulator.
        let columnar = if filter.is_none() && !db.in_txn() {
            plan.scalar_agg_plan(&def.schema).filter(|s| s.len() >= 2)
        } else {
            None
        };
        // Vectorised (columnar) grouped fast path (OLAP phase 3): one numeric
        // GROUP BY column, numeric aggregates, and either no filter or one that
        // compiles to the fast predicate. Falls back to the spilling path if the
        // distinct-group cap is exceeded.
        let columnar_group = if !db.in_txn() {
            plan.columnar_group_plan(&def.schema)
                .and_then(|(gc, specs)| {
                    let needed = agg_needed_mask(&def.schema, filter.as_ref(), &plan)?;
                    match &filter {
                        None => Some((gc, specs, None, needed)),
                        Some(f) => {
                            cpred::compile(f, &def.schema).map(|cp| (gc, specs, Some(cp), needed))
                        }
                    }
                })
        } else {
            None
        };
        let (schema, out_rows) = if let Some(specs) = columnar {
            // Unfiltered scalar aggregation may use the columnar cache (opt-in).
            let results = if colcache::enabled() {
                columnar_cached_scalar(db, &def, &specs).await?
            } else {
                scan_columnar_scalar(db, &def, &specs).await?
            };
            plan.project_scalar(results)?
        } else if est_groups.is_some_and(|g| g > cap) {
            partitioned_aggregate(db, &def, filter.clone(), &plan).await?
        } else if let Some((gc, specs, cf, needed)) = columnar_group {
            let base_len = def.schema.columns.len();
            // The cache holds whole (unfiltered) columns, so it serves only the
            // no-filter case; filtered GROUP BY stays on the compiled-predicate
            // scan path.
            let cached = if colcache::enabled() && cf.is_none() {
                columnar_cached_group(db, &def, gc, &specs, base_len).await?
            } else {
                None
            };
            match cached {
                Some(groups) => plan.project_grouped(groups)?,
                None => {
                    match scan_columnar_group_zm(db, &def, gc, &specs, cf, needed, base_len).await?
                    {
                        Some(groups) => plan.project_grouped(groups)?,
                        None => partitioned_aggregate(db, &def, filter.clone(), &plan).await?,
                    }
                }
            }
        } else {
            let agg = olap_aggregate(db, &def, filter.clone(), &plan).await?;
            if agg.overflowed() {
                // Stats under-estimated (or absent): fall back to the spilling
                // path (bounded memory).
                partitioned_aggregate(db, &def, filter.clone(), &plan).await?
            } else {
                plan.finalize(agg)?
            }
        };
        let (mut schema, mut out_rows) = (schema, out_rows);
        out_rows = apply_having(select.having.as_ref(), proj, &schema, out_rows)?;
        order_output_rows(&mut out_rows, &schema, &order_exprs)?;
        truncate_hidden_columns(&mut schema, &mut out_rows, hidden);
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
                    let hits = cached.search_keys(&q, k, (k * 4).max(64));
                    let keys: Vec<Vec<u8>> = hits.iter().map(|(key, _)| key.clone()).collect();
                    let blobs = db.multi_get(keys).await?;
                    let mut rows = Vec::with_capacity(blobs.len());
                    for bytes in blobs.into_iter().flatten() {
                        rows.push(
                            bincode::deserialize::<Vec<Value>>(&bytes)
                                .map_err(|e| Error::Storage(e.to_string()))?,
                        );
                    }
                    // Order the candidate set by exact distance for a clean top-k.
                    sort_full_rows(&mut rows, &def.schema, &resolved, &db.cancel_token())?;
                    rows.truncate(k);
                    let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
                    return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
                }
            }
        }

        // PK-ordered LIMIT fast path: when ORDER BY is a prefix of the primary
        // key (ascending) and a LIMIT is present, scan in clustered (PK) order
        // and stop as soon as enough matching rows are collected -- no full
        // scan, no sort. Skipped for selective filters (equality / fulltext),
        // where the index path reads far fewer rows than a clustered scan.
        if let Some(lim) = limit {
            if order_is_pk_asc_prefix(&def, &resolved) && !selective_filter(&def, filter.as_ref())?
            {
                let need = offset.saturating_add(lim);
                let prefix = data_prefix(&def.name);
                let mut rows: Vec<Vec<Value>> = Vec::with_capacity(need.min(4096));
                if !db.in_txn() {
                    // Autocommit: iterate clustered order in one read transaction,
                    // decoding straight from borrowed bytes and stopping as soon
                    // as `need` matches are collected (no batch copies).
                    let sch = def.schema.clone();
                    let f = filter.clone();
                    rows = db
                        .raw_db()
                        .scan_fold_until(prefix, rows, move |rows, _k, v| {
                            let row: Vec<Value> = rowdec::decode_row(v)?;
                            let keep = match &f {
                                Some(e) => predicate::matches(e, &sch, &row)?,
                                None => true,
                            };
                            if keep {
                                rows.push(row);
                            }
                            Ok(rows.len() < need)
                        })
                        .await?;
                } else {
                    // In a transaction: use the overlay-aware batch scan.
                    let mut cursor: Option<Vec<u8>> = None;
                    'scan: loop {
                        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 8192).await?;
                        if batch.is_empty() {
                            break;
                        }
                        let last = batch.len() < 8192;
                        cursor = batch.last().map(|(k, _)| k.clone());
                        for (_, v) in batch {
                            let row: Vec<Value> = rowdec::decode_row(&v)?;
                            if let Some(f) = &filter {
                                if !predicate::matches(f, &def.schema, &row)? {
                                    continue;
                                }
                            }
                            rows.push(row);
                            if rows.len() >= need {
                                break 'scan;
                            }
                        }
                        if last {
                            break;
                        }
                    }
                }
                apply_offset_limit(&mut rows, offset, limit);
                let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
                return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
            }
        }

        // Reverse PK-ordered LIMIT fast path: `ORDER BY <pk prefix> DESC LIMIT n`
        // (autocommit). Walk the clustered keyspace backwards, apply the residual
        // WHERE, and stop once `offset + n` rows are collected -- no full scan, no
        // sort. The primary key is never NULL, so the reverse walk is a complete
        // ordering. A residual filter is capped by `ordered_scan_budget`; if it is
        // too selective to fill `need` within budget we fall through to the sorter.
        if let Some(lim) = limit {
            if !db.in_txn()
                && order_is_pk_prefix(&def, &resolved, false)
                && !selective_filter(&def, filter.as_ref())?
            {
                let need = offset.saturating_add(lim);
                let prefix = data_prefix(&def.name);
                let sch = def.schema.clone();
                let f = filter.clone();
                // With no residual filter, skip the first `offset` rows without
                // decoding them and collect just `lim`; otherwise collect `need`
                // and slice locally (each row must be filter-checked to count).
                let (skip, want) = if f.is_none() && offset > 0 {
                    (offset, lim)
                } else {
                    (0, need)
                };
                let budget = if f.is_some() {
                    ordered_scan_budget(need)
                } else {
                    usize::MAX
                };
                let init = OrderedWalk {
                    rows: Vec::with_capacity(want.min(4096)),
                    examined: 0,
                    need: want,
                    budget,
                    budget_hit: false,
                };
                let walk = db
                    .raw_db()
                    .scan_fold_rev_until(prefix, skip, init, move |w, _k, v| {
                        ordered_walk_step(w, v, &f, &sch)
                    })
                    .await?;
                if !walk.budget_hit {
                    let mut rows = walk.rows;
                    if skip > 0 {
                        rows.truncate(lim);
                    } else {
                        apply_offset_limit(&mut rows, offset, limit);
                    }
                    let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
                    return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
                }
            }
        }

        // Indexed ORDER BY ... LIMIT fast path: `ORDER BY <indexed col> [ASC|DESC]
        // LIMIT n` (autocommit). Walk the secondary index in (reverse) key order,
        // following each entry to its row, apply the residual WHERE, and stop at
        // `offset + n` -- ordered top-N without sorting the table. A selective
        // residual falls back via the budget (see above). For a nullable single-
        // column index the walk misses NULL-keyed rows, so the NULL block is
        // spliced in: first for ASC, last for DESC.
        if let Some(lim) = limit {
            if !db.in_txn() && !selective_filter(&def, filter.as_ref())? {
                if let Some(plan) = secondary_order_plan(&def, &resolved) {
                    let need = offset.saturating_add(lim);
                    let iprefix = index::index_scan_prefix(&def.name, &plan.index);
                    let has_filter = filter.is_some();
                    let walk_budget = if has_filter {
                        ordered_scan_budget(need)
                    } else {
                        usize::MAX
                    };

                    // Index walk over the non-NULL rows, in order, residual-filtered.
                    // `skip` steps over that many leading rows without a row lookup
                    // (used only with no residual filter) for a cheap deep OFFSET.
                    let run_walk = |skip: usize, want: usize| {
                        let sch = def.schema.clone();
                        let f = filter.clone();
                        let iprefix = iprefix.clone();
                        async move {
                            db.raw_db()
                                .scan_index_ordered_fold(
                                    iprefix,
                                    plan.rev,
                                    skip,
                                    OrderedWalk {
                                        rows: Vec::with_capacity(want.min(4096)),
                                        examined: 0,
                                        need: want,
                                        budget: walk_budget,
                                        budget_hit: false,
                                    },
                                    move |w, _dk, v| ordered_walk_step(w, v, &f, &sch),
                                )
                                .await
                        }
                    };

                    // Two-range walk for a NULL-indexing index: the value entries
                    // and the `indexnull::` NULL entries, in one snapshot. For ASC
                    // the NULL prefix comes first (NULLs sort first); for DESC the
                    // value prefix comes first (NULLs last). Both give the exact
                    // MySQL ordering including a PK tiebreaker.
                    let nprefix = index::indexnull_scan_prefix(&def.name, &plan.index);
                    let run_two = |skip: usize, want: usize| {
                        let sch = def.schema.clone();
                        let f = filter.clone();
                        let (first, second) = if plan.rev {
                            (iprefix.clone(), nprefix.clone())
                        } else {
                            (nprefix.clone(), iprefix.clone())
                        };
                        async move {
                            db.raw_db()
                                .scan_two_ordered_fold(
                                    first,
                                    second,
                                    plan.rev,
                                    skip,
                                    OrderedWalk {
                                        rows: Vec::with_capacity(want.min(4096)),
                                        examined: 0,
                                        need: want,
                                        budget: walk_budget,
                                        budget_hit: false,
                                    },
                                    move |w, _dk, v| ordered_walk_step(w, v, &f, &sch),
                                )
                                .await
                        }
                    };

                    let mut result: Option<Vec<Vec<Value>>> = None;
                    // Whether the result rows already have OFFSET applied (via the
                    // index-level skip) and so must not be offset again below.
                    let mut paged = false;
                    if plan.null_mode == NullMode::None {
                        let (skip, want) = if !has_filter && offset > 0 {
                            (offset, lim)
                        } else {
                            (0, need)
                        };
                        let walk = run_walk(skip, want).await?;
                        if !walk.budget_hit {
                            paged = skip > 0;
                            result = Some(walk.rows);
                        }
                    } else if plan.null_mode == NullMode::Indexed {
                        // Complete walk (value entries + stored NULL entries) --
                        // correct for both directions and PK tiebreakers, with a
                        // cheap deep OFFSET via the shared skip.
                        let (skip, want) = if !has_filter && offset > 0 {
                            (offset, lim)
                        } else {
                            (0, need)
                        };
                        let walk = run_two(skip, want).await?;
                        if !walk.budget_hit {
                            paged = skip > 0;
                            result = Some(walk.rows);
                        }
                    } else if !plan.rev && plan.has_tiebreaker {
                        // ASC with a tiebreaker on a nullable column: the NULL block
                        // sorts first and would need tiebreaker ordering within it,
                        // which the walk cannot supply cheaply -- leave `result`
                        // None to fall through to the sorter.
                    } else if !plan.rev {
                        // ASC: NULLs sort first. Collect the NULL block, then fill
                        // the remainder from the ascending index walk.
                        let null_budget = ordered_scan_budget(need);
                        let (nulls, null_bail) =
                            collect_null_rows(db, &def, plan.col, &filter, need, null_budget)
                                .await?;
                        if !null_bail {
                            let remaining = need.saturating_sub(nulls.len());
                            let walk = if remaining > 0 {
                                run_walk(0, remaining).await?
                            } else {
                                OrderedWalk {
                                    rows: Vec::new(),
                                    examined: 0,
                                    need: 0,
                                    budget: walk_budget,
                                    budget_hit: false,
                                }
                            };
                            if !walk.budget_hit {
                                let mut rows = nulls;
                                rows.extend(walk.rows);
                                result = Some(rows);
                            }
                        }
                    } else {
                        // DESC: NULLs sort last. Fill from the descending index
                        // walk; only if it is exhausted below `need` do NULLs enter
                        // the top-N, so append the NULL block then.
                        let walk = run_walk(0, need).await?;
                        if !walk.budget_hit {
                            if walk.rows.len() >= need {
                                result = Some(walk.rows);
                            } else if plan.has_tiebreaker {
                                // The NULL block would enter the top-N and needs
                                // tiebreaker ordering within it; fall through to the
                                // sorter instead.
                            } else {
                                let remaining = need - walk.rows.len();
                                let null_budget = ordered_scan_budget(need);
                                let (nulls, null_bail) = collect_null_rows(
                                    db,
                                    &def,
                                    plan.col,
                                    &filter,
                                    remaining,
                                    null_budget,
                                )
                                .await?;
                                if !null_bail {
                                    let mut rows = walk.rows;
                                    rows.extend(nulls);
                                    result = Some(rows);
                                }
                            }
                        }
                    }

                    if let Some(mut rows) = result {
                        if paged {
                            rows.truncate(lim);
                        } else {
                            apply_offset_limit(&mut rows, offset, limit);
                        }
                        let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
                        return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
                    }
                }
            }
        }

        // Memory-bounded ORDER BY for the non-accelerable autocommit case:
        // stream the filtered rows and sort with a top-N heap (when LIMIT is
        // small) or an external merge sort that spills to disk (OOM safety),
        // instead of materialising the whole result set.
        if !resolved.is_empty() && !accelerable(&def, filter.as_ref())? {
            // Stream the (transaction-visible) rows through a spilling sorter so a
            // large ORDER BY stays memory-bounded. Uses the Session's scan_batch,
            // which merges the MVCC snapshot with the transaction's own overlay,
            // so this is correct in autocommit AND inside a transaction (the old
            // code fell back to a full in-memory sort while in a transaction).
            let prefix = data_prefix(&def.name);
            let mut cursor: Option<Vec<u8>> = None;
            let asc: Vec<bool> = resolved.iter().map(|(_, a)| *a).collect();
            let colls: Vec<elyra_core::Collation> = resolved
                .iter()
                .map(|(e, _)| expr_collation(e, &def.schema))
                .collect();
            let mut sorter =
                crate::sort::Sorter::new(asc, colls, offset, limit, crate::sort::sort_max_rows());
            // Late materialisation: with a small LIMIT nearly every scanned row
            // loses the top-N admission test, so decode only the columns the
            // filter and the ORDER BY keys read, and pay for the full row (every
            // TEXT column is a String allocation) only once a row is admitted.
            // `None` = an expression we can't attribute to columns -> decode all.
            let probe = order_probe_mask(&def.schema, filter.as_ref(), &resolved);
            let ncols = def.schema.columns.len();
            let mut probe_buf: Vec<Value> = Vec::with_capacity(ncols);
            let mut keys: Vec<Value> = Vec::with_capacity(resolved.len());
            loop {
                let batch = db.scan_batch(prefix.clone(), cursor.clone(), 8192).await?;
                if batch.is_empty() {
                    break;
                }
                let last = batch.len() < 8192;
                cursor = batch.last().map(|(k, _)| k.clone());
                for (_, v) in batch {
                    // Probe row: either the projected subset (unread columns are
                    // NULL placeholders at their original positions) or, when we
                    // couldn't build a mask, the fully decoded row.
                    match &probe {
                        Some(mask) => {
                            rowdec::decode_projected_into(&v, ncols, mask, &mut probe_buf)?
                        }
                        None => {
                            probe_buf = bincode::deserialize(&v)
                                .map_err(|e| Error::Storage(e.to_string()))?
                        }
                    }
                    if let Some(f) = &filter {
                        if !predicate::matches(f, &def.schema, &probe_buf)? {
                            continue;
                        }
                    }
                    keys.clear();
                    for (e, _) in &resolved {
                        keys.push(predicate::eval_row(e, &def.schema, &probe_buf)?);
                    }
                    if !sorter.admits(&keys) {
                        continue;
                    }
                    let row: Vec<Value> = match &probe {
                        Some(_) => rowdec::decode_row(&v)?,
                        // Already the full row -- take it and leave a fresh
                        // buffer behind for the next iteration.
                        None => std::mem::take(&mut probe_buf),
                    };
                    sorter.push(std::mem::take(&mut keys), row)?;
                }
                if last {
                    break;
                }
            }
            let rows = sorter.finish()?;
            let (schema, out) = project_exprs(&select.projection, &def.schema, &rows)?;
            return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
        }

        let mut rows = scan_rows(db, &def, filter.as_ref()).await?;
        if !resolved.is_empty() {
            sort_full_rows(&mut rows, &def.schema, &resolved, &db.cancel_token())?;
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
        let mut check = db.cancel_check();
        rows = cpu_bound(|| -> Result<Vec<Vec<Value>>> {
            let mut kept = Vec::with_capacity(rows.len());
            for row in rows.into_iter() {
                check.tick()?;
                if predicate::matches(f, &schema, &row)? {
                    kept.push(row);
                }
            }
            Ok(kept)
        })?;
    }

    // Aggregation / grouping.
    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        // Aggregating and ordering materialised rows is pure CPU work.
        let (projection, hidden) = aggregate_projection_with_hidden(
            &select.projection,
            select.having.as_ref(),
            &order_exprs,
            &schema,
        );
        let (osch, orows) = cpu_bound(|| -> Result<(Schema, Vec<Vec<Value>>)> {
            let (mut osch, orows) = aggregate::run(&schema, &projection, &group_by, rows)?;
            let mut orows = apply_having(select.having.as_ref(), &projection, &osch, orows)?;
            order_output_rows(&mut orows, &osch, &order_exprs)?;
            truncate_hidden_columns(&mut osch, &mut orows, hidden);
            apply_offset_limit(&mut orows, offset, limit);
            Ok((osch, orows))
        })?;
        return Ok(QueryResult::Rows(RowStream::literal(osch, orows)));
    }

    // ORDER BY + projection.
    let resolved = resolve_order_aliases(&order_exprs, &select.projection, &schema);
    let cancel = db.cancel_token();
    let (osch, out) = cpu_bound(|| -> Result<(Schema, Vec<Vec<Value>>)> {
        if !resolved.is_empty() {
            sort_full_rows(&mut rows, &schema, &resolved, &cancel)?;
        }
        apply_offset_limit(&mut rows, offset, limit);
        project_exprs(&select.projection, &schema, &rows)
    })?;
    Ok(QueryResult::Rows(RowStream::literal(osch, out)))
}

/// Streaming index nested-loop join for the common shape
/// `SELECT ... FROM driving JOIN partner ON driving.k = partner.<pk|indexed>
///  [WHERE ...] LIMIT n` with no GROUP BY, aggregate, ORDER BY or DISTINCT.
///
/// Scans the driving table incrementally, probes the indexed partner per row,
/// applies the residual WHERE and stops as soon as `offset + limit` output rows
/// exist -- bounded memory and early termination instead of materialising the
/// whole join. Returns `None` when the query does not fit this shape, so the
/// caller falls back to the materialising `join_select` (no behaviour change for
/// anything else).
async fn streaming_nlj_select(
    db: &Session,
    select: &Select,
    filter: Option<&Expr>,
    offset: usize,
    limit: usize,
) -> Result<Option<QueryResult>> {
    // Reads inside a transaction must see the write overlay -> materialising path.
    if db.in_txn() || select.distinct.is_some() || select.having.is_some() || select.from.len() != 1
    {
        return Ok(None);
    }
    let twj = &select.from[0];
    if twj.joins.len() != 1
        || !stored_table_factor(&twj.relation)
        || !stored_table_factor(&twj.joins[0].relation)
    {
        return Ok(None);
    }
    let join = &twj.joins[0];
    let (kind, on) = join_kind(&join.join_operator)?;
    if !matches!(kind, JoinKind::Inner | JoinKind::Left) {
        return Ok(None);
    }
    let Some(on) = on else { return Ok(None) };
    let (ddef, dcols) = resolve_table(db, &twj.relation).await?;
    let (pdef, pcols) = resolve_table(db, &join.relation).await?;
    let driving_schema = Schema::new(dcols.clone());
    let partner_schema = Schema::new(pcols.clone());
    let Some((driving_key, pcol)) = equi_nlj(&on, &driving_schema, &partner_schema) else {
        return Ok(None);
    };
    if !(pdef.pk_cols == [pcol] || index::index_on(&pdef, pcol).is_some()) {
        return Ok(None);
    }

    // Combined schema: driving columns then partner columns (matches build_from).
    let mut all_cols = dcols;
    all_cols.extend(pcols.clone());
    let schema = Schema::new(all_cols);
    let plen = pcols.len();
    let left_outer = kind == JoinKind::Left;
    let want = offset.saturating_add(limit);

    let keep = |row: &[Value]| -> Result<bool> {
        match filter {
            Some(f) => predicate::matches(f, &schema, row),
            None => Ok(true),
        }
    };

    let prefix = data_prefix(&ddef.name);
    let mut cursor: Option<Vec<u8>> = None;
    let mut out: Vec<Vec<Value>> = Vec::new();
    'outer: loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 4096;
        cursor = batch.last().map(|(k, _)| k.clone());
        for (_, v) in batch {
            let l: Vec<Value> = rowdec::decode_row(&v)?;
            let key = predicate::eval_row(&driving_key, &driving_schema, &l)?;
            let matches = if key.is_null() {
                Vec::new()
            } else {
                lookup_rows_by_eq(db, &pdef, pcol, &key).await?
            };
            let matched = !matches.is_empty();
            for m in matches {
                let mut combined = l.clone();
                combined.extend(m);
                if keep(&combined)? {
                    out.push(combined);
                    if out.len() >= want {
                        break 'outer;
                    }
                }
            }
            if left_outer && !matched {
                let mut combined = l.clone();
                combined.extend(std::iter::repeat_n(Value::Null, plen));
                if keep(&combined)? {
                    out.push(combined);
                    if out.len() >= want {
                        break 'outer;
                    }
                }
            }
        }
        if last {
            break;
        }
    }

    apply_offset_limit(&mut out, offset, Some(limit));
    let (osch, rows) = project_exprs(&select.projection, &schema, &out)?;
    Ok(Some(QueryResult::Rows(RowStream::literal(osch, rows))))
}

/// One built step of a left-deep streaming hash join: the partner relation
/// materialised into a hash table, plus the info needed to probe it from the
/// accumulated left row.
/// How one chain step finds its partner rows for a given left row.
enum Partner {
    /// Equi-join: partner rows in a hash table keyed by the collated join key,
    /// probed with `probe_key` evaluated over the left schema.
    Keyed(Box<KeyedPartner>),
    /// Every left row is paired with every partner row: a comma cross join (no
    /// condition at all) or a join whose `ON` has no equality to hash on, in
    /// which case [`JoinChainStep::cond`] filters the pairs. Rows are stored
    /// flat, `plen` values each, as in [`Slot`].
    ///
    /// This is what lets `FROM a, b, c WHERE ...` and `ON a.id < b.id` stream.
    /// The partners are materialised -- as the keyed steps already do -- but the
    /// *product* never is, and the product is what explodes: 4000 rows three ways
    /// is 1.3 billion combinations, which previously grew the process to 97 GB
    /// before the OS killed it. Streaming it into the spilling
    /// sorter/aggregator keeps memory flat.
    All(Vec<Value>),
}

/// A collation-encoded join key.
///
/// Short keys -- every integer, date and short string, i.e. nearly every join key
/// there is -- live inline, so building the hash table costs no allocation per
/// partner row. A `Vec<u8>` key meant 200k allocations *and* 200k frees on a
/// 200k-row join, and the teardown showed up in profiles as plainly as the build.
///
/// `Borrow<[u8]>` lets the table be probed with a plain slice, so probing an
/// already-encoded key allocates nothing either. That makes `Hash` consistency
/// load-bearing: both variants hash exactly as `[u8]` does, or a lookup by slice
/// would miss an inline key and the join would silently lose rows.
#[derive(Clone, Debug)]
enum JoinKey {
    Inline { len: u8, buf: [u8; JoinKey::INLINE] },
    Heap(Box<[u8]>),
}

impl JoinKey {
    const INLINE: usize = 22;

    fn from_bytes(b: &[u8]) -> Self {
        if b.len() <= Self::INLINE {
            let mut buf = [0u8; Self::INLINE];
            buf[..b.len()].copy_from_slice(b);
            JoinKey::Inline {
                len: b.len() as u8,
                buf,
            }
        } else {
            JoinKey::Heap(b.into())
        }
    }

    fn as_bytes(&self) -> &[u8] {
        match self {
            JoinKey::Inline { len, buf } => &buf[..*len as usize],
            JoinKey::Heap(b) => b,
        }
    }
}

impl std::borrow::Borrow<[u8]> for JoinKey {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}
impl PartialEq for JoinKey {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl Eq for JoinKey {}
impl std::hash::Hash for JoinKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Delegate to `[u8]`'s own hashing: `Borrow<[u8]>` requires that a key
        // and its borrowed form hash identically.
        std::hash::Hash::hash(self.as_bytes(), state)
    }
}

/// The partner rows sharing one join key, stored flat: `plen` values per row in
/// one allocation. A `Vec<Vec<Value>>` cost three allocations per partner row
/// (the key, the row, and the vector holding it), and on a 200k-row 1:1 join
/// their *teardown* alone was a third of the query -- for a unique join key, the
/// inner vector existed to hold exactly one row.
type Slot = Vec<Value>;

/// Hash table and probe expression for an equi-join step. Boxed inside [`Partner`]
/// so a cross-join step (a plain `Vec`) does not carry its size.
struct KeyedPartner {
    table: std::collections::HashMap<JoinKey, Slot>,
    probe_key: Expr,
    /// Position of `probe_key` in the left schema when it is a plain column
    /// reference (nearly always). Resolving the name instead costs a lookup --
    /// and, for a qualified reference, a `String` -- on every driving row.
    probe_col: Option<usize>,
    coll: elyra_core::Collation,
}

impl KeyedPartner {
    /// The join key for one left row.
    fn probe(&self, left_schema: &Schema, left: &[Value]) -> Result<Value> {
        match self.probe_col {
            Some(i) => Ok(left.get(i).cloned().unwrap_or(Value::Null)),
            None => predicate::eval_row(&self.probe_key, left_schema, left),
        }
    }

    /// Partner rows for a (non-NULL) key, flat (`plen` values per row). The key
    /// is encoded into `buf` so probing does not allocate per driving row.
    fn lookup<'a>(&'a self, key: &Value, buf: &mut Vec<u8>) -> Option<&'a [Value]> {
        buf.clear();
        key.push_collation_key_coll(buf, self.coll);
        self.table.get(buf.as_slice()).map(|s| s.as_slice())
    }
}

/// Per-chain-depth scratch: the combined row being built, and the encoded probe
/// key. Owned by the caller so a whole join costs a fixed number of buffers
/// rather than two allocations per emitted row.
#[derive(Default)]
struct ChainBuf {
    row: Vec<Value>,
    key: Vec<u8>,
}

struct JoinChainStep {
    partner: Partner,
    /// Schema of the accumulated left side at this step (driving ++ prior partners).
    left_schema: Schema,
    /// Number of partner columns (for LEFT-join NULL extension).
    plen: usize,
    left_outer: bool,
    /// The part of the `ON` condition that is not the hash key: everything, for a
    /// non-equi join; the surviving conjuncts, when one equality was hashed on;
    /// `None` for a plain equi or cross join. Evaluated over the combined
    /// left ++ partner row, whose schema comes with it.
    ///
    /// It is an `ON` condition, not a `WHERE`: a pair it rejects is *unmatched*,
    /// so a LEFT join still NULL-extends the left row. Applying it as a filter
    /// instead would silently drop those rows.
    cond: Option<(Expr, Schema)>,
    /// Partner positions the query reads (`None` = all of them). Every other
    /// position is `NULL` in the stored partner rows -- that is what late
    /// materialisation (ESQL-49) put there -- so the combined row only needs
    /// those positions written per combination, the rest staying at the `NULL`
    /// laid down once per driving row. On a 12-column partner under `COUNT(*)`
    /// that is one write per emitted row instead of twelve.
    pcopy: Option<Vec<usize>>,
}

/// Rewrite a two-table `A RIGHT JOIN B ON c` into the equivalent
/// `B LEFT JOIN A ON c` so the streaming left-deep hash join can handle it
/// (drive from B, keep every B row, NULL-extend unmatched A). The caller must
/// reorder the produced `(B-cols, A-cols)` rows back to the query's `(A, B)`
/// column order via [`right_join_reorder`]. Returns `None` for anything else
/// (multi-join chains, non-table relations, non-`ON` constraints) — those keep
/// the materialising path.
fn rewrite_right_join(twj: &TableWithJoins) -> Option<TableWithJoins> {
    if twj.joins.len() != 1 || !matches!(twj.relation, TableFactor::Table { .. }) {
        return None;
    }
    let join = &twj.joins[0];
    if !matches!(join.relation, TableFactor::Table { .. }) {
        return None;
    }
    let JoinOperator::RightOuter(constraint) = &join.join_operator else {
        return None;
    };
    Some(TableWithJoins {
        relation: join.relation.clone(),
        joins: vec![sqlparser::ast::Join {
            relation: twj.relation.clone(),
            global: join.global,
            join_operator: JoinOperator::LeftOuter(constraint.clone()),
        }],
    })
}

/// Permutation mapping physical `(B-cols[0..nb], A-cols[0..na])` positions to the
/// query's logical `(A-cols, B-cols)` order (for a rewritten RIGHT join).
fn right_join_reorder(nb: usize, na: usize) -> Vec<usize> {
    let mut perm = Vec::with_capacity(na + nb);
    perm.extend(nb..nb + na); // A columns (physically after B)
    perm.extend(0..nb); // B columns (physically first)
    perm
}

/// Reorder one row's columns by `perm` (logical position i <- physical perm[i])
/// into a reusable buffer: this runs per emitted row on the streaming join paths.
fn apply_perm_into(row: &[Value], perm: &[usize], out: &mut Vec<Value>) {
    out.clear();
    out.reserve(perm.len());
    out.extend(perm.iter().map(|&i| row[i].clone()));
}

/// The built chain: driving schema, the per-join steps, the combined output
/// schema, and the decode mask for the driving table's own rows (`None` =
/// decode every column).
struct JoinChain {
    dschema: Schema,
    steps: Vec<JoinChainStep>,
    schema: Schema,
    dmask: Option<Vec<bool>>,
}

/// One resolved relation of the chain, before its rows are read: resolving is
/// catalog-only, so the whole chain (and therefore the combined schema) is known
/// before a single row is decoded -- which is what lets the caller say which
/// columns it will actually read.
struct PendingStep {
    def: TableDef,
    pcols: Vec<ColumnDef>,
    left_schema: Schema,
    /// `None` when there is no equality to hash on (a comma cross join, or an
    /// `ON` like `a.id < b.id`); otherwise (probe key, partner key, collation).
    keys: Option<(Expr, Expr, elyra_core::Collation)>,
    /// Residual `ON` condition over the left ++ partner row, with its schema.
    cond: Option<(Expr, Schema)>,
    left_outer: bool,
}

/// Build a left-deep streaming hash join for a `TableWithJoins` (a driving table
/// plus a chain of `JOIN`s). Each partner is materialised into a hash table
/// keyed by the equi-join key connecting it to the accumulated left side; the
/// driving table is left to be streamed by the caller. Returns `None` when the
/// shape is not a plain-table INNER/LEFT equi-join chain we can stream.
///
/// `needed` is asked, once the combined schema is known but before any row is
/// read, which columns the query actually reads; the partners are then decoded
/// with only those columns materialised (see [`JoinChain::dmask`] for the
/// driving side).
async fn build_join_chain(
    db: &Session,
    from: &[TableWithJoins],
    twj: &TableWithJoins,
    needed: &(dyn Fn(&Schema) -> Option<Vec<bool>> + Sync),
) -> Result<Option<JoinChain>> {
    // Something must be joined: either explicit JOINs on the first entry, or further
    // comma-separated tables.
    if !stored_table_factor(&twj.relation) || (twj.joins.is_empty() && from.len() < 2) {
        return Ok(None);
    }
    let (_ddef, dcols) = resolve_table(db, &twj.relation).await?;
    let dschema = Schema::new(dcols.clone());
    let mut left_cols = dcols;
    let mut pending: Vec<PendingStep> = Vec::with_capacity(twj.joins.len());

    // --- Resolve pass: catalog only, no rows read. ---
    for join in &twj.joins {
        if !stored_table_factor(&join.relation) {
            return Ok(None);
        }
        let (kind, on) = join_kind(&join.join_operator)?;
        if !matches!(kind, JoinKind::Inner | JoinKind::Left) {
            return Ok(None);
        }
        // No `ON` is only unconditional for an explicit CROSS JOIN (or `JOIN`
        // with no constraint). USING and NATURAL also arrive here without an
        // `ON` expression but are *equi* joins, so they must keep declining --
        // treating them as cross joins would return a cartesian product.
        if on.is_none()
            && !matches!(
                join.join_operator,
                JoinOperator::CrossJoin | JoinOperator::Inner(JoinConstraint::None)
            )
        {
            return Ok(None);
        }
        let (pdef, pcols) = resolve_table(db, &join.relation).await?;
        let left_schema = Schema::new(left_cols.clone());
        let pschema = Schema::new(pcols.clone());
        let mut through_cols = left_cols.clone();
        through_cols.extend(pcols.clone());
        let through = Schema::new(through_cols);

        // Prefer an equality to hash on. If the whole `ON` is not one, look for
        // an equality *conjunct* and keep the rest as a residual: `ON a.k = b.k
        // AND a.x > b.x` then still costs O(n+m) instead of O(n*m). With no
        // equality anywhere the step pairs every row and the `ON` filters --
        // O(n*m), which is what a non-equi join inherently is, but streamed, so
        // it answers instead of being refused by the materialising row cap.
        let (keys, cond) = match &on {
            None => (None, None),
            Some(on) => match equi_keys(on, &left_schema, &pschema) {
                Some((lkey, rkey)) => {
                    let coll = join_key_collation(&lkey, &left_schema, &rkey, &pschema);
                    (Some((lkey, rkey, coll)), None)
                }
                None => {
                    let mut parts = Vec::new();
                    split_and(on, &mut parts);
                    let hashable = parts
                        .iter()
                        .position(|p| equi_keys(p, &left_schema, &pschema).is_some());
                    match hashable {
                        Some(i) => {
                            let (lkey, rkey) = equi_keys(&parts[i], &left_schema, &pschema)
                                .expect("position() just matched");
                            let coll = join_key_collation(&lkey, &left_schema, &rkey, &pschema);
                            parts.remove(i);
                            let residual = parts.into_iter().reduce(|a, b| Expr::BinaryOp {
                                left: Box::new(a),
                                op: sqlparser::ast::BinaryOperator::And,
                                right: Box::new(b),
                            });
                            (
                                Some((lkey, rkey, coll)),
                                residual.map(|e| (e, through.clone())),
                            )
                        }
                        None => (None, Some((on.clone(), through.clone()))),
                    }
                }
            },
        };

        left_cols.extend(pcols.clone());
        pending.push(PendingStep {
            def: pdef,
            pcols,
            left_schema,
            keys,
            cond,
            left_outer: kind == JoinKind::Left,
        });
    }

    // Comma-separated tables after the first are cross joins: no condition, so every
    // left row pairs with every partner row. Streaming these is the point of the
    // unkeyed step -- the partners are materialised (bounded by their table size, as
    // the keyed steps already are) while the product, which is what explodes, is not.
    for extra in &from[1..] {
        if !stored_table_factor(&extra.relation) || !extra.joins.is_empty() {
            // A derived table, or a comma entry that itself carries JOINs: leave the
            // whole query to the materialising path rather than half-handle it.
            return Ok(None);
        }
        let (pdef, pcols) = resolve_table(db, &extra.relation).await?;
        let left_schema = Schema::new(left_cols.clone());
        left_cols.extend(pcols.clone());
        pending.push(PendingStep {
            def: pdef,
            pcols,
            left_schema,
            keys: None,
            cond: None,
            // A cross join has no unmatched case to preserve.
            left_outer: false,
        });
    }

    let combined = Schema::new(left_cols);

    // --- Late materialisation: which combined columns does the query read? ---
    // Everything outside the mask is decoded as a NULL placeholder at its own
    // position, so a join between two 12-column tables under `COUNT(*)` copies
    // 24 cheap NULLs per emitted row instead of allocating 24 Strings.
    let mut mask = needed(&combined);
    // Every step probes its hash table with a key evaluated over the accumulated
    // left row, so those columns must be materialised even when the query never
    // selects them -- otherwise the key reads as NULL and the join matches
    // nothing. `left_schema` is a prefix of the combined schema, so its indices
    // are combined indices.
    if let Some(m) = mask.as_mut() {
        let mut give_up = false;
        for p in &pending {
            // The probe key (over the accumulated left row) and any residual ON
            // condition (over left ++ partner) both read columns the projection
            // may never mention. `left_schema` and the residual's schema are
            // prefixes of the combined schema, so their indices are combined
            // indices and can be marked directly.
            let mut refs = Vec::new();
            let ok = p
                .keys
                .as_ref()
                .map(|(lkey, _, _)| collect_col_refs(lkey, &p.left_schema, &mut refs))
                .unwrap_or(true)
                && p.cond
                    .as_ref()
                    .map(|(c, sch)| collect_col_refs(c, sch, &mut refs))
                    .unwrap_or(true);
            if !ok {
                give_up = true;
                break;
            }
            for i in refs {
                if i < m.len() {
                    m[i] = true;
                }
            }
        }
        if give_up {
            mask = None;
        }
    }
    let mask = mask;
    let dlen = dschema.columns.len();
    let dmask = mask.as_ref().map(|m| m[..dlen].to_vec());

    // --- Materialise pass: read each partner's rows, projected. ---
    let mut steps = Vec::with_capacity(pending.len());
    let mut off = dlen;
    for p in pending {
        let plen = p.pcols.len();
        let pschema = Schema::new(p.pcols);
        // The partner's own join key must be decoded whatever the query reads,
        // or the hash table would be keyed on NULL.
        let pmask = mask.as_ref().and_then(|m| {
            let mut sub = m[off..off + plen].to_vec();
            if let Some((_, rkey, _)) = &p.keys {
                let mut refs = Vec::new();
                if !collect_col_refs(rkey, &pschema, &mut refs) {
                    return None; // key we can't attribute -> decode all of it
                }
                for i in refs {
                    if i < sub.len() {
                        sub[i] = true;
                    }
                }
            }
            Some(sub)
        });
        // Which partner positions the combined row has to carry per combination --
        // but only when writing just those is cheaper than copying the whole
        // partner half.
        //
        // Copying the half is a tight clone loop into reserved space; writing
        // single positions costs a bounds check and a drop of the previous value
        // each, which measured ~10x more per value on a 40M-row 1:N join (a
        // narrow 7-column partner got *slower* with selective copying: 742 ->
        // 898 ms, while a 16-column one got faster: 1196 -> 945 ms). So take
        // whichever the widths say is cheaper. This holds only because ESQL-49
        // already made the skipped values `NULL` -- cloning them is cheap; if
        // they were still `String`s the selective path would always win.
        const SELECTIVE_COPY_WEIGHT: usize = 10;
        let pcopy: Option<Vec<usize>> = pmask
            .as_ref()
            .map(|m| -> Vec<usize> {
                m.iter()
                    .enumerate()
                    .filter(|(_, &want)| want)
                    .map(|(i, _)| i)
                    .collect()
            })
            .filter(|cols| cols.len() * SELECTIVE_COPY_WEIGHT < plen);
        let prefix = data_prefix(&p.def.name);
        let mut cursor: Option<Vec<u8>> = None;
        let mut decoded: Vec<Value> = Vec::with_capacity(plen);

        match p.keys {
            Some((lkey, rkey, coll)) => {
                // Materialise the partner into a hash table keyed by its join key.
                let mut table: std::collections::HashMap<JoinKey, Slot> =
                    std::collections::HashMap::new();
                // The partner key is nearly always a plain column; resolve it once
                // instead of per partner row.
                let rcol = expr_col_index(&rkey, &pschema);
                let mut kbuf: Vec<u8> = Vec::new();
                loop {
                    let batch = db.scan_batch(prefix.clone(), cursor.clone(), 8192).await?;
                    if batch.is_empty() {
                        break;
                    }
                    let last = batch.len() < 8192;
                    cursor = batch.last().map(|(k, _)| k.clone());
                    for (_, v) in batch {
                        // Decode into the reusable buffer, then *move* the values
                        // into the key's flat slot: no per-row allocation at all.
                        decode_partner_row(&v, plen, pmask.as_deref(), &mut decoded)?;
                        let key = match rcol {
                            Some(i) => decoded.get(i).cloned().unwrap_or(Value::Null),
                            None => predicate::eval_row(&rkey, &pschema, &decoded)?,
                        };
                        if key.is_null() {
                            continue; // a NULL key matches nothing
                        }
                        kbuf.clear();
                        key.push_collation_key_coll(&mut kbuf, coll);
                        match table.get_mut(kbuf.as_slice()) {
                            Some(slot) => slot.append(&mut decoded),
                            None => {
                                let mut slot: Slot = Vec::with_capacity(plen);
                                slot.append(&mut decoded);
                                table.insert(JoinKey::from_bytes(&kbuf), slot);
                            }
                        }
                    }
                    if last {
                        break;
                    }
                }
                steps.push(JoinChainStep {
                    partner: Partner::Keyed(Box::new(KeyedPartner {
                        table,
                        probe_col: expr_col_index(&lkey, &p.left_schema),
                        probe_key: lkey,
                        coll,
                    })),
                    left_schema: p.left_schema,
                    plen,
                    left_outer: p.left_outer,
                    cond: p.cond,
                    pcopy,
                });
            }
            None => {
                // Flat, `plen` values per row (as the keyed slots are).
                let mut rows: Vec<Value> = Vec::new();
                let mut nrows = 0usize;
                // A partner larger than the per-join row cap would defeat the purpose, so
                // decline and let the materialising path apply its budget instead.
                let cap = join_max_rows();
                loop {
                    let batch = db.scan_batch(prefix.clone(), cursor.clone(), 8192).await?;
                    if batch.is_empty() {
                        break;
                    }
                    let last = batch.len() < 8192;
                    cursor = batch.last().map(|(k, _)| k.clone());
                    for (_, v) in batch {
                        decode_partner_row(&v, plen, pmask.as_deref(), &mut decoded)?;
                        rows.append(&mut decoded);
                        nrows += 1;
                    }
                    if nrows > cap {
                        return Ok(None);
                    }
                    if last {
                        break;
                    }
                }
                steps.push(JoinChainStep {
                    partner: Partner::All(rows),
                    left_schema: p.left_schema,
                    plen,
                    left_outer: p.left_outer,
                    cond: p.cond,
                    pcopy,
                });
            }
        }
        off += plen;
    }

    Ok(Some(JoinChain {
        dschema,
        steps,
        schema: combined,
        dmask,
    }))
}

/// Run a CPU-bound stretch without monopolising an async worker thread.
///
/// Statement execution shares the runtime's workers with the connection listener
/// and every other session, so a long synchronous stretch (a join product, a sort,
/// an aggregation over materialised rows) makes the server unresponsive - new
/// connections are not even accepted. `block_in_place` hands the current worker
/// over to this work and lets the runtime bring up a replacement, so everything
/// else keeps being polled.
///
/// Unlike a deadline check this needs no configuration, so it protects the default
/// setup where no query timeout is set. Falls back to calling `f` directly on a
/// current-thread runtime (used by unit tests), where `block_in_place` is not
/// permitted.
fn cpu_bound<T>(f: impl FnOnce() -> T) -> T {
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}
/// Cooperative yielding for a long-running async loop.
///
/// Statement execution shares the async runtime's worker threads with the
/// connection listener and every other session, so a long synchronous stretch
/// inside one query makes the whole server unresponsive - including to *new*
/// connections, which never get accepted. Handing the worker back periodically
/// lets other tasks progress; the query resumes on its next poll, so throughput
/// is essentially unchanged while latency for everyone else stays bounded.
///
/// This is independent of the query deadline (`CancelCheck`): it keeps the server
/// responsive even when no timeout is configured, which is the default.
struct Pacer {
    tick: u32,
}

impl Pacer {
    /// Iterations between yields. Large enough that the yield is noise, small
    /// enough that no single query monopolises a worker for long.
    const INTERVAL: u32 = 256;

    fn new() -> Self {
        Self { tick: 0 }
    }

    #[inline]
    async fn tick(&mut self) {
        self.tick = self.tick.wrapping_add(1);
        if self.tick.is_multiple_of(Self::INTERVAL) {
            tokio::task::yield_now().await;
        }
    }
}

/// Decode one partner row into a reusable buffer, materialising only the columns
/// the query reads (`None` = all of them). The buffer always ends up `plen` long,
/// which is what lets the partner rows be stored flat.
fn decode_partner_row(
    bytes: &[u8],
    plen: usize,
    mask: Option<&[bool]>,
    out: &mut Vec<Value>,
) -> Result<()> {
    match mask {
        Some(m) => rowdec::decode_projected_into(bytes, plen, m, out)?,
        None => {
            *out = rowdec::decode_row(bytes)?;
        }
    }
    // A stored row whose arity differs from the schema (mid-migration) would
    // break the flat stride, so pad or truncate to the schema width -- the same
    // shape the decoder guarantees for a well-formed row.
    if out.len() != plen {
        out.resize(plen, Value::Null);
    }
    Ok(())
}

/// Decode one driving-table row, materialising only the columns the query reads
/// (`None` = all of them). Skipped columns become `Value::Null` at their own
/// position, so the combined row's layout is unchanged.
fn decode_driving_row(bytes: &[u8], ncols: usize, mask: Option<&[bool]>) -> Result<Vec<Value>> {
    match mask {
        Some(m) => rowdec::decode_projected(bytes, ncols, m),
        None => bincode::deserialize(bytes).map_err(|e| Error::Storage(e.to_string())),
    }
}

/// Stream one driving row's combinations through the chain, calling `emit` for each
/// completed row instead of collecting them.
///
/// Recursing per step keeps memory at O(chain depth) regardless of the product's
/// size. That is what lets `FROM a, b, c WHERE ...` run at all: one driving row of a
/// 4000x4000 cross product is 16 million combinations, so buffering even a single
/// row's expansion is not an option -- it previously grew the process to 97 GB before
/// the OS killed it.
/// Enter the chain, choosing the copy strategy once per query.
///
/// `SELECTIVE` is a const parameter rather than a per-row test because the mere
/// *presence* of the selective branch in the loop cost ~24% on a 40M-row join
/// where it was never taken (739 -> 917 ms): the function grew and the code
/// generated for the common path changed with it. Chains that cannot benefit are
/// therefore compiled without that branch at all.
fn stream_chain(
    selective: bool,
    left: &[Value],
    steps: &[JoinChainStep],
    bufs: &mut [ChainBuf],
    check: &mut elyra_core::cancel::CancelCheck,
    emit: &mut dyn FnMut(&[Value]) -> Result<()>,
) -> Result<()> {
    if selective {
        stream_join_chain::<true>(left, steps, bufs, check, emit)
    } else {
        stream_join_chain::<false>(left, steps, bufs, check, emit)
    }
}

/// True when at least one step reads few enough partner columns that writing
/// just those beats copying the partner half.
fn chain_is_selective(steps: &[JoinChainStep]) -> bool {
    steps.iter().any(|s| s.pcopy.is_some())
}

fn stream_join_chain<const SELECTIVE: bool>(
    left: &[Value],
    steps: &[JoinChainStep],
    bufs: &mut [ChainBuf],
    check: &mut elyra_core::cancel::CancelCheck,
    emit: &mut dyn FnMut(&[Value]) -> Result<()>,
) -> Result<()> {
    let Some((step, rest)) = steps.split_first() else {
        return emit(left);
    };
    // One scratch row per chain depth, owned by the caller: a 1:N join emits far
    // more combinations than it has partner keys, and allocating (and dropping) a
    // combined row for each of them was the largest per-row cost left after
    // ESQL-49. `emit` therefore borrows the row and clones only if it keeps it.
    let (buf, rest_bufs) = bufs
        .split_first_mut()
        .expect("one scratch buffer per chain step");
    let ChainBuf {
        row: buf,
        key: kbuf,
    } = buf;
    let matches: Option<&[Value]> = match &step.partner {
        Partner::Keyed(k) => {
            let key = k.probe(&step.left_schema, left)?;
            if key.is_null() {
                // A NULL join key matches nothing; a LEFT join still NULL-extends.
                None
            } else {
                k.lookup(&key, kbuf)
            }
        }
        // Cross join: every partner row pairs with this left row.
        Partner::All(rows) => Some(rows),
    };
    let mut matched = false;
    if let Some(rows) = matches {
        // The left half is the same for every partner row, so copy it once and
        // rewrite only the partner half per combination. With a fanout of 200
        // that is 199 copies of the left row saved out of every 200.
        let base = left.len();
        let stride = step.plen.max(1);
        buf.clear();
        buf.reserve(base + step.plen);
        buf.extend_from_slice(left);
        // The two copy strategies get a loop each, with everything they need in
        // locals: reading `step` per emitted row (or choosing the strategy there)
        // measured 20%+ on a 40M-row join, even when the choice never changed.
        let cond = step.cond.as_ref();
        match &step.pcopy {
            // Compiled away entirely unless this chain has a step that benefits.
            Some(cols) if SELECTIVE => {
                // Lay the partner half down as NULL once; only the read positions
                // are rewritten per combination, and they are rewritten every
                // time, so no value from a previous partner row can survive.
                buf.extend(std::iter::repeat_n(Value::Null, step.plen));
                for m in rows.chunks(stride) {
                    check.tick()?;
                    for &i in cols {
                        buf[base + i] = m[i].clone();
                    }
                    if let Some((c, sch)) = cond {
                        if !predicate::matches(c, sch, buf)? {
                            continue; // fails the ON condition: not a match
                        }
                    }
                    matched = true;
                    stream_join_chain::<SELECTIVE>(buf, rest, rest_bufs, check, emit)?;
                }
            }
            _ => {
                for m in rows.chunks(stride) {
                    check.tick()?;
                    buf.truncate(base);
                    buf.extend_from_slice(m);
                    if let Some((c, sch)) = cond {
                        if !predicate::matches(c, sch, buf)? {
                            continue; // fails the ON condition: not a match
                        }
                    }
                    matched = true;
                    stream_join_chain::<SELECTIVE>(buf, rest, rest_bufs, check, emit)?;
                }
            }
        }
    }

    if step.left_outer && !matched {
        // Rebuilt from scratch: the loop above may have written partner values
        // into `buf` for pairs the ON condition then rejected.
        buf.clear();
        buf.reserve(left.len() + step.plen);
        buf.extend_from_slice(left);
        buf.extend(std::iter::repeat_n(Value::Null, step.plen));
        stream_join_chain::<SELECTIVE>(buf, rest, rest_bufs, check, emit)?;
    }
    Ok(())
}

/// Streaming index nested-loop **aggregation** for
/// `SELECT ... aggregates ... FROM driving JOIN partner
///  ON driving.k = partner.<pk|indexed> [WHERE] GROUP BY ... [HAVING] [ORDER BY] [LIMIT]`.
///
/// Scans the driving table incrementally, probes the indexed partner per row,
/// applies the residual WHERE, and feeds each joined row into the spilling
/// aggregator (`SpillAgg`) -- so a large join followed by GROUP BY is bounded by
/// the group state (which spills), not by the full join output. The combined
/// schema (driving cols ++ partner cols) matches `build_from`'s index-NLJ path
/// and `join_select`, so projection/GROUP BY/HAVING resolve identically. Returns
/// `None` when the query does not fit this shape, so the caller falls back to
/// the materialising `join_select` (no behaviour change otherwise).
#[allow(clippy::too_many_arguments)]
async fn streaming_join_aggregate(
    db: &Session,
    select: &Select,
    filter: Option<&Expr>,
    group_by: &[Expr],
    order_exprs: &[(Expr, bool)],
    offset: usize,
    limit: Option<usize>,
) -> Result<Option<QueryResult>> {
    // Reads inside a transaction must see the write overlay -> materialising path.
    // `from.len() > 1` is a comma cross join, which build_join_chain now streams via
    // unkeyed steps; it declines anything it cannot handle, so this only needs to
    // reject an empty FROM.
    if db.in_txn() || select.distinct.is_some() || select.from.is_empty() {
        return Ok(None);
    }
    // A two-table RIGHT join is streamed by rewriting it to `B LEFT JOIN A` and
    // reordering the output columns back to (A, B) below.
    let swapped = rewrite_right_join(&select.from[0]);
    let twj = swapped.as_ref().unwrap_or(&select.from[0]);
    // Which combined columns does this aggregation read? Everything else is
    // decoded as a NULL placeholder, so `COUNT(*)` over a join of two wide
    // tables stops allocating a String per column per emitted row.
    //
    // A rewritten RIGHT join permutes the combined row after the chain, so the
    // mask (built against the pre-permutation layout) would not line up: decode
    // everything there rather than reason about the inverse permutation.
    // ORDER BY and HAVING are applied to the *grouped output*, not to combined
    // rows, so they contribute no columns here.
    let pruned = swapped.is_none();
    let needed = |combined: &Schema| -> Option<Vec<bool>> {
        if !pruned {
            return None;
        }
        let plan = aggregate::build_plan(combined, &select.projection, group_by).ok()?;
        let direct: Vec<usize> = plan
            .group_cols()
            .iter()
            .copied()
            .chain(plan.agg_input_cols())
            .collect();
        join_needed_mask(
            combined,
            filter,
            &select.projection,
            group_by,
            None,
            &[],
            &direct,
        )
    };
    // Build the (left-deep) join chain: each partner into a hash table, driving
    // streamed. Handles two or more tables.
    let Some(JoinChain {
        dschema,
        steps,
        schema,
        dmask,
    }) = build_join_chain(db, &select.from, twj, &needed).await?
    else {
        return Ok(None);
    };
    let dlen = dschema.columns.len();
    let (ddef, _) = resolve_table(db, &twj.relation).await?;

    let reorder: Option<Vec<usize>> = swapped.as_ref().map(|_| {
        let nb = dschema.columns.len();
        right_join_reorder(nb, schema.columns.len() - nb)
    });
    let schema = match &reorder {
        Some(perm) => Schema::new(perm.iter().map(|&i| schema.columns[i].clone()).collect()),
        None => schema,
    };

    // Build the aggregation plan; if it isn't a plain aggregate/group plan we can
    // stream, fall back to join_select (which is the authoritative path and will
    // reproduce any real error).
    let plan = match aggregate::build_plan(&schema, &select.projection, group_by) {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };
    let extend = !plan.arg_exprs().is_empty();

    let keep = |row: &[Value]| -> Result<bool> {
        match filter {
            Some(f) => predicate::matches(f, &schema, row),
            None => Ok(true),
        }
    };

    // Stream the driving table, expanding each row through the chain and feeding
    // the spilling aggregator -- so a large join + GROUP BY is bounded by the
    // group state (which spills), not the join output size.
    let mut sa = SpillAgg::new(&plan);
    let prefix = data_prefix(&ddef.name);
    let mut cursor: Option<Vec<u8>> = None;
    let selective = chain_is_selective(&steps);
    let mut bufs: Vec<ChainBuf> = (0..steps.len()).map(|_| ChainBuf::default()).collect();
    let mut permbuf: Vec<Value> = Vec::new();
    let mut fedbuf: Vec<Value> = Vec::new();
    let mut check = db.cancel_check();
    let mut pacer = Pacer::new();
    loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 4096;
        cursor = batch.last().map(|(k, _)| k.clone());
        for (_, v) in batch {
            // Expanding one driving row is synchronous and can be large, so give
            // the runtime a chance to run other tasks between rows.
            pacer.tick().await;
            let l: Vec<Value> = decode_driving_row(&v, dlen, dmask.as_deref())?;
            // Stream the expansion straight into the aggregator: a cross join's
            // product must never be buffered, not even for one driving row. The
            // row is borrowed, so an aggregate that keeps nothing (COUNT(*), SUM)
            // costs no allocation per emitted row at all.
            stream_chain(
                selective,
                &l,
                &steps,
                &mut bufs,
                &mut check,
                &mut |combined| {
                    let combined: &[Value] = match &reorder {
                        Some(perm) => {
                            apply_perm_into(combined, perm, &mut permbuf);
                            &permbuf
                        }
                        None => combined,
                    };
                    if !keep(combined)? {
                        return Ok(());
                    }
                    if extend {
                        plan.extend_row_into(combined, &mut fedbuf)?;
                        sa.feed_extended(&fedbuf)
                    } else {
                        sa.feed_extended(combined)
                    }
                },
            )?;
        }
        if last {
            break;
        }
    }

    // Finalise, then HAVING / ORDER BY / OFFSET-LIMIT over the (small) grouped
    // output -- exactly as join_select's aggregation branch does.
    let (osch, orows) = sa.finalize()?;
    let mut orows = apply_having(select.having.as_ref(), &select.projection, &osch, orows)?;
    order_output_rows(&mut orows, &osch, order_exprs)?;
    apply_offset_limit(&mut orows, offset, limit);
    Ok(Some(QueryResult::Rows(RowStream::literal(osch, orows))))
}

/// Streaming hash join + ORDER BY for
/// `SELECT ... FROM driving JOIN partner ON driving.k = partner.k
///  [WHERE] ORDER BY ... [LIMIT]` (INNER/LEFT, no GROUP BY, no aggregate).
///
/// Builds the partner side into an in-memory hash table, then scans the driving
/// table incrementally and feeds each joined row straight into the spilling
/// `Sorter` (top-N heap for a small LIMIT, external merge sort otherwise). The
/// join *output* is therefore never fully materialised -- peak memory is the
/// partner hash table plus the bounded sorter, not `|driving| x fanout`. Returns
/// `None` when the query does not fit this shape, so the caller falls back to
/// the materialising `join_select`.
#[allow(clippy::too_many_arguments)]
async fn streaming_join_order(
    db: &Session,
    select: &Select,
    filter: Option<&Expr>,
    order_exprs: &[(Expr, bool)],
    offset: usize,
    limit: Option<usize>,
) -> Result<Option<QueryResult>> {
    // Reads inside a transaction must see the write overlay -> materialising path.
    // `from.len() > 1` is a comma cross join, which build_join_chain now streams via
    // unkeyed steps; it declines anything it cannot handle, so this only needs to
    // reject an empty FROM.
    if db.in_txn() || select.distinct.is_some() || select.from.is_empty() {
        return Ok(None);
    }
    // A two-table RIGHT join is streamed by rewriting it to `B LEFT JOIN A` and
    // reordering the output columns back to (A, B) below.
    let swapped = rewrite_right_join(&select.from[0]);
    let twj = swapped.as_ref().unwrap_or(&select.from[0]);
    // Which combined columns does this query read (WHERE, projection, ORDER BY)?
    // The rest are decoded as NULL placeholders at their own positions. A
    // rewritten RIGHT join permutes the combined row after the chain, so the
    // mask would not line up -- decode everything there.
    let pruned = swapped.is_none();
    let needed = |combined: &Schema| -> Option<Vec<bool>> {
        if !pruned {
            return None;
        }
        join_needed_mask(
            combined,
            filter,
            &select.projection,
            &[],
            select.having.as_ref(),
            order_exprs,
            &[],
        )
    };
    // Build the (left-deep) join chain: each partner into a hash table, driving
    // left to be streamed. Handles two or more tables.
    let Some(JoinChain {
        dschema,
        steps,
        schema,
        dmask,
    }) = build_join_chain(db, &select.from, twj, &needed).await?
    else {
        return Ok(None);
    };
    let dlen = dschema.columns.len();
    let (ddef, _) = resolve_table(db, &twj.relation).await?;

    // For a rewritten RIGHT join, restore the query's (A, B) column order in both
    // the schema and every produced row.
    let reorder: Option<Vec<usize>> = swapped.as_ref().map(|_| {
        let nb = dschema.columns.len();
        right_join_reorder(nb, schema.columns.len() - nb)
    });
    let schema = match &reorder {
        Some(perm) => Schema::new(perm.iter().map(|&i| schema.columns[i].clone()).collect()),
        None => schema,
    };

    // ORDER BY keys resolved against the projection + combined schema, exactly
    // as join_select does before sorting.
    let resolved = resolve_order_aliases(order_exprs, &select.projection, &schema);
    if resolved.is_empty() {
        return Ok(None);
    }
    let order_colls: Vec<elyra_core::Collation> = resolved
        .iter()
        .map(|(e, _)| expr_collation(e, &schema))
        .collect();
    let asc: Vec<bool> = resolved.iter().map(|(_, a)| *a).collect();

    let keep = |row: &[Value]| -> Result<bool> {
        match filter {
            Some(f) => predicate::matches(f, &schema, row),
            None => Ok(true),
        }
    };

    // Stream the driving table, expanding each row through the chain into the
    // spilling sorter (top-N heap / external merge). The join output is never
    // fully materialised.
    let mut sorter = crate::sort::Sorter::new(
        asc,
        order_colls,
        offset,
        limit,
        crate::sort::sort_max_rows(),
    );
    let prefix = data_prefix(&ddef.name);
    let mut cursor: Option<Vec<u8>> = None;
    let mut keybuf: Vec<Value> = Vec::with_capacity(resolved.len());
    let selective = chain_is_selective(&steps);
    let mut bufs: Vec<ChainBuf> = (0..steps.len()).map(|_| ChainBuf::default()).collect();
    let mut permbuf: Vec<Value> = Vec::new();
    let mut check = db.cancel_check();
    let mut pacer = Pacer::new();
    loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 4096;
        cursor = batch.last().map(|(k, _)| k.clone());
        for (_, v) in batch {
            // Expanding one driving row is synchronous and can be large, so give
            // the runtime a chance to run other tasks between rows.
            pacer.tick().await;
            let l: Vec<Value> = decode_driving_row(&v, dlen, dmask.as_deref())?;
            // Stream into the spilling sorter for the same reason as the
            // aggregate path: the product is never held, only the top-N / spill.
            stream_chain(
                selective,
                &l,
                &steps,
                &mut bufs,
                &mut check,
                &mut |combined| {
                    let combined: &[Value] = match &reorder {
                        Some(perm) => {
                            apply_perm_into(combined, perm, &mut permbuf);
                            &permbuf
                        }
                        None => combined,
                    };
                    if keep(combined)? {
                        // Evaluate the keys into a reusable buffer and run the top-N
                        // admission test first: under `LIMIT k` almost every joined
                        // row loses it, and a losing row should not be copied at all.
                        keybuf.clear();
                        for (e, _) in &resolved {
                            keybuf.push(predicate::eval_row(e, &schema, combined)?);
                        }
                        if sorter.admits(&keybuf) {
                            sorter.push(std::mem::take(&mut keybuf), combined.to_vec())?;
                        }
                    }
                    Ok(())
                },
            )?;
        }
        if last {
            break;
        }
    }

    let sorted = sorter.finish()?;
    let (osch, out) = project_exprs(&select.projection, &schema, &sorted)?;
    Ok(Some(QueryResult::Rows(RowStream::literal(osch, out))))
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
    let aggregating =
        !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection);
    if aggregating && projection_has_subquery(&select.projection) {
        return Err(Error::Unsupported(
            "correlated subqueries in aggregate join projections are not supported".into(),
        ));
    }

    let (cols, rows) = build_from(db, vindex, &select.from, &[]).await?;
    let schema = Schema::new(cols);

    let mut kept: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        if let Some(f) = &raw_filter {
            let bound = bind_row(f, &schema, &row);
            let resolved = resolve_subqueries_with_outer(db, vindex, bound, &schema, &row).await?;
            if !predicate::matches(&resolved, &schema, &row)? {
                continue;
            }
        }
        kept.push(row);
    }

    if aggregating {
        let (out_schema, out_rows) = aggregate::run(&schema, &select.projection, &group_by, kept)?;
        let mut out_rows = apply_having(
            select.having.as_ref(),
            &select.projection,
            &out_schema,
            out_rows,
        )?;
        let output_order =
            resolve_output_order_expressions(&order_exprs, &select.projection, &out_schema);
        order_output_rows(&mut out_rows, &out_schema, &output_order)?;
        apply_offset_limit(&mut out_rows, offset, limit);
        return Ok(QueryResult::Rows(RowStream::literal(out_schema, out_rows)));
    }

    let resolved_order = resolve_order_aliases(&order_exprs, &select.projection, &schema);
    if !resolved_order.is_empty() {
        sort_rows_with_subqueries(
            db,
            vindex,
            &mut kept,
            &schema,
            &resolved_order,
            |expr, row| bind_row(expr, &schema, row),
        )
        .await?;
    }
    apply_offset_limit(&mut kept, offset, limit);

    // Plain projection when no SELECT-list subqueries.
    if !projection_has_subquery(&select.projection) {
        let (osch, out) = project_exprs(&select.projection, &schema, &kept)?;
        return Ok(QueryResult::Rows(RowStream::literal(osch, out)));
    }

    // Build a projection plan once so wildcard expansion does not repeat for
    // every joined row.
    use sqlparser::ast::SelectItem;
    enum Projection<'a> {
        Column(usize),
        Expr(&'a Expr),
    }

    let mut projection = Vec::new();
    let mut outcols = Vec::new();
    let mut inferred = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (index, column) in schema.columns.iter().enumerate() {
                    projection.push(Projection::Column(index));
                    let mut column = column.clone();
                    column.name = output_column_name(&column.name).to_owned();
                    outcols.push(column);
                }
            }
            SelectItem::QualifiedWildcard(object, _) => {
                let qualifier = object
                    .0
                    .last()
                    .map(|identifier| identifier.value.as_str())
                    .unwrap_or_default();
                let before = projection.len();
                for (index, column) in schema.columns.iter().enumerate() {
                    if let Some((column_qualifier, name)) = column.name.split_once('.') {
                        if column_qualifier.eq_ignore_ascii_case(qualifier) {
                            projection.push(Projection::Column(index));
                            let mut column = column.clone();
                            column.name = name.to_owned();
                            outcols.push(column);
                        }
                    }
                }
                if projection.len() == before {
                    return Err(Error::Unsupported(format!(
                        "qualified wildcard {object}.* matched no relation"
                    )));
                }
            }
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                let name = match item {
                    SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                    SelectItem::UnnamedExpr(expr) => ident_name(expr)
                        .map(str::to_owned)
                        .unwrap_or_else(|| expr.to_string()),
                    _ => unreachable!(),
                };
                inferred.push(projection.len());
                projection.push(Projection::Expr(expr));
                outcols.push(ColumnDef {
                    name,
                    ty: ColumnType::Text,
                    nullable: true,
                    collation: elyra_core::Collation::Ci,
                });
            }
        }
    }

    let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(kept.len());
    for row in &kept {
        let mut vals = Vec::with_capacity(projection.len());
        for item in &projection {
            match item {
                Projection::Column(index) => vals.push(row[*index].clone()),
                Projection::Expr(expr) => {
                    let bound = bind_row(expr, &schema, row);
                    let resolved =
                        resolve_subqueries_with_outer(db, vindex, bound, &schema, row).await?;
                    vals.push(predicate::eval_row(&resolved, &schema, row)?);
                }
            }
        }
        out_rows.push(vals);
    }

    for index in inferred {
        outcols[index].ty = out_rows
            .iter()
            .map(|row| &row[index])
            .find(|v| !v.is_null())
            .map(infer_val)
            .unwrap_or(ColumnType::Text);
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
                    // Preserve the source column's collation so joins, ORDER BY,
                    // GROUP BY and DISTINCT over a joined `_bin` column stay
                    // case-sensitive.
                    collation: c.collation,
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

/// Storage keys of the rows where `col` equals `value`, without fetching them.
///
/// Keys first, rows later: an `IN` list can be unioned and size-checked against the
/// index-range budget while it is still cheap, so abandoning a too-wide list costs
/// only the key lookups.
async fn lookup_keys_by_eq(
    db: &Session,
    def: &TableDef,
    col: usize,
    value: &Value,
) -> Result<Vec<Vec<u8>>> {
    if def.pk_cols == [col] {
        let key = data_key(
            &def.name,
            &keyenc::encode_coll(value, def.collation_of(col))?,
        );
        // The primary key identifies at most one row, but it still has to exist.
        return Ok(if db.get(key.clone()).await?.is_some() {
            vec![key]
        } else {
            Vec::new()
        });
    }
    let Some(idx) = index::index_on(def, col) else {
        return Ok(Vec::new());
    };
    index::lookup_eq(db, &def.name, idx, std::slice::from_ref(value)).await
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
/// Fraction of a table a secondary-index range may match before a sequential scan
/// is the cheaper plan (`ELYRASQL_INDEX_RANGE_MAX_FRACTION`, default 0.06).
///
/// An index range walk pays a *random* keyed fetch per matching row; a sequential
/// scan decodes rows in storage order, which is roughly an order of magnitude
/// cheaper per row. So the index only wins while it matches a small slice of the
/// table. Measured on 200k rows: `amt > 99000` (~1% of rows) took 1.2ms via the
/// index, while `amt > 0` (~100%) took 124ms -- against 2.7ms for the same row set
/// expressed as `amt <> -1`, which is not index-usable and therefore scanned.
fn index_range_max_fraction() -> f64 {
    use std::sync::OnceLock;
    static CACHE: OnceLock<f64> = OnceLock::new();
    *CACHE.get_or_init(|| {
        std::env::var("ELYRASQL_INDEX_RANGE_MAX_FRACTION")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|f| *f > 0.0 && *f <= 1.0)
            .unwrap_or(0.06)
    })
}

/// Approximate row count for planning, cached per (table, write epoch).
///
/// Prefers the `ANALYZE` row count (a single key read). Without statistics it counts
/// keys once per epoch -- a key-only scan, far cheaper than the row fetches the
/// estimate is there to avoid -- and reuses that until the table is next written.
async fn table_rows(db: &Session, def: &TableDef) -> Result<u64> {
    if let Some(st) = catalog::load_stats(db, &def.name).await? {
        if st.rows > 0 {
            return Ok(st.rows);
        }
    }
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    type Cache = HashMap<String, (u64, u64)>; // table -> (epoch, rows)
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let epoch = db.raw_db().write_epoch()?;
    if let Some(&(e, rows)) = cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&def.name)
    {
        if e == epoch {
            return Ok(rows);
        }
    }
    let rows = db.raw_db().count_prefix(data_prefix(&def.name)).await?;
    cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(def.name.clone(), (epoch, rows));
    Ok(rows)
}

/// Upper bound on index entries worth fetching for a range, or `None` when the
/// table is small enough that either plan is cheap.
async fn index_range_budget(db: &Session, def: &TableDef) -> Result<Option<usize>> {
    let rows = table_rows(db, def).await?;
    // Below this a full scan is a few milliseconds anyway, so skip the estimate and
    // the risk of mis-planning a tiny table.
    const SMALL_TABLE: u64 = 4096;
    if rows <= SMALL_TABLE {
        return Ok(None);
    }
    Ok(Some(
        ((rows as f64) * index_range_max_fraction()).max(1.0) as usize
    ))
}

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
/// Fetch the rows a secondary-index range matches, or `None` when the range covers
/// more of the table than `budget` allows and a sequential scan is the better plan.
///
/// The bail-out happens *after* the index keys are walked but *before* the rows are
/// fetched, so a misjudged range costs only a key-only walk -- not the random row
/// fetches that make a wide index range slow in the first place.
async fn index_range(
    db: &Session,
    def: &TableDef,
    idx: &IndexDef,
    rq: &RangeQuery,
    budget: Option<usize>,
) -> Result<Option<Vec<(Vec<u8>, Vec<Value>)>>> {
    let lo = rq.lo.as_ref().map(|(v, i)| (v, *i));
    let hi = rq.hi.as_ref().map(|(v, i)| (v, *i));
    let data_keys = index::lookup_range(db, &def.name, idx, lo, hi).await?;
    if let Some(b) = budget {
        if data_keys.len() > b {
            return Ok(None);
        }
    }
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
    Ok(Some(out))
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
            .all(|t| t.joins.is_empty() && stored_table_factor(&t.relation))
    {
        let mut idx: Vec<(&TableWithJoins, u64)> = Vec::with_capacity(from.len());
        for t in from {
            let est = match &t.relation {
                TableFactor::Table { name, .. } => {
                    let n = name.0.last().map(|i| i.value.clone()).unwrap_or_default();
                    match catalog::load_stats(db, &n).await? {
                        // Histogram-based estimate: table rows scaled by the
                        // selectivity of the WHERE predicates on this table.
                        Some(s) => estimate_filtered_rows(&s, conjuncts),
                        None => u64::MAX,
                    }
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
            let cancel = db.cancel_token();
            let (c, r) = cpu_bound(|| {
                combine(
                    &cur_cols,
                    &cur_rows,
                    &bc,
                    &br,
                    JoinKind::Inner,
                    None,
                    &cancel,
                )
            })?;
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
            let nlj = if stored_table_factor(&join.relation) {
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
                let mut check = db.cancel_check();
                for l in &cur_rows {
                    check.tick()?;
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
            let cancel = db.cancel_token();
            let (c, r) =
                cpu_bound(|| combine(&cur_cols, &cur_rows, &jc, &jr, kind, on.as_ref(), &cancel))?;
            cur_cols = c;
            cur_rows = r;
        }
    }
    Ok((cur_cols, cur_rows))
}

/// Estimate how many rows of a table survive the applicable WHERE predicates,
/// using per-column histograms (falling back to the raw row count).
fn estimate_filtered_rows(stats: &catalog::TableStats, conjuncts: &[Expr]) -> u64 {
    let mut sel = 1.0f64;
    for c in conjuncts {
        if let Some((col, op, val)) = simple_pred(c) {
            if let Some(cs) = stats
                .columns
                .iter()
                .find(|s| s.name.eq_ignore_ascii_case(&col))
            {
                if let Some(s) = cs.selectivity(op, &val) {
                    sel *= s.clamp(0.0, 1.0);
                }
            }
        }
    }
    ((stats.rows as f64) * sel).round().max(0.0) as u64
}

/// Extract `(column, op, literal)` from a simple `col <op> literal` predicate
/// (for histogram selectivity). Returns the unqualified column name.
fn simple_pred(e: &Expr) -> Option<(String, catalog::SelOp, String)> {
    use sqlparser::ast::BinaryOperator as B;
    let Expr::BinaryOp { left, op, right } = e else {
        return None;
    };
    let selop = match op {
        B::Lt => catalog::SelOp::Lt,
        B::LtEq => catalog::SelOp::Le,
        B::Gt => catalog::SelOp::Gt,
        B::GtEq => catalog::SelOp::Ge,
        B::Eq => catalog::SelOp::Eq,
        _ => return None,
    };
    // Accept `col OP literal` or `literal OP col` (flipping the operator).
    let col_of = |x: &Expr| -> Option<String> {
        match x {
            Expr::Identifier(i) => Some(i.value.clone()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
            _ => None,
        }
    };
    let lit_of = |x: &Expr| -> Option<String> {
        match x {
            Expr::Value(v) => Some(v.to_string().trim_matches('\'').to_string()),
            _ => None,
        }
    };
    if let (Some(c), Some(v)) = (col_of(left), lit_of(right)) {
        return Some((c, selop, v));
    }
    if let (Some(c), Some(v)) = (col_of(right), lit_of(left)) {
        let flipped = match selop {
            catalog::SelOp::Lt => catalog::SelOp::Gt,
            catalog::SelOp::Le => catalog::SelOp::Ge,
            catalog::SelOp::Gt => catalog::SelOp::Lt,
            catalog::SelOp::Ge => catalog::SelOp::Le,
            catalog::SelOp::Eq => catalog::SelOp::Eq,
        };
        return Some((c, flipped, v));
    }
    None
}

/// Extract the table name from a `CREATE TABLE [IF NOT EXISTS] name (...)`.
pub fn create_table_name(sql: &str) -> Result<String> {
    let lower = sql.to_ascii_lowercase();
    let after = lower
        .find("table")
        .map(|p| p + "table".len())
        .ok_or_else(|| Error::Parse("expected CREATE TABLE".into()))?;
    let mut rest = sql[after..].trim_start();
    if rest.to_ascii_lowercase().starts_with("if not exists") {
        rest = rest["if not exists".len()..].trim_start();
    }
    let name = rest
        .split(|ch: char| ch.is_whitespace() || ch == '(')
        .next()
        .unwrap_or("")
        .trim_matches(['`', '"']);
    if name.is_empty() {
        return Err(Error::Parse("CREATE TABLE: empty name".into()));
    }
    Ok(name.to_string())
}

/// Parse a `PARTITION BY ...` clause (the text after `PARTITION BY`).
pub fn parse_partition_clause(clause: &str) -> Result<catalog::PartitionSpec> {
    let c = clause.trim();
    let lower = c.to_ascii_lowercase();
    let method = c
        .split(|ch: char| ch.is_whitespace() || ch == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    if !matches!(method.as_str(), "RANGE" | "LIST" | "HASH") {
        return Err(Error::Unsupported(format!(
            "unsupported partition method: {method}"
        )));
    }
    // Column inside the first (...).
    let open = c
        .find('(')
        .ok_or_else(|| Error::Parse("PARTITION BY requires a column".into()))?;
    let close = c[open..]
        .find(')')
        .ok_or_else(|| Error::Parse("PARTITION BY requires a column".into()))?
        + open;
    let column = c[open + 1..close]
        .trim()
        .trim_matches(['`', '"'])
        .to_string();

    let mut parts = Vec::new();
    let mut hash_count = 0u32;
    if method == "HASH" {
        if let Some(p) = lower.find("partitions") {
            hash_count = c[p + "partitions".len()..]
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
        }
    } else {
        // Parse the `(PARTITION name VALUES ...)` list after the column group.
        let rest = &c[close + 1..];
        for seg in rest.split("PARTITION").skip(1) {
            let seg = seg.trim().trim_end_matches([',', ')']).trim();
            if seg.is_empty() {
                continue;
            }
            let name = seg.split_whitespace().next().unwrap_or("").to_string();
            let seg_low = seg.to_ascii_lowercase();
            let mut less_than = None;
            let mut list_values = Vec::new();
            if let Some(lt) = seg_low.find("less than") {
                let after = seg[lt + "less than".len()..].trim();
                let inner = after.trim_start_matches('(').trim_end_matches(')').trim();
                if !inner.eq_ignore_ascii_case("maxvalue") {
                    less_than = inner.parse::<i64>().ok();
                }
            } else if let Some(iv) = seg_low.find(" in ") {
                let after = &seg[iv + 4..];
                if let (Some(o), Some(cl)) = (after.find('('), after.rfind(')')) {
                    list_values = after[o + 1..cl]
                        .split(',')
                        .filter_map(|v| v.trim().parse::<i64>().ok())
                        .collect();
                }
            }
            parts.push(catalog::PartitionDef {
                name,
                less_than,
                list_values,
            });
        }
    }
    Ok(catalog::PartitionSpec {
        method,
        column,
        parts,
        hash_count,
    })
}

/// The `WHERE` predicate selecting a partition's rows (for DROP/TRUNCATE
/// PARTITION). Returns `None` for HASH (not contiguous).
pub fn partition_where(spec: &catalog::PartitionSpec, name: &str) -> Option<String> {
    let col = &spec.column;
    let idx = spec
        .parts
        .iter()
        .position(|p| p.name.eq_ignore_ascii_case(name))?;
    let p = &spec.parts[idx];
    match spec.method.as_str() {
        "RANGE" => {
            let lower = if idx > 0 {
                spec.parts[idx - 1].less_than
            } else {
                None
            };
            let mut conds = Vec::new();
            if let Some(lo) = lower {
                conds.push(format!("`{col}` >= {lo}"));
            }
            if let Some(hi) = p.less_than {
                conds.push(format!("`{col}` < {hi}"));
            }
            Some(if conds.is_empty() {
                "1=1".to_string()
            } else {
                conds.join(" AND ")
            })
        }
        "LIST" => {
            if p.list_values.is_empty() {
                return None;
            }
            let vals: Vec<String> = p.list_values.iter().map(|v| v.to_string()).collect();
            Some(format!("`{col}` IN ({})", vals.join(", ")))
        }
        _ => None,
    }
}

/// Base tables referenced in the FROM of a (materialized view) query.
pub fn matview_base_tables(query: &str) -> Vec<String> {
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    let Ok(stmts) = Parser::parse_sql(&MySqlDialect {}, query) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for stmt in &stmts {
        if let Statement::Query(q) = stmt {
            if let SetExpr::Select(select) = q.body.as_ref() {
                for twj in &select.from {
                    if let TableFactor::Table { name, .. } = &twj.relation {
                        if let Some(n) = name.0.last() {
                            out.push(n.value.clone());
                        }
                    }
                    for j in &twj.joins {
                        if let TableFactor::Table { name, .. } = &j.relation {
                            if let Some(n) = name.0.last() {
                                out.push(n.value.clone());
                            }
                        }
                    }
                }
            }
        }
    }
    out
}

/// Encode `(matdep_key, value)` capturing each base table's current write count,
/// so staleness can be detected later.
pub async fn matview_deps_put(db: &Session, name: &str, query: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut deps: Vec<(String, u64)> = Vec::new();
    for t in matview_base_tables(query) {
        let wc = crate::vindex::read_wcount(db, &t).await?;
        deps.push((t, wc));
    }
    let enc = bincode::serialize(&deps).map_err(|e| Error::Storage(e.to_string()))?;
    Ok((catalog::matdep_key(name), enc))
}

/// Whether a materialized view is stale (a base table changed since last refresh).
pub async fn matview_is_stale(db: &Session, name: &str) -> Result<bool> {
    let Some(b) = db.get(catalog::matdep_key(name)).await? else {
        return Ok(false);
    };
    let deps: Vec<(String, u64)> =
        bincode::deserialize(&b).map_err(|e| Error::Storage(e.to_string()))?;
    for (t, wc) in deps {
        if crate::vindex::read_wcount(db, &t).await? != wc {
            return Ok(true);
        }
    }
    Ok(false)
}

/// A parsed `LOAD DATA INFILE` statement.
pub struct LoadSpec {
    pub path: String,
    pub table: String,
    pub cols: Vec<String>,
    pub field_term: String,
    pub enclosed: Option<char>,
    pub line_term: String,
    pub ignore: usize,
}

/// Parse `LOAD DATA [LOCAL] INFILE '<path>' INTO TABLE <t> [FIELDS TERMINATED BY
/// 'x' [[OPTIONALLY] ENCLOSED BY 'y']] [LINES TERMINATED BY 'z'] [IGNORE n LINES]
/// [(col, ...)]`.
pub fn parse_load_data(sql: &str) -> Result<LoadSpec> {
    let lower = sql.to_ascii_lowercase();
    // Extract the single-quoted string starting at byte position `from`.
    let quoted = |from: usize| -> Option<String> {
        let rest = &sql[from..];
        let start = rest.find('\'')?;
        let after = &rest[start + 1..];
        // Support simple backslash escapes for terminators like '\t'.
        let mut out = String::new();
        let mut chars = after.chars();
        while let Some(c) = chars.next() {
            match c {
                '\'' => return Some(out),
                '\\' => match chars.next() {
                    Some('t') => out.push('\t'),
                    Some('n') => out.push('\n'),
                    Some('r') => out.push('\r'),
                    Some('0') => out.push('\0'),
                    Some(o) => out.push(o),
                    None => break,
                },
                _ => out.push(c),
            }
        }
        Some(out)
    };
    let infile = lower
        .find("infile")
        .ok_or_else(|| Error::Parse("LOAD DATA requires INFILE '<path>'".into()))?;
    let path = quoted(infile).ok_or_else(|| Error::Parse("LOAD DATA: missing file path".into()))?;
    let into = lower
        .find("into table")
        .ok_or_else(|| Error::Parse("LOAD DATA requires INTO TABLE <table>".into()))?;
    let after_into = sql[into + "into table".len()..].trim_start();
    let table = after_into
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .trim_matches(['`', '"'])
        .to_string();
    if table.is_empty() {
        return Err(Error::Parse("LOAD DATA: empty table name".into()));
    }
    let field_term = lower
        .find("fields terminated by")
        .or_else(|| lower.find("columns terminated by"))
        .and_then(|p| quoted(p + "fields terminated by".len()))
        .unwrap_or_else(|| "\t".to_string());
    let enclosed = lower
        .find("enclosed by")
        .and_then(|p| quoted(p + "enclosed by".len()))
        .and_then(|s| s.chars().next());
    let line_term = lower
        .find("lines terminated by")
        .and_then(|p| quoted(p + "lines terminated by".len()))
        .unwrap_or_else(|| "\n".to_string());
    let ignore = lower.find("ignore").and_then(|p| {
        sql[p + "ignore".len()..]
            .split_whitespace()
            .next()
            .and_then(|n| n.parse::<usize>().ok())
    });
    // Optional explicit column list: the last `(...)` group.
    let cols = if let (Some(open), Some(close)) = (sql.rfind('('), sql.rfind(')')) {
        if open < close && open > into {
            sql[open + 1..close]
                .split(',')
                .map(|c| c.trim().trim_matches(['`', '"']).to_string())
                .filter(|c| !c.is_empty())
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    Ok(LoadSpec {
        path,
        table,
        cols,
        field_term,
        enclosed,
        line_term,
        ignore: ignore.unwrap_or(0),
    })
}

/// Turn file `content` into batched `INSERT` statements per the load spec.
pub fn build_load_inserts(spec: &LoadSpec, content: &str, batch: usize) -> Vec<String> {
    let mut stmts = Vec::new();
    let col_list = if spec.cols.is_empty() {
        String::new()
    } else {
        format!(
            " ({})",
            spec.cols
                .iter()
                .map(|c| format!("`{c}`"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let mut rows_iter = content
        .split(spec.line_term.as_str())
        .skip(spec.ignore)
        .filter(|l| !l.is_empty())
        .peekable();
    while rows_iter.peek().is_some() {
        let mut tuples: Vec<String> = Vec::with_capacity(batch);
        for line in rows_iter.by_ref().take(batch) {
            let fields = line.split(spec.field_term.as_str()).map(|f| {
                let f = match spec.enclosed {
                    Some(q) => f.trim_matches(q),
                    None => f,
                };
                if f == "\\N" {
                    "NULL".to_string()
                } else {
                    format!("'{}'", f.replace('\\', "\\\\").replace('\'', "''"))
                }
            });
            tuples.push(format!("({})", fields.collect::<Vec<_>>().join(", ")));
        }
        if !tuples.is_empty() {
            stmts.push(format!(
                "INSERT INTO `{}`{} VALUES {}",
                spec.table,
                col_list,
                tuples.join(", ")
            ));
        }
    }
    stmts
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
        let cancel = db.cancel_token();
        let (c, r) = cpu_bound(|| {
            combine(
                &cur_cols,
                &cur_rows,
                &rcols,
                &rrows,
                JoinKind::Inner,
                Some(&pred),
                &cancel,
            )
        })?;
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

/// Normalise an INNER comma-join (`FROM a, b, c WHERE a.k = b.k AND b.j = c.j`)
/// into an explicit left-deep `JOIN` chain, using the WHERE equi-predicates as
/// the `ON` conditions, so it flows through the full join machinery (cost-based
/// reordering + streaming). Returns `None` unless every table is a plain table
/// and each non-driving table is connected to the ones already in the chain by
/// an equi-predicate. The original WHERE is kept unchanged by the caller (the
/// equi-predicates remain as harmless residual filters), so semantics are
/// preserved -- comma joins are always inner.
fn comma_join_chain(from: &[TableWithJoins], selection: Option<&Expr>) -> Option<TableWithJoins> {
    use sqlparser::ast::Join;
    if from.len() < 2
        || !from
            .iter()
            .all(|t| t.joins.is_empty() && matches!(t.relation, TableFactor::Table { .. }))
    {
        return None;
    }
    let quals: Vec<String> = from
        .iter()
        .map(|t| factor_qualifier(&t.relation).map(|q| q.to_ascii_lowercase()))
        .collect::<Option<Vec<_>>>()?;
    let mut conjuncts = Vec::new();
    if let Some(f) = selection {
        split_and(f, &mut conjuncts);
    }
    // (qual_a, qual_b, predicate) for each equi-conjunct connecting two tables.
    let equis: Vec<(String, String, &Expr)> = conjuncts
        .iter()
        .filter_map(|c| {
            equi_qualifiers(c).map(|(a, b)| (a.to_ascii_lowercase(), b.to_ascii_lowercase(), c))
        })
        .collect();

    let mut used = vec![false; from.len()];
    used[0] = true;
    let mut acc: std::collections::HashSet<String> = [quals[0].clone()].into_iter().collect();
    let mut joins: Vec<Join> = Vec::with_capacity(from.len() - 1);
    for _ in 1..from.len() {
        let mut found: Option<(usize, Expr)> = None;
        'outer: for (i, q) in quals.iter().enumerate() {
            if used[i] {
                continue;
            }
            for (a, b, e) in &equis {
                if (a == q && acc.contains(b)) || (b == q && acc.contains(a)) {
                    found = Some((i, (*e).clone()));
                    break 'outer;
                }
            }
        }
        let (i, on) = found?; // a table with no equi-connector -> not a clean chain
        used[i] = true;
        acc.insert(quals[i].clone());
        joins.push(Join {
            relation: from[i].relation.clone(),
            global: false,
            join_operator: JoinOperator::Inner(JoinConstraint::On(on)),
        });
    }
    Some(TableWithJoins {
        relation: from[0].relation.clone(),
        joins,
    })
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
#[allow(clippy::too_many_arguments)]
fn combine(
    lcols: &[ColumnDef],
    lrows: &[Vec<Value>],
    rcols: &[ColumnDef],
    rrows: &[Vec<Value>],
    kind: JoinKind,
    on: Option<&Expr>,
    cancel: &std::sync::Arc<elyra_core::cancel::QueryCancel>,
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
                // The merge join compares keys under the default collation, so
                // skip it for a `_bin` join key (fall through to the
                // collation-aware hash join below).
                let bin_key = join_key_collation(&lkey, &lschema, &rkey, &rschema).is_bin();
                if !bin_key
                    && kind == JoinKind::Inner
                    && lrows.len() >= MERGE_MIN
                    && rrows.len() >= MERGE_MIN
                {
                    if let (Some(lk), Some(rk)) = (
                        sorted_keyed(lrows, &lschema, &lkey)?,
                        sorted_keyed(rrows, &rschema, &rkey)?,
                    ) {
                        if let Some(out) = merge_join_inner(lk, rk) {
                            return Ok((cols, out));
                        }
                    }
                }
                let rows = hash_join(lrows, rrows, &lschema, &rschema, &lkey, &rkey, kind, cancel)?;
                return Ok((cols, rows));
            }
        }
    }

    let schema = Schema::new(cols.clone());
    let llen = lcols.len();
    let rlen = rcols.len();
    let mut out = Vec::new();
    let mut right_matched = vec![false; rrows.len()];

    // The product of two relations is quadratic, so this is the loop a cross
    // join spends its time in: check the statement's deadline as we go.
    let mut check = elyra_core::cancel::CancelCheck::new(cancel.clone());
    // Bound the buffered product: without this a cross join grows until the
    // process is killed. Released when this join finishes, errors, or unwinds.
    let mut budget = JoinBudget::new();
    for l in lrows {
        let mut matched = false;
        for (ri, r) in rrows.iter().enumerate() {
            check.tick()?;
            budget.account(out.len())?;
            let mut combined = l.clone();
            combined.extend_from_slice(r);
            budget.sample(&combined);
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
    cancel: &std::sync::Arc<elyra_core::cancel::QueryCancel>,
) -> Result<Vec<Vec<Value>>> {
    // A hash join over large inputs can emit far more rows than either side, so
    // it needs its own deadline checks and row budget (its caller only guards the
    // nested-loop fallback).
    let mut check = elyra_core::cancel::CancelCheck::new(cancel.clone());
    let mut budget = JoinBudget::new();
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
    let coll = join_key_collation(lkey, lschema, rkey, rschema);
    let mut out = Vec::new();

    if build_left {
        let mut table: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for (i, l) in lrows.iter().enumerate() {
            check.tick()?;
            if let Some(k) = key_bytes_coll(&predicate::eval_row(lkey, lschema, l)?, coll) {
                table.entry(k).or_default().push(i);
            }
        }
        for r in rrows {
            check.tick()?;
            let probe = key_bytes_coll(&predicate::eval_row(rkey, rschema, r)?, coll);
            let mut matched = false;
            if let Some(k) = probe {
                if let Some(idxs) = table.get(&k) {
                    for &i in idxs {
                        check.tick()?;
                        budget.account(out.len())?;
                        let mut combined = lrows[i].clone();
                        combined.extend_from_slice(r);
                        budget.sample(&combined);
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
        let mut table: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for (i, r) in rrows.iter().enumerate() {
            check.tick()?;
            if let Some(k) = key_bytes_coll(&predicate::eval_row(rkey, rschema, r)?, coll) {
                table.entry(k).or_default().push(i);
            }
        }
        for l in lrows {
            check.tick()?;
            let probe = key_bytes_coll(&predicate::eval_row(lkey, lschema, l)?, coll);
            let mut matched = false;
            if let Some(k) = probe {
                if let Some(idxs) = table.get(&k) {
                    for &i in idxs {
                        check.tick()?;
                        budget.account(out.len())?;
                        let mut combined = l.clone();
                        combined.extend_from_slice(&rrows[i]);
                        budget.sample(&combined);
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
                        // Over budget: give up on the merge path and let the
                        // caller's hash join report the limit with a clear error.
                        if out.len() > join_max_rows() {
                            return None;
                        }
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

/// Hash-key string under an explicit collation; `None` for NULL (never matches,
/// per SQL). `Bin` keeps text case-sensitive so an equi-join on a `_bin` column
/// matches by exact bytes.
/// Hash-join key for `v` under `coll`, or `None` for NULL (which never matches).
///
/// The key is the collation key's **raw bytes**. It must not be turned into a
/// `String`: a collation key is an order-preserving binary encoding, so any byte
/// that is not valid UTF-8 would be replaced by U+FFFD and unrelated values would
/// collide into one key -- producing extra join rows. That is not theoretical: it
/// made every integer in 128..255 hash to the same key, so `a JOIN b ON a.id =
/// b.id` returned a cartesian product of those 128 ids.
/// The schema position of `e` when it is a plain (possibly qualified) column
/// reference, resolved exactly as `predicate::eval_row` would resolve it.
/// `None` for anything else, which keeps the general expression path.
fn expr_col_index(e: &Expr, schema: &Schema) -> Option<usize> {
    match e {
        // A bare identifier is only certainly a column when it matches one
        // exactly: `eval_row` reads `@@var` as a system variable and a name like
        // CURRENT_TIMESTAMP as a niladic function whenever it is *not* an exact
        // column name, and the fast path must not disagree with it.
        Expr::Identifier(id) => schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&id.value)),
        // `@@session.var` arrives here too, and fails to resolve -> None.
        Expr::CompoundIdentifier(parts) => predicate::resolve_index_parts(parts, schema).ok(),
        Expr::Nested(inner) => expr_col_index(inner, schema),
        _ => None,
    }
}

fn key_bytes_coll(v: &Value, coll: elyra_core::Collation) -> Option<Vec<u8>> {
    if v.is_null() {
        None
    } else {
        Some(v.collation_key_coll(coll))
    }
}

/// The comparison collation for an equi-join on two key expressions: binary if
/// either side is a `_bin` column (matching MySQL's coercibility rule), else the
/// default case-insensitive collation.
fn join_key_collation(
    lkey: &Expr,
    lschema: &Schema,
    rkey: &Expr,
    rschema: &Schema,
) -> elyra_core::Collation {
    if expr_collation(lkey, lschema).is_bin() || expr_collation(rkey, rschema).is_bin() {
        elyra_core::Collation::Bin
    } else {
        elyra_core::Collation::Ci
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
    // In a joined schema the columns are qualified (`a.v`, `b.v`), and a
    // *qualified* reference must match the qualifier: `predicate::resolve_index`
    // falls back to matching on the bare suffix, which is right when resolving a
    // value but wrong when deciding which side of a join an expression belongs
    // to -- it would report `b.v` as living in the left schema because `a.v` ends
    // the same way. A bare reference keeps the unique-suffix rule, which is how
    // `col` resolves against a joined schema in the first place.
    let qualified = schema.columns.iter().any(|c| c.name.contains('.'));
    refs.iter().all(|r| {
        if qualified && r.contains('.') {
            schema
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(r))
        } else {
            predicate::resolve_index(r, schema).is_ok()
        }
    })
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
        Ok(v) if !v.is_null() => Ok(Some((idx, v))),
        Ok(_) | Err(_) => Ok(None),
    }
}

/// Extract equality values for every column in `key_cols` from the filter's
/// AND-conjuncts (coerced to column type). `None` if any key column lacks an
/// equality — i.e. the key is not fully specified.
/// The constant value of `e`, or `None` if it references a column or is otherwise
/// row-dependent.
///
/// Evaluated against an empty row rather than pattern-matched on `Expr::Value`, so
/// forms like `-5` (a unary minus over a literal) and `TRUE` are handled with the
/// same semantics the interpreter uses.
fn literal_value(e: &Expr) -> Option<Value> {
    static EMPTY: std::sync::OnceLock<Schema> = std::sync::OnceLock::new();
    let schema = EMPTY.get_or_init(|| Schema::new(Vec::new()));
    predicate::eval_row(e, schema, &[]).ok()
}

/// `col IN (literal, ...)` on a PK or indexed column, as (column index, values).
///
/// Recognised so the values can be looked up through the index instead of scanning
/// the table and testing membership per row. MySQL turns the same shape into one
/// index lookup per value, which is why `g IN (1,2,3,4,5)` cost it 0.33ms against
/// our 4.51ms full scan before this existed.
///
/// Only a *whole-filter* `IN` (or one AND-conjunct of it) qualifies; the remaining
/// conjuncts are re-applied to the fetched rows, so the result is identical to a
/// scan either way.
fn in_list_lookup(def: &TableDef, filter: Option<&Expr>) -> Result<Option<(usize, Vec<Value>)>> {
    let Some(f) = filter else { return Ok(None) };
    let mut conj = Vec::new();
    split_and(f, &mut conj);
    for c in conj {
        let Expr::InList {
            expr,
            list,
            negated: false,
        } = c
        else {
            continue;
        };
        // NOT IN cannot use the index: it selects the complement.
        let Some(name) = ident_name(expr.as_ref()) else {
            continue;
        };
        let short = name.rsplit('.').next().unwrap_or(name);
        let Some(col) = def
            .schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(short))
        else {
            continue;
        };
        if def.pk_cols != [col] && index::index_on(def, col).is_none() {
            continue;
        }
        // Every element must be a literal that can be coerced to the column's
        // type. Both halves matter:
        //  - a column reference or expression would have to be evaluated per row,
        //    which is the thing being avoided;
        //  - a literal of a different type must be coerced before it is encoded as
        //    a key, or the lookup silently finds nothing. Clients can send bound
        //    integers as quoted strings, so `id IN ('1','2')` on an INT primary
        //    key must agree with the equivalent scan.
        let coldef = &def.schema.columns[col];
        let mut vals = Vec::with_capacity(list.len());
        let mut usable = true;
        for item in list.iter() {
            match literal_value(item) {
                // A NULL never matches, and only affects the three-valued outcome
                // of a non-match, which the residual filter re-applies -- so it can
                // be skipped for lookup purposes.
                Some(v) if v.is_null() => {}
                Some(v) => match coerce(v, &coldef.ty, &coldef.name) {
                    Ok(c) => vals.push(c),
                    // Not representable in the column's type: fall back to a scan
                    // rather than guess at the comparison semantics.
                    Err(_) => {
                        usable = false;
                        break;
                    }
                },
                None => {
                    usable = false;
                    break;
                }
            }
        }
        if usable && !vals.is_empty() {
            return Ok(Some((col, vals)));
        }
    }
    Ok(None)
}

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
/// True when `order` is a prefix of the primary key, all ascending -- i.e. the
/// clustered scan order already satisfies the ORDER BY, so no sort is needed.
/// Resolve an ORDER BY expression that is a plain (optionally qualified) column
/// reference to its column index, or `None` for anything else.
fn order_col_index(def: &TableDef, e: &Expr) -> Option<usize> {
    let name = match e {
        Expr::Identifier(id) => &id.value,
        Expr::CompoundIdentifier(parts) => &parts.last()?.value,
        _ => return None,
    };
    def.schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
}

fn order_is_pk_asc_prefix(def: &TableDef, order: &[(Expr, bool)]) -> bool {
    order_is_pk_prefix(def, order, true)
}

/// True when `order` matches a prefix of the primary key, every term in the same
/// direction (`asc`). Backs the clustered forward (ASC) and reverse (DESC) scans.
fn order_is_pk_prefix(def: &TableDef, order: &[(Expr, bool)], asc: bool) -> bool {
    if !def.has_pk() || order.is_empty() || order.len() > def.pk_cols.len() {
        return false;
    }
    for (i, (e, a)) in order.iter().enumerate() {
        if *a != asc {
            return false;
        }
        match order_col_index(def, e) {
            Some(ci) if ci == def.pk_cols[i] => {}
            _ => return false,
        }
    }
    true
}

/// How NULL-keyed rows (omitted from the index's value entries) are handled.
#[derive(PartialEq)]
enum NullMode {
    /// Every index column is `NOT NULL` — the value walk is already complete.
    None,
    /// NULL rows are maintained under the `indexnull::` prefix (single-column
    /// `indexes_nulls` index): a two-range walk is a complete MySQL ordering.
    Indexed,
    /// Legacy single-column nullable index without stored NULL entries: splice
    /// the NULL block via a data scan, or fall back to the sorter.
    Legacy,
}

/// How a secondary index can serve an `ORDER BY ... LIMIT`.
struct SecondaryOrder {
    /// Index to walk.
    index: String,
    /// Walk in reverse key order (i.e. the `ORDER BY` is `DESC`).
    rev: bool,
    /// The leading order column (for the NULL test on the legacy path).
    col: usize,
    /// How NULL-keyed rows are handled.
    null_mode: NullMode,
    /// The `ORDER BY` extends past the index columns into the appended clustered
    /// primary key (a stable-pagination tiebreaker like `..., id`). Only relevant
    /// to the legacy path (a NULL block cannot be tiebroken cheaply there).
    has_tiebreaker: bool,
}

/// If `order` can be served by walking a secondary index in key order, describe
/// how. Requires all terms to share a direction, and the order columns to be a
/// prefix of the index's **walk order** — its columns followed by the appended
/// clustered primary key for a non-unique index. That clustered suffix is why
/// `ORDER BY <indexed col>, <pk...>` (a grid's stable-sort tiebreaker) is served
/// by the same walk.
///
/// Because indexes omit NULL tuples, a nullable order column is only supported
/// for a **single-column** index (the only rows missing from the walk are then
/// exactly the NULL-keyed ones, which the caller splices back in). A composite
/// index must have every column `NOT NULL`, otherwise a row with a NULL in any
/// index column would be silently missing.
fn secondary_order_plan(def: &TableDef, order: &[(Expr, bool)]) -> Option<SecondaryOrder> {
    if order.is_empty() {
        return None;
    }
    let asc = order[0].1;
    if order.iter().any(|(_, a)| *a != asc) {
        return None;
    }
    let mut ocols = Vec::with_capacity(order.len());
    for (e, _) in order {
        ocols.push(order_col_index(def, e)?);
    }
    for idx in &def.indexes {
        if idx.vector || idx.fulltext {
            continue;
        }
        // The walk visits rows ordered by the index columns, then (for a
        // non-unique index) the appended clustered primary key.
        let mut walk_seq = idx.cols.clone();
        if !idx.unique {
            walk_seq.extend_from_slice(&def.pk_cols);
        }
        if ocols.len() > walk_seq.len() || ocols[..] != walk_seq[..ocols.len()] {
            continue;
        }
        let has_tiebreaker = ocols.len() > idx.cols.len();
        let all_not_null = idx.cols.iter().all(|&c| !def.schema.columns[c].nullable);
        let null_mode = if all_not_null {
            NullMode::None
        } else if idx.cols.len() == 1 && idx.indexes_nulls {
            NullMode::Indexed
        } else if idx.cols.len() == 1 {
            NullMode::Legacy
        } else {
            // Composite index with a nullable column and no stored NULL entries:
            // a NULL in any key column drops the row from the walk -- not safe.
            continue;
        };
        return Some(SecondaryOrder {
            index: idx.name.clone(),
            rev: !asc,
            col: idx.cols[0],
            null_mode,
            has_tiebreaker,
        });
    }
    None
}

/// Collect up to `want` rows whose `col` is NULL and that satisfy `filter`, by
/// scanning the clustered data in one read transaction and examining at most
/// `budget` rows. Returns `(rows, budget_hit)`: `budget_hit` is true if the
/// budget was reached before `want` rows were found *and* the scan did not reach
/// the end of the table — i.e. the NULL set is not fully known, so the caller
/// should fall back. Any `want` NULL rows are a valid answer (NULLs are ties).
async fn collect_null_rows(
    db: &Session,
    def: &TableDef,
    col: usize,
    filter: &Option<Expr>,
    want: usize,
    budget: usize,
) -> Result<(Vec<Vec<Value>>, bool)> {
    let prefix = data_prefix(&def.name);
    let sch = def.schema.clone();
    let f = filter.clone();
    let (rows, _examined, budget_hit) = db
        .raw_db()
        .scan_fold_until(
            prefix,
            (Vec::<Vec<Value>>::new(), 0usize, false),
            move |st, _k, v| {
                let row: Vec<Value> = rowdec::decode_row(v)?;
                st.1 += 1;
                if row.get(col).map(|x| x.is_null()).unwrap_or(false) {
                    let keep = match &f {
                        Some(e) => predicate::matches(e, &sch, &row)?,
                        None => true,
                    };
                    if keep {
                        st.0.push(row);
                    }
                }
                if st.0.len() >= want {
                    return Ok(false);
                }
                if st.1 >= budget {
                    st.2 = true;
                    return Ok(false);
                }
                Ok(true)
            },
        )
        .await?;
    Ok((rows, budget_hit))
}

/// Accumulator for a budgeted ordered walk (`ORDER BY ... LIMIT` with a residual
/// `WHERE`). Rows are visited in order; a matching row is kept until `need` are
/// collected. `budget` caps how many rows we examine before giving up so a very
/// selective residual filter cannot turn the walk into a full point-read scan --
/// on `budget_hit` the caller falls back to the streaming filter+sort path.
struct OrderedWalk {
    rows: Vec<Vec<Value>>,
    examined: usize,
    need: usize,
    budget: usize,
    budget_hit: bool,
}

/// One step of an ordered walk: decode the row, apply the residual filter, keep
/// it if it matches, and decide whether to continue. Returns `false` (stop) once
/// `need` rows are collected, or when the examine budget is exhausted (setting
/// `budget_hit` so the caller falls back to the sorter).
fn ordered_walk_step(
    w: &mut OrderedWalk,
    v: &[u8],
    filter: &Option<Expr>,
    schema: &Schema,
) -> Result<bool> {
    let row: Vec<Value> = rowdec::decode_row(v)?;
    w.examined += 1;
    let keep = match filter {
        Some(e) => predicate::matches(e, schema, &row)?,
        None => true,
    };
    if keep {
        w.rows.push(row);
    }
    if w.rows.len() >= w.need {
        return Ok(false);
    }
    if w.examined >= w.budget {
        w.budget_hit = true;
        return Ok(false);
    }
    Ok(true)
}

/// Read a positive `usize` from `var`, else `default`.
fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Max rows an `IN (SELECT ...)` may materialize into an in-memory value list
/// (`ELYRASQL_IN_SUBQUERY_MAX`, default 1,000,000). Beyond this the query errors
/// fail-safe instead of buffering an unbounded list and evaluating it O(N×M).
fn in_subquery_max() -> usize {
    env_usize("ELYRASQL_IN_SUBQUERY_MAX", 1_000_000)
}

/// Max distinct rows `SELECT DISTINCT` may buffer (`ELYRASQL_DISTINCT_MAX`,
/// default 5,000,000) before erroring fail-safe rather than risking OOM.
fn distinct_max() -> usize {
    env_usize("ELYRASQL_DISTINCT_MAX", 5_000_000)
}

/// Max rows a **materialising** join may buffer (`ELYRASQL_JOIN_MAX_ROWS`, default
/// 10,000,000) before erroring fail-safe rather than risking OOM.
///
/// The streaming join paths are unaffected: they never hold the join output, so
/// they are bounded by the spilling sorter/aggregator instead. This guards the
/// shapes that still materialise — `FULL`, non-equi, derived-table and cross joins
/// — where the product of the inputs can dwarf both of them (a 3-way cross join
/// over a 4000-row table reaches ~1.3 billion rows, which took a process from
/// 97 MB to 97 GB RSS before the OS killed it).
fn join_max_rows() -> usize {
    env_usize("ELYRASQL_JOIN_MAX_ROWS", 10_000_000)
}

/// Budget for rows buffered by **all** materialising joins at once
/// (`ELYRASQL_JOIN_MAX_ROWS_TOTAL`, default 20,000,000).
///
/// A per-join cap alone does not bound the server: N concurrent joins each buffer
/// up to their own limit, so memory still scales with concurrency. This ceiling is
/// shared, so a burst of large joins is refused rather than swapping the machine.
fn join_max_rows_total() -> usize {
    env_usize("ELYRASQL_JOIN_MAX_ROWS_TOTAL", 20_000_000)
}

/// Memory ceiling for rows buffered by **all** materialising joins at once
/// (`ELYRASQL_JOIN_MAX_BYTES`, default 2 GiB).
///
/// The row-count ceiling above is a poor proxy for memory: 20M rows measured about
/// 5.4 GB for a narrow schema, but a wide one costs several times that for the same
/// count. This bound is what actually protects the process, with the row count kept
/// as a cheap secondary guard.
fn join_max_bytes() -> usize {
    use std::sync::OnceLock;
    static CACHE: OnceLock<usize> = OnceLock::new();
    *CACHE.get_or_init(|| {
        std::env::var("ELYRASQL_JOIN_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n >= 1 << 20)
            .unwrap_or(2 << 30)
    })
}

/// Rough heap cost of one buffered row.
///
/// Each row is a `Vec<Value>`, so the allocation header and the per-`Value` size
/// dominate for narrow rows, while `Text`/`Blob`/`Vector` payloads dominate for wide
/// ones. Estimated once from a sample row rather than measured per row: the budget
/// only needs the right order of magnitude to keep the process alive.
fn estimated_row_bytes(row: &[Value]) -> usize {
    const VEC_OVERHEAD: usize = 32;
    VEC_OVERHEAD
        + row
            .iter()
            .map(|v| {
                std::mem::size_of::<Value>()
                    + match v {
                        Value::Text(s) => s.len(),
                        Value::Bytes(b) => b.len(),
                        Value::Vector(f) => f.len() * std::mem::size_of::<f32>(),
                        Value::Json(j) => j.len(),
                        _ => 0,
                    }
            })
            .sum::<usize>()
}

/// Bytes currently reserved across all materialising joins.
static JOIN_BYTES_LIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Rows currently buffered across all materialising joins.
static JOIN_ROWS_LIVE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// An RAII reservation in the join row budget, enforcing both the per-join cap and
/// the shared ceiling.
///
/// Reservations are released on drop, so a join that finishes, errors, or unwinds
/// returns its share. As with the connection and prepared-statement budgets this
/// matters more than the check itself: a leaked reservation would permanently
/// shrink what later joins are allowed to do.
struct JoinBudget {
    /// Rows currently reserved from the shared ceiling by this join.
    reserved: usize,
    /// Bytes currently reserved from the shared memory ceiling by this join.
    reserved_bytes: usize,
    /// Estimated heap cost of one row, sampled from the first row this join buffers.
    /// `None` until the first sample, since the width is not known before that.
    row_bytes: Option<usize>,
}

impl JoinBudget {
    /// Rows reserved at a time, so the shared counter is touched rarely rather
    /// than per output row.
    const BLOCK: usize = 65_536;

    fn new() -> Self {
        Self {
            reserved: 0,
            reserved_bytes: 0,
            row_bytes: None,
        }
    }

    /// Sample the row width once, so the byte budget reflects this join's rows
    /// rather than an assumed size. Called with the first row the join buffers.
    #[inline]
    fn sample(&mut self, row: &[Value]) {
        if self.row_bytes.is_none() {
            self.row_bytes = Some(estimated_row_bytes(row).max(1));
        }
    }

    /// Account for a join that has buffered `rows` rows so far, growing the
    /// reservation as needed. Errors when either the per-join cap or the shared
    /// ceiling is reached, with a message that says how to proceed.
    #[inline]
    fn account(&mut self, rows: usize) -> Result<()> {
        if rows <= self.reserved {
            return Ok(());
        }
        let cap = join_max_rows();
        if rows > cap {
            return Err(Error::Query(format!(
                "join would materialise more than {cap} rows (ELYRASQL_JOIN_MAX_ROWS); \
                 add a join condition or a more selective WHERE, or raise the limit"
            )));
        }
        // Grow the reservation in blocks, never past the per-join cap.
        let want = rows.next_multiple_of(Self::BLOCK).min(cap);
        let extra = want - self.reserved;
        let total = join_max_rows_total();
        let mut cur = JOIN_ROWS_LIVE.load(std::sync::atomic::Ordering::Relaxed);
        loop {
            if cur + extra > total {
                return Err(Error::Query(format!(
                    "concurrent joins would buffer more than {total} rows in total \
                     (ELYRASQL_JOIN_MAX_ROWS_TOTAL); retry when the server is less \
                     busy, make the join more selective, or raise the limit"
                )));
            }
            match JOIN_ROWS_LIVE.compare_exchange_weak(
                cur,
                cur + extra,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.reserved = want;
                    // Row counts are only a proxy; the ceiling that actually keeps
                    // the process alive is memory, so reserve bytes for the same
                    // rows using this join's sampled row width.
                    if let Some(per_row) = self.row_bytes {
                        let want_bytes = want.saturating_mul(per_row);
                        if want_bytes > self.reserved_bytes {
                            let extra_bytes = want_bytes - self.reserved_bytes;
                            let total_bytes = join_max_bytes();
                            let mut curb =
                                JOIN_BYTES_LIVE.load(std::sync::atomic::Ordering::Relaxed);
                            loop {
                                if curb + extra_bytes > total_bytes {
                                    return Err(Error::Query(format!(
                                        "concurrent joins would buffer more than {} MiB in \
                                         total (ELYRASQL_JOIN_MAX_BYTES); retry when the \
                                         server is less busy, make the join more selective, \
                                         or raise the limit",
                                        total_bytes / (1 << 20)
                                    )));
                                }
                                match JOIN_BYTES_LIVE.compare_exchange_weak(
                                    curb,
                                    curb + extra_bytes,
                                    std::sync::atomic::Ordering::AcqRel,
                                    std::sync::atomic::Ordering::Relaxed,
                                ) {
                                    Ok(_) => {
                                        self.reserved_bytes = want_bytes;
                                        break;
                                    }
                                    Err(observed) => curb = observed,
                                }
                            }
                        }
                    }
                    return Ok(());
                }
                Err(observed) => cur = observed,
            }
        }
    }

    /// Rows reserved across all joins (asserted in tests).
    #[cfg(test)]
    fn live() -> usize {
        JOIN_ROWS_LIVE.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for JoinBudget {
    fn drop(&mut self) {
        if self.reserved > 0 {
            JOIN_ROWS_LIVE.fetch_sub(self.reserved, std::sync::atomic::Ordering::AcqRel);
        }
        if self.reserved_bytes > 0 {
            JOIN_BYTES_LIVE.fetch_sub(self.reserved_bytes, std::sync::atomic::Ordering::AcqRel);
        }
    }
}

/// Examine budget for a filtered ordered walk before falling back to the sorter.
/// `ELYRASQL_ORDER_SCAN_BUDGET` overrides the default of `max(need * 256, 50k)`.
/// Read per qualifying query (not per row), so it stays tunable at runtime.
fn ordered_scan_budget(need: usize) -> usize {
    match std::env::var("ELYRASQL_ORDER_SCAN_BUDGET")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
    {
        Some(n) => n.max(need),
        None => need.saturating_mul(256).max(50_000),
    }
}

/// True when the filter can be resolved through a *selective* access path -- an
/// equality on the primary key or a secondary index, or a full-text MATCH --
/// in which case the index path reads fewer rows than a clustered PK scan.
fn selective_filter(def: &TableDef, filter: Option<&Expr>) -> Result<bool> {
    let Some(f) = filter else {
        return Ok(false);
    };
    if match_conjunct(f).is_some() {
        return Ok(true);
    }
    if def.has_pk() && key_eq_values(def, Some(f), &def.pk_cols)?.is_some() {
        return Ok(true);
    }
    for idx in &def.indexes {
        if !idx.vector && key_eq_values(def, Some(f), &idx.cols)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

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
    if in_list_lookup(def, filter)?.is_some() {
        return Ok(true);
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
    // `false`: fall back to a sequential scan internally, so every existing caller
    // keeps getting a complete result.
    Ok(collect_matches_inner(db, def, filter, limit, false)
        .await?
        .expect("scan fallback always yields a result"))
}

/// As [`collect_matches`], but returns `None` when the filter is a *secondary*-index
/// range covering more of the table than the index is worth for.
///
/// Callers that have a cheaper way to scan the whole table (the columnar aggregate
/// paths) use this so a wide range does not materialise every row just to aggregate
/// it -- `COUNT(*) ... WHERE amt > 0` should cost what the unfiltered scan costs.
async fn collect_matches_narrow(
    db: &Session,
    def: &TableDef,
    filter: Option<&Expr>,
) -> Result<Option<Vec<(Vec<u8>, Vec<Value>)>>> {
    collect_matches_inner(db, def, filter, None, true).await
}

async fn collect_matches_inner(
    db: &Session,
    def: &TableDef,
    filter: Option<&Expr>,
    limit: Option<usize>,
    bail_on_wide_range: bool,
) -> Result<Option<Vec<(Vec<u8>, Vec<Value>)>>> {
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
                            let row: Vec<Value> = rowdec::decode_row(&bytes)?;
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
                    return Ok(Some(out));
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
                let row: Vec<Value> = rowdec::decode_row(&bytes)?;
                if recheck(&row)? {
                    out.push((key, row));
                }
            }
            return Ok(Some(out));
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
                    let row: Vec<Value> = rowdec::decode_row(&bytes)?;
                    if recheck(&row)? {
                        out.push((data_key, row));
                        if let Some(l) = limit {
                            if out.len() >= l {
                                return Ok(Some(out));
                            }
                        }
                    }
                }
            }
            return Ok(Some(out));
        }
    }

    // `col IN (literals)` on a PK or indexed column: look each value up through the
    // index instead of scanning and testing membership per row. Bounded by the same
    // budget as a range, so a list matching most of the table still scans.
    if let Some((col, vals)) = in_list_lookup(def, filter)? {
        let budget = index_range_budget(db, def).await?;
        // Collect the storage keys for every value *before* fetching any row: keys
        // are cheap, rows are not. If the list turns out to cover too much of the
        // table, the fallback to a scan then costs nothing but the key lookups.
        let mut seen = std::collections::HashSet::new();
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut over_budget = false;
        for v in &vals {
            for k in lookup_keys_by_eq(db, def, col, v).await? {
                // Dedupe so the result is a set: duplicate literals, or a value
                // appearing under several index entries, must not emit a row twice.
                if seen.insert(k.clone()) {
                    keys.push(k);
                }
            }
            if budget.is_some_and(|b| keys.len() > b) {
                over_budget = true;
                break;
            }
        }
        if !over_budget {
            // One batched read for the whole list rather than one per value.
            let blobs = db.multi_get(keys.clone()).await?;
            for (k, blob) in keys.into_iter().zip(blobs) {
                let Some(b) = blob else { continue };
                let row: Vec<Value> = rowdec::decode_row(&b)?;
                if let Some(f) = filter {
                    if !predicate::matches(f, &def.schema, &row)? {
                        continue;
                    }
                }
                out.push((k, row));
                if let Some(l) = limit {
                    if out.len() >= l {
                        return Ok(Some(out));
                    }
                }
            }
            return Ok(Some(out));
        }
        out.clear();
        if bail_on_wide_range {
            return Ok(None);
        }
    }

    // Range fast path: `col > x` / `BETWEEN` on a PK or indexed column uses an
    // ordered range scan, then re-applies the full filter.
    if let Some(rq) = range_bounds(def, filter)? {
        // A clustered (primary-key) range is a sequential read, so it is always
        // worth taking. A *secondary* index range pays a random fetch per row, so it
        // is only worth taking while it matches a small slice of the table --
        // otherwise fall through to the sequential scan below.
        let candidates = if def.pk_cols == [rq.col] {
            Some(clustered_range(db, def, &rq).await?)
        } else {
            let idx = index::index_on(def, rq.col).expect("range_bounds checked index");
            let budget = index_range_budget(db, def).await?;
            index_range(db, def, idx, &rq, budget).await?
        };
        if let Some(candidates) = candidates {
            for (k, row) in candidates {
                if let Some(f) = filter {
                    if !predicate::matches(f, &def.schema, &row)? {
                        continue;
                    }
                }
                out.push((k, row));
                if let Some(l) = limit {
                    if out.len() >= l {
                        return Ok(Some(out));
                    }
                }
            }
            return Ok(Some(out));
        }
        if bail_on_wide_range {
            return Ok(None);
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
            let row: Vec<Value> = rowdec::decode_row(&v)?;
            let keep = match filter {
                Some(f) => predicate::matches(f, &def.schema, &row)?,
                None => true,
            };
            if keep {
                out.push((k, row));
                if let Some(l) = limit {
                    if out.len() >= l {
                        return Ok(Some(out));
                    }
                }
            }
        }
        if last {
            break;
        }
    }
    Ok(Some(out))
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
            let resolved =
                resolve_subqueries_with_outer(db, vindex, bound, &def.schema, &row).await?;
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
    order_by: &[OrderByExpr],
    limit: Option<&Expr>,
) -> Result<QueryResult> {
    if !table.joins.is_empty() {
        if !order_by.is_empty() || limit.is_some() {
            return Err(Error::Unsupported(
                "ORDER BY / LIMIT is supported only for single-table UPDATE".into(),
            ));
        }
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

    let mut matches = mutation_matches(db, vindex, &def, &qualifier, selection, None).await?;
    if !order_by.is_empty() {
        let order = order_by
            .iter()
            .map(|expr| (expr.expr.clone(), expr.asc.unwrap_or(true)))
            .collect::<Vec<_>>();
        sort_mutation_matches(&mut matches, &def.schema, &order, &db.cancel_token())?;
    }
    if let Some(limit) = limit {
        matches.truncate(eval_usize(limit)?);
    }
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
            new_row[*idx] = coerce_for_session(db, v, &col.ty, &col.name)?;
        }
        for (i, ge) in &generated {
            let v = predicate::eval_row(ge, &def.schema, &new_row)?;
            let col = &def.schema.columns[*i];
            new_row[*i] = coerce_for_session(db, v, &col.ty, &col.name)?;
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
                    new_base[s.col] = coerce_for_session(db, v, &col.ty, &col.name)?;
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
/// The 1-based ordinal of a positional ORDER BY / GROUP BY item
/// (`ORDER BY 2`), if `e` is a positive integer literal.
fn order_ordinal(e: &Expr) -> Option<usize> {
    match e {
        Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
            n.parse::<usize>().ok().filter(|&x| x >= 1)
        }
        _ => None,
    }
}

fn projected_order_expr(
    name: &str,
    projection: &[sqlparser::ast::SelectItem],
    schema: &Schema,
) -> Option<Expr> {
    use sqlparser::ast::{Ident, SelectItem};

    let source_expr = |column: &str| {
        let parts = column.split('.').map(Ident::new).collect::<Vec<_>>();
        match parts.as_slice() {
            [identifier] => Expr::Identifier(identifier.clone()),
            _ => Expr::CompoundIdentifier(parts),
        }
    };
    let mut matches = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                matches.extend(
                    schema
                        .columns
                        .iter()
                        .filter(|column| {
                            output_column_name(&column.name).eq_ignore_ascii_case(name)
                        })
                        .map(|column| source_expr(&column.name)),
                );
            }
            SelectItem::QualifiedWildcard(object, _) => {
                let qualifier = object
                    .0
                    .last()
                    .map(|identifier| identifier.value.as_str())
                    .unwrap_or_default();
                matches.extend(schema.columns.iter().filter_map(|column| {
                    let (column_qualifier, column_name) = column.name.split_once('.')?;
                    (column_qualifier.eq_ignore_ascii_case(qualifier)
                        && column_name.eq_ignore_ascii_case(name))
                    .then(|| source_expr(&column.name))
                }));
            }
            SelectItem::UnnamedExpr(expr) => {
                if ident_name(expr).is_some_and(|output| output.eq_ignore_ascii_case(name)) {
                    matches.push(expr.clone());
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if alias.value.eq_ignore_ascii_case(name) {
                    matches.push(expr.clone());
                }
            }
        }
    }
    if matches.len() == 1 {
        matches.pop()
    } else {
        None
    }
}

fn resolve_order_aliases(
    order: &[(Expr, bool)],
    projection: &[sqlparser::ast::SelectItem],
    schema: &Schema,
) -> Vec<(Expr, bool)> {
    use sqlparser::ast::SelectItem;
    order
        .iter()
        .map(|(e, asc)| {
            // Positional ORDER BY -> the Nth projected expression.
            if let Some(n) = order_ordinal(e) {
                if let Some(SelectItem::UnnamedExpr(expr))
                | Some(SelectItem::ExprWithAlias { expr, .. }) = projection.get(n - 1)
                {
                    return (expr.clone(), *asc);
                }
            }
            if let Some(name) = ident_name(e) {
                let is_column = schema
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(name));
                if !is_column {
                    if let Some(expr) = projected_order_expr(name, projection, schema) {
                        return (expr, *asc);
                    }
                }
            }
            (e.clone(), *asc)
        })
        .collect()
}

fn resolve_output_order_expressions(
    order: &[(Expr, bool)],
    projection: &[sqlparser::ast::SelectItem],
    output_schema: &Schema,
) -> Vec<(Expr, bool)> {
    use sqlparser::ast::{Ident, SelectItem};

    order
        .iter()
        .map(|(order_expr, ascending)| {
            let output_name = projection.iter().enumerate().find_map(|(index, item)| {
                let projected_expr = match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                    _ => return None,
                };
                (projected_expr == order_expr)
                    .then(|| output_schema.columns.get(index))
                    .flatten()
                    .map(|column| column.name.clone())
            });
            match output_name {
                Some(name) => (Expr::Identifier(Ident::new(name)), *ascending),
                None => (order_expr.clone(), *ascending),
            }
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
                    names.push(output_column_name(&c.name).to_owned());
                    projs.push(Proj::Col(i));
                }
            }
            // `alias.*` -> every column qualified by `alias` (join schemas name
            // columns `alias.col`). Falls back to matching the bare table name.
            SelectItem::QualifiedWildcard(obj, _) => {
                let qual = obj.0.last().map(|i| i.value.clone()).unwrap_or_default();
                let mut matched = false;
                for (i, c) in schema.columns.iter().enumerate() {
                    if let Some((q, name)) = c.name.split_once('.') {
                        if q.eq_ignore_ascii_case(&qual) {
                            names.push(name.to_owned());
                            projs.push(Proj::Col(i));
                            matched = true;
                        }
                    }
                }
                if !matched {
                    return Err(Error::Unsupported(format!(
                        "unknown table qualifier in `{qual}.*`"
                    )));
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
        // Carry the source column's type AND collation through a direct column
        // projection, so DISTINCT / ORDER BY on a projected `_bin` column stays
        // case-sensitive. Computed expressions default to Text/Ci.
        let (ty, collation) = match p {
            Proj::Col(i) => (schema.columns[*i].ty.clone(), schema.columns[*i].collation),
            Proj::Expr(e) => {
                match col_ref_name(e).and_then(|n| predicate::resolve_index(&n, schema).ok()) {
                    Some(idx) => (
                        schema.columns[idx].ty.clone(),
                        schema.columns[idx].collation,
                    ),
                    None => (
                        out_rows
                            .iter()
                            .map(|r| &r[ci])
                            .find(|v| !v.is_null())
                            .map(infer_val)
                            .unwrap_or(ColumnType::Text),
                        elyra_core::Collation::Ci,
                    ),
                }
            }
        };
        cols.push(ColumnDef {
            name: name.clone(),
            ty,
            nullable: true,
            collation,
        });
    }

    Ok((Schema::new(cols), out_rows))
}

fn output_column_name(name: &str) -> &str {
    name.split_once('.').map_or(name, |(_, column)| column)
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
        Value::UInt(_) => ColumnType::UInt,
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
/// Aggregate function sub-expressions of a HAVING clause that must be computed
/// as hidden output columns because they are not already in the SELECT list.
fn having_hidden_items(
    projection: &[sqlparser::ast::SelectItem],
    having: Option<&Expr>,
) -> Vec<sqlparser::ast::SelectItem> {
    use sqlparser::ast::{Ident, SelectItem};
    let Some(h) = having else { return Vec::new() };
    let mut aggs = Vec::new();
    collect_agg_exprs(h, &mut aggs);
    if aggs.is_empty() {
        return Vec::new();
    }
    let existing: std::collections::HashSet<String> = projection
        .iter()
        .filter_map(|it| match it {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                Some(e.to_string())
            }
            _ => None,
        })
        .collect();
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for a in aggs {
        let t = a.to_string();
        if existing.contains(&t) || !seen.insert(t) {
            continue;
        }
        let alias = Ident::new(format!("__hv_{}", out.len()));
        out.push(SelectItem::ExprWithAlias { expr: a, alias });
    }
    out
}

/// Add projection columns needed only to evaluate HAVING or ORDER BY after
/// grouping. MySQL permits ORDER BY to reference a representative source value
/// that is not returned when full-group enforcement is disabled.
fn aggregate_projection_with_hidden(
    projection: &[sqlparser::ast::SelectItem],
    having: Option<&Expr>,
    order: &[(Expr, bool)],
    source_schema: &Schema,
) -> (Vec<sqlparser::ast::SelectItem>, usize) {
    use sqlparser::ast::SelectItem;

    let mut augmented = projection.to_vec();
    augmented.extend(having_hidden_items(projection, having));
    let resolved_order = resolve_order_aliases(order, projection, source_schema);

    for ((original, _), (resolved, _)) in order.iter().zip(resolved_order) {
        if order_ordinal(original).is_some()
            || projection_exposes_order_expr(&augmented, original, &resolved)
        {
            continue;
        }
        augmented.push(SelectItem::UnnamedExpr(resolved));
    }

    let hidden = augmented.len().saturating_sub(projection.len());
    (augmented, hidden)
}

fn projection_exposes_order_expr(
    projection: &[sqlparser::ast::SelectItem],
    original: &Expr,
    resolved: &Expr,
) -> bool {
    use sqlparser::ast::SelectItem;

    let original_name = ident_name(original);
    projection.iter().any(|item| match item {
        SelectItem::Wildcard(_) => ident_name(resolved).is_some(),
        SelectItem::QualifiedWildcard(_, _) => false,
        SelectItem::UnnamedExpr(expr) => {
            expr == original
                || expr == resolved
                || original_name.is_some_and(|name| {
                    ident_name(expr).is_some_and(|output| output.eq_ignore_ascii_case(name))
                })
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            expr == original
                || expr == resolved
                || original_name.is_some_and(|name| alias.value.eq_ignore_ascii_case(name))
        }
    })
}

fn truncate_hidden_columns(schema: &mut Schema, rows: &mut [Vec<Value>], hidden: usize) {
    if hidden == 0 {
        return;
    }
    let visible = schema.columns.len().saturating_sub(hidden);
    schema.columns.truncate(visible);
    for row in rows {
        row.truncate(visible);
    }
}

/// Collect aggregate-function sub-expressions (not recursing into their args).
fn collect_agg_exprs(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::Function(f) => {
            let name = f
                .name
                .0
                .last()
                .map(|i| i.value.to_ascii_lowercase())
                .unwrap_or_default();
            if matches!(
                name.as_str(),
                "count"
                    | "sum"
                    | "avg"
                    | "min"
                    | "max"
                    | "group_concat"
                    | "std"
                    | "stddev"
                    | "stddev_pop"
                    | "stddev_samp"
                    | "variance"
                    | "var_pop"
                    | "var_samp"
                    | "bit_or"
                    | "bit_and"
                    | "bit_xor"
            ) {
                out.push(e.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_agg_exprs(left, out);
            collect_agg_exprs(right, out);
        }
        Expr::Nested(x) | Expr::UnaryOp { expr: x, .. } => collect_agg_exprs(x, out),
        _ => {}
    }
}

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
    // Resolve any named windows (`WINDOW w AS (...)` + `OVER w`) into inline
    // window specs, so the rest of the pipeline only sees WindowSpecs.
    let resolved = resolve_named_windows(select)?;
    let select = &resolved;
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

    // Classify each projection item once, instead of rebuilding its expression for
    // every row.
    //
    // The original code called `map_expr` per row per item, which cloned the whole
    // expression tree, searched the window list by *deep* `Expr` equality, and
    // allocated a literal node from the value -- then re-interpreted the result. For
    // the ordinary shapes (`SELECT id, ROW_NUMBER() OVER (...) FROM t`) none of that
    // is needed: the item either *is* a window expression, whose value is already
    // computed per row, or contains none at all.
    enum Out<'a> {
        /// A source column expanded from `*` or `relation.*`.
        Column(usize),
        /// The item is exactly window expression `k`: take the precomputed value.
        Window(usize),
        /// The item contains no window function: evaluate it against the row.
        Plain(&'a Expr),
        /// A window function nested inside a larger expression (e.g. `rn + 1`):
        /// substitute and evaluate, as before.
        Mixed(&'a Expr),
    }
    struct PlannedOut<'a> {
        name: String,
        source_column: Option<usize>,
        evaluator: Out<'a>,
    }
    let mut plan: Vec<PlannedOut<'_>> = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                plan.extend(
                    schema
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(index, column)| PlannedOut {
                            name: output_column_name(&column.name).to_owned(),
                            source_column: Some(index),
                            evaluator: Out::Column(index),
                        }),
                );
                continue;
            }
            SelectItem::QualifiedWildcard(object, _) => {
                let qualifier = object
                    .0
                    .last()
                    .map(|identifier| identifier.value.as_str())
                    .unwrap_or_default();
                let relation_matches = select.from.len() == 1
                    && select.from[0].joins.is_empty()
                    && factor_qualifier(&select.from[0].relation)
                        .is_some_and(|relation| relation.eq_ignore_ascii_case(qualifier));
                if !relation_matches {
                    return Err(Error::Unsupported(format!(
                        "qualified wildcard {object}.* matched no relation"
                    )));
                }
                plan.extend(
                    schema
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(index, column)| PlannedOut {
                            name: output_column_name(&column.name).to_owned(),
                            source_column: Some(index),
                            evaluator: Out::Column(index),
                        }),
                );
                continue;
            }
            SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {}
        }
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => unreachable!(),
        };
        let evaluator = if let Some(k) = win_values.iter().position(|(we, _)| we == expr) {
            Out::Window(k)
        } else {
            let mut nested = Vec::new();
            collect_window_exprs(expr, &mut nested);
            if nested.is_empty() {
                Out::Plain(expr)
            } else {
                Out::Mixed(expr)
            }
        };
        let name = match item {
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            SelectItem::UnnamedExpr(expr) => ident_name(expr)
                .map(str::to_owned)
                .unwrap_or_else(|| expr.to_string()),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => unreachable!(),
        };
        plan.push(PlannedOut {
            name,
            source_column: None,
            evaluator,
        });
    }

    let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let mut vals = Vec::with_capacity(plan.len());
        for planned in &plan {
            vals.push(match &planned.evaluator {
                Out::Column(index) => row[*index].clone(),
                Out::Window(k) => win_values[*k].1[i].clone(),
                Out::Plain(e) => predicate::eval_row(e, schema, row)?,
                Out::Mixed(e) => {
                    let subst = |x: &Expr| -> Option<Expr> {
                        win_values
                            .iter()
                            .find(|(we, _)| we == x)
                            .map(|(_, vs)| value_to_expr(&vs[i]))
                    };
                    predicate::eval_row(&map_expr(e, &subst), schema, row)?
                }
            });
        }
        out_rows.push(vals);
    }

    // Output schema (names + inferred types).
    let mut cols = Vec::with_capacity(plan.len());
    for (column_index, planned) in plan.iter().enumerate() {
        let source = planned
            .source_column
            .and_then(|index| schema.columns.get(index));
        let ty = source.map(|column| column.ty.clone()).unwrap_or_else(|| {
            out_rows
                .iter()
                .map(|row| &row[column_index])
                .find(|value| !value.is_null())
                .map(infer_val)
                .unwrap_or(ColumnType::Text)
        });
        cols.push(ColumnDef {
            name: planned.name.clone(),
            ty,
            nullable: source.is_none_or(|column| column.nullable),
            collation: source.map_or(elyra_core::Collation::Ci, |column| column.collation),
        });
    }
    let out_schema = Schema::new(cols);

    // ORDER BY may reference an output column *or* a base-table column that is not
    // projected -- MySQL allows both, and rejecting the latter made queries like
    // `SELECT amt, RANK() OVER (...) FROM t ORDER BY id` fail here while succeeding
    // without the window function. Sort a permutation so a key can be taken from
    // either side: the base rows and the output rows are still index-aligned.
    if !order_exprs.is_empty() {
        let mut perm: Vec<usize> = (0..out_rows.len()).collect();
        let mut keys: Vec<Vec<Value>> = Vec::with_capacity(out_rows.len());
        for (i, orow) in out_rows.iter().enumerate() {
            let mut k = Vec::with_capacity(order_exprs.len());
            for (e, _) in order_exprs {
                // Prefer the output column (so an alias or a projected expression
                // wins, as elsewhere), then fall back to the base row.
                let v = match predicate::eval_row(e, &out_schema, orow) {
                    Ok(v) => v,
                    Err(_) => predicate::eval_row(e, schema, &rows[i])?,
                };
                k.push(v);
            }
            keys.push(k);
        }
        perm.sort_by(|&a, &b| cmp_order_keys(&keys[a], &keys[b], order_exprs));
        let mut sorted = Vec::with_capacity(out_rows.len());
        for i in perm {
            sorted.push(std::mem::take(&mut out_rows[i]));
        }
        out_rows = sorted;
    }
    apply_offset_limit(&mut out_rows, offset, limit);
    Ok(QueryResult::Rows(RowStream::literal(out_schema, out_rows)))
}

/// Replace `OVER w` / `OVER (w ...)` named-window references in a SELECT's
/// projection with inline window specs from its `WINDOW` clause.
fn resolve_named_windows(select: &Select) -> Result<Select> {
    if select.named_window.is_empty() {
        return Ok(select.clone());
    }
    // Build name -> definition, following NamedWindow chains (bounded).
    let mut defs: WindowDefs = std::collections::HashMap::new();
    for nw in &select.named_window {
        defs.insert(nw.0.value.to_ascii_lowercase(), nw.1.clone());
    }
    use sqlparser::ast::SelectItem;
    let mut out = select.clone();
    for item in out.projection.iter_mut() {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                resolve_over_in_expr(e, &defs)?;
            }
            _ => {}
        }
    }
    Ok(out)
}

type WindowDefs = std::collections::HashMap<String, sqlparser::ast::NamedWindowExpr>;

/// Resolve a named window to a concrete spec, following NamedWindow chains.
fn resolve_window_spec(
    name: &str,
    defs: &WindowDefs,
    depth: u32,
) -> Result<sqlparser::ast::WindowSpec> {
    use sqlparser::ast::NamedWindowExpr;
    if depth > 16 {
        return Err(Error::Query("named window reference cycle".into()));
    }
    match defs.get(&name.to_ascii_lowercase()) {
        Some(NamedWindowExpr::WindowSpec(s)) => Ok(s.clone()),
        Some(NamedWindowExpr::NamedWindow(other)) => {
            resolve_window_spec(&other.value, defs, depth + 1)
        }
        None => Err(Error::Query(format!("undefined window: {name}"))),
    }
}

/// Rewrite window-function `.over` references (named windows) to inline specs.
fn resolve_over_in_expr(e: &mut Expr, defs: &WindowDefs) -> Result<()> {
    use sqlparser::ast::WindowType;
    match e {
        Expr::Function(f) => {
            match &f.over {
                Some(WindowType::NamedWindow(name)) => {
                    let spec = resolve_window_spec(&name.value, defs, 0)?;
                    f.over = Some(WindowType::WindowSpec(spec));
                }
                Some(WindowType::WindowSpec(s)) if s.window_name.is_some() => {
                    // `OVER (w ...)`: inherit the base window, add local clauses.
                    let base =
                        resolve_window_spec(&s.window_name.as_ref().unwrap().value, defs, 0)?;
                    let merged = sqlparser::ast::WindowSpec {
                        window_name: None,
                        partition_by: base.partition_by,
                        order_by: if s.order_by.is_empty() {
                            base.order_by
                        } else {
                            s.order_by.clone()
                        },
                        window_frame: s.window_frame.clone().or(base.window_frame),
                    };
                    f.over = Some(WindowType::WindowSpec(merged));
                }
                _ => {}
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            resolve_over_in_expr(left, defs)?;
            resolve_over_in_expr(right, defs)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => {
            resolve_over_in_expr(expr, defs)
        }
        _ => Ok(()),
    }
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
    //
    // Keys are raw collation-key bytes. They used to be built as a `String` by
    // mapping each byte through `as char`, which re-encodes every byte >= 0x80 as a
    // multi-byte UTF-8 sequence, and the key was cloned once per row for the map
    // lookup.
    let mut partitions: Vec<Vec<usize>> = Vec::new();
    if spec.partition_by.is_empty() {
        // No PARTITION BY: one partition over every row, so skip the hashing
        // entirely -- this is the shape `OVER (ORDER BY ...)` takes.
        partitions.push((0..rows.len()).collect());
    } else {
        let mut index: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
        let mut key = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            key.clear();
            for p in &spec.partition_by {
                key.extend_from_slice(&predicate::eval_row(p, schema, row)?.collation_key());
                key.push(1);
            }
            match index.get(key.as_slice()) {
                // Existing partition: no allocation on the hot path.
                Some(&slot) => partitions[slot].push(i),
                None => {
                    index.insert(key.clone(), partitions.len());
                    partitions.push(vec![i]);
                }
            }
        }
    }

    let order: Vec<(Expr, bool)> = spec
        .order_by
        .iter()
        .map(|o| (o.expr.clone(), o.asc.unwrap_or(true)))
        .collect();
    let ordered = !order.is_empty();

    let mut result = vec![Value::Null; rows.len()];
    for mut idxs in partitions {
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
        "ntile" => {
            let buckets = args
                .first()
                .and_then(|e| predicate::eval_row(e, schema, &rows[idxs[0]]).ok())
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0)
                .max(1.0) as usize;
            let n = idxs.len();
            // Distribute n rows into `buckets` groups; the first (n % buckets)
            // groups get one extra row (MySQL semantics).
            //
            // Computed per row (O(rows)), never per bucket: `buckets` comes from
            // the query, so iterating it would let `NTILE(1e12)` spin a CPU core
            // regardless of table size. Buckets beyond the row count are simply
            // empty, which is also what MySQL reports.
            let base = n / buckets; // rows in a "small" bucket
            let rem = n % buckets; // number of "large" buckets (base + 1 rows)
            let big = rem * (base + 1); // rows covered by the large buckets
            for (i, &row) in idxs.iter().enumerate() {
                let bucket = if i < big {
                    i / (base + 1)
                } else {
                    // `base == 0` implies `big == n`, so this branch is only
                    // reached with a non-zero divisor; `checked_div` keeps that
                    // panic-free without relying on the reasoning.
                    (i - big).checked_div(base).map_or(i, |q| rem + q)
                };
                result[row] = Value::Int(bucket as i64 + 1);
            }
        }
        "first_value" => {
            // Frame starts at the partition start by default, so this is the
            // first ordered row's value.
            let v = if idxs.is_empty() {
                Value::Null
            } else {
                arg_val(idxs[0])?
            };
            for &i in idxs {
                result[i] = v.clone();
            }
        }
        "last_value" => {
            // Whole-partition last value (the common intent); explicit frames are
            // not applied to LAST_VALUE here.
            let v = match idxs.last() {
                Some(&last) => arg_val(last)?,
                None => Value::Null,
            };
            for &i in idxs {
                result[i] = v.clone();
            }
        }
        "nth_value" => {
            let nth = args
                .get(1)
                .and_then(|e| predicate::eval_row(e, schema, &rows[idxs[0]]).ok())
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0) as usize;
            let v = if nth >= 1 && nth <= idxs.len() {
                arg_val(idxs[nth - 1])?
            } else {
                Value::Null
            };
            for &i in idxs {
                result[i] = v.clone();
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
        let resolved = resolve_subqueries_with_outer(db, vindex, bound, &def.schema, &row).await?;
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
        sort_rows_with_subqueries(
            db,
            vindex,
            &mut matched,
            &def.schema,
            &resolved,
            |expr, row| bind_outer(expr, outer, &def.schema, row),
        )
        .await?;
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
                // `*` / `table.*`: expand to all base columns of the (single) table.
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                    for v in row {
                        vals.push(v.clone());
                    }
                    continue;
                }
            };
            let bound = bind_outer(expr, outer, &def.schema, row);
            let resolved =
                resolve_subqueries_with_outer(db, vindex, bound, &def.schema, row).await?;
            vals.push(predicate::eval_row(&resolved, &def.schema, row)?);
        }
        out_rows.push(vals);
    }

    // Output schema: names from the projection, types from the first row.
    let mut cols = Vec::new();
    for item in &select.projection {
        match item {
            // Wildcards expand to the base table's columns, in order.
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                for c in &def.schema.columns {
                    cols.push(ColumnDef {
                        name: c.name.clone(),
                        ty: c.ty.clone(),
                        nullable: c.nullable,
                        collation: c.collation,
                    });
                }
            }
            _ => {
                let name = match item {
                    SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                    SelectItem::UnnamedExpr(e) => ident_name(e)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| e.to_string()),
                    _ => format!("col{}", cols.len()),
                };
                let ci = cols.len();
                let ty = out_rows
                    .first()
                    .and_then(|r| r.get(ci))
                    .map(infer_val)
                    .unwrap_or(ColumnType::Text);
                cols.push(ColumnDef {
                    name,
                    ty,
                    nullable: true,
                    collation: elyra_core::Collation::Ci,
                });
            }
        }
    }
    Ok(QueryResult::Rows(RowStream::literal(
        Schema::new(cols),
        out_rows,
    )))
}

/// Rewrite qualified outer column references (`outer.col`) in `expr` to
/// literals from `row`, including inside subqueries. Bare names remain bound
/// to the innermost query scope.
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
        _ => None,
    })
}

/// Resolve subqueries after qualified correlation has been bound. If an inner
/// scope does not own a bare column, retry that name against the immediate
/// outer row. A name that resolves in the inner scope never reaches this path,
/// preserving the usual nearest-scope precedence.
async fn resolve_subqueries_with_outer(
    db: &Session,
    vindex: &VectorRegistry,
    expr: Expr,
    outer_schema: &Schema,
    outer_row: &[Value],
) -> Result<Expr> {
    let mut bound = expr;
    let mut rebound = Vec::new();
    loop {
        match resolve_subqueries(db, vindex, bound.clone()).await {
            Ok(resolved) => return Ok(resolved),
            Err(error) => {
                let Some(column) = bare_unknown_column(&error).map(str::to_owned) else {
                    return Err(error);
                };
                if rebound
                    .iter()
                    .any(|name: &String| name.eq_ignore_ascii_case(&column))
                {
                    return Err(error);
                }
                let index = match predicate::resolve_index(&column, outer_schema) {
                    Ok(index) => index,
                    Err(Error::Catalog(_)) => return Err(error),
                    Err(error) => return Err(error),
                };
                let value = value_to_expr(&outer_row[index]);
                bound = map_expr(&bound, &|candidate| match candidate {
                    Expr::Identifier(identifier)
                        if identifier.value.eq_ignore_ascii_case(&column) =>
                    {
                        Some(value.clone())
                    }
                    _ => None,
                });
                rebound.push(column);
            }
        }
    }
}

fn bare_unknown_column(error: &Error) -> Option<&str> {
    let Error::Catalog(message) = error else {
        return None;
    };
    let column = message.strip_prefix("unknown column: ")?;
    (!column.contains('.')).then_some(column)
}

async fn sort_rows_with_subqueries(
    db: &Session,
    vindex: &VectorRegistry,
    rows: &mut [Vec<Value>],
    schema: &Schema,
    order: &[(Expr, bool)],
    bind: impl Fn(&Expr, &[Value]) -> Expr,
) -> Result<()> {
    let mut check = db.cancel_check();
    let mut keyed = Vec::with_capacity(rows.len());
    for (position, row) in rows.iter().enumerate() {
        check.tick()?;
        let mut keys = Vec::with_capacity(order.len());
        for (expr, _) in order {
            let bound = bind(expr, row);
            let resolved = resolve_subqueries_with_outer(db, vindex, bound, schema, row).await?;
            keys.push(predicate::eval_row(&resolved, schema, row)?);
        }
        keyed.push((keys, position));
    }
    let collations = order_key_collations(order, schema);
    sort_keyed_coll(&mut keyed, order, &collations);
    reorder(rows, &keyed);
    Ok(())
}

/// Materialise a subquery's rows by executing it through the query engine.
async fn run_subquery(
    db: &Session,
    vindex: &VectorRegistry,
    q: &SqlQuery,
) -> Result<Vec<Vec<Value>>> {
    run_subquery_capped(db, vindex, q, usize::MAX).await
}

/// Like [`run_subquery`] but errors fail-safe if the result exceeds `cap` rows,
/// so an `IN (SELECT ...)` over an enormous set cannot exhaust memory.
async fn run_subquery_capped(
    db: &Session,
    vindex: &VectorRegistry,
    q: &SqlQuery,
    cap: usize,
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
                if rows.len() > cap {
                    return Err(Error::Query(format!(
                        "subquery returned more than {cap} rows for IN (...); use a JOIN or \
                         EXISTS, or raise ELYRASQL_IN_SUBQUERY_MAX"
                    )));
                }
            }
            Ok(rows)
        }
        QueryResult::Affected(_) | QueryResult::Insert { .. } => Ok(Vec::new()),
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
        QueryResult::Affected(_) | QueryResult::Insert { .. } => {
            Ok((Schema::new(Vec::new()), Vec::new()))
        }
    }
}

fn value_to_expr(v: &Value) -> Expr {
    use sqlparser::ast::Value as V;
    let lit = match v {
        Value::Null => V::Null,
        Value::Bool(b) => V::Boolean(*b),
        Value::Int(i) => V::Number(i.to_string(), false),
        Value::UInt(u) => V::Number(u.to_string(), false),
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
                let rows = run_subquery_capped(db, vindex, &subquery, in_subquery_max()).await?;
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

/// Detect a `HYBRID(text_col, 'query', vec_col, vec)` ranking call — the
/// first-class hybrid-search primitive that fuses full-text and vector
/// relevance. Returns `(text column, text query, vector column, vector expr)`.
fn hybrid_call(expr: &Expr) -> Option<(String, String, String, &Expr)> {
    let Expr::Function(f) = expr else { return None };
    if !f.name.0.last()?.value.eq_ignore_ascii_case("hybrid") {
        return None;
    }
    let args = fn_arg_exprs(f);
    if args.len() != 4 {
        return None;
    }
    let text_col = ident_name(args[0])?.to_string();
    let text_query = match eval_expr(args[1]).ok()? {
        Value::Text(s) => s,
        v => v.to_wire_string()?,
    };
    let vec_col = ident_name(args[2])?.to_string();
    Some((text_col, text_query, vec_col, args[3]))
}

/// `SELECT ..., HYBRID(text_col, 'query', vec_col, '[..]') AS score FROM t
/// [WHERE ...] ORDER BY score DESC LIMIT k` — fuse a full-text ranking and a
/// vector (HNSW) ranking with **Reciprocal Rank Fusion**, honouring the
/// structured `WHERE` filter. One query, one file: no external search engine.
#[allow(clippy::too_many_arguments)]
async fn hybrid_select(
    db: &Session,
    vindex: &VectorRegistry,
    select: &Select,
    def: &TableDef,
    filter: Option<&Expr>,
    text_col: &str,
    text_query: &str,
    vec_col: &str,
    vec_expr: &Expr,
    offset: usize,
    limit: Option<usize>,
) -> Result<QueryResult> {
    use sqlparser::ast::SelectItem;
    use std::collections::{HashMap, HashSet};
    const RRF_K: f64 = 60.0;
    let k = limit.unwrap_or(10);
    let fanout = (k.max(1) * 10).clamp(50, 500);

    let text_ci = col_of(def, text_col)
        .ok_or_else(|| Error::Query(format!("HYBRID: unknown column {text_col}")))?;
    let vec_ci = col_of(def, vec_col)
        .ok_or_else(|| Error::Query(format!("HYBRID: unknown column {vec_col}")))?;
    if !matches!(def.schema.columns[vec_ci].ty, ColumnType::Vector(_)) {
        return Err(Error::Query(format!(
            "HYBRID: {vec_col} is not a VECTOR column"
        )));
    }
    let qvec = match eval_expr(vec_expr)? {
        Value::Text(s) => parse_vec_free(&s)?,
        Value::Vector(v) => v,
        _ => {
            return Err(Error::Query(
                "HYBRID: vector query must be a vector literal".into(),
            ))
        }
    };

    // --- Vector ranking via the HNSW index ---
    if !def
        .indexes
        .iter()
        .any(|i| i.vector && i.single_col() == Some(vec_ci))
    {
        return Err(Error::Query(format!(
            "HYBRID: {vec_col} has no vector index (CREATE VECTOR INDEX first)"
        )));
    }
    let cached = vindex.get(db, def, vec_ci, Metric::L2).await?;
    let hits = cached.search_keys(&qvec, fanout, (fanout * 2).max(64));
    let vec_rank: HashMap<Vec<u8>, usize> = hits
        .iter()
        .enumerate()
        .map(|(rank, (key, _))| (key.clone(), rank))
        .collect();

    // --- Full-text ranking (term-frequency over stemmed query terms) ---
    let terms: Vec<String> = text_query
        .split_whitespace()
        .map(|w| {
            w.chars()
                .filter(|c| c.is_alphanumeric())
                .collect::<String>()
        })
        .filter(|w| !w.is_empty())
        .map(|w| crate::ft::stem(&w))
        .collect();
    let mut ft_score: HashMap<Vec<u8>, u32> = HashMap::new();
    let ft_idx = def
        .indexes
        .iter()
        .find(|i| i.fulltext && i.single_col() == Some(text_ci));
    if let Some(idx) = ft_idx {
        for term in &terms {
            for dk in index::fulltext_lookup(db, &def.name, &idx.name, term).await? {
                *ft_score.entry(dk).or_default() += 1;
            }
        }
    } else {
        // No full-text index: scan and score by distinct query-term presence.
        let prefix = data_prefix(&def.name);
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let batch = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
            if batch.is_empty() {
                break;
            }
            let last = batch.len() < 4096;
            cursor = batch.last().map(|(k, _)| k.clone());
            for (kk, v) in batch {
                let row: Vec<Value> = rowdec::decode_row(&v)?;
                if let Some(Value::Text(txt)) = row.get(text_ci) {
                    let doc: HashSet<String> = crate::ft::tokenize(txt).into_iter().collect();
                    let hitn = terms.iter().filter(|t| doc.contains(*t)).count() as u32;
                    if hitn > 0 {
                        ft_score.insert(kk, hitn);
                    }
                }
            }
            if last {
                break;
            }
        }
    }
    let mut ft_sorted: Vec<(Vec<u8>, u32)> = ft_score.into_iter().collect();
    ft_sorted.sort_by_key(|b| std::cmp::Reverse(b.1));
    ft_sorted.truncate(fanout);
    let ft_rank: HashMap<Vec<u8>, usize> = ft_sorted
        .iter()
        .enumerate()
        .map(|(r, (kk, _))| (kk.clone(), r))
        .collect();

    // --- Reciprocal Rank Fusion ---
    let mut keys: HashSet<Vec<u8>> = HashSet::new();
    keys.extend(vec_rank.keys().cloned());
    keys.extend(ft_rank.keys().cloned());
    let mut scored: Vec<(Vec<u8>, f64)> = keys
        .into_iter()
        .map(|key| {
            let mut s = 0.0;
            if let Some(r) = vec_rank.get(&key) {
                s += 1.0 / (RRF_K + *r as f64);
            }
            if let Some(r) = ft_rank.get(&key) {
                s += 1.0 / (RRF_K + *r as f64);
            }
            (key, s)
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    // --- Fetch rows, apply the structured WHERE, keep the fused score ---
    let order: Vec<Vec<u8>> = scored.iter().map(|(k, _)| k.clone()).collect();
    let blobs = db.multi_get(order).await?;
    let mut results: Vec<(Vec<Value>, f64)> = Vec::new();
    for ((_, score), blob) in scored.iter().zip(blobs) {
        let Some(bytes) = blob else { continue };
        let row: Vec<Value> = rowdec::decode_row(&bytes)?;
        if let Some(f) = filter {
            if !predicate::matches(f, &def.schema, &row)? {
                continue;
            }
        }
        results.push((row, *score));
    }
    let start = offset.min(results.len());
    results.drain(..start);
    results.truncate(k);

    // --- Project (HYBRID(...) -> the fused score) ---
    enum P<'a> {
        Col(usize),
        Score,
        Expr(&'a Expr),
    }
    let text_col_def = |name: &str, ty: ColumnType| elyra_core::ColumnDef {
        name: name.to_string(),
        ty,
        nullable: true,
        collation: elyra_core::Collation::Ci,
    };
    let mut cols: Vec<elyra_core::ColumnDef> = Vec::new();
    let mut plan: Vec<P> = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (i, c) in def.schema.columns.iter().enumerate() {
                    cols.push(c.clone());
                    plan.push(P::Col(i));
                }
            }
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                let alias = match item {
                    SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                    _ => None,
                };
                if hybrid_call(e).is_some() {
                    cols.push(text_col_def(
                        &alias.unwrap_or_else(|| "score".into()),
                        ColumnType::Float,
                    ));
                    plan.push(P::Score);
                } else if let Some(ci) = ident_name(e).and_then(|n| col_of(def, n)) {
                    let mut c = def.schema.columns[ci].clone();
                    if let Some(a) = alias {
                        c.name = a;
                    }
                    cols.push(c);
                    plan.push(P::Col(ci));
                } else {
                    cols.push(text_col_def(
                        &alias.unwrap_or_else(|| e.to_string()),
                        ColumnType::Text,
                    ));
                    plan.push(P::Expr(e));
                }
            }
            _ => {
                return Err(Error::Unsupported(
                    "unsupported HYBRID projection item".into(),
                ))
            }
        }
    }
    let mut out_rows = Vec::with_capacity(results.len());
    for (row, score) in &results {
        let mut orow = Vec::with_capacity(plan.len());
        for p in &plan {
            orow.push(match p {
                P::Col(i) => row.get(*i).cloned().unwrap_or(Value::Null),
                P::Score => Value::Float(*score),
                P::Expr(e) => predicate::eval_row(e, &def.schema, row)?,
            });
        }
        out_rows.push(orow);
    }
    Ok(QueryResult::Rows(RowStream::literal(
        Schema::new(cols),
        out_rows,
    )))
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
    // Bare COUNT(*) over the whole table (no filter): count keys in the data
    // keyspace without decoding any rows, in parallel over clustered ranges,
    // and seed the result directly instead of feeding N rows.
    if filter.is_none() && plan.is_count_star_only() && !db.in_txn() {
        let prefix = data_prefix(&def.name);
        let raw = db.raw_db();
        let n = match pk_split_ranges(&raw, def, &prefix, agg_workers()).await? {
            Some(ranges) => {
                // One snapshot for all workers: a parallel COUNT(*) can't
                // double-count or miss rows that concurrent commits move across
                // range boundaries.
                let snap = raw.snapshot()?;
                let mut handles = Vec::with_capacity(ranges.len());
                for (start, end) in ranges {
                    let snap = snap.clone();
                    // Each worker observes the deadline itself: aborting only the
                    // awaiting task would leave these threads burning a core each.
                    let mut check = db.cancel_check();
                    handles.push(tokio::task::spawn_blocking(move || -> Result<u64> {
                        // May start after the deadline (the pool queues workers):
                        // check before touching any data.
                        check.tick_now()?;
                        let mut acc = 0u64;
                        snap.scan_range_each(&start, &end, |_k, _v| {
                            check.tick()?;
                            acc += 1;
                            Ok(())
                        })?;
                        Ok(acc)
                    }));
                }
                let mut total = 0u64;
                for h in handles {
                    total += h
                        .await
                        .map_err(|e| Error::Analytics(format!("count worker failed: {e}")))??;
                }
                total
            }
            None => raw.count_prefix(prefix).await?,
        };
        let mut agg = plan.new_aggregator();
        agg.seed_count_star(n);
        return Ok(agg);
    }
    if let Some(f) = &filter {
        // Covering-index COUNT: a bare COUNT(*) whose entire filter is an
        // equality fully covered by a PK/secondary index is answered by
        // counting index entries -- no row fetch, no decode.
        if plan.is_count_star_only() {
            if let Some(n) = index_count_eq(db, def, f).await? {
                let mut agg = plan.new_aggregator();
                let empty: Vec<Value> = Vec::new();
                for _ in 0..n {
                    agg.feed(&empty);
                }
                return Ok(agg);
            }
        }
        // Equality or range on a PK/indexed column: aggregate just the matching
        // rows fetched via the index, rather than scanning the whole table.
        if accelerable(def, Some(f))? {
            // `None` means the filter is a secondary-index range covering too much
            // of the table to be worth the index; fall through to the scan below
            // rather than materialising every row just to aggregate it.
            if let Some(rows) = collect_matches_narrow(db, def, Some(f)).await? {
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
    }
    // Autocommit full scans decode directly from borrowed storage bytes in a
    // single read transaction (no per-row copy). Inside a transaction we must
    // merge the write overlay, so fall back to the batch-copy parallel path.
    if !db.in_txn() {
        return scan_aggregate_fast(db, def, filter, plan).await;
    }
    parallel_aggregate(db, def, filter, plan).await
}

/// Vectorised (columnar) scalar aggregation state for one worker. Rows are
/// extracted into per-column `f64` arrays, then aggregated with tight,
/// SIMD-friendly loops over the contiguous arrays instead of per-row `Value`
/// dispatch. Arrays are flushed into the running accumulators every FLUSH rows
/// to bound memory.
struct ColAgg {
    // static config (per agg slot)
    funcs: Vec<elyra_olap::AggFunc>,
    agg_slot: Vec<Option<usize>>, // column array index; None = COUNT(*)
    is_int: Vec<bool>,
    slot_of: Vec<i32>, // col -> array index or -1
    ncols: usize,
    // batch buffers, one per distinct column
    arrays: Vec<Vec<f64>>,
    batch_rows: u64,
    // running accumulators, one per agg
    count: Vec<i64>,
    sum: Vec<f64>,
    min: Vec<f64>,
    max: Vec<f64>,
    has: Vec<bool>,
}

const COLAGG_FLUSH: u64 = 1 << 20;

impl ColAgg {
    fn new(specs: &[(elyra_olap::AggFunc, Option<usize>, bool)], ncols: usize) -> Self {
        let mut dcols: Vec<usize> = specs.iter().filter_map(|(_, c, _)| *c).collect();
        dcols.sort_unstable();
        dcols.dedup();
        let mut slot_of = vec![-1i32; ncols];
        for (i, &c) in dcols.iter().enumerate() {
            slot_of[c] = i as i32;
        }
        let n = specs.len();
        ColAgg {
            funcs: specs.iter().map(|s| s.0).collect(),
            agg_slot: specs
                .iter()
                .map(|s| s.1.map(|c| slot_of[c] as usize))
                .collect(),
            is_int: specs.iter().map(|s| s.2).collect(),
            slot_of,
            ncols,
            arrays: vec![Vec::new(); dcols.len()],
            batch_rows: 0,
            count: vec![0; n],
            sum: vec![0.0; n],
            min: vec![f64::INFINITY; n],
            max: vec![f64::NEG_INFINITY; n],
            has: vec![false; n],
        }
    }

    fn feed(&mut self, v: &[u8]) -> Result<()> {
        rowdec::extract_numeric_cols(v, self.ncols, &self.slot_of, &mut self.arrays)?;
        self.batch_rows += 1;
        if self.batch_rows >= COLAGG_FLUSH {
            self.flush();
        }
        Ok(())
    }

    fn flush(&mut self) {
        use elyra_olap::AggFunc::*;
        for a in 0..self.funcs.len() {
            match self.funcs[a] {
                CountStar => self.count[a] += self.batch_rows as i64,
                Count => self.count[a] += self.arrays[self.agg_slot[a].unwrap()].len() as i64,
                Sum | Avg => {
                    let arr = &self.arrays[self.agg_slot[a].unwrap()];
                    self.count[a] += arr.len() as i64;
                    self.sum[a] += arr.iter().sum::<f64>();
                }
                Min => {
                    let arr = &self.arrays[self.agg_slot[a].unwrap()];
                    if !arr.is_empty() {
                        self.has[a] = true;
                        self.min[a] =
                            self.min[a].min(arr.iter().copied().fold(f64::INFINITY, f64::min));
                    }
                }
                Max => {
                    let arr = &self.arrays[self.agg_slot[a].unwrap()];
                    if !arr.is_empty() {
                        self.has[a] = true;
                        self.max[a] =
                            self.max[a].max(arr.iter().copied().fold(f64::NEG_INFINITY, f64::max));
                    }
                }
                _ => {}
            }
        }
        for arr in &mut self.arrays {
            arr.clear();
        }
        self.batch_rows = 0;
    }

    fn merge(&mut self, o: &ColAgg) {
        use elyra_olap::AggFunc::*;
        for a in 0..self.funcs.len() {
            self.count[a] += o.count[a];
            self.sum[a] += o.sum[a];
            if o.has[a] {
                self.has[a] = true;
                match self.funcs[a] {
                    Min => self.min[a] = self.min[a].min(o.min[a]),
                    Max => self.max[a] = self.max[a].max(o.max[a]),
                    _ => {}
                }
            }
        }
    }

    fn finish(&self) -> Vec<Value> {
        use elyra_olap::AggFunc::*;
        (0..self.funcs.len())
            .map(|a| match self.funcs[a] {
                CountStar | Count => Value::Int(self.count[a]),
                Sum => {
                    if self.count[a] == 0 {
                        Value::Null
                    } else if self.is_int[a] && self.sum[a].fract() == 0.0 {
                        Value::Int(self.sum[a] as i64)
                    } else {
                        Value::Float(self.sum[a])
                    }
                }
                Avg => {
                    if self.count[a] == 0 {
                        Value::Null
                    } else {
                        Value::Float(self.sum[a] / self.count[a] as f64)
                    }
                }
                Min => {
                    if !self.has[a] {
                        Value::Null
                    } else if self.is_int[a] {
                        Value::Int(self.min[a] as i64)
                    } else {
                        Value::Float(self.min[a])
                    }
                }
                Max => {
                    if !self.has[a] {
                        Value::Null
                    } else if self.is_int[a] {
                        Value::Int(self.max[a] as i64)
                    } else {
                        Value::Float(self.max[a])
                    }
                }
                _ => Value::Null,
            })
            .collect()
    }
}

/// Run vectorised scalar aggregation (no GROUP BY, no filter) over parallel
/// clustered ranges and return one `Value` per aggregate slot.
async fn scan_columnar_scalar(
    db: &Session,
    def: &TableDef,
    specs: &[(elyra_olap::AggFunc, Option<usize>, bool)],
) -> Result<Vec<Value>> {
    let ncols = def.schema.columns.len();
    let prefix = data_prefix(&def.name);
    let raw = db.raw_db();
    let workers = agg_workers();
    if workers > 1 {
        if let Some(ranges) = pk_split_ranges(&raw, def, &prefix, workers).await? {
            let snap = raw.snapshot()?; // one consistent view for all workers
            let mut handles = Vec::with_capacity(ranges.len());
            for (start, end) in ranges {
                let snap = snap.clone();
                let specs = specs.to_vec();
                let mut check = db.cancel_check();
                handles.push(tokio::task::spawn_blocking(move || -> Result<_> {
                    check.tick_now()?;
                    let mut st = ColAgg::new(&specs, ncols);
                    snap.scan_range_each(&start, &end, |_k, v| {
                        check.tick()?;
                        st.feed(v)
                    })?;
                    Ok(st)
                }));
            }
            let mut result = ColAgg::new(specs, ncols);
            for h in handles {
                let mut part = h
                    .await
                    .map_err(|e| Error::Analytics(format!("columnar-agg worker failed: {e}")))??;
                part.flush();
                result.merge(&part);
            }
            return Ok(result.finish());
        }
    }
    let st = ColAgg::new(specs, ncols);
    // Runs on a blocking thread, which a wall-clock timeout cannot interrupt, so
    // the deadline has to be observed from inside the scan itself.
    let mut check = db.cancel_check();
    let mut st = raw
        .scan_fold(prefix, st, move |st, _k, v| {
            check.tick()?;
            st.feed(v)
        })
        .await?;
    st.flush();
    Ok(st.finish())
}

type FxU64Map =
    std::collections::HashMap<u64, u32, std::hash::BuildHasherDefault<elyra_olap::FxHasher>>;

/// Vectorised (columnar) *grouped* aggregation state for one worker (OLAP phase
/// 3). One numeric GROUP BY column, numeric aggregates. Only the needed columns
/// are decoded; the group key is kept exactly (integer value or canonical float
/// bits), and per-group accumulators live in flat `f64`/`i64` arrays indexed by
/// `group_ordinal * naggs + slot`, avoiding the byte-key encoding and per-row
/// `Value` dispatch of the general grouping path.
struct ColGroup {
    group_col: usize,
    // static agg config (per slot)
    funcs: Vec<elyra_olap::AggFunc>,
    agg_arg: Vec<Option<usize>>, // base column read by this agg; None = COUNT(*)
    is_int: Vec<bool>,
    naggs: usize,
    // decode
    ncols: usize,
    needed: Vec<bool>,
    buf: Vec<Value>,
    // optional pushed-down compiled filter
    cfilter: Option<cpred::CompiledPredicate>,
    // grouping: canonical key bits -> group ordinal, plus a dedicated NULL group
    index: FxU64Map,
    null_gid: u32,       // u32::MAX until a NULL-keyed row is seen
    keyvals: Vec<Value>, // group ordinal -> representative group-column value
    // flat accumulators, naggs per group
    count: Vec<i64>,
    sum: Vec<f64>,
    min: Vec<f64>,
    max: Vec<f64>,
    has: Vec<bool>,
    // distinct-group cap (bounds memory; on overflow the caller re-runs spilling)
    max_groups: usize,
    overflow: bool,
}

const NO_GID: u32 = u32::MAX;

impl ColGroup {
    fn new(
        group_col: usize,
        specs: &[(elyra_olap::AggFunc, Option<usize>, bool)],
        ncols: usize,
        needed: Vec<bool>,
        cfilter: Option<cpred::CompiledPredicate>,
    ) -> Self {
        let n = specs.len();
        ColGroup {
            group_col,
            funcs: specs.iter().map(|s| s.0).collect(),
            agg_arg: specs.iter().map(|s| s.1).collect(),
            is_int: specs.iter().map(|s| s.2).collect(),
            naggs: n,
            ncols,
            needed,
            buf: Vec::with_capacity(ncols),
            cfilter,
            index: FxU64Map::default(),
            null_gid: NO_GID,
            keyvals: Vec::new(),
            count: Vec::new(),
            sum: Vec::new(),
            min: Vec::new(),
            max: Vec::new(),
            has: Vec::new(),
            max_groups: elyra_olap::default_max_groups(),
            overflow: false,
        }
    }

    /// Allocate accumulator slots for a new group and return its ordinal, or
    /// `None` if the group cap is reached (sets the overflow flag).
    fn new_group(&mut self, keyval: Value) -> Option<u32> {
        if self.max_groups > 0 && self.keyvals.len() >= self.max_groups {
            self.overflow = true;
            return None;
        }
        let gid = self.keyvals.len() as u32;
        self.keyvals.push(keyval);
        self.count.resize(self.count.len() + self.naggs, 0);
        self.sum.resize(self.sum.len() + self.naggs, 0.0);
        self.min.resize(self.min.len() + self.naggs, f64::INFINITY);
        self.max
            .resize(self.max.len() + self.naggs, f64::NEG_INFINITY);
        self.has.resize(self.has.len() + self.naggs, false);
        Some(gid)
    }

    fn feed(&mut self, v: &[u8]) -> Result<()> {
        rowdec::decode_projected_into(v, self.ncols, &self.needed, &mut self.buf)?;
        if let Some(cp) = &self.cfilter {
            if !cp.matches(&self.buf) {
                return Ok(());
            }
        }
        // Resolve the group ordinal from the (exactly-keyed) group column.
        let gid = match self.buf.get(self.group_col) {
            Some(Value::Null) | None => {
                if self.null_gid == NO_GID {
                    match self.new_group(Value::Null) {
                        Some(g) => self.null_gid = g,
                        None => return Ok(()),
                    }
                }
                self.null_gid
            }
            Some(v) => {
                let (bits, keyval) = match v {
                    Value::Int(i) => (*i as u64, Value::Int(*i)),
                    Value::Float(f) => (elyra_core::canonical_f64_bits(*f), Value::Float(*f)),
                    // Typed Int/Float column: other variants do not occur.
                    other => (
                        elyra_core::canonical_f64_bits(other.as_f64().unwrap_or(f64::NAN)),
                        other.clone(),
                    ),
                };
                match self.index.get(&bits) {
                    Some(&g) => g,
                    None => match self.new_group(keyval) {
                        Some(g) => {
                            self.index.insert(bits, g);
                            g
                        }
                        None => return Ok(()),
                    },
                }
            }
        };
        let base = gid as usize * self.naggs;
        for a in 0..self.naggs {
            match self.funcs[a] {
                elyra_olap::AggFunc::CountStar => self.count[base + a] += 1,
                _ => {
                    let n = self.agg_arg[a]
                        .and_then(|c| self.buf.get(c))
                        .and_then(|v| v.as_f64());
                    if let Some(n) = n {
                        use elyra_olap::AggFunc::*;
                        match self.funcs[a] {
                            Count => self.count[base + a] += 1,
                            Sum | Avg => {
                                self.sum[base + a] += n;
                                self.count[base + a] += 1;
                            }
                            Min => {
                                self.has[base + a] = true;
                                if n < self.min[base + a] {
                                    self.min[base + a] = n;
                                }
                            }
                            Max => {
                                self.has[base + a] = true;
                                if n > self.max[base + a] {
                                    self.max[base + a] = n;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, o: ColGroup) {
        self.overflow |= o.overflow;
        let ColGroup {
            index,
            null_gid,
            keyvals,
            count,
            sum,
            min,
            max,
            has,
            naggs,
            ..
        } = o;
        let merge_slots = |me: &mut ColGroup, dst: u32, src: u32| {
            let (db, sb) = (dst as usize * naggs, src as usize * naggs);
            for a in 0..naggs {
                me.count[db + a] += count[sb + a];
                me.sum[db + a] += sum[sb + a];
                if has[sb + a] {
                    me.has[db + a] = true;
                    if min[sb + a] < me.min[db + a] {
                        me.min[db + a] = min[sb + a];
                    }
                    if max[sb + a] > me.max[db + a] {
                        me.max[db + a] = max[sb + a];
                    }
                }
            }
        };
        if null_gid != NO_GID {
            if self.null_gid == NO_GID {
                match self.new_group(Value::Null) {
                    Some(g) => self.null_gid = g,
                    None => return,
                }
            }
            let dst = self.null_gid;
            merge_slots(self, dst, null_gid);
        }
        for (bits, src) in index {
            let dst = match self.index.get(&bits) {
                Some(&g) => g,
                None => match self.new_group(keyvals[src as usize].clone()) {
                    Some(g) => {
                        self.index.insert(bits, g);
                        g
                    }
                    None => continue,
                },
            };
            merge_slots(self, dst, src);
        }
    }

    /// Finalise into `(group sample row, aggregate results)` tuples. The sample
    /// carries the group-column value at its own position so the normal
    /// projection can read it.
    fn into_groups(self, base_len: usize) -> Vec<(Vec<Value>, Vec<Value>)> {
        use elyra_olap::AggFunc::*;
        let ngroups = self.keyvals.len();
        let mut out = Vec::with_capacity(ngroups);
        for gid in 0..ngroups {
            let base = gid * self.naggs;
            let results: Vec<Value> = (0..self.naggs)
                .map(|a| {
                    let (c, s) = (self.count[base + a], self.sum[base + a]);
                    match self.funcs[a] {
                        CountStar | Count => Value::Int(c),
                        Sum => {
                            if c == 0 {
                                Value::Null
                            } else if self.is_int[a] && s.fract() == 0.0 {
                                Value::Int(s as i64)
                            } else {
                                Value::Float(s)
                            }
                        }
                        Avg => {
                            if c == 0 {
                                Value::Null
                            } else {
                                Value::Float(s / c as f64)
                            }
                        }
                        Min => {
                            if !self.has[base + a] {
                                Value::Null
                            } else if self.is_int[a] {
                                Value::Int(self.min[base + a] as i64)
                            } else {
                                Value::Float(self.min[base + a])
                            }
                        }
                        Max => {
                            if !self.has[base + a] {
                                Value::Null
                            } else if self.is_int[a] {
                                Value::Int(self.max[base + a] as i64)
                            } else {
                                Value::Float(self.max[base + a])
                            }
                        }
                        _ => Value::Null,
                    }
                })
                .collect();
            let mut sample = vec![Value::Null; base_len];
            if self.group_col < base_len {
                sample[self.group_col] = self.keyvals[gid].clone();
            }
            out.push((sample, results));
        }
        out
    }
}

/// Run vectorised grouped aggregation over parallel clustered ranges. Returns
/// `None` if the distinct-group cap was exceeded (caller falls back to the
/// spilling path).
#[allow(clippy::too_many_arguments)]
async fn scan_columnar_group(
    db: &Session,
    def: &TableDef,
    group_col: usize,
    specs: &[(elyra_olap::AggFunc, Option<usize>, bool)],
    cfilter: Option<cpred::CompiledPredicate>,
    needed: Vec<bool>,
    base_len: usize,
    explicit_ranges: Option<Vec<(Vec<u8>, Vec<u8>)>>,
) -> Result<Option<Vec<(Vec<Value>, Vec<Value>)>>> {
    let ncols = def.schema.columns.len();
    let prefix = data_prefix(&def.name);
    let raw = db.raw_db();
    let workers = agg_workers();
    // Work units: explicit (zone-map surviving) ranges if given, otherwise the
    // clustered PK split for parallelism, otherwise a single full-prefix scan.
    let ranges: Option<Vec<(Vec<u8>, Vec<u8>)>> = match explicit_ranges {
        Some(rs) => Some(rs),
        None if workers > 1 => pk_split_ranges(&raw, def, &prefix, workers).await?,
        None => None,
    };
    let result = match ranges {
        Some(rs) => {
            let snap = raw.snapshot()?; // one consistent view for all workers
            let mut handles = Vec::with_capacity(rs.len());
            for (start, end) in rs {
                let snap = snap.clone();
                let specs = specs.to_vec();
                let needed = needed.clone();
                let cf = cfilter.clone();
                let mut check = db.cancel_check();
                handles.push(tokio::task::spawn_blocking(move || -> Result<_> {
                    check.tick_now()?;
                    let mut st = ColGroup::new(group_col, &specs, ncols, needed, cf);
                    snap.scan_range_each(&start, &end, |_k, v| {
                        check.tick()?;
                        st.feed(v)
                    })?;
                    Ok(st)
                }));
            }
            let mut result =
                ColGroup::new(group_col, specs, ncols, needed.clone(), cfilter.clone());
            for h in handles {
                let part = h.await.map_err(|e| {
                    Error::Analytics(format!("columnar-group worker failed: {e}"))
                })??;
                result.merge(part);
            }
            result
        }
        None => {
            let st = ColGroup::new(group_col, specs, ncols, needed.clone(), cfilter.clone());
            let mut check = db.cancel_check();
            raw.scan_fold(prefix, st, move |st, _k, v| {
                check.tick()?;
                st.feed(v)
            })
            .await?
        }
    };
    if result.overflow {
        return Ok(None);
    }
    Ok(Some(result.into_groups(base_len)))
}

/// Get a table's zone map at `epoch`, building it from one consistent snapshot
/// if absent. Returns `None` if a write committed during the build (so its
/// statistics can't be trusted for skipping this time).
async fn get_or_build_zonemap(
    db: &Session,
    def: &TableDef,
    epoch: u64,
) -> Result<Option<std::sync::Arc<zonemap::ZoneMap>>> {
    if let Some(zm) = zonemap::get(&def.name, epoch) {
        return Ok(Some(zm));
    }
    let raw = db.raw_db();
    let prefix = data_prefix(&def.name);
    let upper = prefix_successor(&prefix);
    let mut check = db.cancel_check();
    let b = raw
        .scan_fold(
            prefix,
            zonemap::Builder::new(&def.schema),
            move |b, k, v| {
                check.tick()?;
                b.feed(k, v)
            },
        )
        .await?;
    if raw.write_epoch()? != epoch {
        return Ok(None);
    }
    let zm = std::sync::Arc::new(b.finish(epoch, upper));
    zonemap::store(&def.name, zm.clone());
    Ok(Some(zm))
}

/// Zone-map-aware wrapper over [`scan_columnar_group`]: when zone maps are
/// enabled and the filter has numeric bounds, skip chunks that cannot match,
/// then re-validate that no write raced the skipping scan (else recompute in
/// full). Correctness never depends on the zone map -- only which rows are read.
async fn scan_columnar_group_zm(
    db: &Session,
    def: &TableDef,
    group_col: usize,
    specs: &[(elyra_olap::AggFunc, Option<usize>, bool)],
    cfilter: Option<cpred::CompiledPredicate>,
    needed: Vec<bool>,
    base_len: usize,
) -> Result<Option<Vec<(Vec<Value>, Vec<Value>)>>> {
    if zonemap::enabled() && !db.in_txn() {
        if let Some(cf) = &cfilter {
            let bounds = cf.bounds();
            if !bounds.is_empty() {
                let epoch = db.raw_db().write_epoch()?;
                if let Some(zm) = get_or_build_zonemap(db, def, epoch).await? {
                    let ranges = zm.surviving_ranges(&bounds);
                    let res = scan_columnar_group(
                        db,
                        def,
                        group_col,
                        specs,
                        cfilter.clone(),
                        needed.clone(),
                        base_len,
                        Some(ranges),
                    )
                    .await?;
                    // If nothing committed during the skipping scan, the skip was
                    // valid; otherwise fall through to a full, unskipped scan.
                    if db.raw_db().write_epoch()? == epoch {
                        return Ok(res);
                    }
                }
            }
        }
    }
    scan_columnar_group(db, def, group_col, specs, cfilter, needed, base_len, None).await
}

/// Build a columnar cache entry for `def` at epoch `e0` from a single consistent
/// snapshot, or `None` if the table's blobs exceed the cache budget or a write
/// committed during the build (so it must not be cached).
async fn build_cached_table(
    db: &Session,
    def: &TableDef,
    e0: u64,
) -> Result<Option<colcache::CachedTable>> {
    let budget = colcache::budget_bytes();
    let prefix = data_prefix(&def.name);
    let raw = db.raw_db();
    struct Acc {
        blobs: Vec<Vec<u8>>,
        bytes: usize,
        over: bool,
    }
    let acc = raw
        .scan_fold_until(
            prefix,
            Acc {
                blobs: Vec::new(),
                bytes: 0,
                over: false,
            },
            move |a, _k, v| {
                a.bytes += v.len();
                if a.bytes > budget {
                    a.over = true;
                    return Ok(false);
                }
                a.blobs.push(v.to_vec());
                Ok(true)
            },
        )
        .await?;
    if acc.over {
        return Ok(None);
    }
    let ct = colcache::build(&def.schema, e0, &acc.blobs)?;
    if ct.bytes > budget {
        return Ok(None);
    }
    // The scan was one snapshot; if the write sequence is unchanged across the
    // whole build, that snapshot is exactly epoch e0 and safe to cache.
    if raw.write_epoch()? != e0 {
        return Ok(None);
    }
    Ok(Some(ct))
}

/// Scalar aggregation via the columnar cache (build-on-miss). Falls back to the
/// scan path when the table is too large to cache.
async fn columnar_cached_scalar(
    db: &Session,
    def: &TableDef,
    specs: &[(elyra_olap::AggFunc, Option<usize>, bool)],
) -> Result<Vec<Value>> {
    let epoch = db.raw_db().write_epoch()?;
    if let Some(ct) = colcache::get(&def.name, epoch) {
        return Ok(colcache::scalar_agg(&ct, specs));
    }
    match build_cached_table(db, def, epoch).await? {
        Some(ct) => {
            let ct = std::sync::Arc::new(ct);
            colcache::store(&def.name, ct.clone());
            Ok(colcache::scalar_agg(&ct, specs))
        }
        None => scan_columnar_scalar(db, def, specs).await,
    }
}

/// Grouped aggregation via the columnar cache (build-on-miss). Returns `None`
/// when the cache can't serve it (table too big, or the distinct-group cap is
/// exceeded), so the caller uses the scan/spill path.
async fn columnar_cached_group(
    db: &Session,
    def: &TableDef,
    group_col: usize,
    specs: &[(elyra_olap::AggFunc, Option<usize>, bool)],
    base_len: usize,
) -> Result<Option<Vec<(Vec<Value>, Vec<Value>)>>> {
    let epoch = db.raw_db().write_epoch()?;
    if let Some(ct) = colcache::get(&def.name, epoch) {
        return Ok(colcache::group_agg(&ct, group_col, specs, base_len));
    }
    match build_cached_table(db, def, epoch).await? {
        Some(ct) => {
            let ct = std::sync::Arc::new(ct);
            colcache::store(&def.name, ct.clone());
            Ok(colcache::group_agg(&ct, group_col, specs, base_len))
        }
        None => Ok(None),
    }
}

/// Degree of parallelism for full-scan aggregation: `ELYRASQL_AGG_WORKERS` if
/// set (clamped to 1..=64), else min(available cores, 8).
fn agg_workers() -> usize {
    use std::sync::OnceLock;
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        if let Some(v) = std::env::var("ELYRASQL_AGG_WORKERS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            return v.clamp(1, 64);
        }
        // Full-scan aggregation is largely memory-bandwidth bound: ~4 parallel
        // readers saturate bandwidth, and beyond that the coordination and
        // read-transaction overhead makes it slower (measured). Cap the default
        // at 4 regardless of core count; operators can raise it explicitly.
        std::thread::available_parallelism()
            .map(|n| n.get().min(4))
            .unwrap_or(4)
    })
}

/// Zero-copy scan + filter + aggregate for the autocommit case. When the table
/// has a single integer primary key, the clustered keyspace is split into N
/// sub-ranges aggregated in parallel (each in its own read transaction),
/// otherwise a single-pass scan is used. Every worker decodes only the needed
/// columns straight from borrowed bytes, reusing one row buffer.
async fn scan_aggregate_fast(
    db: &Session,
    def: &TableDef,
    filter: Option<Expr>,
    plan: &AggPlan,
) -> Result<GroupAggregator> {
    let prefix = data_prefix(&def.name);
    let schema = def.schema.clone();
    let needed = agg_needed_mask(&schema, filter.as_ref(), plan);
    let ncols = schema.columns.len();
    let arg_exprs = plan.arg_exprs().to_vec();
    let raw = db.raw_db();
    // Compile the filter once (pre-resolved column indices, native comparison)
    // for the common numeric-conjunction shape; fall back to the interpreter.
    let cfilter = filter.as_ref().and_then(|f| cpred::compile(f, &schema));

    // A closure factory: builds the per-worker fold body (each captures its own
    // aggregator + reusable buffer).
    let make_body = |filter: Option<Expr>,
                     cfilter: Option<cpred::CompiledPredicate>,
                     needed: Option<Vec<bool>>,
                     schema: Schema,
                     arg_exprs: Vec<Expr>| {
        let mut buf: Vec<Value> = Vec::with_capacity(ncols);
        move |agg: &mut GroupAggregator, _k: &[u8], v: &[u8]| -> Result<()> {
            match &needed {
                Some(m) => rowdec::decode_projected_into(v, ncols, m, &mut buf)?,
                None => buf = bincode::deserialize(v).map_err(|e| Error::Storage(e.to_string()))?,
            }
            let keep = match (&cfilter, &filter) {
                (Some(cp), _) => cp.matches(&buf),
                (None, Some(e)) => predicate::matches(e, &schema, &buf)?,
                (None, None) => true,
            };
            if keep {
                if arg_exprs.is_empty() {
                    agg.feed(&buf);
                } else {
                    let mut r = buf.clone();
                    for e in &arg_exprs {
                        r.push(predicate::eval_row(e, &schema, &buf)?);
                    }
                    agg.feed(&r);
                }
            }
            Ok(())
        }
    };

    // Parallel split for single integer-PK tables: each worker aggregates a
    // clustered sub-range and the partials merge. The group aggregator reuses
    // its key buffer, so grouped aggregation no longer thrashes the allocator
    // across threads. `ELYRASQL_AGG_WORKERS` overrides the degree of parallelism
    // (0/1 = single-threaded); default is min(cores, 8).
    // A DISTINCT aggregate whose value merges additively (SUM/AVG/GROUP_CONCAT) must
    // NOT be split across workers: a value seen by two workers would be added
    // twice. COUNT(DISTINCT) is safe because merging unions the distinct set and
    // the result is that union.s size, so it keeps its parallelism.

    let workers = if plan.has_unmergeable_distinct() {
        1
    } else {
        agg_workers()
    };
    if workers > 1 {
        if let Some(ranges) = pk_split_ranges(&raw, def, &prefix, workers).await? {
            // One snapshot shared by every worker: the parallel range scans then
            // observe a single consistent point-in-time view (concurrent commits
            // are all-or-nothing across the whole aggregate).
            let snap = raw.snapshot()?;
            let mut handles = Vec::with_capacity(ranges.len());
            for (start, end) in ranges {
                let snap = snap.clone();
                let mut body = make_body(
                    filter.clone(),
                    cfilter.clone(),
                    needed.clone(),
                    schema.clone(),
                    arg_exprs.clone(),
                );
                let mut agg0 = plan.new_aggregator();
                let mut check = db.cancel_check();
                handles.push(tokio::task::spawn_blocking(move || -> Result<_> {
                    check.tick_now()?;
                    snap.scan_range_each(&start, &end, |k, v| {
                        check.tick()?;
                        body(&mut agg0, k, v)
                    })?;
                    Ok(agg0)
                }));
            }
            let mut result = plan.new_aggregator();
            for h in handles {
                let part = h
                    .await
                    .map_err(|e| Error::Analytics(format!("scan worker failed: {e}")))??;
                result.merge(part);
            }
            return Ok(result);
        }
    }

    // Fallback: single-pass full-prefix scan.
    let mut body = make_body(filter, cfilter, needed, schema, arg_exprs);
    let mut check = db.cancel_check();
    raw.scan_fold(prefix, plan.new_aggregator(), move |acc, k, v| {
        check.tick()?;
        body(acc, k, v)
    })
    .await
}

/// Split the clustered keyspace of a single-integer-PK table into up to `n`
/// contiguous `[start, end)` key ranges of roughly equal PK span, for parallel
/// scanning. Returns `None` (caller does a single-pass scan) unless the table
/// has exactly one BIGINT/INT primary-key column with a usable value spread.
async fn pk_split_ranges(
    raw: &elyra_storage::Db,
    def: &TableDef,
    prefix: &[u8],
    n: usize,
) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
    if def.pk_cols.len() != 1 {
        return Ok(None);
    }
    let ci = def.pk_cols[0];
    if !matches!(def.schema.columns[ci].ty, elyra_core::ColumnType::Int) {
        return Ok(None);
    }
    let Some((first, last)) = raw.prefix_bounds(prefix.to_vec()).await? else {
        return Ok(None);
    };
    let plen = prefix.len();
    // Decode the 8-byte order-preserving integer key that follows the prefix.
    let decode = |key: &[u8]| -> Option<i64> {
        let b = key.get(plen..plen + 8)?;
        let u = u64::from_be_bytes(b.try_into().ok()?);
        Some((u ^ 0x8000_0000_0000_0000) as i64)
    };
    let (Some(lo), Some(hi)) = (decode(&first), decode(&last)) else {
        return Ok(None);
    };
    // Need a spread wide enough to bother splitting.
    if hi <= lo || (hi as i128 - lo as i128) < n as i128 {
        return Ok(None);
    }
    let key_of = |pk: i64| -> Vec<u8> {
        let mut k = prefix.to_vec();
        k.extend_from_slice(&((pk as u64) ^ 0x8000_0000_0000_0000).to_be_bytes());
        k
    };
    let span = hi as i128 - lo as i128;
    let mut ranges = Vec::with_capacity(n);
    let upper = prefix_successor(prefix); // exclusive end past the last row
    for i in 0..n {
        let start = if i == 0 {
            first.clone()
        } else {
            key_of((lo as i128 + span * i as i128 / n as i128) as i64)
        };
        let end = if i == n - 1 {
            upper.clone()
        } else {
            key_of((lo as i128 + span * (i as i128 + 1) / n as i128) as i64)
        };
        if start < end {
            ranges.push((start, end));
        }
    }
    if ranges.len() < 2 {
        return Ok(None);
    }
    Ok(Some(ranges))
}

/// Smallest key strictly greater than every key with the given prefix.
fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut u = prefix.to_vec();
    while let Some(b) = u.last_mut() {
        if *b < 0xff {
            *b += 1;
            return u;
        }
        u.pop();
    }
    // All-0xFF prefix: use an unbounded-ish sentinel (won't happen for our
    // namespaced table prefixes).
    vec![0xff; prefix.len() + 1]
}

/// Estimate the number of distinct GROUP BY groups from column statistics
/// (product of per-column NDV). `None` = unknown (not analyzed / a column
/// without stats), in which case the caller uses the in-memory path with an
/// overflow fallback. A capped NDV is treated as "very large".
async fn estimate_group_count(
    db: &Session,
    def: &TableDef,
    group_cols: &[usize],
) -> Result<Option<u64>> {
    if group_cols.is_empty() {
        return Ok(Some(1));
    }
    let Some(stats) = catalog::load_stats(db, &def.name).await? else {
        return Ok(None);
    };
    let mut prod = 1u64;
    for &ci in group_cols {
        let Some(name) = def.schema.columns.get(ci).map(|c| c.name.as_str()) else {
            return Ok(None);
        };
        let Some(cs) = stats
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
        else {
            return Ok(None);
        };
        if cs.ndv_capped {
            return Ok(Some(u64::MAX));
        }
        prod = prod.saturating_mul(cs.ndv.max(1));
    }
    Ok(Some(prod))
}

/// Partitioned, spill-to-disk aggregation used when the in-memory aggregation
/// overflows the group cap. Rows are routed to partitions by group-key hash and
/// spilled to temp files; each partition is then aggregated independently in
/// bounded memory. Returns finalized output rows.
/// Reusable resident-plus-spill group aggregator, shared by the base-table
/// scan path and the streaming join path. It aggregates the first `max_groups`
/// distinct groups fully in memory; every later row for a *new* group is routed
/// to one of `SPILL_PARTS` disk partitions. Because a group is either resident
/// (all its rows aggregated in memory) or absent (all its rows spilled), the
/// resident and spilled group sets are disjoint, so their results concatenate
/// without a cross-merge. Memory is bounded by the group cap plus partition
/// buffers, independent of input size.
struct SpillAgg<'p> {
    plan: &'p AggPlan,
    resident: GroupAggregator,
    parts: crate::aggspill::Partitions,
    group_cols: Vec<usize>,
}

const SPILL_PARTS: usize = 256;

impl<'p> SpillAgg<'p> {
    fn new(plan: &'p AggPlan) -> Self {
        SpillAgg {
            resident: plan.new_aggregator(),
            parts: crate::aggspill::Partitions::new(SPILL_PARTS, crate::sort::sort_max_rows()),
            group_cols: plan.group_cols().to_vec(),
            plan,
        }
    }

    /// Feed a row that has already had `extend_row` applied (when the plan needs
    /// argument expressions). Resident groups aggregate in memory; overflow-group
    /// rows spill.
    fn feed_extended(&mut self, fed: &[Value]) -> Result<()> {
        if !self.resident.try_feed(fed) {
            let gk: Vec<Value> = self
                .group_cols
                .iter()
                .map(|&c| fed.get(c).cloned().unwrap_or(Value::Null))
                .collect();
            let p = crate::aggspill::partition_of(&Value::row_collation_key(&gk), SPILL_PARTS);
            // Only a spilled row needs to be owned.
            self.parts.route(p, fed.to_vec())?;
        }
        Ok(())
    }

    /// Finalise resident groups, then aggregate each spilled partition
    /// independently and concatenate (all group sets are disjoint).
    fn finalize(mut self) -> Result<(Schema, Vec<Vec<Value>>)> {
        let (schema, resident_rows) = self.plan.finalize(self.resident)?;
        let mut out_rows: Vec<Vec<Value>> = resident_rows;
        for p in 0..self.parts.len() {
            let mut agg = self.plan.new_aggregator();
            let mut any = false;
            self.parts.drain_each(p, |row| {
                // Rows were already filtered and extended before spilling.
                any = true;
                agg.feed(&row);
                Ok(())
            })?;
            // An empty partition contributes nothing. It must be skipped rather
            // than finalised: for an aggregate with no GROUP BY, finalising an
            // empty group set legitimately means "bare aggregate over zero rows"
            // and yields one row (`COUNT(*)` -> 0), so finalising all 256 empty
            // partitions appended 256 bogus zero rows after the real result.
            if !any {
                continue;
            }
            if agg.overflowed() {
                return Err(Error::Query(format!(
                    "GROUP BY partition still exceeds the group limit ({}); raise \
                     ELYRASQL_GROUP_MAX_GROUPS",
                    elyra_olap::default_max_groups()
                )));
            }
            let (_s, rows) = self.plan.finalize(agg)?;
            out_rows.extend(rows);
        }
        Ok((schema, out_rows))
    }
}

/// Batched cursor scan that reads from ONE consistent view for the whole
/// statement. In a transaction the session snapshot+overlay is already
/// consistent, so we defer to `db.scan_batch`; in autocommit `snap` pins one raw
/// snapshot up front so a long multi-batch scan can't tear across concurrent
/// commits.
async fn pinned_scan_batch(
    db: &Session,
    snap: &Option<elyra_storage::Snapshot>,
    prefix: &[u8],
    cursor: &Option<Vec<u8>>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    match snap {
        None => db.scan_batch(prefix.to_vec(), cursor.clone(), limit).await,
        Some(s) => {
            let start = match cursor {
                Some(a) => {
                    let mut k = a.clone();
                    k.push(0);
                    k
                }
                None => prefix.to_vec(),
            };
            let end = prefix_successor(prefix);
            let s = s.clone();
            tokio::task::spawn_blocking(move || s.scan_range(&start, Some(&end), limit))
                .await
                .map_err(|e| Error::Analytics(format!("scan task failed: {e}")))?
        }
    }
}

async fn partitioned_aggregate(
    db: &Session,
    def: &TableDef,
    filter: Option<Expr>,
    plan: &AggPlan,
) -> Result<(Schema, Vec<Vec<Value>>)> {
    // Single pass over the table, feeding the shared resident+spill aggregator.
    let extend = !plan.arg_exprs().is_empty();
    let mut sa = SpillAgg::new(plan);
    let mut fedbuf: Vec<Value> = Vec::new();
    let prefix = data_prefix(&def.name);
    // In autocommit, pin one snapshot so this multi-batch scan reads a single
    // consistent view (concurrent commits are all-or-nothing across the whole
    // aggregate). In a transaction the session snapshot+overlay is already
    // consistent, so defer to `scan_batch`.
    let snap = if db.in_txn() {
        None
    } else {
        Some(db.raw_db().snapshot()?)
    };
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let batch = pinned_scan_batch(db, &snap, &prefix, &cursor, 8192).await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 8192;
        cursor = batch.last().map(|(k, _)| k.clone());
        for (_, v) in batch {
            let row: Vec<Value> = rowdec::decode_row(&v)?;
            if let Some(f) = &filter {
                if !predicate::matches(f, &def.schema, &row)? {
                    continue;
                }
            }
            if extend {
                plan.extend_row_into(&row, &mut fedbuf)?;
                sa.feed_extended(&fedbuf)?;
            } else {
                sa.feed_extended(&row)?;
            }
        }
        if last {
            break;
        }
    }
    sa.finalize()
}

/// Scan the table in batches and aggregate them across worker threads, merging
/// partial aggregators. Memory is bounded by (workers x batch), independent of
/// table size — the core OLAP property.
/// If `e` is `col = literal` (either order), push `col`'s index and return
/// true; otherwise false. Used to prove a filter is a pure equality set.
fn is_col_eq_literal(e: &Expr, schema: &Schema, out: &mut Vec<usize>) -> bool {
    let Expr::BinaryOp { left, op, right } = e else {
        return false;
    };
    if !matches!(op, sqlparser::ast::BinaryOperator::Eq) {
        return false;
    }
    let ident = |x: &Expr| -> Option<usize> {
        let name = match x {
            Expr::Identifier(id) => id.value.clone(),
            Expr::CompoundIdentifier(parts) => parts.last()?.value.clone(),
            _ => return None,
        };
        schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&name))
    };
    let lit = |x: &Expr| matches!(x, Expr::Value(_));
    if let (Some(ci), true) = (ident(left), lit(right)) {
        out.push(ci);
        return true;
    }
    if let (true, Some(ci)) = (lit(left), ident(right)) {
        out.push(ci);
        return true;
    }
    false
}

/// Count matching rows for a filter that is *exactly* an equality set fully
/// covered by the primary key or a secondary index, without fetching rows.
/// Returns `None` when the filter isn't a clean covered equality (caller then
/// takes the normal path). Correctness rests on two facts: every conjunct is a
/// `col = literal`, and the equality columns are exactly an index's columns --
/// so the index entries are precisely the matching rows.
async fn index_count_eq(db: &Session, def: &TableDef, filter: &Expr) -> Result<Option<u64>> {
    let mut conj = Vec::new();
    split_and(filter, &mut conj);
    let mut refcols: Vec<usize> = Vec::new();
    for c in &conj {
        if !is_col_eq_literal(c, &def.schema, &mut refcols) {
            return Ok(None);
        }
    }
    if refcols.is_empty() {
        return Ok(None);
    }
    let same_set = |cols: &[usize]| {
        let mut a = refcols.clone();
        a.sort_unstable();
        a.dedup();
        let mut b = cols.to_vec();
        b.sort_unstable();
        b.dedup();
        a == b
    };
    if def.has_pk() && same_set(&def.pk_cols) {
        if let Some(vals) = key_eq_values(def, Some(filter), &def.pk_cols)? {
            let key = data_key(
                &def.name,
                &keyenc::encode_key_coll(&vals, &def.pk_collations())?,
            );
            return Ok(Some(u64::from(db.get(key).await?.is_some())));
        }
    }
    for idx in &def.indexes {
        if idx.vector {
            continue;
        }
        if same_set(&idx.cols) {
            if let Some(vals) = key_eq_values(def, Some(filter), &idx.cols)? {
                let keys = index::lookup_eq(db, &def.name, idx, &vals).await?;
                return Ok(Some(keys.len() as u64));
            }
        }
    }
    Ok(None)
}

/// Collect the schema column indices referenced by `e` into `out`. Returns
/// `false` if the expression contains any form we don't fully understand, in
/// which case the caller must conservatively assume *all* columns are needed.
fn collect_col_refs(e: &Expr, schema: &Schema, out: &mut Vec<usize>) -> bool {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    // Resolve exactly as `predicate::eval_row` does, or give up. Anything else
    // risks marking the wrong column: in a joined schema the columns are
    // qualified (`a.k`, `b.k`), so matching on the bare suffix alone would map
    // `b.k` onto `a.k` -- the mask would then skip the column the query reads
    // and it would decode as NULL, silently wrong.
    let find = |name: &str, out: &mut Vec<usize>| -> bool {
        match predicate::resolve_index(name, schema) {
            Ok(i) => {
                out.push(i);
                true
            }
            // Unknown or ambiguous (and niladic functions like
            // CURRENT_TIMESTAMP, which are not columns) -> decode everything.
            Err(_) => false,
        }
    };
    match e {
        Expr::Value(_) | Expr::TypedString { .. } => true,
        Expr::Identifier(id) => find(&id.value, out),
        Expr::CompoundIdentifier(parts) => {
            let qualified = parts
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            find(&qualified, out)
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_col_refs(left, schema, out) && collect_col_refs(right, schema, out)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::Cast { expr, .. } => collect_col_refs(expr, schema, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_col_refs(expr, schema, out)
                && collect_col_refs(low, schema, out)
                && collect_col_refs(high, schema, out)
        }
        Expr::InList { expr, list, .. } => {
            collect_col_refs(expr, schema, out)
                && list.iter().all(|x| collect_col_refs(x, schema, out))
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .as_ref()
                .map(|o| collect_col_refs(o, schema, out))
                .unwrap_or(true)
                && conditions.iter().all(|c| collect_col_refs(c, schema, out))
                && results.iter().all(|r| collect_col_refs(r, schema, out))
                && else_result
                    .as_ref()
                    .map(|er| collect_col_refs(er, schema, out))
                    .unwrap_or(true)
        }
        Expr::Function(f) => match &f.args {
            FunctionArguments::None => true,
            FunctionArguments::List(list) => list.args.iter().all(|a| match a {
                // COUNT(*) and the like reference no column.
                FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => true,
                FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(x),
                    ..
                } => collect_col_refs(x, schema, out),
                _ => false,
            }),
            _ => false,
        },
        // Anything else (subqueries, MATCH, JSON access, ...) -> be safe.
        _ => false,
    }
}

/// The set of columns of a *joined* (combined) schema that a query actually
/// reads: WHERE, projection, GROUP BY, HAVING, ORDER BY, plus any column an
/// aggregator reads directly by index. `None` means "decode every column" --
/// which is also the honest answer for `SELECT *`, since it reads them all.
///
/// The mask never moves a column: unread columns are decoded as `Value::Null`
/// placeholders *at their original positions*, so every downstream consumer
/// (filters, projections, ORDER BY, aggregates, nested chain steps) keeps
/// indexing the combined row exactly as before. That is deliberate -- pruning
/// positions instead would mean remapping indices in five places, where a
/// single wrong index yields wrong values rather than an error.
#[allow(clippy::too_many_arguments)]
fn join_needed_mask(
    schema: &Schema,
    filter: Option<&Expr>,
    projection: &[sqlparser::ast::SelectItem],
    group_by: &[Expr],
    having: Option<&Expr>,
    order: &[(Expr, bool)],
    direct_cols: &[usize],
) -> Option<Vec<bool>> {
    let mut refs: Vec<usize> = Vec::new();
    for e in filter.into_iter().chain(having) {
        if !collect_col_refs(e, schema, &mut refs) {
            return None;
        }
    }
    for item in projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                if !collect_col_refs(e, schema, &mut refs) {
                    return None;
                }
            }
            // `SELECT *` / `SELECT t.*` genuinely reads every column.
            _ => return None,
        }
    }
    for e in group_by.iter().chain(order.iter().map(|(e, _)| e)) {
        if !collect_col_refs(e, schema, &mut refs) {
            return None;
        }
    }
    refs.extend_from_slice(direct_cols);
    let mut mask = vec![false; schema.columns.len()];
    for i in refs {
        if i < mask.len() {
            mask[i] = true;
        }
    }
    Some(mask)
}

/// The set of columns needed to *decide* whether an ordered row is worth
/// materialising: the filter plus every ORDER BY key expression. `None` means
/// "couldn't determine statically -> decode all".
///
/// Note this deliberately excludes the projection: the point is to run the
/// filter and the top-N admission test on a cheap partial row, then decode the
/// full row only for the rows that survive both.
fn order_probe_mask(
    schema: &Schema,
    filter: Option<&Expr>,
    order: &[(Expr, bool)],
) -> Option<Vec<bool>> {
    let mut refs: Vec<usize> = Vec::new();
    if let Some(f) = filter {
        if !collect_col_refs(f, schema, &mut refs) {
            return None;
        }
    }
    for (e, _) in order {
        if !collect_col_refs(e, schema, &mut refs) {
            return None;
        }
    }
    let mut mask = vec![false; schema.columns.len()];
    for i in refs {
        if i < mask.len() {
            mask[i] = true;
        }
    }
    Some(mask)
}

/// The set of columns an aggregation reads: filter + group-by + aggregate
/// arguments. `None` means "couldn't determine statically -> decode all".
fn agg_needed_mask(schema: &Schema, filter: Option<&Expr>, plan: &AggPlan) -> Option<Vec<bool>> {
    let mut refs: Vec<usize> = Vec::new();
    if let Some(f) = filter {
        if !collect_col_refs(f, schema, &mut refs) {
            return None;
        }
    }
    for e in plan.arg_exprs() {
        if !collect_col_refs(e, schema, &mut refs) {
            return None;
        }
    }
    refs.extend_from_slice(plan.group_cols());
    // Columns aggregators read directly (e.g. SUM(age)) -- these bypass
    // arg_exprs, so they must be added explicitly or the scan would decode
    // them as NULL and silently produce wrong aggregates.
    refs.extend(plan.agg_input_cols());
    refs.extend(plan.sample_input_cols());
    let mut mask = vec![false; schema.columns.len()];
    for i in refs {
        if i < mask.len() {
            mask[i] = true;
        }
    }
    Some(mask)
}

async fn parallel_aggregate(
    db: &Session,
    def: &TableDef,
    filter: Option<Expr>,
    plan: &AggPlan,
) -> Result<GroupAggregator> {
    const BATCH: usize = 8192;
    // A DISTINCT aggregate whose value merges additively (SUM/AVG/GROUP_CONCAT) must
    // NOT be split across workers: a value seen by two workers would be added
    // twice. COUNT(DISTINCT) is safe because merging unions the distinct set and
    // the result is that union.s size, so it keeps its parallelism.

    let workers = if plan.has_unmergeable_distinct() {
        1
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    };
    let prefix = data_prefix(&def.name);
    let schema = def.schema.clone();
    // Only decode the columns this aggregation actually reads (filter + group +
    // aggregate arguments); everything else is skipped in place. `None` = a
    // column reference we couldn't resolve statically, so decode everything.
    let needed = agg_needed_mask(&schema, filter.as_ref(), plan);
    let ncols = schema.columns.len();

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
        let needed = needed.clone();
        let mut check = db.cancel_check();
        handles.push(tokio::task::spawn_blocking(
            move || -> Result<GroupAggregator> {
                check.tick_now()?;
                for b in &blobs {
                    check.tick()?;
                    let row: Vec<Value> = match &needed {
                        Some(mask) => rowdec::decode_projected(b, ncols, mask)?,
                        None => {
                            bincode::deserialize(b).map_err(|e| Error::Storage(e.to_string()))?
                        }
                    };
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
/// Resolve the text collation to use for each ORDER BY key: if the key is a
/// direct reference to a `_bin` column, sort case-sensitively; otherwise the
/// default case-insensitive collation.
fn order_key_collations(order: &[(Expr, bool)], schema: &Schema) -> Vec<elyra_core::Collation> {
    order
        .iter()
        .map(|(e, _)| expr_collation(e, schema))
        .collect()
}

/// The collation of a direct column-reference expression, else the default.
fn expr_collation(e: &Expr, schema: &Schema) -> elyra_core::Collation {
    if let Some(name) = ident_name(e) {
        let want = name.rsplit('.').next().unwrap_or(name);
        if let Some(c) = schema.columns.iter().find(|c| {
            let have = c.name.rsplit('.').next().unwrap_or(&c.name);
            c.name.eq_ignore_ascii_case(name) || have.eq_ignore_ascii_case(want)
        }) {
            return c.collation;
        }
    }
    elyra_core::Collation::Ci
}

fn sort_full_rows(
    rows: &mut [Vec<Value>],
    schema: &Schema,
    order: &[(Expr, bool)],
    cancel: &std::sync::Arc<elyra_core::cancel::QueryCancel>,
) -> Result<()> {
    // Precompute sort keys once per row. Evaluating a key can be arbitrarily
    // expensive (any expression), so observe the deadline while doing it.
    let mut check = elyra_core::cancel::CancelCheck::new(cancel.clone());
    let mut keyed: Vec<(Vec<Value>, usize)> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        check.tick()?;
        let mut keys = Vec::with_capacity(order.len());
        for (e, _) in order {
            keys.push(predicate::eval_row(e, schema, row)?);
        }
        keyed.push((keys, i));
    }
    let colls = order_key_collations(order, schema);
    sort_keyed_coll(&mut keyed, order, &colls);
    reorder(rows, &keyed);
    Ok(())
}

fn sort_mutation_matches(
    rows: &mut [(Vec<u8>, Vec<Value>)],
    schema: &Schema,
    order: &[(Expr, bool)],
    cancel: &std::sync::Arc<elyra_core::cancel::QueryCancel>,
) -> Result<()> {
    let mut check = elyra_core::cancel::CancelCheck::new(cancel.clone());
    let mut keyed = Vec::with_capacity(rows.len());
    for (position, (_, row)) in rows.iter().enumerate() {
        check.tick()?;
        let keys = order
            .iter()
            .map(|(expr, _)| predicate::eval_row(expr, schema, row))
            .collect::<Result<Vec<_>>>()?;
        keyed.push((keys, position));
    }
    let collations = order_key_collations(order, schema);
    sort_keyed_coll(&mut keyed, order, &collations);
    let reordered = keyed
        .iter()
        .map(|(_, position)| rows[*position].clone())
        .collect::<Vec<_>>();
    for (slot, row) in rows.iter_mut().zip(reordered) {
        *slot = row;
    }
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
        // Positional ORDER BY (e.g. `ORDER BY 2`) -> the Nth output column.
        if let Some(n) = order_ordinal(e) {
            if n >= 1 && n <= schema.columns.len() {
                cols.push(n - 1);
                continue;
            }
            return Err(Error::Query(format!(
                "ORDER BY position {n} is out of range (1..{})",
                schema.columns.len()
            )));
        }
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
    // Collation per key: the resolved output column's collation.
    let colls: Vec<elyra_core::Collation> =
        cols.iter().map(|&c| schema.columns[c].collation).collect();
    let mut keyed: Vec<(Vec<Value>, usize)> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| (cols.iter().map(|&c| row[c].clone()).collect(), i))
        .collect();
    sort_keyed_coll(&mut keyed, order, &colls);
    reorder(rows, &keyed);
    Ok(())
}

fn sort_keyed_coll(
    keyed: &mut [(Vec<Value>, usize)],
    order: &[(Expr, bool)],
    colls: &[elyra_core::Collation],
) {
    keyed.sort_by(|a, b| {
        for (i, (_, asc)) in order.iter().enumerate() {
            let coll = colls.get(i).copied().unwrap_or(elyra_core::Collation::Ci);
            let ord = a.0[i].total_cmp_coll(&b.0[i], coll);
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
/// Build equi-height histogram boundaries (B+1 sorted wire-string values) from a
/// column sample. Returns empty if the sample is too small to be useful.
fn equi_height_hist(sample: &mut [Value], buckets: usize) -> Vec<String> {
    if sample.len() < buckets * 2 {
        return Vec::new();
    }
    sample.sort_by(|a, b| a.compare(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sample.len();
    let mut out = Vec::with_capacity(buckets + 1);
    for k in 0..=buckets {
        let idx = (k * (n - 1)) / buckets;
        if let Some(s) = sample[idx].to_wire_string() {
            out.push(s);
        }
    }
    // Only keep a monotonic, useful histogram.
    if out.len() < 2 {
        return Vec::new();
    }
    out
}

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
    // Reservoir sample per column for equi-height histograms.
    const SAMPLE_CAP: usize = 20_000;
    const HIST_BUCKETS: usize = 32;
    let mut sample: Vec<Vec<Value>> = vec![Vec::new(); ncols];
    let mut seen: Vec<u64> = vec![0; ncols];
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;

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
            let row: Vec<Value> = rowdec::decode_row(v)?;
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
                // Reservoir sampling for the histogram.
                seen[i] += 1;
                if sample[i].len() < SAMPLE_CAP {
                    sample[i].push(val.clone());
                } else {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let j = (rng % seen[i]) as usize;
                    if j < SAMPLE_CAP {
                        sample[i][j] = val.clone();
                    }
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
            hist: equi_height_hist(&mut sample[i], HIST_BUCKETS),
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
    for prefix in [
        data_prefix(name),
        index_table_prefix(name),
        indexnull_table_prefix(name),
    ] {
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
    coerce_with_mode(v, ty, col, true)
}

fn coerce_for_session(db: &Session, v: Value, ty: &ColumnType, col: &str) -> Result<Value> {
    coerce_with_mode(v, ty, col, db.strict_sql_mode())
}

fn coerce_with_mode(v: Value, ty: &ColumnType, col: &str, strict: bool) -> Result<Value> {
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
        (ColumnType::Date, Value::Text(s)) => {
            match elyra_core::datetime::parse_date(&s).or_else(|| {
                // MySQL accepts a valid datetime-shaped string for a DATE
                // column and stores its date component. Client libraries can
                // bind date values in this form.
                elyra_core::datetime::parse_datetime(&s)
                    .map(|micros| micros.div_euclid(86_400_000_000) as i32)
            }) {
                Some(date) => Value::Date(date),
                None if !strict => Value::Text("0000-00-00".into()),
                None => return Err(Error::Type(format!("invalid DATE literal: {s}"))),
            }
        }
        (ColumnType::DateTime, Value::DateTime(t)) => Value::DateTime(t),
        (ColumnType::DateTime, Value::Text(s)) => match elyra_core::datetime::parse_datetime(&s) {
            Some(datetime) => Value::DateTime(datetime),
            None if !strict => Value::Text("0000-00-00 00:00:00".into()),
            None => return Err(Error::Type(format!("invalid DATETIME literal: {s}"))),
        },
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
            let v = elyra_core::value::rescale_decimal(u, s, *sc)
                .ok_or_else(|| Error::Type(format!("DECIMAL value is out of range for {col}")))?;
            Value::Decimal(v, *sc)
        }
        (ColumnType::Time, Value::Time(t)) => Value::Time(t),
        (ColumnType::Time, Value::Text(s)) => match elyra_core::datetime::parse_time(&s) {
            Some(time) => Value::Time(time),
            None if !strict => Value::Text("00:00:00".into()),
            None => return Err(Error::Type(format!("invalid TIME literal: {s}"))),
        },
        (ColumnType::Json, Value::Json(s)) => Value::Json(s),
        (ColumnType::Json, Value::Text(s)) => {
            if elyra_core::value::is_valid_json(&s) {
                Value::Json(s)
            } else {
                return Err(Error::Type(format!("invalid JSON literal: {s}")));
            }
        }
        (ColumnType::Vector(dim), Value::Text(s)) => Value::Vector(parse_vector(&s, *dim)?),
        // BIGINT UNSIGNED.
        (ColumnType::UInt, Value::UInt(u)) => Value::UInt(u),
        (ColumnType::UInt, Value::Int(i)) if i >= 0 => Value::UInt(i as u64),
        (ColumnType::UInt, Value::Int(_)) if !strict => Value::UInt(0),
        (ColumnType::UInt, Value::Int(i)) => {
            return Err(Error::Type(format!("invalid UNSIGNED value: {i}")))
        }
        (ColumnType::UInt, Value::Bool(b)) => Value::UInt(b as u64),
        (ColumnType::UInt, Value::Float(f))
            if f.is_finite() && (0.0..18_446_744_073_709_551_616.0).contains(&f) =>
        {
            Value::UInt(f as u64)
        }
        (ColumnType::UInt, Value::Float(_)) if !strict => Value::UInt(0),
        (ColumnType::UInt, Value::Float(f)) => {
            return Err(Error::Type(format!("invalid UNSIGNED value: {f}")))
        }
        (ColumnType::UInt, Value::Text(s)) => {
            let text = s.trim();
            if let Ok(value) = text.parse::<u64>() {
                Value::UInt(value)
            } else if let Ok(value) = text.parse::<f64>() {
                if value.is_finite() && (0.0..18_446_744_073_709_551_616.0).contains(&value) {
                    Value::UInt(value as u64)
                } else if !strict {
                    Value::UInt(if value.is_sign_negative() {
                        0
                    } else {
                        u64::MAX
                    })
                } else {
                    return Err(Error::Type(format!("invalid UNSIGNED value: {s}")));
                }
            } else if !strict {
                Value::UInt(mysql_integer_prefix(&s).max(0) as u64)
            } else {
                return Err(Error::Type(format!("invalid UNSIGNED value: {s}")));
            }
        }
        (ColumnType::Int, Value::UInt(u)) => Value::Int(u as i64),
        (ColumnType::Float, Value::UInt(u)) => Value::Float(u as f64),
        // Lenient (MySQL-style) conversions.
        (ColumnType::Int, Value::Float(f)) => Value::Int(f as i64),
        (ColumnType::Int, Value::Text(s)) => {
            match s
                .trim()
                .parse::<i64>()
                .or_else(|_| s.trim().parse::<f64>().map(|f| f as i64))
            {
                Ok(value) => Value::Int(value),
                Err(_) if !strict => Value::Int(mysql_integer_prefix(&s)),
                Err(_) => return Err(Error::Type(format!("invalid INTEGER value: {s}"))),
            }
        }
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

fn mysql_integer_prefix(value: &str) -> i64 {
    let value = value.trim_start();
    let bytes = value.as_bytes();
    let mut end = usize::from(matches!(bytes.first(), Some(b'+') | Some(b'-')));
    let mut digits = 0usize;
    while bytes.get(end).is_some_and(u8::is_ascii_digit) {
        end += 1;
        digits += 1;
    }
    if bytes.get(end) == Some(&b'.') {
        end += 1;
        while bytes.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return 0;
    }
    if matches!(bytes.get(end), Some(b'e') | Some(b'E')) {
        let exponent_start = end;
        end += 1;
        if matches!(bytes.get(end), Some(b'+') | Some(b'-')) {
            end += 1;
        }
        let exponent_digits = end;
        while bytes.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
        if end == exponent_digits {
            end = exponent_start;
        }
    }
    value[..end]
        .parse::<f64>()
        .map(|number| number as i64)
        .unwrap_or(0)
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

#[cfg(test)]
mod plan_tests {
    use super::{
        order_col_index, order_is_pk_prefix, ordered_scan_budget, secondary_order_plan, NullMode,
    };
    use crate::catalog::{IndexDef, TableDef};
    use elyra_core::{ColumnDef, ColumnType, Schema};
    use sqlparser::ast::{Expr, Ident};

    fn idx(name: &str, cols: Vec<usize>, unique: bool, indexes_nulls: bool) -> IndexDef {
        IndexDef {
            name: name.into(),
            cols,
            unique,
            vector: false,
            fulltext: false,
            col_collations: Vec::new(),
            indexes_nulls,
        }
    }

    // Table t(id INT PK, revenue INT NULL, grp INT NOT NULL) with a nullable
    // single-column index on revenue and a NOT NULL one on grp.
    fn tbl() -> TableDef {
        TableDef {
            name: "t".into(),
            schema: Schema::new(vec![
                ColumnDef::new("id", ColumnType::Int, false),
                ColumnDef::new("revenue", ColumnType::Int, true),
                ColumnDef::new("grp", ColumnType::Int, false),
            ]),
            pk_cols: vec![0],
            indexes: vec![
                idx("ix_rev", vec![1], false, true),
                idx("ix_grp", vec![2], false, true),
            ],
            col_meta: Vec::new(),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
        }
    }

    fn ob(name: &str, asc: bool) -> (Expr, bool) {
        (Expr::Identifier(Ident::new(name)), asc)
    }

    #[test]
    fn resolves_order_columns() {
        let t = tbl();
        assert_eq!(
            order_col_index(&t, &Expr::Identifier(Ident::new("revenue"))),
            Some(1)
        );
        assert_eq!(
            order_col_index(&t, &Expr::Identifier(Ident::new("grp"))),
            Some(2)
        );
        assert_eq!(
            order_col_index(&t, &Expr::Identifier(Ident::new("nope"))),
            None
        );
    }

    #[test]
    fn pk_prefix_direction() {
        let t = tbl();
        assert!(order_is_pk_prefix(&t, &[ob("id", true)], true));
        assert!(order_is_pk_prefix(&t, &[ob("id", false)], false));
        assert!(!order_is_pk_prefix(&t, &[ob("id", true)], false));
        assert!(!order_is_pk_prefix(&t, &[ob("revenue", true)], true));
    }

    #[test]
    fn secondary_plan_null_modes() {
        let t = tbl();
        // Nullable single-column index -> Indexed (NULLs stored).
        let p = secondary_order_plan(&t, &[ob("revenue", false)]).unwrap();
        assert_eq!(p.index, "ix_rev");
        assert!(p.rev); // DESC
        assert!(p.null_mode == NullMode::Indexed);
        assert!(!p.has_tiebreaker);

        // NOT NULL single-column index -> None (complete walk, no NULL block).
        let p = secondary_order_plan(&t, &[ob("grp", true)]).unwrap();
        assert_eq!(p.index, "ix_grp");
        assert!(p.null_mode == NullMode::None);

        // Legacy (no stored NULLs) on a nullable column.
        let mut t2 = tbl();
        t2.indexes[0].indexes_nulls = false;
        let p = secondary_order_plan(&t2, &[ob("revenue", true)]).unwrap();
        assert!(p.null_mode == NullMode::Legacy);
    }

    #[test]
    fn secondary_plan_pk_tiebreaker() {
        let t = tbl();
        // The non-unique index appends the clustered PK, so `revenue, id` matches.
        let p = secondary_order_plan(&t, &[ob("revenue", false), ob("id", false)]).unwrap();
        assert_eq!(p.index, "ix_rev");
        assert!(p.has_tiebreaker);
        // Mixed directions cannot use one walk.
        assert!(secondary_order_plan(&t, &[ob("revenue", true), ob("id", false)]).is_none());
        // A trailing non-PK column is not the clustered suffix.
        assert!(secondary_order_plan(&t, &[ob("revenue", false), ob("grp", false)]).is_none());
    }

    #[test]
    fn scan_budget_default() {
        // max(256 * need, 50_000)
        assert_eq!(ordered_scan_budget(40), 50_000);
        assert_eq!(ordered_scan_budget(1000), 256_000);
    }
}

#[cfg(test)]
mod cancel_tests {
    use crate::Engine;
    use elyra_core::Privilege;
    use elyra_storage::Db;

    /// A cancelled statement must stop inside the engine's row loops and report
    /// it, rather than running to completion. Uses an explicit cancel (not the
    /// timeout) so the test does not depend on process-wide env configuration.
    #[tokio::test]
    async fn cancelled_statement_aborts_scan() {
        let db = Db::in_memory().unwrap();
        let engine = Engine::new(db);
        let sess = engine.session();
        engine
            .execute(
                "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
                Privilege::Admin,
                &sess,
            )
            .await
            .unwrap();
        for i in 1..=3000 {
            engine
                .execute(
                    &format!("INSERT INTO t VALUES ({i}, {})", i % 9),
                    Privilege::Admin,
                    &sess,
                )
                .await
                .unwrap();
        }
        // Sanity: the query succeeds while the statement is not cancelled.
        engine
            .execute(
                "SELECT COUNT(*) FROM t WHERE v > 0",
                Privilege::Admin,
                &sess,
            )
            .await
            .map(|_| ())
            .expect("baseline query should succeed");

        // Now ask the session to stop; the next statement must refuse to grind
        // through its rows.
        sess.cancel_token().cancel();
        let err = engine
            .execute(
                "SELECT COUNT(*) FROM t WHERE v > 0",
                Privilege::Admin,
                &sess,
            )
            .await
            .map(|_| ())
            .expect_err("a cancelled statement must not run to completion");
        assert!(
            err.to_string().contains("cancelled"),
            "expected a cancellation error, got: {err}"
        );

        // Clearing the cancellation makes the session usable again.
        sess.disarm_cancel();
        engine
            .execute(
                "SELECT COUNT(*) FROM t WHERE v > 0",
                Privilege::Admin,
                &sess,
            )
            .await
            .map(|_| ())
            .expect("session should be reusable after the cancellation is cleared");
    }

    /// A join that explodes must also observe cancellation: the expansion of a
    /// single driving row is where a runaway join spends its time.
    #[tokio::test]
    async fn cancelled_statement_aborts_join_expansion() {
        let db = Db::in_memory().unwrap();
        let engine = Engine::new(db);
        let sess = engine.session();
        engine
            .execute(
                "CREATE TABLE j (id INT PRIMARY KEY, v INT)",
                Privilege::Admin,
                &sess,
            )
            .await
            .unwrap();
        for i in 1..=400 {
            engine
                .execute(
                    &format!("INSERT INTO j VALUES ({i}, {})", i % 3),
                    Privilege::Admin,
                    &sess,
                )
                .await
                .unwrap();
        }
        sess.cancel_token().cancel();
        let err = engine
            .execute(
                "SELECT a.id FROM j a, j b, j c WHERE a.v = b.v AND b.v = c.v ORDER BY a.id DESC",
                Privilege::Admin,
                &sess,
            )
            .await
            .map(|_| ())
            .expect_err("a cancelled join must abort");
        assert!(
            err.to_string().contains("cancelled"),
            "expected a cancellation error, got: {err}"
        );
    }
}

#[cfg(test)]
mod join_budget_tests {
    use super::{join_max_rows, join_max_rows_total, JoinBudget};

    /// The shared budget is global, so these must not run concurrently.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn reservation_is_released_on_drop() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = JoinBudget::live();
        {
            let mut b = JoinBudget::new();
            b.account(1).unwrap();
            assert!(
                JoinBudget::live() > before,
                "accounting rows must reserve from the shared budget"
            );
            b.account(JoinBudget::BLOCK * 2).unwrap();
        }
        assert_eq!(
            JoinBudget::live(),
            before,
            "the reservation must be returned when the join ends"
        );
    }

    #[test]
    fn per_join_cap_is_enforced() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = JoinBudget::live();
        let mut b = JoinBudget::new();
        let err = b
            .account(join_max_rows() + 1)
            .expect_err("past the per-join cap must fail");
        assert!(
            err.to_string().contains("ELYRASQL_JOIN_MAX_ROWS"),
            "the error should name the knob: {err}"
        );
        drop(b);
        assert_eq!(JoinBudget::live(), before);
    }

    // The byte ceiling is what actually protects the process: a row count is only a
    // proxy, and a wide row costs many times what a narrow one does.
    #[test]
    fn byte_ceiling_accounts_for_row_width() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let narrow = [elyra_core::Value::Int(1), elyra_core::Value::Int(2)];
        let wide = [
            elyra_core::Value::Text("x".repeat(4096)),
            elyra_core::Value::Int(1),
        ];
        assert!(
            super::estimated_row_bytes(&wide) > 10 * super::estimated_row_bytes(&narrow),
            "a 4 KiB text row must cost far more than two integers"
        );
        // Sampling is what ties the reservation to this join's actual width.
        let mut b = JoinBudget::new();
        b.sample(&wide);
        b.account(1).unwrap();
        let after_wide = super::JOIN_BYTES_LIVE.load(std::sync::atomic::Ordering::Relaxed);
        assert!(after_wide > 0, "reserving rows must reserve bytes too");
        drop(b);
        assert_eq!(
            super::JOIN_BYTES_LIVE.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "byte reservation must be returned on drop"
        );
    }

    // A per-join cap alone does not bound the server: concurrent joins must share
    // a ceiling, and it must be fully reclaimed afterwards.
    #[test]
    fn shared_ceiling_bounds_concurrent_joins() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = JoinBudget::live();
        let total = join_max_rows_total();
        let per = join_max_rows();
        let mut held = Vec::new();
        let mut refused = false;
        // Each "join" reserves up to the per-join cap; enough of them must be
        // refused by the shared ceiling rather than all succeeding.
        for _ in 0..(total / per + 2) {
            let mut b = JoinBudget::new();
            match b.account(per) {
                Ok(()) => held.push(b),
                Err(e) => {
                    assert!(
                        e.to_string().contains("ELYRASQL_JOIN_MAX_ROWS_TOTAL"),
                        "the error should name the shared knob: {e}"
                    );
                    refused = true;
                    break;
                }
            }
        }
        assert!(
            refused,
            "the shared ceiling must refuse a burst of large joins"
        );
        assert!(JoinBudget::live() <= total);
        drop(held);
        assert_eq!(JoinBudget::live(), before, "budget must be fully reclaimed");
    }
}

#[cfg(test)]
mod join_key_tests {
    use super::JoinKey;
    use std::collections::HashMap;
    use std::hash::{BuildHasher, RandomState};

    fn hash_of<H: std::hash::Hash + ?Sized>(rs: &RandomState, v: &H) -> u64 {
        rs.hash_one(v)
    }

    /// Both variants must hash and compare exactly as the borrowed slice does --
    /// otherwise a lookup by slice misses an inline key and the join silently
    /// drops rows. Checked across the inline/heap boundary.
    #[test]
    fn hashes_and_compares_as_the_borrowed_slice() {
        let rs = RandomState::new();
        for len in [0usize, 1, 8, 9, 21, 22, 23, 24, 64, 300] {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            let k = JoinKey::from_bytes(&bytes);
            assert_eq!(k.as_bytes(), &bytes[..], "round-trip at len {len}");
            assert_eq!(
                hash_of(&rs, &k),
                hash_of(&rs, &bytes[..]),
                "key must hash as its slice at len {len}"
            );
            let heap = JoinKey::Heap(bytes.clone().into_boxed_slice());
            assert_eq!(
                k, heap,
                "variants with equal bytes must be equal (len {len})"
            );
            assert_eq!(
                hash_of(&rs, &k),
                hash_of(&rs, &heap),
                "variants with equal bytes must hash equally (len {len})"
            );
        }
    }

    #[test]
    fn probing_by_slice_finds_both_representations() {
        let mut m: HashMap<JoinKey, u32> = HashMap::new();
        let short = b"short-key".to_vec();
        let long = vec![7u8; 100];
        m.insert(JoinKey::from_bytes(&short), 1);
        m.insert(JoinKey::from_bytes(&long), 2);
        assert_eq!(m.get(short.as_slice()), Some(&1));
        assert_eq!(m.get(long.as_slice()), Some(&2));
        assert_eq!(m.get(b"missing".as_slice()), None);
        assert_eq!(m.len(), 2);
    }
}
