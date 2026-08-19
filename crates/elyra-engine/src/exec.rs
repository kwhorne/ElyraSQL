//! Statement execution over the clustered single-file store.
//!
//! Implements `CREATE TABLE`, `INSERT`, `SELECT ... FROM`, `DROP TABLE`.
//! Inserts are batched into one group-commit; scans stream.

use crate::session::Session;
use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, Assignment, AssignmentTarget, CharacterLength,
    ColumnOption, CreateIndex, CreateTable, DataType, Delete, ExactNumberInfo, FromTable, Ident,
    Insert, JoinConstraint, JoinOperator, ObjectName, OrderByExpr, Query as SqlQuery, Select,
    SetExpr, TableAlias, TableAliasColumnDef, TableConstraint, TableFactor, TableWithJoins, Visit,
    VisitMut, Visitor, VisitorMut, With,
};
use std::ops::ControlFlow;

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

/// Resolve a stored relation name in the session's single logical database.
/// ElyraSQL has one physical catalog per session, so silently discarding a
/// different schema qualifier would read the wrong relation.
pub(crate) fn stored_table_ident(db: &Session, name: &ObjectName) -> Result<String> {
    match name.0.as_slice() {
        [table] => Ok(table.value.clone()),
        [schema, table] if schema.value == db.database() => Ok(table.value.clone()),
        [schema, _] => Err(Error::UnknownDatabase(schema.value.clone())),
        [] => Err(Error::Catalog("empty table name".into())),
        _ => Err(Error::Parse(format!(
            "invalid qualified table name: {name}"
        ))),
    }
}

/// Validate a database argument used by metadata statements that do not name
/// a table. ElyraSQL exposes exactly the session's selected logical database.
pub(crate) fn selected_database_ident(db: &Session, name: &ObjectName) -> Result<()> {
    match name.0.as_slice() {
        [database] if database.value == db.database() => Ok(()),
        [database] => Err(Error::UnknownDatabase(database.value.clone())),
        [] => Err(Error::Catalog("empty database name".into())),
        _ => Err(Error::Parse(format!(
            "invalid qualified database name: {name}"
        ))),
    }
}

/// Resolve the column portion of an assignment while retaining and validating
/// every qualifier. Dropping leading components can redirect a rejected MySQL
/// statement to the local table, which is especially dangerous for DML.
fn assignment_column_for_table(
    db: &Session,
    relation: &ObjectName,
    alias: Option<&sqlparser::ast::TableAlias>,
    target: &ObjectName,
) -> Result<String> {
    let relation_name = stored_table_ident(db, relation)?;
    match target.0.as_slice() {
        [column] => Ok(column.value.clone()),
        [qualifier, column] => {
            let expected = alias
                .map(|a| a.name.value.as_str())
                .unwrap_or(relation_name.as_str());
            if qualifier.value == expected {
                Ok(column.value.clone())
            } else {
                Err(Error::UnknownColumn(target.to_string()))
            }
        }
        [schema, qualifier, column] => {
            let expected = alias
                .map(|a| a.name.value.as_str())
                .unwrap_or(relation_name.as_str());
            if schema.value == db.database() && qualifier.value == expected {
                Ok(column.value.clone())
            } else {
                Err(Error::UnknownColumn(target.to_string()))
            }
        }
        [] => Err(Error::UnknownColumn(String::new())),
        _ => Err(Error::Parse(format!(
            "invalid qualified assignment target: {target}"
        ))),
    }
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

/// The declared width of an integer type, in bits, or `None` for anything whose
/// range is the full 64 bits (or which is not an integer at all).
///
/// Storage is 64-bit regardless; this is only the *constraint* MySQL applies, so
/// `TINYINT` refuses 300 while still occupying the same slot as a `BIGINT`.
fn declared_int_bits(dt: &DataType) -> Option<u8> {
    match dt {
        DataType::TinyInt(_) | DataType::UnsignedTinyInt(_) => Some(8),
        DataType::SmallInt(_) | DataType::UnsignedSmallInt(_) => Some(16),
        DataType::MediumInt(_) | DataType::UnsignedMediumInt(_) => Some(24),
        DataType::Int(_)
        | DataType::Integer(_)
        | DataType::UnsignedInt(_)
        | DataType::UnsignedInteger(_) => Some(32),
        _ => None,
    }
}

/// Inclusive range a value must fall in for a column of `bits` width.
fn int_bounds(bits: u8, unsigned: bool) -> (i128, i128) {
    if unsigned {
        (0, (1i128 << bits) - 1)
    } else {
        (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
    }
}

fn character_length(length: &Option<CharacterLength>) -> Option<u64> {
    match length {
        Some(CharacterLength::IntegerLength { length, .. }) => Some(*length),
        Some(CharacterLength::Max) | None => None,
    }
}

fn exact_number(precision: &ExactNumberInfo) -> (u64, u64) {
    match precision {
        ExactNumberInfo::None => (10, 0),
        ExactNumberInfo::Precision(precision) => (*precision, 0),
        ExactNumberInfo::PrecisionAndScale(precision, scale) => (*precision, *scale),
    }
}

fn declaration(
    data_type: impl Into<String>,
    column_type: impl Into<String>,
    character_maximum_length: Option<u64>,
    numeric_precision: Option<u64>,
    numeric_scale: Option<u64>,
) -> catalog::ColumnDeclaration {
    catalog::ColumnDeclaration {
        data_type: data_type.into(),
        column_type: column_type.into(),
        character_maximum_length,
        numeric_precision,
        numeric_scale,
    }
}

fn integer_declaration(
    name: &str,
    display_width: Option<u64>,
    unsigned: bool,
    precision: u64,
) -> catalog::ColumnDeclaration {
    let display_width = (name == "tinyint").then_some(display_width).flatten();
    let mut column_type = match display_width {
        Some(width) => format!("{name}({width})"),
        None => name.to_owned(),
    };
    if unsigned {
        column_type.push_str(" unsigned");
    }
    declaration(name, column_type, None, Some(precision), Some(0))
}

/// Preserve the MySQL declaration separately from the compact storage type.
/// `ColumnType` intentionally merges families that share a representation
/// (for example `INT` and `BIGINT`), so it cannot reconstruct this later.
fn declaration_from_data_type(dt: &DataType) -> Result<catalog::ColumnDeclaration> {
    Ok(match dt {
        DataType::Bool | DataType::Boolean => {
            declaration("tinyint", "tinyint(1)", None, Some(3), Some(0))
        }
        DataType::TinyInt(width) => integer_declaration("tinyint", *width, false, 3),
        DataType::UnsignedTinyInt(width) => integer_declaration("tinyint", *width, true, 3),
        DataType::SmallInt(width) => integer_declaration("smallint", *width, false, 5),
        DataType::UnsignedSmallInt(width) => integer_declaration("smallint", *width, true, 5),
        DataType::MediumInt(width) => integer_declaration("mediumint", *width, false, 7),
        DataType::UnsignedMediumInt(width) => integer_declaration("mediumint", *width, true, 7),
        DataType::Int(width) | DataType::Integer(width) => {
            integer_declaration("int", *width, false, 10)
        }
        DataType::UnsignedInt(width) | DataType::UnsignedInteger(width) => {
            integer_declaration("int", *width, true, 10)
        }
        DataType::BigInt(width) => integer_declaration("bigint", *width, false, 19),
        DataType::UnsignedBigInt(width) => integer_declaration("bigint", *width, true, 20),
        DataType::Varchar(length)
        | DataType::Nvarchar(length)
        | DataType::CharacterVarying(length)
        | DataType::CharVarying(length) => {
            let maximum = character_length(length);
            let column_type = maximum
                .map(|length| format!("varchar({length})"))
                .unwrap_or_else(|| "varchar".into());
            declaration("varchar", column_type, maximum, None, None)
        }
        DataType::Char(length) | DataType::Character(length) => {
            let maximum = character_length(length).or(Some(1));
            let column_type = maximum
                .map(|length| format!("char({length})"))
                .unwrap_or_else(|| "char".into());
            declaration("char", column_type, maximum, None, None)
        }
        // Binary strings carry a byte limit, not a character limit, but it is
        // the same field: `character_maximum_length` is what MySQL's
        // `information_schema` reports for both, and result-column widths need
        // it for `VARBINARY(n)` exactly as for `VARCHAR(n)`.
        DataType::Varbinary(length) => {
            let maximum = *length;
            let column_type = maximum
                .map(|length| format!("varbinary({length})"))
                .unwrap_or_else(|| "varbinary".into());
            declaration("varbinary", column_type, maximum, None, None)
        }
        DataType::Binary(length) => {
            let maximum = length.or(Some(1));
            let column_type = maximum
                .map(|length| format!("binary({length})"))
                .unwrap_or_else(|| "binary".into());
            declaration("binary", column_type, maximum, None, None)
        }
        DataType::Text => declaration("text", "text", Some(65_535), None, None),
        DataType::TinyText => declaration("tinytext", "tinytext", Some(255), None, None),
        DataType::MediumText => {
            declaration("mediumtext", "mediumtext", Some(16_777_215), None, None)
        }
        DataType::LongText => declaration("longtext", "longtext", Some(4_294_967_295), None, None),
        DataType::Datetime(precision) => {
            let column_type = precision
                .map(|precision| format!("datetime({precision})"))
                .unwrap_or_else(|| "datetime".into());
            declaration("datetime", column_type, None, None, None)
        }
        DataType::Timestamp(precision, _) => {
            let column_type = precision
                .map(|precision| format!("timestamp({precision})"))
                .unwrap_or_else(|| "timestamp".into());
            declaration("timestamp", column_type, None, None, None)
        }
        DataType::Decimal(info) | DataType::Numeric(info) | DataType::Dec(info) => {
            let (precision, scale) = exact_number(info);
            declaration(
                "decimal",
                format!("decimal({precision},{scale})"),
                None,
                Some(precision),
                Some(scale),
            )
        }
        _ => declaration_from_storage_type(&map_type(dt)?),
    })
}

/// Metadata fallback for catalogs created before declared-type sidecars existed.
fn declaration_from_storage_type(ty: &ColumnType) -> catalog::ColumnDeclaration {
    match ty {
        ColumnType::Bool => declaration("tinyint", "tinyint(1)", None, Some(3), Some(0)),
        ColumnType::Int => declaration("bigint", "bigint", None, Some(19), Some(0)),
        ColumnType::UInt => declaration("bigint", "bigint unsigned", None, Some(20), Some(0)),
        ColumnType::Float => declaration("double", "double", None, Some(53), None),
        ColumnType::Text => declaration("text", "text", Some(65_535), None, None),
        ColumnType::Bytes => declaration("blob", "blob", None, None, None),
        ColumnType::Vector(dimension) => {
            declaration("vector", format!("vector({dimension})"), None, None, None)
        }
        ColumnType::Date => declaration("date", "date", None, None, None),
        ColumnType::DateTime => declaration("datetime", "datetime", None, None, None),
        ColumnType::Decimal(precision, scale) => declaration(
            "decimal",
            format!("decimal({precision},{scale})"),
            None,
            Some(u64::from(*precision)),
            Some(u64::from(*scale)),
        ),
        ColumnType::Time => declaration("time", "time", None, None, None),
        ColumnType::Json => declaration("json", "json", None, None, None),
    }
}

fn column_declaration<'a>(
    declarations: Option<&'a catalog::ColumnDeclarations>,
    column: &ColumnDef,
    index: usize,
) -> std::borrow::Cow<'a, catalog::ColumnDeclaration> {
    declarations
        .and_then(|declarations| declarations.columns.get(index))
        .map(std::borrow::Cow::Borrowed)
        .unwrap_or_else(|| std::borrow::Cow::Owned(declaration_from_storage_type(&column.ty)))
}

fn optional_u64_value(value: Option<u64>) -> Value {
    value
        .and_then(|value| i64::try_from(value).ok())
        .map(Value::Int)
        .unwrap_or(Value::Null)
}

fn check_declared_character_length(
    declaration: &catalog::ColumnDeclaration,
    value: &Value,
    column: &str,
    row_number: usize,
) -> Result<()> {
    if !matches!(declaration.data_type.as_str(), "char" | "varchar") {
        return Ok(());
    }
    let Some(maximum) = declaration.character_maximum_length else {
        return Ok(());
    };
    let Some(value) = value.to_wire_string() else {
        return Ok(());
    };
    let too_long = usize::try_from(maximum).map_or(true, |maximum| value.chars().count() > maximum);
    if too_long {
        return Err(Error::DataTooLong(format!(
            "Data too long for column '{column}' at row {row_number}"
        )));
    }
    Ok(())
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
        | DataType::BigInt(_) => ColumnType::Int,
        // Every integer width is stored as 64 bits, but `UNSIGNED` is a
        // *constraint*, not a width: dropping it on the narrower types (as this
        // used to) let `TINYINT UNSIGNED` accept -1 while `BIGINT UNSIGNED`
        // rejected it, so the same schema was enforced inconsistently.
        DataType::UnsignedTinyInt(_)
        | DataType::UnsignedSmallInt(_)
        | DataType::UnsignedMediumInt(_)
        | DataType::UnsignedInt(_)
        | DataType::UnsignedInteger(_)
        | DataType::UnsignedBigInt(_) => ColumnType::UInt,
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
        | DataType::CharVarying(_)
        | DataType::Character(_)
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
    let name = stored_table_ident(db, &ct.name)?;

    if catalog::exists(db, &name).await? {
        if ct.if_not_exists {
            return Ok(QueryResult::Affected(0));
        }
        return Err(Error::Catalog(format!("table already exists: {name}")));
    }

    // CREATE TABLE ... LIKE source: copy the structure, no data.
    if let Some(src) = &ct.like {
        let sname = stored_table_ident(db, src)?;
        let mut def = catalog::load(db, &sname).await?;
        def.name = name.clone();
        let mut puts = vec![(catalog_key(&name), def.encode()?)];
        // Declarations and integer constraints are table-scoped sidecars, not
        // part of `TableDef`; `CREATE ... LIKE` must copy them alongside the
        // catalog entry or the clone becomes a lossy schema copy.
        for key in [
            catalog::colwidth_key as fn(&str) -> Vec<u8>,
            catalog::coldecl_key,
        ] {
            if let Some(value) = db.get(key(&sname)).await? {
                puts.push((key(&name), value));
            }
        }
        db.commit_write(puts, vec![]).await?;
        return Ok(QueryResult::Affected(0));
    }

    // CREATE TABLE ... AS SELECT: derive structure from the query, copy rows.
    if let Some(q) = &ct.query {
        return create_table_as(db, vindex, &name, &ct, q).await;
    }

    let mut columns = Vec::with_capacity(ct.columns.len());
    let mut declarations = Vec::with_capacity(ct.columns.len());
    let mut col_meta: Vec<ColMeta> = Vec::with_capacity(ct.columns.len());
    let mut pk_cols: Vec<usize> = Vec::new();
    let mut indexes: Vec<IndexDef> = Vec::new();
    let mut checks: Vec<String> = Vec::new();
    let mut foreign_keys: Vec<ForeignKey> = Vec::new();

    for (idx, col) in ct.columns.iter().enumerate() {
        // Same rule as ALTER TABLE ADD COLUMN: a table cannot hold two columns
        // of the same name, so say so instead of creating one.
        if ct.columns[..idx]
            .iter()
            .any(|earlier| predicate::identifier_eq(&earlier.name.value, &col.name.value))
        {
            return Err(Error::Duplicate(format!(
                "duplicate column name '{}'",
                col.name.value
            )));
        }
        let ty = map_type(&col.data_type)?;
        let declared_type = declaration_from_data_type(&col.data_type)?;
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
            qualifier: Vec::new(),
            result_metadata: Default::default(),
        });
        declarations.push(declared_type);
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
                        .position(|c| predicate::identifier_eq(&c.name, &ident.value))
                        .ok_or_else(|| {
                            Error::Catalog(format!("unknown primary key column: {}", ident.value))
                        })?;
                    columns[i].nullable = false;
                    pk_cols.push(i);
                }
            }
            TableConstraint::Unique {
                name: constraint_name,
                index_name,
                columns: cols,
                ..
            } => {
                let mut idxs = Vec::new();
                for ident in cols {
                    let i = columns
                        .iter()
                        .position(|c| predicate::identifier_eq(&c.name, &ident.value))
                        .ok_or_else(|| {
                            Error::Catalog(format!("unknown unique column: {}", ident.value))
                        })?;
                    idxs.push(i);
                }
                let iname = index_name
                    .as_ref()
                    .or(constraint_name.as_ref())
                    .map(|name| name.value.clone())
                    .unwrap_or_else(|| {
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
                        .position(|column| predicate::identifier_eq(&column.name, &ident.value))
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
                let ref_table = stored_table_ident(db, foreign_table)?;
                let mut fk_cols = Vec::new();
                for ident in cols {
                    let i = columns
                        .iter()
                        .position(|c| predicate::identifier_eq(&c.name, &ident.value))
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
                    ref_table,
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
        storage_generation: 0,
    };
    let widths = catalog::ColumnWidths {
        bits: ct
            .columns
            .iter()
            .map(|c| declared_int_bits(&c.data_type))
            .collect(),
    };
    let declarations = catalog::ColumnDeclarations {
        columns: declarations,
    };
    // Written unconditionally: a table re-created under a name that previously
    // had widths must not inherit them.
    let puts = vec![
        (catalog_key(&name), def.encode()?),
        (
            catalog::colwidth_key(&name),
            bincode::serialize(&widths).map_err(|e| Error::Storage(e.to_string()))?,
        ),
        (
            catalog::coldecl_key(&name),
            bincode::serialize(&declarations).map_err(|e| Error::Storage(e.to_string()))?,
        ),
    ];
    db.commit_write(puts, vec![]).await?;
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
        qualifier: Vec::new(),
        result_metadata: Default::default(),
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
                qualifier: Vec::new(),
                result_metadata: Default::default(),
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

struct ExplainAccess {
    kind: &'static str,
    possible_keys: Option<String>,
    key: Option<String>,
    rows: String,
    extra: String,
}

#[derive(Default)]
struct ExplainFeatureVisitor {
    incremental_window: bool,
}

impl Visitor for ExplainFeatureVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Function(function) = expression {
            self.incremental_window |= function.over.is_some()
                && window_aggregate_is_incremental(&function_name(function));
        }
        ControlFlow::Continue(())
    }
}

async fn explain_first_access(
    db: &Session,
    stmt: &sqlparser::ast::Statement,
    table: &str,
    rows_estimate: String,
) -> Result<ExplainAccess> {
    use sqlparser::ast::{SetExpr, Statement};
    let select = match stmt {
        Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => Some(select.as_ref()),
            _ => None,
        },
        _ => None,
    };
    let selection = select.and_then(|select| select.selection.as_ref());
    let def = match catalog::load(db, table).await {
        Ok(def) => def,
        Err(Error::Catalog(_)) => {
            return Ok(ExplainAccess {
                kind: "ALL",
                possible_keys: None,
                key: None,
                rows: rows_estimate,
                extra: selection.map_or_else(String::new, |_| "Using where".into()),
            });
        }
        Err(error) => return Err(error),
    };
    let mut feature_extra = Vec::new();
    if let (Some(filter), Some(from)) = (selection, select.and_then(|select| select.from.first())) {
        let outer = factor_qualifier_object(db, &from.relation)
            .map(|qualifier| object_name_parts(&qualifier))
            .unwrap_or_else(|| vec![table.to_string()]);
        if correlated_exists_membership_eligible(db, filter, &def, &outer).await? {
            feature_extra.push("Using semi-join membership");
        }
    }
    if select.is_some_and(|select| select.distinct.is_some()) {
        feature_extra.push("Distinct (spill-capable)");
    }
    if let Some(select) = select {
        let mut visitor = ExplainFeatureVisitor::default();
        let _ = select.visit(&mut visitor);
        if visitor.incremental_window {
            feature_extra.push("Incremental window aggregate");
        }
    }
    let decorate = |mut access: ExplainAccess| {
        if !feature_extra.is_empty() {
            if !access.extra.is_empty() {
                access.extra.push_str("; ");
            }
            access.extra.push_str(&feature_extra.join("; "));
        }
        access
    };
    if def.has_pk() && key_eq_values(&def, selection, &def.pk_cols)?.is_some() {
        return Ok(decorate(ExplainAccess {
            kind: "const",
            possible_keys: Some("PRIMARY".into()),
            key: Some("PRIMARY".into()),
            rows: "1".into(),
            extra: "Using where".into(),
        }));
    }
    for index in &def.indexes {
        if !index.vector && key_eq_values(&def, selection, &index.cols)?.is_some() {
            return Ok(decorate(ExplainAccess {
                kind: "ref",
                possible_keys: Some(index.name.clone()),
                key: Some(index.name.clone()),
                rows: "1".into(),
                extra: "Using index condition; Using where".into(),
            }));
        }
    }
    if let Some(range) = composite_range_bounds(&def, selection)? {
        return Ok(decorate(ExplainAccess {
            kind: "range",
            possible_keys: Some(range.index.name.clone()),
            key: Some(range.index.name.clone()),
            rows: rows_estimate,
            extra: "Using index condition; Using where".into(),
        }));
    }
    if let Some(range) = range_bounds(&def, selection)? {
        let key = if def.pk_cols == [range.col] {
            "PRIMARY".to_string()
        } else {
            index::index_on(&def, range.col)
                .map(|index| index.name.clone())
                .unwrap_or_default()
        };
        return Ok(decorate(ExplainAccess {
            kind: "range",
            possible_keys: Some(key.clone()),
            key: Some(key),
            rows: rows_estimate,
            extra: "Using index condition; Using where".into(),
        }));
    }
    Ok(decorate(ExplainAccess {
        kind: "ALL",
        possible_keys: None,
        key: None,
        rows: rows_estimate,
        extra: selection.map_or_else(String::new, |_| "Using where".into()),
    }))
}

fn explain_row(table: Option<String>, access: ExplainAccess, extra: Option<&str>) -> Vec<Value> {
    let mut access_extra = access.extra;
    if let Some(extra) = extra {
        if !access_extra.is_empty() {
            access_extra.push_str("; ");
        }
        access_extra.push_str(extra);
    }
    vec![
        Value::Text("1".into()),
        Value::Text("SIMPLE".into()),
        table.map(Value::Text).unwrap_or(Value::Null),
        Value::Null,
        Value::Text(access.kind.into()),
        access.possible_keys.map(Value::Text).unwrap_or(Value::Null),
        access.key.map(Value::Text).unwrap_or(Value::Null),
        Value::Null,
        Value::Null,
        Value::Text(access.rows),
        Value::Text("100.00".into()),
        Value::Text(access_extra),
    ]
}

/// `EXPLAIN <statement>` — a MySQL-shaped summary of access paths that the
/// executor can prove it will use. It remains a compact trace rather than a
/// full cost model.
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
    let select = match stmt {
        sqlparser::ast::Statement::Query(query) => match query.body.as_ref() {
            SetExpr::Select(select) => Some(select.as_ref()),
            _ => None,
        },
        _ => None,
    };
    let indexed_join = match select {
        Some(select) => match guaranteed_indexed_join_access(db, select).await {
            Ok(access) => access,
            Err(Error::Catalog(_) | Error::UnknownDatabase(_)) => None,
            Err(error) => return Err(error),
        },
        None => None,
    };
    let table = indexed_join
        .as_ref()
        .map(|join| join.driver_table.clone())
        .or_else(|| explain_first_table(stmt));
    let rows_est = match &table {
        Some(t) => catalog::load_stats(db, t)
            .await?
            .map(|s| s.rows.to_string())
            .unwrap_or_else(|| "0".into()),
        None => "0".into(),
    };
    let access = match table.as_deref() {
        Some(table) => explain_first_access(db, stmt, table, rows_est).await?,
        None => ExplainAccess {
            kind: "",
            possible_keys: None,
            key: None,
            rows: rows_est,
            extra: String::new(),
        },
    };
    let mut rows = vec![explain_row(table, access, None)];
    if let Some(join) = indexed_join {
        let partner_rows = catalog::load_stats(db, &join.partner_table)
            .await?
            .map(|stats| stats.rows.to_string())
            .unwrap_or_else(|| "0".into());
        rows.push(explain_row(
            Some(join.partner_table),
            ExplainAccess {
                kind: join.access_type,
                possible_keys: Some(join.index_name.clone()),
                key: Some(join.index_name),
                rows: partner_rows,
                extra: "Using index condition".into(),
            },
            Some("Indexed nested-loop join"),
        ));
    }
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
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
        ("group_concat_max_len", "1024".into()),
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
            "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION".into(),
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
    enum Token {
        Literal(char),
        Any,
        One,
    }

    let text = name
        .chars()
        .map(|character| character.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut tokens = Vec::with_capacity(pattern.len());
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        if character == '\\' {
            tokens.push(Token::Literal(
                characters.next().unwrap_or('\\').to_ascii_lowercase(),
            ));
        } else {
            tokens.push(match character {
                '%' => Token::Any,
                '_' => Token::One,
                literal => Token::Literal(literal.to_ascii_lowercase()),
            });
        }
    }

    let (mut text_index, mut token_index) = (0usize, 0usize);
    let (mut wildcard, mut retry_text) = (None, 0usize);
    while text_index < text.len() {
        match tokens.get(token_index) {
            Some(Token::Any) => {
                wildcard = Some(token_index);
                retry_text = text_index;
                token_index += 1;
            }
            Some(Token::One) => {
                text_index += 1;
                token_index += 1;
            }
            Some(Token::Literal(literal)) if *literal == text[text_index] => {
                text_index += 1;
                token_index += 1;
            }
            _ => {
                let Some(wildcard_index) = wildcard else {
                    return false;
                };
                retry_text += 1;
                text_index = retry_text;
                token_index = wildcard_index + 1;
            }
        }
    }
    while matches!(tokens.get(token_index), Some(Token::Any)) {
        token_index += 1;
    }
    token_index == tokens.len()
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
pub fn show_variables(
    db: &Session,
    filter: Option<&sqlparser::ast::ShowStatementFilter>,
) -> Result<QueryResult> {
    let pat = show_filter_pattern(filter);
    let rows: Vec<Vec<Value>> = system_variables()
        .into_iter()
        .filter(|(name, _)| pat.as_deref().is_none_or(|p| show_like(name, p)))
        .map(|(name, val)| {
            let value = match name {
                "autocommit" => if db.autocommit() { "ON" } else { "OFF" }.into(),
                "foreign_key_checks" => if db.foreign_key_checks() { "ON" } else { "OFF" }.into(),
                "group_concat_max_len" => db.group_concat_max_len().to_string(),
                "sql_mode" => db.sql_mode(),
                "transaction_isolation" | "tx_isolation" => db.transaction_isolation(),
                _ => val,
            };
            vec![Value::Text(name.to_string()), Value::Text(value)]
        })
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
pub async fn show_table_status(db: &Session, pattern: Option<&str>) -> Result<QueryResult> {
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
    for n in names
        .into_iter()
        .filter(|name| pattern.is_none_or(|pattern| show_like(name, pattern)))
    {
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
    let declarations = catalog::load_declarations(db, table).await?;
    let head = ["Field", "Type", "Null", "Key", "Default", "Extra"];
    let schema = Schema::new(
        head.iter()
            .map(|n| ColumnDef {
                name: (*n).to_string(),
                ty: ColumnType::Text,
                nullable: *n == "Default",
                collation: elyra_core::Collation::Ci,
                qualifier: Vec::new(),
                result_metadata: Default::default(),
            })
            .collect(),
    );
    let mut rows = Vec::with_capacity(def.schema.columns.len());
    for (i, c) in def.schema.columns.iter().enumerate() {
        let declaration = column_declaration(declarations.as_ref(), c, i);
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
            Value::Text(declaration.column_type.clone()),
            Value::Text(if c.nullable { "YES" } else { "NO" }.to_string()),
            Value::Text(key.to_string()),
            default,
            Value::Text(extra.to_string()),
        ]);
    }
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// The `ON DELETE`/`ON UPDATE` clause to echo for a referential action, or
/// `None` for the default that MySQL leaves implicit.
fn ref_action_sql(action: catalog::RefAction) -> Option<&'static str> {
    match action {
        catalog::RefAction::NoAction => None,
        catalog::RefAction::Restrict => Some("RESTRICT"),
        catalog::RefAction::Cascade => Some("CASCADE"),
        catalog::RefAction::SetNull => Some("SET NULL"),
    }
}

/// SHOW CREATE TABLE: reconstruct the DDL from the catalog definition.
pub async fn show_create_table(db: &Session, name: &str) -> Result<QueryResult> {
    let def = catalog::load(db, name).await?;
    let declarations = catalog::load_declarations(db, name).await?;
    let mut lines: Vec<String> = Vec::new();
    for (i, c) in def.schema.columns.iter().enumerate() {
        let declaration = column_declaration(declarations.as_ref(), c, i);
        let meta = def.meta(i);
        let mut s = format!(
            "  `{}` {}",
            c.name,
            declaration.column_type.to_ascii_uppercase()
        );
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
    // CHECK and FOREIGN KEY constraints are enforced but used to be invisible
    // here, so a dump taken through SHOW CREATE TABLE silently dropped them and
    // schema-diff tools saw a table that did not match the one they had.
    for chk in &def.checks {
        lines.push(format!("  CHECK ({chk})"));
    }
    for fk in &def.foreign_keys {
        let cols = fk
            .columns
            .iter()
            .map(|&i| format!("`{}`", def.schema.columns[i].name))
            .collect::<Vec<_>>()
            .join(", ");
        let ref_cols = fk
            .ref_columns
            .iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut line = format!(
            "  CONSTRAINT `{}` FOREIGN KEY ({cols}) REFERENCES `{}` ({ref_cols})",
            fk.name, fk.ref_table
        );
        // MySQL omits the default NO ACTION, so only referential actions that
        // were actually asked for are echoed back.
        if let Some(action) = ref_action_sql(fk.on_delete) {
            line.push_str(&format!(" ON DELETE {action}"));
        }
        if let Some(action) = ref_action_sql(fk.on_update) {
            line.push_str(&format!(" ON UPDATE {action}"));
        }
        lines.push(line);
    }
    let ddl = format!("CREATE TABLE `{name}` (\n{}\n)", lines.join(",\n"));
    let schema = Schema::new(vec![
        ColumnDef {
            name: "Table".into(),
            ty: ColumnType::Text,
            nullable: false,
            collation: elyra_core::Collation::Ci,
            qualifier: Vec::new(),
            result_metadata: Default::default(),
        },
        ColumnDef {
            name: "Create Table".into(),
            ty: ColumnType::Text,
            nullable: false,
            collation: elyra_core::Collation::Ci,
            qualifier: Vec::new(),
            result_metadata: Default::default(),
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
                qualifier: Vec::new(),
                result_metadata: Default::default(),
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
        let [schema, table] = name.0.as_slice() else {
            return None;
        };
        let table = table.value.to_ascii_lowercase();
        if schema.value.eq_ignore_ascii_case("information_schema") {
            return Some(table);
        }
        // Expose a few `mysql.*` catalog tables (prefixed so the virtual-
        // table provider can tell them apart).
        if schema.value.eq_ignore_ascii_case("mysql") {
            return Some(format!("mysql.{table}"));
        }
    }
    None
}

/// Virtual relations exposed through the `information_schema` catalog.
///
/// Keeping this list next to the provider lets `TABLES` and `COLUMNS` describe
/// the same capability surface that the planner accepts.
const INFORMATION_SCHEMA_VIEWS: &[&str] = &[
    "tables",
    "columns",
    "statistics",
    "key_column_usage",
    "referential_constraints",
    "table_constraints",
    "check_constraints",
    "column_statistics",
    "partitions",
    "engines",
    "triggers",
    "routines",
    "views",
    "events",
    "schemata",
    "collation_character_set_applicability",
];

const COLUMN_PRIVILEGES: &str = "select,insert,update,references";

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

/// MySQL generates names for unnamed CHECK constraints from the table name and
/// a one-based ordinal. ElyraSQL stores only their expressions, so derive the
/// same stable name whenever catalog rows are materialised.
fn check_constraint_name(table_name: &str, ordinal: usize) -> String {
    format!("{table_name}_chk_{}", ordinal + 1)
}

/// The unique key targeted by a foreign key, if the referenced table still has
/// a matching primary or unique key. Older catalogs may contain a foreign key
/// that predates this validation, in which case the standard metadata is NULL.
fn referenced_constraint_name<'a>(
    definition: &'a TableDef,
    referenced_columns: &[String],
) -> Option<&'a str> {
    let matches_columns = |columns: &[usize]| {
        columns.len() == referenced_columns.len()
            && columns
                .iter()
                .zip(referenced_columns)
                .all(|(&index, name)| {
                    predicate::identifier_eq(&definition.schema.columns[index].name, name)
                })
    };
    if matches_columns(&definition.pk_cols) {
        Some("PRIMARY")
    } else {
        definition
            .indexes
            .iter()
            .find(|index| index.unique && matches_columns(&index.cols))
            .map(|index| index.name.as_str())
    }
}

/// Whether a predicate explicitly requests rows describing the virtual catalog
/// itself. This keeps existing unscoped catalog queries limited to persisted
/// tables while allowing clients to probe `TABLE_SCHEMA = 'information_schema'`.
fn requests_information_schema_rows(expr: &Expr) -> bool {
    fn table_schema_reference(expr: &Expr) -> bool {
        match expr {
            Expr::Identifier(identifier) => identifier.value.eq_ignore_ascii_case("table_schema"),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .is_some_and(|identifier| identifier.value.eq_ignore_ascii_case("table_schema")),
            _ => false,
        }
    }

    fn information_schema_literal(expr: &Expr) -> bool {
        matches!(
            literal_value(expr),
            Some(Value::Text(value)) if value.eq_ignore_ascii_case("information_schema")
        )
    }

    match expr {
        Expr::Nested(expr) => requests_information_schema_rows(expr),
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::And | sqlparser::ast::BinaryOperator::Or,
            right,
        } => requests_information_schema_rows(left) || requests_information_schema_rows(right),
        Expr::BinaryOp {
            left,
            op: sqlparser::ast::BinaryOperator::Eq,
            right,
        } => {
            (table_schema_reference(left) && information_schema_literal(right))
                || (table_schema_reference(right) && information_schema_literal(left))
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => table_schema_reference(expr) && list.iter().any(information_schema_literal),
        _ => false,
    }
}

fn information_schema_schema(view: &str) -> Result<Schema> {
    let text = |name: &str| ColumnDef {
        name: name.to_owned(),
        ty: ColumnType::Text,
        nullable: true,
        collation: elyra_core::Collation::Ci,
        qualifier: Vec::new(),
        result_metadata: Default::default(),
    };
    let int = |name: &str| ColumnDef {
        name: name.to_owned(),
        ty: ColumnType::Int,
        nullable: true,
        collation: elyra_core::Collation::Ci,
        qualifier: Vec::new(),
        result_metadata: Default::default(),
    };
    let text_columns = |names: &[&str]| Schema::new(names.iter().map(|name| text(name)).collect());

    let schema = match view {
        "tables" => Schema::new(vec![
            text("TABLE_SCHEMA"),
            text("TABLE_NAME"),
            text("TABLE_TYPE"),
            text("ENGINE"),
            int("TABLE_ROWS"),
            int("DATA_LENGTH"),
            int("INDEX_LENGTH"),
            text("TABLE_COMMENT"),
            text("TABLE_COLLATION"),
            int("AUTO_INCREMENT"),
            text("CREATE_OPTIONS"),
        ]),
        "columns" => Schema::new(vec![
            text("TABLE_SCHEMA"),
            text("TABLE_NAME"),
            text("COLUMN_NAME"),
            int("ORDINAL_POSITION"),
            text("COLUMN_DEFAULT"),
            text("IS_NULLABLE"),
            text("DATA_TYPE"),
            int("CHARACTER_MAXIMUM_LENGTH"),
            int("NUMERIC_PRECISION"),
            int("NUMERIC_SCALE"),
            text("COLUMN_TYPE"),
            text("COLUMN_KEY"),
            text("EXTRA"),
            text("COLLATION_NAME"),
            text("COLUMN_COMMENT"),
            text("GENERATION_EXPRESSION"),
            text("CHARACTER_SET_NAME"),
            text("PRIVILEGES"),
        ]),
        "statistics" => Schema::new(vec![
            text("TABLE_SCHEMA"),
            text("TABLE_NAME"),
            int("NON_UNIQUE"),
            text("INDEX_NAME"),
            int("SEQ_IN_INDEX"),
            text("COLUMN_NAME"),
            text("COLLATION"),
            int("CARDINALITY"),
            int("SUB_PART"),
            text("NULLABLE"),
            text("INDEX_TYPE"),
        ]),
        "key_column_usage" => Schema::new(vec![
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
        ]),
        "referential_constraints" => Schema::new(vec![
            text("CONSTRAINT_CATALOG"),
            text("CONSTRAINT_SCHEMA"),
            text("CONSTRAINT_NAME"),
            text("UNIQUE_CONSTRAINT_CATALOG"),
            text("UNIQUE_CONSTRAINT_SCHEMA"),
            text("UNIQUE_CONSTRAINT_NAME"),
            text("MATCH_OPTION"),
            text("UPDATE_RULE"),
            text("DELETE_RULE"),
            text("TABLE_NAME"),
            text("REFERENCED_TABLE_NAME"),
        ]),
        "table_constraints" => Schema::new(vec![
            text("CONSTRAINT_CATALOG"),
            text("CONSTRAINT_SCHEMA"),
            text("CONSTRAINT_NAME"),
            text("TABLE_SCHEMA"),
            text("TABLE_NAME"),
            text("CONSTRAINT_TYPE"),
            text("ENFORCED"),
        ]),
        "check_constraints" => Schema::new(vec![
            text("CONSTRAINT_CATALOG"),
            text("CONSTRAINT_SCHEMA"),
            text("CONSTRAINT_NAME"),
            text("CHECK_CLAUSE"),
        ]),
        "column_statistics" => Schema::new(vec![
            text("TABLE_NAME"),
            text("COLUMN_NAME"),
            int("NDV"),
            int("NULLS"),
            text("MIN_VALUE"),
            text("MAX_VALUE"),
            text("HISTOGRAM"),
        ]),
        "partitions" => Schema::new(vec![
            text("TABLE_NAME"),
            text("PARTITION_NAME"),
            text("PARTITION_METHOD"),
            text("PARTITION_EXPRESSION"),
            text("PARTITION_DESCRIPTION"),
        ]),
        "mysql.user" => text_columns(&[
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
        ]),
        "mysql.db" => text_columns(&["Host", "Db", "User", "Select_priv", "Insert_priv"]),
        "engines" => text_columns(&[
            "ENGINE",
            "SUPPORT",
            "COMMENT",
            "TRANSACTIONS",
            "XA",
            "SAVEPOINTS",
        ]),
        "triggers" => text_columns(&[
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
        ]),
        "routines" => text_columns(&[
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
            "DTD_IDENTIFIER",
        ]),
        "views" => text_columns(&[
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "VIEW_DEFINITION",
            "CHECK_OPTION",
            "IS_UPDATABLE",
            "DEFINER",
            "SECURITY_TYPE",
            "CHARACTER_SET_CLIENT",
            "COLLATION_CONNECTION",
        ]),
        "events" => text_columns(&[
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
        ]),
        "schemata" => text_columns(&[
            "CATALOG_NAME",
            "SCHEMA_NAME",
            "DEFAULT_CHARACTER_SET_NAME",
            "DEFAULT_COLLATION_NAME",
            "SQL_PATH",
        ]),
        "collation_character_set_applicability" => {
            text_columns(&["COLLATION_NAME", "CHARACTER_SET_NAME"])
        }
        other => {
            return Err(Error::Unsupported(format!(
                "information_schema.{other} is not available"
            )))
        }
    };
    Ok(schema)
}

/// Build the rows of an `information_schema` view (`tables` or `columns`).
///
/// Virtual-catalog rows are only needed for explicit capability probes. Keeping
/// them scoped avoids changing historical unqualified virtual-view queries.
async fn information_schema(
    db: &Session,
    view: &str,
    include_catalog_rows: bool,
) -> Result<(Schema, Vec<Vec<Value>>)> {
    let schema = information_schema_schema(view)?;
    let names = catalog::list_tables(db).await?;
    let database = db.database();
    match view {
        "tables" => {
            let mut rows = Vec::with_capacity(names.len() + INFORMATION_SCHEMA_VIEWS.len());
            for n in names {
                let def = catalog::load(db, &n).await?;
                let table_rows = match catalog::load_stats(db, &n).await? {
                    Some(s) => Value::Int(s.rows as i64),
                    None => Value::Null,
                };
                let auto_increment = if (0..def.schema.columns.len())
                    .any(|column_index| def.meta(column_index).auto_increment)
                {
                    Value::Int(read_autoinc(db, &n).await?.saturating_add(1))
                } else {
                    Value::Null
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
                    auto_increment,
                    Value::Text(String::new()),
                ]);
            }
            if include_catalog_rows {
                rows.extend(INFORMATION_SCHEMA_VIEWS.iter().map(|view| {
                    vec![
                        Value::Text("information_schema".into()),
                        Value::Text(view.to_ascii_uppercase()),
                        Value::Text("SYSTEM VIEW".into()),
                        Value::Text("MEMORY".into()),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Text(String::new()),
                        Value::Text("utf8mb4_0900_ai_ci".into()),
                        Value::Null,
                        Value::Text(String::new()),
                    ]
                }));
            }
            Ok((schema, rows))
        }
        "columns" => {
            let mut rows = Vec::new();
            for tname in names {
                let def = catalog::load(db, &tname).await?;
                let declarations = catalog::load_declarations(db, &tname).await?;
                for (i, c) in def.schema.columns.iter().enumerate() {
                    let declaration = column_declaration(declarations.as_ref(), c, i);
                    let meta = def.meta(i);
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
                        Value::Text(declaration.data_type.clone()),
                        optional_u64_value(declaration.character_maximum_length),
                        optional_u64_value(declaration.numeric_precision),
                        optional_u64_value(declaration.numeric_scale),
                        Value::Text(declaration.column_type.clone()),
                        Value::Text(column_key(&def, i).into()),
                        Value::Text(column_extra(&meta).into()),
                        collation,
                        Value::Text(String::new()),
                        match &meta.generated {
                            Some(g) => Value::Text(g.clone()),
                            None => Value::Text(String::new()),
                        },
                        charset,
                        Value::Text(COLUMN_PRIVILEGES.into()),
                    ]);
                }
            }
            if include_catalog_rows {
                for view in INFORMATION_SCHEMA_VIEWS {
                    let virtual_schema = information_schema_schema(view)?;
                    for (ordinal, column) in virtual_schema.columns.iter().enumerate() {
                        let ty = column.ty.display_name();
                        let is_text = matches!(column.ty, ColumnType::Text | ColumnType::Json);
                        let collation = match (is_text, column.collation) {
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
                            Value::Text("information_schema".into()),
                            Value::Text(view.to_ascii_uppercase()),
                            Value::Text(column.name.clone()),
                            Value::Int(ordinal as i64 + 1),
                            Value::Null,
                            Value::Text(if column.nullable { "YES" } else { "NO" }.into()),
                            Value::Text(ty.clone()),
                            Value::Text(ty),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            collation,
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            charset,
                            Value::Text(COLUMN_PRIVILEGES.into()),
                        ]);
                    }
                }
            }
            Ok((schema, rows))
        }
        "statistics" => {
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
            let mut rows = Vec::new();
            for table_name in names {
                let def = catalog::load(db, &table_name).await?;
                for foreign_key in &def.foreign_keys {
                    let unique_constraint = match catalog::load(db, &foreign_key.ref_table).await {
                        Ok(referenced) => {
                            referenced_constraint_name(&referenced, &foreign_key.ref_columns)
                                .map(str::to_owned)
                        }
                        // A dropped or not-yet-created parent must not make the
                        // whole metadata view unreadable. Its unique-key fields
                        // are unknown, but the foreign-key row is still useful.
                        Err(Error::Catalog(_)) => None,
                        Err(error) => return Err(error),
                    };
                    let unique_catalog = unique_constraint
                        .as_ref()
                        .map(|_| Value::Text("def".into()))
                        .unwrap_or(Value::Null);
                    let unique_schema = unique_constraint
                        .as_ref()
                        .map(|_| Value::Text(database.clone()))
                        .unwrap_or(Value::Null);
                    let unique_name = unique_constraint.map(Value::Text).unwrap_or(Value::Null);
                    rows.push(vec![
                        Value::Text("def".into()),
                        Value::Text(database.clone()),
                        Value::Text(foreign_key.name.clone()),
                        unique_catalog,
                        unique_schema,
                        unique_name,
                        Value::Text("NONE".into()),
                        Value::Text(ref_action_name(foreign_key.on_update).into()),
                        Value::Text(ref_action_name(foreign_key.on_delete).into()),
                        Value::Text(table_name.clone()),
                        Value::Text(foreign_key.ref_table.clone()),
                    ]);
                }
            }
            Ok((schema, rows))
        }
        "table_constraints" => {
            let mut rows = Vec::new();
            for table_name in names {
                let def = catalog::load(db, &table_name).await?;
                let mut push = |name: &str, kind: &str| {
                    rows.push(vec![
                        Value::Text("def".into()),
                        Value::Text(database.clone()),
                        Value::Text(name.into()),
                        Value::Text(database.clone()),
                        Value::Text(table_name.clone()),
                        Value::Text(kind.into()),
                        Value::Text("YES".into()),
                    ]);
                };
                if def.has_pk() {
                    push("PRIMARY", "PRIMARY KEY");
                }
                for index in def.indexes.iter().filter(|index| index.unique) {
                    push(&index.name, "UNIQUE");
                }
                for foreign_key in &def.foreign_keys {
                    push(&foreign_key.name, "FOREIGN KEY");
                }
                for ordinal in 0..def.checks.len() {
                    let name = check_constraint_name(&table_name, ordinal);
                    push(&name, "CHECK");
                }
            }
            Ok((schema, rows))
        }
        "check_constraints" => {
            let mut rows = Vec::new();
            for table_name in names {
                let def = catalog::load(db, &table_name).await?;
                rows.extend(def.checks.iter().enumerate().map(|(ordinal, expression)| {
                    vec![
                        Value::Text("def".into()),
                        Value::Text(database.clone()),
                        Value::Text(check_constraint_name(&table_name, ordinal)),
                        Value::Text(expression.clone()),
                    ]
                }));
            }
            Ok((schema, rows))
        }
        "column_statistics" => {
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
            Ok((schema, Vec::new()))
        }
        "engines" => {
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
                        Value::Text(String::new()),
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
            Ok((schema, Vec::new()))
        }
        "schemata" => {
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
        "collation_character_set_applicability" => {
            let rows = ["utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci", "utf8mb4_bin"]
                .into_iter()
                .map(|collation| vec![Value::Text(collation.into()), Value::Text("utf8mb4".into())])
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
        let (mut osch, orows) = aggregate::run(
            &schema,
            &projection,
            group_by,
            rows,
            db.group_concat_max_len(),
        )?;
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
    let (osch, out) = project_exprs(&select.projection, &schema, &rows, None)?;
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
    let declarations = (!ct.columns.is_empty())
        .then(|| {
            ct.columns
                .iter()
                .map(|column| declaration_from_data_type(&column.data_type))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let columns: Vec<ColumnDef> = if ct.columns.is_empty() {
        qschema
            .columns
            .iter()
            .map(|c| ColumnDef {
                name: column_name(c).to_owned(),
                ty: c.ty.clone(),
                nullable: true,
                collation: elyra_core::Collation::Ci,
                qualifier: Vec::new(),
                result_metadata: Default::default(),
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
                qualifier: Vec::new(),
                result_metadata: Default::default(),
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
        storage_generation: 0,
    };
    let mut puts = vec![(catalog_key(name), def.encode()?)];
    if let Some(declarations) = declarations.as_ref() {
        puts.push((
            catalog::coldecl_key(name),
            bincode::serialize(&catalog::ColumnDeclarations {
                columns: declarations.clone(),
            })
            .map_err(|e| Error::Storage(e.to_string()))?,
        ));
    }
    let mut rowid = 0u64;
    for row in &rows {
        rowid += 1;
        let mut r = vec![Value::Null; def.schema.columns.len()];
        for (i, col) in def.schema.columns.iter().enumerate() {
            if let Some(v) = row.get(i) {
                r[i] = coerce(v.clone(), &col.ty, &col.name)?;
            }
        }
        if db.strict_sql_mode() {
            if let Some(declarations) = declarations.as_ref() {
                for (i, declaration) in declarations.iter().enumerate() {
                    let (Some(value), Some(column)) = (r.get(i), def.schema.columns.get(i)) else {
                        continue;
                    };
                    let row_number = usize::try_from(rowid).unwrap_or(usize::MAX);
                    check_declared_character_length(declaration, value, &column.name, row_number)?;
                }
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
    let def = catalog::load(db, name).await?;
    let storage_name = def.storage_name();
    let mut deletes = vec![rowid_key(name), autoinc_key(name)];
    for prefix in [
        def.data_prefix(),
        index_table_prefix(&storage_name),
        indexnull_table_prefix(&storage_name),
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
    if !db.in_txn() {
        if let [AlterTableOperation::AddConstraint(sqlparser::ast::TableConstraint::PrimaryKey {
            columns,
            ..
        })] = ops
        {
            return alter_add_primary_key_shadow(db, name, columns).await;
        }
    }
    // ALTER helpers persist catalog, row, and index changes independently. Keep
    // the whole statement behind a private checkpoint so a later operation
    // cannot expose changes made by an earlier one.
    let implicit_transaction = !db.in_txn();
    if implicit_transaction {
        db.begin()?;
    }
    // ALTER helpers that rewrite a table scan a snapshot before staging new
    // keys. Validate those scanned ranges at commit so a concurrent write
    // cannot survive in an obsolete row/key layout.
    db.require_serializable_validation()?;
    // An implicit ALTER owns its whole transaction, so an error can discard it
    // directly. Checkpoint logging would clone every rewritten key solely to
    // support a partial rollback that can never be needed. Explicit user
    // transactions still need a checkpoint to preserve earlier statements.
    let checkpoint = if implicit_transaction {
        None
    } else {
        Some(db.transaction_checkpoint()?)
    };

    match alter_table_inner(db, name, ops).await {
        Ok(result) => {
            if let Some(checkpoint) = checkpoint {
                db.release_transaction_checkpoint(checkpoint)?;
            }
            if implicit_transaction {
                db.commit().await?;
            }
            Ok(result)
        }
        Err(error) => {
            if let Some(checkpoint) = checkpoint {
                db.rollback_transaction_checkpoint(checkpoint)?;
            }
            if implicit_transaction {
                db.rollback();
            }
            Err(error)
        }
    }
}

fn rewrite_batch_rows() -> usize {
    std::env::var("ELYRASQL_REWRITE_BATCH_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(100_000)
        .clamp(1, 100_000)
}

fn generation_gc_key(table: &str, generation: u64) -> Vec<u8> {
    format!("meta::generation-gc::{table}::{generation:016x}").into_bytes()
}

const GENERATION_GC_PREFIX: &[u8] = b"meta::generation-gc::";

fn generation_gc_value(table: &str, generation: u64) -> Result<Vec<u8>> {
    bincode::serialize(&(table, generation)).map_err(|error| Error::Storage(error.to_string()))
}

async fn cleanup_generation(db: &elyra_storage::Db, table: &str, generation: u64) -> Result<()> {
    let prefixes = [
        catalog::data_prefix_generation(table, generation),
        catalog::index_table_prefix_generation(table, generation),
        catalog::indexnull_table_prefix_generation(table, generation),
    ];
    for prefix in prefixes {
        loop {
            let batch = db.scan_batch(prefix.clone(), None, 4096).await?;
            if batch.is_empty() {
                break;
            }
            db.commit(Vec::new(), batch.into_iter().map(|(key, _)| key).collect())
                .await?;
        }
    }
    db.commit(Vec::new(), vec![generation_gc_key(table, generation)])
        .await
}

pub(crate) async fn resume_generation_cleanup(db: &elyra_storage::Db) -> Result<()> {
    let mut cursor = None;
    loop {
        let markers = db
            .scan_batch(GENERATION_GC_PREFIX.to_vec(), cursor.clone(), 256)
            .await?;
        if markers.is_empty() {
            break;
        }
        let last = markers.len() < 256;
        cursor = markers.last().map(|(key, _)| key.clone());
        for (_, value) in markers {
            let (table, generation): (String, u64) =
                bincode::deserialize(&value).map_err(|error| Error::Storage(error.to_string()))?;
            cleanup_generation(db, &table, generation).await?;
        }
        if last {
            break;
        }
    }
    Ok(())
}

async fn alter_add_primary_key_shadow(
    db: &Session,
    name: &ObjectName,
    columns: &[Ident],
) -> Result<QueryResult> {
    let table = stored_table_ident(db, name)?;
    db.begin()?;
    let result = async {
        let mut definition = catalog::load(db, &table).await?;
        let old_generation = definition.storage_generation;
        let new_generation = old_generation
            .checked_add(1)
            .ok_or_else(|| Error::Storage("table generation exhausted".into()))?;
        let old_definition = definition.clone();

        if definition.has_pk() {
            return Err(Error::Query("multiple primary keys are not allowed".into()));
        }
        if columns.is_empty() {
            return Err(Error::Query(
                "ALTER TABLE ADD PRIMARY KEY requires at least one column".into(),
            ));
        }
        let mut primary_columns = Vec::with_capacity(columns.len());
        for column in columns {
            let index = definition
                .schema
                .columns
                .iter()
                .position(|candidate| predicate::identifier_eq(&candidate.name, &column.value))
                .ok_or_else(|| Error::Catalog(format!("unknown column: {column}")))?;
            if primary_columns.contains(&index) {
                return Err(Error::Query(format!(
                    "duplicate column '{}' in primary key",
                    column.value
                )));
            }
            primary_columns.push(index);
        }
        definition.pk_cols = primary_columns;
        for &column in &definition.pk_cols {
            definition.schema.columns[column].nullable = false;
        }
        definition.storage_generation = new_generation;

        // The table write sequence and catalog value form a compact validation
        // token for the snapshot used to build the shadow generation.
        db.lock_keys(&[wcount_key(&table), catalog_key(&table)]);

        // Remove an unreachable generation left by an interrupted earlier
        // attempt before reusing its generation number.
        cleanup_generation(&db.raw_db(), &table, new_generation).await?;

        let source_prefix = old_definition.data_prefix();
        let target_prefix = definition.data_prefix();
        let primary_collations = definition.pk_collations();
        let batch_rows = rewrite_batch_rows();
        let mut cursor = None;
        let mut cancel = db.cancel_check();
        let mut pacer = Pacer::new();
        loop {
            let batch = db
                .scan_batch(source_prefix.clone(), cursor.clone(), batch_rows)
                .await?;
            if batch.is_empty() {
                break;
            }
            let last = batch.len() < batch_rows;
            cursor = batch.last().map(|(key, _)| key.clone());
            let mut new_entries = Vec::new();
            let mut auxiliary_entries = Vec::new();
            for (_, encoded_row) in batch {
                cancel.tick()?;
                pacer.tick().await;
                let row = rowdec::decode_row(&encoded_row)?;
                if definition
                    .pk_cols
                    .iter()
                    .any(|&column| row[column].is_null())
                {
                    return Err(Error::Query(
                        "primary key columns cannot contain NULL".into(),
                    ));
                }
                let clustered =
                    keyenc::encode_columns_coll(&row, &definition.pk_cols, &primary_collations)?;
                let mut data_key = target_prefix.clone();
                data_key.extend_from_slice(&clustered);
                let (non_unique, unique) =
                    index::partition_entries_for_row(&definition, &row, &data_key)?;
                new_entries.push((data_key, encoded_row));
                new_entries.extend(unique);
                auxiliary_entries.extend(non_unique);
            }
            db.raw_db()
                .commit_insert(new_entries, auxiliary_entries, Vec::new())
                .await?;
            if last {
                break;
            }
        }

        let marker = generation_gc_key(&table, old_generation);
        db.commit_write(
            vec![
                (catalog_key(&table), definition.encode()?),
                (
                    catalog::generation_key(&table),
                    new_generation.to_le_bytes().to_vec(),
                ),
                (marker, generation_gc_value(&table, old_generation)?),
                bump_wcount(db, &table).await?,
            ],
            vec![rowid_key(&table)],
        )
        .await?;
        db.commit().await?;
        Ok((old_generation, new_generation))
    }
    .await;

    match result {
        Ok((old_generation, _new_generation)) => {
            let cleanup_db = db.raw_db().clone();
            let cleanup_table = table.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    cleanup_generation(&cleanup_db, &cleanup_table, old_generation).await
                {
                    tracing::warn!(%error, table = %cleanup_table, "generation cleanup failed");
                }
            });
            Ok(QueryResult::Affected(0))
        }
        Err(error) => {
            db.rollback();
            // A failed build never made the target generation reachable.
            if let Ok(definition) = catalog::load(db, &table).await {
                if let Some(new_generation) = definition.storage_generation.checked_add(1) {
                    let _ = cleanup_generation(&db.raw_db(), &table, new_generation).await;
                }
            }
            Err(error)
        }
    }
}

async fn alter_table_inner(
    db: &Session,
    name: &ObjectName,
    ops: &[AlterTableOperation],
) -> Result<QueryResult> {
    let tname = stored_table_ident(db, name)?;

    // Qualifier errors must be discovered before any ALTER operation commits.
    // Several ALTER helpers persist independently, so validating lazily inside
    // the loop could leave an earlier column or index behind after a later FK
    // or rename target is rejected.
    for op in ops {
        match op {
            AlterTableOperation::RenameTable { table_name } => {
                stored_table_ident(db, table_name)?;
            }
            AlterTableOperation::AddConstraint(TableConstraint::ForeignKey {
                foreign_table,
                ..
            }) => {
                stored_table_ident(db, foreign_table)?;
            }
            _ => {}
        }
    }

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
                    .position(|c| predicate::identifier_eq(&c.name, &old_column_name.value))
                    .ok_or_else(|| Error::Catalog(format!("unknown column: {old_column_name}")))?;
                def.schema.columns[i].name = new_column_name.value.clone();
            }
            AlterTableOperation::RenameTable { table_name } => {
                let new = stored_table_ident(db, table_name)?;
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
                    let ref_table = stored_table_ident(db, foreign_table)?;
                    let mut fk_cols = Vec::new();
                    for ident in cols {
                        let i = def
                            .schema
                            .columns
                            .iter()
                            .position(|c| predicate::identifier_eq(&c.name, &ident.value))
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
                        ref_table,
                        ref_columns: referred_columns.iter().map(|i| i.value.clone()).collect(),
                        on_delete: map_ref_action(on_delete),
                        on_update: map_ref_action(on_update),
                    });
                    continue;
                }
                let (idx_name, columns, unique) = match tc {
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
                    TC::PrimaryKey { columns, .. } => {
                        alter_add_primary_key(db, &mut def, columns).await?;
                        continue;
                    }
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

/// Add a clustered primary key to a rowid table, re-keying every stored row and
/// rebuilding secondary-index entries against the new clustered keys.
async fn alter_add_primary_key(db: &Session, def: &mut TableDef, columns: &[Ident]) -> Result<()> {
    if def.has_pk() {
        return Err(Error::Query("multiple primary keys are not allowed".into()));
    }
    if columns.is_empty() {
        return Err(Error::Query(
            "ALTER TABLE ADD PRIMARY KEY requires at least one column".into(),
        ));
    }

    let mut pk_cols = Vec::with_capacity(columns.len());
    for column in columns {
        let index = def
            .schema
            .columns
            .iter()
            .position(|candidate| predicate::identifier_eq(&candidate.name, &column.value))
            .ok_or_else(|| Error::Catalog(format!("unknown column: {column}")))?;
        if pk_cols.contains(&index) {
            return Err(Error::Query(format!(
                "duplicate column '{}' in primary key",
                column.value
            )));
        }
        pk_cols.push(index);
    }

    let old_def = def.clone();
    def.pk_cols = pk_cols;
    for &column in &def.pk_cols {
        def.schema.columns[column].nullable = false;
    }

    let mut puts = vec![(catalog_key(&def.name), def.encode()?)];
    let mut deletes = Vec::new();
    let mut clustered_keys = std::collections::HashSet::new();
    let pk_collations = def.pk_collations();
    let clustered_prefix = def.data_prefix();
    let rewrite_budget = db.transaction_write_budget_remaining();
    let mut rewrite_bytes = puts
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum();
    let prefix = old_def.data_prefix();
    let mut cursor = None;
    loop {
        let batch = db.scan_batch(prefix.clone(), cursor.clone(), 4096).await?;
        if batch.is_empty() {
            break;
        }
        let last = batch.len() < 4096;
        cursor = batch.last().map(|(key, _)| key.clone());
        for (old_key, encoded_row) in batch {
            let row = rowdec::decode_row(&encoded_row)?;
            if def.pk_cols.iter().any(|&column| row[column].is_null()) {
                return Err(Error::Query(
                    "primary key columns cannot contain NULL".into(),
                ));
            }
            let clustered = keyenc::encode_columns_coll(&row, &def.pk_cols, &pk_collations)?;
            if !clustered_keys.insert(clustered.clone()) {
                return Err(Error::Duplicate("duplicate primary key".into()));
            }
            let mut new_key = clustered_prefix.clone();
            new_key.extend_from_slice(&clustered);
            let mut row_deletes = index::entry_keys_for_row(&old_def, &row, &old_key)?;
            if new_key != old_key {
                row_deletes.push(old_key);
            }
            let mut row_puts = vec![(new_key.clone(), encoded_row)];
            row_puts.extend(index::entries_for_row(def, &row, &new_key)?);
            let additional = clustered.len()
                + row_deletes.iter().map(Vec::len).sum::<usize>()
                + row_puts
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>();
            rewrite_bytes = reserve_alter_rewrite_bytes(rewrite_bytes, additional, rewrite_budget)?;
            deletes.extend(row_deletes);
            puts.extend(row_puts);
        }
        if last {
            break;
        }
    }

    deletes.push(rowid_key(&def.name));
    puts.push(bump_wcount(db, &def.name).await?);
    db.commit_write(puts, deletes).await
}

fn reserve_alter_rewrite_bytes(current: usize, additional: usize, budget: usize) -> Result<usize> {
    let total = current.saturating_add(additional);
    if total > budget {
        return Err(Error::Query(format!(
            "ALTER TABLE rewrite exceeded {budget} bytes; raise \
             ELYRASQL_TXN_MAX_BYTES to allow a larger rewrite"
        )));
    }
    Ok(total)
}

#[cfg(test)]
mod alter_rewrite_budget_tests {
    use super::reserve_alter_rewrite_bytes;

    #[test]
    fn rejects_before_the_rewrite_buffer_exceeds_its_budget() {
        assert_eq!(reserve_alter_rewrite_bytes(60, 40, 100).unwrap(), 100);
        let error = reserve_alter_rewrite_bytes(60, 41, 100).unwrap_err();
        assert!(error.to_string().contains("rewrite exceeded 100 bytes"));
    }
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
        .position(|c| predicate::identifier_eq(&c.name, old))
        .ok_or_else(|| Error::Catalog(format!("unknown column: {old}")))?;

    let new_ty = map_type(data_type)?;
    let declared_type = declaration_from_data_type(data_type)?;
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
    let check_name = new_name.unwrap_or(&def.schema.columns[i].name);
    check_existing_character_length(db, def, i, &declared_type, check_name).await?;
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
    adjust_widths(db, &def.name, i, WidthOp::Set(declared_int_bits(data_type))).await?;
    adjust_declarations(db, &def.name, i, DeclarationOp::Set(declared_type)).await?;
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
        .position(|c| predicate::identifier_eq(&c.name, name))
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
            let declared_type = declaration_from_data_type(data_type)?;
            let old_ty = def.schema.columns[i].ty.clone();
            if def.pk_cols.contains(&i) && new_ty != old_ty {
                return Err(Error::Unsupported(
                    "cannot change the type of a primary key column".into(),
                ));
            }
            check_existing_character_length(
                db,
                def,
                i,
                &declared_type,
                &def.schema.columns[i].name,
            )
            .await?;
            def.schema.columns[i].ty = new_ty.clone();
            if new_ty != old_ty {
                recoerce_column(db, def, i).await?;
            }
            adjust_widths(db, &def.name, i, WidthOp::Set(declared_int_bits(data_type))).await?;
            adjust_declarations(db, &def.name, i, DeclarationOp::Set(declared_type)).await?;
        }
        other => {
            return Err(Error::Unsupported(format!(
                "ALTER COLUMN operation not supported: {other}"
            )))
        }
    }
    Ok(())
}

/// Reject an ALTER that would leave existing character data too wide for its
/// new declaration. The calling ALTER runs behind a private checkpoint, so an
/// error leaves both rows and catalog metadata unchanged.
async fn check_existing_character_length(
    db: &Session,
    def: &TableDef,
    column: usize,
    declaration: &catalog::ColumnDeclaration,
    column_name: &str,
) -> Result<()> {
    if !db.strict_sql_mode() {
        return Ok(());
    }
    let rows = collect_matches(db, def, None, None).await?;
    for (row_number, (_, row)) in rows.iter().enumerate() {
        if let Some(value) = row.get(column) {
            check_declared_character_length(declaration, value, column_name, row_number + 1)?;
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

/// Keep the declared-width list aligned with a table's columns as `ALTER`
/// changes them. Absent list = a table from before widths were recorded; leave
/// it absent so its behaviour does not change under the user.
async fn adjust_widths(db: &Session, table: &str, at: usize, op: WidthOp) -> Result<()> {
    let Some(mut widths) = catalog::load_widths(db, table).await? else {
        return Ok(());
    };
    match op {
        WidthOp::Insert(bits) => {
            if at <= widths.bits.len() {
                widths.bits.insert(at, bits);
            }
        }
        WidthOp::Remove => {
            if at < widths.bits.len() {
                widths.bits.remove(at);
            }
        }
        WidthOp::Set(bits) => {
            if at < widths.bits.len() {
                widths.bits[at] = bits;
            }
        }
    }
    db.commit_write(
        vec![(
            catalog::colwidth_key(table),
            bincode::serialize(&widths).map_err(|e| Error::Storage(e.to_string()))?,
        )],
        vec![],
    )
    .await
}

enum WidthOp {
    Insert(Option<u8>),
    Remove,
    Set(Option<u8>),
}

/// Keep declared-type metadata positional with schema columns. As with integer
/// widths, a missing sidecar identifies a catalog written before this feature;
/// do not create one during ALTER because that would fabricate declarations the
/// old catalog never recorded.
async fn adjust_declarations(
    db: &Session,
    table: &str,
    at: usize,
    op: DeclarationOp,
) -> Result<()> {
    let Some(mut declarations) = catalog::load_declarations(db, table).await? else {
        return Ok(());
    };
    match op {
        DeclarationOp::Insert(declaration) => {
            if at <= declarations.columns.len() {
                declarations.columns.insert(at, declaration);
            }
        }
        DeclarationOp::Remove => {
            if at < declarations.columns.len() {
                declarations.columns.remove(at);
            }
        }
        DeclarationOp::Set(declaration) => {
            if let Some(existing) = declarations.columns.get_mut(at) {
                *existing = declaration;
            }
        }
    }
    db.commit_write(
        vec![(
            catalog::coldecl_key(table),
            bincode::serialize(&declarations).map_err(|e| Error::Storage(e.to_string()))?,
        )],
        vec![],
    )
    .await
}

enum DeclarationOp {
    Insert(catalog::ColumnDeclaration),
    Remove,
    Set(catalog::ColumnDeclaration),
}

async fn alter_add_column(
    db: &Session,
    def: &mut TableDef,
    col: &sqlparser::ast::ColumnDef,
) -> Result<()> {
    // Two columns of the same name is not a state the rest of the engine can
    // represent: name resolution picks whichever it finds first, so the second
    // is unreachable while still occupying a slot in every stored row, and the
    // DDL we emit for the table can no longer be replayed. A migration runner
    // retrying a partly applied migration is the usual way to get here, and it
    // needs the same 1060 it would get from MySQL.
    if def
        .schema
        .columns
        .iter()
        .any(|existing| predicate::identifier_eq(&existing.name, &col.name.value))
    {
        return Err(Error::Duplicate(format!(
            "duplicate column name '{}'",
            col.name.value
        )));
    }
    let ty = map_type(&col.data_type)?;
    let declared_type = declaration_from_data_type(&col.data_type)?;
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
    if db.strict_sql_mode() {
        check_declared_character_length(&declared_type, &default, &col.name.value, 1)?;
    }
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
        qualifier: Vec::new(),
        result_metadata: Default::default(),
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
                &def.storage_name(),
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
    adjust_widths(
        db,
        &def.name,
        def.schema.columns.len().saturating_sub(1),
        WidthOp::Insert(declared_int_bits(&col.data_type)),
    )
    .await?;
    adjust_declarations(
        db,
        &def.name,
        def.schema.columns.len().saturating_sub(1),
        DeclarationOp::Insert(declared_type),
    )
    .await?;
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
        .position(|c| predicate::identifier_eq(&c.name, name))
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
    let prefix = def.data_prefix();
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
    adjust_widths(db, &def.name, idx, WidthOp::Remove).await?;
    adjust_declarations(db, &def.name, idx, DeclarationOp::Remove).await?;
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
    let old_generation = def.storage_generation;
    let old_prefix = catalog::data_prefix_generation(&old, old_generation);
    let target_generation = db
        .get(catalog::generation_key(new))
        .await?
        .and_then(|bytes| bytes.as_slice().try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
        .max(old_generation);
    def.name = new.to_string();
    def.storage_generation = target_generation;
    for foreign_key in &mut def.foreign_keys {
        if foreign_key.ref_table.eq_ignore_ascii_case(&old) {
            foreign_key.ref_table = new.to_string();
        }
    }

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();

    // Re-key all data rows and rebuild their index entries under the new name.
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
            let mut new_key = def.data_prefix();
            new_key.extend_from_slice(clustered);
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
        catalog::index_table_prefix_generation(&old, old_generation),
        catalog::indexnull_table_prefix_generation(&old, old_generation),
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
    // Retain the old name's watermark. A deferred cleanup may still be
    // deleting an earlier generation, so a later CREATE with this name must
    // not reuse that physical keyspace.
    if old_generation != 0 {
        puts.push((
            catalog::generation_key(&old),
            old_generation.to_le_bytes().to_vec(),
        ));
    }
    puts.push((catalog_key(new), def.encode()?));
    if target_generation != 0 {
        puts.push((
            catalog::generation_key(new),
            target_generation.to_le_bytes().to_vec(),
        ));
    }
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
    for key in [
        catalog::colwidth_key as fn(&str) -> Vec<u8>,
        catalog::coldecl_key,
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
                .position(|d| predicate::identifier_eq(&d.name, c))
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
    let prefix = def.data_prefix();
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
    let table = stored_table_ident(db, &ci.table_name)?;
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
            .position(|c| predicate::identifier_eq(&c.name, col_name))
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
    let prefix = def.data_prefix();
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
    let storage_table = catalog::load(db, table).await?.storage_name();
    let mut keys = Vec::new();
    for index_name in index_names {
        for prefix in [
            index::index_scan_prefix(&storage_table, index_name),
            index::indexnull_scan_prefix(&storage_table, index_name),
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
    let name = stored_table_ident(db, &ins.table_name)?;
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
                    .position(|col| predicate::identifier_eq(&col.name, &c.value))
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
            let target = ins
                .table_alias
                .as_ref()
                .cloned()
                .or_else(|| ins.table_name.0.last().cloned())
                .ok_or_else(|| Error::Catalog("empty insert target".into()))?;
            let qualifier = canonical_relation_qualifier(db, Some(&ins.table_name), &target);
            let validation_schema = qualify_relation_schema(def.schema.clone(), &qualifier);
            let ctes = std::collections::HashMap::new();
            for a in assigns {
                validate_expression_column_references(
                    db,
                    &a.value,
                    &validation_schema,
                    None,
                    &ctes,
                )
                .await?;
                let col = match &a.target {
                    AssignmentTarget::ColumnName(n) => {
                        assignment_column_for_table(db, &ins.table_name, None, n)?
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
                    .position(|c| predicate::identifier_eq(&c.name, &col))
                    .ok_or_else(|| Error::UnknownColumn(col.clone()))?;
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
    let clustered_prefix = def.data_prefix();

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
                let need =
                    !provided[ai] || row[ai].is_null() || (is_zero && !db.no_auto_value_on_zero());
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
            let encoded = keyenc::encode_columns_coll(&row, &def.pk_cols, &pk_colls)?;
            let mut key = clustered_prefix.clone();
            key.extend_from_slice(&encoded);
            key
        } else {
            next_rowid += 1;
            let mut key = clustered_prefix.clone();
            key.extend_from_slice(&keyenc::encode_rowid(next_rowid));
            key
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
        check_widths_batch(db, &def, &built).await?;
        if db.foreign_key_checks() && !def.foreign_keys.is_empty() {
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
        // redb's B-tree writer benefits materially from monotonic key order.
        // These sets have order-independent semantics: every `new` key must be
        // unique and auxiliary index/counter keys contain no competing writes.
        new_puts.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        aux_puts.sort_unstable_by(|left, right| left.0.cmp(&right.0));
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
                let previous = std::mem::replace(&mut batch[pos].1, row);
                affected += replaced_row_count(&previous, &batch[pos].1);
            } else if on_dup {
                let previous = batch[pos].1.clone();
                batch[pos].1 = apply_dup(&previous, &row)?;
                affected += updated_row_count(&previous, &batch[pos].1);
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
            affected += if replace {
                replaced_row_count(&old_row, &new_row)
            } else {
                updated_row_count(&old_row, &new_row)
            };
            pos_of.insert(key.clone(), batch.len());
            batch.push((key, new_row));
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
    check_widths_batch(db, &def, &batch).await?;
    if db.foreign_key_checks() && !def.foreign_keys.is_empty() {
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

/// Affected-rows contribution of one `INSERT ... ON DUPLICATE KEY UPDATE` row
/// that landed on an existing row.
///
/// MySQL documents this exactly: 1 if the row was inserted as new, **2** if an
/// existing row was updated, and **0** if the existing row was set to the values
/// it already had. The 2 is the attempted insert plus the update, and it is how a
/// client tells an insert from an update.
fn updated_row_count(old_row: &[Value], new_row: &[Value]) -> u64 {
    if old_row == new_row {
        0
    } else {
        2
    }
}

/// Affected-rows contribution of one `REPLACE` row that landed on an existing row.
///
/// 2 when the stored row actually changed (the delete plus the insert), and 1
/// when it did not: MySQL reports 1 for a `REPLACE` whose replacement is
/// identical, because no delete is performed. Measured against MySQL 8.4 rather
/// than inferred -- the manual documents only the 1-or-2 pair.
fn replaced_row_count(old_row: &[Value], new_row: &[Value]) -> u64 {
    if old_row == new_row {
        1
    } else {
        2
    }
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
                        .position(|c| predicate::identifier_eq(&c.name, col))
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
        if matches!(c, '\'' | '"' | '`') {
            i = copy_quoted_segment(&cs, i, &mut out);
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
pub(crate) fn contains_uvar_reference(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut quote = None;
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if let Some(delimiter) = quote {
            if byte == b'\\' && delimiter != b'`' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if byte == delimiter {
                if bytes.get(i + 1) == Some(&delimiter) {
                    i += 2;
                    continue;
                }
                quote = None;
            }
            i += 1;
            continue;
        }

        if matches!(byte, b'\'' | b'"' | b'`') {
            quote = Some(byte);
            i += 1;
            continue;
        }
        if byte == b'@' {
            if bytes.get(i + 1) == Some(&b'@') {
                i += 2;
                continue;
            }
            if bytes
                .get(i + 1)
                .is_some_and(|next| next.is_ascii_alphanumeric() || *next == b'_')
            {
                return true;
            }
        }
        i += 1;
    }
    false
}

pub(crate) fn substitute_uvars(
    sql: &str,
    vars: &std::collections::HashMap<String, Value>,
) -> String {
    let cs: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if matches!(c, '\'' | '"' | '`') {
            i = copy_quoted_segment(&cs, i, &mut out);
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

/// Replace `@@session` system-variable references with this connection's
/// current values, leaving quoted segments untouched. The SQL frontend has no
/// session context, so resolving them before parsing keeps all expression paths
/// (literal SELECTs, filters, projections, and SET right-hand sides) consistent.
pub(crate) fn substitute_system_vars(sql: &str, resolve: impl Fn(&str) -> Value) -> String {
    let cs: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < cs.len() {
        let c = cs[i];
        if matches!(c, '\'' | '"' | '`') {
            i = copy_quoted_segment(&cs, i, &mut out);
            continue;
        }
        if c == '@' && cs.get(i + 1) == Some(&'@') {
            let start = i + 2;
            let mut end = start;
            while end < cs.len()
                && (cs[end].is_ascii_alphanumeric() || matches!(cs[end], '_' | '.'))
            {
                end += 1;
            }
            if end > start {
                let name = cs[start..end].iter().collect::<String>();
                out.push_str(&value_sql_literal(&resolve(&name)));
                i = end;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

pub(crate) fn copy_quoted_segment(chars: &[char], start: usize, out: &mut String) -> usize {
    let quote = chars[start];
    out.push(quote);
    let mut i = start + 1;
    while i < chars.len() {
        let current = chars[i];
        out.push(current);
        if current == '\\' && quote != '`' && i + 1 < chars.len() {
            out.push(chars[i + 1]);
            i += 2;
            continue;
        }
        if current == quote {
            if i + 1 < chars.len() && chars[i + 1] == quote {
                out.push(chars[i + 1]);
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    i
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
            .position(|c| predicate::identifier_eq(&c.name, col))
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
            .position(|c| predicate::identifier_eq(&c.name, &col))
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
/// Positions of `ref_cols` in the parent's schema, or `None` if any is unknown.
fn referenced_column_indexes(parent: &TableDef, ref_cols: &[String]) -> Option<Vec<usize>> {
    ref_cols
        .iter()
        .map(|name| {
            parent
                .schema
                .columns
                .iter()
                .position(|c| predicate::identifier_eq(&c.name, name))
        })
        .collect()
}

fn fk_probe_key(parent: &TableDef, ref_cols: &[String], vals: &[Value]) -> Result<Vec<u8>> {
    let name_match = |cols: &[usize]| {
        cols.len() == ref_cols.len()
            && cols
                .iter()
                .zip(ref_cols)
                .all(|(&i, rc)| predicate::identifier_eq(&parent.schema.columns[i].name, rc))
    };
    if !parent.pk_cols.is_empty() && name_match(&parent.pk_cols) {
        return Ok(data_key(
            &parent.storage_name(),
            &keyenc::encode_key_coll(vals, &parent.pk_collations())?,
        ));
    }
    for idx in &parent.indexes {
        if idx.unique && !idx.vector && name_match(&idx.cols) {
            return index::unique_probe_key(
                &parent.storage_name(),
                &idx.name,
                vals,
                &idx.col_collations,
            );
        }
    }
    Err(Error::Query(format!(
        "foreign key must reference the primary key or a unique index of '{}'",
        parent.name
    )))
}

/// Refuse values that do not fit their column's *declared* integer width.
///
/// Storage is 64-bit for every integer type, so this is the only thing standing
/// between `TINYINT` and a value MySQL would reject with 1264. Tables written
/// before widths were recorded have no entry and keep the old behaviour.
async fn check_widths_batch(
    db: &Session,
    def: &TableDef,
    batch: &[(Vec<u8>, Vec<Value>)],
) -> Result<()> {
    if let Some(widths) = catalog::load_widths(db, &def.name).await? {
        for (_, row) in batch {
            for (i, bits) in widths.bits.iter().enumerate() {
                let (Some(bits), Some(value), Some(column)) =
                    (*bits, row.get(i), def.schema.columns.get(i))
                else {
                    continue;
                };
                let unsigned = matches!(column.ty, ColumnType::UInt);
                let n: i128 = match value {
                    Value::Int(v) => *v as i128,
                    Value::UInt(v) => *v as i128,
                    _ => continue,
                };
                let (lo, hi) = int_bounds(bits, unsigned);
                if n < lo || n > hi {
                    return Err(Error::OutOfRange(format!(
                        "value {n} is out of range for column '{}'",
                        column.name
                    )));
                }
            }
        }
    }

    // In strict SQL mode, MySQL rejects over-long character input rather than
    // silently truncating it. A sidecar is absent for pre-existing catalogs,
    // so those tables deliberately preserve their historical behaviour.
    if !db.strict_sql_mode() {
        return Ok(());
    }
    let Some(declarations) = catalog::load_declarations(db, &def.name).await? else {
        return Ok(());
    };
    for (row_number, (_, row)) in batch.iter().enumerate() {
        for (i, declaration) in declarations.columns.iter().enumerate() {
            let (Some(value), Some(column)) = (row.get(i), def.schema.columns.get(i)) else {
                continue;
            };
            check_declared_character_length(declaration, value, &column.name, row_number + 1)?;
        }
    }
    Ok(())
}

/// Verify every foreign key of `def` for the rows in `batch`: each non-NULL
/// referencing tuple must exist in the parent (error 1452 otherwise).
async fn check_fk_batch(
    db: &Session,
    def: &TableDef,
    batch: &[(Vec<u8>, Vec<Value>)],
) -> Result<()> {
    for fk in &def.foreign_keys {
        // A self-referencing key can be satisfied by a row of this same
        // statement: MySQL checks each row as it is inserted, so a row may point
        // at one written *earlier in the batch* (or at itself). Batched dumps of
        // any `parent_id`-shaped table depend on this — without it a multi-row
        // INSERT of a hierarchy is refused however it is ordered.
        let parent_is_self = predicate::identifier_eq(&fk.ref_table, &def.name);
        let parent = if parent_is_self {
            def.clone()
        } else {
            catalog::load(db, &fk.ref_table).await?
        };
        let parent_key_cols = if parent_is_self {
            referenced_column_indexes(&parent, &fk.ref_columns)
        } else {
            None
        };

        let mut supplied: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut probes = Vec::new();
        for (_, row) in batch {
            // Record what this row offers as a parent before checking what it
            // needs, so a row that references itself (`(5, 5)`) is satisfied.
            if let Some(cols) = &parent_key_cols {
                let key_vals: Vec<Value> = cols.iter().map(|&i| row[i].clone()).collect();
                if !key_vals.iter().any(|v| v.is_null()) {
                    if let Ok(key) = fk_probe_key(&parent, &fk.ref_columns, &key_vals) {
                        supplied.insert(key);
                    }
                }
            }

            let vals: Vec<Value> = fk.columns.iter().map(|&i| row[i].clone()).collect();
            if vals.iter().any(|v| v.is_null()) {
                continue; // a NULL in the referencing tuple is allowed
            }
            let probe = fk_probe_key(&parent, &fk.ref_columns, &vals)?;
            if supplied.contains(&probe) {
                continue; // satisfied by an earlier row of this statement
            }
            probes.push(probe);
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

// Client SQL is checked before parsing, but stored views compose already-parsed
// queries during both validation and execution. Keep that recursion bounded,
// including cyclic definitions, and poll large recursive futures on a growable
// stack segment.
const MAX_QUERY_NESTING: usize = 64;

tokio::task_local! {
    static QUERY_NESTING: std::cell::Cell<usize>;
}

struct QueryNestingGuard;

impl QueryNestingGuard {
    fn enter() -> Result<Self> {
        QUERY_NESTING.with(|depth| {
            let next = depth.get() + 1;
            if next > MAX_QUERY_NESTING {
                return Err(Error::Query(format!(
                    "query nesting exceeds {MAX_QUERY_NESTING} levels"
                )));
            }
            depth.set(next);
            Ok(Self)
        })
    }
}

impl Drop for QueryNestingGuard {
    fn drop(&mut self) {
        let _ = QUERY_NESTING.try_with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

pub async fn select(
    db: &Session,
    vindex: &VectorRegistry,
    query: &SqlQuery,
) -> Result<QueryResult> {
    if QUERY_NESTING.try_with(|_| ()).is_ok() {
        select_with_stack(db, vindex, query).await
    } else {
        QUERY_NESTING
            .scope(
                std::cell::Cell::new(0),
                select_with_stack(db, vindex, query),
            )
            .await
    }
}

async fn select_with_stack(
    db: &Session,
    vindex: &VectorRegistry,
    query: &SqlQuery,
) -> Result<QueryResult> {
    const RED_ZONE: usize = 1024 * 1024;
    const STACK_SIZE: usize = 2 * 1024 * 1024;

    let _nesting = QueryNestingGuard::enter()?;
    let mut future = Box::pin(select_inner(db, vindex, query));
    std::future::poll_fn(move |context| {
        stacker::maybe_grow(RED_ZONE, STACK_SIZE, || {
            std::future::Future::poll(future.as_mut(), context)
        })
    })
    .await
}

async fn select_inner(
    db: &Session,
    vindex: &VectorRegistry,
    query: &SqlQuery,
) -> Result<QueryResult> {
    // Expand CTEs (WITH ...) into derived tables, then execute. Recursive CTEs
    // take a fixpoint-materialisation path via temporary relations.
    if let Some(w) = &query.with {
        validate_unique_cte_names(w)?;
        if w.recursive {
            guard_cte_ast_complexity(query)?;
            return Box::pin(execute_recursive_cte(db, vindex, query)).await;
        }
        let expanded = expand_ctes(query)?;
        return Box::pin(select(db, vindex, &expanded)).await;
    }

    // Validate database-backed qualifiers before view expansion turns a view
    // into a derived table and changes the applicable qualifier policy.
    let qualifier_normalized;
    let query = if from_has_plain_table(query) {
        qualifier_normalized = {
            let mut normalized = query.clone();
            normalize_query_qualifiers(&mut normalized, &db.database())?;
            normalized
        };
        &qualifier_normalized
    } else {
        query
    };

    validate_select_alias_hiding(db, query)?;
    let query_before_binding = query;

    let wildcard_bound = bind_qualified_wildcards(db, query)?;
    let query = wildcard_bound.as_ref().unwrap_or(query);

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
            // Re-enter through the pre-binding query. Reusing the expanded
            // derived form would try to bind an already-bound physical/view
            // wildcard under query-scoped derived-table rules.
            let mut inner_q = query_before_binding.clone();
            inner_q.limit = None;
            inner_q.offset = None;
            if let SetExpr::Select(si) = inner_q.body.as_mut() {
                si.distinct = None;
            }
            let res = Box::pin(select(db, vindex, &inner_q)).await?;
            let QueryResult::Rows(stream) = res else {
                return Ok(res);
            };
            return Ok(QueryResult::Rows(
                distinct_rows(stream, d_offset, d_limit, distinct_max(), db.cancel_token()).await?,
            ));
        }
    }

    // Normalise an INNER comma-join (`FROM a, b WHERE a.k = b.k`) into an explicit
    // JOIN chain so it gets cost-based reordering and streaming. Done before the
    // `select` local shadows the function name below.
    if let SetExpr::Select(s) = query.body.as_ref() {
        if s.from.len() > 1 && s.from.iter().all(|t| t.joins.is_empty()) {
            if let Some(chain) = comma_join_chain(db, &s.from, s.selection.as_ref()) {
                let mut q2 = query.clone();
                if let SetExpr::Select(sm) = q2.body.as_mut() {
                    sm.from = vec![chain];
                }
                return Box::pin(select(db, vindex, &q2)).await;
            }
        }
    }

    // View expansion and derived-only queries can introduce query-scoped
    // relations. Normalize again under those final relation policies.
    let mut normalized_query = query.clone();
    normalize_query_qualifiers(&mut normalized_query, &db.database())?;
    let query = &normalized_query;
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
            query_before_binding,
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
        let quals = join_qualifiers(db, &select.from);
        let qualified_filter_correlated = raw_filter
            .as_ref()
            .is_some_and(|filter| filter_correlated_any(filter, &quals));
        let mut resolved_bare_filter = None;
        let potential_bare_filter = raw_filter
            .as_ref()
            .filter(|filter| {
                !qualified_filter_correlated && expr_has_potential_bare_correlation(filter)
            })
            .cloned();
        let bare_filter_correlated = if let Some(filter) = potential_bare_filter {
            match resolve_subqueries(db, vindex, filter.clone()).await {
                Ok(resolved) => {
                    // The bare names belonged to the subquery's local scope.
                    // Preserve the one-time resolution rather than executing it
                    // again for every joined row.
                    resolved_bare_filter = Some(resolved);
                    false
                }
                Err(error) if bare_unknown_column(&error, &filter).is_some() => true,
                Err(error) => return Err(error),
            }
        } else {
            false
        };
        let correlated = qualified_filter_correlated
            || bare_filter_correlated
            || projection_correlated_any(&select.projection, &quals)
            // A bare name inside a subquery can correlate only after the joined
            // logical schema exists. This matters for a coalesced USING/NATURAL
            // key, which is deliberately one bare outer column. Let the
            // per-row path first try the inner query, then fall back to that
            // outer schema; ambiguity still propagates as an error. Keep
            // aggregate/grouped projections on their existing path, where
            // correlated projection subqueries are not supported.
            || (group_by.is_empty()
                && !aggregate::projection_has_aggregate(&select.projection)
                && projection_has_potential_bare_correlation(&select.projection));
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
            Some(filter) => match resolved_bare_filter {
                Some(resolved) => Some(resolved),
                None => Some(resolve_subqueries(db, vindex, filter).await?),
            },
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

    // FROM-less SELECT (e.g. `SELECT 1`, recursive-CTE anchors): at most one row.
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
                qualifier: Vec::new(),
                result_metadata: Default::default(),
            });
            vals.push(v);
        }
        let mut rows = if pass { vec![vals] } else { Vec::new() };
        apply_offset_limit(&mut rows, offset, limit);
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
        let include_catalog_rows = select
            .selection
            .as_ref()
            .is_some_and(requests_information_schema_rows);
        let (schema, rows) = information_schema(db, &view, include_catalog_rows).await?;
        let qualifier = wildcard_schema_qualifier(db, &select.from[0].relation)
            .unwrap_or_else(|| ObjectName(vec![Ident::new(view)]));
        let schema = qualify_relation_schema(schema, &qualifier);
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
        TableFactor::Table { name, .. } => stored_table_ident(db, name)?,
        _ => {
            return Err(Error::Unsupported(
                "only plain table references are supported".into(),
            ))
        }
    };
    let def = catalog::load(db, &table).await?;

    // Outer table name/alias, used to detect and bind correlated subqueries.
    let outer = factor_qualifier_object(db, &select.from[0].relation)
        .map(|qualifier| object_name_parts(&qualifier))
        .unwrap_or_else(|| vec![table.clone()]);

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
        let mut plan = aggregate::build_plan(&def.schema, proj, &group_by)?;
        plan.set_group_concat_max_len(db.group_concat_max_len());
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
                    let (schema, out) = project_exprs(
                        &select.projection,
                        &def.schema,
                        &rows,
                        single_relation_alias(select).as_deref(),
                    )?;
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
                let prefix = def.data_prefix();
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
                let (schema, out) = project_exprs(
                    &select.projection,
                    &def.schema,
                    &rows,
                    single_relation_alias(select).as_deref(),
                )?;
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
                let prefix = def.data_prefix();
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
                    let (schema, out) = project_exprs(
                        &select.projection,
                        &def.schema,
                        &rows,
                        single_relation_alias(select).as_deref(),
                    )?;
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
                    let iprefix = index::index_scan_prefix(&def.storage_name(), &plan.index);
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
                    let nprefix = index::indexnull_scan_prefix(&def.storage_name(), &plan.index);
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
                        let (schema, out) = project_exprs(
                            &select.projection,
                            &def.schema,
                            &rows,
                            single_relation_alias(select).as_deref(),
                        )?;
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
            let prefix = def.data_prefix();
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
            let (schema, out) = project_exprs(
                &select.projection,
                &def.schema,
                &rows,
                single_relation_alias(select).as_deref(),
            )?;
            return Ok(QueryResult::Rows(RowStream::literal(schema, out)));
        }

        let mut rows = scan_rows(db, &def, filter.as_ref()).await?;
        if !resolved.is_empty() {
            sort_full_rows(&mut rows, &def.schema, &resolved, &db.cancel_token())?;
        }
        apply_offset_limit(&mut rows, offset, limit);
        let (schema, out) = project_exprs(
            &select.projection,
            &def.schema,
            &rows,
            single_relation_alias(select).as_deref(),
        )?;
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
                .position(|c| predicate::identifier_eq(&c.name, ident))
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

    // Every column on this path comes from the one relation being scanned, so
    // that is what result metadata reports as their source table.
    let out_schema = match single_relation_alias(select) {
        Some(relation) => {
            let tables = vec![relation; out_cols.len()];
            Schema::with_tables(out_cols, tables)
        }
        None => Schema::new(out_cols),
    };

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

    let (schema, rows) = build_from(db, vindex, &select.from, &conjuncts).await?;
    let cancel = db.cancel_token();
    let group_concat_max_len = db.group_concat_max_len();
    cpu_bound(move || {
        finish_materialized_select(
            select,
            filter.as_ref(),
            schema,
            rows,
            &group_by,
            &order_exprs,
            offset,
            limit,
            &cancel,
            group_concat_max_len,
        )
    })
}

/// Apply the relational work shared by materialised joins and flattened derived
/// chains. Callers run this through [`cpu_bound`] so row loops do not monopolise
/// an async worker.
#[allow(clippy::too_many_arguments)]
fn finish_materialized_select(
    select: &Select,
    filter: Option<&Expr>,
    schema: Schema,
    mut rows: Vec<Vec<Value>>,
    group_by: &[Expr],
    order_exprs: &[(Expr, bool)],
    offset: usize,
    limit: Option<usize>,
    cancel: &std::sync::Arc<elyra_core::cancel::QueryCancel>,
    group_concat_max_len: usize,
) -> Result<QueryResult> {
    // WHERE over the joined rows.
    let mut check = elyra_core::cancel::CancelCheck::new(cancel.clone());
    check.tick_now()?;
    if let Some(filter) = filter {
        let mut kept = Vec::with_capacity(rows.len());
        for row in rows {
            check.tick()?;
            if predicate::matches(filter, &schema, &row)? {
                kept.push(row);
            }
        }
        rows = kept;
    }

    // Aggregation / grouping.
    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        // Aggregating and ordering materialised rows is pure CPU work.
        let (projection, hidden) = aggregate_projection_with_hidden(
            &select.projection,
            select.having.as_ref(),
            order_exprs,
            &schema,
        );
        let (mut osch, orows) =
            aggregate::run(&schema, &projection, group_by, rows, group_concat_max_len)?;
        let mut orows = apply_having(select.having.as_ref(), &projection, &osch, orows)?;
        order_output_rows(&mut orows, &osch, order_exprs)?;
        truncate_hidden_columns(&mut osch, &mut orows, hidden);
        apply_offset_limit(&mut orows, offset, limit);
        return Ok(QueryResult::Rows(RowStream::literal(osch, orows)));
    }

    // ORDER BY + projection.
    let resolved = resolve_order_aliases(order_exprs, &select.projection, &schema);
    if !resolved.is_empty() {
        sort_full_rows(&mut rows, &schema, &resolved, cancel)?;
    }
    apply_offset_limit(&mut rows, offset, limit);
    let (osch, out) = project_exprs(&select.projection, &schema, &rows, None)?;
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

    let fast_filter = filter.map(|f| FastFilter::build(f, &schema));

    let keep = |row: &[Value]| -> Result<bool> {
        match &fast_filter {
            Some(ff) => ff.matches(row, &schema),
            None => Ok(true),
        }
    };

    let prefix = ddef.data_prefix();
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
    let (osch, rows) = project_exprs(&select.projection, &schema, &out, None)?;
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
        let prefix = p.def.data_prefix();
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
    let mut plan = match aggregate::build_plan(&schema, &select.projection, group_by) {
        Ok(p) => p,
        Err(_) => return Ok(None),
    };
    plan.set_group_concat_max_len(db.group_concat_max_len());
    let extend = !plan.arg_exprs().is_empty();

    let fast_filter = filter.as_ref().map(|f| FastFilter::build(f, &schema));

    let keep = |row: &[Value]| -> Result<bool> {
        match &fast_filter {
            Some(ff) => ff.matches(row, &schema),
            None => Ok(true),
        }
    };

    // Stream the driving table, expanding each row through the chain and feeding
    // the spilling aggregator -- so a large join + GROUP BY is bounded by the
    // group state (which spills), not the join output size.
    let mut sa = SpillAgg::new(&plan);
    let prefix = ddef.data_prefix();
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

    let fast_filter = filter.map(|f| FastFilter::build(f, &schema));

    let keep = |row: &[Value]| -> Result<bool> {
        match &fast_filter {
            Some(ff) => ff.matches(row, &schema),
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
    let prefix = ddef.data_prefix();
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
    let (osch, out) = project_exprs(&select.projection, &schema, &sorted, None)?;
    Ok(Some(QueryResult::Rows(RowStream::literal(osch, out))))
}

/// The table qualifiers (alias or name) of every relation in a FROM clause.
fn join_qualifiers(db: &Session, from: &[TableWithJoins]) -> Vec<Vec<String>> {
    join_qualifier_bindings(db, from)
        .into_iter()
        .map(|(qualifier, _)| qualifier)
        .collect()
}

fn join_qualifier_bindings(db: &Session, from: &[TableWithJoins]) -> Vec<(Vec<String>, bool)> {
    let mut bindings = Vec::new();
    let mut push = |relation: &TableFactor| {
        let Some(qualifier) = factor_qualifier_object(db, relation) else {
            return;
        };
        let explicit_alias = match relation {
            TableFactor::Table { alias, .. } | TableFactor::Derived { alias, .. } => {
                alias.is_some()
            }
            _ => false,
        };
        bindings.push((object_name_parts(&qualifier), explicit_alias));
    };
    for table in from {
        push(&table.relation);
        for join in &table.joins {
            push(&join.relation);
        }
    }
    bindings
}

#[derive(Clone)]
enum RelationQualifierPolicy {
    DatabaseBacked { database: String },
    QueryScoped,
}

#[derive(Clone)]
struct RelationQualifier {
    canonical: Vec<Ident>,
    policy: RelationQualifierPolicy,
}

impl RelationQualifier {
    fn matches(&self, prefix: &[Ident]) -> bool {
        let Some(canonical) = self.canonical.last().map(|identifier| &identifier.value) else {
            return false;
        };
        match (&self.policy, prefix) {
            (_, [alias]) if &alias.value == canonical => true,
            (RelationQualifierPolicy::DatabaseBacked { database }, [actual_database, alias]) => {
                &actual_database.value == database && &alias.value == canonical
            }
            (RelationQualifierPolicy::QueryScoped, [_, alias]) => &alias.value == canonical,
            _ => false,
        }
    }
}

fn relation_qualifier(factor: &TableFactor, selected_database: &str) -> Option<RelationQualifier> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table = name.0.last()?.clone();
            let canonical = alias
                .as_ref()
                .map(|alias| alias.name.clone())
                .unwrap_or(table);
            let database = name
                .0
                .iter()
                .rev()
                .nth(1)
                .map(|identifier| identifier.value.clone())
                .unwrap_or_else(|| selected_database.to_owned());
            Some(RelationQualifier {
                canonical: vec![Ident::new(database.clone()), canonical],
                policy: RelationQualifierPolicy::DatabaseBacked { database },
            })
        }
        // MySQL accepts a two-part prefix for a derived/CTE column reference.
        // Qualified wildcards are validated separately and remain alias-only.
        TableFactor::Derived { alias, .. } => alias.as_ref().map(|alias| RelationQualifier {
            canonical: vec![Ident::new(selected_database), alias.name.clone()],
            policy: RelationQualifierPolicy::QueryScoped,
        }),
        _ => None,
    }
}

fn collect_query_relation_qualifiers(
    body: &SetExpr,
    selected_database: &str,
    qualifiers: &mut Vec<RelationQualifier>,
) {
    match body {
        SetExpr::Select(select) => {
            for table in &select.from {
                qualifiers.extend(relation_qualifier(&table.relation, selected_database));
                qualifiers.extend(
                    table
                        .joins
                        .iter()
                        .filter_map(|join| relation_qualifier(&join.relation, selected_database)),
                );
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_query_relation_qualifiers(left, selected_database, qualifiers);
            collect_query_relation_qualifiers(right, selected_database, qualifiers);
        }
        // A nested query receives its own visitor scope.
        SetExpr::Query(_) | SetExpr::Values(_) | SetExpr::Insert(_) | SetExpr::Update(_) => {}
        SetExpr::Table(_) => {}
    }
}

struct QualifierNormalizer<'a> {
    selected_database: &'a str,
    scopes: Vec<Vec<RelationQualifier>>,
}

impl QualifierNormalizer<'_> {
    fn canonical(&self, prefix: &[Ident]) -> Option<Vec<Ident>> {
        let relation_name = prefix.last()?;
        for scope in self.scopes.iter().rev() {
            let matches = scope
                .iter()
                .filter(|relation| relation.matches(prefix))
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [relation] => return Some(relation.canonical.clone()),
                [_, _, ..] if prefix.len() == 1 => {
                    let selected = matches
                        .iter()
                        .filter(|relation| {
                            matches!(
                                &relation.policy,
                                RelationQualifierPolicy::DatabaseBacked { database }
                                    if database == self.selected_database
                            )
                        })
                        .collect::<Vec<_>>();
                    if let [relation] = selected.as_slice() {
                        return Some(relation.canonical.clone());
                    }
                    return None;
                }
                [_, _, ..] => return None,
                [] => {}
            }
            // The nearest scope that owns this exact relation name also owns
            // invalid longer forms; do not fall through to an outer scope.
            if scope.iter().any(|relation| {
                relation
                    .canonical
                    .last()
                    .is_some_and(|canonical| canonical.value == relation_name.value)
            }) {
                return None;
            }
        }
        None
    }

    fn unknown(reference: &[Ident]) -> ControlFlow<Error> {
        let reference = reference
            .iter()
            .map(|identifier| identifier.value.as_str())
            .collect::<Vec<_>>()
            .join(".");
        ControlFlow::Break(Error::Catalog(format!("unknown column: {reference}")))
    }
}

impl VisitorMut for QualifierNormalizer<'_> {
    type Break = Error;

    fn pre_visit_query(&mut self, query: &mut SqlQuery) -> ControlFlow<Self::Break> {
        let mut qualifiers = Vec::new();
        collect_query_relation_qualifiers(&query.body, self.selected_database, &mut qualifiers);
        self.scopes.push(qualifiers);

        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &mut SqlQuery) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::CompoundIdentifier(parts) = expr else {
            return ControlFlow::Continue(());
        };
        if parts
            .first()
            .is_some_and(|identifier| identifier.value.starts_with("@@"))
        {
            return ControlFlow::Continue(());
        }
        let Some((column, prefix)) = parts.split_last() else {
            return ControlFlow::Continue(());
        };
        let Some(canonical) = self.canonical(prefix) else {
            return Self::unknown(parts);
        };
        let column = column.clone();
        parts.clear();
        parts.extend(canonical);
        parts.push(column);
        ControlFlow::Continue(())
    }
}

fn normalize_query_qualifiers(query: &mut SqlQuery, selected_database: &str) -> Result<()> {
    let mut normalizer = QualifierNormalizer {
        selected_database,
        scopes: Vec::new(),
    };
    match query.visit(&mut normalizer) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
}

fn object_names_equal(left: &ObjectName, right: &ObjectName) -> bool {
    left.0.len() == right.0.len()
        && left
            .0
            .iter()
            .zip(&right.0)
            .all(|(left, right)| left.value == right.value)
}

fn canonical_relation_qualifier(
    db: &Session,
    name: Option<&ObjectName>,
    alias: &Ident,
) -> ObjectName {
    let schema = name
        .and_then(|name| (name.0.len() >= 2).then(|| name.0[name.0.len() - 2].clone()))
        .unwrap_or_else(|| Ident::new(db.database()));
    ObjectName(vec![schema, alias.clone()])
}

fn factor_qualifier_object(db: &Session, relation: &TableFactor) -> Option<ObjectName> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let relation_name = alias
                .as_ref()
                .map(|alias| alias.name.clone())
                .or_else(|| name.0.last().cloned())?;
            Some(canonical_relation_qualifier(db, Some(name), &relation_name))
        }
        TableFactor::Derived { alias, .. } => alias
            .as_ref()
            .map(|alias| canonical_relation_qualifier(db, None, &alias.name)),
        _ => None,
    }
}

pub(crate) fn wildcard_matches_relation(
    db: &Session,
    object: &ObjectName,
    relation: &TableFactor,
) -> bool {
    if let TableFactor::Derived {
        alias: Some(alias), ..
    } = relation
    {
        return matches!(object.0.as_slice(), [qualifier] if qualifier.value == alias.name.value);
    }
    let Some(canonical) = factor_qualifier_object(db, relation) else {
        return false;
    };
    object_names_equal(object, &canonical)
        || matches!(
            (object.0.as_slice(), canonical.0.last()),
            ([qualifier], Some(relation_name))
                if qualifier.value == relation_name.value
        )
}

fn object_name_text(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| part.value.as_str())
        .collect::<Vec<_>>()
        .join(".")
}

fn object_name_parts(name: &ObjectName) -> Vec<String> {
    name.0.iter().map(|part| part.value.clone()).collect()
}

fn hidden_source_qualifier(relation: &TableFactor) -> Option<Vec<String>> {
    let TableFactor::Table {
        name,
        alias: Some(alias),
        ..
    } = relation
    else {
        return None;
    };
    let source = object_name_parts(name);
    source
        .last()
        .is_none_or(|part| part != &alias.name.value)
        .then_some(source)
}

fn hidden_source_qualifiers(from: &[TableWithJoins]) -> Vec<Vec<String>> {
    from.iter()
        .flat_map(|table| {
            std::iter::once(&table.relation).chain(table.joins.iter().map(|join| &join.relation))
        })
        .filter_map(hidden_source_qualifier)
        .collect()
}

fn qualifier_components_equal(left: &[String], right: &[String]) -> bool {
    left == right
}

fn qualifier_short_names_equal(left: &[String], right: &[String]) -> bool {
    left.last()
        .zip(right.last())
        .is_some_and(|(left, right)| left == right)
}

fn validate_unique_relation_qualifiers(
    db: &Session,
    from: &[TableWithJoins],
    operation: &str,
) -> Result<Vec<Vec<String>>> {
    let bindings = join_qualifier_bindings(db, from);
    for (index, (qualifier, explicit_alias)) in bindings.iter().enumerate() {
        if bindings[..index]
            .iter()
            .any(|(other, other_explicit_alias)| {
                qualifier_components_equal(other, qualifier)
                    || ((*explicit_alias || *other_explicit_alias)
                        && qualifier_short_names_equal(other, qualifier))
            })
        {
            return Err(Error::Query(format!(
                "duplicate table alias in {operation}: {}",
                qualifier.join(".")
            )));
        }
    }
    Ok(bindings
        .into_iter()
        .map(|(qualifier, _)| qualifier)
        .collect())
}

fn qualifier_is_hidden(
    reference: &[Ident],
    hidden: &[Vec<String>],
    visible: &[Vec<String>],
) -> bool {
    let visible_exact = visible
        .iter()
        .any(|source| ident_qualifier_has_source_suffix(reference, source));
    let visible_case_mismatch = visible.iter().any(|source| {
        ident_qualifier_has_source_suffix_case_insensitive(reference, source)
            && !ident_qualifier_has_source_suffix(reference, source)
    });
    !visible_exact
        && (visible_case_mismatch
            || hidden
                .iter()
                .any(|source| ident_qualifier_has_source_suffix(reference, source)))
}

fn ident_qualifier_is_visible_source_suffix(reference: &[Ident], source: &[String]) -> bool {
    !reference.is_empty()
        && reference.len() <= source.len()
        && source[source.len() - reference.len()..]
            .iter()
            .zip(reference)
            .all(|(source, reference)| source == &reference.value)
}

fn validate_assignment_target_qualifier(name: &ObjectName, visible: &[Vec<String>]) -> Result<()> {
    let qualifier = &name.0[..name.0.len().saturating_sub(1)];
    if qualifier.len() > 2 {
        return Err(Error::Parse(format!("invalid assignment target: {name}")));
    }
    if !qualifier.is_empty()
        && !visible
            .iter()
            .any(|source| ident_qualifier_is_visible_source_suffix(qualifier, source))
    {
        return Err(Error::UnknownColumn(name.to_string()));
    }
    Ok(())
}

struct HiddenSourceVisitor<'a> {
    hidden: &'a [Vec<String>],
    visible: &'a [Vec<String>],
    nested_query_depth: usize,
}

impl Visitor for HiddenSourceVisitor<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &SqlQuery) -> ControlFlow<Self::Break> {
        self.nested_query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &SqlQuery) -> ControlFlow<Self::Break> {
        self.nested_query_depth = self.nested_query_depth.saturating_sub(1);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if self.nested_query_depth == 0 {
            if let Expr::CompoundIdentifier(parts) = expr {
                if parts.len() >= 2
                    && qualifier_is_hidden(&parts[..parts.len() - 1], self.hidden, self.visible)
                {
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn ast_uses_hidden_source<T: Visit>(
    ast: &T,
    hidden: &[Vec<String>],
    visible: &[Vec<String>],
) -> bool {
    let mut visitor = HiddenSourceVisitor {
        hidden,
        visible,
        nested_query_depth: 0,
    };
    matches!(ast.visit(&mut visitor), ControlFlow::Break(()))
}

fn ident_qualifier_has_source_suffix(reference: &[Ident], source: &[String]) -> bool {
    if reference.is_empty() || source.is_empty() {
        return false;
    }
    let reference_ends_with_source = source.len() <= reference.len()
        && reference[reference.len() - source.len()..]
            .iter()
            .zip(source)
            .all(|(reference, source)| reference.value == *source);
    let source_ends_with_reference = reference.len() <= source.len()
        && source[source.len() - reference.len()..]
            .iter()
            .zip(reference)
            .all(|(source, reference)| source == &reference.value);
    reference_ends_with_source || source_ends_with_reference
}

fn ident_qualifier_has_source_suffix_case_insensitive(
    reference: &[Ident],
    source: &[String],
) -> bool {
    if reference.is_empty() || source.is_empty() {
        return false;
    }
    let reference_ends_with_source = source.len() <= reference.len()
        && reference[reference.len() - source.len()..]
            .iter()
            .zip(source)
            .all(|(reference, source)| reference.value.eq_ignore_ascii_case(source));
    let source_ends_with_reference = reference.len() <= source.len()
        && source[source.len() - reference.len()..]
            .iter()
            .zip(reference)
            .all(|(source, reference)| source.eq_ignore_ascii_case(&reference.value));
    reference_ends_with_source || source_ends_with_reference
}

fn validate_ast_alias_hiding<T: Visit>(
    ast: &T,
    hidden: &[Vec<String>],
    visible: &[Vec<String>],
) -> Result<()> {
    if ast_uses_hidden_source(ast, hidden, visible) {
        return Err(Error::Catalog(
            "an aliased table must be referenced by its alias".into(),
        ));
    }
    Ok(())
}

fn validate_select_alias_hiding(db: &Session, query: &SqlQuery) -> Result<()> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(());
    };
    let visible = validate_unique_relation_qualifiers(db, &select.from, "SELECT")?;
    let hidden = hidden_source_qualifiers(&select.from);
    validate_ast_alias_hiding(select.as_ref(), &hidden, &visible)?;
    if let Some(order_by) = &query.order_by {
        for order in &order_by.exprs {
            validate_ast_alias_hiding(&order.expr, &hidden, &visible)?;
        }
    }
    Ok(())
}

fn column_name(column: &ColumnDef) -> &str {
    if column.qualifier.is_empty() {
        return &column.name;
    }
    let qualifier_len = column.qualifier.iter().map(String::len).sum::<usize>()
        + column.qualifier.len().saturating_sub(1);
    column.name.get(qualifier_len + 1..).unwrap_or(&column.name)
}

fn qualifier_parts_match(stored: &[String], requested: &[Ident]) -> bool {
    requested.len() <= stored.len()
        && stored[stored.len() - requested.len()..]
            .iter()
            .zip(requested)
            .all(|(stored, requested)| stored == &requested.value)
}

fn column_table(column: &ColumnDef) -> Option<&str> {
    column.qualifier.last().map(String::as_str)
}

/// Result-metadata source for one schema column. Logical columns created by a
/// USING/NATURAL join intentionally have no qualifier (so bare name resolution
/// sees one key), but retain their selected source separately in `Schema`.
fn schema_column_table(schema: &Schema, index: usize) -> Option<&str> {
    schema
        .table_of(index)
        .or_else(|| schema.columns.get(index).and_then(column_table))
}

fn wildcard_column_name<'a>(
    column: &'a ColumnDef,
    qualifier: &ObjectName,
    unqualified_schema: bool,
) -> Option<&'a str> {
    if unqualified_schema {
        return Some(column_name(column));
    }
    qualifier_parts_match(&column.qualifier, &qualifier.0).then(|| column_name(column))
}

fn wildcard_schema_qualifier(db: &Session, relation: &TableFactor) -> Option<ObjectName> {
    factor_qualifier_object(db, relation)
}

fn malformed_virtual_source(relation: &TableFactor) -> Option<&ObjectName> {
    let TableFactor::Table { name, .. } = relation else {
        return None;
    };
    (name.0.len() != 2
        && name.0[..name.0.len().saturating_sub(1)].iter().any(|part| {
            part.value.eq_ignore_ascii_case("information_schema")
                || part.value.eq_ignore_ascii_case("mysql")
        }))
    .then_some(name)
}

fn bind_qualified_wildcards(db: &Session, query: &SqlQuery) -> Result<Option<SqlQuery>> {
    use sqlparser::ast::SelectItem;

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    let relations = select
        .from
        .iter()
        .flat_map(|table| {
            std::iter::once(&table.relation).chain(table.joins.iter().map(|join| &join.relation))
        })
        .collect::<Vec<_>>();
    if let Some(name) = relations
        .iter()
        .find_map(|relation| malformed_virtual_source(relation))
    {
        return Err(Error::Unsupported(format!(
            "virtual relation {name} must have exactly two name components"
        )));
    }

    let mut bindings = Vec::new();
    for (index, item) in select.projection.iter().enumerate() {
        let SelectItem::QualifiedWildcard(object, _) = item else {
            continue;
        };
        let mut matches = relations
            .iter()
            .copied()
            .filter(|relation| wildcard_matches_relation(db, object, relation));
        let Some(relation) = matches.next() else {
            return Err(Error::Unsupported(format!(
                "qualified wildcard {object}.* matched no relation"
            )));
        };
        if matches.next().is_some() {
            return Err(Error::Query(format!(
                "qualified wildcard {object}.* is ambiguous"
            )));
        }
        let qualifier = wildcard_schema_qualifier(db, relation).ok_or_else(|| {
            Error::Unsupported(format!("qualified wildcard {object}.* matched no relation"))
        })?;
        bindings.push((index, qualifier));
    }
    if bindings.is_empty() {
        return Ok(None);
    }

    let mut bound = query.clone();
    let SetExpr::Select(select) = bound.body.as_mut() else {
        unreachable!("the original query body was SELECT")
    };
    for (index, qualifier) in bindings {
        let SelectItem::QualifiedWildcard(object, _) = &mut select.projection[index] else {
            unreachable!("binding indices only contain qualified wildcards")
        };
        *object = qualifier;
    }
    Ok(Some(bound))
}

pub(crate) async fn describe_relation_schema(
    db: &Session,
    relation: &TableFactor,
) -> Result<Schema> {
    if let Some(name) = malformed_virtual_source(relation) {
        return Err(Error::Unsupported(format!(
            "virtual relation {name} must have exactly two name components"
        )));
    }
    if let Some(view) = information_schema_view(relation) {
        return information_schema_schema(&view);
    }
    let TableFactor::Table { name, .. } = relation else {
        return Err(Error::Unsupported(
            "only plain table references can be described".into(),
        ));
    };
    catalog::load(db, &stored_table_ident(db, name)?)
        .await
        .map(|definition| definition.schema)
}

fn qualify_relation_schema(mut schema: Schema, qualifier: &ObjectName) -> Schema {
    let qualifier_text = object_name_text(qualifier);
    let qualifier_parts = object_name_parts(qualifier);
    for column in &mut schema.columns {
        column.name = format!("{qualifier_text}.{}", column.name);
        column.qualifier.clone_from(&qualifier_parts);
    }
    schema
}

fn filter_correlated_any(f: &Expr, quals: &[Vec<String>]) -> bool {
    quals.iter().any(|q| filter_correlated(f, q))
}

fn projection_correlated_any(
    projection: &[sqlparser::ast::SelectItem],
    quals: &[Vec<String>],
) -> bool {
    quals.iter().any(|q| projection_correlated(projection, q))
}

/// Bind every qualified column reference (`alias.col`) that resolves in the
/// joined `schema` to its literal value from `row`, including inside
/// subqueries. Outer references in correlated subqueries become literals; the
/// subquery's own columns are left untouched.
fn row_binding_index(parts: &[Ident], schema: &Schema) -> Result<Option<usize>> {
    match predicate::resolve_index_parts(parts, schema) {
        Ok(index) => Ok(Some(index)),
        Err(ambiguous @ Error::Query(_)) => Err(ambiguous),
        Err(_) => Ok(None),
    }
}

fn bind_row(db: &Session, expr: &Expr, schema: &Schema, row: &[Value]) -> Result<Expr> {
    let error = std::cell::RefCell::new(None);
    let bound = bind_outer_references(db, expr, &[], &|e| {
        if let Expr::CompoundIdentifier(parts) = e {
            match row_binding_index(parts, schema) {
                Ok(Some(index)) => return Some(value_to_expr(&row[index])),
                Err(ambiguous) => {
                    *error.borrow_mut() = Some(ambiguous);
                    return Some(e.clone());
                }
                Ok(None) => {}
            }
        }
        None
    });
    match error.into_inner() {
        Some(error) => Err(error),
        None => Ok(bound),
    }
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

    let (schema, rows) = build_from(db, vindex, &select.from, &[]).await?;

    let mut kept: Vec<Vec<Value>> = Vec::new();
    for row in rows {
        if let Some(f) = &raw_filter {
            let bound = bind_row(db, f, &schema, &row)?;
            let resolved = resolve_subqueries_with_outer(db, vindex, bound, &schema, &row).await?;
            if !predicate::matches(&resolved, &schema, &row)? {
                continue;
            }
        }
        kept.push(row);
    }

    if aggregating {
        let (out_schema, out_rows) = aggregate::run(
            &schema,
            &select.projection,
            &group_by,
            kept,
            db.group_concat_max_len(),
        )?;
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
            |expr, row| bind_row(db, expr, &schema, row),
        )
        .await?;
    }
    apply_offset_limit(&mut kept, offset, limit);

    // Plain projection when no SELECT-list subqueries.
    if !projection_has_subquery(&select.projection) {
        let (osch, out) = project_exprs(&select.projection, &schema, &kept, None)?;
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
    // Source table per output column, for result metadata. The qualifier is only
    // available while the internal "alias.col" name is being shortened, so it is
    // captured here rather than recovered later.
    let mut outtables: Vec<String> = Vec::new();
    let mut inferred = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for index in unqualified_wildcard_indices(&schema) {
                    let column = &schema.columns[index];
                    projection.push(Projection::Column(index));
                    let mut column = column.clone();
                    outtables.push(
                        schema_column_table(&schema, index)
                            .unwrap_or_default()
                            .to_owned(),
                    );
                    column.name = column_name(&column).to_owned();
                    column.qualifier.clear();
                    outcols.push(column);
                }
            }
            SelectItem::QualifiedWildcard(object, _) => {
                let unqualified_schema = schema
                    .columns
                    .iter()
                    .all(|column| column.qualifier.is_empty());
                let before = projection.len();
                for (index, column) in schema.columns.iter().enumerate() {
                    if let Some(name) = wildcard_column_name(column, object, unqualified_schema) {
                        projection.push(Projection::Column(index));
                        let mut column = column.clone();
                        outtables.push(
                            schema_column_table(&schema, index)
                                .unwrap_or_default()
                                .to_owned(),
                        );
                        column.name = name.to_owned();
                        column.qualifier.clear();
                        outcols.push(column);
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
                // A projected expression has no source table -- neither does it
                // in MySQL, which reports an empty table for computed columns.
                outtables.push(
                    expr_col_index(expr, &schema)
                        .and_then(|index| schema_column_table(&schema, index))
                        .unwrap_or_default()
                        .to_owned(),
                );
                outcols.push(ColumnDef {
                    name,
                    ty: ColumnType::Text,
                    nullable: true,
                    collation: elyra_core::Collation::Ci,
                    qualifier: Vec::new(),
                    result_metadata: Default::default(),
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
                    let bound = bind_row(db, expr, &schema, row)?;
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
        Schema::with_tables(outcols, outtables),
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
        TableFactor::Table { name, .. } => {
            let tname = stored_table_ident(db, name)?;
            let def = catalog::load(db, &tname).await?;
            let qualifier = factor_qualifier_object(db, tf)
                .ok_or_else(|| Error::Catalog("empty table qualifier".into()))?;
            let qualifier_parts = qualifier
                .0
                .iter()
                .map(|part| part.value.clone())
                .collect::<Vec<_>>();
            let a = object_name_text(&qualifier);
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
                    qualifier: qualifier_parts.clone(),
                    result_metadata: c.result_metadata,
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
        let include_catalog_rows = conjuncts.iter().any(requests_information_schema_rows);
        let (schema, rows) = information_schema(db, &view, include_catalog_rows).await?;
        let qualifier =
            wildcard_schema_qualifier(db, tf).unwrap_or_else(|| ObjectName(vec![Ident::new(view)]));
        return Ok((qualify_relation_schema(schema, &qualifier).columns, rows));
    }

    // Derived table: materialise the subquery and qualify its columns.
    if let TableFactor::Derived {
        subquery,
        alias: Some(alias),
        ..
    } = tf
    {
        let qualifier = factor_qualifier_object(db, tf).ok_or_else(|| {
            Error::Query("a derived table (FROM (SELECT ...)) needs an alias".into())
        })?;
        let (schema, rows) = run_subquery_schema(db, vindex, subquery).await?;
        let schema = apply_col_aliases(schema, &alias_column_names(alias))?;
        let cols = qualify_columns(&schema, &qualifier);
        return Ok((cols, rows));
    }

    if matches!(tf, TableFactor::Derived { .. }) {
        return Err(Error::Query(
            "a derived table (FROM (SELECT ...)) needs an alias".into(),
        ));
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

fn qualify_columns(schema: &Schema, qualifier: &ObjectName) -> Vec<ColumnDef> {
    qualify_relation_schema(schema.clone(), qualifier).columns
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
            &def.storage_name(),
            &keyenc::encode_coll(value, def.collation_of(col))?,
        );
        return Ok(match db.get(key).await? {
            Some(b) => vec![deser(b)?],
            None => vec![],
        });
    }
    if let Some(idx) = index::index_on(def, col) {
        let dks =
            index::lookup_eq(db, &def.storage_name(), idx, std::slice::from_ref(value)).await?;
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
            &def.storage_name(),
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
    index::lookup_eq(db, &def.storage_name(), idx, std::slice::from_ref(value)).await
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
    let plain_index = |e: &Expr, schema: &Schema| -> Option<usize> {
        match e {
            Expr::Identifier(identifier) => {
                predicate::resolve_index_parts(std::slice::from_ref(identifier), schema).ok()
            }
            Expr::CompoundIdentifier(parts) => predicate::resolve_index_parts(parts, schema).ok(),
            _ => None,
        }
    };
    if refs_in_schema(left, driving) {
        if let Some(index) = plain_index(right, partner) {
            return Some(((**left).clone(), index));
        }
    }
    if refs_in_schema(right, driving) {
        if let Some(index) = plain_index(left, partner) {
            return Some(((**right).clone(), index));
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

/// An equality-constrained leading prefix followed by a range on the next
/// column of a composite secondary index.
struct CompositeRangeQuery<'a> {
    index: &'a IndexDef,
    prefix: Vec<Value>,
    lo: Option<(Value, bool)>,
    hi: Option<(Value, bool)>,
}

type RangeBound = Option<(Value, bool)>;
type ColumnBounds = (RangeBound, RangeBound);
type EqualityConstraints = std::collections::HashMap<usize, Value>;
type RangeConstraints = std::collections::HashMap<usize, ColumnBounds>;

fn merge_lower_bound(
    current: &mut Option<(Value, bool)>,
    candidate: (Value, bool),
    collation: elyra_core::Collation,
) {
    let replace = match current {
        None => true,
        Some((value, inclusive)) => match candidate.0.compare_coll(value, collation) {
            Some(std::cmp::Ordering::Greater) => true,
            Some(std::cmp::Ordering::Equal) => *inclusive && !candidate.1,
            _ => false,
        },
    };
    if replace {
        *current = Some(candidate);
    }
}

fn merge_upper_bound(
    current: &mut Option<(Value, bool)>,
    candidate: (Value, bool),
    collation: elyra_core::Collation,
) {
    let replace = match current {
        None => true,
        Some((value, inclusive)) => match candidate.0.compare_coll(value, collation) {
            Some(std::cmp::Ordering::Less) => true,
            Some(std::cmp::Ordering::Equal) => *inclusive && !candidate.1,
            _ => false,
        },
    };
    if replace {
        *current = Some(candidate);
    }
}

fn predicate_constraints(
    def: &TableDef,
    filter: Option<&Expr>,
) -> Result<(EqualityConstraints, RangeConstraints)> {
    use sqlparser::ast::BinaryOperator::*;
    use std::collections::HashMap;
    let mut equalities = HashMap::new();
    let mut ranges: RangeConstraints = HashMap::new();
    let Some(filter) = filter else {
        return Ok((equalities, ranges));
    };
    let mut conjuncts = Vec::new();
    split_and(filter, &mut conjuncts);
    for conjunct in &conjuncts {
        if let Some((col, value)) = eq_col_literal(def, Some(conjunct))? {
            equalities.entry(col).or_insert(value);
        }
        if let Some((col, op, value)) = as_range(def, conjunct)? {
            let bounds = ranges.entry(col).or_default();
            let collation = def.collation_of(col);
            match op {
                Gt => merge_lower_bound(&mut bounds.0, (value, false), collation),
                GtEq => merge_lower_bound(&mut bounds.0, (value, true), collation),
                Lt => merge_upper_bound(&mut bounds.1, (value, false), collation),
                LtEq => merge_upper_bound(&mut bounds.1, (value, true), collation),
                _ => {}
            }
        } else if let Some((col, lo, hi)) = as_between(def, conjunct)? {
            let bounds = ranges.entry(col).or_default();
            let collation = def.collation_of(col);
            merge_lower_bound(&mut bounds.0, (lo, true), collation);
            merge_upper_bound(&mut bounds.1, (hi, true), collation);
        }
    }
    Ok((equalities, ranges))
}

fn composite_range_bounds<'a>(
    def: &'a TableDef,
    filter: Option<&Expr>,
) -> Result<Option<CompositeRangeQuery<'a>>> {
    let (equalities, ranges) = predicate_constraints(def, filter)?;
    for index in &def.indexes {
        if index.vector || index.fulltext || index.cols.len() < 2 {
            continue;
        }
        let prefix_len = index
            .cols
            .iter()
            .take_while(|col| equalities.contains_key(col))
            .count();
        if prefix_len == 0 || prefix_len >= index.cols.len() {
            continue;
        }
        let range_col = index.cols[prefix_len];
        let Some((lo, hi)) = ranges.get(&range_col) else {
            continue;
        };
        // Composite entries are omitted when *any* indexed component is NULL.
        // The equality and range predicates themselves reject NULL in their
        // columns, but a nullable trailing component could omit an otherwise
        // qualifying row and make this scan incomplete.
        if index.cols[prefix_len + 1..]
            .iter()
            .any(|&col| def.schema.columns[col].nullable)
        {
            continue;
        }
        let prefix = index.cols[..prefix_len]
            .iter()
            .map(|col| equalities[col].clone())
            .collect::<Vec<_>>();
        if keyenc::encode_key_coll(&prefix, &index.col_collations).is_err()
            || lo.as_ref().is_some_and(|(value, _)| {
                keyenc::encode_coll(value, def.collation_of(range_col)).is_err()
            })
            || hi.as_ref().is_some_and(|(value, _)| {
                keyenc::encode_coll(value, def.collation_of(range_col)).is_err()
            })
        {
            continue;
        }
        return Ok(Some(CompositeRangeQuery {
            index,
            prefix,
            lo: lo.clone(),
            hi: hi.clone(),
        }));
    }
    Ok(None)
}

/// Detect a range over a PK/indexed column from the filter's AND-conjuncts
/// (`col >|>=|<|<= lit`, `col BETWEEN a AND b`). Only columns with
/// order-encodable bound values qualify.
fn range_bounds(def: &TableDef, filter: Option<&Expr>) -> Result<Option<RangeQuery>> {
    let (_, map) = predicate_constraints(def, filter)?;

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
/// Where a coerced literal landed relative to the literal the query actually
/// wrote. `None` when the two are not numerically comparable, in which case the
/// caller must treat the coercion as exact (text/date/enum coercions, which are
/// value-preserving in a way this check cannot see).
fn numeric_rounding(original: &Value, coerced: &Value) -> Option<std::cmp::Ordering> {
    if !matches!(original, Value::Float(_) | Value::Decimal(..)) {
        return None;
    }
    coerced.compare(original)
}

/// Whether coercing the literal to the column's type preserved its value, so an
/// index seek on the coerced key answers the same question the query asked.
fn coercion_is_exact(original: &Value, coerced: &Value) -> bool {
    !matches!(
        numeric_rounding(original, coerced),
        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Greater)
    )
}

/// Move a range bound into the column's domain without changing which rows it
/// selects.
///
/// A bound literal is *compared*, not stored, so it must not be rounded the way
/// an INSERT value is: on an `INT` key, `k > 1024.5` means `k >= 1025`, and
/// rounding the bound to 1025 while keeping the strict `>` silently drops row
/// 1025. When coercion moved the value, the bound's inclusivity is flipped to
/// compensate. That is exact rather than approximate: coercion rounds to the
/// *nearest* value the column can hold, so no representable value lies strictly
/// between the literal and its coerced form.
fn adjust_bound_op(
    op: &sqlparser::ast::BinaryOperator,
    original: &Value,
    coerced: &Value,
) -> sqlparser::ast::BinaryOperator {
    use sqlparser::ast::BinaryOperator::*;
    use std::cmp::Ordering;
    match numeric_rounding(original, coerced) {
        // Rounded up: `col > lit` and `col >= lit` are both `col >= coerced`,
        // while `col < lit` and `col <= lit` are both `col < coerced`.
        Some(Ordering::Greater) => match op {
            Gt | GtEq => GtEq,
            _ => Lt,
        },
        // Rounded down: the mirror image.
        Some(Ordering::Less) => match op {
            Gt | GtEq => Gt,
            _ => LtEq,
        },
        _ => op.clone(),
    }
}

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
            .position(|c| predicate::identifier_eq(&c.name, n))
    };
    let coerce_col = |col: usize, v: &Value| {
        let c = &def.schema.columns[col];
        coerce(v.clone(), &c.ty, &c.name).ok()
    };
    if let Some(col) = ident_name(left).and_then(col_of) {
        if let Ok(v) = eval_expr(right) {
            if let Some(cv) = coerce_col(col, &v) {
                let op = adjust_bound_op(op, &v, &cv);
                return Ok(Some((col, op, cv)));
            }
        }
    }
    if let Some(col) = ident_name(right).and_then(col_of) {
        if let Ok(v) = eval_expr(left) {
            if let Some(cv) = coerce_col(col, &v) {
                let flipped = match op {
                    Gt => Lt,
                    GtEq => LtEq,
                    Lt => Gt,
                    LtEq => GtEq,
                    _ => unreachable!(),
                };
                let op = adjust_bound_op(&flipped, &v, &cv);
                return Ok(Some((col, op, cv)));
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
            .position(|c| predicate::identifier_eq(&c.name, n))
    };
    let Some(col) = ident_name(e).and_then(col_of) else {
        return Ok(None);
    };
    let c = &def.schema.columns[col];
    match (eval_expr(low), eval_expr(high)) {
        (Ok(lo), Ok(hi)) => match (
            coerce(lo.clone(), &c.ty, &c.name),
            coerce(hi.clone(), &c.ty, &c.name),
        ) {
            // BETWEEN's bounds are inclusive on both ends and there is nowhere to
            // record a flipped inclusivity, so a bound that coercion moved falls
            // back to a scan (which re-applies the filter exactly) rather than
            // shifting the range by one row.
            (Ok(lo_c), Ok(hi_c))
                if coercion_is_exact(&lo, &lo_c) && coercion_is_exact(&hi, &hi_c) =>
            {
                Ok(Some((col, lo_c, hi_c)))
            }
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
    let rows = db.raw_db().count_prefix(def.data_prefix()).await?;
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
    let prefix = def.data_prefix();
    let coll = def.pk_collations().first().copied().unwrap_or_default();
    let mut start = match &rq.lo {
        Some((v, incl)) => {
            let mut b = data_key(&def.storage_name(), &keyenc::encode_coll(v, coll)?);
            if !*incl {
                b.push(0x00); // strictly after the row with pk == v
            }
            b
        }
        None => prefix.clone(),
    };
    let end = match &rq.hi {
        Some((v, incl)) => {
            let mut b = data_key(&def.storage_name(), &keyenc::encode_coll(v, coll)?);
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
    let data_keys = index::lookup_range(db, &def.storage_name(), idx, lo, hi).await?;
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

async fn composite_index_range(
    db: &Session,
    def: &TableDef,
    query: &CompositeRangeQuery<'_>,
    budget: Option<usize>,
) -> Result<Option<Vec<(Vec<u8>, Vec<Value>)>>> {
    let lo = query
        .lo
        .as_ref()
        .map(|(value, inclusive)| (value, *inclusive));
    let hi = query
        .hi
        .as_ref()
        .map(|(value, inclusive)| (value, *inclusive));
    let data_keys =
        index::lookup_prefix_range(db, &def.storage_name(), query.index, &query.prefix, lo, hi)
            .await?;
    if budget.is_some_and(|budget| data_keys.len() > budget) {
        return Ok(None);
    }
    let blobs = db.multi_get(data_keys.clone()).await?;
    let mut out = Vec::with_capacity(data_keys.len());
    for (key, blob) in data_keys.into_iter().zip(blobs) {
        if let Some(blob) = blob {
            out.push((key, rowdec::decode_row(&blob)?));
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
) -> Result<(Schema, Vec<Vec<Value>>)> {
    let pushdown_safe = outer_join_pushdown_safety(from);

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
            if let Some((columns, rows)) =
                build_inner_join_reordered(db, vindex, twj, conjuncts).await?
            {
                return Ok((Schema::new(columns), rows));
            }
            // Fell back (a predicate wasn't a clean equi-connector): use the
            // sequential left-to-right plan below, which applies each ON at its
            // own two-relation step.
        }
    }

    let mut cur_schema = Schema::default();
    let mut cur_rows: Vec<Vec<Value>> = Vec::new();
    let mut first = true;

    // Cost-based ordering for a pure comma cross-join (every entry is a plain
    // base table with no explicit JOINs): drive from the smallest analyzed
    // table. This is safe because cross-join + global WHERE is commutative.
    let ordered: Vec<(usize, &TableWithJoins)> = if from.len() > 1
        && from
            .iter()
            .all(|t| t.joins.is_empty() && stored_table_factor(&t.relation))
    {
        let mut idx: Vec<(usize, &TableWithJoins, u64)> = Vec::with_capacity(from.len());
        for (ti, t) in from.iter().enumerate() {
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
            idx.push((ti, t, est));
        }
        idx.sort_by_key(|(_, _, est)| *est);
        idx.into_iter().map(|(ti, t, _)| (ti, t)).collect()
    } else {
        from.iter().enumerate().collect()
    };

    for (ti, twj) in ordered {
        let base_conjuncts = if pushdown_safe[ti][0] { conjuncts } else { &[] };
        let (bc, mut br) = load_relation(db, vindex, &twj.relation, base_conjuncts).await?;
        br = apply_pushdown(br, &bc, base_conjuncts)?;
        let base_schema = Schema::new(bc);
        if first {
            cur_schema = base_schema;
            cur_rows = br;
            first = false;
        } else {
            let cancel = db.cancel_token();
            let (c, r) = cpu_bound(|| {
                combine(
                    &cur_schema,
                    &cur_rows,
                    &base_schema,
                    &br,
                    JoinKind::Inner,
                    None,
                    &cancel,
                )
            })?;
            cur_schema = c;
            cur_rows = r;
        }
        for (ji, join) in twj.joins.iter().enumerate() {
            // Index nested-loop join: when the driving side is small and the
            // partner is indexed on the join key, probe the partner per row
            // instead of materialising it in full.
            let driving_schema = cur_schema.clone();
            let (kind, on) = join_kind(&join.join_operator)?;
            let left_outer = kind == JoinKind::Left;

            // Index nested-loop join only applies to a plain (indexed) table
            // partner, not a derived table.
            let nlj = if stored_table_factor(&join.relation) {
                let (pdef, pcols) = resolve_table(db, &join.relation).await?;
                let partner_schema = Schema::new(pcols.clone());
                if let Some(expression) = on.as_ref() {
                    validate_join_on_refs(
                        expression,
                        &combined_join_schema(&driving_schema, &partner_schema),
                    )?;
                }
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
                cur_schema.columns.extend(pcols);
                cur_rows = out;
                continue;
            }

            // Fallback: materialise the partner (with pushdown) and hash/nested join.
            let partner_conjuncts = if pushdown_safe[ti][ji + 1] {
                conjuncts
            } else {
                &[]
            };
            let (jc, mut jr) = load_relation(db, vindex, &join.relation, partner_conjuncts).await?;
            jr = apply_pushdown(jr, &jc, partner_conjuncts)?;
            let partner_schema = Schema::new(jc);
            let using_keys = resolve_using_keys(&join.join_operator, &cur_schema, &partner_schema)?;
            let resolved_keys = using_keys
                .as_ref()
                .map(|keys| using_key_pairs(keys, &cur_schema, &partner_schema));
            let condition = resolved_keys
                .as_deref()
                .map(JoinCondition::ResolvedKeys)
                .or_else(|| on.as_ref().map(JoinCondition::On));
            let cancel = db.cancel_token();
            let (cols, rows) = cpu_bound(|| {
                combine(
                    &cur_schema,
                    &cur_rows,
                    &partner_schema,
                    &jr,
                    kind,
                    condition,
                    &cancel,
                )
            })?;
            (cur_schema, cur_rows) = match using_keys {
                Some(keys) => {
                    coalesce_using_join(&cur_schema, &partner_schema, cols, rows, kind, &keys)
                }
                None => (cols, rows),
            };
        }
    }
    Ok((cur_schema, cur_rows))
}

/// For every base relation and explicit join partner, whether a WHERE predicate
/// can be applied before the join without hiding rows that the outer join may
/// NULL-extend.
fn outer_join_pushdown_safety(from: &[TableWithJoins]) -> Vec<Vec<bool>> {
    let mut safe: Vec<Vec<bool>> = from
        .iter()
        .map(|twj| vec![true; twj.joins.len() + 1])
        .collect();
    let mut prior = Vec::new();

    for (ti, twj) in from.iter().enumerate() {
        prior.push((ti, 0));
        for (ji, join) in twj.joins.iter().enumerate() {
            let partner = (ti, ji + 1);
            match join.join_operator {
                JoinOperator::LeftOuter(_) => safe[partner.0][partner.1] = false,
                JoinOperator::RightOuter(_) => {
                    for &(pti, pji) in &prior {
                        safe[pti][pji] = false;
                    }
                }
                JoinOperator::FullOuter(_) => {
                    for &(pti, pji) in &prior {
                        safe[pti][pji] = false;
                    }
                    safe[partner.0][partner.1] = false;
                }
                // The executor rejects every remaining sqlparser join kind.
                _ => {}
            }
            prior.push(partner);
        }
    }

    safe
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
                .find(|s| predicate::identifier_eq(&s.name, &col))
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

/// Encode `(matdep_key, value)` capturing each base table's current write count,
/// so staleness can be detected later.
pub async fn matview_deps_put(db: &Session, name: &str, query: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    let query = parse_query(query)?;
    let relations = validated_query_relations(db, &query, None).await?;
    let mut deps: Vec<(String, u64)> = Vec::new();
    for t in relations.base_tables {
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
    let batch = batch.max(1);
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
        let mut tuples: Vec<String> = Vec::with_capacity(batch.min(50_000));
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

#[cfg(test)]
mod load_data_tests {
    use super::{build_load_inserts, LoadSpec};

    fn spec() -> LoadSpec {
        LoadSpec {
            path: String::new(),
            table: "items".into(),
            cols: vec!["id".into(), "label".into()],
            field_term: "\t".into(),
            enclosed: None,
            line_term: "\n".into(),
            ignore: 0,
        }
    }

    #[test]
    fn load_builder_honors_bulk_boundaries_and_zero_batch() {
        let content = "1\tone\n2\ttwo\n3\tthree\n";
        let statements = build_load_inserts(&spec(), content, 2);
        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("(\'1\', \'one\'), (\'2\', \'two\')"));
        assert!(statements[1].contains("(\'3\', \'three\')"));

        let zero_batch = build_load_inserts(&spec(), content, 0);
        assert_eq!(zero_batch.len(), 3);
    }
}

/// Execute an all-INNER join chain over base tables in a cost-based order.
/// Loads each table (with predicate pushdown), then greedily joins starting from
/// the smallest, always extending along an available equi-join predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GuaranteedIndexedJoinAccess {
    pub driver_table: String,
    pub partner_table: String,
    pub index_name: String,
    pub access_type: &'static str,
}

async fn stored_base_table_name(db: &Session, factor: &TableFactor) -> Result<Option<String>> {
    let TableFactor::Table { name, .. } = factor else {
        return Ok(None);
    };
    let table = match stored_table_ident(db, name) {
        Ok(table) => table,
        Err(Error::Catalog(_) | Error::UnknownDatabase(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok(catalog::exists(db, &table).await?.then_some(table))
}

/// Report the subset of selective joins that is guaranteed to use delayed
/// indexed probes: a two-table INNER equi-join with exactly one indexable local
/// predicate, that predicate being a point lookup on the driver's single-column
/// primary key.  The PK lookup guarantees at most one driving row, so execution
/// cannot cross [`NLJ_MAX_DRIVING`] and fall back to materialising the partner.
///
/// This is deliberately narrower than the optimizer.  It is suitable for
/// truthful EXPLAIN metadata without reading table rows or changing session
/// state; plans outside the guaranteed subset simply return `None`.
pub(crate) async fn guaranteed_indexed_join_access(
    db: &Session,
    select: &Select,
) -> Result<Option<GuaranteedIndexedJoinAccess>> {
    if select.from.len() != 1 {
        return Ok(None);
    }
    let twj = &select.from[0];
    if twj.joins.len() != 1
        || !stored_table_factor(&twj.relation)
        || !stored_table_factor(&twj.joins[0].relation)
    {
        return Ok(None);
    }
    if stored_base_table_name(db, &twj.relation).await?.is_none()
        || stored_base_table_name(db, &twj.joins[0].relation)
            .await?
            .is_none()
    {
        return Ok(None);
    }
    let (kind, on) = join_kind(&twj.joins[0].join_operator)?;
    if kind != JoinKind::Inner {
        return Ok(None);
    }
    let Some(on) = on else { return Ok(None) };
    if !matches!(
        on,
        Expr::BinaryOp {
            op: sqlparser::ast::BinaryOperator::Eq,
            ..
        }
    ) {
        return Ok(None);
    }

    let (left_def, left_cols) = resolve_table(db, &twj.relation).await?;
    let (right_def, right_cols) = resolve_table(db, &twj.joins[0].relation).await?;
    let mut conjuncts = Vec::new();
    if let Some(filter) = &select.selection {
        split_and(filter, &mut conjuncts);
    }
    let left_schema = Schema::new(left_cols.clone());
    let right_schema = Schema::new(right_cols.clone());
    let pk_point = |def: &TableDef, schema: &Schema| -> Result<bool> {
        let mut found = false;
        for conjunct in &conjuncts {
            if !refs_in_schema(conjunct, schema) {
                continue;
            }
            if let Some((column, _)) = eq_col_literal(def, Some(conjunct))? {
                if def.pk_cols == [column] {
                    found = true;
                } else if index::index_on(def, column).is_some() {
                    // Both sides would be optimizer candidates; without reading
                    // rows we cannot guarantee which one becomes the driver.
                    return Ok(false);
                }
            }
        }
        Ok(found)
    };
    let left_point = pk_point(&left_def, &left_schema)?;
    let right_point = pk_point(&right_def, &right_schema)?;
    let (driver_schema, partner_schema, driver_table, partner_def) = match (left_point, right_point)
    {
        (true, false) => (left_schema, right_schema, left_def.name.clone(), right_def),
        (false, true) => (right_schema, left_schema, right_def.name.clone(), left_def),
        _ => return Ok(None),
    };
    let Some((_, partner_col)) = equi_nlj(&on, &driver_schema, &partner_schema) else {
        return Ok(None);
    };
    if partner_def.pk_cols == [partner_col] {
        return Ok(Some(GuaranteedIndexedJoinAccess {
            driver_table,
            partner_table: partner_def.name,
            index_name: "PRIMARY".into(),
            access_type: "eq_ref",
        }));
    }
    Ok(
        index::index_on(&partner_def, partner_col).map(|idx| GuaranteedIndexedJoinAccess {
            driver_table,
            partner_table: partner_def.name.clone(),
            index_name: idx.name.clone(),
            access_type: "ref",
        }),
    )
}

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
    for relation in &relations {
        if stored_base_table_name(db, relation).await?.is_none() {
            return Ok(None);
        }
    }

    // Resolve and estimate each relation without reading its rows.  In particular,
    // do not eagerly scan a large future partner: once a selective relation has
    // become the driver we may be able to probe that partner's index directly.
    struct Candidate<'a> {
        relation: &'a TableFactor,
        def: TableDef,
        cols: Vec<ColumnDef>,
        est: u64,
        accelerable: bool,
    }
    let mut candidates: Vec<Candidate<'_>> = Vec::with_capacity(relations.len());
    for rel in &relations {
        let (def, cols) = resolve_table(db, rel).await?;
        let schema = Schema::new(cols.clone());
        let accelerable = conjuncts
            .iter()
            .any(|c| refs_in_schema(c, &schema) && is_accelerable(&def, c).unwrap_or(false));
        let est = catalog::load_stats(db, &def.name)
            .await?
            .map(|stats| estimate_filtered_rows(&stats, conjuncts))
            .unwrap_or(u64::MAX);
        candidates.push(Candidate {
            relation: rel,
            def,
            cols,
            est,
            accelerable,
        });
    }

    // Prefer a relation with an indexable local predicate even before ANALYZE has
    // produced statistics.  Loading it is itself an index lookup and gives the
    // exact (usually tiny) driving cardinality.
    let mut remaining: Vec<usize> = (0..candidates.len()).collect();
    remaining.sort_by_key(|&i| (!candidates[i].accelerable, candidates[i].est));
    let start = remaining.remove(0);
    let (mut cur_cols, mut cur_rows) =
        load_relation(db, vindex, candidates[start].relation, conjuncts).await?;
    cur_rows = apply_pushdown(cur_rows, &cur_cols, conjuncts)?;

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
            let t_aliases = relation_aliases(&candidates[i].cols);
            for pred in &on_preds {
                if let Some((lq, rq)) = equi_qualifiers(pred) {
                    let connects = (relation_aliases_contain(&cur_aliases, &lq)
                        && relation_aliases_contain(&t_aliases, &rq))
                        || (relation_aliases_contain(&cur_aliases, &rq)
                            && relation_aliases_contain(&t_aliases, &lq));
                    if connects && candidates[i].est < best_est {
                        best_est = candidates[i].est;
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
        let candidate = &candidates[idx];
        let left_schema = Schema::new(cur_cols);
        let right_schema = Schema::new(candidate.cols.clone());

        // A selective driver plus an indexed equality on the next table is the
        // key case: fetch only matching partner rows instead of scanning and
        // hashing the complete partner.  Partner-local WHERE predicates remain
        // pushdowns, and the full WHERE is still evaluated after the join.
        if cur_rows.len() <= NLJ_MAX_DRIVING {
            if let Some((driving_key, partner_col)) = equi_nlj(&pred, &left_schema, &right_schema) {
                if candidate.def.pk_cols == [partner_col]
                    || index::index_on(&candidate.def, partner_col).is_some()
                {
                    let mut out = Vec::new();
                    let mut check = db.cancel_check();
                    for left in &cur_rows {
                        check.tick()?;
                        let key = predicate::eval_row(&driving_key, &left_schema, left)?;
                        if key.is_null() {
                            continue;
                        }
                        let matches =
                            lookup_rows_by_eq(db, &candidate.def, partner_col, &key).await?;
                        for partner in apply_pushdown(matches, &candidate.cols, conjuncts)? {
                            let mut combined = Vec::with_capacity(left.len() + partner.len());
                            combined.extend_from_slice(left);
                            combined.extend(partner);
                            out.push(combined);
                        }
                    }
                    cur_cols = left_schema.columns;
                    cur_cols.extend(candidate.cols.clone());
                    cur_rows = out;
                    continue;
                }
            }
        }

        let (rcols, mut rrows) = load_relation(db, vindex, candidate.relation, conjuncts).await?;
        rrows = apply_pushdown(rrows, &rcols, conjuncts)?;
        let right_schema = Schema::new(rcols);
        let cancel = db.cancel_token();
        let (c, r) = cpu_bound(|| {
            combine(
                &left_schema,
                &cur_rows,
                &right_schema,
                &rrows,
                JoinKind::Inner,
                Some(JoinCondition::On(&pred)),
                &cancel,
            )
        })?;
        cur_cols = c.columns;
        cur_rows = r;
    }
    Ok(Some((cur_cols, cur_rows)))
}

/// Structured alias/table qualifiers present in a relation.
fn relation_aliases(cols: &[ColumnDef]) -> std::collections::HashSet<Vec<String>> {
    cols.iter()
        .filter(|column| !column.qualifier.is_empty())
        .map(|column| column.qualifier.clone())
        .collect()
}

fn qualifier_component_suffix(stored: &[String], reference: &[String]) -> bool {
    reference.len() <= stored.len()
        && stored[stored.len() - reference.len()..]
            .iter()
            .zip(reference)
            .all(|(stored, reference)| stored == reference)
}

fn relation_aliases_contain(
    relations: &std::collections::HashSet<Vec<String>>,
    reference: &[String],
) -> bool {
    relations
        .iter()
        .any(|relation| qualifier_component_suffix(relation, reference))
}

/// For an equi predicate `A.x = B.y`, the two operand qualifiers `(a, b)`.
fn equi_qualifiers(pred: &Expr) -> Option<(Vec<String>, Vec<String>)> {
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
fn comma_join_chain(
    db: &Session,
    from: &[TableWithJoins],
    selection: Option<&Expr>,
) -> Option<TableWithJoins> {
    use sqlparser::ast::Join;
    if from.len() < 2
        || !from
            .iter()
            .all(|t| t.joins.is_empty() && matches!(t.relation, TableFactor::Table { .. }))
    {
        return None;
    }
    let quals: Vec<Vec<String>> = from
        .iter()
        .map(|table| {
            factor_qualifier_object(db, &table.relation)
                .map(|qualifier| qualifier.0.iter().map(|part| part.value.clone()).collect())
        })
        .collect::<Option<Vec<_>>>()?;
    let mut conjuncts = Vec::new();
    if let Some(f) = selection {
        split_and(f, &mut conjuncts);
    }
    // (qual_a, qual_b, predicate) for each equi-conjunct connecting two tables.
    let equis: Vec<(Vec<String>, Vec<String>, &Expr)> = conjuncts
        .iter()
        .filter_map(|c| equi_qualifiers(c).map(|(a, b)| (a, b, c)))
        .collect();

    let mut used = vec![false; from.len()];
    used[0] = true;
    let mut acc: std::collections::HashSet<Vec<String>> = [quals[0].clone()].into_iter().collect();
    let mut joins: Vec<Join> = Vec::with_capacity(from.len() - 1);
    for _ in 1..from.len() {
        let mut found: Option<(usize, Expr)> = None;
        'outer: for (i, q) in quals.iter().enumerate() {
            if used[i] {
                continue;
            }
            for (a, b, e) in &equis {
                if (qualifier_component_suffix(q, a)
                    && acc
                        .iter()
                        .any(|relation| qualifier_component_suffix(relation, b)))
                    || (qualifier_component_suffix(q, b)
                        && acc
                            .iter()
                            .any(|relation| qualifier_component_suffix(relation, a)))
                {
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

fn expr_qualifier(e: &Expr) -> Option<Vec<String>> {
    match e {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => Some(
            parts[..parts.len() - 1]
                .iter()
                .map(|part| part.value.clone())
                .collect(),
        ),
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

#[derive(Clone, Debug)]
struct UsingKey {
    name: String,
    left: usize,
    right: usize,
}

/// Columns exposed by an unqualified `*`, excluding only the operand indexes a
/// USING/NATURAL join marked as qualified-access-only.
pub(crate) fn unqualified_wildcard_indices(schema: &Schema) -> Vec<usize> {
    schema.unqualified_indices().collect()
}

fn find_logical_column(schema: &Schema, name: &str, side: &str) -> Result<usize> {
    let matches: Vec<usize> = unqualified_wildcard_indices(schema)
        .into_iter()
        .filter(|&index| predicate::identifier_eq(column_name(&schema.columns[index]), name))
        .collect();
    match matches.as_slice() {
        [index] => Ok(*index),
        [] => Err(Error::Catalog(format!(
            "unknown column '{name}' in {side} relation of USING/NATURAL join"
        ))),
        _ => Err(Error::Query(format!(
            "ambiguous column '{name}' in {side} relation of USING/NATURAL join"
        ))),
    }
}

fn join_constraint(operator: &JoinOperator) -> Option<&JoinConstraint> {
    match operator {
        JoinOperator::Inner(constraint)
        | JoinOperator::LeftOuter(constraint)
        | JoinOperator::RightOuter(constraint)
        | JoinOperator::FullOuter(constraint) => Some(constraint),
        _ => None,
    }
}

/// Resolve USING/NATURAL column pairs in MySQL's logical output order. For
/// INNER/LEFT joins the left operand is first; RIGHT joins behave as if their
/// operands were swapped, so the right operand determines key and unique-column
/// order.
fn resolve_using_keys(
    operator: &JoinOperator,
    left: &Schema,
    right: &Schema,
) -> Result<Option<Vec<UsingKey>>> {
    let Some(constraint) = join_constraint(operator) else {
        return Ok(None);
    };
    let requested = match constraint {
        JoinConstraint::Using(columns) => Some(
            columns
                .iter()
                .map(|column| column.value.clone())
                .collect::<Vec<_>>(),
        ),
        JoinConstraint::Natural => None,
        JoinConstraint::On(_) | JoinConstraint::None => return Ok(None),
    };

    if let Some(names) = &requested {
        let mut seen = Vec::<String>::new();
        for name in names {
            if seen
                .iter()
                .any(|existing| predicate::identifier_eq(existing, name))
            {
                return Err(Error::Query(format!(
                    "column '{name}' appears more than once in USING"
                )));
            }
            seen.push(name.clone());
            find_logical_column(left, name, "left")?;
            find_logical_column(right, name, "right")?;
        }
    }

    let right_first = matches!(operator, JoinOperator::RightOuter(_));
    let first = if right_first { right } else { left };
    let mut names = Vec::new();
    for index in unqualified_wildcard_indices(first) {
        let name = column_name(&first.columns[index]);
        if names
            .iter()
            .any(|seen: &String| predicate::identifier_eq(seen, name))
        {
            continue;
        }
        let selected = requested.as_ref().map_or_else(
            || {
                let other_schema = if right_first { left } else { right };
                unqualified_wildcard_indices(other_schema)
                    .into_iter()
                    .any(|other| {
                        predicate::identifier_eq(column_name(&other_schema.columns[other]), name)
                    })
            },
            |using| {
                using
                    .iter()
                    .any(|column| predicate::identifier_eq(column, name))
            },
        );
        if selected {
            names.push(name.to_owned());
        }
    }

    names
        .into_iter()
        .map(|name| {
            Ok(UsingKey {
                left: find_logical_column(left, &name, "left")?,
                right: find_logical_column(right, &name, "right")?,
                name,
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn column_def_expr(column: &ColumnDef) -> Expr {
    let mut parts = column
        .qualifier
        .iter()
        .cloned()
        .map(sqlparser::ast::Ident::new)
        .collect::<Vec<_>>();
    parts.push(sqlparser::ast::Ident::new(column_name(column)));
    match parts.as_slice() {
        [identifier] => Expr::Identifier(identifier.clone()),
        _ => Expr::CompoundIdentifier(parts),
    }
}

fn using_key_pairs(keys: &[UsingKey], left: &Schema, right: &Schema) -> Vec<(Expr, Expr)> {
    keys.iter()
        .map(|key| {
            (
                column_def_expr(&left.columns[key.left]),
                column_def_expr(&right.columns[key.right]),
            )
        })
        .collect()
}

fn coalesce_using_join(
    left: &Schema,
    right: &Schema,
    physical_schema: Schema,
    physical_rows: Vec<Vec<Value>>,
    kind: JoinKind,
    keys: &[UsingKey],
) -> (Schema, Vec<Vec<Value>>) {
    let left_len = left.columns.len();
    let mut columns = keys
        .iter()
        .map(|key| {
            let source = if kind == JoinKind::Right {
                &right.columns[key.right]
            } else {
                &left.columns[key.left]
            };
            let mut column = source.clone();
            column.name = key.name.clone();
            column.qualifier.clear();
            column
        })
        .collect::<Vec<_>>();
    let mut tables = keys
        .iter()
        .map(|key| {
            let (schema, index) = if kind == JoinKind::Right {
                (right, key.right)
            } else {
                (left, key.left)
            };
            schema_column_table(schema, index)
                .unwrap_or_default()
                .to_owned()
        })
        .collect::<Vec<_>>();

    let hidden_bare: std::collections::HashSet<usize> = keys
        .iter()
        .flat_map(|key| {
            [
                (key.left, &left.columns[key.left]),
                (left_len + key.right, &right.columns[key.right]),
            ]
        })
        .filter_map(|(index, column)| column.qualifier.is_empty().then_some(index))
        .collect();
    let physical_order = if kind == JoinKind::Right {
        (left_len..physical_schema.columns.len())
            .chain(0..left_len)
            .collect::<Vec<_>>()
    } else {
        (0..physical_schema.columns.len()).collect()
    };
    let physical_order = physical_order
        .into_iter()
        .filter(|index| !hidden_bare.contains(index))
        .collect::<Vec<_>>();
    columns.extend(
        physical_order
            .iter()
            .map(|&index| physical_schema.columns[index].clone()),
    );
    tables.extend(physical_order.iter().map(|&index| {
        schema_column_table(&physical_schema, index)
            .unwrap_or_default()
            .to_owned()
    }));

    let key_physical: std::collections::HashSet<usize> = keys
        .iter()
        .flat_map(|key| [key.left, left_len + key.right])
        .collect();
    let mut schema = Schema::with_tables(columns, tables);
    for (output_index, &physical_index) in physical_order.iter().enumerate() {
        if physical_schema.is_hidden_from_unqualified(physical_index)
            || key_physical.contains(&physical_index)
        {
            schema.hide_from_unqualified(keys.len() + output_index);
        }
    }
    let output_len = schema.columns.len();

    let rows = physical_rows
        .into_iter()
        .map(|row| {
            let mut output = Vec::with_capacity(output_len);
            output.extend(keys.iter().map(|key| {
                let left_value = &row[key.left];
                let right_value = &row[left_len + key.right];
                match kind {
                    JoinKind::Right => right_value.clone(),
                    JoinKind::Full if left_value.is_null() => right_value.clone(),
                    JoinKind::Inner | JoinKind::Left | JoinKind::Full => left_value.clone(),
                }
            }));
            output.extend(physical_order.iter().map(|&index| row[index].clone()));
            output
        })
        .collect();
    (schema, rows)
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

#[derive(Clone, Copy)]
enum JoinCondition<'a> {
    On(&'a Expr),
    ResolvedKeys(&'a [(Expr, Expr)]),
}

/// Combine two materialised relations under a join kind. Compatible equi-`ON`
/// and resolved USING/NATURAL keys use a hash join; other conditions use the
/// general nested-loop evaluator.
#[allow(clippy::too_many_arguments)]
fn combine(
    lschema: &Schema,
    lrows: &[Vec<Value>],
    rschema: &Schema,
    rrows: &[Vec<Value>],
    kind: JoinKind,
    condition: Option<JoinCondition<'_>>,
    cancel: &std::sync::Arc<elyra_core::cancel::QueryCancel>,
) -> Result<(Schema, Vec<Vec<Value>>)> {
    let schema = combined_join_schema(lschema, rschema);

    // Validate analyzable ON references against the actual combined schema
    // before a hash/merge optimization splits the expression back into its two
    // inputs. Otherwise a bare name that is valid on the accumulated left side
    // can bypass the ambiguity introduced by the new right side.
    if let Some(JoinCondition::On(expression)) = condition {
        validate_join_on_refs(expression, &schema)?;
    }

    // Hash join for equi INNER/LEFT/RIGHT (cost-based build side). For large
    // INNER equi-joins whose inputs are already sorted on the join key (e.g.
    // clustered primary-key scans), use a streaming merge join instead — no hash
    // table, and the output stays ordered.
    let inferred_keys = match condition {
        Some(JoinCondition::On(expression)) => equi_key_pairs(expression, lschema, rschema),
        Some(JoinCondition::ResolvedKeys(_)) | None => None,
    };
    let key_pairs = match condition {
        Some(JoinCondition::On(_)) => inferred_keys.as_deref(),
        Some(JoinCondition::ResolvedKeys(keys)) => Some(keys),
        None => None,
    };
    if matches!(kind, JoinKind::Inner | JoinKind::Left | JoinKind::Right) {
        if let Some(keys) = key_pairs
            .filter(|keys| !keys.is_empty() && hash_key_pairs_compatible(keys, lschema, rschema))
        {
            const MERGE_MIN: usize = 2048;
            if let [(lkey, rkey)] = keys {
                // The merge join compares keys under the default collation,
                // so skip it for a `_bin` join key (fall through to the
                // collation-aware hash join below).
                let bin_key = join_key_collation(lkey, lschema, rkey, rschema).is_bin();
                if !bin_key
                    && kind == JoinKind::Inner
                    && lrows.len() >= MERGE_MIN
                    && rrows.len() >= MERGE_MIN
                {
                    if let (Some(lk), Some(rk)) = (
                        sorted_keyed(lrows, lschema, lkey)?,
                        sorted_keyed(rrows, rschema, rkey)?,
                    ) {
                        if let Some(out) = merge_join_inner(lk, rk) {
                            return Ok((schema, out));
                        }
                    }
                }
            }
            let rows = hash_join(lrows, rrows, lschema, rschema, keys, kind, cancel)?;
            return Ok((schema, rows));
        }
    }

    let llen = lschema.columns.len();
    let rlen = rschema.columns.len();
    let mut out = Vec::new();
    let mut right_matched = vec![false; rrows.len()];

    // The product of two relations is quadratic, so this is the loop a cross
    // join spends its time in: check the statement's deadline as we go.
    let mut check = elyra_core::cancel::CancelCheck::new(cancel.clone());
    // Bound the buffered product: without this a cross join grows until the
    // process is killed. Released when this join finishes, errors, or unwinds.
    let mut budget = JoinBudget::new();

    // Fast path for a simple cross-relation comparison (non-equi join like
    // `a.v < b.v`): compare two values directly per pair and clone only when
    // the condition matches, skipping the general predicate evaluator and its
    // repeated schema lookups.
    let fast_cmp = condition.as_ref().and_then(|c| {
        if let JoinCondition::On(expression) = c {
            cross_comparison(expression, lschema, rschema)
        } else {
            None
        }
    });

    if let Some(ref fc) = fast_cmp {
        for l in lrows {
            check.tick()?;
            let mut matched = false;
            for (ri, r) in rrows.iter().enumerate() {
                #[cfg(test)]
                NESTED_JOIN_COMPARISONS.with(|c| c.set(c.get() + 1));
                if !fc.matches_row(l, r) {
                    continue;
                }
                budget.account(out.len())?;
                let mut combined = l.clone();
                combined.extend_from_slice(r);
                budget.sample(&combined);
                out.push(combined);
                matched = true;
                right_matched[ri] = true;
            }
            if matches!(kind, JoinKind::Left | JoinKind::Full) && !matched {
                let mut combined = l.clone();
                combined.extend(std::iter::repeat_n(Value::Null, rlen));
                out.push(combined);
            }
        }
    } else {
        for l in lrows {
            let mut matched = false;
            for (ri, r) in rrows.iter().enumerate() {
                #[cfg(test)]
                NESTED_JOIN_COMPARISONS.with(|c| c.set(c.get() + 1));
                check.tick()?;
                budget.account(out.len())?;
                let mut combined = l.clone();
                combined.extend_from_slice(r);
                budget.sample(&combined);
                let keep = match condition {
                    Some(JoinCondition::On(expression)) => {
                        predicate::matches(expression, &schema, &combined)?
                    }
                    Some(JoinCondition::ResolvedKeys(keys)) => {
                        resolved_key_pairs_match(l, lschema, r, rschema, keys)?
                    }
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
    Ok((schema, out))
}

/// Physical joined schema with lookup visibility and result metadata preserved.
fn combined_join_schema(lschema: &Schema, rschema: &Schema) -> Schema {
    let mut cols = lschema.columns.clone();
    cols.extend_from_slice(&rschema.columns);
    let tables = (0..lschema.columns.len())
        .map(|index| {
            schema_column_table(lschema, index)
                .unwrap_or_default()
                .to_owned()
        })
        .chain((0..rschema.columns.len()).map(|index| {
            schema_column_table(rschema, index)
                .unwrap_or_default()
                .to_owned()
        }))
        .collect();
    let mut schema = Schema::with_tables(cols, tables);
    for index in 0..lschema.columns.len() {
        if lschema.is_hidden_from_unqualified(index) {
            schema.hide_from_unqualified(index);
        }
    }
    let left_len = lschema.columns.len();
    for index in 0..rschema.columns.len() {
        if rschema.is_hidden_from_unqualified(index) {
            schema.hide_from_unqualified(left_len + index);
        }
    }

    schema
}

/// Schema visible to GROUP BY, HAVING, and ORDER BY after projection aliases
/// have been introduced. A bare projection alias takes precedence over a
/// same-named input column, while the input remains available when qualified.
fn projected_expression_schema(source: &Schema, output: &Schema) -> Schema {
    let mut schema = combined_join_schema(output, source);
    let output_len = output.columns.len();
    for (index, column) in source.columns.iter().enumerate() {
        if output
            .columns
            .iter()
            .any(|projected| predicate::identifier_eq(column_name(projected), column_name(column)))
        {
            schema.hide_from_unqualified(output_len + index);
        }
    }
    schema
}

fn resolved_key_pairs_match(
    left_row: &[Value],
    left_schema: &Schema,
    right_row: &[Value],
    right_schema: &Schema,
    keys: &[(Expr, Expr)],
) -> Result<bool> {
    for (left, right) in keys {
        let left_value = predicate::eval_row(left, left_schema, left_row)?;
        let right_value = predicate::eval_row(right, right_schema, right_row)?;
        let collation = join_key_collation(left, left_schema, right, right_schema);
        if !left_value
            .compare_coll(&right_value, collation)
            .is_some_and(std::cmp::Ordering::is_eq)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
std::thread_local! {
    static NESTED_JOIN_COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
    keys: &[(Expr, Expr)],
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
    let collations = keys
        .iter()
        .map(|(left, right)| join_key_collation(left, lschema, right, rschema))
        .collect::<Vec<_>>();
    let left_keys = keys.iter().map(|(left, _)| left).collect::<Vec<_>>();
    let right_keys = keys.iter().map(|(_, right)| right).collect::<Vec<_>>();
    let mut out = Vec::new();

    if build_left {
        let mut table: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for (i, l) in lrows.iter().enumerate() {
            check.tick()?;
            if let Some(k) = composite_key_bytes(l, lschema, &left_keys, &collations)? {
                table.entry(k).or_default().push(i);
            }
        }
        for r in rrows {
            check.tick()?;
            let probe = composite_key_bytes(r, rschema, &right_keys, &collations)?;
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
            if let Some(k) = composite_key_bytes(r, rschema, &right_keys, &collations)? {
                table.entry(k).or_default().push(i);
            }
        }
        for l in lrows {
            check.tick()?;
            let probe = composite_key_bytes(l, lschema, &left_keys, &collations)?;
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
        Expr::Identifier(id) => {
            predicate::resolve_index_parts(std::slice::from_ref(id), schema).ok()
        }
        // `@@session.var` arrives here too, and fails to resolve -> None.
        Expr::CompoundIdentifier(parts) => predicate::resolve_index_parts(parts, schema).ok(),
        Expr::Nested(inner) => expr_col_index(inner, schema),
        _ => None,
    }
}

fn composite_key_bytes(
    row: &[Value],
    schema: &Schema,
    expressions: &[&Expr],
    collations: &[elyra_core::Collation],
) -> Result<Option<Vec<u8>>> {
    let mut composite = Vec::new();
    for (expression, &collation) in expressions.iter().zip(collations) {
        let value = predicate::eval_row(expression, schema, row)?;
        let Some(bytes) = key_bytes_coll(&value, collation) else {
            return Ok(None);
        };
        composite.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
        composite.extend_from_slice(&bytes);
    }
    Ok(Some(composite))
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

/// A pre-solved comparison between a column in the left schema and a column in
/// the right schema. Recognising one (for a non-equi join like `a.v < b.v`) lets
/// the nested-loop join compare two values directly per pair — neither cloning
/// the full combined row nor evaluating the general predicate — and only clone
/// for rows that survive the condition.
struct CrossComparison {
    left_col: usize,
    right_col: usize,
    bin: bool,
    op: ComparisonOp,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CrossComparison {
    #[inline]
    fn matches_row(&self, lrow: &[Value], rrow: &[Value]) -> bool {
        if lrow[self.left_col].is_null() || rrow[self.right_col].is_null() {
            return false;
        }
        let coll = if self.bin {
            elyra_core::Collation::Bin
        } else {
            elyra_core::Collation::Ci
        };
        let ordering = lrow[self.left_col].compare_coll(&rrow[self.right_col], coll);
        match self.op {
            ComparisonOp::Eq => ordering == Some(std::cmp::Ordering::Equal),
            ComparisonOp::NotEq => ordering.is_some_and(|o| o != std::cmp::Ordering::Equal),
            ComparisonOp::Lt => ordering == Some(std::cmp::Ordering::Less),
            ComparisonOp::LtEq => ordering.is_some_and(|o| o != std::cmp::Ordering::Greater),
            ComparisonOp::Gt => ordering == Some(std::cmp::Ordering::Greater),
            ComparisonOp::GtEq => ordering.is_some_and(|o| o != std::cmp::Ordering::Less),
        }
    }
}

/// If `on` is `A <op> B` where `A` references only the left relation and `B`
/// only the right (or vice-versa), return a pre-solved [`CrossComparison`].
/// Falls back to `None` for anything more complex (literals, compound
/// expressions), leaving the caller to use the general predicate evaluator.
fn cross_comparison(on: &Expr, lschema: &Schema, rschema: &Schema) -> Option<CrossComparison> {
    use sqlparser::ast::BinaryOperator as B;
    let Expr::BinaryOp { left, op, right } = on else {
        return None;
    };
    let op = match op {
        B::Eq => ComparisonOp::Eq,
        B::NotEq => ComparisonOp::NotEq,
        B::Lt => ComparisonOp::Lt,
        B::LtEq => ComparisonOp::LtEq,
        B::Gt => ComparisonOp::Gt,
        B::GtEq => ComparisonOp::GtEq,
        _ => return None,
    };
    let (left_col, right_col) = if refs_in_schema(left, lschema) && refs_in_schema(right, rschema) {
        (
            expr_col_index(left, lschema)?,
            expr_col_index(right, rschema)?,
        )
    } else if refs_in_schema(right, lschema) && refs_in_schema(left, rschema) {
        (
            expr_col_index(right, lschema)?,
            expr_col_index(left, rschema)?,
        )
    } else {
        return None;
    };
    let bin = expr_collation(left, lschema).is_bin() || expr_collation(right, rschema).is_bin();
    Some(CrossComparison {
        left_col,
        right_col,
        bin,
        op,
    })
}

/// A pre-parsed WHERE filter for the streaming join paths. Splits an
/// `AND`-connected predicate into individual conjuncts and resolves every
/// simple `col <op> col` comparison to column indices once, so the per-row
/// closure avoids the general expression evaluator and its repeated schema
/// lookups. Conjuncts that cannot be resolved fall through to the general
/// `predicate::matches` evaluator.
struct FastFilter {
    parts: Vec<FastFilterPart>,
}

enum FastFilterPart {
    Simple(SimpleConjunct),
    Complex(Box<Expr>),
}

struct SimpleConjunct {
    left: usize,
    right: usize,
    op: ComparisonOp,
    bin: bool,
}

impl FastFilter {
    fn build(filter: &Expr, schema: &Schema) -> Self {
        let mut conjuncts = Vec::new();
        split_and(filter, &mut conjuncts);
        let mut parts = Vec::with_capacity(conjuncts.len());
        for c in conjuncts {
            match simple_conjunct(&c, schema) {
                Some(sc) => parts.push(FastFilterPart::Simple(sc)),
                None => parts.push(FastFilterPart::Complex(Box::new(c))),
            }
        }
        FastFilter { parts }
    }

    #[inline]
    fn matches(&self, row: &[Value], schema: &Schema) -> Result<bool> {
        for p in &self.parts {
            match p {
                FastFilterPart::Simple(sc) => {
                    if !sc.matches_row(row) {
                        return Ok(false);
                    }
                }
                FastFilterPart::Complex(expr) => {
                    if !predicate::matches(expr, schema, row)? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }
}

impl SimpleConjunct {
    #[inline]
    fn matches_row(&self, row: &[Value]) -> bool {
        if row[self.left].is_null() || row[self.right].is_null() {
            return false;
        }
        let coll = if self.bin {
            elyra_core::Collation::Bin
        } else {
            elyra_core::Collation::Ci
        };
        let ordering = row[self.left].compare_coll(&row[self.right], coll);
        match self.op {
            ComparisonOp::Eq => ordering == Some(std::cmp::Ordering::Equal),
            ComparisonOp::NotEq => ordering.is_some_and(|o| o != std::cmp::Ordering::Equal),
            ComparisonOp::Lt => ordering == Some(std::cmp::Ordering::Less),
            ComparisonOp::LtEq => ordering.is_some_and(|o| o != std::cmp::Ordering::Greater),
            ComparisonOp::Gt => ordering == Some(std::cmp::Ordering::Greater),
            ComparisonOp::GtEq => ordering.is_some_and(|o| o != std::cmp::Ordering::Less),
        }
    }
}

/// Try to resolve a conjunct into a pre-solved column-index comparison.
fn simple_conjunct(expr: &Expr, schema: &Schema) -> Option<SimpleConjunct> {
    use sqlparser::ast::BinaryOperator as B;
    let Expr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    let op = match op {
        B::Eq => ComparisonOp::Eq,
        B::NotEq => ComparisonOp::NotEq,
        B::Lt => ComparisonOp::Lt,
        B::LtEq => ComparisonOp::LtEq,
        B::Gt => ComparisonOp::Gt,
        B::GtEq => ComparisonOp::GtEq,
        _ => return None,
    };
    let li = expr_col_index(left, schema)?;
    let ri = expr_col_index(right, schema)?;
    let bin = expr_collation(left, schema).is_bin() || expr_collation(right, schema).is_bin();
    Some(SimpleConjunct {
        left: li,
        right: ri,
        op,
        bin,
    })
}

/// Extract every cross-relation equality from a predicate made entirely of
/// `AND`-connected equi predicates. These become one composite hash key, so a
/// multi-column USING/NATURAL join does not fall through to a quadratic scan.
fn equi_key_pairs(on: &Expr, lschema: &Schema, rschema: &Schema) -> Option<Vec<(Expr, Expr)>> {
    let mut conjuncts = Vec::new();
    split_and(on, &mut conjuncts);
    conjuncts
        .iter()
        .map(|conjunct| equi_keys(conjunct, lschema, rschema))
        .collect()
}

/// Hash only direct-column pairs whose type-tagged keys preserve every equality
/// that [`Value::compare`] can report. Other pairs retain the general nested
/// evaluator, including SQL's numeric/text and decimal/integer coercions.
fn hash_key_pairs_compatible(keys: &[(Expr, Expr)], left: &Schema, right: &Schema) -> bool {
    keys.iter().all(|(left_key, right_key)| {
        let Some(left_type) = expr_col_index(left_key, left)
            .and_then(|index| left.columns.get(index))
            .map(|column| &column.ty)
        else {
            return false;
        };
        let Some(right_type) = expr_col_index(right_key, right)
            .and_then(|index| right.columns.get(index))
            .map(|column| &column.ty)
        else {
            return false;
        };
        hash_key_types_compatible(left_type, right_type)
    })
}

fn hash_key_types_compatible(left: &ColumnType, right: &ColumnType) -> bool {
    use ColumnType::{Bool, Bytes, Date, DateTime, Decimal, Int, Json, Text, Time, UInt};

    match (left, right) {
        (Int | UInt, Int | UInt)
        | (Bool, Bool)
        | (Bytes, Bytes)
        | (Date, Date)
        | (DateTime, DateTime)
        | (Time, Time)
        | (Text | Json, Text | Json) => true,
        (Decimal(_, left_scale), Decimal(_, right_scale)) => left_scale == right_scale,
        _ => false,
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
    refs.iter().all(|reference| match reference {
        Expr::Identifier(id) => predicate::resolve_index(&id.value, schema).is_ok(),
        Expr::CompoundIdentifier(parts) => predicate::resolve_index_parts(parts, schema).is_ok(),
        _ => false,
    })
}

/// Collect column references from `expr`. Returns false if the expression
/// contains a construct we do not analyse (so callers stay conservative).
fn collect_refs<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) -> bool {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            out.push(expr);
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

fn validate_join_on_refs(expr: &Expr, schema: &Schema) -> Result<()> {
    let mut references = Vec::new();
    if !collect_refs(expr, &mut references) {
        return Ok(());
    }
    for reference in references {
        // Use the expression evaluator's exact identifier rules so niladic
        // functions and system variables remain valid while unknown or
        // ambiguous columns still fail before an optimized join path.
        predicate::eval_row(reference, schema, &[])?;
    }
    Ok(())
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
        .position(|c| predicate::identifier_eq(&c.name, name))
    else {
        return Ok(None);
    };
    // Coerce the literal to the column's type so index/PK key encoding matches
    // the stored entries (e.g. a DATE column vs a '2024-01-01' text literal).
    let col = &def.schema.columns[idx];
    let original = eval_expr(lit_expr)?;
    match coerce(original.clone(), &col.ty, &col.name) {
        // A literal the column cannot hold exactly (`k = 1024.5` on an INT key)
        // matches nothing; seeking the rounded key would match the wrong row, so
        // the lookup is declined and the scan's filter answers it.
        Ok(v) if !v.is_null() && coercion_is_exact(&original, &v) => Ok(Some((idx, v))),
        _ => Ok(None),
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
        let Some(col) = def
            .schema
            .columns
            .iter()
            .position(|c| predicate::identifier_eq(&c.name, name))
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
                Some(v) => match coerce(v.clone(), &coldef.ty, &coldef.name) {
                    // As for `=`: a rounded key would match a row the literal does
                    // not, so an inexact member sends the whole list to a scan.
                    Ok(c) if coercion_is_exact(&v, &c) => vals.push(c),
                    Ok(_) => {
                        usable = false;
                        break;
                    }
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
        .position(|c| predicate::identifier_eq(&c.name, name))
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
    let prefix = def.data_prefix();
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

/// Max distinct rows `SELECT DISTINCT` keeps in its in-memory fast path
/// (`ELYRASQL_DISTINCT_MAX`, default 5,000,000) before spilling to disk.
fn distinct_max() -> usize {
    env_usize("ELYRASQL_DISTINCT_MAX", 5_000_000)
}

/// Approximate byte budget for the in-memory DISTINCT fast path. Row-count
/// limits alone are unsafe for wide projections.
fn distinct_max_bytes() -> usize {
    env_usize("ELYRASQL_DISTINCT_MAX_BYTES", 256 << 20)
}

fn distinct_resident_exceeds(
    rows: usize,
    bytes: usize,
    row_limit: usize,
    byte_limit: usize,
) -> bool {
    rows > row_limit.max(1) || bytes > byte_limit
}

async fn run_distinct_blocking<F, T>(context: &'static str, work: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| Error::Storage(format!("DISTINCT {context} worker failed: {error}")))?
}

fn distinct_row_key(
    row: &[Value],
    schema: &Schema,
    collations: &[elyra_core::Collation],
) -> Vec<u8> {
    let mut key = Vec::with_capacity(row.len() * 9);
    for (index, value) in row.iter().enumerate() {
        let collation = collations.get(index).copied().unwrap_or_default();
        if matches!(
            schema.columns.get(index).map(|column| &column.ty),
            Some(ColumnType::Float)
        ) {
            if let Some(number) = value.as_mysql_f64() {
                Value::Float(number).push_collation_key_coll(&mut key, collation);
                continue;
            }
        }
        value.push_collation_key_coll(&mut key, collation);
    }
    key
}

/// Deduplicate a projected stream while preserving the first representative of
/// each collation-aware value and the inner query's row order.
///
/// Small results stay on the hash-set fast path. Once `memory_rows` distinct
/// values have accumulated, the retained representatives and all later input
/// are externally sorted by value and ordinal using the independent ORDER BY
/// memory budget. The first ordinal in each value group is then sorted back
/// into input order. Bounded results stay literal; unbounded results are written
/// directly to the frame format consumed by [`RowStream::spill`].
async fn distinct_rows(
    mut input: RowStream,
    offset: usize,
    limit: Option<usize>,
    memory_rows: usize,
    cancel: std::sync::Arc<elyra_core::cancel::QueryCancel>,
) -> Result<RowStream> {
    use std::io::{BufWriter, Write};

    let schema = input.schema.clone();
    let collations: Vec<elyra_core::Collation> = schema
        .columns
        .iter()
        .map(|column| column.collation)
        .collect();
    let mut seen = std::collections::HashSet::new();
    let mut resident: Vec<(u64, Vec<Value>)> = Vec::new();
    let mut resident_bytes = 0usize;
    let mut value_sorter = None;
    let mut ordinal = 0u64;
    let mut check = elyra_core::cancel::CancelCheck::new(cancel.clone());
    check.tick_now()?;

    loop {
        let batch = input.next_batch(8192).await?;
        if batch.is_empty() {
            break;
        }
        if let Some(mut sorter) = value_sorter.take() {
            let batch_len = u64::try_from(batch.len())
                .map_err(|_| Error::Query("SELECT DISTINCT batch is too large".into()))?;
            let start_ordinal = ordinal;
            ordinal = ordinal.checked_add(batch_len).ok_or_else(|| {
                Error::Query("SELECT DISTINCT input row ordinal overflowed".into())
            })?;
            let collations = collations.clone();
            let distinct_schema = schema.clone();
            let blocking_cancel = cancel.clone();
            sorter = run_distinct_blocking("spill", move || -> Result<crate::sort::Sorter> {
                let mut check = elyra_core::cancel::CancelCheck::new(blocking_cancel);
                for (position, row) in batch.into_iter().enumerate() {
                    check.tick()?;
                    push_distinct_candidate(
                        &mut sorter,
                        row,
                        start_ordinal + position as u64,
                        &distinct_schema,
                        &collations,
                    )?;
                }
                Ok(sorter)
            })
            .await?;
            value_sorter = Some(sorter);
            continue;
        }
        for row in batch {
            check.tick()?;
            if let Some(sorter) = &mut value_sorter {
                push_distinct_candidate(sorter, row, ordinal, &schema, &collations)?;
            } else {
                let key = distinct_row_key(&row, &schema, &collations);
                let key_len = key.len();
                if seen.insert(key) {
                    resident_bytes = resident_bytes
                        .saturating_add(key_len.saturating_add(estimated_row_bytes(&row)));
                    resident.push((ordinal, row));
                    if distinct_resident_exceeds(
                        resident.len(),
                        resident_bytes,
                        memory_rows,
                        distinct_max_bytes(),
                    ) {
                        let mut sorter = crate::sort::Sorter::new(
                            vec![true, true],
                            vec![elyra_core::Collation::Bin; 2],
                            0,
                            None,
                            crate::sort::sort_max_rows(),
                        );
                        for (resident_ordinal, resident_row) in resident.drain(..) {
                            push_distinct_candidate(
                                &mut sorter,
                                resident_row,
                                resident_ordinal,
                                &schema,
                                &collations,
                            )?;
                        }
                        seen.clear();
                        seen.shrink_to_fit();
                        resident_bytes = 0;
                        value_sorter = Some(sorter);
                    }
                }
            }
            ordinal = ordinal.checked_add(1).ok_or_else(|| {
                Error::Query("SELECT DISTINCT input row ordinal overflowed".into())
            })?;
        }
    }

    let Some(mut value_sorter) = value_sorter else {
        let mut rows = resident.into_iter().map(|(_, row)| row).collect();
        apply_offset_limit(&mut rows, offset, limit);
        return Ok(RowStream::literal(schema, rows));
    };

    let blocking_cancel = cancel.clone();
    let distinct_schema = schema.clone();
    let order_sorter = run_distinct_blocking("merge", move || -> Result<crate::sort::Sorter> {
        let mut check = elyra_core::cancel::CancelCheck::new(blocking_cancel);
        let mut order_sorter = crate::sort::Sorter::new(
            vec![true],
            vec![elyra_core::Collation::Bin],
            offset,
            limit,
            crate::sort::sort_max_rows(),
        );
        let mut previous_key = None;
        value_sorter.finish_with(|mut candidate| {
            check.tick()?;
            let candidate_ordinal = candidate
                .pop()
                .ok_or_else(|| Error::Storage("DISTINCT spill row missing ordinal".into()))?;
            let key = distinct_row_key(&candidate, &distinct_schema, &collations);
            if previous_key.as_ref() != Some(&key) {
                previous_key = Some(key);
                order_sorter.push(vec![candidate_ordinal], candidate)?;
            }
            Ok(())
        })?;
        Ok(order_sorter)
    })
    .await?;

    if limit.is_some_and(|limit| limit <= crate::sort::sort_max_rows()) {
        let rows = run_distinct_blocking("order", move || {
            let mut order_sorter = order_sorter;
            order_sorter.finish()
        })
        .await?;
        return Ok(RowStream::literal(schema, rows));
    }

    let columns = schema.columns.len();
    let (path, file, numeric_types) = run_distinct_blocking("output", move || {
        let (path, file) = create_distinct_spill()?;
        let mut writer = BufWriter::new(file);
        let mut numeric_types = crate::stream::NumericTypeReconciler::new(columns);
        let mut order_sorter = order_sorter;
        let mut check = elyra_core::cancel::CancelCheck::new(cancel);
        let write_result = order_sorter.finish_with(|row| {
            check.tick()?;
            numeric_types.observe(&row);
            let frame =
                bincode::serialize(&row).map_err(|error| Error::Storage(error.to_string()))?;
            if frame.len() > elyra_core::max_frame_bytes() || frame.len() > u32::MAX as usize {
                return Err(Error::Storage("DISTINCT spill row frame too large".into()));
            }
            writer.write_all(&(frame.len() as u32).to_le_bytes())?;
            writer.write_all(&frame)?;
            Ok(())
        });
        if let Err(error) = write_result {
            drop(writer);
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        if let Err(error) = writer.flush() {
            drop(writer);
            let _ = std::fs::remove_file(&path);
            return Err(Error::Io(error));
        }
        let file = writer.into_inner().map_err(|error| {
            let _ = std::fs::remove_file(&path);
            Error::Io(error.into_error())
        })?;
        Ok((path, file, numeric_types))
    })
    .await?;
    let mut schema = schema;
    numeric_types.reconcile(&mut schema);
    RowStream::spill(schema, path, file)
}

fn push_distinct_candidate(
    sorter: &mut crate::sort::Sorter,
    row: Vec<Value>,
    ordinal: u64,
    schema: &Schema,
    collations: &[elyra_core::Collation],
) -> Result<()> {
    let keys = vec![
        Value::Bytes(distinct_row_key(&row, schema, collations)),
        Value::UInt(ordinal),
    ];
    let mut candidate = row;
    candidate.push(Value::UInt(ordinal));
    sorter.push(keys, candidate)
}

fn create_distinct_spill() -> Result<(std::path::PathBuf, std::fs::File)> {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    loop {
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "elyrasql-sort-{}-distinct-{sequence}.tmp",
            std::process::id()
        ));
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(Error::Io(error)),
        }
    }
}

#[cfg(test)]
mod distinct_spill_tests {
    use super::*;

    #[test]
    fn wide_rows_spill_before_the_row_count_limit() {
        assert!(distinct_resident_exceeds(2, 101, 1_000, 100));
        assert!(!distinct_resident_exceeds(2, 100, 1_000, 100));
    }

    #[tokio::test]
    async fn spill_work_runs_off_the_async_runtime_thread() {
        let runtime_thread = std::thread::current().id();
        let worker_thread = run_distinct_blocking("test", || Ok(std::thread::current().id()))
            .await
            .unwrap();
        assert_ne!(worker_thread, runtime_thread);
    }

    async fn collect(mut stream: RowStream) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        loop {
            let batch = stream.next_batch(2).await.unwrap();
            if batch.is_empty() {
                return rows;
            }
            rows.extend(batch);
        }
    }

    fn text_stream(values: &[&str]) -> RowStream {
        let schema = Schema::new(vec![ColumnDef::new("v", ColumnType::Text, false)]);
        let rows = values
            .iter()
            .map(|value| vec![Value::Text((*value).into())])
            .collect();
        RowStream::literal(schema, rows)
    }

    #[tokio::test]
    async fn spilled_distinct_preserves_first_representative_and_input_order() {
        let stream = distinct_rows(
            text_stream(&["z", "A", "z", "b", "a", "c", "B"]),
            0,
            None,
            2,
            std::sync::Arc::new(elyra_core::cancel::QueryCancel::new()),
        )
        .await
        .unwrap();

        assert_eq!(
            collect(stream).await,
            vec![
                vec![Value::Text("z".into())],
                vec![Value::Text("A".into())],
                vec![Value::Text("b".into())],
                vec![Value::Text("c".into())],
            ]
        );
    }

    #[tokio::test]
    async fn spilled_distinct_applies_offset_and_limit_after_deduplication() {
        let stream = distinct_rows(
            text_stream(&["z", "A", "z", "b", "a", "c", "B"]),
            1,
            Some(2),
            2,
            std::sync::Arc::new(elyra_core::cancel::QueryCancel::new()),
        )
        .await
        .unwrap();

        assert_eq!(
            collect(stream).await,
            vec![vec![Value::Text("A".into())], vec![Value::Text("b".into())]]
        );
    }

    #[tokio::test]
    async fn spilled_distinct_groups_by_the_canonical_key_not_sort_equality() {
        let schema = Schema::new(vec![ColumnDef::new("v", ColumnType::Float, false)]);
        let input = RowStream::literal(
            schema,
            vec![
                vec![Value::Int(1)],
                vec![Value::Float(1.0)],
                vec![Value::Int(1)],
            ],
        );
        let stream = distinct_rows(
            input,
            0,
            None,
            1,
            std::sync::Arc::new(elyra_core::cancel::QueryCancel::new()),
        )
        .await
        .unwrap();

        assert_eq!(stream.schema.columns[0].ty, ColumnType::Int);
        assert_eq!(collect(stream).await, vec![vec![Value::Int(1)]]);
    }

    #[cfg(unix)]
    #[test]
    fn distinct_spill_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let (path, file) = create_distinct_spill().unwrap();
        let mode = file.metadata().unwrap().permissions().mode() & 0o777;
        drop(file);
        let _ = std::fs::remove_file(path);
        assert_eq!(mode, 0o600);
    }

    #[tokio::test]
    async fn thresholds_zero_and_one_match_in_memory_for_mixed_rows() {
        let mut ci = ColumnDef::new("ci", ColumnType::Text, true);
        ci.collation = elyra_core::Collation::Ci;
        let mut bin = ColumnDef::new("bin", ColumnType::Text, true);
        bin.collation = elyra_core::Collation::Bin;
        let schema = Schema::new(vec![ci, bin, ColumnDef::new("n", ColumnType::Float, true)]);
        let rows = vec![
            vec![
                Value::Text("A".into()),
                Value::Text("x".into()),
                Value::Int(1),
            ],
            vec![
                Value::Text("a".into()),
                Value::Text("x".into()),
                Value::Int(1),
            ],
            vec![
                Value::Text("a".into()),
                Value::Text("X".into()),
                Value::Int(1),
            ],
            vec![Value::Null, Value::Null, Value::Decimal(100, 2)],
            vec![Value::Null, Value::Null, Value::Decimal(100, 2)],
            vec![
                Value::Text("b".into()),
                Value::Text("x".into()),
                Value::Float(1.0),
            ],
        ];

        let expected = collect(
            distinct_rows(
                RowStream::literal(schema.clone(), rows.clone()),
                1,
                Some(3),
                usize::MAX,
                std::sync::Arc::new(elyra_core::cancel::QueryCancel::new()),
            )
            .await
            .unwrap(),
        )
        .await;
        for threshold in [0, 1] {
            let actual = collect(
                distinct_rows(
                    RowStream::literal(schema.clone(), rows.clone()),
                    1,
                    Some(3),
                    threshold,
                    std::sync::Arc::new(elyra_core::cancel::QueryCancel::new()),
                )
                .await
                .unwrap(),
            )
            .await;
            assert_eq!(actual, expected, "threshold={threshold}");
        }
    }

    #[tokio::test]
    async fn empty_distinct_preserves_schema_and_limit_zero_is_empty() {
        let schema = Schema::new(vec![ColumnDef::new("v", ColumnType::Float, true)]);
        let stream = distinct_rows(
            RowStream::literal(schema, Vec::new()),
            usize::MAX,
            Some(0),
            0,
            std::sync::Arc::new(elyra_core::cancel::QueryCancel::new()),
        )
        .await
        .unwrap();
        assert_eq!(stream.schema.columns[0].ty, ColumnType::Float);
        assert!(collect(stream).await.is_empty());
    }

    #[tokio::test]
    async fn distinct_observes_preexisting_cancellation() {
        let cancel = std::sync::Arc::new(elyra_core::cancel::QueryCancel::new());
        cancel.cancel();
        let error = match distinct_rows(text_stream(&["a", "b"]), 0, None, 1, cancel).await {
            Ok(_) => panic!("cancelled DISTINCT unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("query cancelled"));
    }
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
                    .position(|c| predicate::identifier_eq(&c.name, n))
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
    Ok(composite_range_bounds(def, filter)?.is_some() || range_bounds(def, filter)?.is_some())
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
    if limit == Some(0) {
        return Ok(Some(out));
    }

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
                        .position(|c| predicate::identifier_eq(&c.name, n))
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
                        for dk in index::fulltext_lookup(db, &def.storage_name(), &idx.name, &stem)
                            .await?
                        {
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
                &def.storage_name(),
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
            let data_keys = index::lookup_eq(db, &def.storage_name(), idx, &vals).await?;
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

    // Composite secondary-index range: equality on a non-empty leading prefix,
    // then a range on the immediately following column.
    if let Some(query) = composite_range_bounds(def, filter)? {
        let budget = index_range_budget(db, def).await?;
        if let Some(candidates) = composite_index_range(db, def, &query, budget).await? {
            for (key, row) in candidates {
                if recheck(&row)? {
                    out.push((key, row));
                    if limit.is_some_and(|limit| out.len() >= limit) {
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

    // Range fast path: `col > x` / `BETWEEN` on a PK or single-column index
    // uses an ordered range scan, then re-applies the full filter.
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

    let prefix = def.data_prefix();
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

fn table_of(db: &Session, twj: &TableWithJoins) -> Result<String> {
    match &twj.relation {
        TableFactor::Table { name, .. } => stored_table_ident(db, name),
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
    qualifier: &[String],
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
            let bound = bind_outer(db, f, qualifier, &def.schema, &row);
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
    limit: Option<usize>,
) -> Result<QueryResult> {
    let hidden = hidden_source_qualifiers(std::slice::from_ref(table));
    let visible = validate_unique_relation_qualifiers(db, std::slice::from_ref(table), "mutation")?;
    if let Some(selection) = selection {
        validate_ast_alias_hiding(selection, &hidden, &visible)?;
    }
    for order in order_by {
        validate_ast_alias_hiding(&order.expr, &hidden, &visible)?;
    }
    for assignment in assignments {
        validate_ast_alias_hiding(&assignment.value, &hidden, &visible)?;
        if let AssignmentTarget::ColumnName(name) = &assignment.target {
            let qualifier = &name.0[..name.0.len().saturating_sub(1)];
            validate_assignment_target_qualifier(name, &visible)?;
            if qualifier_is_hidden(qualifier, &hidden, &visible) {
                return Err(Error::UnknownColumn(name.to_string()));
            }
        }
    }
    if !table.joins.is_empty() {
        if !order_by.is_empty() || limit.is_some() {
            return Err(Error::Unsupported(
                "ORDER BY / LIMIT is supported only for single-table UPDATE".into(),
            ));
        }
        return multi_update(db, vindex, table, assignments, selection).await;
    }
    let name = table_of(db, table)?;
    let def = catalog::load(db, &name).await?;
    let qualifier_object = factor_qualifier_object(db, &table.relation)
        .ok_or_else(|| Error::Catalog("empty table qualifier".into()))?;
    let qualifier = object_name_parts(&qualifier_object);
    let validation_schema = qualify_relation_schema(def.schema.clone(), &qualifier_object);
    let ctes = std::collections::HashMap::new();
    if let Some(selection) = selection {
        validate_expression_columns(
            db,
            selection,
            &validation_schema,
            None,
            &ctes,
            ROW_FUNCTIONS,
        )
        .await?;
    }
    for order in order_by {
        validate_expression_columns(
            db,
            &order.expr,
            &validation_schema,
            None,
            &ctes,
            ROW_FUNCTIONS,
        )
        .await?;
    }
    for assignment in assignments {
        validate_expression_columns(
            db,
            &assignment.value,
            &validation_schema,
            None,
            &ctes,
            ROW_FUNCTIONS,
        )
        .await?;
    }
    let (relation_name, relation_alias) = match &table.relation {
        TableFactor::Table { name, alias, .. } => (name, alias.as_ref()),
        _ => {
            return Err(Error::Unsupported(
                "only plain table references are supported".into(),
            ))
        }
    };

    // Resolve assignment targets to column indices.
    let mut sets: Vec<(usize, &Expr)> = Vec::with_capacity(assignments.len());
    for a in assignments {
        let col = match &a.target {
            AssignmentTarget::ColumnName(n) => {
                assignment_column_for_table(db, relation_name, relation_alias, n)?
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
            .position(|c| predicate::identifier_eq(&c.name, &col))
            .ok_or_else(|| Error::UnknownColumn(col.clone()))?;
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
        matches.truncate(limit);
    }
    // MySQL reports *changed* rows, not matched rows: an UPDATE that assigns a
    // column the value it already had reports 0. (With CLIENT_FOUND_ROWS a client
    // asks for matched rows instead, but that capability is not negotiated here,
    // so the changed-row count is always the right answer. If it is ever
    // honoured, this and the upsert counts in `insert` become conditional.)
    let mut affected: u64 = 0;

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
    let check_fk = db.foreign_key_checks() && !def.foreign_keys.is_empty();
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
                &def.storage_name(),
                &keyenc::encode_key_coll(&pk_vals, &def.pk_collations())?,
            )
        } else {
            old_key.clone()
        };

        // Width constraints apply even to a table with no keys or foreign
        // keys, so retain every updated row for the common validation pass.
        uniq_batch.push((new_key.clone(), new_row.clone()));

        // Index maintenance: drop old entries, write new ones. Deletes are
        // applied before puts, so unchanged index entries survive.
        deletes.extend(index::entry_keys_for_row(&def, &old_row, &old_key)?);
        let new_index_entries = index::entries_for_row(&def, &new_row, &new_key)?;
        if new_key != old_key {
            deletes.push(old_key);
        }
        let encoded = bincode::serialize(&new_row).map_err(|e| Error::Storage(e.to_string()))?;
        if new_row != old_row {
            affected += 1;
        }
        fk_parent_changes.push((old_row, new_row.clone()));
        puts.push((new_key, encoded));
        puts.extend(new_index_entries);
    }

    if check_uniq {
        check_unique_batch(db, &def, &uniq_batch).await?;
    }
    check_widths_batch(db, &def, &uniq_batch).await?;
    if check_fk {
        check_fk_batch(db, &def, &uniq_batch).await?;
    }

    // Parent-side ON UPDATE referential actions for children referencing a
    // changed key (RESTRICT/CASCADE/SET NULL, single level).
    let mut wcounts: Vec<String> = vec![name.clone()];
    if db.foreign_key_checks() {
        cascade_parent_update(
            db,
            &def,
            &fk_parent_changes,
            &mut puts,
            &mut deletes,
            &mut wcounts,
        )
        .await?;
    }
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

pub async fn delete(
    db: &Session,
    vindex: &VectorRegistry,
    del: &Delete,
    limit_override: Option<usize>,
) -> Result<QueryResult> {
    let relations = match &del.from {
        FromTable::WithFromKeyword(v) | FromTable::WithoutKeyword(v) => v,
    };
    // In `DELETE FROM targets USING sources`, `from` names writable targets;
    // only `using` introduces the relation scope. In `DELETE targets FROM
    // sources`, the source scope is carried directly in `from`.
    let scope = del.using.as_deref().unwrap_or(relations);
    let hidden = hidden_source_qualifiers(scope);
    let visible = validate_unique_relation_qualifiers(db, scope, "mutation")?;
    if let Some(selection) = &del.selection {
        validate_ast_alias_hiding(selection, &hidden, &visible)?;
    }
    for order in &del.order_by {
        validate_ast_alias_hiding(&order.expr, &hidden, &visible)?;
    }
    // Multi-table DELETE: USING, a join in FROM, or explicit target tables.
    if del.using.is_some()
        || relations.len() != 1
        || !relations[0].joins.is_empty()
        || !del.tables.is_empty()
    {
        return multi_delete(db, vindex, del, relations).await;
    }
    let name = table_of(db, &relations[0])?;
    let def = catalog::load(db, &name).await?;
    let qualifier_object = factor_qualifier_object(db, &relations[0].relation)
        .ok_or_else(|| Error::Catalog("empty table qualifier".into()))?;
    let qualifier = object_name_parts(&qualifier_object);
    let validation_schema = qualify_relation_schema(def.schema.clone(), &qualifier_object);
    let ctes = std::collections::HashMap::new();
    if let Some(selection) = &del.selection {
        validate_expression_columns(
            db,
            selection,
            &validation_schema,
            None,
            &ctes,
            ROW_FUNCTIONS,
        )
        .await?;
    }
    for order in &del.order_by {
        validate_expression_columns(
            db,
            &order.expr,
            &validation_schema,
            None,
            &ctes,
            ROW_FUNCTIONS,
        )
        .await?;
    }

    let limit = del
        .limit
        .as_ref()
        .map(eval_usize)
        .transpose()?
        .or(limit_override);

    let matches =
        mutation_matches(db, vindex, &def, &qualifier, del.selection.as_ref(), limit).await?;
    let affected = matches.len() as u64;

    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();
    let mut wcounts: Vec<String> = vec![name.clone()];

    // Foreign keys referencing this table: RESTRICT / CASCADE / SET NULL.
    if db.foreign_key_checks() {
        cascade_parent_delete(
            db,
            &def,
            &matches,
            &mut puts,
            &mut deletes,
            &mut wcounts,
            None,
        )
        .await?;
    }

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

/// Tables that declare a foreign key referencing `parent`, **including `parent`
/// itself** when it references its own key. Excluding it meant a self-referencing
/// `ON DELETE CASCADE` never fired, so deleting the root of a hierarchy left the
/// children behind pointing at a row that no longer existed.
async fn referencing_children(db: &Session, parent: &str) -> Result<Vec<TableDef>> {
    let mut out = Vec::new();
    for t in catalog::list_tables(db).await? {
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
///
/// Runs to a fixed point, so a cascade reaches grandchildren and follows a
/// self-referencing key down a hierarchy. Rows already removed are remembered,
/// which both prevents duplicate work and terminates a cycle; `MAX_CASCADE_DEPTH`
/// is a backstop so a pathological schema cannot loop forever.
async fn cascade_parent_delete(
    db: &Session,
    parent: &TableDef,
    matches: &[(Vec<u8>, Vec<Value>)],
    puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    deletes: &mut Vec<Vec<u8>>,
    wcounts: &mut Vec<String>,
    scheduled_deletes: Option<&std::collections::HashSet<Vec<u8>>>,
) -> Result<()> {
    const MAX_CASCADE_DEPTH: usize = 64;
    let mut removed: std::collections::HashSet<Vec<u8>> = scheduled_deletes
        .map(|keys| keys.iter().cloned().collect())
        .unwrap_or_default();
    let mut frontier: CascadeLevel = vec![(parent.clone(), matches.to_vec())];
    for _ in 0..MAX_CASCADE_DEPTH {
        let mut next: CascadeLevel = Vec::new();
        for (level_parent, level_rows) in &frontier {
            cascade_one_level(
                db,
                level_parent,
                level_rows,
                puts,
                deletes,
                wcounts,
                &mut removed,
                &mut next,
            )
            .await?;
        }
        if next.is_empty() {
            return Ok(());
        }
        frontier = next;
    }
    Err(Error::Query(format!(
        "ON DELETE CASCADE exceeded {MAX_CASCADE_DEPTH} levels from '{}'",
        parent.name
    )))
}

/// A table and the rows of it that were just deleted, for the next cascade level.
type CascadeLevel = Vec<(TableDef, Vec<(Vec<u8>, Vec<Value>)>)>;

/// One level of the cascade. Newly deleted child rows are appended to `next` so
/// the caller can follow them.
#[allow(clippy::too_many_arguments)]
async fn cascade_one_level(
    db: &Session,
    parent: &TableDef,
    matches: &[(Vec<u8>, Vec<Value>)],
    puts: &mut Vec<(Vec<u8>, Vec<u8>)>,
    deletes: &mut Vec<Vec<u8>>,
    wcounts: &mut Vec<String>,
    removed: &mut std::collections::HashSet<Vec<u8>>,
    next: &mut CascadeLevel,
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
                            .position(|c| predicate::identifier_eq(&c.name, rc))
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
                        let mut cascaded: Vec<(Vec<u8>, Vec<Value>)> = Vec::new();
                        for (ck, crow) in child_rows {
                            if !removed.insert(ck.clone()) {
                                continue; // already going, or scheduled by the caller
                            }
                            deletes.extend(index::entry_keys_for_row(child, &crow, &ck)?);
                            deletes.push(ck.clone());
                            cascaded.push((ck, crow));
                            touched = true;
                        }
                        if !cascaded.is_empty() {
                            next.push((child.clone(), cascaded));
                        }
                    }
                    RefAction::SetNull => {
                        for (ck, crow) in child_rows {
                            if removed.contains(&ck) {
                                continue;
                            }
                            let mut nrow = crow.clone();
                            for &fc in &fk.columns {
                                nrow[fc] = Value::Null;
                            }
                            deletes.extend(index::entry_keys_for_row(child, &crow, &ck)?);
                            let enc = bincode::serialize(&nrow)
                                .map_err(|e| Error::Storage(e.to_string()))?;
                            puts.push((ck.clone(), enc));
                            puts.extend(index::entries_for_row(child, &nrow, &ck)?);
                            touched = true;
                        }
                    }
                    _ => {
                        // A row that is itself being deleted cannot block the
                        // delete: that is what makes `DELETE FROM t` work on a
                        // self-referencing table.
                        if child_rows.iter().all(|(key, _)| removed.contains(key)) {
                            continue;
                        }
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
                    .position(|c| predicate::identifier_eq(&c.name, rc))
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
        let data_keys = index::lookup_eq(db, &child.storage_name(), idx, vals).await?;
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

fn resolve_target_qualifier(
    targets: &std::collections::HashMap<Vec<String>, TargetInfo>,
    requested: &[String],
    operation: &str,
) -> Result<Vec<String>> {
    if requested.len() > 2 {
        return Err(Error::Parse(format!(
            "invalid qualified table name: {}",
            requested.join(".")
        )));
    }
    if targets.contains_key(requested) {
        return Ok(requested.to_vec());
    }
    let matches = targets
        .iter()
        .filter(|(qualifier, _)| qualifier_component_suffix(qualifier, requested))
        .map(|(qualifier, _)| qualifier.clone())
        .collect::<Vec<_>>();
    let requested_text = requested.join(".");
    match matches.as_slice() {
        [qualifier] => Ok(qualifier.clone()),
        [] => Err(Error::UnknownTable(requested_text)),
        _ => Err(Error::Query(format!(
            "ambiguous table in {operation}: {requested_text}"
        ))),
    }
}

/// Map each plain table in `from` (by qualifier) to its base definition and the
/// combined-schema indices of its columns.
async fn collect_targets(
    db: &Session,
    from: &[TableWithJoins],
    schema: &Schema,
) -> Result<std::collections::HashMap<Vec<String>, TargetInfo>> {
    let mut factors: Vec<&TableFactor> = Vec::new();
    for twj in from {
        factors.push(&twj.relation);
        for j in &twj.joins {
            factors.push(&j.relation);
        }
    }
    let mut map: std::collections::HashMap<Vec<String>, TargetInfo> =
        std::collections::HashMap::new();
    for tf in factors {
        if let TableFactor::Table { name, .. } = tf {
            let tname = stored_table_ident(db, name)?;
            let qualifier = factor_qualifier_object(db, tf)
                .ok_or_else(|| Error::Catalog("empty table qualifier".into()))?;
            let qualifier = qualifier
                .0
                .iter()
                .map(|part| part.value.clone())
                .collect::<Vec<_>>();
            if map
                .keys()
                .any(|existing| qualifier_short_names_equal(existing, &qualifier))
            {
                return Err(Error::Query(format!(
                    "duplicate table alias in mutation: {}",
                    qualifier.join(".")
                )));
            }
            let def = catalog::load(db, &tname).await?;
            let mut col_idx = Vec::with_capacity(def.schema.columns.len());
            for c in &def.schema.columns {
                let i = schema
                    .columns
                    .iter()
                    .position(|column| {
                        qualifier_components_equal(&column.qualifier, &qualifier)
                            && predicate::identifier_eq(column_name(column), &c.name)
                    })
                    .ok_or_else(|| {
                        Error::Query(format!(
                            "column {}.{} not found in join output",
                            qualifier.join("."),
                            c.name
                        ))
                    })?;
                col_idx.push(i);
            }
            map.insert(
                qualifier,
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
    let (schema, rows) = build_from(db, vindex, from, &[]).await?;
    let filter = match selection {
        Some(f) => Some(resolve_subqueries(db, vindex, f.clone()).await?),
        None => None,
    };
    let targets = collect_targets(db, from, &schema).await?;
    let primary = factor_qualifier_object(db, &table.relation).map(|qualifier| {
        qualifier
            .0
            .iter()
            .map(|part| part.value.clone())
            .collect::<Vec<_>>()
    });

    struct SetOp<'a> {
        qual: Vec<String>,
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
        let (requested, colname) = if n.0.len() >= 2 {
            (
                n.0[..n.0.len() - 1]
                    .iter()
                    .map(|part| part.value.clone())
                    .collect::<Vec<_>>(),
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
        let qual = resolve_target_qualifier(&targets, &requested, "UPDATE")?;
        let info = &targets[&qual];
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
            .position(|c| predicate::identifier_eq(&c.name, &colname))
            .ok_or_else(|| Error::UnknownColumn(colname.clone()))?;
        sets.push(SetOp {
            qual,
            col,
            expr: &a.value,
        });
    }

    // Per target table: pk -> (old base row, new base row). A base row hit by
    // multiple joined rows is updated once (first match).
    type RowMap = std::collections::HashMap<Vec<u8>, (Vec<Value>, Vec<Value>)>;
    let mut updated: std::collections::HashMap<Vec<String>, RowMap> =
        std::collections::HashMap::new();
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
                &info.def.storage_name(),
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
        let batch = rowsmap
            .iter()
            .map(|(key, (_, new_base))| (key.clone(), new_base.clone()))
            .collect::<Vec<_>>();
        check_widths_batch(db, &info.def, &batch).await?;
        for (pk_key, (old_base, new_base)) in rowsmap {
            let new_pk: Vec<Value> = info
                .def
                .pk_cols
                .iter()
                .map(|&i| new_base[i].clone())
                .collect();
            let new_key = data_key(
                &info.def.storage_name(),
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
    let source_relations = del.using.as_deref().unwrap_or(relations);
    let (schema, rows) = build_from(db, vindex, source_relations, &[]).await?;
    let filter = match &del.selection {
        Some(f) => Some(resolve_subqueries(db, vindex, f.clone()).await?),
        None => None,
    };
    let targets = collect_targets(db, source_relations, &schema).await?;

    let requested_quals: Vec<Vec<String>> = if del.tables.is_empty() {
        let target_relations = if del.using.is_some() {
            relations
        } else {
            &relations[..1]
        };
        target_relations
            .iter()
            .map(|table| {
                factor_qualifier_object(db, &table.relation)
                    .map(|qualifier| qualifier.0.iter().map(|part| part.value.clone()).collect())
                    .ok_or_else(|| Error::Query("no target table for DELETE".into()))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        del.tables
            .iter()
            .map(|table| table.0.iter().map(|part| part.value.clone()).collect())
            .collect()
    };
    let mut del_quals = requested_quals
        .iter()
        .map(|qualifier| resolve_target_qualifier(&targets, qualifier, "DELETE"))
        .collect::<Result<Vec<_>>>()?;
    del_quals.sort();
    del_quals.dedup();
    for q in &del_quals {
        let info = targets
            .get(q)
            .ok_or_else(|| Error::UnknownTable(q.join(".")))?;
        if !info.def.has_pk() {
            return Err(Error::Unsupported(
                "multi-table DELETE requires a primary key on the target table".into(),
            ));
        }
    }

    let mut per_table: std::collections::HashMap<
        Vec<String>,
        std::collections::HashMap<Vec<u8>, Vec<Value>>,
    > = std::collections::HashMap::new();
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
                &info.def.storage_name(),
                &keyenc::encode_key_coll(&pk_vals, &info.def.pk_collations())?,
            );
            per_table.entry(q.clone()).or_default().insert(pk_key, base);
        }
    }

    let scheduled_deletes = per_table
        .values()
        .flat_map(|rows| rows.keys().cloned())
        .collect::<std::collections::HashSet<_>>();
    let affected = scheduled_deletes.len() as u64;
    let mut processed_deletes = std::collections::HashSet::new();
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();
    let mut wcounts: Vec<String> = Vec::new();
    let mut after_work: Vec<(Vec<catalog::TriggerDef>, Schema, Vec<Vec<Value>>)> = Vec::new();
    for q in &del_quals {
        let Some(rowsmap) = per_table.get(q) else {
            continue;
        };
        let info = &targets[q];
        let matches = rowsmap
            .iter()
            .filter(|&(key, _)| processed_deletes.insert(key.clone()))
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect::<Vec<_>>();
        if matches.is_empty() {
            continue;
        }
        if db.foreign_key_checks() {
            cascade_parent_delete(
                db,
                &info.def,
                &matches,
                &mut puts,
                &mut deletes,
                &mut wcounts,
                Some(&scheduled_deletes),
            )
            .await?;
        }

        let after_del = catalog::load_triggers(db, &info.name)
            .await?
            .into_iter()
            .filter(|trigger| !trigger.before && trigger.event == catalog::TrigEvent::Delete)
            .collect::<Vec<_>>();
        let mut deleted_rows = Vec::new();
        for (pk_key, base) in matches {
            if !after_del.is_empty() {
                deleted_rows.push(base.clone());
            }
            deletes.extend(index::entry_keys_for_row(&info.def, &base, &pk_key)?);
            deletes.push(pk_key);
        }
        wcounts.push(info.name.clone());
        if !after_del.is_empty() {
            after_work.push((after_del, info.def.schema.clone(), deleted_rows));
        }
    }
    wcounts.sort_by_key(|name| name.to_ascii_lowercase());
    wcounts.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    for table in wcounts {
        puts.push(bump_wcount(db, &table).await?);
    }
    db.commit_write(puts, deletes).await?;
    for (triggers, schema, rows) in after_work {
        for row in rows {
            queue_after(db, &triggers, &schema, None, Some(&row))?;
        }
    }
    Ok(QueryResult::Affected(affected))
}

fn ident_name(e: &Expr) -> Option<&str> {
    match e {
        Expr::Identifier(id) => Some(&id.value),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.as_str()),
        _ => None,
    }
}

pub(crate) fn eval_usize(e: &Expr) -> Result<usize> {
    match eval_expr(e)? {
        Value::Int(i) if i >= 0 => Ok(i as usize),
        Value::UInt(i) => usize::try_from(i)
            .map_err(|_| Error::Query(format!("expected non-negative integer, got UInt({i})"))),
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

    let source_expr = |column: &ColumnDef| {
        let mut parts = column
            .qualifier
            .iter()
            .cloned()
            .map(Ident::new)
            .collect::<Vec<_>>();
        parts.push(Ident::new(column_name(column)));
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
                        .filter(|column| predicate::identifier_eq(column_name(column), name))
                        .map(source_expr),
                );
            }
            SelectItem::QualifiedWildcard(object, _) => {
                let unqualified_schema = schema
                    .columns
                    .iter()
                    .all(|column| column.qualifier.is_empty());
                matches.extend(schema.columns.iter().filter_map(|column| {
                    let column_name = wildcard_column_name(column, object, unqualified_schema)?;
                    predicate::identifier_eq(column_name, name).then(|| source_expr(column))
                }));
            }
            SelectItem::UnnamedExpr(expr) => {
                if ident_name(expr).is_some_and(|output| predicate::identifier_eq(output, name)) {
                    matches.push(expr.clone());
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if predicate::identifier_eq(&alias.value, name) {
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
                // Only a bare ORDER BY identifier can name a projection alias;
                // qualified identifiers continue to name source columns.
                if matches!(e, Expr::Identifier(_)) {
                    let mut aliases = projection.iter().filter_map(|item| match item {
                        SelectItem::ExprWithAlias { expr, alias }
                            if predicate::identifier_eq(&alias.value, name) =>
                        {
                            Some(expr)
                        }
                        _ => None,
                    });
                    if let Some(alias_expr) = aliases.next() {
                        if aliases.next().is_none() {
                            return (alias_expr.clone(), *asc);
                        }
                    }
                }
                let is_column = schema
                    .columns
                    .iter()
                    .any(|c| predicate::identifier_eq(&c.name, name));
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
    // The relation to credit a plain column to when the schema's names are not
    // qualified, which is the case for a single-table scan: it reads the table's
    // own bare column names. `None` for join/derived schemas, which carry their
    // own "alias.col" qualifiers.
    default_table: Option<&str>,
) -> Result<(Schema, Vec<Vec<Value>>)> {
    use sqlparser::ast::SelectItem;

    enum Proj<'a> {
        Col(usize),
        Expr(&'a Expr),
    }
    let direct_column = |expr: &Expr| match expr {
        Expr::Identifier(identifier) => {
            predicate::resolve_index_parts(std::slice::from_ref(identifier), schema).ok()
        }
        Expr::CompoundIdentifier(parts) => predicate::resolve_index_parts(parts, schema).ok(),
        _ => None,
    };
    let mut names: Vec<String> = Vec::new();
    let mut projs: Vec<Proj> = Vec::new();

    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                for i in unqualified_wildcard_indices(schema) {
                    let c = &schema.columns[i];
                    names.push(column_name(c).to_owned());
                    projs.push(Proj::Col(i));
                }
            }
            // `alias.*` -> every column qualified by `alias` (join schemas name
            // columns `alias.col`). Falls back to matching the bare table name.
            SelectItem::QualifiedWildcard(obj, _) => {
                let unqualified_schema = schema
                    .columns
                    .iter()
                    .all(|column| column.qualifier.is_empty());
                let mut matched = false;
                for (i, c) in schema.columns.iter().enumerate() {
                    if let Some(name) = wildcard_column_name(c, obj, unqualified_schema) {
                        names.push(name.to_owned());
                        projs.push(Proj::Col(i));
                        matched = true;
                    }
                }
                // A single-table scan carries bare column names, so `t.*` has no
                // qualifier to match even though it names the relation being
                // read. Accept it there rather than refusing valid MySQL.
                let qualifier = object_name_text(obj);
                if !matched
                    && default_table
                        .is_some_and(|table| obj.0.last().is_some_and(|part| part.value == table))
                {
                    for (i, c) in schema.columns.iter().enumerate() {
                        names.push(column_name(c).to_owned());
                        projs.push(Proj::Col(i));
                        matched = true;
                    }
                }
                if !matched {
                    return Err(Error::Unsupported(format!(
                        "unknown table qualifier in `{qualifier}.*`"
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
    let mut tables: Vec<String> = Vec::with_capacity(projs.len());
    for (ci, (name, p)) in names.iter().zip(&projs).enumerate() {
        // Carry a direct source column's type, nullability, collation, and
        // result metadata through projection. Computed expressions default to
        // Text/Ci and have no source attributes.
        let (ty, nullable, collation) = match p {
            Proj::Col(i) => (
                schema.columns[*i].ty.clone(),
                schema.columns[*i].nullable,
                schema.columns[*i].collation,
            ),
            Proj::Expr(e) => match direct_column(e) {
                Some(idx) => (
                    schema.columns[idx].ty.clone(),
                    schema.columns[idx].nullable,
                    schema.columns[idx].collation,
                ),
                None => (
                    out_rows
                        .iter()
                        .map(|r| &r[ci])
                        .find(|v| !v.is_null())
                        .map(infer_val)
                        .unwrap_or(ColumnType::Text),
                    true,
                    elyra_core::Collation::Ci,
                ),
            },
        };
        // Result metadata: the relation a projected column came from. An
        // expression keeps an empty table, as it does in MySQL.
        let source_column = match p {
            Proj::Col(i) => Some(*i),
            Proj::Expr(e) => direct_column(e),
        };
        tables.push(
            source_column
                .and_then(|index| schema_column_table(schema, index).or(default_table))
                .unwrap_or_default()
                .to_owned(),
        );
        cols.push(ColumnDef {
            name: name.clone(),
            ty,
            nullable,
            collation,
            qualifier: Vec::new(),
            result_metadata: source_column
                .map(|index| schema.columns[index].result_metadata)
                .unwrap_or_default(),
        });
    }

    Ok((Schema::with_tables(cols, tables), out_rows))
}

/// The alias (or table name) of the single relation a select reads, if it reads
/// exactly one. A single-table scan projects the table's own bare column names,
/// so this is where its result metadata gets its source table from.
fn single_relation_alias(select: &sqlparser::ast::Select) -> Option<String> {
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    match &select.from[0].relation {
        TableFactor::Table { name, alias, .. } => alias
            .as_ref()
            .map(|alias| alias.name.value.clone())
            .or_else(|| name.0.last().map(|name| name.value.clone())),
        TableFactor::Derived { alias, .. } => alias.as_ref().map(|alias| alias.name.value.clone()),
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
                    ident_name(expr).is_some_and(|output| predicate::identifier_eq(output, name))
                })
        }
        SelectItem::ExprWithAlias { expr, alias } => {
            expr == original
                || expr == resolved
                || original_name.is_some_and(|name| predicate::identifier_eq(&alias.value, name))
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
fn filter_correlated(filter: &Expr, outer: &[String]) -> bool {
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
fn query_refs_qualifier(q: &SqlQuery, qualifier: &[String]) -> bool {
    let found = std::cell::Cell::new(false);
    let check = |e: &Expr| -> Option<Expr> {
        if let Expr::CompoundIdentifier(parts) = e {
            let prefix = parts
                .get(..parts.len().saturating_sub(1))
                .unwrap_or_default();
            if qualifier_parts_match(qualifier, prefix) {
                found.set(true);
            }
        }
        None
    };
    let _ = rewrite_query(q, &check);
    found.get()
}

// sqlparser's generated visitors and AST drop path recurse through every
// derived query. Keep enough headroom for the rest of the statement on a Tokio
// worker's comparatively small stack; routine linear execution is flattened by
// `run_derived_query_chain` below.
const MAX_CTE_EXPANSION_DEPTH: usize = 16;
// A shallow CTE can still expand exponentially when it references a previous
// definition more than once. Bound the generated tree as well as its depth.
const MAX_CTE_EXPANSION_NODES: usize = 256;
// Definitions that are never referenced do not contribute to the expanded
// tree, but still consume parser, visitor, and scope-management work. Bound the
// source AST independently, leaving enough room for the supported dependency
// depth and the 101-layer fail-safe regression to reach the expansion guard.
const MAX_CTE_AST_NODES: usize = 4096;

struct CteScope<T>(std::rc::Rc<CteScopeNode<T>>);

impl<T> Clone for CteScope<T> {
    fn clone(&self) -> Self {
        Self(std::rc::Rc::clone(&self.0))
    }
}

enum CteScopeNode<T> {
    Root,
    Binding {
        name: String,
        value: Option<T>,
        parent: CteScope<T>,
    },
}

impl<T> Default for CteScope<T> {
    fn default() -> Self {
        Self(std::rc::Rc::new(CteScopeNode::Root))
    }
}

impl<T> CteScope<T> {
    fn bind(&self, name: String, value: T) -> Self {
        Self(std::rc::Rc::new(CteScopeNode::Binding {
            name,
            value: Some(value),
            parent: self.clone(),
        }))
    }

    fn shadow(&self, name: String) -> Self {
        Self(std::rc::Rc::new(CteScopeNode::Binding {
            name,
            value: None,
            parent: self.clone(),
        }))
    }

    fn get(&self, wanted: &str) -> Option<&T> {
        let mut node = self.0.as_ref();
        loop {
            match node {
                CteScopeNode::Root => return None,
                CteScopeNode::Binding {
                    name,
                    value,
                    parent,
                } => {
                    if name.eq_ignore_ascii_case(wanted) {
                        return value.as_ref();
                    }
                    node = parent.0.as_ref();
                }
            }
        }
    }
}

#[derive(Clone)]
struct InlineCte {
    query: std::rc::Rc<SqlQuery>,
    columns: Vec<TableAliasColumnDef>,
    cost: CteExpansionCost,
}

type InlineCteScope = CteScope<InlineCte>;

#[derive(Default)]
struct CteAstBudget {
    nodes: usize,
}

impl CteAstBudget {
    fn charge(&mut self, nodes: usize) -> ControlFlow<Error> {
        self.nodes = match self.nodes.checked_add(nodes) {
            Some(total) if total <= MAX_CTE_AST_NODES => total,
            _ => {
                return ControlFlow::Break(Error::Parse(format!(
                    "CTE expansion limit exceeded (AST node limit {MAX_CTE_AST_NODES}); simplify the query"
                )))
            }
        };
        ControlFlow::Continue(())
    }
}

struct CteAstCounter {
    budget: CteAstBudget,
}

impl Visitor for CteAstCounter {
    type Break = Error;

    fn pre_visit_query(&mut self, query: &SqlQuery) -> ControlFlow<Self::Break> {
        self.budget
            .charge(1 + query.with.as_ref().map_or(0, |with| with.cte_tables.len()))
    }

    fn pre_visit_table_factor(&mut self, _table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        self.budget.charge(1)
    }

    fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
        self.budget.charge(1)
    }
}

#[derive(Clone, Copy, Default)]
struct CteExpansionCost {
    depth: usize,
    nodes: usize,
}

impl CteExpansionCost {
    fn include(&mut self, nested: Self) -> Result<()> {
        let depth = nested.depth.saturating_add(1);
        if depth > MAX_CTE_EXPANSION_DEPTH {
            return Err(Error::Parse(format!(
                "CTE expansion limit exceeded (depth limit {MAX_CTE_EXPANSION_DEPTH}); simplify the query"
            )));
        }

        let nodes = self
            .nodes
            .checked_add(nested.nodes)
            .and_then(|nodes| nodes.checked_add(1))
            .filter(|nodes| *nodes <= MAX_CTE_EXPANSION_NODES)
            .ok_or_else(|| {
                Error::Parse(format!(
                    "CTE expansion limit exceeded (node limit {MAX_CTE_EXPANSION_NODES}); simplify the query"
                ))
            })?;

        self.depth = self.depth.max(depth);
        self.nodes = nodes;
        Ok(())
    }

    fn merge(&mut self, nested: Self) -> Result<()> {
        let nodes = self
            .nodes
            .checked_add(nested.nodes)
            .filter(|nodes| *nodes <= MAX_CTE_EXPANSION_NODES)
            .ok_or_else(|| {
                Error::Parse(format!(
                    "CTE expansion limit exceeded (node limit {MAX_CTE_EXPANSION_NODES}); simplify the query"
                ))
            })?;
        self.depth = self.depth.max(nested.depth);
        self.nodes = nodes;
        Ok(())
    }
}

fn validate_unique_cte_names(with: &With) -> Result<()> {
    for (index, cte) in with.cte_tables.iter().enumerate() {
        if with.cte_tables[..index].iter().any(|prior| {
            prior
                .alias
                .name
                .value
                .eq_ignore_ascii_case(&cte.alias.name.value)
        }) {
            return Err(Error::Query(format!(
                "duplicate CTE name: {}",
                cte.alias.name.value
            )));
        }
    }
    Ok(())
}

/// Expand every non-recursive `WITH` visible from `query`, inlining CTEs as
/// derived tables at every relation reference in the query tree. A scope stack
/// keeps nested `WITH` names lexical, and each CTE body is expanded only against
/// definitions visible at its declaration point.
fn expand_ctes(query: &SqlQuery) -> Result<SqlQuery> {
    guard_cte_ast_complexity(query)?;
    let mut expanded = query.clone();
    expand_ctes_with_scope(&mut expanded, InlineCteScope::default())?;
    Ok(expanded)
}

fn guard_cte_ast_complexity(query: &SqlQuery) -> Result<()> {
    let mut counter = CteAstCounter {
        budget: CteAstBudget::default(),
    };
    if let ControlFlow::Break(error) = query.visit(&mut counter) {
        return Err(error);
    }
    Ok(())
}

fn expand_ctes_with_scope(
    query: &mut SqlQuery,
    parent_scope: InlineCteScope,
) -> Result<CteExpansionCost> {
    let mut expander = InlineCteExpander {
        scopes: vec![parent_scope],
        cost: CteExpansionCost::default(),
        saved_with: Vec::new(),
    };
    match query.visit(&mut expander) {
        ControlFlow::Continue(()) => Ok(expander.cost),
        ControlFlow::Break(error) => Err(error),
    }
}

struct InlineCteExpander {
    scopes: Vec<InlineCteScope>,
    cost: CteExpansionCost,
    saved_with: Vec<Option<sqlparser::ast::With>>,
}

impl VisitorMut for InlineCteExpander {
    type Break = Error;

    fn pre_visit_query(&mut self, query: &mut SqlQuery) -> ControlFlow<Self::Break> {
        if let Some(with) = &query.with {
            if let Err(error) = validate_unique_cte_names(with) {
                return ControlFlow::Break(error);
            }
        }
        let mut scope = self.scopes.last().cloned().unwrap_or_default();
        let mut saved_with = None;
        if let Some(mut with) = query.with.take() {
            if with.recursive {
                // A nested recursive WITH is materialised when that query is
                // executed. Earlier names and the current self-reference are
                // local; later names do not hide an outer binding yet.
                for cte in &mut with.cte_tables {
                    scope = scope.shadow(cte.alias.name.value.clone());
                    let cost = match expand_ctes_with_scope(cte.query.as_mut(), scope.clone()) {
                        Ok(cost) => cost,
                        Err(error) => return ControlFlow::Break(error),
                    };
                    if let Err(error) = self.cost.merge(cost) {
                        return ControlFlow::Break(error);
                    }
                }
                saved_with = Some(with);
            } else {
                for cte in with.cte_tables {
                    let mut body = *cte.query;
                    let cost = match expand_ctes_with_scope(&mut body, scope.clone()) {
                        Ok(cost) => cost,
                        Err(error) => return ControlFlow::Break(error),
                    };
                    scope = scope.bind(
                        cte.alias.name.value,
                        InlineCte {
                            query: std::rc::Rc::new(body),
                            columns: cte.alias.columns,
                            cost,
                        },
                    );
                }
            }
        }
        self.saved_with.push(saved_with);
        self.scopes.push(scope);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut SqlQuery) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        query.with = self.saved_with.pop().flatten();
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        let TableFactor::Table {
            name, alias, args, ..
        } = table_factor
        else {
            return ControlFlow::Continue(());
        };
        if args.is_some() || name.0.len() != 1 {
            return ControlFlow::Continue(());
        }
        let cte_name = &name.0[0].value;
        let Some(body) = self.scopes.last().and_then(|scope| scope.get(cte_name)) else {
            return ControlFlow::Continue(());
        };
        if let Err(error) = self.cost.include(body.cost) {
            return ControlFlow::Break(error);
        }
        let mut alias = alias.clone().unwrap_or_else(|| TableAlias {
            name: sqlparser::ast::Ident::new(cte_name.clone()),
            columns: Vec::new(),
        });
        if alias.columns.is_empty() {
            alias.columns.clone_from(&body.columns);
        }
        *table_factor = TableFactor::Derived {
            lateral: false,
            subquery: Box::new(body.query.as_ref().clone()),
            alias: Some(alias),
        };
        ControlFlow::Continue(())
    }
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
    let direct_column = |expr: &Expr| match expr {
        Expr::Identifier(identifier) => {
            predicate::resolve_index_parts(std::slice::from_ref(identifier), schema).ok()
        }
        Expr::CompoundIdentifier(parts) => predicate::resolve_index_parts(parts, schema).ok(),
        _ => None,
    };
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
                            name: column_name(column).to_owned(),
                            source_column: Some(index),
                            evaluator: Out::Column(index),
                        }),
                );
                continue;
            }
            SelectItem::QualifiedWildcard(object, _) => {
                let relation_matches = select.from.len() == 1 && select.from[0].joins.is_empty();
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
                            name: column_name(column).to_owned(),
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
            source_column: direct_column(expr),
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
    // Result metadata: a column that came straight from a relation keeps that
    // relation's name; a window value or an expression has no source table.
    let mut tables: Vec<String> = Vec::with_capacity(plan.len());
    let default_table = single_relation_alias(select);
    for (column_index, planned) in plan.iter().enumerate() {
        let source = planned
            .source_column
            .and_then(|index| schema.columns.get(index));
        tables.push(
            source
                .and_then(column_table)
                .or(default_table.as_deref())
                .unwrap_or_default()
                .to_owned(),
        );
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
            qualifier: Vec::new(),
            result_metadata: source.map_or_else(Default::default, |column| column.result_metadata),
        });
    }
    let out_schema = Schema::with_tables(cols, tables);

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
    validate_window_function_arity(&name, function_argument_count(f)?)?;
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
                .and_then(|v| v.as_mysql_f64())
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
            let aggregate = WindowAggregate::new(name, count_star, arg0, rows, schema, idxs)?;

            match frame_mode(frame, ordered)? {
                FrameMode::Rows => {
                    let f = frame.expect("rows frame present");
                    for (p, &i) in idxs.iter().enumerate() {
                        let (lo, hi) = rows_bounds(f, p, n, schema, rows, idxs)?;
                        result[i] = aggregate.evaluate(lo, hi, idxs, rows, schema)?;
                    }
                }
                FrameMode::Whole => {
                    let agg = aggregate.evaluate(0, n.saturating_sub(1), idxs, rows, schema)?;
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
                        let agg = aggregate.evaluate(0, q - 1, idxs, rows, schema)?;
                        for &i in &idxs[p..q] {
                            result[i] = agg.clone();
                        }
                        p = q;
                    }
                }
                FrameMode::Range => {
                    let f = frame.expect("range frame present");
                    if order.len() != 1 {
                        return Err(Error::Unsupported(
                            "RANGE offset frames require exactly one numeric ORDER BY expression"
                                .into(),
                        ));
                    }
                    let keys: Vec<Value> = idxs
                        .iter()
                        .map(|&i| predicate::eval_row(&order[0].0, schema, &rows[i]))
                        .collect::<Result<_>>()?;
                    let numeric_keys = keys
                        .iter()
                        .map(|key| {
                            if key.is_null() {
                                Ok(None)
                            } else {
                                RangeNumeric::from_value(key).map(Some)
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let (peer_lows, peer_highs) = peer_bounds(&keys);
                    let start_offset = frame_offset_value(&f.start_bound, schema, rows, idxs)?;
                    let end_offset = f
                        .end_bound
                        .as_ref()
                        .map(|bound| frame_offset_value(bound, schema, rows, idxs))
                        .transpose()?
                        .flatten();
                    let bounds = WindowRangeBounds {
                        frame: f,
                        keys: &keys,
                        numeric_keys: &numeric_keys,
                        peer_lows: &peer_lows,
                        peer_highs: &peer_highs,
                        ascending: order[0].1,
                        start_offset: start_offset.as_ref(),
                        end_offset: end_offset.as_ref(),
                    };
                    for (p, &i) in idxs.iter().enumerate() {
                        let (lo, hi) = window_range_bounds(&bounds, p)?;
                        result[i] = aggregate.evaluate(lo, hi, idxs, rows, schema)?;
                    }
                }
                FrameMode::Groups => {
                    let f = frame.expect("groups frame present");
                    let keys: Vec<Vec<Value>> =
                        idxs.iter().map(|&i| order_key(i)).collect::<Result<_>>()?;
                    let (group_starts, row_groups) = peer_groups(&keys);
                    for (p, &i) in idxs.iter().enumerate() {
                        let (lo, hi) =
                            groups_bounds(f, row_groups[p], &group_starts, n, schema, rows, idxs)?;
                        result[i] = aggregate.evaluate(lo, hi, idxs, rows, schema)?;
                    }
                }
            }
        }
        "ntile" => {
            let buckets = args
                .first()
                .and_then(|e| predicate::eval_row(e, schema, &rows[idxs[0]]).ok())
                .and_then(|v| v.as_mysql_f64())
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
                .and_then(|v| v.as_mysql_f64())
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
    Range,
    Groups,
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
            } else if running {
                Ok(FrameMode::Whole)
            } else if matches!(f.units, U::Range) {
                if ordered {
                    Ok(FrameMode::Range)
                } else {
                    Err(Error::Unsupported(
                        "RANGE offset frames require exactly one numeric ORDER BY expression"
                            .into(),
                    ))
                }
            } else {
                Ok(FrameMode::Groups)
            }
        }
    }
}

fn frame_offset_value(
    bound: &sqlparser::ast::WindowFrameBound,
    schema: &Schema,
    rows: &[Vec<Value>],
    idxs: &[usize],
) -> Result<Option<Value>> {
    use sqlparser::ast::WindowFrameBound as B;
    use sqlparser::ast::{Visit, Visitor};
    use std::ops::ControlFlow;

    struct NonConstantFinder;
    impl Visitor for NonConstantFinder {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if matches!(
                expr,
                Expr::Identifier(_)
                    | Expr::CompoundIdentifier(_)
                    | Expr::Function(_)
                    | Expr::Subquery(_)
                    | Expr::Exists { .. }
            ) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    let expr = match bound {
        B::Preceding(Some(expr)) | B::Following(Some(expr)) => expr,
        _ => return Ok(None),
    };
    if expr.visit(&mut NonConstantFinder).is_break() {
        return Err(Error::Query(
            "window frame offsets must be constant expressions".into(),
        ));
    }
    let value = predicate::eval_row(expr, schema, &rows[idxs[0]])?;
    Ok(Some(value))
}

#[derive(Clone, Copy)]
struct FixedNumeric {
    raw: i128,
    scale: u8,
}

#[derive(Clone, Copy)]
enum RangeNumeric {
    Fixed(FixedNumeric),
    Float(f64),
}

impl RangeNumeric {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Int(value) => Ok(Self::Fixed(FixedNumeric {
                raw: i128::from(*value),
                scale: 0,
            })),
            Value::UInt(value) => Ok(Self::Fixed(FixedNumeric {
                raw: i128::from(*value),
                scale: 0,
            })),
            Value::Decimal(raw, scale) => Ok(Self::Fixed(FixedNumeric {
                raw: *raw,
                scale: *scale,
            })),
            Value::Float(value) if value.is_finite() => Ok(Self::Float(*value)),
            _ => Err(Error::Unsupported(
                "RANGE offset frames require numeric ORDER BY values and offsets; temporal offsets are not supported"
                    .into(),
            )),
        }
    }

    fn non_negative(self) -> bool {
        match self {
            Self::Fixed(value) => value.raw >= 0,
            Self::Float(value) => value >= 0.0,
        }
    }

    fn shifted(self, offset: Self, add: bool) -> Result<Self> {
        match (self, offset) {
            (Self::Fixed(left), Self::Fixed(right)) => {
                let scale = left.scale.max(right.scale);
                let left = scale_fixed(left, scale)?;
                let right = scale_fixed(right, scale)?;
                let raw = if add {
                    left.checked_add(right)
                } else {
                    left.checked_sub(right)
                }
                .ok_or_else(|| Error::Unsupported("RANGE numeric boundary overflow".into()))?;
                Ok(Self::Fixed(FixedNumeric { raw, scale }))
            }
            (Self::Float(left), Self::Float(right)) => Ok(Self::Float(if add {
                left + right
            } else {
                left - right
            })),
            (Self::Float(left), Self::Fixed(right)) => {
                let divisor = 10f64.powi(i32::from(right.scale));
                Ok(Self::Float(if add {
                    left + right.raw as f64 / divisor
                } else {
                    left - right.raw as f64 / divisor
                }))
            }
            (Self::Fixed(_), Self::Float(_)) => Err(Error::Unsupported(
                "floating RANGE offsets are not supported for exact integer or DECIMAL ordering keys"
                    .into(),
            )),
        }
    }

    fn compare(self, other: Self) -> Result<std::cmp::Ordering> {
        match (self, other) {
            (Self::Fixed(left), Self::Fixed(right)) => {
                let scale = left.scale.max(right.scale);
                Ok(scale_fixed(left, scale)?.cmp(&scale_fixed(right, scale)?))
            }
            (Self::Float(left), Self::Float(right)) => Ok(left.total_cmp(&right)),
            (Self::Float(left), Self::Fixed(right)) => {
                Ok(left.total_cmp(&(right.raw as f64 / 10f64.powi(i32::from(right.scale)))))
            }
            (Self::Fixed(_), Self::Float(_)) => Err(Error::Unsupported(
                "mixed floating and exact RANGE ordering values are not supported".into(),
            )),
        }
    }
}

fn scale_fixed(value: FixedNumeric, scale: u8) -> Result<i128> {
    let factor = 10_i128
        .checked_pow(u32::from(scale - value.scale))
        .ok_or_else(|| Error::Unsupported("RANGE decimal scale overflow".into()))?;
    value
        .raw
        .checked_mul(factor)
        .ok_or_else(|| Error::Unsupported("RANGE decimal value overflow".into()))
}

#[derive(Clone, Copy)]
struct WindowRangeBounds<'a> {
    frame: &'a sqlparser::ast::WindowFrame,
    keys: &'a [Value],
    numeric_keys: &'a [Option<RangeNumeric>],
    peer_lows: &'a [usize],
    peer_highs: &'a [usize],
    ascending: bool,
    start_offset: Option<&'a Value>,
    end_offset: Option<&'a Value>,
}

fn window_range_bounds(bounds: &WindowRangeBounds<'_>, p: usize) -> Result<(usize, usize)> {
    use sqlparser::ast::WindowFrameBound as B;
    let WindowRangeBounds {
        frame,
        keys,
        numeric_keys,
        peer_lows,
        peer_highs,
        ascending,
        start_offset,
        end_offset,
    } = *bounds;
    for offset in [start_offset, end_offset].into_iter().flatten() {
        if !RangeNumeric::from_value(offset)?.non_negative() {
            return Err(Error::Query(
                "window frame offsets must be non-negative numeric constants".into(),
            ));
        }
    }
    let current = &keys[p];
    if current.is_null() {
        let (peer_lo, peer_hi) = (peer_lows[p], peer_highs[p]);
        let null_boundary = |bound: &B, start: bool| match bound {
            B::Preceding(None) => 0,
            B::Following(None) => keys.len().saturating_sub(1),
            B::CurrentRow | B::Preceding(Some(_)) | B::Following(Some(_)) => {
                if start {
                    peer_lo
                } else {
                    peer_hi
                }
            }
        };
        return Ok((
            null_boundary(&frame.start_bound, true),
            null_boundary(frame.end_bound.as_ref().unwrap_or(&B::CurrentRow), false),
        ));
    }
    let current = RangeNumeric::from_value(current)?;
    let boundary = |bound: &B, offset: Option<&Value>, start: bool| -> Result<usize> {
        match bound {
            B::Preceding(None) => Ok(0),
            B::Following(None) => Ok(keys.len().saturating_sub(1)),
            B::CurrentRow => Ok(if start { peer_lows[p] } else { peer_highs[p] }),
            B::Preceding(Some(_)) | B::Following(Some(_)) => {
                let offset = RangeNumeric::from_value(
                    offset.ok_or_else(|| Error::Query("window frame offset is missing".into()))?,
                )?;
                let add = matches!(bound, B::Following(_)) == ascending;
                let target = current.shifted(offset, add)?;
                if start {
                    range_lower_bound(numeric_keys, target, ascending)
                } else {
                    range_upper_bound(numeric_keys, target, ascending)
                }
            }
        }
    };
    let lo = boundary(&frame.start_bound, start_offset, true)?;
    let hi = boundary(
        frame.end_bound.as_ref().unwrap_or(&B::CurrentRow),
        end_offset,
        false,
    )?;
    if hi == usize::MAX {
        return Ok((1, 0));
    }
    Ok((lo, hi))
}

fn peer_bounds<T: PartialEq>(keys: &[T]) -> (Vec<usize>, Vec<usize>) {
    let mut lows = vec![0; keys.len()];
    let mut highs = vec![0; keys.len()];
    let mut start = 0;
    while start < keys.len() {
        let mut end = start + 1;
        while end < keys.len() && keys[end] == keys[start] {
            end += 1;
        }
        for position in start..end {
            lows[position] = start;
            highs[position] = end - 1;
        }
        start = end;
    }
    (lows, highs)
}

fn range_lower_bound(
    keys: &[Option<RangeNumeric>],
    target: RangeNumeric,
    ascending: bool,
) -> Result<usize> {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let before = match keys[mid] {
            None => ascending,
            Some(value) => {
                let ordering = value.compare(target)?;
                if ascending {
                    ordering.is_lt()
                } else {
                    ordering.is_gt()
                }
            }
        };
        if before {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn range_upper_bound(
    keys: &[Option<RangeNumeric>],
    target: RangeNumeric,
    ascending: bool,
) -> Result<usize> {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let after = match keys[mid] {
            None => !ascending,
            Some(value) => {
                let ordering = value.compare(target)?;
                if ascending {
                    ordering.is_gt()
                } else {
                    ordering.is_lt()
                }
            }
        };
        if after {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    Ok(lo.checked_sub(1).unwrap_or(usize::MAX))
}

fn peer_groups<T: PartialEq>(keys: &[T]) -> (Vec<usize>, Vec<usize>) {
    let mut starts = Vec::new();
    let mut row_groups = Vec::with_capacity(keys.len());
    for (p, key) in keys.iter().enumerate() {
        if p == 0 || key != &keys[p - 1] {
            starts.push(p);
        }
        row_groups.push(starts.len() - 1);
    }
    (starts, row_groups)
}

#[allow(clippy::too_many_arguments)]
fn groups_bounds(
    frame: &sqlparser::ast::WindowFrame,
    group: usize,
    starts: &[usize],
    row_count: usize,
    schema: &Schema,
    rows: &[Vec<Value>],
    idxs: &[usize],
) -> Result<(usize, usize)> {
    use sqlparser::ast::WindowFrameBound as B;
    let group_count = starts.len();
    let boundary_group = |bound: &B| -> Result<isize> {
        let offset = match frame_offset_value(bound, schema, rows, idxs)? {
            None => Some(0),
            Some(Value::Int(value)) => isize::try_from(value).ok().filter(|v| *v >= 0),
            Some(Value::UInt(value)) => isize::try_from(value).ok(),
            Some(Value::Decimal(raw, scale)) => {
                let divisor = 10_i128.checked_pow(u32::from(scale));
                divisor
                    .filter(|divisor| raw >= 0 && raw % divisor == 0)
                    .and_then(|divisor| isize::try_from(raw / divisor).ok())
            }
            Some(_) => None,
        }
        .ok_or_else(|| {
            Error::Query("GROUPS frame offsets must be exact non-negative integers".into())
        })?;
        Ok(match bound {
            B::Preceding(None) => 0,
            B::Following(None) => group_count as isize - 1,
            B::CurrentRow => group as isize,
            B::Preceding(Some(_)) => (group as isize).checked_sub(offset).unwrap_or(isize::MIN),
            B::Following(Some(_)) => (group as isize).checked_add(offset).unwrap_or(isize::MAX),
        })
    };
    let lo_group = boundary_group(&frame.start_bound)?.max(0) as usize;
    let hi_group = boundary_group(frame.end_bound.as_ref().unwrap_or(&B::CurrentRow))?
        .min(group_count as isize - 1);
    if hi_group < 0 || lo_group as isize > hi_group || lo_group >= group_count {
        return Ok((1, 0));
    }
    let hi_group = hi_group as usize;
    let hi = starts.get(hi_group + 1).copied().unwrap_or(row_count) - 1;
    Ok((starts[lo_group], hi))
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
    Ok(v.as_mysql_f64().unwrap_or(0.0) as isize)
}

struct WindowAggregate<'a> {
    name: &'a str,
    count_star: bool,
    arg: Option<&'a Expr>,
    values: Vec<Value>,
    non_null_count: Vec<usize>,
    numeric_count: Vec<usize>,
    non_integer_count: Vec<usize>,
    integer_sums: Vec<i128>,
    numeric_sums: NumericRangeSums,
}

struct NumericRangeSums {
    leaf_count: usize,
    tree: Vec<f64>,
}

impl NumericRangeSums {
    fn new(values: &[Value]) -> Self {
        let leaf_count = values.len().next_power_of_two().max(1);
        let mut tree = vec![0.0; leaf_count * 2];
        for (index, value) in values.iter().enumerate() {
            tree[leaf_count + index] = value.as_mysql_f64().unwrap_or(0.0);
        }
        for index in (1..leaf_count).rev() {
            tree[index] = tree[index * 2] + tree[index * 2 + 1];
        }
        Self { leaf_count, tree }
    }

    fn sum(&self, mut start: usize, mut end: usize) -> f64 {
        start += self.leaf_count;
        end += self.leaf_count;
        let mut left_sum = 0.0;
        let mut right_sum = 0.0;
        while start < end {
            if start % 2 == 1 {
                left_sum += self.tree[start];
                start += 1;
            }
            if end % 2 == 1 {
                end -= 1;
                right_sum += self.tree[end];
            }
            start /= 2;
            end /= 2;
        }
        left_sum + right_sum
    }
}

fn window_aggregate_is_incremental(name: &str) -> bool {
    matches!(name, "sum" | "count" | "avg")
}

impl<'a> WindowAggregate<'a> {
    fn new(
        name: &'a str,
        count_star: bool,
        arg: Option<&'a Expr>,
        rows: &[Vec<Value>],
        schema: &Schema,
        idxs: &[usize],
    ) -> Result<Self> {
        let values = match arg {
            Some(expr) => idxs
                .iter()
                .map(|&index| predicate::eval_row(expr, schema, &rows[index]))
                .collect::<Result<Vec<_>>>()?,
            None => vec![Value::Null; idxs.len()],
        };
        let mut non_null_count = Vec::with_capacity(values.len() + 1);
        let mut numeric_count = Vec::with_capacity(values.len() + 1);
        let mut non_integer_count = Vec::with_capacity(values.len() + 1);
        let mut integer_sums: Vec<i128> = Vec::with_capacity(values.len() + 1);
        non_null_count.push(0);
        numeric_count.push(0);
        non_integer_count.push(0);
        integer_sums.push(0);
        for value in &values {
            non_null_count.push(
                non_null_count.last().copied().unwrap_or_default() + usize::from(!value.is_null()),
            );
            numeric_count.push(
                numeric_count.last().copied().unwrap_or_default()
                    + usize::from(value.as_mysql_f64().is_some()),
            );
            non_integer_count.push(
                non_integer_count.last().copied().unwrap_or_default()
                    + usize::from(!matches!(value, Value::Int(_) | Value::Null)),
            );
            let integer = match value {
                Value::Int(value) => i128::from(*value),
                _ => 0,
            };
            integer_sums.push(
                integer_sums
                    .last()
                    .copied()
                    .unwrap_or_default()
                    .checked_add(integer)
                    .ok_or_else(|| Error::Query("window integer sum overflowed".into()))?,
            );
        }
        let numeric_sums = NumericRangeSums::new(&values);
        Ok(Self {
            name,
            count_star,
            arg,
            values,
            non_null_count,
            numeric_count,
            non_integer_count,
            integer_sums,
            numeric_sums,
        })
    }

    fn evaluate(
        &self,
        lo: usize,
        hi: usize,
        idxs: &[usize],
        rows: &[Vec<Value>],
        schema: &Schema,
    ) -> Result<Value> {
        if lo > hi || lo >= self.values.len() {
            return Ok(if self.name == "count" {
                Value::Int(0)
            } else {
                Value::Null
            });
        }
        let end = hi.min(self.values.len() - 1) + 1;
        let len = end - lo;
        if self.count_star {
            return Ok(Value::Int(len as i64));
        }
        if !window_aggregate_is_incremental(self.name) {
            return window_agg(self.name, false, &idxs[lo..end], self.arg, rows, schema);
        }
        match self.name {
            "count" => Ok(Value::Int(
                (self.non_null_count[end] - self.non_null_count[lo]) as i64,
            )),
            "sum" | "avg" => {
                let count = self.numeric_count[end] - self.numeric_count[lo];
                if count == 0 {
                    return Ok(Value::Null);
                }
                if self.non_integer_count[end] != self.non_integer_count[lo] {
                    let sum = self.numeric_sums.sum(lo, end);
                    return Ok(if self.name == "avg" {
                        Value::Float(sum / count as f64)
                    } else {
                        Value::Float(sum)
                    });
                }
                let integer_sum = self.integer_sums[end] - self.integer_sums[lo];
                if self.name == "avg" {
                    Ok(Value::Float(integer_sum as f64 / count as f64))
                } else {
                    i64::try_from(integer_sum)
                        .map(Value::Int)
                        .map_err(|_| Error::Query("window integer sum overflowed".into()))
                }
            }
            _ => unreachable!("incremental window aggregate helper and dispatcher diverged"),
        }
    }
}

#[cfg(test)]
mod window_incremental_tests {
    use super::*;

    fn fixture() -> (Schema, Vec<Vec<Value>>, Expr, Vec<usize>) {
        let schema = Schema::new(vec![ColumnDef::new("v", ColumnType::Int, true)]);
        let rows = vec![
            vec![Value::Int(2)],
            vec![Value::Null],
            vec![Value::Int(5)],
            vec![Value::Int(-1)],
        ];
        let expression = Expr::Identifier(Ident::new("v"));
        (schema, rows, expression, vec![0, 1, 2, 3])
    }

    #[test]
    fn prefix_aggregates_preserve_null_and_empty_frame_semantics() {
        let (schema, rows, expression, idxs) = fixture();
        let sum =
            WindowAggregate::new("sum", false, Some(&expression), &rows, &schema, &idxs).unwrap();
        let count =
            WindowAggregate::new("count", false, Some(&expression), &rows, &schema, &idxs).unwrap();
        let avg =
            WindowAggregate::new("avg", false, Some(&expression), &rows, &schema, &idxs).unwrap();

        assert_eq!(
            sum.evaluate(1, 3, &idxs, &rows, &schema).unwrap(),
            Value::Int(4)
        );
        assert_eq!(
            count.evaluate(1, 3, &idxs, &rows, &schema).unwrap(),
            Value::Int(2)
        );
        assert_eq!(
            avg.evaluate(1, 3, &idxs, &rows, &schema).unwrap(),
            Value::Float(2.0)
        );
        assert_eq!(
            sum.evaluate(2, 1, &idxs, &rows, &schema).unwrap(),
            Value::Null
        );
        assert_eq!(
            count.evaluate(2, 1, &idxs, &rows, &schema).unwrap(),
            Value::Int(0)
        );
    }

    #[test]
    fn integer_prefix_subtraction_is_exact_above_f64_precision() {
        let schema = Schema::new(vec![ColumnDef::new("v", ColumnType::Int, false)]);
        let rows = vec![vec![Value::Int(9_007_199_254_740_992)], vec![Value::Int(1)]];
        let expression = Expr::Identifier(Ident::new("v"));
        let idxs = vec![0, 1];
        let sum =
            WindowAggregate::new("sum", false, Some(&expression), &rows, &schema, &idxs).unwrap();

        assert_eq!(
            sum.evaluate(1, 1, &idxs, &rows, &schema).unwrap(),
            Value::Int(1)
        );
    }

    #[test]
    fn floating_range_sum_avoids_prefix_cancellation() {
        let schema = Schema::new(vec![ColumnDef::new("v", ColumnType::Float, false)]);
        let rows = vec![
            vec![Value::Float(9_007_199_254_740_992.0)],
            vec![Value::Float(1.0)],
        ];
        let expression = Expr::Identifier(Ident::new("v"));
        let idxs = vec![0, 1];
        let sum =
            WindowAggregate::new("sum", false, Some(&expression), &rows, &schema, &idxs).unwrap();

        assert_eq!(
            sum.evaluate(1, 1, &idxs, &rows, &schema).unwrap(),
            Value::Float(1.0)
        );
    }
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
            let nums: Vec<f64> = vals.iter().filter_map(Value::as_mysql_f64).collect();
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

fn virtual_relation_name(name: &ObjectName) -> Option<String> {
    let [schema, table] = name.0.as_slice() else {
        return None;
    };
    if schema.value.eq_ignore_ascii_case("information_schema") {
        Some(table.value.to_ascii_lowercase())
    } else if schema.value.eq_ignore_ascii_case("mysql") {
        Some(format!("mysql.{}", table.value.to_ascii_lowercase()))
    } else {
        None
    }
}

fn virtual_relation_supported(name: &str) -> bool {
    INFORMATION_SCHEMA_VIEWS.contains(&name) || matches!(name, "mysql.user" | "mysql.db")
}

fn collect_expr_subqueries(expr: &Expr) -> Vec<SqlQuery> {
    use sqlparser::ast::{Visit, Visitor};
    use std::ops::ControlFlow;

    #[derive(Default)]
    struct ImmediateQueryCollector {
        depth: usize,
        queries: Vec<SqlQuery>,
    }

    impl Visitor for ImmediateQueryCollector {
        type Break = std::convert::Infallible;

        fn pre_visit_query(&mut self, query: &SqlQuery) -> ControlFlow<Self::Break> {
            if self.depth == 0 {
                self.queries.push(query.clone());
            }
            self.depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &SqlQuery) -> ControlFlow<Self::Break> {
            self.depth -= 1;
            ControlFlow::Continue(())
        }
    }

    let mut collector = ImmediateQueryCollector::default();
    let _ = expr.visit(&mut collector);
    collector.queries
}

fn collect_factor_relations(
    factor: &TableFactor,
    ctes: &std::collections::HashSet<String>,
    relations: &mut Vec<ObjectName>,
) {
    match factor {
        TableFactor::Table { name, .. } => {
            let is_cte = matches!(name.0.as_slice(), [relation]
                if ctes.contains(&relation.value.to_ascii_lowercase()));
            if !is_cte {
                relations.push(name.clone());
            }
        }
        TableFactor::Derived { subquery, .. } => {
            collect_query_relations_inner(subquery, ctes, relations)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_table_with_joins_relations(table_with_joins, ctes, relations),
        _ => {}
    }
}

fn collect_table_with_joins_relations(
    table: &TableWithJoins,
    ctes: &std::collections::HashSet<String>,
    relations: &mut Vec<ObjectName>,
) {
    collect_factor_relations(&table.relation, ctes, relations);
    for join in &table.joins {
        collect_factor_relations(&join.relation, ctes, relations);
        let constraint = match &join.join_operator {
            JoinOperator::Inner(constraint)
            | JoinOperator::LeftOuter(constraint)
            | JoinOperator::RightOuter(constraint)
            | JoinOperator::FullOuter(constraint)
            | JoinOperator::LeftSemi(constraint)
            | JoinOperator::RightSemi(constraint)
            | JoinOperator::LeftAnti(constraint)
            | JoinOperator::RightAnti(constraint) => Some(constraint),
            _ => None,
        };
        if let Some(JoinConstraint::On(expr)) = constraint {
            for query in collect_expr_subqueries(expr) {
                collect_query_relations_inner(&query, ctes, relations);
            }
        }
    }
}

fn collect_select_expr_relations(
    select: &Select,
    ctes: &std::collections::HashSet<String>,
    relations: &mut Vec<ObjectName>,
) {
    let mut expressions: Vec<&Expr> = Vec::new();
    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => expressions.push(expr),
            _ => {}
        }
    }
    expressions.extend(select.prewhere.iter());
    expressions.extend(select.selection.iter());
    expressions.extend(select.having.iter());
    expressions.extend(select.qualify.iter());
    expressions.extend(&select.cluster_by);
    expressions.extend(&select.distribute_by);
    expressions.extend(&select.sort_by);
    if let sqlparser::ast::GroupByExpr::Expressions(group_by, _) = &select.group_by {
        expressions.extend(group_by);
    }
    for expr in expressions {
        for query in collect_expr_subqueries(expr) {
            collect_query_relations_inner(&query, ctes, relations);
        }
    }
}

fn collect_set_relations(
    body: &SetExpr,
    ctes: &std::collections::HashSet<String>,
    relations: &mut Vec<ObjectName>,
) {
    match body {
        SetExpr::Select(select) => {
            for table in &select.from {
                collect_table_with_joins_relations(table, ctes, relations);
            }
            collect_select_expr_relations(select, ctes, relations);
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_relations(left, ctes, relations);
            collect_set_relations(right, ctes, relations);
        }
        SetExpr::Query(query) => collect_query_relations_inner(query, ctes, relations),
        SetExpr::Table(table) => {
            if let Some(table_name) = &table.table_name {
                if table.schema_name.is_none() && ctes.contains(&table_name.to_ascii_lowercase()) {
                    return;
                }
                let mut parts = Vec::with_capacity(2);
                if let Some(schema_name) = &table.schema_name {
                    parts.push(sqlparser::ast::Ident::new(schema_name.clone()));
                }
                parts.push(sqlparser::ast::Ident::new(table_name.clone()));
                relations.push(ObjectName(parts));
            }
        }
        _ => {}
    }
}

fn collect_query_relations_inner(
    query: &SqlQuery,
    outer_ctes: &std::collections::HashSet<String>,
    relations: &mut Vec<ObjectName>,
) {
    let mut ctes = outer_ctes.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.to_ascii_lowercase();
            let mut body_ctes = ctes.clone();
            if with.recursive {
                body_ctes.insert(name.clone());
            }
            collect_query_relations_inner(&cte.query, &body_ctes, relations);
            ctes.insert(name);
        }
    }
    collect_set_relations(&query.body, &ctes, relations);

    if let Some(order_by) = &query.order_by {
        for order in &order_by.exprs {
            for nested in collect_expr_subqueries(&order.expr) {
                collect_query_relations_inner(&nested, &ctes, relations);
            }
        }
    }
    if let Some(limit) = &query.limit {
        for nested in collect_expr_subqueries(limit) {
            collect_query_relations_inner(&nested, &ctes, relations);
        }
    }
    if let Some(offset) = &query.offset {
        for nested in collect_expr_subqueries(&offset.value) {
            collect_query_relations_inner(&nested, &ctes, relations);
        }
    }
    for limit_by in &query.limit_by {
        for nested in collect_expr_subqueries(limit_by) {
            collect_query_relations_inner(&nested, &ctes, relations);
        }
    }
}

fn relation_dependency_order(
    graph: &std::collections::HashMap<String, Vec<String>>,
) -> Result<Vec<String>> {
    fn visit(
        view: &str,
        graph: &std::collections::HashMap<String, Vec<String>>,
        active: &mut std::collections::HashSet<String>,
        complete: &mut std::collections::HashSet<String>,
        order: &mut Vec<String>,
    ) -> Result<()> {
        if complete.contains(view) {
            return Ok(());
        }
        if !active.insert(view.to_string()) {
            return Err(Error::Query(format!(
                "circular view dependency involving {view}"
            )));
        }
        if let Some(dependencies) = graph.get(view) {
            for dependency in dependencies {
                visit(dependency, graph, active, complete, order)?;
            }
        }
        active.remove(view);
        complete.insert(view.to_string());
        order.push(view.to_string());
        Ok(())
    }

    let mut active = std::collections::HashSet::new();
    let mut complete = std::collections::HashSet::new();
    let mut order = Vec::new();
    for view in graph.keys() {
        visit(view, graph, &mut active, &mut complete, &mut order)?;
    }
    Ok(order)
}

struct ValidatedQueryRelations {
    base_tables: Vec<String>,
    matviews: Vec<String>,
}

async fn validated_query_relations(
    db: &Session,
    query: &SqlQuery,
    forbidden_view: Option<&str>,
) -> Result<ValidatedQueryRelations> {
    let mut pending: Vec<(Option<String>, SqlQuery)> = vec![(None, query.clone())];
    let mut loaded_views = std::collections::HashSet::new();
    let mut dependency_graph: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    let mut base_tables = Vec::new();
    let mut seen_base_tables = std::collections::HashSet::new();
    let mut matviews = std::collections::HashSet::new();

    while let Some((owner, query)) = pending.pop() {
        let mut relations = Vec::new();
        collect_query_relations_inner(&query, &std::collections::HashSet::new(), &mut relations);
        for relation in relations {
            if let Some(virtual_name) = virtual_relation_name(&relation) {
                if !virtual_relation_supported(&virtual_name) {
                    return Err(Error::Catalog(format!("no such table: {relation}")));
                }
                continue;
            }

            let table = stored_table_ident(db, &relation)?;
            if forbidden_view.is_some_and(|target| table == target) {
                return Err(Error::Query(format!(
                    "view {table} cannot reference itself"
                )));
            }
            if let Some(sql) = catalog::load_view(db, &table).await? {
                if let Some(owner) = &owner {
                    dependency_graph
                        .entry(owner.clone())
                        .or_default()
                        .push(table.clone());
                }
                dependency_graph.entry(table.clone()).or_default();
                if loaded_views.insert(table.clone()) {
                    pending.push((Some(table), parse_query(&sql)?));
                }
                continue;
            }
            if !catalog::exists(db, &table).await? {
                return Err(Error::Catalog(format!("no such table: {table}")));
            }
            if let Some(sql) = db.get(catalog::matview_key(&table)).await? {
                let sql = String::from_utf8_lossy(&sql).into_owned();
                if let Some(owner) = &owner {
                    dependency_graph
                        .entry(owner.clone())
                        .or_default()
                        .push(table.clone());
                }
                dependency_graph.entry(table.clone()).or_default();
                matviews.insert(table.clone());
                if loaded_views.insert(table.clone()) {
                    pending.push((Some(table), parse_query(&sql)?));
                }
            } else if seen_base_tables.insert(table.clone()) {
                base_tables.push(table);
            }
        }
    }

    let dependency_order = relation_dependency_order(&dependency_graph)?;
    Ok(ValidatedQueryRelations {
        base_tables,
        matviews: dependency_order
            .into_iter()
            .filter(|relation| matviews.contains(relation))
            .collect(),
    })
}

/// Validate every physical relation named by a query, recursively including
/// stored-view dependencies, before an operation with side effects begins.
pub(crate) async fn validate_query_relations(db: &Session, query: &SqlQuery) -> Result<()> {
    validated_query_relations(db, query, None).await.map(|_| ())
}

pub(crate) async fn query_materialized_relations(
    db: &Session,
    query: &SqlQuery,
) -> Result<Vec<String>> {
    validated_query_relations(db, query, None)
        .await
        .map(|relations| relations.matviews)
}

#[cfg(test)]
mod query_relation_validation_tests {
    use super::collect_query_relations_inner;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn collects_relations_from_expression_subqueries() {
        let mut statements = Parser::parse_sql(
            &MySqlDialect {},
            "SELECT * FROM outer_table AS o
             WHERE EXISTS (
                 SELECT 1 FROM nested_table AS n WHERE n.id = o.id
             )",
        )
        .unwrap();
        let Statement::Query(query) = statements.remove(0) else {
            panic!("expected query")
        };
        let mut relations = Vec::new();
        collect_query_relations_inner(&query, &std::collections::HashSet::new(), &mut relations);
        let names = relations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        assert_eq!(names, ["outer_table", "nested_table"]);
    }
}

fn immediate_column_references(expr: &Expr) -> Vec<Expr> {
    use sqlparser::ast::{Visit, Visitor};
    use std::ops::ControlFlow;

    #[derive(Default)]
    struct Collector {
        query_depth: usize,
        references: Vec<Expr>,
    }

    impl Visitor for Collector {
        type Break = std::convert::Infallible;

        fn pre_visit_query(&mut self, _query: &SqlQuery) -> ControlFlow<Self::Break> {
            self.query_depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &SqlQuery) -> ControlFlow<Self::Break> {
            self.query_depth -= 1;
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.query_depth == 0
                && matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
            {
                self.references.push(expr.clone());
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector::default();
    let _ = expr.visit(&mut collector);
    collector.references
}

fn column_reference_resolves(reference: &Expr, schema: &Schema) -> Result<bool> {
    let (resolution, qualified) = match reference {
        Expr::Identifier(identifier) if identifier.value.starts_with('@') => return Ok(true),
        Expr::Identifier(identifier) => {
            (predicate::resolve_index(&identifier.value, schema), false)
        }
        Expr::CompoundIdentifier(parts)
            if parts
                .first()
                .is_some_and(|part| part.value.starts_with('@')) =>
        {
            return Ok(true);
        }
        Expr::CompoundIdentifier(parts) => (predicate::resolve_index_parts(parts, schema), true),
        _ => return Ok(true),
    };
    match resolution {
        Ok(_) => Ok(true),
        Err(Error::Catalog(_)) | Err(Error::UnknownColumn(_)) => Ok(false),
        // A qualified correlated reference can share its bare column name with
        // multiple local relations. No local exact match means it belongs to an
        // outer scope; only an unqualified local ambiguity is terminal here.
        Err(Error::Query(_)) if qualified => Ok(false),
        Err(error) => Err(error),
    }
}

fn column_reference_name(reference: &Expr) -> String {
    match reference {
        Expr::Identifier(identifier) => identifier.value.clone(),
        Expr::CompoundIdentifier(parts) => parts
            .iter()
            .map(|part| part.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        _ => reference.to_string(),
    }
}

fn function_name(function: &sqlparser::ast::Function) -> String {
    function
        .name
        .0
        .last()
        .map(|identifier| identifier.value.to_ascii_lowercase())
        .unwrap_or_default()
}

fn aggregate_function(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "group_concat"
            | "stddev"
            | "std"
            | "stddev_pop"
            | "stddev_samp"
            | "variance"
            | "var_pop"
            | "var_samp"
            | "bit_or"
            | "bit_and"
            | "bit_xor"
            | "facet"
            | "percentile"
            | "quantile"
            | "median"
    )
}

fn window_function(name: &str) -> bool {
    matches!(
        name,
        "row_number"
            | "rank"
            | "dense_rank"
            | "lag"
            | "lead"
            | "sum"
            | "count"
            | "avg"
            | "min"
            | "max"
            | "ntile"
            | "first_value"
            | "last_value"
            | "nth_value"
    )
}

fn validate_window_function_arity(name: &str, arity: usize) -> Result<()> {
    let valid = match name {
        "row_number" | "rank" | "dense_rank" => arity == 0,
        "lag" | "lead" => (1..=3).contains(&arity),
        "sum" | "count" | "avg" | "min" | "max" | "ntile" | "first_value" | "last_value" => {
            arity == 1
        }
        "nth_value" => arity == 2,
        _ => {
            return Err(Error::Unsupported(format!(
                "unknown window function: {name}"
            )))
        }
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Query(format!(
            "invalid argument count for window function {}",
            name.to_ascii_uppercase()
        )))
    }
}

fn function_argument_count(function: &sqlparser::ast::Function) -> Result<usize> {
    match &function.args {
        sqlparser::ast::FunctionArguments::None => Ok(0),
        sqlparser::ast::FunctionArguments::List(arguments) => Ok(arguments.args.len()),
        sqlparser::ast::FunctionArguments::Subquery(_) => {
            Err(Error::Unsupported("subquery function argument".into()))
        }
    }
}

#[derive(Clone, Copy)]
struct FunctionContext {
    aggregates: bool,
    windows: bool,
    hybrid: bool,
    clause: &'static str,
}

const ROW_FUNCTIONS: FunctionContext = FunctionContext {
    aggregates: false,
    windows: false,
    hybrid: false,
    clause: "row expression",
};
const PROJECTION_FUNCTIONS: FunctionContext = FunctionContext {
    aggregates: true,
    windows: true,
    hybrid: true,
    clause: "projection",
};
const ORDER_FUNCTIONS: FunctionContext = FunctionContext {
    aggregates: true,
    windows: true,
    hybrid: false,
    clause: "ORDER BY",
};
const AGGREGATE_FUNCTIONS: FunctionContext = FunctionContext {
    aggregates: true,
    windows: false,
    hybrid: false,
    clause: "aggregate expression",
};

fn validate_function(function: &sqlparser::ast::Function, context: FunctionContext) -> Result<()> {
    let name = function_name(function);
    let arity = function_argument_count(function)?;

    if function.over.is_some() {
        if !context.windows {
            return Err(Error::Query(format!(
                "window function {name} is not allowed in {}",
                context.clause
            )));
        }
        if !window_function(&name) {
            return Err(Error::Unsupported(format!(
                "unknown window function: {name}"
            )));
        }
        return validate_window_function_arity(&name, arity);
    }
    if aggregate_function(&name) {
        if !context.aggregates {
            return Err(Error::Query(format!(
                "aggregate function {name} is not allowed in {}",
                context.clause
            )));
        }
        return aggregate::validate_function_arity(&name, arity);
    }
    if name == "hybrid" {
        if !context.hybrid {
            return Err(Error::Query(format!(
                "HYBRID is not allowed in {}",
                context.clause
            )));
        }
        if arity != 4 {
            return Err(Error::Query("HYBRID expects 4 arguments".into()));
        }
        return Ok(());
    }
    if predicate::scalar_function_supported(&name) {
        return predicate::validate_scalar_function_arity(&name, arity);
    }
    Err(Error::Unsupported(format!("unknown function: {name}")))
}

struct FunctionValidator {
    context: FunctionContext,
    query_depth: usize,
    expression_depth: usize,
    aggregate_depth: usize,
    window_depth: usize,
    error: Option<Error>,
}

impl FunctionValidator {
    fn new(context: FunctionContext) -> Self {
        Self {
            context,
            query_depth: 0,
            expression_depth: 0,
            aggregate_depth: 0,
            window_depth: 0,
            error: None,
        }
    }
}

impl sqlparser::ast::Visitor for FunctionValidator {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &SqlQuery) -> std::ops::ControlFlow<Self::Break> {
        self.query_depth += 1;
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &SqlQuery) -> std::ops::ControlFlow<Self::Break> {
        self.query_depth -= 1;
        std::ops::ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> std::ops::ControlFlow<Self::Break> {
        if self.query_depth != 0 {
            return std::ops::ControlFlow::Continue(());
        }
        let mut context = self.context;
        context.hybrid &= self.expression_depth == 0;
        self.expression_depth += 1;
        let Expr::Function(function) = expression else {
            return std::ops::ControlFlow::Continue(());
        };
        let name = function_name(function);
        let aggregate = function.over.is_none() && aggregate_function(&name);
        let window = function.over.is_some();
        if (aggregate || window) && (self.aggregate_depth != 0 || self.window_depth != 0) {
            self.error = Some(Error::Query(
                "aggregate and window functions cannot be nested".into(),
            ));
            return std::ops::ControlFlow::Break(());
        }
        if let Err(error) = validate_function(function, context) {
            self.error = Some(error);
            return std::ops::ControlFlow::Break(());
        }
        self.aggregate_depth += usize::from(aggregate);
        self.window_depth += usize::from(window);
        std::ops::ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expression: &Expr) -> std::ops::ControlFlow<Self::Break> {
        if self.query_depth == 0 {
            if let Expr::Function(function) = expression {
                let name = function_name(function);
                self.aggregate_depth -=
                    usize::from(function.over.is_none() && aggregate_function(&name));
                self.window_depth -= usize::from(function.over.is_some());
            }
            self.expression_depth -= 1;
        }
        std::ops::ControlFlow::Continue(())
    }
}

fn validate_expression_functions(expression: &Expr, context: FunctionContext) -> Result<()> {
    use sqlparser::ast::Visit;

    let mut validator = FunctionValidator::new(context);
    let _ = expression.visit(&mut validator);
    match validator.error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn validate_expression_columns(
    db: &Session,
    expression: &Expr,
    local: &Schema,
    outer: Option<&Schema>,
    ctes: &std::collections::HashMap<String, Schema>,
    functions: FunctionContext,
) -> Result<()> {
    validate_expression_functions(expression, functions)?;
    validate_expression_column_references(db, expression, local, outer, ctes).await
}

async fn validate_expression_column_references(
    db: &Session,
    expression: &Expr,
    local: &Schema,
    outer: Option<&Schema>,
    ctes: &std::collections::HashMap<String, Schema>,
) -> Result<()> {
    for reference in immediate_column_references(expression) {
        if column_reference_resolves(&reference, local)? {
            continue;
        }
        if let Some(outer) = outer {
            if column_reference_resolves(&reference, outer)? {
                continue;
            }
        }
        return Err(Error::UnknownColumn(column_reference_name(&reference)));
    }
    let mut nested_outer = local.clone();
    if let Some(outer) = outer {
        nested_outer.columns.extend(outer.columns.iter().cloned());
    }
    for subquery in collect_expr_subqueries(expression) {
        Box::pin(static_query_schema_scoped(
            db,
            &subquery,
            Some(&nested_outer),
            ctes,
        ))
        .await?;
    }
    Ok(())
}

fn qualify_static_schema(db: &Session, schema: Schema, factor: &TableFactor) -> Result<Schema> {
    let qualifier = factor_qualifier_object(db, factor)
        .ok_or_else(|| Error::Catalog("empty table qualifier".into()))?;
    Ok(qualify_relation_schema(schema, &qualifier))
}

async fn static_factor_schema(
    db: &Session,
    factor: &TableFactor,
    outer: Option<&Schema>,
    ctes: &std::collections::HashMap<String, Schema>,
) -> Result<Schema> {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            if let [relation] = name.0.as_slice() {
                if let Some(schema) = ctes.get(&relation.value.to_ascii_lowercase()) {
                    let alias_columns = alias
                        .as_ref()
                        .map(|alias| {
                            alias
                                .columns
                                .iter()
                                .map(|column| column.name.value.clone())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    let schema = apply_col_aliases(schema.clone(), &alias_columns)?;
                    return qualify_static_schema(db, schema, factor);
                }
            }
            if let Some(view) = information_schema_view(factor) {
                let schema = information_schema_schema(&view)?;
                return qualify_static_schema(db, schema, factor);
            }
            let table = stored_table_ident(db, name)?;
            if let Some(sql) = catalog::load_view(db, &table).await? {
                let schema = Box::pin(static_query_schema(db, &parse_query(&sql)?, outer)).await?;
                return qualify_static_schema(db, schema, factor);
            }
            resolve_table(db, factor)
                .await
                .map(|(_, columns)| Schema::new(columns))
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let schema = Box::pin(static_query_schema_scoped(db, subquery, outer, ctes)).await?;
            let alias = alias.as_ref().ok_or_else(|| {
                Error::Query("a derived table (FROM (SELECT ...)) needs an alias".into())
            })?;
            let alias_columns = alias
                .columns
                .iter()
                .map(|column| column.name.value.clone())
                .collect::<Vec<_>>();
            let schema = apply_col_aliases(schema, &alias_columns)?;
            qualify_static_schema(db, schema, factor)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => static_table_with_joins_schema(db, table_with_joins, outer, ctes).await,
        _ => Err(Error::Unsupported(
            "only plain and derived table references can be validated".into(),
        )),
    }
}

async fn static_table_with_joins_schema(
    db: &Session,
    table: &TableWithJoins,
    outer: Option<&Schema>,
    ctes: &std::collections::HashMap<String, Schema>,
) -> Result<Schema> {
    let mut schema = Box::pin(static_factor_schema(db, &table.relation, outer, ctes)).await?;
    for join in &table.joins {
        let joined = Box::pin(static_factor_schema(db, &join.relation, outer, ctes)).await?;
        let (kind, _) = join_kind(&join.join_operator)?;
        let using_keys = resolve_using_keys(&join.join_operator, &schema, &joined)?;
        let physical = combined_join_schema(&schema, &joined);
        let constraint = join_constraint(&join.join_operator);
        if let Some(JoinConstraint::On(expression)) = constraint {
            Box::pin(validate_expression_columns(
                db,
                expression,
                &physical,
                outer,
                ctes,
                ROW_FUNCTIONS,
            ))
            .await?;
        }
        schema = match using_keys {
            Some(keys) => {
                coalesce_using_join(&schema, &joined, physical, Vec::new(), kind, &keys).0
            }
            None => physical,
        };
    }
    Ok(schema)
}

async fn static_select_schema(
    db: &Session,
    select: &Select,
    outer: Option<&Schema>,
    ctes: &std::collections::HashMap<String, Schema>,
) -> Result<(Schema, Schema)> {
    let mut source = Schema::new(Vec::new());
    for table in &select.from {
        let schema = Box::pin(static_table_with_joins_schema(db, table, outer, ctes)).await?;
        source = combined_join_schema(&source, &schema);
    }

    if let Some(selection) = &select.selection {
        Box::pin(validate_expression_columns(
            db,
            selection,
            &source,
            outer,
            ctes,
            ROW_FUNCTIONS,
        ))
        .await?;
    }
    for item in &select.projection {
        if let sqlparser::ast::SelectItem::UnnamedExpr(expression)
        | sqlparser::ast::SelectItem::ExprWithAlias {
            expr: expression, ..
        } = item
        {
            Box::pin(validate_expression_columns(
                db,
                expression,
                &source,
                outer,
                ctes,
                PROJECTION_FUNCTIONS,
            ))
            .await?;
        }
    }
    let default_table = single_relation_alias(select);
    let output = project_exprs(&select.projection, &source, &[], default_table.as_deref())
        .map(|(schema, _)| schema)?;
    let visible = projected_expression_schema(&source, &output);
    if let sqlparser::ast::GroupByExpr::Expressions(expressions, _) = &select.group_by {
        for expression in expressions {
            Box::pin(validate_expression_columns(
                db,
                expression,
                &visible,
                outer,
                ctes,
                ROW_FUNCTIONS,
            ))
            .await?;
        }
    }
    if let Some(having) = &select.having {
        Box::pin(validate_expression_columns(
            db,
            having,
            &visible,
            outer,
            ctes,
            AGGREGATE_FUNCTIONS,
        ))
        .await?;
    }
    Ok((output, visible))
}

async fn static_set_schema(
    db: &Session,
    body: &SetExpr,
    outer: Option<&Schema>,
    ctes: &std::collections::HashMap<String, Schema>,
) -> Result<Schema> {
    match body {
        SetExpr::Select(select) => static_select_schema(db, select, outer, ctes)
            .await
            .map(|(output, _)| output),
        SetExpr::Query(query) => Box::pin(static_query_schema_scoped(db, query, outer, ctes)).await,
        SetExpr::SetOperation { left, right, .. } => {
            let left = Box::pin(static_set_schema(db, left, outer, ctes)).await?;
            let right = Box::pin(static_set_schema(db, right, outer, ctes)).await?;
            if left.columns.len() != right.columns.len() {
                return Err(Error::Query(
                    "set-operation arms have different column counts".into(),
                ));
            }
            Ok(left)
        }
        SetExpr::Values(_) | SetExpr::Table(_) => Err(Error::Unsupported(
            "query form is not supported by execution".into(),
        )),
        _ => Err(Error::Unsupported(
            "query form cannot be statically validated".into(),
        )),
    }
}

async fn static_query_schema(
    db: &Session,
    query: &SqlQuery,
    outer: Option<&Schema>,
) -> Result<Schema> {
    static_query_schema_scoped(db, query, outer, &std::collections::HashMap::new()).await
}

async fn static_query_schema_scoped(
    db: &Session,
    query: &SqlQuery,
    outer: Option<&Schema>,
    inherited_ctes: &std::collections::HashMap<String, Schema>,
) -> Result<Schema> {
    if QUERY_NESTING.try_with(|_| ()).is_ok() {
        static_query_schema_with_stack(db, query, outer, inherited_ctes).await
    } else {
        QUERY_NESTING
            .scope(
                std::cell::Cell::new(0),
                static_query_schema_with_stack(db, query, outer, inherited_ctes),
            )
            .await
    }
}

async fn static_query_schema_with_stack(
    db: &Session,
    query: &SqlQuery,
    outer: Option<&Schema>,
    inherited_ctes: &std::collections::HashMap<String, Schema>,
) -> Result<Schema> {
    const RED_ZONE: usize = 1024 * 1024;
    const STACK_SIZE: usize = 2 * 1024 * 1024;

    let _nesting = QueryNestingGuard::enter()?;
    let mut future = Box::pin(static_query_schema_scoped_inner(
        db,
        query,
        outer,
        inherited_ctes,
    ));
    std::future::poll_fn(move |context| {
        stacker::maybe_grow(RED_ZONE, STACK_SIZE, || {
            std::future::Future::poll(future.as_mut(), context)
        })
    })
    .await
}

async fn static_query_schema_scoped_inner(
    db: &Session,
    query: &SqlQuery,
    outer: Option<&Schema>,
    inherited_ctes: &std::collections::HashMap<String, Schema>,
) -> Result<Schema> {
    let mut ctes = inherited_ctes.clone();
    if let Some(with) = &query.with {
        let reachable = reachable_recursive_ctes(query, with);
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            let key = name.to_ascii_lowercase();
            if !reachable.contains(&key) {
                continue;
            }
            let alias_columns = cte
                .alias
                .columns
                .iter()
                .map(|column| column.name.value.clone())
                .collect::<Vec<_>>();
            let recursive = with.recursive && query_refs_table(&cte.query, &name);
            let schema = if recursive {
                let (_, anchor, recursive_term) = split_recursive(&cte.query, &name)?;
                let anchor_schema =
                    Box::pin(static_query_schema_scoped(db, &anchor, outer, &ctes)).await?;
                let anchor_schema = apply_col_aliases(anchor_schema, &alias_columns)?;
                ctes.insert(key.clone(), anchor_schema.clone());
                let recursive_schema = Box::pin(static_query_schema_scoped(
                    db,
                    &recursive_term,
                    outer,
                    &ctes,
                ))
                .await?;
                if anchor_schema.columns.len() != recursive_schema.columns.len() {
                    return Err(Error::Query(format!(
                        "recursive CTE {name} arms have different column counts"
                    )));
                }
                anchor_schema
            } else {
                let schema =
                    Box::pin(static_query_schema_scoped(db, &cte.query, outer, &ctes)).await?;
                apply_col_aliases(schema, &alias_columns)?
            };
            ctes.insert(key, schema);
        }
    }

    let (schema, order_schema) = match query.body.as_ref() {
        SetExpr::Select(select) => static_select_schema(db, select, outer, &ctes).await?,
        _ => {
            let schema = Box::pin(static_set_schema(db, &query.body, outer, &ctes)).await?;
            (schema.clone(), schema)
        }
    };
    if let Some(order_by) = &query.order_by {
        for order in &order_by.exprs {
            Box::pin(validate_expression_columns(
                db,
                &order.expr,
                &order_schema,
                outer,
                &ctes,
                ORDER_FUNCTIONS,
            ))
            .await?;
        }
    }
    if let Some(limit) = &query.limit {
        Box::pin(validate_expression_columns(
            db,
            limit,
            &Schema::new(Vec::new()),
            outer,
            &ctes,
            ROW_FUNCTIONS,
        ))
        .await?;
    }
    if let Some(offset) = &query.offset {
        Box::pin(validate_expression_columns(
            db,
            &offset.value,
            &Schema::new(Vec::new()),
            outer,
            &ctes,
            ROW_FUNCTIONS,
        ))
        .await?;
    }
    Ok(schema)
}

pub(crate) async fn validate_query_columns(db: &Session, query: &SqlQuery) -> Result<()> {
    let mut normalized = query.clone();
    normalize_query_qualifiers(&mut normalized, &db.database())?;
    static_query_schema(db, &normalized, None).await.map(|_| ())
}

/// `CREATE VIEW name [(cols)] AS SELECT ...` — store the view's SELECT text.
pub async fn create_view(
    db: &Session,
    name: &ObjectName,
    columns: &[sqlparser::ast::ViewColumnDef],
    query: &SqlQuery,
    or_replace: bool,
) -> Result<QueryResult> {
    let name = stored_table_ident(db, name)?;
    if catalog::exists(db, &name).await? {
        return Err(Error::Catalog(format!(
            "cannot create view: a table named '{name}' exists"
        )));
    }
    if !or_replace && catalog::load_view(db, &name).await?.is_some() {
        return Err(Error::Catalog(format!("view already exists: {name}")));
    }
    validated_query_relations(db, query, Some(&name)).await?;
    validate_query_columns(db, query).await?;

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
    if information_schema_view(tf).is_some() {
        return Ok(());
    }
    if let TableFactor::Table { name, alias, .. } = tf {
        let table = stored_table_ident(db, name)?;
        if let Some(sql) = catalog::load_view(db, &table).await? {
            let vq = parse_query(&sql)?;
            let al = alias.clone().unwrap_or_else(|| sqlparser::ast::TableAlias {
                name: sqlparser::ast::Ident::new(table),
                columns: Vec::new(),
            });
            *tf = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(vq),
                alias: Some(al),
            };
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
static TEMP_OWNER_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Debug, Clone)]
struct OwnedTempTable {
    name: String,
    definition: Vec<u8>,
    owner: Vec<u8>,
}

fn temp_name(n: u64, base: &str) -> String {
    let clean: String = base.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    format!("__cte_{n}_{clean}")
}

fn temp_owner_key(name: &str) -> Vec<u8> {
    format!("sys::cte-owner::{name}").into_bytes()
}

fn reachable_recursive_ctes(query: &SqlQuery, with: &With) -> std::collections::HashSet<String> {
    let names = with
        .cte_tables
        .iter()
        .map(|cte| cte.alias.name.value.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut outer = query.clone();
    outer.with = None;
    let mut reachable = names
        .iter()
        .filter(|name| query_refs_table(&outer, name))
        .cloned()
        .collect::<std::collections::HashSet<_>>();

    loop {
        let before = reachable.len();
        for (index, cte) in with.cte_tables.iter().enumerate() {
            let name = cte.alias.name.value.to_ascii_lowercase();
            if !reachable.contains(&name) {
                continue;
            }
            // A CTE body can see earlier declarations, plus itself when the
            // WITH clause is recursive. Later declarations do not shadow a
            // physical table of the same name at this declaration point.
            let visible = index + usize::from(with.recursive);
            for dependency in names.iter().take(visible) {
                if query_refs_table(&cte.query, dependency) {
                    reachable.insert(dependency.clone());
                }
            }
        }
        if reachable.len() == before {
            return reachable;
        }
    }
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
    let mut created: Vec<OwnedTempTable> = Vec::new();
    let reachable = reachable_recursive_ctes(query, with);

    let result = async {
        for cte in &with.cte_tables {
            let cname = cte.alias.name.value.clone();
            if !reachable.contains(&cname.to_ascii_lowercase()) {
                continue;
            }
            // Rewrite references to earlier CTEs in this body.
            let body = rewrite_table_refs((*cte.query).clone(), &temp_names);
            let alias_cols: Vec<String> = cte
                .alias
                .columns
                .iter()
                .map(|c| c.name.value.clone())
                .collect();

            let temp = if query_refs_table(&body, &cname) {
                materialize_recursive(db, vindex, &cname, &body, &alias_cols, &mut created).await?
            } else {
                let (schema, rows) = run_subquery_schema(db, vindex, &body).await?;
                let schema = apply_col_aliases(schema, &alias_cols)?;
                let owned = create_temp_table(db, &cname, &schema).await?;
                let temp = owned.name.clone();
                created.push(owned.clone());
                fill_table(db, &owned, &schema, &rows).await?;
                temp
            };
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
    for temp in &created {
        let _ = drop_temp_table(db, temp).await;
    }

    let (schema, rows) = result?;
    Ok(QueryResult::Rows(RowStream::literal(schema, rows)))
}

/// Fixpoint materialisation of a recursive CTE into an owned temporary table.
async fn materialize_recursive(
    db: &Session,
    vindex: &VectorRegistry,
    cname: &str,
    body: &SqlQuery,
    alias_cols: &[String],
    created: &mut Vec<OwnedTempTable>,
) -> Result<String> {
    const MAX_ITERS: usize = 1000;
    let (distinct, anchor_q, rec_q) = split_recursive(body, cname)?;

    let (schema, anchor_rows) = run_subquery_schema(db, vindex, &anchor_q).await?;
    let schema = apply_col_aliases(schema, alias_cols)?;
    let recursive_columns = schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    validate_recursive_table_reference(rec_q.body.as_ref(), cname, &recursive_columns)?;
    let owned = create_temp_table(db, cname, &schema).await?;
    let temp = owned.name.clone();
    created.push(owned.clone());

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
    self_map.insert(cname.to_ascii_lowercase(), temp.clone());
    let rec_q = rewrite_table_refs(rec_q, &self_map);
    let (recursive_schema, _) = run_subquery_schema(db, vindex, &rec_q).await?;
    if recursive_schema.columns.len() != schema.columns.len() {
        return Err(Error::Query(format!(
            "recursive CTE {cname} arms have different column counts"
        )));
    }

    let mut iters = 0;
    while !frontier.is_empty() {
        iters += 1;
        if iters > MAX_ITERS {
            return Err(Error::Query(format!(
                "recursive CTE '{cname}' exceeded {MAX_ITERS} iterations"
            )));
        }
        // The recursive term sees only the previous iteration's rows.
        clear_table(db, &owned).await?;
        fill_table(db, &owned, &schema, &frontier).await?;

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
    clear_table(db, &owned).await?;
    fill_table(db, &owned, &schema, &all_rows).await?;
    Ok(temp)
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
    let scope = map.iter().fold(CteScope::default(), |scope, (name, temp)| {
        scope.bind(name.clone(), temp.clone())
    });
    let mut rewriter = TempTableRewriter {
        scopes: vec![scope],
        saved_with: Vec::new(),
    };
    let _ = VisitMut::visit(&mut query, &mut rewriter);
    query
}

fn query_refs_table(query: &SqlQuery, name: &str) -> bool {
    refs_table(query, name)
}

fn setexpr_refs_table(body: &SetExpr, name: &str) -> bool {
    refs_table_count(body, name) != 0
}

struct TempTableRewriter {
    scopes: Vec<CteScope<String>>,
    saved_with: Vec<Option<sqlparser::ast::With>>,
}

impl VisitorMut for TempTableRewriter {
    type Break = std::convert::Infallible;

    fn pre_visit_query(&mut self, query: &mut SqlQuery) -> ControlFlow<Self::Break> {
        let mut scope = self.scopes.last().cloned().unwrap_or_default();
        let mut saved_with = query.with.take();
        if let Some(with) = &mut saved_with {
            for cte in &mut with.cte_tables {
                if with.recursive {
                    scope = scope.shadow(cte.alias.name.value.clone());
                }
                let mut nested = Self {
                    scopes: vec![scope.clone()],
                    saved_with: Vec::new(),
                };
                let _ = VisitMut::visit(cte.query.as_mut(), &mut nested);
                if !with.recursive {
                    scope = scope.shadow(cte.alias.name.value.clone());
                }
            }
        }
        self.saved_with.push(saved_with);
        self.scopes.push(scope);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut SqlQuery) -> ControlFlow<Self::Break> {
        self.scopes.pop();
        query.with = self.saved_with.pop().flatten();
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        let TableFactor::Table {
            name, alias, args, ..
        } = table_factor
        else {
            return ControlFlow::Continue(());
        };
        if args.is_some() || name.0.len() != 1 {
            return ControlFlow::Continue(());
        }
        let original = name.0[0].value.clone();
        let Some(temp) = self.scopes.last().and_then(|scope| scope.get(&original)) else {
            return ControlFlow::Continue(());
        };
        *name = ObjectName(vec![sqlparser::ast::Ident::new(temp.clone())]);
        if alias.is_none() {
            *alias = Some(sqlparser::ast::TableAlias {
                name: sqlparser::ast::Ident::new(original),
                columns: Vec::new(),
            });
        }
        ControlFlow::Continue(())
    }
}

fn refs_table<T: VisitMut + Clone>(node: &T, name: &str) -> bool {
    refs_table_count(node, name) != 0
}

fn refs_table_count<T: VisitMut + Clone>(node: &T, name: &str) -> usize {
    let mut node = node.clone();
    let mut counter = TableRefCounter {
        name,
        shadowed: vec![false],
        saved_with: Vec::new(),
        count: 0,
    };
    let _ = VisitMut::visit(&mut node, &mut counter);
    counter.count
}

struct TableRefCounter<'a> {
    name: &'a str,
    shadowed: Vec<bool>,
    saved_with: Vec<Option<sqlparser::ast::With>>,
    count: usize,
}

impl VisitorMut for TableRefCounter<'_> {
    type Break = std::convert::Infallible;

    fn pre_visit_query(&mut self, query: &mut SqlQuery) -> ControlFlow<Self::Break> {
        let mut shadowed = self.shadowed.last().copied().unwrap_or(false);
        let mut saved_with = query.with.take();
        if let Some(with) = &mut saved_with {
            for cte in &mut with.cte_tables {
                if with.recursive && cte.alias.name.value.eq_ignore_ascii_case(self.name) {
                    shadowed = true;
                }
                let mut nested = Self {
                    name: self.name,
                    shadowed: vec![shadowed],
                    saved_with: Vec::new(),
                    count: 0,
                };
                let _ = VisitMut::visit(cte.query.as_mut(), &mut nested);
                self.count = self.count.saturating_add(nested.count);
                if !with.recursive && cte.alias.name.value.eq_ignore_ascii_case(self.name) {
                    shadowed = true;
                }
            }
        }
        self.saved_with.push(saved_with);
        self.shadowed.push(shadowed);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut SqlQuery) -> ControlFlow<Self::Break> {
        self.shadowed.pop();
        query.with = self.saved_with.pop().flatten();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if self.shadowed.last().copied().unwrap_or(false) {
            return ControlFlow::Continue(());
        }
        match table_factor {
            TableFactor::Table {
                name, args: None, ..
            } if name.0.len() == 1 && name.0[0].value.eq_ignore_ascii_case(self.name) => {
                self.count = self.count.saturating_add(1);
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

fn validate_recursive_table_reference(
    body: &SetExpr,
    name: &str,
    recursive_columns: &[String],
) -> Result<()> {
    let total = refs_table_count(body, name);
    if total != 1 {
        return Err(Error::Unsupported(format!(
            "recursive table '{name}' must be referenced exactly once"
        )));
    }
    if direct_table_ref_count(body, name) != 1 {
        return Err(Error::Unsupported(format!(
            "recursive table '{name}' must not be referenced in a subquery"
        )));
    }
    if recursive_ref_on_nullable_join_side(body, name)
        && !recursive_ref_is_null_rejected(body, name, recursive_columns)
    {
        return Err(Error::Unsupported(format!(
            "recursive table '{name}' must not appear on the nullable side of an outer join"
        )));
    }
    Ok(())
}

fn recursive_ref_on_nullable_join_side(body: &SetExpr, name: &str) -> bool {
    let SetExpr::Select(select) = body else {
        return false;
    };
    select
        .from
        .iter()
        .any(|table| table_ref_location(table, name).1)
}

/// MySQL permits a recursive reference on an outer join's nullable side when
/// the WHERE clause rejects every NULL-extended recursive row, making the join
/// equivalent to an inner join. Stay conservative: only recognise predicates
/// whose three-valued-logic behaviour is unambiguous.
fn recursive_ref_is_null_rejected(
    body: &SetExpr,
    name: &str,
    recursive_columns: &[String],
) -> bool {
    let SetExpr::Select(select) = body else {
        return false;
    };
    let Some(qualifier) = recursive_ref_qualifier(select, name) else {
        return false;
    };
    select
        .selection
        .as_ref()
        .is_some_and(|selection| null_rejects_qualifier(selection, &qualifier, recursive_columns))
}

fn recursive_ref_qualifier(select: &Select, name: &str) -> Option<String> {
    select.from.iter().find_map(|table| {
        recursive_factor_qualifier(&table.relation, name).or_else(|| {
            table
                .joins
                .iter()
                .find_map(|join| recursive_factor_qualifier(&join.relation, name))
        })
    })
}

fn recursive_factor_qualifier(factor: &TableFactor, name: &str) -> Option<String> {
    match factor {
        TableFactor::Table {
            name: relation,
            alias,
            args: None,
            ..
        } if relation.0.len() == 1 && relation.0[0].value.eq_ignore_ascii_case(name) => {
            Some(alias.as_ref().map_or_else(
                || relation.0[0].value.clone(),
                |alias| alias.name.value.clone(),
            ))
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let nested = recursive_ref_qualifier_from_table(table_with_joins, name)?;
            Some(
                alias
                    .as_ref()
                    .map_or(nested, |alias| alias.name.value.clone()),
            )
        }
        _ => None,
    }
}

fn recursive_ref_qualifier_from_table(table: &TableWithJoins, name: &str) -> Option<String> {
    recursive_factor_qualifier(&table.relation, name).or_else(|| {
        table
            .joins
            .iter()
            .find_map(|join| recursive_factor_qualifier(&join.relation, name))
    })
}

fn null_rejects_qualifier(expr: &Expr, qualifier: &str, recursive_columns: &[String]) -> bool {
    !null_truth_values(expr, qualifier, recursive_columns).contains(SqlTruth::TRUE)
}

#[derive(Clone, Copy)]
struct SqlTruth(u8);

impl SqlTruth {
    const TRUE: u8 = 1;
    const FALSE: u8 = 2;
    const UNKNOWN: u8 = 4;
    const ANY: Self = Self(Self::TRUE | Self::FALSE | Self::UNKNOWN);

    fn contains(self, value: u8) -> bool {
        self.0 & value != 0
    }

    fn not(self) -> Self {
        let mut values = self.0 & Self::UNKNOWN;
        if self.contains(Self::TRUE) {
            values |= Self::FALSE;
        }
        if self.contains(Self::FALSE) {
            values |= Self::TRUE;
        }
        Self(values)
    }

    fn and(self, other: Self) -> Self {
        let mut values = 0;
        if self.contains(Self::TRUE) && other.contains(Self::TRUE) {
            values |= Self::TRUE;
        }
        if self.contains(Self::FALSE) || other.contains(Self::FALSE) {
            values |= Self::FALSE;
        }
        if (self.contains(Self::UNKNOWN)
            && (other.contains(Self::TRUE) || other.contains(Self::UNKNOWN)))
            || (other.contains(Self::UNKNOWN)
                && (self.contains(Self::TRUE) || self.contains(Self::UNKNOWN)))
        {
            values |= Self::UNKNOWN;
        }
        Self(values)
    }

    fn or(self, other: Self) -> Self {
        let mut values = 0;
        if self.contains(Self::TRUE) || other.contains(Self::TRUE) {
            values |= Self::TRUE;
        }
        if self.contains(Self::FALSE) && other.contains(Self::FALSE) {
            values |= Self::FALSE;
        }
        if (self.contains(Self::UNKNOWN)
            && (other.contains(Self::FALSE) || other.contains(Self::UNKNOWN)))
            || (other.contains(Self::UNKNOWN)
                && (self.contains(Self::FALSE) || self.contains(Self::UNKNOWN)))
        {
            values |= Self::UNKNOWN;
        }
        Self(values)
    }

    fn predicate_test(self, true_for: u8) -> Self {
        let mut values = 0;
        if self.0 & true_for != 0 {
            values |= Self::TRUE;
        }
        if self.0 & (Self::TRUE | Self::FALSE | Self::UNKNOWN) & !true_for != 0 {
            values |= Self::FALSE;
        }
        Self(values)
    }
}

fn null_truth_values(expr: &Expr, qualifier: &str, recursive_columns: &[String]) -> SqlTruth {
    use sqlparser::ast::BinaryOperator::{And, Eq, Gt, GtEq, Lt, LtEq, NotEq, Or};
    use sqlparser::ast::UnaryOperator;

    match expr {
        Expr::Value(sqlparser::ast::Value::Boolean(true)) => SqlTruth(SqlTruth::TRUE),
        Expr::Value(sqlparser::ast::Value::Boolean(false)) => SqlTruth(SqlTruth::FALSE),
        Expr::Value(sqlparser::ast::Value::Null) => SqlTruth(SqlTruth::UNKNOWN),
        Expr::Nested(inner) => null_truth_values(inner, qualifier, recursive_columns),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => null_truth_values(expr, qualifier, recursive_columns).not(),
        Expr::BinaryOp {
            left,
            op: And,
            right,
        } => null_truth_values(left, qualifier, recursive_columns).and(null_truth_values(
            right,
            qualifier,
            recursive_columns,
        )),
        Expr::BinaryOp {
            left,
            op: Or,
            right,
        } => null_truth_values(left, qualifier, recursive_columns).or(null_truth_values(
            right,
            qualifier,
            recursive_columns,
        )),
        Expr::BinaryOp {
            left,
            op: Eq | NotEq | Lt | LtEq | Gt | GtEq,
            right,
        } if null_propagates_from_qualifier(left, qualifier, recursive_columns)
            || null_propagates_from_qualifier(right, qualifier, recursive_columns) =>
        {
            SqlTruth(SqlTruth::UNKNOWN)
        }
        Expr::IsNull(inner)
            if null_propagates_from_qualifier(inner, qualifier, recursive_columns) =>
        {
            SqlTruth(SqlTruth::TRUE)
        }
        Expr::IsNotNull(inner)
            if null_propagates_from_qualifier(inner, qualifier, recursive_columns) =>
        {
            SqlTruth(SqlTruth::FALSE)
        }
        Expr::IsTrue(inner) => {
            null_truth_values(inner, qualifier, recursive_columns).predicate_test(SqlTruth::TRUE)
        }
        Expr::IsFalse(inner) => {
            null_truth_values(inner, qualifier, recursive_columns).predicate_test(SqlTruth::FALSE)
        }
        Expr::IsNotTrue(inner) => null_truth_values(inner, qualifier, recursive_columns)
            .predicate_test(SqlTruth::FALSE | SqlTruth::UNKNOWN),
        Expr::IsNotFalse(inner) => null_truth_values(inner, qualifier, recursive_columns)
            .predicate_test(SqlTruth::TRUE | SqlTruth::UNKNOWN),
        Expr::IsUnknown(inner) => {
            null_truth_values(inner, qualifier, recursive_columns).predicate_test(SqlTruth::UNKNOWN)
        }
        Expr::IsNotUnknown(inner) => null_truth_values(inner, qualifier, recursive_columns)
            .predicate_test(SqlTruth::TRUE | SqlTruth::FALSE),
        Expr::Between {
            expr, low, high, ..
        } if null_propagates_from_qualifier(expr, qualifier, recursive_columns)
            || null_propagates_from_qualifier(low, qualifier, recursive_columns)
            || null_propagates_from_qualifier(high, qualifier, recursive_columns) =>
        {
            SqlTruth(SqlTruth::UNKNOWN)
        }
        Expr::InList { expr, .. }
            if null_propagates_from_qualifier(expr, qualifier, recursive_columns) =>
        {
            SqlTruth(SqlTruth::UNKNOWN)
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. }
        | Expr::RLike { expr, pattern, .. }
            if null_propagates_from_qualifier(expr, qualifier, recursive_columns)
                || null_propagates_from_qualifier(pattern, qualifier, recursive_columns) =>
        {
            SqlTruth(SqlTruth::UNKNOWN)
        }
        _ if null_propagates_from_qualifier(expr, qualifier, recursive_columns) => {
            SqlTruth(SqlTruth::UNKNOWN)
        }
        _ => SqlTruth::ANY,
    }
}

fn null_propagates_from_qualifier(
    expr: &Expr,
    qualifier: &str,
    recursive_columns: &[String],
) -> bool {
    use sqlparser::ast::BinaryOperator::{And, Or, Spaceship, Xor};

    match expr {
        Expr::Identifier(identifier) => recursive_columns
            .iter()
            .any(|column| predicate::identifier_eq(column, &identifier.value)),
        Expr::CompoundIdentifier(parts) => {
            parts.len() >= 2 && parts[parts.len() - 2].value == qualifier
        }
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::Cast { expr: inner, .. }
        | Expr::Ceil { expr: inner, .. }
        | Expr::Floor { expr: inner, .. } => {
            null_propagates_from_qualifier(inner, qualifier, recursive_columns)
        }
        Expr::BinaryOp { left, op, right } if !matches!(op, And | Or | Xor | Spaceship) => {
            null_propagates_from_qualifier(left, qualifier, recursive_columns)
                || null_propagates_from_qualifier(right, qualifier, recursive_columns)
        }
        Expr::Function(function) => {
            null_propagating_function(function, qualifier, recursive_columns)
        }
        _ => false,
    }
}

fn null_propagating_function(
    function: &sqlparser::ast::Function,
    qualifier: &str,
    recursive_columns: &[String],
) -> bool {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

    let name = function
        .name
        .0
        .last()
        .map(|part| part.value.to_ascii_lowercase());
    let Some(name) = name else {
        return false;
    };
    if !matches!(
        name.as_str(),
        "abs"
            | "ceil"
            | "ceiling"
            | "floor"
            | "sign"
            | "sqrt"
            | "exp"
            | "ln"
            | "log"
            | "log10"
            | "log2"
            | "upper"
            | "ucase"
            | "lower"
            | "lcase"
            | "length"
            | "octet_length"
            | "char_length"
            | "character_length"
            | "reverse"
            | "trim"
            | "ltrim"
            | "rtrim"
            | "bit_count"
            | "to_days"
            | "ascii"
            | "ord"
            | "bin"
            | "oct"
            | "crc32"
    ) {
        return false;
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return false;
    };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(argument))] = arguments.args.as_slice() else {
        return false;
    };
    null_propagates_from_qualifier(argument, qualifier, recursive_columns)
}

/// Returns `(contains_recursive_ref, ref_is_on_nullable_outer_join_side)`.
fn table_ref_location(table: &TableWithJoins, name: &str) -> (bool, bool) {
    let (mut left_has_ref, mut invalid) = factor_ref_location(&table.relation, name);
    for join in &table.joins {
        let (right_has_ref, right_invalid) = factor_ref_location(&join.relation, name);
        invalid |= right_invalid;
        match &join.join_operator {
            JoinOperator::LeftOuter(_) => invalid |= right_has_ref,
            JoinOperator::RightOuter(_) => invalid |= left_has_ref,
            JoinOperator::FullOuter(_) => invalid |= left_has_ref || right_has_ref,
            _ => {}
        }
        left_has_ref |= right_has_ref;
    }
    (left_has_ref, invalid)
}

fn factor_ref_location(factor: &TableFactor, name: &str) -> (bool, bool) {
    match factor {
        TableFactor::Table {
            name: relation,
            args: None,
            ..
        } if relation.0.len() == 1 && relation.0[0].value.eq_ignore_ascii_case(name) => {
            (true, false)
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_ref_location(table_with_joins, name),
        _ => (false, false),
    }
}

fn direct_table_ref_count(body: &SetExpr, name: &str) -> usize {
    let SetExpr::Select(select) = body else {
        return 0;
    };
    select
        .from
        .iter()
        .map(|table| {
            direct_factor_ref_count(&table.relation, name)
                + table
                    .joins
                    .iter()
                    .map(|join| direct_factor_ref_count(&join.relation, name))
                    .sum::<usize>()
        })
        .sum()
}

fn direct_factor_ref_count(factor: &TableFactor, name: &str) -> usize {
    match factor {
        TableFactor::Table {
            name: relation,
            args: None,
            ..
        } if relation.0.len() == 1 && relation.0[0].value.eq_ignore_ascii_case(name) => 1,
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            direct_factor_ref_count(&table_with_joins.relation, name)
                + table_with_joins
                    .joins
                    .iter()
                    .map(|join| direct_factor_ref_count(&join.relation, name))
                    .sum::<usize>()
        }
        _ => 0,
    }
}

fn alias_column_names(alias: &TableAlias) -> Vec<String> {
    alias
        .columns
        .iter()
        .map(|column| column.name.value.clone())
        .collect()
}

fn apply_col_aliases(mut schema: Schema, alias_cols: &[String]) -> Result<Schema> {
    if alias_cols.is_empty() {
        return Ok(schema);
    }
    if alias_cols.len() != schema.columns.len() {
        return Err(Error::Query(format!(
            "column alias count {} does not match query column count {}",
            alias_cols.len(),
            schema.columns.len()
        )));
    }
    for (column, alias) in schema.columns.iter_mut().zip(alias_cols) {
        column.name.clone_from(alias);
    }
    Ok(schema)
}

async fn create_temp_table(db: &Session, base: &str, schema: &Schema) -> Result<OwnedTempTable> {
    loop {
        let number = TEMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = temp_name(number, base);
        let definition = TableDef {
            name: name.clone(),
            schema: schema.clone(),
            pk_cols: Vec::new(),
            indexes: Vec::new(),
            col_meta: Vec::new(),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            storage_generation: 0,
        }
        .encode()?;
        let owner_number = TEMP_OWNER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let owner = format!(
            "{}:{}:{owner_number}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        )
        .into_bytes();
        let table_key = catalog_key(&name);
        let owner_key = temp_owner_key(&name);
        let puts = vec![
            (table_key.clone(), definition.clone()),
            (owner_key.clone(), owner.clone()),
        ];

        if db.in_txn() {
            if db.get(table_key).await?.is_some()
                || db.get(catalog::view_key(&name)).await?.is_some()
                || db.get(catalog::matview_key(&name)).await?.is_some()
                || db.get(owner_key).await?.is_some()
            {
                continue;
            }
            db.commit_write(puts, vec![]).await?;
        } else {
            let validation = elyra_storage::Validation {
                keys: vec![
                    (table_key, None),
                    (catalog::view_key(&name), None),
                    (catalog::matview_key(&name), None),
                    (owner_key, None),
                ],
                ranges: Vec::new(),
            };
            match db.raw_db().commit_validated(validation, puts, vec![]).await {
                Ok(()) => catalog::bump_epoch(),
                Err(Error::Conflict(_)) => continue,
                Err(error) => return Err(error),
            }
        }

        return Ok(OwnedTempTable {
            name,
            definition,
            owner,
        });
    }
}

async fn drop_temp_table(db: &Session, temp: &OwnedTempTable) -> Result<()> {
    let table_key = catalog_key(&temp.name);
    let owner_key = temp_owner_key(&temp.name);
    if db.get(table_key.clone()).await?.as_deref() != Some(temp.definition.as_slice())
        || db.get(owner_key.clone()).await?.as_deref() != Some(temp.owner.as_slice())
    {
        return Ok(());
    }

    let deletes = table_delete_keys(db, &temp.name).await?;
    if db.in_txn() {
        return db.commit_write(vec![], deletes).await;
    }

    let validation = elyra_storage::Validation {
        keys: vec![
            (table_key, Some(temp.definition.clone())),
            (owner_key, Some(temp.owner.clone())),
        ],
        ranges: Vec::new(),
    };
    match db
        .raw_db()
        .commit_validated(validation, vec![], deletes)
        .await
    {
        Ok(()) => {
            catalog::bump_epoch();
            Ok(())
        }
        // The relation changed after our read and is no longer ours to drop.
        Err(Error::Conflict(_)) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn commit_temp_write(
    db: &Session,
    temp: &OwnedTempTable,
    mut expected: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    puts: Vec<(Vec<u8>, Vec<u8>)>,
    deletes: Vec<Vec<u8>>,
) -> Result<()> {
    let table_key = catalog_key(&temp.name);
    let owner_key = temp_owner_key(&temp.name);
    if db.in_txn() {
        if db.get(table_key).await?.as_deref() != Some(temp.definition.as_slice())
            || db.get(owner_key).await?.as_deref() != Some(temp.owner.as_slice())
        {
            return Err(Error::Conflict(
                "temporary CTE relation ownership changed".into(),
            ));
        }
        return db.commit_write(puts, deletes).await;
    }

    expected.push((table_key, Some(temp.definition.clone())));
    expected.push((owner_key, Some(temp.owner.clone())));
    db.raw_db()
        .commit_validated(
            elyra_storage::Validation {
                keys: expected,
                ranges: Vec::new(),
            },
            puts,
            deletes,
        )
        .await
}

async fn fill_table(
    db: &Session,
    temp: &OwnedTempTable,
    schema: &Schema,
    rows: &[Vec<Value>],
) -> Result<()> {
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(rows.len() + 1);
    let rowid_storage_key = rowid_key(&temp.name);
    let expected_rowid = db.get(rowid_storage_key.clone()).await?;
    let mut rowid = match expected_rowid.as_deref() {
        Some(bytes) if bytes.len() == 8 => {
            u64::from_le_bytes(bytes.try_into().expect("checked length"))
        }
        _ => 0,
    };
    for r in rows {
        rowid += 1;
        let mut row = vec![Value::Null; schema.columns.len()];
        for (i, col) in schema.columns.iter().enumerate() {
            if let Some(v) = r.get(i) {
                row[i] = coerce(v.clone(), &col.ty, &col.name)?;
            }
        }
        let encoded = bincode::serialize(&row).map_err(|e| Error::Storage(e.to_string()))?;
        puts.push((data_key(&temp.name, &keyenc::encode_rowid(rowid)), encoded));
    }
    puts.push((rowid_storage_key.clone(), rowid.to_le_bytes().to_vec()));
    commit_temp_write(
        db,
        temp,
        vec![(rowid_storage_key, expected_rowid)],
        puts,
        vec![],
    )
    .await
}

async fn clear_table(db: &Session, temp: &OwnedTempTable) -> Result<()> {
    let prefix = data_prefix(&temp.name);
    let rowid_storage_key = rowid_key(&temp.name);
    // Read the row counter before the range. Every normal insert advances it,
    // so validating this value catches rows added during the subsequent scan.
    let expected_rowid = db.get(rowid_storage_key.clone()).await?;
    let mut deletes = vec![rowid_storage_key.clone()];
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
    commit_temp_write(
        db,
        temp,
        vec![(rowid_storage_key, expected_rowid)],
        vec![],
        deletes,
    )
    .await
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

/// Whether a projection subquery contains a bare identifier that may belong to
/// the joined outer schema. Qualified correlation is detected separately; this
/// covers logical USING/NATURAL keys while avoiding the per-row path for
/// obviously uncorrelated forms such as `(SELECT 1)`.
fn query_has_bare_identifier(query: &SqlQuery) -> bool {
    let found = std::cell::Cell::new(false);
    let _ = rewrite_query(query, &|candidate| {
        if let Expr::Identifier(identifier) = candidate {
            if !identifier.value.starts_with("@@") {
                found.set(true);
            }
        }
        None
    });
    found.get()
}

fn expr_has_potential_bare_correlation(expr: &Expr) -> bool {
    let found = std::cell::Cell::new(false);
    let _ = map_expr(expr, &|candidate| match candidate {
        Expr::Subquery(query)
        | Expr::InSubquery {
            subquery: query, ..
        }
        | Expr::Exists {
            subquery: query, ..
        } => {
            if query_has_bare_identifier(query) {
                found.set(true);
            }
            // The nested query was inspected recursively above.
            Some(candidate.clone())
        }
        _ => None,
    });
    found.get()
}

fn projection_has_potential_bare_correlation(projection: &[sqlparser::ast::SelectItem]) -> bool {
    use sqlparser::ast::SelectItem;

    projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_has_potential_bare_correlation(expr)
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
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
fn projection_correlated(projection: &[sqlparser::ast::SelectItem], outer: &[String]) -> bool {
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
    outer: &[String],
    corr_filter: &Expr,
    group_by: &[Expr],
    order_exprs: &[(Expr, bool)],
    offset: usize,
    limit: Option<usize>,
) -> Result<QueryResult> {
    let all = scan_rows(db, def, None).await?;
    let mut matched: Vec<Vec<Value>> = Vec::new();

    // A deliberately narrow semi/anti-join rewrite for the most common
    // correlated shape. Anything that cannot be proven equivalent keeps the
    // general per-row subquery path below.
    let decorrelated = prepare_correlated_exists(db, corr_filter, def, outer).await?;

    for row in all {
        let matches = if let Some(plan) = &decorrelated {
            plan.matches(&def.schema, &row)?
        } else {
            let bound = bind_outer(db, corr_filter, outer, &def.schema, &row);
            let resolved =
                resolve_subqueries_with_outer(db, vindex, bound, &def.schema, &row).await?;
            predicate::matches(&resolved, &def.schema, &row)?
        };
        if matches {
            matched.push(row);
        }
    }

    if !group_by.is_empty() || aggregate::projection_has_aggregate(&select.projection) {
        let (schema, out) = aggregate::run(
            &def.schema,
            &select.projection,
            group_by,
            matched,
            db.group_concat_max_len(),
        )?;
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
            |expr, row| Ok(bind_outer(db, expr, outer, &def.schema, row)),
        )
        .await?;
    }
    apply_offset_limit(&mut matched, offset, limit);

    // No SELECT-list subqueries: plain projection.
    if !projection_has_subquery(&select.projection) {
        let (schema, out) = project_exprs(
            &select.projection,
            &def.schema,
            &matched,
            single_relation_alias(select).as_deref(),
        )?;
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
            let bound = bind_outer(db, expr, outer, &def.schema, row);
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
                        qualifier: Vec::new(),
                        result_metadata: c.result_metadata,
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
                    qualifier: Vec::new(),
                    result_metadata: Default::default(),
                });
            }
        }
    }
    Ok(QueryResult::Rows(RowStream::literal(
        Schema::new(cols),
        out_rows,
    )))
}

struct CorrelatedExistsPlan {
    outer_column: usize,
    inner_keys: std::collections::HashSet<Vec<u8>>,
    collation: elyra_core::Collation,
    negated: bool,
    residual: Option<Expr>,
}

impl CorrelatedExistsPlan {
    fn matches(&self, schema: &Schema, outer_row: &[Value]) -> Result<bool> {
        let member = key_bytes_coll(&outer_row[self.outer_column], self.collation)
            .is_some_and(|key| self.inner_keys.contains(&key));
        if member == self.negated {
            return Ok(false);
        }
        self.residual.as_ref().map_or(Ok(true), |residual| {
            predicate::matches(residual, schema, outer_row)
        })
    }
}

/// Build a one-time semantic-key membership set for a safe correlated
/// `EXISTS`/`NOT EXISTS` semi-join. The accepted slice is intentionally strict:
/// one outer-table column equals one column of one plain inner table, with no
/// other inner clauses or query modifiers. Incompatible types/collations and
/// oversized inner inputs silently retain the nested-loop implementation.
async fn prepare_correlated_exists(
    db: &Session,
    filter: &Expr,
    outer_def: &TableDef,
    outer_qualifier: &[String],
) -> Result<Option<CorrelatedExistsPlan>> {
    let Some(shape) =
        prepare_correlated_exists_shape(db, filter, outer_def, outer_qualifier).await?
    else {
        return Ok(None);
    };
    let inner_rows = scan_rows(db, &shape.inner_def, None).await?;
    if inner_rows.len() > in_subquery_max() {
        return Ok(None);
    }
    let inner_keys = inner_rows
        .iter()
        .filter_map(|row| key_bytes_coll(&row[shape.inner_column], shape.collation))
        .collect();
    Ok(Some(CorrelatedExistsPlan {
        outer_column: shape.outer_column,
        inner_keys,
        collation: shape.collation,
        negated: shape.negated,
        residual: shape.residual,
    }))
}

struct CorrelatedExistsShape {
    outer_column: usize,
    inner_column: usize,
    inner_def: TableDef,
    collation: elyra_core::Collation,
    negated: bool,
    residual: Option<Expr>,
}

/// Whether `filter` has the exact correlated semi/anti-membership shape used
/// by execution. This performs catalog/type analysis only; it does not scan or
/// execute the inner query, so callers such as `EXPLAIN` can use it safely.
pub(crate) async fn correlated_exists_membership_eligible(
    db: &Session,
    filter: &Expr,
    outer_def: &TableDef,
    outer_qualifier: &[String],
) -> Result<bool> {
    Ok(
        prepare_correlated_exists_shape(db, filter, outer_def, outer_qualifier)
            .await?
            .is_some(),
    )
}

async fn prepare_correlated_exists_shape(
    db: &Session,
    filter: &Expr,
    outer_def: &TableDef,
    outer_qualifier: &[String],
) -> Result<Option<CorrelatedExistsShape>> {
    let mut conjuncts = Vec::new();
    split_and(filter, &mut conjuncts);
    let candidates: Vec<&Expr> = conjuncts
        .iter()
        .filter(|expr| {
            matches!(expr, Expr::Exists { subquery, .. } if query_refs_qualifier(subquery, outer_qualifier))
        })
        .collect();
    let [exists] = candidates.as_slice() else {
        return Ok(None);
    };
    let Expr::Exists { subquery, negated } = *exists else {
        return Ok(None);
    };

    if subquery.with.is_some()
        || subquery.order_by.is_some()
        || subquery.limit.is_some()
        || !subquery.limit_by.is_empty()
        || subquery.offset.is_some()
        || subquery.fetch.is_some()
        || !subquery.locks.is_empty()
        || subquery.for_clause.is_some()
        || subquery.settings.is_some()
        || subquery.format_clause.is_some()
    {
        return Ok(None);
    }
    let SetExpr::Select(inner_select) = subquery.body.as_ref() else {
        return Ok(None);
    };
    let sqlparser::ast::GroupByExpr::Expressions(group_by, modifiers) = &inner_select.group_by
    else {
        return Ok(None);
    };
    if inner_select.distinct.is_some()
        || inner_select.top.is_some()
        || inner_select.into.is_some()
        || !inner_select.lateral_views.is_empty()
        || inner_select.prewhere.is_some()
        || !group_by.is_empty()
        || !modifiers.is_empty()
        || inner_select.having.is_some()
        || !inner_select.cluster_by.is_empty()
        || !inner_select.distribute_by.is_empty()
        || !inner_select.sort_by.is_empty()
        || !inner_select.named_window.is_empty()
        || inner_select.qualify.is_some()
        || inner_select.value_table_mode.is_some()
        || inner_select.connect_by.is_some()
        || inner_select.from.len() != 1
        || !inner_select.from[0].joins.is_empty()
        || projection_correlated(&inner_select.projection, outer_qualifier)
        || aggregate::projection_has_aggregate(&inner_select.projection)
        || projection_has_window(&inner_select.projection)
    {
        return Ok(None);
    }
    let TableFactor::Table {
        name: inner_name,
        alias: inner_alias,
        ..
    } = &inner_select.from[0].relation
    else {
        return Ok(None);
    };
    let Some(correlation) = inner_select.selection.as_ref() else {
        return Ok(None);
    };
    let Expr::BinaryOp {
        left,
        op: sqlparser::ast::BinaryOperator::Eq,
        right,
    } = correlation
    else {
        return Ok(None);
    };

    let inner_table = stored_table_ident(db, inner_name)?;
    let inner_def = catalog::load(db, &inner_table).await?;
    let inner_qualifier = factor_qualifier_object(db, &inner_select.from[0].relation)
        .map(|qualifier| object_name_parts(&qualifier))
        .unwrap_or_else(|| {
            inner_alias
                .as_ref()
                .map(|alias| vec![alias.name.value.clone()])
                .unwrap_or_else(|| vec![inner_table])
        });
    let pair = correlation_column_pair(
        left,
        right,
        outer_qualifier,
        &outer_def.schema,
        &inner_qualifier,
        &inner_def.schema,
    )
    .or_else(|| {
        correlation_column_pair(
            right,
            left,
            outer_qualifier,
            &outer_def.schema,
            &inner_qualifier,
            &inner_def.schema,
        )
    });
    let Some((outer_column, inner_column)) = pair else {
        return Ok(None);
    };
    let outer_column_def = &outer_def.schema.columns[outer_column];
    let inner_column_def = &inner_def.schema.columns[inner_column];
    if outer_column_def.ty != inner_column_def.ty
        || outer_column_def.collation != inner_column_def.collation
        // SQL NaN equality is false while the grouping/hash key deliberately
        // canonicalises NaNs. Vector equality likewise has no scalar-key
        // contract. Keep both on the interpreter path.
        || matches!(outer_column_def.ty, ColumnType::Float | ColumnType::Vector(_))
    {
        return Ok(None);
    }

    let collation = outer_column_def.collation;

    // The membership predicate is evaluated directly. Normalise the remaining
    // outer-only conjuncts once so the hot row loop neither clones/maps the AST
    // nor resolves a subquery. Any residual construct we cannot prove local to
    // the outer schema keeps the general interpreter path.
    let residual_conjuncts = conjuncts
        .iter()
        .filter(|conjunct| conjunct != exists)
        .map(|conjunct| normalise_outer_references(conjunct, outer_qualifier))
        .collect::<Vec<_>>();
    if residual_conjuncts
        .iter()
        .any(|conjunct| expr_has_subquery(conjunct) || !refs_in_schema(conjunct, &outer_def.schema))
    {
        return Ok(None);
    }
    let residual = residual_conjuncts
        .into_iter()
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: sqlparser::ast::BinaryOperator::And,
            right: Box::new(right),
        });
    Ok(Some(CorrelatedExistsShape {
        outer_column,
        inner_column,
        inner_def,
        collation,
        negated: *negated,
        residual,
    }))
}

fn normalise_outer_references(expr: &Expr, outer_qualifier: &[String]) -> Expr {
    map_expr(expr, &|candidate| match candidate {
        Expr::CompoundIdentifier(parts)
            if parts.len() >= 2
                && qualifier_parts_match(outer_qualifier, &parts[..parts.len() - 1]) =>
        {
            parts.last().cloned().map(Expr::Identifier)
        }
        _ => None,
    })
}

fn correlation_column_pair(
    outer_expr: &Expr,
    inner_expr: &Expr,
    outer_qualifier: &[String],
    outer_schema: &Schema,
    inner_qualifier: &[String],
    inner_schema: &Schema,
) -> Option<(usize, usize)> {
    let outer_column = qualified_column_index(outer_expr, outer_qualifier, outer_schema, false)?;
    let inner_column = qualified_column_index(inner_expr, inner_qualifier, inner_schema, true)?;
    Some((outer_column, inner_column))
}

fn qualified_column_index(
    expr: &Expr,
    qualifier: &[String],
    schema: &Schema,
    allow_bare: bool,
) -> Option<usize> {
    let column = match expr {
        Expr::Nested(inner) => return qualified_column_index(inner, qualifier, schema, allow_bare),
        Expr::Identifier(identifier) if allow_bare => &identifier.value,
        Expr::CompoundIdentifier(parts)
            if parts.len() >= 2 && qualifier_parts_match(qualifier, &parts[..parts.len() - 1]) =>
        {
            &parts.last()?.value
        }
        _ => return None,
    };
    schema
        .columns
        .iter()
        .position(|candidate| predicate::identifier_eq(&candidate.name, column))
}

/// Rewrite qualified outer column references (`outer.col`) in `expr` to
/// literals from `row`, including inside subqueries. Bare names remain bound
/// to the innermost query scope.
fn bind_outer(db: &Session, expr: &Expr, outer: &[String], schema: &Schema, row: &[Value]) -> Expr {
    bind_outer_references(db, expr, &[], &|e| match e {
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = &parts[..parts.len() - 1];
            let col = &parts[parts.len() - 1].value;
            if qualifier_parts_match(outer, qualifier) {
                schema
                    .columns
                    .iter()
                    .position(|c| predicate::identifier_eq(&c.name, col))
                    .map(|i| value_to_expr(&row[i]))
            } else {
                None
            }
        }
        _ => None,
    })
}

/// Bind qualified references while respecting relation names introduced by
/// nested query scopes. A local relation shadows an outer relation with the
/// same qualifier.
fn bind_outer_references(
    db: &Session,
    expr: &Expr,
    shadowed: &[Vec<String>],
    bind: &dyn Fn(&Expr) -> Option<Expr>,
) -> Expr {
    map_expr(expr, &|candidate| match candidate {
        Expr::Subquery(query) => Some(Expr::Subquery(Box::new(bind_outer_query(
            db, query, shadowed, bind,
        )))),
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Some(Expr::InSubquery {
            expr: Box::new(bind_outer_references(db, expr, shadowed, bind)),
            subquery: Box::new(bind_outer_query(db, subquery, shadowed, bind)),
            negated: *negated,
        }),
        Expr::Exists { subquery, negated } => Some(Expr::Exists {
            subquery: Box::new(bind_outer_query(db, subquery, shadowed, bind)),
            negated: *negated,
        }),
        Expr::CompoundIdentifier(parts)
            if parts.len() >= 2
                && shadowed.iter().any(|qualifier| {
                    qualifier_parts_match(qualifier, &parts[..parts.len() - 1])
                }) =>
        {
            Some(candidate.clone())
        }
        _ => bind(candidate),
    })
}

fn bind_outer_query(
    db: &Session,
    query: &SqlQuery,
    inherited_shadowing: &[Vec<String>],
    bind: &dyn Fn(&Expr) -> Option<Expr>,
) -> SqlQuery {
    fn bind_set_expr(
        db: &Session,
        set_expr: &mut SetExpr,
        inherited_shadowing: &[Vec<String>],
        bind: &dyn Fn(&Expr) -> Option<Expr>,
    ) {
        match set_expr {
            SetExpr::Select(select) => {
                let mut shadowed = inherited_shadowing.to_vec();
                shadowed.extend(join_qualifiers(db, &select.from));
                rewrite_select_expressions(select, &|expr| {
                    Some(bind_outer_references(db, expr, &shadowed, bind))
                });
            }
            SetExpr::SetOperation { left, right, .. } => {
                bind_set_expr(db, left, inherited_shadowing, bind);
                bind_set_expr(db, right, inherited_shadowing, bind);
            }
            SetExpr::Query(query) => {
                **query = bind_outer_query(db, query, inherited_shadowing, bind);
            }
            SetExpr::Values(_) | SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Table(_) => {}
        }
    }

    let mut query = query.clone();
    bind_set_expr(db, &mut query.body, inherited_shadowing, bind);
    let order_shadowing = match query.body.as_ref() {
        SetExpr::Select(select) => {
            let mut shadowed = inherited_shadowing.to_vec();
            shadowed.extend(join_qualifiers(db, &select.from));
            shadowed
        }
        _ => inherited_shadowing.to_vec(),
    };
    if let Some(order_by) = &mut query.order_by {
        for order in &mut order_by.exprs {
            order.expr = bind_outer_references(db, &order.expr, &order_shadowing, bind);
        }
    }
    query
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
                let Some(column) = bare_unknown_column(&error, &bound).map(str::to_owned) else {
                    return Err(error);
                };
                if rebound
                    .iter()
                    .any(|name: &String| predicate::identifier_eq(name, &column))
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
                        if predicate::identifier_eq(&identifier.value, &column) =>
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

fn bare_unknown_column<'a>(error: &'a Error, expr: &Expr) -> Option<&'a str> {
    let Error::Catalog(message) = error else {
        return None;
    };
    let column = message.strip_prefix("unknown column: ")?;
    let is_bare = std::cell::Cell::new(false);
    let _ = map_expr(expr, &|candidate| {
        if let Expr::Identifier(identifier) = candidate {
            if predicate::identifier_eq(&identifier.value, column) {
                is_bare.set(true);
            }
        }
        None
    });
    is_bare.get().then_some(column)
}

async fn sort_rows_with_subqueries(
    db: &Session,
    vindex: &VectorRegistry,
    rows: &mut [Vec<Value>],
    schema: &Schema,
    order: &[(Expr, bool)],
    bind: impl Fn(&Expr, &[Value]) -> Result<Expr>,
) -> Result<()> {
    let mut check = db.cancel_check();
    let mut keyed = Vec::with_capacity(rows.len());
    for (position, row) in rows.iter().enumerate() {
        check.tick()?;
        let mut keys = Vec::with_capacity(order.len());
        for (expr, _) in order {
            let bound = bind(expr, row)?;
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
    if let Some(result) = run_derived_query_chain(db, vindex, q).await? {
        return Ok(result);
    }
    materialize_query_result(Box::pin(select(db, vindex, q)).await?).await
}

struct DerivedQueryLayer<'a> {
    query: &'a SqlQuery,
    select: &'a Select,
    subquery: &'a SqlQuery,
    input_alias: &'a TableAlias,
    group_by: &'a [Expr],
}

/// Return a derived-table layer that can be evaluated over already-materialised
/// rows without changing the normal SELECT semantics. Unsupported or recursive
/// shapes fall back to the regular executor.
fn derived_query_layer(query: &SqlQuery) -> Option<DerivedQueryLayer<'_>> {
    if query.with.is_some()
        || !query.limit_by.is_empty()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
    {
        return None;
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let sqlparser::ast::GroupByExpr::Expressions(group_by, modifiers) = &select.group_by else {
        return None;
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !modifiers.is_empty()
        || projection_has_subquery(&select.projection)
        || projection_has_window(&select.projection)
        || select.selection.as_ref().is_some_and(expr_has_subquery)
        || select.having.as_ref().is_some_and(expr_has_subquery)
        || group_by.iter().any(expr_has_subquery)
        || query.order_by.as_ref().is_some_and(|order_by| {
            order_by
                .exprs
                .iter()
                .any(|order| expr_has_subquery(&order.expr))
        })
        || select.from.len() != 1
    {
        return None;
    }

    let relation = &select.from[0];
    if !relation.joins.is_empty() {
        return None;
    }
    let TableFactor::Derived {
        lateral: false,
        subquery,
        alias: Some(alias),
    } = &relation.relation
    else {
        return None;
    };

    Some(DerivedQueryLayer {
        query,
        select,
        subquery,
        input_alias: alias,
        group_by,
    })
}

/// Evaluate a linear chain of simple derived tables from the inside out. Doing
/// this iteratively avoids recursively polling the full SELECT/join executor for
/// every inlined CTE dependency, which can exhaust a worker's stack even for a
/// routine chain.
async fn run_derived_query_chain(
    db: &Session,
    vindex: &VectorRegistry,
    query: &SqlQuery,
) -> Result<Option<(Schema, Vec<Vec<Value>>)>> {
    let mut current = query;
    let mut layers = Vec::new();
    while let Some(layer) = derived_query_layer(current) {
        current = layer.subquery;
        layers.push(layer);
    }
    if layers.is_empty() {
        return Ok(None);
    }

    let (mut schema, mut rows) =
        materialize_query_result(Box::pin(select(db, vindex, current)).await?).await?;
    for layer in layers.into_iter().rev() {
        let input_qualifier = canonical_relation_qualifier(db, None, &layer.input_alias.name);
        let aliased_schema =
            apply_col_aliases(schema.clone(), &alias_column_names(layer.input_alias))?;
        let input_schema = Schema::new(qualify_columns(&aliased_schema, &input_qualifier));
        let order_exprs: Vec<(Expr, bool)> = match &layer.query.order_by {
            Some(order_by) => order_by
                .exprs
                .iter()
                .map(|order| (order.expr.clone(), order.asc.unwrap_or(true)))
                .collect(),
            None => Vec::new(),
        };
        let offset = match &layer.query.offset {
            Some(offset) => eval_usize(&offset.value)?,
            None => 0,
        };
        let limit = match &layer.query.limit {
            Some(limit) => Some(eval_usize(limit)?),
            None => None,
        };
        let cancel = db.cancel_token();
        let group_concat_max_len = db.group_concat_max_len();
        let result = cpu_bound(move || {
            finish_materialized_select(
                layer.select,
                layer.select.selection.as_ref(),
                input_schema,
                rows,
                layer.group_by,
                &order_exprs,
                offset,
                limit,
                &cancel,
                group_concat_max_len,
            )
        })?;
        (schema, rows) = materialize_query_result(result).await?;
    }
    Ok(Some((schema, rows)))
}

async fn materialize_query_result(result: QueryResult) -> Result<(Schema, Vec<Vec<Value>>)> {
    match result {
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
fn rewrite_select_expressions(select: &mut Select, f: &dyn Fn(&Expr) -> Option<Expr>) {
    for item in &mut select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e)
            | sqlparser::ast::SelectItem::ExprWithAlias { expr: e, .. } => {
                *e = map_expr(e, f);
            }
            _ => {}
        }
    }
    if let Some(selection) = &select.selection {
        select.selection = Some(map_expr(selection, f));
    }
    if let Some(having) = &select.having {
        select.having = Some(map_expr(having, f));
    }
    if let sqlparser::ast::GroupByExpr::Expressions(expressions, _) = &mut select.group_by {
        for expression in expressions {
            *expression = map_expr(expression, f);
        }
    }
    for table in &mut select.from {
        rewrite_table_factor(&mut table.relation, f);
        for join in &mut table.joins {
            rewrite_table_factor(&mut join.relation, f);
            if let sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(e))
            | sqlparser::ast::JoinOperator::LeftOuter(sqlparser::ast::JoinConstraint::On(e))
            | sqlparser::ast::JoinOperator::RightOuter(sqlparser::ast::JoinConstraint::On(e))
            | sqlparser::ast::JoinOperator::FullOuter(sqlparser::ast::JoinConstraint::On(e)) =
                &mut join.join_operator
            {
                *e = map_expr(e, f);
            }
        }
    }
}

fn rewrite_table_factor(table: &mut TableFactor, f: &dyn Fn(&Expr) -> Option<Expr>) {
    match table {
        TableFactor::Derived { subquery, .. } => {
            **subquery = rewrite_query(subquery, f);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            rewrite_table_factor(&mut table_with_joins.relation, f);
            for join in &mut table_with_joins.joins {
                rewrite_table_factor(&mut join.relation, f);
            }
        }
        _ => {}
    }
}

fn rewrite_set_expr(set_expr: &mut SetExpr, f: &dyn Fn(&Expr) -> Option<Expr>) {
    match set_expr {
        SetExpr::Select(select) => rewrite_select_expressions(select, f),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left, f);
            rewrite_set_expr(right, f);
        }
        SetExpr::Query(query) => **query = rewrite_query(query, f),
        SetExpr::Values(_) | SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Table(_) => {}
    }
}

fn rewrite_query(q: &SqlQuery, f: &dyn Fn(&Expr) -> Option<Expr>) -> SqlQuery {
    let mut q = q.clone();
    rewrite_set_expr(&mut q.body, f);
    if let Some(order_by) = &mut q.order_by {
        for order in &mut order_by.exprs {
            order.expr = map_expr(&order.expr, f);
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
            for dk in index::fulltext_lookup(db, &def.storage_name(), &idx.name, term).await? {
                *ft_score.entry(dk).or_default() += 1;
            }
        }
    } else {
        // No full-text index: scan and score by distinct query-term presence.
        let prefix = def.data_prefix();
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
        qualifier: Vec::new(),
        result_metadata: Default::default(),
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
        .position(|c| predicate::identifier_eq(&c.name, name))
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
        let prefix = def.data_prefix();
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
                agg.seed_count_star(n);
                return Ok(agg);
            }
            if let Some(n) = index_count_composite_range(db, def, f).await? {
                let mut agg = plan.new_aggregator();
                agg.seed_count_star(n);
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
    let prefix = def.data_prefix();
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
    let prefix = def.data_prefix();
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
    let prefix = def.data_prefix();
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
    let prefix = def.data_prefix();
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
    let prefix = def.data_prefix();
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
            .find(|c| predicate::identifier_eq(&c.name, name))
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
    let prefix = def.data_prefix();
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
            .position(|c| predicate::identifier_eq(&c.name, &name))
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
                &def.storage_name(),
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
                let keys = index::lookup_eq(db, &def.storage_name(), idx, &vals).await?;
                return Ok(Some(keys.len() as u64));
            }
        }
    }
    Ok(None)
}

/// Count a clean equality-prefix/composite-range predicate directly in the
/// secondary-index keyspace. Any residual conjunct declines this covering path
/// so the ordinary executor can fetch rows and recheck it.
async fn index_count_composite_range(
    db: &Session,
    def: &TableDef,
    filter: &Expr,
) -> Result<Option<u64>> {
    let Some(query) = composite_range_bounds(def, Some(filter))? else {
        return Ok(None);
    };
    let range_column = query.index.cols[query.prefix.len()];
    let prefix_columns = &query.index.cols[..query.prefix.len()];
    let mut conjuncts = Vec::new();
    split_and(filter, &mut conjuncts);
    let mut seen_prefix = std::collections::HashSet::new();
    for conjunct in &conjuncts {
        if let Some((column, _)) = eq_col_literal(def, Some(conjunct))? {
            if prefix_columns.contains(&column) && seen_prefix.insert(column) {
                continue;
            }
            return Ok(None);
        }
        if as_range(def, conjunct)?.is_some_and(|(column, _, _)| column == range_column)
            || as_between(def, conjunct)?.is_some_and(|(column, _, _)| column == range_column)
        {
            continue;
        }
        return Ok(None);
    }

    let lo = query
        .lo
        .as_ref()
        .map(|(value, inclusive)| (value, *inclusive));
    let hi = query
        .hi
        .as_ref()
        .map(|(value, inclusive)| (value, *inclusive));
    if db.in_txn() {
        return Ok(Some(
            index::lookup_prefix_range(db, &def.storage_name(), query.index, &query.prefix, lo, hi)
                .await?
                .len() as u64,
        ));
    }
    let Some((start, end)) =
        index::prefix_range_scan_bounds(&def.storage_name(), query.index, &query.prefix, lo, hi)?
    else {
        return Ok(Some(0));
    };
    let count = db
        .raw_db()
        .scan_range_fold(start, end, 0u64, |count, _, _| {
            *count += 1;
            Ok(())
        })
        .await?;
    Ok(Some(count))
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
    let find = |parts: &[Ident], out: &mut Vec<usize>| -> bool {
        match predicate::resolve_index_parts(parts, schema) {
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
        Expr::Identifier(id) => find(std::slice::from_ref(id), out),
        Expr::CompoundIdentifier(parts) => find(parts, out),
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
    let prefix = def.data_prefix();
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
    let index = match e {
        Expr::Identifier(id) => predicate::resolve_index(&id.value, schema).ok(),
        Expr::CompoundIdentifier(parts) => predicate::resolve_index_parts(parts, schema).ok(),
        Expr::Nested(inner) => return expr_collation(inner, schema),
        _ => None,
    };
    index
        .map(|index| schema.columns[index].collation)
        .unwrap_or(elyra_core::Collation::Ci)
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
        let idx = schema
            .columns
            .iter()
            .position(|c| predicate::identifier_eq(&c.name, &name))
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

    let prefix = def.data_prefix();
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

    let deletes = table_delete_keys(db, name).await?;
    db.commit_write(vec![], deletes).await?;
    Ok(QueryResult::Affected(0))
}

/// Collect every key owned by a table. Keeping this shared with temporary CTE
/// cleanup ensures ordinary DROP also clears any internal ownership marker.
async fn table_delete_keys(db: &Session, name: &str) -> Result<Vec<Vec<u8>>> {
    let definition = catalog::load(db, name).await?;
    let storage_name = definition.storage_name();
    // Collect the table's data and index keys in batches.
    // The generation watermark deliberately survives DROP. Deferred cleanup
    // for this logical name may still be running, and a recreated table must
    // use a keyspace newer than anything cleanup can target.
    let mut deletes = vec![
        catalog_key(name),
        rowid_key(name),
        autoinc_key(name),
        temp_owner_key(name),
        catalog::colwidth_key(name),
        catalog::coldecl_key(name),
    ];
    for prefix in [
        definition.data_prefix(),
        index_table_prefix(&storage_name),
        indexnull_table_prefix(&storage_name),
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
    Ok(deletes)
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
        (ColumnType::Float, Value::Decimal(units, scale)) => {
            Value::Float(units as f64 / 10f64.powi(scale as i32))
        }
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
            return Err(Error::OutOfRange(format!("invalid UNSIGNED value: {i}")))
        }
        (ColumnType::UInt, Value::Bool(b)) => Value::UInt(b as u64),
        (ColumnType::UInt, Value::Float(f))
            if f.is_finite() && (0.0..18_446_744_073_709_551_616.0).contains(&f) =>
        {
            Value::UInt(f.round() as u64)
        }
        (ColumnType::UInt, Value::Float(_)) if !strict => Value::UInt(0),
        (ColumnType::UInt, Value::Float(f)) => {
            return Err(Error::OutOfRange(format!("invalid UNSIGNED value: {f}")))
        }
        (ColumnType::UInt, Value::Decimal(units, scale)) => {
            let rounded = round_decimal_to_integer(units, scale);
            match u64::try_from(rounded) {
                Ok(value) => Value::UInt(value),
                Err(_) if !strict => Value::UInt(if rounded.is_negative() { 0 } else { u64::MAX }),
                Err(_) => {
                    return Err(Error::Type(format!(
                        "invalid UNSIGNED value: {}",
                        Value::Decimal(units, scale)
                            .to_wire_string()
                            .unwrap_or_default()
                    )))
                }
            }
        }
        (ColumnType::UInt, Value::Text(s)) => {
            let text = s.trim();
            if let Ok(value) = text.parse::<u64>() {
                Value::UInt(value)
            } else if let Ok(value) = text.parse::<f64>() {
                if value.is_finite() && (0.0..18_446_744_073_709_551_616.0).contains(&value) {
                    Value::UInt(value.round() as u64)
                } else if !strict {
                    Value::UInt(if value.is_sign_negative() {
                        0
                    } else {
                        u64::MAX
                    })
                } else {
                    return Err(Error::OutOfRange(format!("invalid UNSIGNED value: {s}")));
                }
            } else if !strict {
                Value::UInt(mysql_integer_prefix(&s).max(0) as u64)
            } else {
                return Err(Error::OutOfRange(format!("invalid UNSIGNED value: {s}")));
            }
        }
        (ColumnType::Int, Value::UInt(u)) => Value::Int(u as i64),
        (ColumnType::Float, Value::UInt(u)) => Value::Float(u as f64),
        // Lenient (MySQL-style) conversions.
        (ColumnType::Int, Value::Float(f)) => Value::Int(f.round() as i64),
        (ColumnType::Int, Value::Decimal(units, scale)) => {
            let rounded = round_decimal_to_integer(units, scale);
            Value::Int(rounded.clamp(i64::MIN as i128, i64::MAX as i128) as i64)
        }
        (ColumnType::Int, Value::Text(s)) => {
            match s
                .trim()
                .parse::<i64>()
                .or_else(|_| s.trim().parse::<f64>().map(|f| f.round() as i64))
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

fn round_decimal_to_integer(units: i128, scale: u8) -> i128 {
    if scale == 0 {
        return units;
    }
    let Some(divisor) = 10_i128.checked_pow(scale.into()) else {
        // An i128 numerator divided by 10^39 or greater has magnitude below
        // one half, so it always rounds to zero.
        return 0;
    };
    let quotient = units / divisor;
    let remainder = units % divisor;
    if remainder.abs() >= divisor / 2 {
        quotient + units.signum()
    } else {
        quotient
    }
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
        .map(|number| number.round() as i64)
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
mod generation_cleanup_tests {
    use super::{generation_gc_key, generation_gc_value, resume_generation_cleanup};
    use crate::catalog;
    use elyra_storage::Db;

    #[tokio::test]
    async fn startup_cleanup_reclaims_every_stale_generation_keyspace() {
        let db = Db::in_memory().unwrap();
        let table = "table::with::separators";
        let generation = 7;
        let stale_keys = [
            catalog::data_prefix_generation(table, generation),
            catalog::index_table_prefix_generation(table, generation),
            catalog::indexnull_table_prefix_generation(table, generation),
        ]
        .map(|mut prefix| {
            prefix.extend_from_slice(b"entry");
            prefix
        });
        let marker = generation_gc_key(table, generation);
        let mut puts = stale_keys
            .iter()
            .cloned()
            .map(|key| (key, b"value".to_vec()))
            .collect::<Vec<_>>();
        puts.push((
            marker.clone(),
            generation_gc_value(table, generation).unwrap(),
        ));
        db.commit(puts, Vec::new()).await.unwrap();

        resume_generation_cleanup(&db).await.unwrap();

        for key in stale_keys {
            assert_eq!(db.get(key).await.unwrap(), None);
        }
        assert_eq!(db.get(marker).await.unwrap(), None);
    }
}

#[cfg(test)]
mod cte_rewrite_tests {
    use crate::{Engine, QueryResult, Session};
    use elyra_core::{ColumnDef, ColumnType, Error, Privilege, Schema, Value};
    use elyra_storage::Db;

    const EXPANSION_LIMIT_CHILD: &str = "ELYRASQL_CTE_EXPANSION_LIMIT_CHILD";

    fn engine_and_session() -> (Engine, Session) {
        let engine = Engine::new(Db::in_memory().unwrap());
        let session = engine.session();
        (engine, session)
    }

    async fn schema_and_rows(
        engine: &Engine,
        session: &Session,
        sql: &str,
    ) -> (elyra_core::Schema, Vec<Vec<Value>>) {
        let mut outcomes = engine
            .execute(sql, Privilege::Admin, session)
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1, "expected one outcome for `{sql}`");
        let QueryResult::Rows(mut stream) = outcomes.remove(0) else {
            panic!("expected rows for `{sql}`");
        };
        let schema = stream.schema.clone();
        let mut result = Vec::new();
        loop {
            let batch = stream.next_batch(128).await.unwrap();
            if batch.is_empty() {
                return (schema, result);
            }
            result.extend(batch);
        }
    }

    async fn rows(engine: &Engine, session: &Session, sql: &str) -> Vec<Vec<Value>> {
        schema_and_rows(engine, session, sql).await.1
    }

    #[tokio::test]
    async fn cte_column_aliases_survive_nested_scalar_derived_and_set_inlining() {
        let (engine, session) = engine_and_session();

        for sql in [
            "WITH c(renamed) AS (SELECT 7 AS original), \
                  nested AS (SELECT renamed FROM c) \
             SELECT renamed FROM nested",
            "WITH c(renamed) AS (SELECT 7 AS original) \
             SELECT (SELECT renamed FROM c)",
            "WITH c(renamed) AS (SELECT 7 AS original) \
             SELECT derived.renamed FROM (SELECT renamed FROM c) AS derived",
            "WITH c(renamed) AS (SELECT 7 AS original) \
             SELECT renamed FROM c UNION ALL SELECT 8 ORDER BY renamed",
        ] {
            let expected = if sql.contains("UNION ALL") {
                vec![vec![Value::Int(7)], vec![Value::Int(8)]]
            } else {
                vec![vec![Value::Int(7)]]
            };
            assert_eq!(rows(&engine, &session, sql).await, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn cte_column_aliases_drive_join_resolution_and_result_metadata() {
        let (engine, session) = engine_and_session();
        engine
            .execute(
                "CREATE TABLE cte_alias_right (join_key INT, right_payload INT)",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO cte_alias_right VALUES (1, 11)",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();

        let (schema, result) = schema_and_rows(
            &engine,
            &session,
            "WITH c(join_key, left_payload) AS (SELECT 1 AS old_key, 7 AS old_payload) \
             SELECT c.join_key, c.left_payload, r.right_payload \
             FROM c JOIN cte_alias_right AS r USING (join_key)",
        )
        .await;
        assert_eq!(
            schema
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["join_key", "left_payload", "right_payload"]
        );
        assert_eq!(schema.tables, ["c", "c", "r"]);
        assert_eq!(
            result,
            vec![vec![Value::Int(1), Value::Int(7), Value::Int(11)]]
        );
    }

    #[tokio::test]
    async fn nonrecursive_cte_column_alias_width_must_match_the_query() {
        let (engine, session) = engine_and_session();

        for sql in [
            "WITH c(only_one) AS (SELECT 1, 2) SELECT * FROM c",
            "WITH c(one, too_many) AS (SELECT 1) SELECT * FROM c",
        ] {
            let Err(error) = engine.execute(sql, Privilege::Admin, &session).await else {
                panic!("mismatched CTE column aliases unexpectedly succeeded")
            };
            assert!(error.to_string().contains("column alias count"), "{error}");
        }
    }

    #[tokio::test]
    async fn recursive_cte_column_alias_width_must_match_the_query() {
        let (engine, session) = engine_and_session();

        for sql in [
            "WITH RECURSIVE seq(only_one) AS (\
                 SELECT 1, 2 UNION ALL SELECT only_one + 1, 2 FROM seq WHERE only_one < 2\
             ) SELECT * FROM seq",
            "WITH RECURSIVE seq(one, too_many) AS (\
                 SELECT 1 UNION ALL SELECT one + 1 FROM seq WHERE one < 2\
             ) SELECT * FROM seq",
        ] {
            let Err(error) = engine.execute(sql, Privilege::Admin, &session).await else {
                panic!("mismatched recursive CTE aliases unexpectedly succeeded")
            };
            assert!(error.to_string().contains("column alias count"), "{error}");
        }
    }

    #[tokio::test]
    async fn duplicate_cte_names_are_rejected_only_within_the_same_scope() {
        let (engine, session) = engine_and_session();

        for sql in [
            "WITH c AS (SELECT 1), C AS (SELECT 2) SELECT * FROM c",
            "WITH RECURSIVE c(n) AS (SELECT 1), C(n) AS (SELECT 2) SELECT * FROM c",
        ] {
            let Err(error) = engine.execute(sql, Privilege::Admin, &session).await else {
                panic!("case-insensitive duplicate CTE unexpectedly succeeded")
            };
            assert!(error.to_string().contains("duplicate CTE name"), "{error}");
        }

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH c(n) AS (SELECT 1) \
                 SELECT nested.n FROM (WITH C(n) AS (SELECT 2) SELECT n FROM C) AS nested",
            )
            .await,
            vec![vec![Value::Int(2)]]
        );
    }

    #[tokio::test]
    async fn nonrecursive_ctes_expand_in_derived_and_scalar_subqueries() {
        let (engine, session) = engine_and_session();

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH c AS (SELECT 7 AS n) \
                 SELECT derived.n FROM (SELECT n FROM c) AS derived",
            )
            .await,
            vec![vec![Value::Int(7)]]
        );
        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH c AS (SELECT 8 AS n) SELECT (SELECT n FROM c) AS n",
            )
            .await,
            vec![vec![Value::Int(8)]]
        );
    }

    #[tokio::test]
    async fn nonrecursive_ctes_expand_in_all_supported_set_operands() {
        let (engine, session) = engine_and_session();

        for (sql, expected) in [
            (
                "WITH c AS (SELECT 2 AS n) \
                 SELECT n FROM c UNION ALL SELECT 3 AS n ORDER BY n",
                vec![vec![Value::Int(2)], vec![Value::Int(3)]],
            ),
            (
                "WITH c AS (SELECT 2 AS n) SELECT n FROM c INTERSECT SELECT 2 AS n",
                vec![vec![Value::Int(2)]],
            ),
            (
                "WITH c AS (SELECT 2 AS n) SELECT n FROM c EXCEPT SELECT 3 AS n",
                vec![vec![Value::Int(2)]],
            ),
        ] {
            assert_eq!(rows(&engine, &session, sql).await, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn nested_with_shadows_outer_ctes_without_hiding_other_outer_names() {
        let (engine, session) = engine_and_session();

        let shadowed = "WITH c AS (SELECT 1 AS n) SELECT shadowed_value.n \
                        FROM (WITH c AS (SELECT 2 AS n) SELECT n FROM c) AS shadowed_value";
        assert_eq!(
            rows(&engine, &session, shadowed).await,
            vec![vec![Value::Int(2)]]
        );
        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH outer_value AS (SELECT 9 AS n) SELECT nested.n \
                 FROM (WITH local_value AS (SELECT 2 AS n) \
                       SELECT n FROM outer_value) AS nested",
            )
            .await,
            vec![vec![Value::Int(9)]]
        );
    }

    #[tokio::test]
    async fn cte_rewrite_does_not_capture_qualified_physical_tables() {
        let (engine, session) = engine_and_session();
        engine
            .execute("CREATE TABLE c (n INT)", Privilege::Admin, &session)
            .await
            .unwrap();
        engine
            .execute("INSERT INTO c VALUES (42)", Privilege::Admin, &session)
            .await
            .unwrap();

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH c AS (SELECT 1 AS n) SELECT n FROM elyra.c",
            )
            .await,
            vec![vec![Value::Int(42)]]
        );
    }

    #[tokio::test]
    async fn recursive_ctes_rewrite_outer_derived_and_scalar_references() {
        let (engine, session) = engine_and_session();
        let cte = "WITH RECURSIVE seq(n) AS (\
                       SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3\
                   )";

        assert_eq!(
            rows(
                &engine,
                &session,
                &format!("{cte} SELECT derived.n FROM (SELECT n FROM seq) AS derived ORDER BY n"),
            )
            .await,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );

        assert_eq!(
            rows(
                &engine,
                &session,
                &format!("{cte} SELECT (SELECT MAX(n) FROM seq) AS max_n"),
            )
            .await,
            vec![vec![Value::Int(3)]]
        );
    }

    #[tokio::test]
    async fn recursive_table_reference_must_be_direct_and_unique() {
        let (engine, session) = engine_and_session();

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH RECURSIVE seq(n) AS (\
                     SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3\
                 ) SELECT n FROM seq ORDER BY n",
            )
            .await,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );
        for sql in [
            "WITH RECURSIVE seq(n) AS (\
                 SELECT 1 UNION ALL \
                 SELECT prior.n + 1 FROM (SELECT n FROM seq) AS prior WHERE prior.n < 3\
             ) SELECT n FROM seq",
            "WITH RECURSIVE seq(n) AS (\
                 SELECT 1 UNION ALL SELECT (SELECT n + 1 FROM seq)\
             ) SELECT n FROM seq",
            "WITH RECURSIVE seq(n) AS (\
                 SELECT 1 UNION ALL \
                 SELECT left_seq.n + 1 FROM seq AS left_seq JOIN seq AS right_seq \
                 ON left_seq.n = right_seq.n WHERE left_seq.n < 3\
             ) SELECT n FROM seq",
            "WITH RECURSIVE seq(n) AS (\
                 SELECT 1 UNION ALL \
                 SELECT COALESCE(seq.n, 2) FROM (SELECT 1 AS x) AS seed \
                 LEFT JOIN seq ON seq.n < 0\
             ) SELECT n FROM seq",
        ] {
            let Err(error) = engine.execute(sql, Privilege::Admin, &session).await else {
                panic!("invalid recursive table placement unexpectedly succeeded")
            };
            assert!(
                error.to_string().contains("recursive table"),
                "unexpected error for `{sql}`: {error}"
            );
        }
    }

    #[tokio::test]
    async fn null_rejecting_where_allows_recursive_ref_on_outer_join_nullable_side() {
        let (engine, session) = engine_and_session();

        for recursive_value in ["c.n", "n", "ABS(c.n)"] {
            let sql = format!(
                "WITH RECURSIVE c(n) AS (\
                     SELECT 1 UNION ALL \
                     SELECT c.n + 1 FROM (SELECT 1 AS d) seed \
                     LEFT JOIN c ON TRUE WHERE {recursive_value} < 3\
                 ) SELECT n FROM c ORDER BY n"
            );
            assert_eq!(
                rows(&engine, &session, &sql).await,
                vec![
                    vec![Value::Int(1)],
                    vec![Value::Int(2)],
                    vec![Value::Int(3)]
                ],
                "{sql}"
            );
        }
        for predicate in ["c.n < 3 OR FALSE", "NOT (c.n IS NULL OR c.n >= 3)"] {
            let sql = format!(
                "WITH RECURSIVE c(n) AS (\
                     SELECT 1 UNION ALL \
                     SELECT c.n + 1 FROM (SELECT 1 AS d) seed \
                     LEFT JOIN c ON TRUE WHERE {predicate}\
                 ) SELECT n FROM c ORDER BY n"
            );
            assert_eq!(
                rows(&engine, &session, &sql).await,
                vec![
                    vec![Value::Int(1)],
                    vec![Value::Int(2)],
                    vec![Value::Int(3)]
                ],
                "{sql}"
            );
        }
        for (anchor, predicate) in [(0, "c.n"), (1, "NOT c.n"), (0, "c.n IS TRUE")] {
            let sql = format!(
                "WITH RECURSIVE c(n) AS (\
                     SELECT {anchor} UNION ALL \
                     SELECT c.n + 1 FROM (SELECT 1 AS d) seed \
                     LEFT JOIN c ON TRUE WHERE {predicate}\
                 ) SELECT n FROM c"
            );
            assert_eq!(
                rows(&engine, &session, &sql).await,
                vec![vec![Value::Int(anchor)]],
                "{sql}"
            );
        }
        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH RECURSIVE c AS (\
                     SELECT 1 AS n UNION ALL \
                     SELECT c.n + 1 FROM (SELECT 1 AS d) seed \
                     LEFT JOIN c ON TRUE WHERE n < 3\
                 ) SELECT n FROM c ORDER BY n",
            )
            .await,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );

        engine
            .execute(
                "CREATE TABLE cte_wildcard_anchor (n INT)",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO cte_wildcard_anchor VALUES (1)",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH RECURSIVE c AS (\
                     SELECT * FROM cte_wildcard_anchor UNION ALL \
                     SELECT c.n + 1 FROM (SELECT 1 AS d) seed \
                     LEFT JOIN c ON TRUE WHERE n < 3\
                 ) SELECT n FROM c ORDER BY n",
            )
            .await,
            vec![
                vec![Value::Int(1)],
                vec![Value::Int(2)],
                vec![Value::Int(3)]
            ]
        );
    }

    #[tokio::test]
    async fn unused_recursive_ctes_are_not_materialized_or_validated() {
        let (engine, session) = engine_and_session();

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH RECURSIVE unused(x, y) AS (SELECT 1) SELECT 42",
            )
            .await,
            vec![vec![Value::Int(42)]]
        );
        assert!(
            engine
                .execute(
                    "WITH RECURSIVE used(x, y) AS (SELECT 1) SELECT * FROM used",
                    Privilege::Admin,
                    &session,
                )
                .await
                .is_err(),
            "a referenced CTE must still validate its declared width"
        );
    }

    #[tokio::test]
    async fn nested_ctes_shadow_an_outer_recursive_name_at_the_declaration_point() {
        let (engine, session) = engine_and_session();

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH RECURSIVE seq(n) AS (\
                     SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2\
                 ) SELECT nested.n FROM (\
                     WITH seq AS (SELECT n + 10 AS n FROM seq) \
                     SELECT n FROM seq\
                 ) AS nested ORDER BY nested.n",
            )
            .await,
            vec![vec![Value::Int(11)], vec![Value::Int(12)]]
        );
    }

    #[tokio::test]
    async fn nested_recursive_ctes_do_not_pre_shadow_an_outer_recursive_name() {
        let (engine, session) = engine_and_session();

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH RECURSIVE seq(n) AS (\
                     SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2\
                 ) SELECT nested.n FROM (\
                     WITH RECURSIVE shifted AS (SELECT n + 10 AS n FROM seq), \
                                    seq AS (SELECT n FROM shifted) \
                     SELECT n FROM seq\
                 ) AS nested ORDER BY nested.n",
            )
            .await,
            vec![vec![Value::Int(11)], vec![Value::Int(12)]]
        );
    }

    #[tokio::test]
    async fn nested_recursive_ctes_do_not_pre_shadow_an_outer_inline_name() {
        let (engine, session) = engine_and_session();

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH seq AS (SELECT 1 AS n) \
                 SELECT nested.n FROM (\
                     WITH RECURSIVE shifted AS (SELECT n + 10 AS n FROM seq), \
                                    seq AS (SELECT n FROM shifted) \
                     SELECT n FROM seq\
                 ) AS nested ORDER BY nested.n",
            )
            .await,
            vec![vec![Value::Int(11)]]
        );

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH seq AS (SELECT 100 AS n) \
                 SELECT nested.n FROM (\
                     WITH RECURSIVE seq(n) AS (\
                         SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2\
                     ) SELECT n FROM seq\
                 ) AS nested ORDER BY nested.n",
            )
            .await,
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
    }

    #[test]
    fn recursive_reference_finder_observes_declaration_point_scope() {
        let query =
            super::parse_query("WITH seq AS (SELECT n + 10 AS n FROM seq) SELECT n FROM seq")
                .unwrap();
        assert!(super::query_refs_table(&query, "seq"));

        let shadowed = super::parse_query(
            "WITH seed AS (SELECT 1 AS n), \
                  seq AS (SELECT n FROM seed), \
                  later AS (SELECT n FROM seq) \
             SELECT n FROM later",
        )
        .unwrap();
        assert!(!super::query_refs_table(&shadowed, "seq"));

        let nested_recursive = super::parse_query(
            "WITH RECURSIVE shifted AS (SELECT n + 10 AS n FROM seq), \
                            seq AS (SELECT n FROM shifted) \
             SELECT n FROM seq",
        )
        .unwrap();
        assert!(super::query_refs_table(&nested_recursive, "seq"));
    }

    #[tokio::test]
    async fn recursive_cte_forward_references_remain_rejected() {
        let (engine, session) = engine_and_session();
        let result = engine
            .execute(
                "WITH RECURSIVE early AS (SELECT n FROM later), \
                                later AS (SELECT 1 AS n) \
                 SELECT n FROM early",
                Privilege::Admin,
                &session,
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn earlier_recursive_cte_resolves_later_name_as_physical_table() {
        let (engine, session) = engine_and_session();
        engine
            .execute("CREATE TABLE later (n INT)", Privilege::Admin, &session)
            .await
            .unwrap();
        engine
            .execute("INSERT INTO later VALUES (5)", Privilege::Admin, &session)
            .await
            .unwrap();

        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH RECURSIVE early AS (SELECT n FROM later), \
                                later(x, y) AS (SELECT 1) \
                 SELECT n FROM early",
            )
            .await,
            vec![vec![Value::Int(5)]]
        );
    }

    #[tokio::test]
    async fn temp_cleanup_does_not_drop_a_replacement_table() {
        let (engine, session) = engine_and_session();
        let schema = Schema::new(vec![ColumnDef::new("n", ColumnType::Int, true)]);
        let owned = super::create_temp_table(&session, "owner_race", &schema)
            .await
            .unwrap();

        engine
            .execute(
                &format!("DROP TABLE {}", owned.name),
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        engine
            .execute(
                &format!("CREATE TABLE {} (n INT)", owned.name),
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        engine
            .execute(
                &format!("INSERT INTO {} VALUES (99)", owned.name),
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();

        let replacement_write =
            super::fill_table(&session, &owned, &schema, &[vec![Value::Int(7)]]).await;
        assert!(
            matches!(replacement_write, Err(Error::Conflict(_))),
            "materialization must stop when the temporary relation was replaced"
        );
        super::drop_temp_table(&session, &owned).await.unwrap();
        assert_eq!(
            rows(&engine, &session, &format!("SELECT n FROM {}", owned.name),).await,
            vec![vec![Value::Int(99)]]
        );
    }

    #[tokio::test]
    async fn failed_recursive_cte_materialization_cleans_up_internal_tables() {
        let (engine, session) = engine_and_session();
        let counter = 8_000_000_000_000_000_000_u64;
        let sentinel = super::temp_name(counter, "cleanup_seq");
        super::TEMP_COUNTER.store(counter, std::sync::atomic::Ordering::SeqCst);
        engine
            .execute(
                &format!("CREATE TABLE {sentinel} (n INT)"),
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        engine
            .execute(
                &format!("INSERT INTO {sentinel} VALUES (99)"),
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "WITH RECURSIVE cleanup_seq(n) AS (\
                     SELECT 1 UNION ALL \
                     SELECT missing_column FROM cleanup_seq WHERE n < 2\
                 ) SELECT n FROM cleanup_seq",
                Privilege::Admin,
                &session,
            )
            .await;
        assert!(result.is_err());

        assert_eq!(
            rows(&engine, &session, &format!("SELECT n FROM {sentinel}"),).await,
            vec![vec![Value::Int(99)]],
            "recursive cleanup must not remove a pre-existing user relation"
        );

        let internal_catalog_rows = session
            .scan_batch(b"catalog::__cte_".to_vec(), None, 128)
            .await
            .unwrap();
        assert!(
            internal_catalog_rows
                .iter()
                .all(|(key, _)| { key == &super::catalog_key(&sentinel) }),
            "failed recursive CTE leaked internal catalog rows"
        );
        engine
            .execute(
                &format!("DROP TABLE {sentinel}"),
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        assert_eq!(
            rows(
                &engine,
                &session,
                "WITH RECURSIVE cleanup_seq(n) AS (\
                     SELECT 1 UNION ALL SELECT n + 1 FROM cleanup_seq WHERE n < 2\
                 ) SELECT n FROM cleanup_seq ORDER BY n",
            )
            .await,
            vec![vec![Value::Int(1)], vec![Value::Int(2)]]
        );
        assert_eq!(
            rows(&engine, &session, "SELECT 42").await,
            vec![vec![Value::Int(42)]]
        );
    }

    #[tokio::test]
    async fn ordinary_cte_dependency_chain_still_executes() {
        let (engine, session) = engine_and_session();
        let definitions = (0..super::MAX_CTE_EXPANSION_DEPTH)
            .map(|index| {
                if index == 0 {
                    "c0 AS (SELECT 7 AS n)".to_owned()
                } else {
                    format!("c{index} AS (SELECT n + 1 AS n FROM c{})", index - 1)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        assert_eq!(
            rows(
                &engine,
                &session,
                &format!(
                    "WITH {definitions} SELECT n FROM c{}",
                    super::MAX_CTE_EXPANSION_DEPTH - 1
                ),
            )
            .await,
            vec![vec![Value::Int(22)]]
        );
    }

    #[tokio::test]
    async fn binary_collation_survives_a_cte_dependency_chain() {
        let (engine, session) = engine_and_session();
        engine
            .execute(
                "CREATE TABLE cte_bin_values (s VARCHAR(8) COLLATE utf8mb4_bin)",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO cte_bin_values VALUES ('a'), ('A')",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        assert_eq!(
            rows(
                &engine,
                &session,
                "SELECT s FROM cte_bin_values WHERE s = 'a'",
            )
            .await,
            vec![vec![Value::Text("a".into())]]
        );
        let definitions = (0..super::MAX_CTE_EXPANSION_DEPTH)
            .map(|index| {
                if index == 0 {
                    "c0 AS (SELECT s FROM cte_bin_values)".to_owned()
                } else {
                    format!("c{index} AS (SELECT s FROM c{})", index - 1)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        assert_eq!(
            rows(
                &engine,
                &session,
                &format!(
                    "WITH {definitions} SELECT s FROM c{} WHERE s = 'a'",
                    super::MAX_CTE_EXPANSION_DEPTH - 1
                ),
            )
            .await,
            vec![vec![Value::Text("a".into())]]
        );
        assert_eq!(
            rows(
                &engine,
                &session,
                &format!(
                    "WITH {definitions} SELECT s FROM c{} ORDER BY s",
                    super::MAX_CTE_EXPANSION_DEPTH - 1
                ),
            )
            .await,
            vec![vec![Value::Text("A".into())], vec![Value::Text("a".into())]]
        );
        let grouped = rows(
            &engine,
            &session,
            &format!(
                "WITH {definitions} SELECT s FROM c{} GROUP BY s",
                super::MAX_CTE_EXPANSION_DEPTH - 1
            ),
        )
        .await;
        assert_eq!(grouped.len(), 2);
        assert!(grouped.contains(&vec![Value::Text("A".into())]));
        assert!(grouped.contains(&vec![Value::Text("a".into())]));
    }

    #[tokio::test]
    async fn cte_dependency_chain_over_the_depth_limit_is_rejected() {
        let (engine, session) = engine_and_session();
        let definitions = (0..=super::MAX_CTE_EXPANSION_DEPTH)
            .map(|index| {
                if index == 0 {
                    "c0 AS (SELECT 7 AS n)".to_owned()
                } else {
                    format!("c{index} AS (SELECT n FROM c{})", index - 1)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let result = engine
            .execute(
                &format!(
                    "WITH {definitions} SELECT n FROM c{}",
                    super::MAX_CTE_EXPANSION_DEPTH
                ),
                Privilege::Admin,
                &session,
            )
            .await;

        let Err(Error::Parse(message)) = result else {
            panic!("over-limit CTE dependency chain unexpectedly succeeded");
        };
        assert!(message.contains("CTE expansion limit exceeded (depth limit"));
    }

    #[tokio::test]
    async fn many_unused_cte_definitions_are_rejected_as_too_complex() {
        let (engine, session) = engine_and_session();
        let definitions = (0..5_000)
            .map(|index| format!("unused_{index} AS (SELECT {index} AS n)"))
            .collect::<Vec<_>>()
            .join(", ");
        let result = engine
            .execute(
                &format!("WITH {definitions} SELECT 1"),
                Privilege::Admin,
                &session,
            )
            .await;

        let Err(Error::Parse(message)) = result else {
            panic!("excessive unused CTE definitions unexpectedly succeeded");
        };
        assert!(message.contains("CTE expansion limit exceeded (AST node limit"));
    }

    #[tokio::test]
    async fn cancelled_iterative_cte_chain_stops_between_layers() {
        let (engine, session) = engine_and_session();
        let definitions = (0..super::MAX_CTE_EXPANSION_DEPTH)
            .map(|index| {
                if index == 0 {
                    "c0 AS (SELECT 1 AS n)".to_owned()
                } else {
                    format!("c{index} AS (SELECT n + 1 AS n FROM c{})", index - 1)
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        session.cancel_token().cancel();
        let result = engine
            .execute(
                &format!(
                    "WITH {definitions} SELECT n FROM c{}",
                    super::MAX_CTE_EXPANSION_DEPTH - 1
                ),
                Privilege::Admin,
                &session,
            )
            .await;

        let Err(error) = result else {
            panic!("a cancelled iterative CTE chain must stop");
        };
        assert!(error.to_string().contains("cancelled"));
        session.disarm_cancel();
    }

    #[tokio::test]
    async fn excessive_cte_expansion_width_returns_a_complexity_error() {
        let (engine, session) = engine_and_session();
        let relations = (0..=super::MAX_CTE_EXPANSION_NODES)
            .map(|index| {
                if index == 0 {
                    "c AS c0".to_owned()
                } else {
                    format!("CROSS JOIN c AS c{index}")
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        let result = engine
            .execute(
                &format!("WITH c AS (SELECT 1 AS n) SELECT c0.n FROM {relations}"),
                Privilege::Admin,
                &session,
            )
            .await;

        let Err(Error::Parse(message)) = result else {
            panic!("excessive CTE expansion unexpectedly succeeded");
        };
        assert!(message.contains("CTE expansion limit exceeded (node limit"));
    }

    #[test]
    fn excessive_cte_expansion_returns_an_error_without_aborting() {
        if std::env::var_os(EXPANSION_LIMIT_CHILD).is_some() {
            let definitions = (0..=100)
                .map(|index| {
                    if index == 0 {
                        "c0 AS (SELECT n FROM seq)".to_owned()
                    } else {
                        format!("c{index} AS (SELECT n FROM c{})", index - 1)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "WITH RECURSIVE seq(n) AS (\
                     SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 3\
                 ) SELECT nested.n FROM (\
                     WITH {definitions} SELECT n FROM c100\
                 ) AS nested"
            );
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(async {
                let (engine, session) = engine_and_session();
                engine.execute(&sql, Privilege::Admin, &session).await
            });
            let message = match result {
                Err(Error::Parse(message)) => message,
                Err(error) => panic!("expected a SQL complexity error, got {error}"),
                Ok(_) => panic!("excessive CTE expansion unexpectedly succeeded"),
            };
            assert!(
                message.contains("CTE expansion limit exceeded (depth limit"),
                "unexpected error: {message}"
            );
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "exec::cte_rewrite_tests::excessive_cte_expansion_returns_an_error_without_aborting",
                "--nocapture",
            ])
            .env(EXPANSION_LIMIT_CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child process failed with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(test)]
mod coercion_tests {
    use elyra_core::{ColumnType, Value};

    use super::{coerce_with_mode, round_decimal_to_integer};

    #[test]
    fn rounds_decimal_integers_exactly() {
        assert_eq!(round_decimal_to_integer(14, 1), 1);
        assert_eq!(round_decimal_to_integer(15, 1), 2);
        assert_eq!(round_decimal_to_integer(-15, 1), -2);
        assert_eq!(round_decimal_to_integer(i128::MAX, 39), 0);
    }

    #[test]
    fn coerces_decimal_literals_to_unsigned_columns() {
        assert_eq!(
            coerce_with_mode(Value::Decimal(125, 1), &ColumnType::UInt, "n", true).unwrap(),
            Value::UInt(13)
        );
        assert!(coerce_with_mode(Value::Decimal(-15, 1), &ColumnType::UInt, "n", true).is_err());
        assert_eq!(
            coerce_with_mode(Value::Decimal(-15, 1), &ColumnType::UInt, "n", false).unwrap(),
            Value::UInt(0)
        );
    }
}

#[cfg(test)]
mod numeric_evaluation_coercion_tests {
    use crate::{Engine, QueryResult, Session};
    use elyra_core::{Privilege, Value};
    use elyra_storage::Db;

    async fn rows(engine: &Engine, session: &Session, sql: &str) -> Vec<Vec<Value>> {
        let mut outcomes = engine
            .execute(sql, Privilege::Admin, session)
            .await
            .unwrap_or_else(|error| panic!("query failed for {sql}: {error}"));
        assert_eq!(outcomes.len(), 1, "expected one outcome for {sql}");
        let QueryResult::Rows(mut stream) = outcomes.remove(0) else {
            panic!("expected rows for {sql}");
        };
        let mut result = Vec::new();
        loop {
            let batch = stream.next_batch(128).await.unwrap();
            if batch.is_empty() {
                return result;
            }
            result.extend(batch);
        }
    }

    #[tokio::test]
    async fn numeric_evaluation_coerces_text_consistently() {
        let engine = Engine::new(Db::in_memory().unwrap());
        let session = engine.session();

        assert_eq!(
            rows(
                &engine,
                &session,
                "SELECT '3.5tail' + 0, 0 + '3', '3' * 2, -'3tail', \
                        CAST('3.25tail' AS FLOAT), CAST('4.5tail' AS DOUBLE), \
                        ABS('-3.5tail'), ROUND('3.7tail'), SQRT('9tail'), \
                        UNIX_TIMESTAMP('1970-01-02 00:00:00')",
            )
            .await,
            vec![vec![
                Value::Float(3.5),
                Value::Float(3.0),
                Value::Float(6.0),
                Value::Float(-3.0),
                Value::Float(3.25),
                Value::Float(4.5),
                Value::Float(3.5),
                Value::Int(4),
                Value::Float(3.0),
                Value::Int(86_400),
            ]]
        );

        assert_eq!(
            rows(
                &engine,
                &session,
                "SELECT 'not a number' + 1, CAST('not a number' AS FLOAT), \
                        ABS('not a number'), ROUND('not a number'), \
                        SQRT('not a number'), UNIX_TIMESTAMP('not a number'), \
                        NULL + '3'",
            )
            .await,
            vec![vec![
                Value::Float(1.0),
                Value::Float(0.0),
                Value::Float(0.0),
                Value::Int(0),
                Value::Float(0.0),
                Value::Int(0),
                Value::Null,
            ]]
        );
    }

    #[tokio::test]
    async fn sum_and_avg_include_text_coerced_to_zero() {
        let engine = Engine::new(Db::in_memory().unwrap());
        let session = engine.session();
        engine
            .execute(
                "CREATE TABLE numeric_text (v VARCHAR(32))",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();
        engine
            .execute(
                "INSERT INTO numeric_text VALUES ('2.5tail'), ('3'), ('not a number'), (NULL)",
                Privilege::Admin,
                &session,
            )
            .await
            .unwrap();

        let result = rows(&engine, &session, "SELECT SUM(v), AVG(v) FROM numeric_text").await;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0][0], Value::Float(5.5));
        let Value::Float(avg) = result[0][1] else {
            panic!("AVG must return a float: {:?}", result[0][1]);
        };
        assert!((avg - 5.5 / 3.0).abs() < f64::EPSILON);
    }
}

#[cfg(test)]
mod substitution_tests {
    use std::collections::HashMap;

    use elyra_core::Value;

    use super::{contains_uvar_reference, substitute_uvars, substitute_vars};

    #[test]
    fn detects_only_unquoted_user_variable_references() {
        assert!(!contains_uvar_reference(
            r#"INSERT INTO t VALUES ('otilia@example.com', "quoted@example", `column@example`)"#
        ));
        assert!(!contains_uvar_reference("SELECT @@global.max_connections"));
        assert!(contains_uvar_reference(
            "SELECT 'otilia@example.com', @answer"
        ));
        assert!(contains_uvar_reference("SELECT @answer, 'å🚗'"));
    }

    #[test]
    fn user_variable_substitution_skips_mysql_quoted_segments() {
        let vars = HashMap::from([("answer".to_owned(), Value::Int(42))]);
        let sql = r#"SELECT 'otilia@example.com', 'O\'Keefe', "quoted@example", `column@example`, @answer"#;

        assert_eq!(
            substitute_uvars(sql, &vars),
            r#"SELECT 'otilia@example.com', 'O\'Keefe', "quoted@example", `column@example`, 42"#
        );
    }

    #[test]
    fn procedure_variable_substitution_skips_escaped_string_literals() {
        let vars = HashMap::from([("example".to_owned(), Value::Text("changed".to_owned()))]);
        let sql = r#"SELECT 'O\'Keefe@example.com', example"#;

        assert_eq!(
            substitute_vars(sql, &vars),
            r#"SELECT 'O\'Keefe@example.com', 'changed'"#
        );
    }
}

#[cfg(test)]
mod plan_tests {
    use super::{
        expr_qualifier, order_col_index, order_is_pk_prefix, ordered_scan_budget, relation_aliases,
        row_binding_index, secondary_order_plan, NullMode,
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
            storage_generation: 0,
        }
    }

    fn ob(name: &str, asc: bool) -> (Expr, bool) {
        (Expr::Identifier(Ident::new(name)), asc)
    }

    #[test]
    fn join_planning_preserves_complete_relation_qualifiers() {
        let schema = Schema::new(vec![ColumnDef::new(
            "elyra.orders.id",
            ColumnType::Int,
            false,
        )
        .with_qualifier(vec!["elyra".into(), "orders".into()])]);
        assert_eq!(
            relation_aliases(&schema.columns),
            [vec!["elyra".to_string(), "orders".to_string()]]
                .into_iter()
                .collect()
        );
        let expression = Expr::CompoundIdentifier(vec![
            Ident::new("elyra"),
            Ident::new("orders"),
            Ident::new("id"),
        ]);
        assert_eq!(
            expr_qualifier(&expression),
            Some(vec!["elyra".to_string(), "orders".to_string()])
        );
    }

    #[test]
    fn ambiguous_short_outer_qualifiers_are_not_silently_left_unbound() {
        let schema = Schema::new(vec![
            ColumnDef::new("db1.qa.id", ColumnType::Int, false)
                .with_qualifier(vec!["db1".into(), "qa".into()]),
            ColumnDef::new("db2.qa.id", ColumnType::Int, false)
                .with_qualifier(vec!["db2".into(), "qa".into()]),
        ]);
        let parts = [Ident::new("qa"), Ident::new("id")];
        assert!(matches!(
            row_binding_index(&parts, &schema),
            Err(elyra_core::Error::Query(message)) if message.contains("ambiguous")
        ));
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

#[cfg(test)]
mod composite_join_tests {
    use std::sync::Arc;

    use elyra_core::cancel::QueryCancel;
    use elyra_core::{ColumnDef, ColumnType, Schema, Value};
    use sqlparser::ast::{BinaryOperator, Expr};

    use super::{column_def_expr, combine, JoinCondition, JoinKind, NESTED_JOIN_COMPARISONS};

    #[test]
    fn multi_key_equality_avoids_quadratic_fallback() {
        let left_columns = [
            ColumnDef::new("l.id", ColumnType::Int, false).with_qualifier(vec!["l".into()]),
            ColumnDef::new("l.code", ColumnType::Int, false).with_qualifier(vec!["l".into()]),
        ];
        let right_columns = [
            ColumnDef::new("r.id", ColumnType::Int, false).with_qualifier(vec!["r".into()]),
            ColumnDef::new("r.code", ColumnType::Int, false).with_qualifier(vec!["r".into()]),
        ];
        let left_schema = Schema::new(left_columns.into());
        let right_schema = Schema::new(right_columns.into());
        let left_rows = (0..256)
            .map(|value| vec![Value::Int(value / 4), Value::Int(value % 4)])
            .collect::<Vec<_>>();
        let right_rows = left_rows.clone();
        let equality = |left: usize, right: usize| Expr::BinaryOp {
            left: Box::new(column_def_expr(&left_schema.columns[left])),
            op: BinaryOperator::Eq,
            right: Box::new(column_def_expr(&right_schema.columns[right])),
        };
        let predicate = Expr::BinaryOp {
            left: Box::new(equality(0, 0)),
            op: BinaryOperator::And,
            right: Box::new(equality(1, 1)),
        };

        NESTED_JOIN_COMPARISONS.with(|comparisons| comparisons.set(0));
        let (_, rows) = combine(
            &left_schema,
            &left_rows,
            &right_schema,
            &right_rows,
            JoinKind::Inner,
            Some(JoinCondition::On(&predicate)),
            &Arc::new(QueryCancel::new()),
        )
        .unwrap();
        let comparisons = NESTED_JOIN_COMPARISONS.with(std::cell::Cell::get);

        assert_eq!(rows.len(), left_rows.len());
        assert!(
            comparisons <= left_rows.len() * 4,
            "multi-key equality used {comparisons} pairwise comparisons"
        );
    }
}
