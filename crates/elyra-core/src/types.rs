//! Column and schema types. MySQL-flavoured surface, plus VECTOR.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Bool,
    /// 64-bit signed integer (MySQL BIGINT).
    Int,
    /// 64-bit float (MySQL DOUBLE).
    Float,
    /// Arbitrary-length UTF-8 text.
    Text,
    /// Raw bytes (MySQL BLOB).
    Bytes,
    /// Fixed-dimension float32 vector for ANN search. Dimension is the arg.
    Vector(u32),
    /// Calendar date.
    Date,
    /// Date + time of day.
    DateTime,
    /// Fixed-point decimal: (precision, scale).
    Decimal(u8, u8),
    /// Time of day.
    Time,
    /// JSON document.
    Json,
}

impl ColumnType {
    /// Human-readable ElyraSQL/MySQL type name (used in metadata responses).
    pub fn display_name(&self) -> String {
        match self {
            ColumnType::Bool => "TINYINT(1)".into(),
            ColumnType::Int => "BIGINT".into(),
            ColumnType::Float => "DOUBLE".into(),
            ColumnType::Text => "TEXT".into(),
            ColumnType::Bytes => "BLOB".into(),
            ColumnType::Vector(d) => format!("VECTOR({d})"),
            ColumnType::Date => "DATE".into(),
            ColumnType::DateTime => "DATETIME".into(),
            ColumnType::Decimal(p, s) => format!("DECIMAL({p},{s})"),
            ColumnType::Time => "TIME".into(),
            ColumnType::Json => "JSON".into(),
        }
    }
}

/// Text collation for a column: the default is case-insensitive (`Ci`); `Bin`
/// makes comparison, ordering, indexing and uniqueness case-sensitive
/// (`COLLATE ..._bin` / `BINARY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Collation {
    #[default]
    Ci,
    Bin,
}

impl Collation {
    /// Interpret a SQL collation or charset name.
    pub fn from_name(name: &str) -> Collation {
        let n = name.to_ascii_lowercase();
        if n == "binary" || n.ends_with("_bin") || n.ends_with("_cs") {
            Collation::Bin
        } else {
            Collation::Ci
        }
    }
    pub fn is_bin(self) -> bool {
        matches!(self, Collation::Bin)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
    /// Text collation (defaults to the case-insensitive `Ci`).
    #[serde(default)]
    pub collation: Collation,
}

impl ColumnDef {
    /// A column with the default case-insensitive collation.
    pub fn new(name: impl Into<String>, ty: ColumnType, nullable: bool) -> Self {
        ColumnDef {
            name: name.into(),
            ty,
            nullable,
            collation: Collation::Ci,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }
}
