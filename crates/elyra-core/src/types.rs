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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    pub nullable: bool,
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
        self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
}
