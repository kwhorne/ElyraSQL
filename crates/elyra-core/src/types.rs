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
    /// 64-bit unsigned integer (MySQL `BIGINT UNSIGNED`). Added last so existing
    /// bincode-encoded catalogs (which never contain it) still decode.
    UInt,
}

impl ColumnType {
    /// Human-readable ElyraSQL/MySQL type name (used in metadata responses).
    pub fn display_name(&self) -> String {
        match self {
            ColumnType::Bool => "TINYINT(1)".into(),
            ColumnType::Int => "BIGINT".into(),
            ColumnType::UInt => "BIGINT UNSIGNED".into(),
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
    /// Structured relation qualifier used while planning and executing a query.
    ///
    /// Keeping identifier components separate is necessary because both table
    /// and column identifiers may themselves contain dots. Like result metadata
    /// on [`Schema`], this is transient so catalog encoding stays compatible.
    #[serde(skip)]
    pub qualifier: Vec<String>,
    /// Direct-storage attributes used only when a column is sent in a result
    /// set. This stays out of catalog encoding so existing databases remain
    /// readable; the engine reconstructs it from the table definition on load.
    #[serde(skip)]
    pub result_metadata: ResultColumnMetadata,
}

/// Source-key attributes used by the MySQL result-column descriptor.
///
/// They deliberately live beside, rather than inside, persisted table schema:
/// indexes and auto-increment state already describe them in the catalog, and
/// adding an encoded field would make bincode catalogs incompatible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResultColumnMetadata {
    pub primary_key: bool,
    pub unique_key: bool,
    pub auto_increment: bool,
}

impl ColumnDef {
    /// A column with the default case-insensitive collation.
    pub fn new(name: impl Into<String>, ty: ColumnType, nullable: bool) -> Self {
        ColumnDef {
            name: name.into(),
            ty,
            nullable,
            collation: Collation::Ci,
            qualifier: Vec::new(),
            result_metadata: ResultColumnMetadata::default(),
        }
    }

    pub fn with_qualifier(mut self, qualifier: Vec<String>) -> Self {
        self.qualifier = qualifier;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    /// Source table (or alias) per column, for result metadata only: MySQL puts
    /// the bare column name in the name field and the qualifier here, which is
    /// the only way a client can tell two same-named columns of a join apart.
    ///
    /// Empty, or exactly as long as `columns`. An entry is empty for anything
    /// that has no source table (an expression, a literal, an aggregate) — MySQL
    /// reports an empty table for those too.
    ///
    /// **Not persisted.** `Schema` is bincode-encoded into the catalog as part of
    /// `TableDef`, and bincode is not self-describing, so a new *encoded* field
    /// would make every existing database unreadable. `serde(skip)` keeps the
    /// on-disk encoding byte-identical and gives old catalogs `Default` on read.
    #[serde(skip)]
    pub tables: Vec<String>,
    /// Execution-only columns that qualified references may access, but bare
    /// references and unqualified wildcards must ignore.
    #[serde(skip)]
    unqualified_hidden: Vec<bool>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        let unqualified_hidden = vec![false; columns.len()];
        Self {
            columns,
            tables: Vec::new(),
            unqualified_hidden,
        }
    }

    /// A schema that also knows where each column came from. `tables` is ignored
    /// unless it lines up with `columns`, so a caller that can only qualify some
    /// of its columns passes empty strings for the rest rather than a short list.
    pub fn with_tables(columns: Vec<ColumnDef>, tables: Vec<String>) -> Self {
        let tables = if tables.len() == columns.len() {
            tables
        } else {
            Vec::new()
        };
        let unqualified_hidden = vec![false; columns.len()];
        Self {
            columns,
            tables,
            unqualified_hidden,
        }
    }

    /// The source table of column `i`, if it has one.
    pub fn table_of(&self, i: usize) -> Option<&str> {
        self.tables
            .get(i)
            .map(String::as_str)
            .filter(|t| !t.is_empty())
    }

    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Hide one physical column from bare resolution and unqualified `*` while
    /// retaining it for an exact qualified reference such as `table.column`.
    pub fn hide_from_unqualified(&mut self, index: usize) {
        if self.unqualified_hidden.len() < self.columns.len() {
            self.unqualified_hidden.resize(self.columns.len(), false);
        }
        if let Some(hidden) = self.unqualified_hidden.get_mut(index) {
            *hidden = true;
        }
    }

    /// Whether a physical column is excluded from bare resolution and `*`.
    pub fn is_hidden_from_unqualified(&self, index: usize) -> bool {
        self.unqualified_hidden.get(index).copied().unwrap_or(false)
    }

    /// Physical indexes exposed through an unqualified wildcard.
    pub fn unqualified_indices(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.columns.len()).filter(|&index| !self.is_hidden_from_unqualified(index))
    }
}

#[cfg(test)]
mod schema_metadata_tests {
    use super::{ColumnDef, ColumnType, ResultColumnMetadata, Schema};

    fn cols() -> Vec<ColumnDef> {
        vec![
            ColumnDef::new("id", ColumnType::Int, false),
            ColumnDef::new("name", ColumnType::Text, true),
        ]
    }

    #[test]
    fn qualifiers_never_reach_the_encoded_form() {
        let plain = Schema::new(cols());
        let qualified_columns = cols()
            .into_iter()
            .enumerate()
            .map(|(index, mut column)| {
                column.qualifier = vec!["elyra".into(), "users".into()];
                column.result_metadata = ResultColumnMetadata {
                    primary_key: index == 0,
                    unique_key: index == 1,
                    auto_increment: index == 0,
                };
                column
            })
            .collect();
        let qualified =
            Schema::with_tables(qualified_columns, vec!["users".into(), "users".into()]);
        assert_eq!(
            bincode::serialize(&plain).unwrap(),
            bincode::serialize(&qualified).unwrap(),
            "the catalog encoding must not change with result metadata"
        );
        let decoded: Schema =
            bincode::deserialize(&bincode::serialize(&qualified).unwrap()).unwrap();
        assert!(decoded.tables.is_empty());
        assert!(decoded
            .columns
            .iter()
            .all(|column| column.qualifier.is_empty()));
        assert!(decoded
            .columns
            .iter()
            .all(|column| column.result_metadata == ResultColumnMetadata::default()));
    }

    #[test]
    fn a_mismatched_qualifier_list_is_dropped_rather_than_misaligned() {
        let short = Schema::with_tables(cols(), vec!["users".into()]);
        assert_eq!(short.table_of(0), None);

        let full = Schema::with_tables(cols(), vec!["users".into(), String::new()]);
        assert_eq!(full.table_of(0), Some("users"));
        assert_eq!(
            full.table_of(1),
            None,
            "an empty entry means 'no source table'"
        );
        assert_eq!(full.table_of(9), None);
    }
}
