//! ElyraSQL error model. No internal engine names leak through here.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

/// What the catalog refused.
///
/// Clients branch on the specific MySQL code: an ORM reports "no such table" and
/// "unknown column" very differently, and a migration tool may treat "already
/// exists" as success. This used to be recovered by matching prefixes of the
/// human-readable message, which meant rewording an error silently changed the
/// code a client saw. The kind is now carried explicitly, so the wire code never
/// depends on message text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogError {
    /// The named table, view or materialized view does not exist.
    /// `ER_NO_SUCH_TABLE`. Also the bucket for catalog refusals with no more
    /// specific code, which is what the message-matching default did.
    Missing,
    /// A table or view of that name is already there. `ER_TABLE_EXISTS_ERROR`.
    Exists,
    /// A column named in a key, index or constraint does not exist on the table.
    /// `ER_BAD_FIELD_ERROR`.
    UnknownColumn,
    /// An index of that name is already there. `ER_DUP_KEYNAME`.
    IndexExists,
    /// The named index does not exist. `ER_KEY_DOES_NOT_EXIST`.
    UnknownIndex,
}

/// Which kind of duplicate was refused. A duplicate *key value* and a duplicate
/// *column name* are different errors to a client: 1062 is retryable-ish data,
/// 1060 is a schema mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicateError {
    /// A row collides with an existing key value. `ER_DUP_ENTRY`.
    Entry,
    /// A `CREATE`/`ALTER` names the same column twice. `ER_DUP_FIELDNAME`.
    ColumnName,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("syntax error: {0}")]
    Parse(String),

    #[error("catalog error: {1}")]
    Catalog(CatalogError, String),

    #[error("unknown database: {0}")]
    UnknownDatabase(String),

    #[error("unknown table: {0}")]
    UnknownTable(String),

    #[error("unknown column: {0}")]
    UnknownColumn(String),

    #[error("storage error: {0}")]
    Storage(String),

    /// The database file is held by another handle, in this process or another.
    ///
    /// Distinct from [`Error::Storage`] because callers act on it: an embedded
    /// caller waits and retries, a server reports a configuration mistake. It
    /// used to be indistinguishable without matching on the storage engine's
    /// message text.
    #[error("database file is locked by another handle: {0}")]
    StorageLocked(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("serialization failure: {0}")]
    Conflict(String),

    #[error("duplicate entry: {1}")]
    Duplicate(DuplicateError, String),

    #[error("foreign key constraint: {0}")]
    ForeignKey(String),

    #[error("type error: {0}")]
    Type(String),

    #[error("out of range: {0}")]
    OutOfRange(String),

    #[error("data too long: {0}")]
    DataTooLong(String),

    #[error("vector error: {0}")]
    Vector(String),

    #[error("analytics error: {0}")]
    Analytics(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// MySQL error code to report over the wire. Defaults to 1064
    /// (ER_PARSE_ERROR) for parse issues, 1105 (ER_UNKNOWN_ERROR) otherwise.
    pub fn mysql_code(&self) -> u16 {
        match self {
            Error::Parse(_) => 1064,
            Error::Catalog(kind, _) => match kind {
                CatalogError::Missing => 1146,       // ER_NO_SUCH_TABLE
                CatalogError::Exists => 1050,        // ER_TABLE_EXISTS_ERROR
                CatalogError::UnknownColumn => 1054, // ER_BAD_FIELD_ERROR
                CatalogError::IndexExists => 1061,   // ER_DUP_KEYNAME
                CatalogError::UnknownIndex => 1176,  // ER_KEY_DOES_NOT_EXIST
            },
            Error::UnknownDatabase(_) => 1049, // ER_BAD_DB_ERROR
            Error::UnknownTable(_) => 1109,    // ER_UNKNOWN_TABLE
            Error::UnknownColumn(_) => 1054,   // ER_BAD_FIELD_ERROR
            Error::Type(_) => 1366,            // ER_TRUNCATED_WRONG_VALUE
            // MySQL answers 1264 for a value that does not fit the *column* and
            // 1690 for an expression that overflows its type. Storing a value is
            // by far the common case here, and both share SQLSTATE 22003.
            Error::OutOfRange(_) => 1264,  // ER_WARN_DATA_OUT_OF_RANGE
            Error::DataTooLong(_) => 1406, // ER_DATA_TOO_LONG
            Error::Unsupported(_) => 1235, // ER_NOT_SUPPORTED_YET
            Error::Conflict(_) => 1213,    // ER_LOCK_DEADLOCK (serialization failure)
            Error::StorageLocked(_) => 1015, // ER_CANT_LOCK
            Error::Duplicate(DuplicateError::ColumnName, _) => 1060, // ER_DUP_FIELDNAME
            Error::Duplicate(DuplicateError::Entry, _) => 1062, // ER_DUP_ENTRY
            Error::ForeignKey(_) => 1452,  // ER_NO_REFERENCED_ROW
            _ => 1105,
        }
    }

    /// MySQL SQLSTATE string.
    pub fn sqlstate(&self) -> &'static [u8; 5] {
        match self {
            Error::Parse(_) => b"42000",
            Error::Catalog(kind, _) => match kind {
                CatalogError::UnknownColumn => b"42S22",
                CatalogError::Exists => b"42S01",
                CatalogError::IndexExists | CatalogError::UnknownIndex => b"42000",
                CatalogError::Missing => b"42S02",
            },
            Error::UnknownDatabase(_) => b"42000",
            Error::UnknownTable(_) => b"42S02",
            Error::UnknownColumn(_) => b"42S22",
            Error::OutOfRange(_) => b"22003",
            Error::DataTooLong(_) => b"22001",
            Error::Duplicate(DuplicateError::ColumnName, _) => b"42S21",
            _ => b"HY000",
        }
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::{CatalogError, DuplicateError, Error};

    /// The kind-to-code mapping is the wire contract clients branch on: an ORM
    /// reports 1146 and 1054 very differently, and a migration tool may treat
    /// 1050 as success. These pairs are the same ones the previous
    /// message-prefix matcher produced, so this pins the refactor as
    /// behaviour-preserving as well as pinning the codes.
    #[test]
    fn catalog_kinds_map_to_the_code_clients_branch_on() {
        let of = |kind| Error::Catalog(kind, "message text is irrelevant".into());
        for (kind, code, state) in [
            (CatalogError::Missing, 1146u16, b"42S02"),
            (CatalogError::Exists, 1050, b"42S01"),
            (CatalogError::UnknownColumn, 1054, b"42S22"),
            (CatalogError::IndexExists, 1061, b"42000"),
            (CatalogError::UnknownIndex, 1176, b"42000"),
        ] {
            assert_eq!(of(kind).mysql_code(), code, "{kind:?}");
            assert_eq!(of(kind).sqlstate(), state, "{kind:?}");
        }
    }

    /// Rewording an error must not change the code a client sees. That was the
    /// whole failure mode of deriving it from the message.
    #[test]
    fn the_code_does_not_depend_on_the_message() {
        for message in [
            "",
            "no such table: t",
            "already exists",
            "unknown column: g",
        ] {
            let error = Error::Catalog(CatalogError::Exists, message.into());
            assert_eq!(error.mysql_code(), 1050, "{message:?}");
            assert_eq!(error.sqlstate(), b"42S01", "{message:?}");
        }
        for message in ["duplicate column name 'a'", "anything at all"] {
            let error = Error::Duplicate(DuplicateError::Entry, message.into());
            assert_eq!(error.mysql_code(), 1062, "{message:?}");
        }
    }

    #[test]
    fn duplicate_kinds_separate_data_from_schema_mistakes() {
        let entry = Error::Duplicate(DuplicateError::Entry, "key 'PRIMARY'".into());
        assert_eq!(entry.mysql_code(), 1062);
        assert_eq!(entry.sqlstate(), b"HY000");
        let column = Error::Duplicate(DuplicateError::ColumnName, "'a'".into());
        assert_eq!(column.mysql_code(), 1060);
        assert_eq!(column.sqlstate(), b"42S21");
    }

    #[test]
    fn data_too_long_uses_mysqls_string_truncation_error() {
        let error = Error::DataTooLong("Data too long for column 'name' at row 1".into());
        assert_eq!(error.mysql_code(), 1406);
        assert_eq!(error.sqlstate(), b"22001");
    }
}
