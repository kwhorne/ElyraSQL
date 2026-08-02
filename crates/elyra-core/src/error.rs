//! ElyraSQL error model. No internal engine names leak through here.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("syntax error: {0}")]
    Parse(String),

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("unknown database: {0}")]
    UnknownDatabase(String),

    #[error("unknown table: {0}")]
    UnknownTable(String),

    #[error("unknown column: {0}")]
    UnknownColumn(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("serialization failure: {0}")]
    Conflict(String),

    #[error("duplicate entry: {0}")]
    Duplicate(String),

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
            Error::Catalog(m) => catalog_code(m),
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
            // A duplicate *key value* and a duplicate *column name* are
            // different errors to a client: 1062 is retryable-ish data, 1060 is
            // a schema mistake.
            Error::Duplicate(m) if m.starts_with("duplicate column name") => 1060,
            Error::Duplicate(_) => 1062,  // ER_DUP_ENTRY
            Error::ForeignKey(_) => 1452, // ER_NO_REFERENCED_ROW
            _ => 1105,
        }
    }

    /// MySQL SQLSTATE string.
    pub fn sqlstate(&self) -> &'static [u8; 5] {
        match self {
            Error::Parse(_) => b"42000",
            Error::Catalog(m) => match catalog_code(m) {
                1054 => b"42S22", // ER_BAD_FIELD_ERROR
                1050 => b"42S01", // ER_TABLE_EXISTS_ERROR
                1061 | 1176 => b"42000",
                _ => b"42S02", // ER_NO_SUCH_TABLE
            },
            Error::UnknownDatabase(_) => b"42000",
            Error::UnknownTable(_) => b"42S02",
            Error::UnknownColumn(_) => b"42S22",
            Error::OutOfRange(_) => b"22003",
            Error::DataTooLong(_) => b"22001",
            Error::Duplicate(m) if m.starts_with("duplicate column name") => b"42S21",
            _ => b"HY000",
        }
    }
}

/// `Catalog` covers everything the catalog can refuse, but clients branch on the
/// specific code: an ORM reports "no such table" and "unknown column" very
/// differently, and a migration tool may treat "already exists" as success.
/// MySQL has distinct codes for each, so answering 1146 for all of them
/// mislabels most. The variant carries no structure, so the message prefix is
/// the only discriminator available; anything unrecognised keeps the old bucket.
fn catalog_code(message: &str) -> u16 {
    let m = message.trim_start();
    if m.starts_with("unknown column")
        || m.starts_with("unknown primary key column")
        || m.starts_with("unknown unique column")
        || m.starts_with("unknown index column")
        || m.starts_with("unknown foreign key column")
    {
        return 1054; // ER_BAD_FIELD_ERROR
    }
    if m.starts_with("index already exists") {
        return 1061; // ER_DUP_KEYNAME
    }
    if m.starts_with("unknown index") || m.starts_with("no such index") {
        return 1176; // ER_KEY_DOES_NOT_EXIST
    }
    if m.contains("already exists") {
        return 1050; // ER_TABLE_EXISTS_ERROR
    }
    1146 // ER_NO_SUCH_TABLE
}

#[cfg(test)]
mod catalog_code_tests {
    use super::Error;

    #[test]
    fn catalog_errors_map_to_the_code_clients_branch_on() {
        let code = |m: &str| Error::Catalog(m.into()).mysql_code();
        assert_eq!(code("unknown column: g"), 1054);
        assert_eq!(code("unknown index column: g"), 1054);
        assert_eq!(code("no such table: t"), 1146);
        assert_eq!(code("no such materialized view: v"), 1146);
        assert_eq!(code("index already exists: ix"), 1061);
        assert_eq!(code("table already exists: t"), 1050);
        assert_eq!(code("unknown index: ix"), 1176);
        assert_eq!(
            Error::Catalog("unknown column: g".into()).sqlstate(),
            b"42S22"
        );
        assert_eq!(
            Error::Catalog("no such table: t".into()).sqlstate(),
            b"42S02"
        );
    }

    #[test]
    fn data_too_long_uses_mysqls_string_truncation_error() {
        let error = Error::DataTooLong("Data too long for column 'name' at row 1".into());
        assert_eq!(error.mysql_code(), 1406);
        assert_eq!(error.sqlstate(), b"22001");
    }
}
