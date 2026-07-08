//! ElyraSQL error model. No internal engine names leak through here.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("syntax error: {0}")]
    Parse(String),

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("type error: {0}")]
    Type(String),

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
            Error::Catalog(_) => 1146,     // ER_NO_SUCH_TABLE-ish bucket
            Error::Type(_) => 1366,        // ER_TRUNCATED_WRONG_VALUE
            Error::Unsupported(_) => 1235, // ER_NOT_SUPPORTED_YET
            _ => 1105,
        }
    }

    /// MySQL SQLSTATE string.
    pub fn sqlstate(&self) -> &'static [u8; 5] {
        match self {
            Error::Parse(_) => b"42000",
            Error::Catalog(_) => b"42S02",
            _ => b"HY000",
        }
    }
}
