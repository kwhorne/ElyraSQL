//! ElyraSQL core: shared types, errors and value model.
//!
//! Everything user-facing is branded ElyraSQL. Internal engines
//! (storage, query, analytics) depend on this crate for a common
//! vocabulary of values, types and errors.

pub mod error;
pub mod types;
pub mod value;

pub use error::{Error, Result};
pub use types::{ColumnType, ColumnDef, Schema};
pub use value::Value;

use serde::{Deserialize, Serialize};

/// Access level granted to a connection. Ordered: `Read` < `Write` < `Admin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Privilege {
    /// SELECT and session commands only.
    Read,
    /// Plus INSERT / UPDATE / DELETE.
    Write,
    /// Plus DDL (CREATE / DROP / CREATE INDEX).
    Admin,
}

/// The product name, used in banners, wire handshakes and logs.
pub const PRODUCT_NAME: &str = "ElyraSQL";

/// Server version reported over the wire (MySQL-compatible string).
///
/// Format: `<major>.<minor>.<patch>-ElyraSQL`. MySQL clients parse the
/// leading `x.y.z`, so we prefix a MySQL-looking version and tag the rest.
pub const SERVER_VERSION: &str = "8.0.0-ElyraSQL-0.1.0";
