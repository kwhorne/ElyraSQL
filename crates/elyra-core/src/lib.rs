//! ElyraSQL core: shared types, errors and value model.
//!
//! Everything user-facing is branded ElyraSQL. Internal engines
//! (storage, query, analytics) depend on this crate for a common
//! vocabulary of values, types and errors.

pub mod datetime;
pub mod error;
pub mod json;
pub mod types;
pub mod users;
pub mod value;

pub use error::{Error, Result};
pub use types::{Collation, ColumnDef, ColumnType, Schema};
pub use value::{canonical_f64_bits, fold, Value};

/// Upper bound (in bytes) on any single length-prefixed frame or record read
/// from the network (cluster/replication), the binlog, or a spill file, before
/// the buffer is allocated. Rejecting oversized lengths turns a corrupt file or
/// a malicious/garbled packet into an error instead of an out-of-memory crash.
///
/// Configurable via `ELYRASQL_MAX_FRAME_MB` (default 1024 MiB). It must be at
/// least as large as the biggest transaction that will be replicated or logged
/// (see `ELYRASQL_TXN_MAX_BYTES`); deployments that never use large
/// transactions can lower it (e.g. 64) for tighter denial-of-service defence.
pub fn max_frame_bytes() -> usize {
    use std::sync::OnceLock;
    static CACHE: OnceLock<usize> = OnceLock::new();
    *CACHE.get_or_init(|| {
        let mb = std::env::var("ELYRASQL_MAX_FRAME_MB")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&m| m >= 1)
            .unwrap_or(1024);
        mb.saturating_mul(1 << 20)
    })
}

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
pub const SERVER_VERSION: &str = "8.0.0-ElyraSQL-1.3.0";
