//! Order-preserving ("memcomparable") key encoding.
//!
//! ElyraSQL stores rows clustered on their key inside the ordered `redb`
//! B-tree, so the byte ordering of encoded keys must match SQL value order.
//! That lets PK point-lookups and range scans ride the B-tree directly.

use elyra_core::{Error, Result, Value};

/// Encode a clustered-index key value into order-preserving bytes.
pub fn encode(value: &Value) -> Result<Vec<u8>> {
    Ok(match value {
        // Flip the sign bit so negatives sort before positives, big-endian
        // so lexicographic byte order equals numeric order.
        Value::Int(i) => (*i as u64 ^ 0x8000_0000_0000_0000).to_be_bytes().to_vec(),
        // UTF-8 bytes already sort correctly for a single trailing key.
        Value::Text(s) => s.as_bytes().to_vec(),
        Value::Bool(b) => vec![*b as u8],
        other => {
            return Err(Error::Unsupported(format!(
                "value type cannot be used as a primary key: {other:?}"
            )))
        }
    })
}

/// Encode a hidden rowid (used when a table has no primary key). Big-endian
/// so rowids iterate in insertion/numeric order.
pub fn encode_rowid(rowid: u64) -> [u8; 8] {
    rowid.to_be_bytes()
}
