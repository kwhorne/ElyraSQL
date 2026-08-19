//! Order-preserving ("memcomparable") key encoding, single- and multi-column.
//!
//! Rows are clustered on their key inside the ordered `redb` B-tree, so the
//! byte ordering of encoded keys must match SQL value order. Composite keys
//! concatenate per-component encodings; fixed-width components are inherently
//! self-delimiting, and text is escaped + terminated so it is too.

use elyra_core::{fold, Collation, Error, Result, Value};

/// Encode a composite key honoring each component's collation (case-sensitive
/// for `Bin`). `colls` is matched positionally; missing entries default to `Ci`.
pub fn encode_key_coll(values: &[Value], colls: &[Collation]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len() * 8);
    for (i, v) in values.iter().enumerate() {
        let c = colls.get(i).copied().unwrap_or(Collation::Ci);
        encode_component(v, c, &mut out)?;
    }
    Ok(out)
}

/// Encode selected columns from a row without first cloning them into a
/// temporary value vector. `columns` and `colls` are both in key order.
pub fn encode_columns_coll(
    row: &[Value],
    columns: &[usize],
    colls: &[Collation],
) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(columns.len() * 8);
    for (position, &column) in columns.iter().enumerate() {
        let value = row
            .get(column)
            .ok_or_else(|| Error::Storage("key column is outside the stored row".into()))?;
        let collation = colls.get(position).copied().unwrap_or(Collation::Ci);
        encode_component(value, collation, &mut out)?;
    }
    Ok(out)
}

/// Encode a single value (convenience for one-column keys/bounds).
pub fn encode(value: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(8);
    encode_component(value, Collation::Ci, &mut out)?;
    Ok(out)
}

/// Encode a single value with an explicit collation.
pub fn encode_coll(value: &Value, coll: Collation) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(8);
    encode_component(value, coll, &mut out)?;
    Ok(out)
}

fn encode_component(value: &Value, coll: Collation, out: &mut Vec<u8>) -> Result<()> {
    match value {
        // Flip the sign bit so negatives sort first; big-endian for order.
        Value::Int(i) => out.extend_from_slice(&(*i as u64 ^ 0x8000_0000_0000_0000).to_be_bytes()),
        // Unsigned: plain big-endian is already order-preserving (no sign bit).
        Value::UInt(u) => out.extend_from_slice(&u.to_be_bytes()),
        Value::Date(d) => out.extend_from_slice(&(*d as u32 ^ 0x8000_0000).to_be_bytes()),
        Value::DateTime(t) => {
            out.extend_from_slice(&(*t as u64 ^ 0x8000_0000_0000_0000).to_be_bytes())
        }
        Value::Decimal(u, _) => out.extend_from_slice(&(*u as u128 ^ (1u128 << 127)).to_be_bytes()),
        Value::Time(t) => out.extend_from_slice(&(*t as u64 ^ 0x8000_0000_0000_0000).to_be_bytes()),
        Value::Bool(b) => out.push(*b as u8),
        // Escape 0x00 as 0x00 0x01, terminate with 0x00 0x00 (< any 0x00 0x01),
        // making text self-delimiting while preserving byte order. Text is
        // case-folded so index/PK order and uniqueness match the default
        // case-insensitive collation.
        Value::Text(s) | Value::Json(s) => {
            let folded;
            let s: &str = if coll.is_bin() {
                s
            } else {
                folded = fold(s);
                &folded
            };
            for &b in s.as_bytes() {
                if b == 0x00 {
                    out.extend_from_slice(&[0x00, 0x01]);
                } else {
                    out.push(b);
                }
            }
            out.extend_from_slice(&[0x00, 0x00]);
        }
        other => {
            return Err(Error::Unsupported(format!(
                "value type cannot be used as a key: {other:?}"
            )))
        }
    }
    Ok(())
}

/// Encode a hidden rowid (tables without a primary key). Big-endian keeps
/// rowids in numeric/insertion order.
pub fn encode_rowid(rowid: u64) -> [u8; 8] {
    rowid.to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_column_encoding_matches_materialized_tuple_encoding() {
        let row = vec![
            Value::Text("ignored".into()),
            Value::Int(-7),
            Value::Text("Case".into()),
        ];
        let materialized = vec![row[2].clone(), row[1].clone()];
        let collations = [Collation::Bin, Collation::Ci];
        assert_eq!(
            encode_columns_coll(&row, &[2, 1], &collations).unwrap(),
            encode_key_coll(&materialized, &collations).unwrap()
        );
    }
}
