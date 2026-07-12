//! Projection-aware row decoding.
//!
//! Rows are stored as `bincode::serialize(&Vec<Value>)`. Scans that only need a
//! few columns (e.g. `COUNT(*)`, `GROUP BY age`, `WHERE age = 42`) waste time
//! fully deserialising every column of every row -- notably allocating a
//! `String` for each `TEXT`/`JSON` column that the query never looks at.
//!
//! [`decode_projected`] parses the same bincode stream but materialises only the
//! columns whose `needed` flag is set; unwanted columns are skipped in place
//! (their bytes advanced over, no allocation) and returned as [`Value::Null`]
//! placeholders. Callers must set `needed[i]` for every column referenced by the
//! filter, grouping, aggregate arguments or projection; the query planner does
//! this conservatively (any unrecognised expression forces a full decode).
//!
//! The decoder mirrors bincode 1.3's default layout (little-endian, fixed-int,
//! `u32` enum discriminant, `u64` length prefixes). The unit tests below
//! round-trip every [`Value`] variant against `bincode::serialize`, so any
//! future format or enum change is caught in CI rather than silently
//! mis-decoding data.

use elyra_core::{Error, Result, Value};

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.p + n > self.b.len() {
            return Err(Error::Storage("row decode: unexpected end of buffer".into()));
        }
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    fn u32(&mut self) -> Result<u32> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }
    fn u64(&mut self) -> Result<u64> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes(s.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }
}

/// Decode a bincode-encoded `Vec<Value>`, materialising only columns whose
/// `needed[i]` flag is set. Non-needed columns are returned as `Value::Null`.
/// `ncols` is the table's column count (used to validate the stored row).
pub fn decode_projected(bytes: &[u8], ncols: usize, needed: &[bool]) -> Result<Vec<Value>> {
    let mut c = Cur { b: bytes, p: 0 };
    let count = c.u64()? as usize;
    // If the stored arity differs from the schema (e.g. mid-migration rows),
    // fall back to a full decode for safety.
    if count != ncols {
        return bincode::deserialize::<Vec<Value>>(bytes).map_err(|e| Error::Storage(e.to_string()));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let tag = c.u32()?;
        let want = needed.get(i).copied().unwrap_or(true);
        let v = match tag {
            0 => Value::Null,
            1 => {
                let b = c.take(1)?[0];
                if want {
                    Value::Bool(b != 0)
                } else {
                    Value::Null
                }
            }
            2 => {
                let n = c.i64()?;
                if want {
                    Value::Int(n)
                } else {
                    Value::Null
                }
            }
            3 => {
                let bits = c.u64()?;
                if want {
                    Value::Float(f64::from_bits(bits))
                } else {
                    Value::Null
                }
            }
            4 | 11 => {
                let len = c.u64()? as usize;
                let s = c.take(len)?;
                if want {
                    let text = std::str::from_utf8(s)
                        .map_err(|_| Error::Storage("row decode: invalid utf8".into()))?
                        .to_string();
                    if tag == 4 {
                        Value::Text(text)
                    } else {
                        Value::Json(text)
                    }
                } else {
                    Value::Null
                }
            }
            5 => {
                let len = c.u64()? as usize;
                let s = c.take(len)?;
                if want {
                    Value::Bytes(s.to_vec())
                } else {
                    Value::Null
                }
            }
            6 => {
                let len = c.u64()? as usize;
                let s = c.take(len * 4)?;
                if want {
                    let mut v = Vec::with_capacity(len);
                    for k in 0..len {
                        let o = k * 4;
                        v.push(f32::from_le_bytes([s[o], s[o + 1], s[o + 2], s[o + 3]]));
                    }
                    Value::Vector(v)
                } else {
                    Value::Null
                }
            }
            7 => {
                let s = c.take(4)?;
                if want {
                    Value::Date(i32::from_le_bytes([s[0], s[1], s[2], s[3]]))
                } else {
                    Value::Null
                }
            }
            8 => {
                let n = c.i64()?;
                if want {
                    Value::DateTime(n)
                } else {
                    Value::Null
                }
            }
            9 => {
                let s = c.take(16)?;
                let unscaled = i128::from_le_bytes(s.try_into().unwrap());
                let scale = c.take(1)?[0];
                if want {
                    Value::Decimal(unscaled, scale)
                } else {
                    Value::Null
                }
            }
            10 => {
                let n = c.i64()?;
                if want {
                    Value::Time(n)
                } else {
                    Value::Null
                }
            }
            // Unknown variant tag: bail out to a full, authoritative decode.
            _ => {
                return bincode::deserialize::<Vec<Value>>(bytes)
                    .map_err(|e| Error::Storage(e.to_string()));
            }
        };
        out.push(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_variants() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(-42),
            Value::Float(3.5),
            Value::Text("hello".into()),
            Value::Bytes(vec![1, 2, 3, 255]),
            Value::Vector(vec![1.0, -2.0, 0.5]),
            Value::Date(20000),
            Value::DateTime(1_700_000_000_000_000),
            Value::Decimal(1050, 2),
            Value::Time(3_600_000_000),
            Value::Json("{\"a\":1}".into()),
        ]
    }

    #[test]
    fn full_decode_matches_bincode() {
        let row = all_variants();
        let bytes = bincode::serialize(&row).unwrap();
        let needed = vec![true; row.len()];
        let got = decode_projected(&bytes, row.len(), &needed).unwrap();
        assert_eq!(got, row);
    }

    #[test]
    fn projected_decode_skips_unwanted() {
        let row = all_variants();
        let bytes = bincode::serialize(&row).unwrap();
        // Keep only odd indices; even indices must come back as Null.
        let needed: Vec<bool> = (0..row.len()).map(|i| i % 2 == 1).collect();
        let got = decode_projected(&bytes, row.len(), &needed).unwrap();
        for (i, (g, orig)) in got.iter().zip(row.iter()).enumerate() {
            if i % 2 == 1 {
                assert_eq!(g, orig, "kept column {i} must match");
            } else {
                assert_eq!(*g, Value::Null, "skipped column {i} must be Null");
            }
        }
    }

    #[test]
    fn arity_mismatch_falls_back() {
        let row = all_variants();
        let bytes = bincode::serialize(&row).unwrap();
        // Ask for the wrong column count -> full decode fallback (all present).
        let got = decode_projected(&bytes, row.len() + 1, &[false]).unwrap();
        assert_eq!(got, row);
    }
}
