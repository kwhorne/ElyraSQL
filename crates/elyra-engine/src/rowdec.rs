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
            return Err(Error::Storage(
                "row decode: unexpected end of buffer".into(),
            ));
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
    let mut out = Vec::with_capacity(ncols);
    decode_projected_into(bytes, ncols, needed, &mut out)?;
    Ok(out)
}

/// Decode every column of a stored row.
///
/// Equivalent to `bincode::deserialize::<Vec<Value>>`, and the same decoder the
/// projected path uses, so the two cannot disagree about a value encoding. Falls
/// back to `bincode` for anything it does not recognise, which keeps `bincode`
/// authoritative for the format.
pub fn decode_row(bytes: &[u8]) -> Result<Vec<Value>> {
    let mut c = Cur { b: bytes, p: 0 };
    let Ok(count) = c.u64() else {
        return bincode::deserialize(bytes).map_err(|e| Error::Storage(e.to_string()));
    };
    let ncols = count as usize;
    let mut out = Vec::with_capacity(ncols.min(1024));
    match decode_cols(&mut c, ncols, None, &mut out) {
        Ok(true) => Ok(out),
        // Unknown tag or truncated stream: let bincode decide (and report).
        Ok(false) | Err(_) => {
            bincode::deserialize(bytes).map_err(|e| Error::Storage(e.to_string()))
        }
    }
}

/// Like [`decode_projected`] but decodes into a caller-owned buffer (cleared
/// first), so a scan can reuse one allocation across millions of rows.
pub fn decode_projected_into(
    bytes: &[u8],
    ncols: usize,
    needed: &[bool],
    out: &mut Vec<Value>,
) -> Result<()> {
    out.clear();
    let mut c = Cur { b: bytes, p: 0 };
    let count = c.u64()? as usize;
    // If the stored arity differs from the schema (e.g. mid-migration rows),
    // fall back to a full decode for safety.
    if count != ncols {
        *out =
            bincode::deserialize::<Vec<Value>>(bytes).map_err(|e| Error::Storage(e.to_string()))?;
        return Ok(());
    }
    if !decode_cols(&mut c, count, Some(needed), out)? {
        *out =
            bincode::deserialize::<Vec<Value>>(bytes).map_err(|e| Error::Storage(e.to_string()))?;
    }
    Ok(())
}

/// Decode `count` columns from `c`, materialising those `needed` marks (or all of
/// them when it is `None`). Shared by the projected and the full decoder so the
/// two can never drift apart on a value encoding.
///
/// Returns `false` for a value tag it does not know, meaning the caller must fall
/// back to `bincode`, which is the authoritative decoder; `out` is then undefined
/// and must be discarded.
fn decode_cols(
    c: &mut Cur<'_>,
    count: usize,
    needed: Option<&[bool]>,
    out: &mut Vec<Value>,
) -> Result<bool> {
    for i in 0..count {
        let tag = c.u32()?;
        let want = needed.is_none_or(|n| n.get(i).copied().unwrap_or(true));
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
            // `UInt` (BIGINT UNSIGNED). Missing before, so every row of a table
            // with such a column took the full `bincode` fallback.
            12 => {
                let n = c.u64()?;
                if want {
                    Value::UInt(n)
                } else {
                    Value::Null
                }
            }
            // Unknown variant tag: bail out to the authoritative decoder.
            _ => return Ok(false),
        };
        out.push(v);
    }
    Ok(true)
}

/// One column's values extracted for the vectorised aggregation paths, typed by
/// the column: an integer column keeps its values as `i64` so its aggregates
/// stay exact past 2^53 (see `elyra_olap::NumSlot`); a float column as `f64`.
#[derive(Debug, Clone, PartialEq)]
pub enum NumBuf {
    Int(Vec<i64>),
    Float(Vec<f64>),
}

impl NumBuf {
    pub fn new(is_int: bool) -> Self {
        if is_int {
            NumBuf::Int(Vec::new())
        } else {
            NumBuf::Float(Vec::new())
        }
    }

    fn len(&self) -> usize {
        match self {
            NumBuf::Int(v) => v.len(),
            NumBuf::Float(v) => v.len(),
        }
    }

    fn truncate(&mut self, n: usize) {
        match self {
            NumBuf::Int(v) => v.truncate(n),
            NumBuf::Float(v) => v.truncate(n),
        }
    }

    pub fn clear(&mut self) {
        self.truncate(0);
    }

    /// Append an integer. A float column widens it, as its coercion would.
    #[inline]
    fn push_int(&mut self, n: i64) {
        match self {
            NumBuf::Int(v) => v.push(n),
            NumBuf::Float(v) => v.push(n as f64),
        }
    }

    /// Append an unsigned integer; `false` if it does not fit an integer
    /// column's `i64` exactly.
    #[inline]
    fn push_uint(&mut self, n: u64) -> bool {
        match self {
            NumBuf::Int(v) => match i64::try_from(n) {
                Ok(i) => {
                    v.push(i);
                    true
                }
                Err(_) => false,
            },
            NumBuf::Float(v) => {
                v.push(n as f64);
                true
            }
        }
    }

    /// Append a float; `false` for an integer column, which cannot hold a
    /// fraction exactly (the caller then uses the general path).
    #[inline]
    fn push_float(&mut self, f: f64) -> bool {
        match self {
            NumBuf::Float(v) => {
                v.push(f);
                true
            }
            NumBuf::Int(_) => false,
        }
    }

    /// Append a decoded value by its own variant. NULL and non-numeric values
    /// are not pushed. `false` if it cannot be kept exactly (see above).
    fn push_value(&mut self, v: Option<&Value>) -> bool {
        match v {
            Some(Value::Int(n)) => {
                self.push_int(*n);
                true
            }
            Some(Value::Bool(b)) => {
                self.push_int(i64::from(*b));
                true
            }
            Some(Value::UInt(n)) => self.push_uint(*n),
            Some(Value::Float(f)) => self.push_float(*f),
            _ => true,
        }
    }
}

/// Extract the numeric (`Int`/`Float`/`Bool`/`UInt`) values of selected
/// columns from a bincode row into per-column typed buffers, in a single walk.
/// `slot_of[col]` is the destination buffer index, or `-1` to skip. NULL and
/// non-numeric values are not pushed, so each buffer holds that column's present
/// values. This is the row-to-columnar step of the vectorised aggregation path.
///
/// Returns `Ok(false)` if a value cannot be kept exactly in its buffer (a
/// fraction, or an unsigned value past `i64::MAX`, in an integer column); the
/// caller must then answer through the general path. The column's own coercion
/// means that does not happen for well-formed rows.
///
/// A value tag the fast walk does not know falls back to the authoritative
/// `bincode` decoder. The values this row already pushed are rolled back first:
/// they used to stay, so the fallback counted them a second time.
pub fn extract_numeric_cols(
    bytes: &[u8],
    ncols: usize,
    slot_of: &[i32],
    bufs: &mut [NumBuf],
) -> Result<bool> {
    let mut c = Cur { b: bytes, p: 0 };
    let count = c.u64()? as usize;
    if count != ncols {
        return push_decoded(bytes, slot_of, bufs);
    }
    // Buffers this row has pushed to, so an unknown tag can undo them. Each
    // buffer gains at most one value per row; a row with more than 64 extracted
    // columns records the lengths instead.
    let mut pushed: u64 = 0;
    let wide = bufs.len() > 64;
    let lens: Vec<usize> = if wide {
        bufs.iter().map(NumBuf::len).collect()
    } else {
        Vec::new()
    };
    for i in 0..count {
        let tag = c.u32()?;
        let slot = slot_of.get(i).copied().unwrap_or(-1);
        match tag {
            0 => {} // NULL -> not pushed
            1 => {
                let b = c.take(1)?[0];
                if slot >= 0 {
                    bufs[slot as usize].push_int(i64::from(b != 0));
                    if !wide {
                        pushed |= 1u64 << slot;
                    }
                }
            }
            2 => {
                let n = c.i64()?;
                if slot >= 0 {
                    bufs[slot as usize].push_int(n);
                    if !wide {
                        pushed |= 1u64 << slot;
                    }
                }
            }
            3 => {
                let bits = c.u64()?;
                if slot >= 0 {
                    if !bufs[slot as usize].push_float(f64::from_bits(bits)) {
                        return Ok(false);
                    }
                    if !wide {
                        pushed |= 1u64 << slot;
                    }
                }
            }
            4 | 5 | 11 => {
                let len = c.u64()? as usize;
                c.take(len)?;
            }
            6 => {
                let len = c.u64()? as usize;
                c.take(len * 4)?;
            }
            7 => {
                c.take(4)?;
            }
            8 | 10 => {
                c.i64()?;
            }
            9 => {
                c.take(16)?;
                c.take(1)?;
            }
            // `UInt` (BIGINT UNSIGNED). Unknown here before, so every row of a
            // table with such a column went to the fallback below.
            12 => {
                let n = c.u64()?;
                if slot >= 0 {
                    if !bufs[slot as usize].push_uint(n) {
                        return Ok(false);
                    }
                    if !wide {
                        pushed |= 1u64 << slot;
                    }
                }
            }
            _ => {
                if wide {
                    for (b, &n) in bufs.iter_mut().zip(&lens) {
                        b.truncate(n);
                    }
                } else {
                    for (s, b) in bufs.iter_mut().enumerate() {
                        if pushed & (1 << s) != 0 {
                            b.truncate(b.len() - 1);
                        }
                    }
                }
                return push_decoded(bytes, slot_of, bufs);
            }
        }
    }
    Ok(true)
}

/// Decode the whole row with `bincode` and push each selected column's value.
fn push_decoded(bytes: &[u8], slot_of: &[i32], bufs: &mut [NumBuf]) -> Result<bool> {
    let row =
        bincode::deserialize::<Vec<Value>>(bytes).map_err(|e| Error::Storage(e.to_string()))?;
    for (col, s) in slot_of.iter().enumerate() {
        if *s >= 0 && !bufs[*s as usize].push_value(row.get(col)) {
            return Ok(false);
        }
    }
    Ok(true)
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
            Value::UInt(u64::MAX),
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

    fn extract(row: &[Value], slot_of: &[i32], kinds: &[bool]) -> (bool, Vec<NumBuf>) {
        let bytes = bincode::serialize(&row.to_vec()).unwrap();
        let mut bufs: Vec<NumBuf> = kinds.iter().map(|&k| NumBuf::new(k)).collect();
        let ok = extract_numeric_cols(&bytes, row.len(), slot_of, &mut bufs).unwrap();
        (ok, bufs)
    }

    /// The regression: a `BIGINT UNSIGNED` column after an extracted one used to
    /// send the row to the fallback, which pushed the earlier column again, so
    /// `SUM(a), COUNT(*)` on such a table came back doubled.
    #[test]
    fn an_unsigned_column_does_not_double_the_columns_before_it() {
        let row = [Value::Int(1), Value::Int(10), Value::UInt(u64::MAX)];
        let (ok, bufs) = extract(&row, &[-1, 0, -1], &[true]);
        assert!(ok);
        assert_eq!(bufs, vec![NumBuf::Int(vec![10])]);
    }

    #[test]
    fn integers_are_extracted_exactly_past_2_pow_53() {
        let big = 9_007_199_254_740_993i64;
        let (ok, bufs) = extract(
            &[Value::Int(big), Value::Float(2.5)],
            &[0, 1],
            &[true, false],
        );
        assert!(ok);
        assert_eq!(bufs, vec![NumBuf::Int(vec![big]), NumBuf::Float(vec![2.5])]);
    }

    #[test]
    fn a_value_an_integer_buffer_cannot_hold_exactly_is_refused() {
        // A fraction, and an unsigned value past i64::MAX.
        assert!(!extract(&[Value::Float(1.5)], &[0], &[true]).0);
        assert!(!extract(&[Value::UInt(u64::MAX)], &[0], &[true]).0);
        // Both fit a float buffer, and an in-range unsigned fits an integer one.
        let (ok, bufs) = extract(&[Value::UInt(u64::MAX)], &[0], &[false]);
        assert!(ok);
        assert_eq!(bufs, vec![NumBuf::Float(vec![u64::MAX as f64])]);
        let (ok, bufs) = extract(&[Value::UInt(7)], &[0], &[true]);
        assert!(ok);
        assert_eq!(bufs, vec![NumBuf::Int(vec![7])]);
    }

    #[test]
    fn nulls_and_non_numeric_values_are_not_pushed() {
        let row = [Value::Null, Value::Text("x".into()), Value::Bool(true)];
        let (ok, bufs) = extract(&row, &[0, 1, 2], &[true, true, true]);
        assert!(ok);
        assert_eq!(
            bufs,
            vec![
                NumBuf::Int(vec![]),
                NumBuf::Int(vec![]),
                NumBuf::Int(vec![1])
            ]
        );
    }

    /// A row written before a column was added has fewer columns than the
    /// schema; it is decoded by `bincode` and pushed exactly once.
    #[test]
    fn a_short_row_is_decoded_once() {
        let row = vec![Value::Int(5), Value::UInt(9)];
        let bytes = bincode::serialize(&row).unwrap();
        let mut bufs = vec![NumBuf::new(true), NumBuf::new(true)];
        let ok = extract_numeric_cols(&bytes, 3, &[0, 1, -1], &mut bufs).unwrap();
        assert!(ok);
        assert_eq!(bufs, vec![NumBuf::Int(vec![5]), NumBuf::Int(vec![9])]);
    }
}
