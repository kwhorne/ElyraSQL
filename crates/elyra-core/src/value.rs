//! Runtime value model shared across all engines.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// Case-fold text for the default case-insensitive collation. Applied at every
/// point text is compared, ordered, indexed, grouped, or de-duplicated, so all
/// paths agree that e.g. `'Foo' = 'foo'`.
pub fn fold(s: &str) -> String {
    if s.is_ascii() {
        let mut o = s.to_string();
        o.make_ascii_lowercase();
        o
    } else {
        s.to_lowercase()
    }
}

/// Case-insensitive ordering of two strings under the default collation, with
/// an allocation-free fast path for the common all-ASCII case.
pub fn fold_cmp(a: &str, b: &str) -> Ordering {
    if a.is_ascii() && b.is_ascii() {
        let (ab, bb) = (a.as_bytes(), b.as_bytes());
        let n = ab.len().min(bb.len());
        for i in 0..n {
            let x = ab[i].to_ascii_lowercase();
            let y = bb[i].to_ascii_lowercase();
            if x != y {
                return x.cmp(&y);
            }
        }
        ab.len().cmp(&bb.len())
    } else {
        a.to_lowercase().cmp(&b.to_lowercase())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Vector(Vec<f32>),
    /// DATE: days since 1970-01-01.
    Date(i32),
    /// DATETIME/TIMESTAMP: microseconds since the Unix epoch.
    DateTime(i64),
    /// DECIMAL: unscaled integer value and its scale (digits after the point).
    Decimal(i128, u8),
    /// TIME: microseconds since midnight.
    Time(i64),
    /// JSON document (stored as its text form).
    Json(String),
    /// 64-bit **unsigned** integer (MySQL `BIGINT UNSIGNED`). Kept distinct from
    /// `Int` so values above `i64::MAX` (e.g. bitwise results, large unsigned
    /// columns) are represented and displayed correctly. Added last so existing
    /// bincode-encoded rows (which never contain it) still decode.
    UInt(u64),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Numeric view for arithmetic/aggregation (Int/Float/Bool/Decimal).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::UInt(u) => Some(*u as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::Decimal(u, s) => Some(*u as f64 / 10f64.powi(*s as i32)),
            _ => None,
        }
    }

    /// SQL comparison with implicit cross-type coercion. `None` when either
    /// operand is NULL (three-valued logic) or the types are incomparable.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        self.compare_coll(other, crate::Collation::Ci)
    }

    /// Compare under an explicit text collation (`Bin` = case-sensitive).
    pub fn compare_coll(&self, other: &Value, coll: crate::Collation) -> Option<Ordering> {
        use Value::*;
        if self.is_null() || other.is_null() {
            return None;
        }
        // Case-sensitive text comparison for the binary collation.
        if coll.is_bin() {
            match (self, other) {
                (Text(a), Text(b))
                | (Json(a), Json(b))
                | (Json(a), Text(b))
                | (Text(b), Json(a)) => {
                    return Some(a.cmp(b));
                }
                _ => {}
            }
        }
        match (self, other) {
            (Date(_), _) | (_, Date(_)) => {
                to_days(self).zip(to_days(other)).map(|(a, b)| a.cmp(&b))
            }
            (DateTime(_), _) | (_, DateTime(_)) => to_micros(self)
                .zip(to_micros(other))
                .map(|(a, b)| a.cmp(&b)),
            (Decimal(au, asc), Decimal(bu, bsc)) => Some(cmp_decimal(*au, *asc, *bu, *bsc)),
            (Decimal(..), _) | (_, Decimal(..)) => coerce_f64(self)
                .zip(coerce_f64(other))
                .and_then(|(a, b)| a.partial_cmp(&b)),
            (Time(_), _) | (_, Time(_)) => to_micros_of_day(self)
                .zip(to_micros_of_day(other))
                .map(|(a, b)| a.cmp(&b)),
            // Text compares under the default case-insensitive collation.
            (Text(a), Text(b)) => Some(fold_cmp(a, b)),
            (Json(a), Json(b)) => Some(fold_cmp(a, b)),
            (Json(a), Text(b)) | (Text(b), Json(a)) => Some(fold_cmp(a, b)),
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            (Bytes(a), Bytes(b)) => Some(a.cmp(b)),
            // Exact integer comparisons (avoid f64 precision loss above 2^53).
            (UInt(a), UInt(b)) => Some(a.cmp(b)),
            (Int(a), Int(b)) => Some(a.cmp(b)),
            (UInt(a), Int(b)) => Some(cmp_u64_i64(*a, *b)),
            (Int(a), UInt(b)) => Some(cmp_u64_i64(*b, *a).reverse()),
            // Mixed numeric/string comparison coerces the string to a number
            // (MySQL implicit conversion), so `int_col = '5'` and bound
            // parameters rendered as string literals match numeric columns.
            _ => coerce_f64(self)
                .zip(coerce_f64(other))
                .and_then(|(a, b)| a.partial_cmp(&b)),
        }
    }

    /// A canonical, type-tagged, collation-folded key for grouping, DISTINCT,
    /// hash joins, and set-operation de-duplication. Two values that compare
    /// equal under the default collation produce the same key; values of
    /// different types never collide.
    pub fn collation_key(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.push_collation_key(&mut out);
        out
    }

    /// Append this value's collation key to `out` (self-delimiting).
    pub fn push_collation_key(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(0),
            Value::Int(i) => {
                out.push(1);
                out.extend_from_slice(&i.to_le_bytes());
            }
            Value::Bool(b) => {
                out.push(2);
                out.push(*b as u8);
            }
            Value::Float(f) => {
                out.push(3);
                out.extend_from_slice(&canonical_f64_bits(*f).to_le_bytes());
            }
            Value::Text(s) | Value::Json(s) => {
                out.push(4);
                let f = fold(s);
                out.extend_from_slice(&(f.len() as u32).to_le_bytes());
                out.extend_from_slice(f.as_bytes());
            }
            // UInt that fits in i64 keys identically to Int (so 5 == 5 across
            // signedness for GROUP BY/DISTINCT); larger values get their own tag.
            Value::UInt(u) if *u <= i64::MAX as u64 => {
                out.push(1);
                out.extend_from_slice(&(*u as i64).to_le_bytes());
            }
            Value::UInt(u) => {
                out.push(6);
                out.extend_from_slice(&u.to_le_bytes());
            }
            other => {
                out.push(5);
                let s = format!("{other:?}");
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
        }
    }

    /// Collation key for a whole row (self-delimiting concatenation).
    pub fn row_collation_key(row: &[Value]) -> Vec<u8> {
        let mut out = Vec::with_capacity(row.len() * 9);
        for v in row {
            v.push_collation_key(&mut out);
        }
        out
    }

    /// Append this value's collation key under an explicit text collation. Under
    /// `Bin`, text/JSON is keyed by its exact bytes (case-sensitive) instead of
    /// case-folded, so `GROUP BY`/`DISTINCT` on a `_bin` column distinguishes
    /// case. Non-text values are unaffected. The tag bytes match
    /// [`push_collation_key`], so keys are only ever compared within one column
    /// (which uses a single collation), never across.
    pub fn push_collation_key_coll(&self, out: &mut Vec<u8>, coll: crate::Collation) {
        match self {
            Value::Text(s) | Value::Json(s) if coll.is_bin() => {
                out.push(4);
                out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
            }
            _ => self.push_collation_key(out),
        }
    }

    /// Collation key for a single value under an explicit collation.
    pub fn collation_key_coll(&self, coll: crate::Collation) -> Vec<u8> {
        let mut out = Vec::new();
        self.push_collation_key_coll(&mut out, coll);
        out
    }

    /// Collation key for a whole row, one collation per column (missing entries
    /// default to case-insensitive).
    pub fn row_collation_key_coll(row: &[Value], colls: &[crate::Collation]) -> Vec<u8> {
        let mut out = Vec::with_capacity(row.len() * 9);
        for (i, v) in row.iter().enumerate() {
            let c = colls.get(i).copied().unwrap_or(crate::Collation::Ci);
            v.push_collation_key_coll(&mut out, c);
        }
        out
    }

    /// Total order under an explicit text collation (NULL sorts first, then
    /// `compare_coll`). Under `Bin`, text compares case-sensitively.
    pub fn total_cmp_coll(&self, other: &Value, coll: crate::Collation) -> Ordering {
        match (self.is_null(), other.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => self.compare_coll(other, coll).unwrap_or_else(|| {
                match (self.as_f64(), other.as_f64()) {
                    (Some(a), Some(b)) => a.total_cmp(&b),
                    _ => self.type_rank().cmp(&other.type_rank()),
                }
            }),
        }
    }

    /// Total order for sorting/extremes: NULL sorts first, then `compare`.
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        match (self.is_null(), other.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => self.compare(other).unwrap_or_else(|| {
                // Incomparable under `compare` (e.g. NaN, or two unrelated
                // types). Use a deterministic, allocation-free total order
                // rather than comparing Debug strings (which allocated on every
                // comparison and sorted numbers lexicographically, e.g. 10 < 2).
                match (self.as_f64(), other.as_f64()) {
                    // Both numeric (covers NaN via IEEE total order).
                    (Some(a), Some(b)) => a.total_cmp(&b),
                    // Otherwise order by a stable per-variant rank.
                    _ => self.type_rank().cmp(&other.type_rank()),
                }
            }),
        }
    }

    /// Stable ordering rank per value variant, used only as the final tiebreak
    /// in `total_cmp` for otherwise-incomparable values.
    fn type_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,
            Value::UInt(_) => 2,
            Value::Decimal(..) => 3,
            Value::Float(_) => 4,
            Value::Date(_) => 5,
            Value::Time(_) => 6,
            Value::DateTime(_) => 7,
            Value::Text(_) => 8,
            Value::Json(_) => 9,
            Value::Bytes(_) => 10,
            Value::Vector(_) => 11,
        }
    }

    /// Render as a MySQL text-protocol column value.
    pub fn to_wire_string(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Bool(b) => Some(if *b { "1".into() } else { "0".into() }),
            Value::Int(i) => Some(i.to_string()),
            Value::UInt(u) => Some(u.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Text(s) => Some(s.clone()),
            Value::Bytes(b) => Some(String::from_utf8_lossy(b).into_owned()),
            Value::Vector(v) => {
                let inner = v
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                Some(format!("[{inner}]"))
            }
            Value::Date(d) => Some(crate::datetime::format_date(*d)),
            Value::DateTime(t) => Some(crate::datetime::format_datetime(*t)),
            Value::Decimal(units, scale) => Some(format_decimal(*units, *scale)),
            Value::Time(t) => Some(crate::datetime::format_time(*t)),
            Value::Json(s) => Some(s.clone()),
        }
    }
}

/// Canonical bit pattern of an `f64` for hashing/grouping keys. Collapses
/// `-0.0` and `+0.0` (which have different sign bits but compare equal in SQL)
/// to the same key, and maps every NaN to one canonical NaN so that grouping,
/// DISTINCT, and hash joins treat equal floats as equal.
pub fn canonical_f64_bits(f: f64) -> u64 {
    if f == 0.0 {
        0 // both +0.0 and -0.0
    } else if f.is_nan() {
        f64::NAN.to_bits() // canonical quiet NaN
    } else {
        f.to_bits()
    }
}

/// Numeric value for comparison: the native numeric types, plus a numeric
/// string parsed to a number (MySQL coerces strings in numeric comparisons).
fn coerce_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Text(s) | Value::Json(s) => s.trim().parse::<f64>().ok(),
        _ => v.as_f64(),
    }
}

/// Exact ordering of a `u64` against an `i64` (no f64 rounding).
fn cmp_u64_i64(a: u64, b: i64) -> Ordering {
    if b < 0 {
        Ordering::Greater // any u64 >= 0 > any negative i64
    } else {
        a.cmp(&(b as u64))
    }
}

fn to_days(v: &Value) -> Option<i64> {
    match v {
        Value::Date(d) => Some(*d as i64),
        Value::DateTime(m) => Some(m.div_euclid(86_400_000_000)),
        Value::Int(i) => Some(*i),
        Value::Text(s) => crate::datetime::parse_date(s)
            .map(|d| d as i64)
            .or_else(|| crate::datetime::parse_datetime(s).map(|m| m.div_euclid(86_400_000_000))),
        _ => None,
    }
}

fn to_micros(v: &Value) -> Option<i64> {
    match v {
        Value::DateTime(t) => Some(*t),
        Value::Date(d) => Some(*d as i64 * 86_400 * 1_000_000),
        Value::Int(i) => Some(*i),
        Value::Text(s) => crate::datetime::parse_datetime(s),
        _ => None,
    }
}

fn to_micros_of_day(v: &Value) -> Option<i64> {
    match v {
        Value::Time(t) => Some(*t),
        Value::Text(s) => crate::datetime::parse_time(s),
        _ => None,
    }
}

fn cmp_decimal(au: i128, asc: u8, bu: i128, bsc: u8) -> Ordering {
    let s = asc.max(bsc);
    let a = au.saturating_mul(10i128.saturating_pow((s - asc) as u32));
    let b = bu.saturating_mul(10i128.saturating_pow((s - bsc) as u32));
    a.cmp(&b)
}

/// Maximum JSON nesting depth accepted by the validator. Bounds recursion so
/// pathologically nested input (`[[[[...]]]]`) is rejected instead of
/// overflowing the thread stack. Comfortably above MySQL's documented depth
/// limit (100).
const MAX_JSON_DEPTH: u32 = 200;

/// Minimal JSON validator (structure only, no dependency).
pub fn is_valid_json(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut pos = 0;
    skip_ws(bytes, &mut pos);
    if !parse_json_value(bytes, &mut pos, 0) {
        return false;
    }
    skip_ws(bytes, &mut pos);
    pos == bytes.len()
}

fn skip_ws(b: &[u8], p: &mut usize) {
    while *p < b.len() && matches!(b[*p], b' ' | b'\t' | b'\n' | b'\r') {
        *p += 1;
    }
}

fn parse_json_value(b: &[u8], p: &mut usize, depth: u32) -> bool {
    skip_ws(b, p);
    if *p >= b.len() {
        return false;
    }
    match b[*p] {
        b'{' => parse_json_object(b, p, depth),
        b'[' => parse_json_array(b, p, depth),
        b'"' => parse_json_string(b, p),
        b't' => consume(b, p, "true"),
        b'f' => consume(b, p, "false"),
        b'n' => consume(b, p, "null"),
        _ => parse_json_number(b, p),
    }
}

fn consume(b: &[u8], p: &mut usize, lit: &str) -> bool {
    if b[*p..].starts_with(lit.as_bytes()) {
        *p += lit.len();
        true
    } else {
        false
    }
}

fn parse_json_string(b: &[u8], p: &mut usize) -> bool {
    if b.get(*p) != Some(&b'"') {
        return false;
    }
    *p += 1;
    while *p < b.len() {
        match b[*p] {
            b'"' => {
                *p += 1;
                return true;
            }
            b'\\' => *p += 2,
            _ => *p += 1,
        }
    }
    false
}

fn parse_json_number(b: &[u8], p: &mut usize) -> bool {
    let start = *p;
    while *p < b.len() && matches!(b[*p], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E') {
        *p += 1;
    }
    *p > start && s_from(b, start, *p).parse::<f64>().is_ok()
}

fn s_from(b: &[u8], a: usize, z: usize) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(&b[a..z])
}

fn parse_json_array(b: &[u8], p: &mut usize, depth: u32) -> bool {
    if depth >= MAX_JSON_DEPTH {
        return false;
    }
    *p += 1; // [
    skip_ws(b, p);
    if b.get(*p) == Some(&b']') {
        *p += 1;
        return true;
    }
    loop {
        if !parse_json_value(b, p, depth + 1) {
            return false;
        }
        skip_ws(b, p);
        match b.get(*p) {
            Some(b',') => {
                *p += 1;
            }
            Some(b']') => {
                *p += 1;
                return true;
            }
            _ => return false,
        }
    }
}

fn parse_json_object(b: &[u8], p: &mut usize, depth: u32) -> bool {
    if depth >= MAX_JSON_DEPTH {
        return false;
    }
    *p += 1; // {
    skip_ws(b, p);
    if b.get(*p) == Some(&b'}') {
        *p += 1;
        return true;
    }
    loop {
        skip_ws(b, p);
        if !parse_json_string(b, p) {
            return false;
        }
        skip_ws(b, p);
        if b.get(*p) != Some(&b':') {
            return false;
        }
        *p += 1;
        if !parse_json_value(b, p, depth + 1) {
            return false;
        }
        skip_ws(b, p);
        match b.get(*p) {
            Some(b',') => {
                *p += 1;
            }
            Some(b'}') => {
                *p += 1;
                return true;
            }
            _ => return false,
        }
    }
}

/// Render an unscaled decimal with its scale, e.g. `(12345, 2)` -> `123.45`.
pub fn format_decimal(units: i128, scale: u8) -> String {
    if scale == 0 {
        return units.to_string();
    }
    let neg = units < 0;
    let digits = units.unsigned_abs().to_string();
    let scale = scale as usize;
    let s = if digits.len() <= scale {
        format!("0.{:0>width$}", digits, width = scale)
    } else {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    };
    if neg {
        format!("-{s}")
    } else {
        s
    }
}

/// Parse a decimal string into `(unscaled, scale)` at the given target scale.
pub fn parse_decimal(s: &str, target_scale: u8) -> Option<(i128, u8)> {
    let s = s.trim();
    let (neg, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, ""),
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let ts = target_scale as usize;
    let frac = if frac_part.len() >= ts {
        frac_part[..ts].to_string()
    } else {
        format!("{:0<width$}", frac_part, width = ts)
    };
    let combined = format!("{}{}", int_part, frac);
    let mut v: i128 = combined.parse().ok()?;
    if neg {
        v = -v;
    }
    Some((v, target_scale))
}

#[cfg(test)]
mod collation_tests {
    use super::*;

    #[test]
    fn text_compares_case_insensitively() {
        assert_eq!(
            Value::Text("Foo".into()).compare(&Value::Text("foo".into())),
            Some(Ordering::Equal)
        );
        assert_eq!(fold_cmp("Apple", "apple"), Ordering::Equal);
        assert_eq!(fold_cmp("apple", "Banana"), Ordering::Less);
        assert_eq!(fold_cmp("ÆØÅ", "æøå"), Ordering::Equal);
    }

    #[test]
    fn collation_key_folds_and_tags() {
        assert_eq!(
            Value::Text("Foo".into()).collation_key(),
            Value::Text("foo".into()).collation_key()
        );
        assert_ne!(
            Value::Text("5".into()).collation_key(),
            Value::Int(5).collation_key()
        );
    }

    #[test]
    fn signed_zero_floats_group_together() {
        // -0.0 and +0.0 are equal in SQL and must produce the same group key.
        assert_eq!(
            Value::Float(-0.0).collation_key(),
            Value::Float(0.0).collation_key()
        );
        assert_eq!(canonical_f64_bits(-0.0), canonical_f64_bits(0.0));
        // Distinct NaNs collapse to one key.
        let nan1 = f64::from_bits(0x7ff8_0000_0000_0001);
        let nan2 = f64::from_bits(0x7ff8_0000_0000_0002);
        assert!(nan1.is_nan() && nan2.is_nan());
        assert_eq!(
            Value::Float(nan1).collation_key(),
            Value::Float(nan2).collation_key()
        );
    }

    #[test]
    fn total_cmp_orders_numbers_numerically_not_lexically() {
        // The old Debug-string fallback sorted 10.0 before 2.0. Ensure numeric
        // order holds even for NaN (which makes `compare` return None).
        let nan = f64::NAN;
        let mut v = [
            Value::Float(10.0),
            Value::Float(2.0),
            Value::Float(nan),
            Value::Float(1.5),
        ];
        v.sort_by(|a, b| a.total_cmp(b));
        let nums: Vec<f64> = v
            .iter()
            .map(|x| if let Value::Float(f) = x { *f } else { 0.0 })
            .collect();
        assert_eq!(&nums[..3], &[1.5, 2.0, 10.0]);
        assert!(nums[3].is_nan()); // NaN sorts last, deterministically
    }

    #[test]
    fn deeply_nested_json_is_rejected_not_crashing() {
        // 10k levels would overflow a recursive parser without the depth guard.
        let deep = format!("{}{}", "[".repeat(10_000), "]".repeat(10_000));
        assert!(!is_valid_json(&deep));
        // Reasonable nesting still validates.
        assert!(is_valid_json(&format!(
            "{}1{}",
            "[".repeat(50),
            "]".repeat(50)
        )));
        assert!(is_valid_json(r#"{"a":[1,2,{"b":true}],"c":null}"#));
    }
}

#[cfg(test)]
mod value_props {
    use super::*;
    use crate::Collation;
    use proptest::prelude::*;

    /// A strategy generating an arbitrary `Value` across all variants.
    fn any_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            any::<u64>().prop_map(Value::UInt),
            any::<f64>()
                .prop_filter("no NaN", |f| !f.is_nan())
                .prop_map(Value::Float),
            ".*".prop_map(Value::Text),
            proptest::collection::vec(any::<u8>(), 0..32).prop_map(Value::Bytes),
            proptest::collection::vec(any::<f32>().prop_filter("finite", |f| f.is_finite()), 0..8)
                .prop_map(Value::Vector),
            any::<i32>().prop_map(Value::Date),
            any::<i64>().prop_map(Value::DateTime),
            (any::<i128>(), 0u8..=18).prop_map(|(u, s)| Value::Decimal(u, s)),
            any::<i64>().prop_map(Value::Time),
            ".*".prop_map(Value::Json),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(3000))]

        /// bincode round-trips every Value variant losslessly -- the on-disk row
        /// codec must be exact.
        #[test]
        fn bincode_roundtrip(v in any_value()) {
            let bytes = bincode::serialize(&v).unwrap();
            let back: Value = bincode::deserialize(&bytes).unwrap();
            prop_assert_eq!(v, back);
        }

        /// A whole row round-trips too.
        #[test]
        fn row_bincode_roundtrip(row in proptest::collection::vec(any_value(), 0..12)) {
            let bytes = bincode::serialize(&row).unwrap();
            let back: Vec<Value> = bincode::deserialize(&bytes).unwrap();
            prop_assert_eq!(row, back);
        }

        /// total_cmp is reflexive (a value equals itself).
        #[test]
        fn total_cmp_reflexive(v in any_value()) {
            prop_assert_eq!(v.total_cmp(&v), std::cmp::Ordering::Equal);
        }

        /// Equal collation keys are consistent with equality under the same
        /// collation: two texts differing only in case share a Ci key but not a
        /// Bin key (when they actually differ in case).
        #[test]
        fn collation_key_case_folding(s in "[a-zA-Z]{1,16}") {
            let lower = Value::Text(s.to_lowercase());
            let upper = Value::Text(s.to_uppercase());
            // Ci: folded keys match.
            prop_assert_eq!(
                lower.collation_key_coll(Collation::Ci),
                upper.collation_key_coll(Collation::Ci)
            );
            // Bin: keys match iff the strings are byte-identical.
            let bin_eq = lower.collation_key_coll(Collation::Bin)
                == upper.collation_key_coll(Collation::Bin);
            prop_assert_eq!(bin_eq, s.to_lowercase() == s.to_uppercase());
        }

        /// A value keyed twice yields identical bytes (determinism).
        #[test]
        fn collation_key_deterministic(v in any_value()) {
            prop_assert_eq!(v.collation_key(), v.collation_key());
        }
    }
}
