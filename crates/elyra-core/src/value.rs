//! Runtime value model shared across all engines.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

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
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Numeric view for arithmetic/aggregation (Int/Float/Bool/Decimal).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::Decimal(u, s) => Some(*u as f64 / 10f64.powi(*s as i32)),
            _ => None,
        }
    }

    /// SQL comparison with implicit cross-type coercion. `None` when either
    /// operand is NULL (three-valued logic) or the types are incomparable.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        use Value::*;
        if self.is_null() || other.is_null() {
            return None;
        }
        match (self, other) {
            (Date(_), _) | (_, Date(_)) => {
                to_days(self).zip(to_days(other)).map(|(a, b)| a.cmp(&b))
            }
            (DateTime(_), _) | (_, DateTime(_)) => to_micros(self)
                .zip(to_micros(other))
                .map(|(a, b)| a.cmp(&b)),
            (Decimal(au, asc), Decimal(bu, bsc)) => Some(cmp_decimal(*au, *asc, *bu, *bsc)),
            (Decimal(..), _) | (_, Decimal(..)) => self
                .as_f64()
                .zip(other.as_f64())
                .and_then(|(a, b)| a.partial_cmp(&b)),
            (Time(_), _) | (_, Time(_)) => to_micros_of_day(self)
                .zip(to_micros_of_day(other))
                .map(|(a, b)| a.cmp(&b)),
            (Text(a), Text(b)) => Some(a.cmp(b)),
            (Json(a), Json(b)) => Some(a.cmp(b)),
            (Json(a), Text(b)) | (Text(b), Json(a)) => Some(a.cmp(b)),
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            (Bytes(a), Bytes(b)) => Some(a.cmp(b)),
            _ => self
                .as_f64()
                .zip(other.as_f64())
                .and_then(|(a, b)| a.partial_cmp(&b)),
        }
    }

    /// Total order for sorting/extremes: NULL sorts first, then `compare`.
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        match (self.is_null(), other.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => self
                .compare(other)
                .unwrap_or_else(|| format!("{self:?}").cmp(&format!("{other:?}"))),
        }
    }

    /// Render as a MySQL text-protocol column value.
    pub fn to_wire_string(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Bool(b) => Some(if *b { "1".into() } else { "0".into() }),
            Value::Int(i) => Some(i.to_string()),
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

fn to_days(v: &Value) -> Option<i64> {
    match v {
        Value::Date(d) => Some(*d as i64),
        Value::Int(i) => Some(*i),
        Value::Text(s) => crate::datetime::parse_date(s).map(|d| d as i64),
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

/// Minimal JSON validator (structure only, no dependency).
pub fn is_valid_json(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut pos = 0;
    skip_ws(bytes, &mut pos);
    if !parse_json_value(bytes, &mut pos) {
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

fn parse_json_value(b: &[u8], p: &mut usize) -> bool {
    skip_ws(b, p);
    if *p >= b.len() {
        return false;
    }
    match b[*p] {
        b'{' => parse_json_object(b, p),
        b'[' => parse_json_array(b, p),
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

fn parse_json_array(b: &[u8], p: &mut usize) -> bool {
    *p += 1; // [
    skip_ws(b, p);
    if b.get(*p) == Some(&b']') {
        *p += 1;
        return true;
    }
    loop {
        if !parse_json_value(b, p) {
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

fn parse_json_object(b: &[u8], p: &mut usize) -> bool {
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
        if !parse_json_value(b, p) {
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
