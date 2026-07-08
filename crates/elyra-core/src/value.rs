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
            (Date(_), _) | (_, Date(_)) => to_days(self).zip(to_days(other)).map(|(a, b)| a.cmp(&b)),
            (DateTime(_), _) | (_, DateTime(_)) => {
                to_micros(self).zip(to_micros(other)).map(|(a, b)| a.cmp(&b))
            }
            (Decimal(au, asc), Decimal(bu, bsc)) => Some(cmp_decimal(*au, *asc, *bu, *bsc)),
            (Decimal(..), _) | (_, Decimal(..)) => {
                self.as_f64().zip(other.as_f64()).and_then(|(a, b)| a.partial_cmp(&b))
            }
            (Text(a), Text(b)) => Some(a.cmp(b)),
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            (Bytes(a), Bytes(b)) => Some(a.cmp(b)),
            _ => self.as_f64().zip(other.as_f64()).and_then(|(a, b)| a.partial_cmp(&b)),
        }
    }

    /// Total order for sorting/extremes: NULL sorts first, then `compare`.
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        match (self.is_null(), other.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => self.compare(other).unwrap_or_else(|| format!("{self:?}").cmp(&format!("{other:?}"))),
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
                let inner = v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",");
                Some(format!("[{inner}]"))
            }
            Value::Date(d) => Some(crate::datetime::format_date(*d)),
            Value::DateTime(t) => Some(crate::datetime::format_datetime(*t)),
            Value::Decimal(units, scale) => Some(format_decimal(*units, *scale)),
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

fn cmp_decimal(au: i128, asc: u8, bu: i128, bsc: u8) -> Ordering {
    let s = asc.max(bsc);
    let a = au.saturating_mul(10i128.saturating_pow((s - asc) as u32));
    let b = bu.saturating_mul(10i128.saturating_pow((s - bsc) as u32));
    a.cmp(&b)
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
    if !int_part.chars().all(|c| c.is_ascii_digit()) || !frac_part.chars().all(|c| c.is_ascii_digit())
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
