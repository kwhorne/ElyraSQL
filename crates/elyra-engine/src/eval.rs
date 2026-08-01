//! Minimal expression + query evaluation for the scaffold.
//!
//! Supports literal/arithmetic `SELECT` without a `FROM` clause, which is
//! enough to answer `SELECT 1`, `SELECT 1+1 AS two`, `SELECT 'hi'`. Table
//! scans, joins and aggregation land once the storage executor is wired up.

use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, UnaryOperator, Value as SqlValue};

use crate::stream::RowStream;
use crate::QueryResult;

/// Evaluate a `SELECT` without a `FROM` clause (literals/arithmetic).
pub fn eval_literal_select(q: &Query) -> Result<QueryResult> {
    let select = match q.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => {
            return Err(Error::Unsupported(
                "only simple SELECT is implemented".into(),
            ))
        }
    };

    let mut columns = Vec::new();
    let mut row = Vec::new();

    for (i, item) in select.projection.iter().enumerate() {
        let (name, expr) = match item {
            SelectItem::UnnamedExpr(e) => (e.to_string(), e),
            SelectItem::ExprWithAlias { expr, alias } => (alias.value.clone(), expr),
            other => {
                return Err(Error::Unsupported(format!(
                    "projection item not supported: {other}"
                )))
            }
        };
        let value = eval_expr(expr)?;
        let ty = infer_type(&value);
        let _ = i;
        columns.push(ColumnDef {
            name,
            ty,
            nullable: true,
            collation: elyra_core::Collation::Ci,
        });
        row.push(value);
    }

    Ok(QueryResult::Rows(RowStream::literal(
        Schema::new(columns),
        vec![row],
    )))
}

pub fn eval_expr(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(v) => literal(v),
        Expr::Nested(e) => eval_expr(e),
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr)?;
            match (op, v) {
                (UnaryOperator::Minus, Value::Int(i)) => {
                    i.checked_neg().map(Value::Int).ok_or_else(|| {
                        Error::OutOfRange(format!("BIGINT value is out of range in '-({i})'"))
                    })
                }
                (UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                (UnaryOperator::Minus, Value::Decimal(units, scale)) => units
                    .checked_neg()
                    .map(|units| Value::Decimal(units, scale))
                    .ok_or_else(|| Error::OutOfRange("DECIMAL value is out of range".into())),
                (UnaryOperator::Plus, v) => Ok(v),
                // Bitwise NOT and other operators via the full evaluator.
                (op, _) => {
                    let full = Expr::UnaryOp {
                        op: *op,
                        expr: expr.clone(),
                    };
                    crate::predicate::eval_row(&full, &elyra_core::Schema::new(Vec::new()), &[])
                }
            }
        }
        // Binary operators (arithmetic, comparison, bitwise, INTERVAL, JSON,
        // exact decimal) all go through the full evaluator.
        Expr::BinaryOp { .. } => {
            crate::predicate::eval_row(expr, &elyra_core::Schema::new(Vec::new()), &[])
        }
        // Delegate anything else (functions, JSON operators, ...) to the full
        // row evaluator with an empty schema/row.
        other => crate::predicate::eval_row(other, &elyra_core::Schema::new(Vec::new()), &[]),
    }
}

pub(crate) fn literal(v: &SqlValue) -> Result<Value> {
    match v {
        SqlValue::Number(n, _) => number_literal(n),
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        SqlValue::HexStringLiteral(hex) => decode_hex_literal(hex).map(Value::Bytes),
        SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        SqlValue::Null => Ok(Value::Null),
        other => Err(Error::Unsupported(format!(
            "literal not supported: {other}"
        ))),
    }
}

fn number_literal(number: &str) -> Result<Value> {
    if !number.contains(['e', 'E']) {
        if let Some((_, fraction)) = number.split_once('.') {
            let scale = u8::try_from(fraction.len())
                .map_err(|_| Error::OutOfRange("DECIMAL literal scale exceeds 255".into()))?;
            if scale > 38 {
                return Err(Error::OutOfRange(
                    "DECIMAL literal scale exceeds the supported maximum of 38".into(),
                ));
            }
            return elyra_core::value::parse_decimal(number, scale)
                .map(|(units, scale)| Value::Decimal(units, scale))
                .ok_or_else(|| {
                    Error::OutOfRange(format!("DECIMAL literal is out of range: {number}"))
                });
        }
    }

    if let Ok(integer) = number.parse::<i64>() {
        Ok(Value::Int(integer))
    } else if let Ok(unsigned) = number.parse::<u64>() {
        // Fits an unsigned 64-bit but not a signed one (e.g. a large
        // BIGINT UNSIGNED literal) -> keep it exact rather than lossy f64.
        Ok(Value::UInt(unsigned))
    } else {
        number
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|_| Error::Type(format!("invalid number literal: {number}")))
    }
}

fn decode_hex_literal(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return Err(Error::Type(format!(
            "hex literal must contain an even number of digits: X'{hex}'"
        )));
    }

    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_digit(pair[0]).ok_or_else(|| invalid_hex_literal(hex))?;
            let low = hex_digit(pair[1]).ok_or_else(|| invalid_hex_literal(hex))?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn invalid_hex_literal(hex: &str) -> Error {
    Error::Type(format!("invalid hex literal: X'{hex}'"))
}

fn infer_type(v: &Value) -> ColumnType {
    match v {
        Value::Null => ColumnType::Text,
        Value::Bool(_) => ColumnType::Bool,
        Value::Int(_) => ColumnType::Int,
        Value::UInt(_) => ColumnType::UInt,
        Value::Float(_) => ColumnType::Float,
        Value::Text(_) => ColumnType::Text,
        Value::Bytes(_) => ColumnType::Bytes,
        Value::Vector(v) => ColumnType::Vector(v.len() as u32),
        Value::Date(_) => ColumnType::Date,
        Value::DateTime(_) => ColumnType::DateTime,
        Value::Decimal(_, s) => ColumnType::Decimal(38, *s),
        Value::Time(_) => ColumnType::Time,
        Value::Json(_) => ColumnType::Json,
    }
}

#[cfg(test)]
mod tests {
    use elyra_core::Value;
    use sqlparser::ast::Value as SqlValue;

    use super::literal;

    #[test]
    fn hex_literal_decodes_binary_bytes() {
        assert_eq!(
            literal(&SqlValue::HexStringLiteral("00aF10".into())).unwrap(),
            Value::Bytes(vec![0x00, 0xaf, 0x10])
        );
    }

    #[test]
    fn hex_literal_rejects_an_odd_digit_count() {
        let error = literal(&SqlValue::HexStringLiteral("abc".into())).unwrap_err();
        assert!(error.to_string().contains("even number of digits"));
    }

    #[test]
    fn decimal_literal_preserves_all_digits() {
        assert_eq!(
            literal(&SqlValue::Number("170812946.3720907892".into(), false)).unwrap(),
            Value::Decimal(1_708_129_463_720_907_892, 10)
        );
    }

    #[test]
    fn decimal_literal_rejects_an_unsupported_scale() {
        let literal = format!("0.{}", "0".repeat(39));
        let error = super::literal(&SqlValue::Number(literal, false)).unwrap_err();
        assert!(error.to_string().contains("supported maximum of 38"));
    }
}
