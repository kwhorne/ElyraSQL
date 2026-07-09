//! Minimal expression + query evaluation for the scaffold.
//!
//! Supports literal/arithmetic `SELECT` without a `FROM` clause, which is
//! enough to answer `SELECT 1`, `SELECT 1+1 AS two`, `SELECT 'hi'`. Table
//! scans, joins and aggregation land once the storage executor is wired up.

use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, Query, SelectItem, SetExpr, UnaryOperator, Value as SqlValue,
};

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
        Expr::Value(v) => eval_literal(value_of(v)),
        Expr::Nested(e) => eval_expr(e),
        Expr::UnaryOp { op, expr } => {
            let v = eval_expr(expr)?;
            match (op, v) {
                (UnaryOperator::Minus, Value::Int(i)) => Ok(Value::Int(-i)),
                (UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
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
        Expr::BinaryOp { left, op, right } => {
            // Comparisons, date arithmetic (INTERVAL), and anything non-numeric
            // go through the full evaluator; keep the fast path for plain math.
            let simple = matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ) && !matches!(left.as_ref(), Expr::Interval(_))
                && !matches!(right.as_ref(), Expr::Interval(_));
            if !simple {
                return crate::predicate::eval_row(expr, &elyra_core::Schema::new(Vec::new()), &[]);
            }
            let l = eval_expr(left)?;
            let r = eval_expr(right)?;
            eval_binary(l, op, r)
        }
        // Delegate anything else (functions, JSON operators, ...) to the full
        // row evaluator with an empty schema/row.
        other => crate::predicate::eval_row(other, &elyra_core::Schema::new(Vec::new()), &[]),
    }
}

fn eval_binary(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    let (lf, rf) = (as_f64(&l), as_f64(&r));
    match (lf, rf) {
        (Some(a), Some(b)) => {
            let both_int = matches!(l, Value::Int(_)) && matches!(r, Value::Int(_));
            let res = match op {
                Plus => a + b,
                Minus => a - b,
                Multiply => a * b,
                Divide => {
                    if b == 0.0 {
                        return Ok(Value::Null); // MySQL: x/0 -> NULL
                    }
                    a / b
                }
                Modulo => a % b,
                _ => return Err(Error::Unsupported("unsupported binary operator".into())),
            };
            if both_int && matches!(op, Plus | Minus | Multiply | Modulo) {
                Ok(Value::Int(res as i64))
            } else {
                Ok(Value::Float(res))
            }
        }
        _ => Err(Error::Type("arithmetic on non-numeric value".into())),
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn eval_literal(v: &SqlValue) -> Result<Value> {
    match v {
        SqlValue::Number(n, _) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(Value::Int(i))
            } else {
                n.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| Error::Type(format!("invalid number literal: {n}")))
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        SqlValue::Null => Ok(Value::Null),
        other => Err(Error::Unsupported(format!(
            "literal not supported: {other}"
        ))),
    }
}

fn infer_type(v: &Value) -> ColumnType {
    match v {
        Value::Null => ColumnType::Text,
        Value::Bool(_) => ColumnType::Bool,
        Value::Int(_) => ColumnType::Int,
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

/// Bridge helper: newer `sqlparser` wraps literals in `ValueWithSpan`. This
/// indirection keeps [`eval_literal`] working regardless of that wrapping.
fn value_of(v: &SqlValue) -> &SqlValue {
    v
}
