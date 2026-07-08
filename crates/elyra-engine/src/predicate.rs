//! Row-aware expression evaluation for `WHERE` predicates.
//!
//! Unlike [`crate::eval`] (literals only), this evaluates expressions that
//! reference columns, resolved against a row + its schema.

use elyra_core::{Error, Result, Schema, Value};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};

/// Evaluate `expr` against a row. Column identifiers resolve via `schema`.
pub fn eval_row(expr: &Expr, schema: &Schema, row: &[Value]) -> Result<Value> {
    match expr {
        Expr::Value(v) => literal(v),
        Expr::Nested(e) => eval_row(e, schema, row),
        Expr::Identifier(id) => resolve(&id.value, schema, row),
        Expr::CompoundIdentifier(parts) => {
            let name = parts.last().map(|i| i.value.as_str()).unwrap_or("");
            resolve(name, schema, row)
        }
        Expr::IsNull(e) => Ok(Value::Bool(eval_row(e, schema, row)?.is_null())),
        Expr::IsNotNull(e) => Ok(Value::Bool(!eval_row(e, schema, row)?.is_null())),
        Expr::UnaryOp { op, expr } => {
            let v = eval_row(expr, schema, row)?;
            match (op, v) {
                (UnaryOperator::Not, v) => Ok(Value::Bool(!truthy(&v))),
                (UnaryOperator::Minus, Value::Int(i)) => Ok(Value::Int(-i)),
                (UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                (UnaryOperator::Plus, v) => Ok(v),
                _ => Err(Error::Unsupported("unsupported unary operator".into())),
            }
        }
        Expr::Between { expr, negated, low, high } => {
            let v = eval_row(expr, schema, row)?;
            let lo = eval_row(low, schema, row)?;
            let hi = eval_row(high, schema, row)?;
            let inside = cmp(&v, &lo)?.map(|o| o.is_ge()).unwrap_or(false)
                && cmp(&v, &hi)?.map(|o| o.is_le()).unwrap_or(false);
            Ok(Value::Bool(if *negated { !inside } else { inside }))
        }
        Expr::BinaryOp { left, op, right } => {
            binary(eval_row(left, schema, row)?, op, || eval_row(right, schema, row), left, right, schema, row)
        }
        other => Err(Error::Unsupported(format!(
            "expression not supported in WHERE: {other}"
        ))),
    }
}

/// Does `expr` filter `row` in or out?
pub fn matches(expr: &Expr, schema: &Schema, row: &[Value]) -> Result<bool> {
    Ok(truthy(&eval_row(expr, schema, row)?))
}

fn resolve(name: &str, schema: &Schema, row: &[Value]) -> Result<Value> {
    let idx = schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Catalog(format!("unknown column: {name}")))?;
    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
}

fn literal(v: &SqlValue) -> Result<Value> {
    match v {
        SqlValue::Number(n, _) => n
            .parse::<i64>()
            .map(Value::Int)
            .or_else(|_| n.parse::<f64>().map(Value::Float))
            .map_err(|_| Error::Type(format!("invalid number: {n}"))),
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            Ok(Value::Text(s.clone()))
        }
        SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        SqlValue::Null => Ok(Value::Null),
        other => Err(Error::Unsupported(format!("literal not supported: {other}"))),
    }
}

#[allow(clippy::too_many_arguments)]
fn binary(
    l: Value,
    op: &BinaryOperator,
    eval_right: impl FnOnce() -> Result<Value>,
    _lexpr: &Expr,
    _rexpr: &Expr,
    _schema: &Schema,
    _row: &[Value],
) -> Result<Value> {
    use BinaryOperator::*;
    // Short-circuit logical operators.
    match op {
        And => return Ok(Value::Bool(truthy(&l) && truthy(&eval_right()?))),
        Or => return Ok(Value::Bool(truthy(&l) || truthy(&eval_right()?))),
        _ => {}
    }
    let r = eval_right()?;
    match op {
        Eq => Ok(Value::Bool(cmp(&l, &r)?.map(|o| o.is_eq()).unwrap_or(false))),
        NotEq => Ok(Value::Bool(cmp(&l, &r)?.map(|o| o.is_ne()).unwrap_or(true))),
        Lt => Ok(Value::Bool(cmp(&l, &r)?.map(|o| o.is_lt()).unwrap_or(false))),
        LtEq => Ok(Value::Bool(cmp(&l, &r)?.map(|o| o.is_le()).unwrap_or(false))),
        Gt => Ok(Value::Bool(cmp(&l, &r)?.map(|o| o.is_gt()).unwrap_or(false))),
        GtEq => Ok(Value::Bool(cmp(&l, &r)?.map(|o| o.is_ge()).unwrap_or(false))),
        Plus | Minus | Multiply | Divide | Modulo => arith(l, op, r),
        _ => Err(Error::Unsupported(format!("operator not supported: {op}"))),
    }
}

fn arith(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    let (Some(a), Some(b)) = (num(&l), num(&r)) else {
        return Err(Error::Type("arithmetic on non-numeric value".into()));
    };
    let both_int = matches!(l, Value::Int(_)) && matches!(r, Value::Int(_));
    let res = match op {
        Plus => a + b,
        Minus => a - b,
        Multiply => a * b,
        Divide => {
            if b == 0.0 {
                return Ok(Value::Null);
            }
            a / b
        }
        Modulo => a % b,
        _ => unreachable!(),
    };
    Ok(if both_int && !matches!(op, Divide) {
        Value::Int(res as i64)
    } else {
        Value::Float(res)
    })
}

/// Three-way compare; `None` when either side is NULL (SQL semantics).
fn cmp(l: &Value, r: &Value) -> Result<Option<std::cmp::Ordering>> {
    use std::cmp::Ordering;
    if l.is_null() || r.is_null() {
        return Ok(None);
    }
    if let (Some(a), Some(b)) = (num(l), num(r)) {
        return Ok(a.partial_cmp(&b));
    }
    match (l, r) {
        (Value::Text(a), Value::Text(b)) => Ok(Some(a.cmp(b))),
        (Value::Bool(a), Value::Bool(b)) => Ok(Some(a.cmp(b))),
        _ => Ok(Some(Ordering::Equal).filter(|_| l == r).or(None)),
    }
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        _ => false,
    }
}
