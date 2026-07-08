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
            // Qualified reference like `t.col` -> match a combined-schema
            // column named "t.col".
            let qualified = parts.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join(".");
            resolve(&qualified, schema, row)
        }
        Expr::Function(f) => eval_function(f, schema, row),
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

/// Evaluate a scalar function. Currently the ElyraSQL vector distance family.
fn eval_function(f: &sqlparser::ast::Function, schema: &Schema, row: &[Value]) -> Result<Value> {
    use elyra_vector::Metric;
    let name = f.name.0.last().map(|i| i.value.to_ascii_lowercase()).unwrap_or_default();
    let metric = match name.as_str() {
        "vec_distance" | "vec_l2_distance" | "vec_distance_l2" => Metric::L2,
        "vec_cosine_distance" | "vec_distance_cosine" => Metric::Cosine,
        "vec_inner_product" | "vec_distance_ip" => Metric::InnerProduct,
        other => return Err(Error::Unsupported(format!("unknown function: {other}"))),
    };

    let args = function_arg_exprs(f)?;
    if args.len() != 2 {
        return Err(Error::Query(format!("{name} expects 2 arguments")));
    }
    let a = to_vector(&eval_row(args[0], schema, row)?)?;
    let b = to_vector(&eval_row(args[1], schema, row)?)?;
    match elyra_vector::distance(&a, &b, metric) {
        Some(d) => Ok(Value::Float(d as f64)),
        None => Err(Error::Vector(format!(
            "vector dimension mismatch: {} vs {}",
            a.len(),
            b.len()
        ))),
    }
}

fn function_arg_exprs(f: &sqlparser::ast::Function) -> Result<Vec<&Expr>> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let FunctionArguments::List(list) = &f.args else {
        return Err(Error::Query("function requires arguments".into()));
    };
    let mut out = Vec::new();
    for a in &list.args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => out.push(e),
            _ => return Err(Error::Unsupported("unsupported function argument".into())),
        }
    }
    Ok(out)
}

/// Coerce a value to a vector: a `VECTOR` value, or a `'[..]'` text literal.
fn to_vector(v: &Value) -> Result<Vec<f32>> {
    match v {
        Value::Vector(x) => Ok(x.clone()),
        Value::Text(s) => {
            let inner = s.trim().trim_start_matches('[').trim_end_matches(']');
            inner
                .split(',')
                .filter(|t| !t.trim().is_empty())
                .map(|t| t.trim().parse::<f32>().map_err(|_| Error::Vector(format!("bad vector element: {t}"))))
                .collect()
        }
        other => Err(Error::Vector(format!("value is not a vector: {other:?}"))),
    }
}

fn resolve(name: &str, schema: &Schema, row: &[Value]) -> Result<Value> {
    let idx = resolve_index(name, schema)?;
    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
}

/// Resolve a column reference to an index. Handles both single-table (bare
/// names) and joined (qualified "table.col") schemas: exact match first, then
/// a unique bare-suffix match (so `col` resolves against `t.col`).
pub fn resolve_index(name: &str, schema: &Schema) -> Result<usize> {
    if let Some(i) = schema.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name)) {
        return Ok(i);
    }
    let bare = |n: &str| n.rsplit('.').next().unwrap_or(n).to_string();
    let hits: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| bare(&c.name).eq_ignore_ascii_case(name))
        .map(|(i, _)| i)
        .collect();
    match hits.len() {
        1 => Ok(hits[0]),
        0 => Err(Error::Catalog(format!("unknown column: {name}"))),
        _ => Err(Error::Query(format!("ambiguous column: {name}"))),
    }
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

/// Three-way compare with SQL cross-type coercion; `None` when either side is
/// NULL. Delegates to the shared [`Value::compare`].
fn cmp(l: &Value, r: &Value) -> Result<Option<std::cmp::Ordering>> {
    Ok(l.compare(r))
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64()
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        _ => false,
    }
}
