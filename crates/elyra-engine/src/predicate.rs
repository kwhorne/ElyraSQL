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
        Expr::Identifier(id) => {
            // Niladic functions like CURRENT_TIMESTAMP appear as bare identifiers.
            if !schema
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&id.value))
            {
                if let Some(v) = niladic_fn(&id.value) {
                    return Ok(v);
                }
            }
            resolve(&id.value, schema, row)
        }
        Expr::CompoundIdentifier(parts) => {
            // Qualified reference like `t.col` -> match a combined-schema
            // column named "t.col".
            let qualified = parts
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            resolve(&qualified, schema, row)
        }
        Expr::Function(f) => eval_function(f, schema, row),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let v = eval_row(expr, schema, row)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            let mut found = false;
            for item in list {
                if v.compare(&eval_row(item, schema, row)?) == Some(std::cmp::Ordering::Equal) {
                    found = true;
                    break;
                }
            }
            Ok(Value::Bool(found != *negated))
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
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let v = eval_row(expr, schema, row)?;
            let lo = eval_row(low, schema, row)?;
            let hi = eval_row(high, schema, row)?;
            let inside = cmp(&v, &lo)?.map(|o| o.is_ge()).unwrap_or(false)
                && cmp(&v, &hi)?.map(|o| o.is_le()).unwrap_or(false);
            Ok(Value::Bool(if *negated { !inside } else { inside }))
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Arrow => {
                json_path(eval_row(left, schema, row)?, right, schema, row, false)
            }
            BinaryOperator::LongArrow => {
                json_path(eval_row(left, schema, row)?, right, schema, row, true)
            }
            _ => binary(
                eval_row(left, schema, row)?,
                op,
                || eval_row(right, schema, row),
                left,
                right,
                schema,
                row,
            ),
        },
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let s = eval_row(expr, schema, row)?;
            let from = match substring_from {
                Some(e) => eval_row(e, schema, row)?,
                None => Value::Int(1),
            };
            let mut a = vec![s, from];
            if let Some(e) = substring_for {
                a.push(eval_row(e, schema, row)?);
            }
            Ok(substring(&a))
        }
        Expr::Trim {
            expr,
            trim_where,
            trim_what,
            ..
        } => {
            use sqlparser::ast::TrimWhereField;
            let s = match eval_row(expr, schema, row)? {
                Value::Null => return Ok(Value::Null),
                v => v.to_wire_string().unwrap_or_default(),
            };
            let what = match trim_what {
                Some(e) => eval_row(e, schema, row)?.to_wire_string(),
                None => None,
            };
            let res = match (trim_where, what.as_deref()) {
                (Some(TrimWhereField::Leading), Some(w)) => s.trim_start_matches(w).to_string(),
                (Some(TrimWhereField::Trailing), Some(w)) => s.trim_end_matches(w).to_string(),
                (_, Some(w)) => s.trim_start_matches(w).trim_end_matches(w).to_string(),
                (Some(TrimWhereField::Leading), None) => s.trim_start().to_string(),
                (Some(TrimWhereField::Trailing), None) => s.trim_end().to_string(),
                (_, None) => s.trim().to_string(),
            };
            Ok(Value::Text(res))
        }
        Expr::Ceil { expr, .. } => Ok(match eval_row(expr, schema, row)?.as_f64() {
            Some(x) => Value::Int(x.ceil() as i64),
            None => Value::Null,
        }),
        Expr::Floor { expr, .. } => Ok(match eval_row(expr, schema, row)?.as_f64() {
            Some(x) => Value::Int(x.floor() as i64),
            None => Value::Null,
        }),
        Expr::Position { expr, r#in } => {
            let sub = eval_row(expr, schema, row)?.to_wire_string();
            let s = eval_row(r#in, schema, row)?.to_wire_string();
            Ok(match (sub, s) {
                (Some(sub), Some(s)) => Value::Int(match s.find(&sub) {
                    Some(b) => s[..b].chars().count() as i64 + 1,
                    None => 0,
                }),
                _ => Value::Null,
            })
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let op = match operand {
                Some(o) => Some(eval_row(o, schema, row)?),
                None => None,
            };
            for (cond, res) in conditions.iter().zip(results.iter()) {
                let hit = match &op {
                    Some(ov) => {
                        eval_row(cond, schema, row)?.compare(ov) == Some(std::cmp::Ordering::Equal)
                    }
                    None => truthy(&eval_row(cond, schema, row)?),
                };
                if hit {
                    return eval_row(res, schema, row);
                }
            }
            match else_result {
                Some(e) => eval_row(e, schema, row),
                None => Ok(Value::Null),
            }
        }
        Expr::Cast {
            expr, data_type, ..
        } => {
            let v = eval_row(expr, schema, row)?;
            cast_value(v, data_type)
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
/// Apply a JSON path operator. `unquote` selects `->>` (raw text) over `->`.
fn json_path(
    doc: Value,
    path_expr: &Expr,
    schema: &Schema,
    row: &[Value],
    unquote: bool,
) -> Result<Value> {
    let path = match eval_row(path_expr, schema, row)? {
        Value::Text(s) | Value::Json(s) => s,
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(Error::Type(format!(
                "JSON path must be a string, got {other:?}"
            )))
        }
    };
    let text = match &doc {
        Value::Json(s) | Value::Text(s) => s.clone(),
        Value::Null => return Ok(Value::Null),
        other => {
            return Err(Error::Type(format!(
                "-> requires a JSON value, got {other:?}"
            )))
        }
    };
    let Some(parsed) = elyra_core::json::parse(&text) else {
        return Err(Error::Type("left side of -> is not valid JSON".into()));
    };
    Ok(match parsed.extract(&path) {
        Some(v) if unquote => Value::Text(v.to_unquoted()),
        Some(v) => Value::Json(v.to_json_string()),
        None => Value::Null,
    })
}

fn eval_function(f: &sqlparser::ast::Function, schema: &Schema, row: &[Value]) -> Result<Value> {
    use elyra_vector::Metric;
    let name = f
        .name
        .0
        .last()
        .map(|i| i.value.to_ascii_lowercase())
        .unwrap_or_default();

    if name == "json_extract" {
        let args = function_arg_exprs(f)?;
        if args.len() != 2 {
            return Err(Error::Query("JSON_EXTRACT expects (doc, path)".into()));
        }
        return json_path(eval_row(args[0], schema, row)?, args[1], schema, row, false);
    }
    if name == "json_unquote" {
        let args = function_arg_exprs(f)?;
        if args.len() != 1 {
            return Err(Error::Query("JSON_UNQUOTE expects (doc)".into()));
        }
        return Ok(match eval_row(args[0], schema, row)? {
            Value::Json(s) | Value::Text(s) => Value::Text(
                elyra_core::json::parse(&s)
                    .map(|j| j.to_unquoted())
                    .unwrap_or(s),
            ),
            v => v,
        });
    }

    match name.as_str() {
        "json_array" | "json_object" | "json_quote" | "json_valid" | "json_type"
        | "json_length" | "json_keys" | "json_contains" | "json_set" | "json_insert"
        | "json_replace" | "json_remove" => {
            let args = function_arg_exprs(f)?;
            let vals: Vec<Value> = args
                .iter()
                .map(|e| eval_row(e, schema, row))
                .collect::<Result<_>>()?;
            return eval_json_fn(&name, &vals);
        }
        _ => {}
    }

    // Conditional functions (short-circuiting / NULL-aware).
    let args_exprs = function_arg_exprs(f)?;
    match name.as_str() {
        "coalesce" => {
            for a in &args_exprs {
                let v = eval_row(a, schema, row)?;
                if !v.is_null() {
                    return Ok(v);
                }
            }
            return Ok(Value::Null);
        }
        "ifnull" | "nvl" if args_exprs.len() == 2 => {
            let a = eval_row(args_exprs[0], schema, row)?;
            return Ok(if a.is_null() {
                eval_row(args_exprs[1], schema, row)?
            } else {
                a
            });
        }
        "nullif" if args_exprs.len() == 2 => {
            let a = eval_row(args_exprs[0], schema, row)?;
            let b = eval_row(args_exprs[1], schema, row)?;
            return Ok(if a.compare(&b) == Some(std::cmp::Ordering::Equal) {
                Value::Null
            } else {
                a
            });
        }
        "if" if args_exprs.len() == 3 => {
            let c = truthy(&eval_row(args_exprs[0], schema, row)?);
            let branch = if c { args_exprs[1] } else { args_exprs[2] };
            return eval_row(branch, schema, row);
        }
        _ => {}
    }

    // Other scalar functions: eager argument evaluation.
    let argv: Vec<Value> = args_exprs
        .iter()
        .map(|e| eval_row(e, schema, row))
        .collect::<Result<_>>()?;
    if let Some(v) = eval_scalar(&name, &argv)? {
        return Ok(v);
    }

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

/// Evaluate the JSON construction / manipulation / inspection functions.
fn eval_json_fn(name: &str, vals: &[Value]) -> Result<Value> {
    use elyra_core::json::{self, Json, SetMode};

    // Parse a value as a JSON document (NULL propagates).
    let get_doc = |v: &Value| -> Result<Option<Json>> {
        match v {
            Value::Null => Ok(None),
            Value::Json(s) | Value::Text(s) => json::parse(s)
                .map(Some)
                .ok_or_else(|| Error::Type("invalid JSON document".into())),
            other => Err(Error::Type(format!("not a JSON document: {other:?}"))),
        }
    };
    let as_str = |v: &Value| -> Result<String> {
        match v {
            Value::Text(s) | Value::Json(s) => Ok(s.clone()),
            other => other
                .to_wire_string()
                .ok_or_else(|| Error::Type(format!("expected a string, got {other:?}"))),
        }
    };

    match name {
        "json_array" => Ok(Value::Json(
            Json::Arr(vals.iter().map(Json::from_value).collect()).to_json_string(),
        )),
        "json_object" => {
            if vals.len() % 2 != 0 {
                return Err(Error::Query("JSON_OBJECT expects key/value pairs".into()));
            }
            let mut pairs = Vec::with_capacity(vals.len() / 2);
            for kv in vals.chunks(2) {
                pairs.push((as_str(&kv[0])?, Json::from_value(&kv[1])));
            }
            Ok(Value::Json(Json::Obj(pairs).to_json_string()))
        }
        "json_quote" => match &vals[0] {
            Value::Null => Ok(Value::Null),
            v => Ok(Value::Json(Json::Str(as_str(v)?).to_json_string())),
        },
        "json_valid" => match &vals[0] {
            Value::Null => Ok(Value::Null),
            v => Ok(Value::Bool(json::parse(&as_str(v)?).is_some())),
        },
        "json_type" => match get_doc(&vals[0])? {
            None => Ok(Value::Null),
            Some(j) => Ok(Value::Text(j.type_name().to_string())),
        },
        "json_length" => match get_doc(&vals[0])? {
            None => Ok(Value::Null),
            Some(j) => {
                let target = match vals.get(1) {
                    Some(p) => j.extract(&as_str(p)?),
                    None => Some(j),
                };
                Ok(target.map_or(Value::Null, |t| Value::Int(t.length() as i64)))
            }
        },
        "json_keys" => match get_doc(&vals[0])? {
            None => Ok(Value::Null),
            Some(j) => {
                let target = match vals.get(1) {
                    Some(p) => j.extract(&as_str(p)?),
                    None => Some(j),
                };
                Ok(match target.and_then(|t| t.keys()) {
                    Some(keys) => Value::Json(
                        Json::Arr(keys.into_iter().map(Json::Str).collect()).to_json_string(),
                    ),
                    None => Value::Null,
                })
            }
        },
        "json_contains" => {
            let (target, candidate) = (get_doc(&vals[0])?, get_doc(&vals[1])?);
            match (target, candidate) {
                (Some(mut t), Some(c)) => {
                    if let Some(p) = vals.get(2) {
                        match t.extract(&as_str(p)?) {
                            Some(sub) => t = sub,
                            None => return Ok(Value::Null),
                        }
                    }
                    Ok(Value::Bool(t.contains(&c)))
                }
                _ => Ok(Value::Null),
            }
        }
        "json_set" | "json_insert" | "json_replace" => {
            let mode = match name {
                "json_insert" => SetMode::Insert,
                "json_replace" => SetMode::Replace,
                _ => SetMode::Set,
            };
            let Some(mut doc) = get_doc(&vals[0])? else {
                return Ok(Value::Null);
            };
            if vals[1..].len() % 2 != 0 {
                return Err(Error::Query(format!(
                    "{} expects (doc, path, val, ...)",
                    name.to_ascii_uppercase()
                )));
            }
            for pv in vals[1..].chunks(2) {
                doc.set_path(&as_str(&pv[0])?, Json::from_value(&pv[1]), mode);
            }
            Ok(Value::Json(doc.to_json_string()))
        }
        "json_remove" => {
            let Some(mut doc) = get_doc(&vals[0])? else {
                return Ok(Value::Null);
            };
            for p in &vals[1..] {
                doc.remove_path(&as_str(p)?);
            }
            Ok(Value::Json(doc.to_json_string()))
        }
        other => Err(Error::Unsupported(format!(
            "unknown JSON function: {other}"
        ))),
    }
}

/// Evaluate niladic date/time functions written as bare identifiers.
fn niladic_fn(name: &str) -> Option<Value> {
    let n = name.to_ascii_lowercase();
    matches!(
        n.as_str(),
        "current_timestamp"
            | "current_date"
            | "current_time"
            | "now"
            | "localtime"
            | "localtimestamp"
            | "sysdate"
            | "curdate"
            | "curtime"
    )
    .then(|| eval_scalar(&n, &[]).ok().flatten())
    .flatten()
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn wire(v: &Value) -> Option<String> {
    if v.is_null() {
        None
    } else {
        v.to_wire_string()
    }
}
fn sstr(a: &[Value], i: usize) -> Option<String> {
    a.get(i).and_then(wire)
}
fn nnum(a: &[Value], i: usize) -> Option<f64> {
    a.get(i).and_then(|v| v.as_f64())
}
fn str1(a: &[Value], f: impl Fn(String) -> String) -> Value {
    match sstr(a, 0) {
        Some(s) => Value::Text(f(s)),
        None => Value::Null,
    }
}
fn math1(a: &[Value], f: impl Fn(f64) -> f64) -> Value {
    match nnum(a, 0) {
        Some(x) => Value::Float(f(x)),
        None => Value::Null,
    }
}
fn math1i(a: &[Value], f: impl Fn(f64) -> f64) -> Value {
    match nnum(a, 0) {
        Some(x) => Value::Int(f(x) as i64),
        None => Value::Null,
    }
}
fn math2(a: &[Value], f: impl Fn(f64, f64) -> f64) -> Value {
    match (nnum(a, 0), nnum(a, 1)) {
        (Some(x), Some(y)) => Value::Float(f(x, y)),
        _ => Value::Null,
    }
}

/// Dispatch scalar functions with eager, already-evaluated arguments. Returns
/// `Ok(None)` when the name is not a known scalar function.
fn eval_scalar(name: &str, a: &[Value]) -> Result<Option<Value>> {
    use std::cmp::Ordering;
    let out = match name {
        // ---- date / time ----
        "now" | "current_timestamp" | "localtime" | "localtimestamp" | "sysdate" => {
            Value::DateTime(now_micros())
        }
        "curdate" | "current_date" => Value::Date(now_micros().div_euclid(86_400_000_000) as i32),
        "curtime" | "current_time" => Value::Time(now_micros().rem_euclid(86_400_000_000)),
        "unix_timestamp" => {
            if a.is_empty() {
                Value::Int(now_micros() / 1_000_000)
            } else {
                match &a[0] {
                    Value::DateTime(m) => Value::Int(m / 1_000_000),
                    Value::Null => Value::Null,
                    v => v
                        .as_f64()
                        .map(|n| Value::Int(n as i64))
                        .unwrap_or(Value::Null),
                }
            }
        }
        "uuid" => Value::Text(gen_uuid()),
        // ---- string ----
        "concat" => {
            let mut s = String::new();
            for v in a {
                match wire(v) {
                    Some(x) => s.push_str(&x),
                    None => return Ok(Some(Value::Null)),
                }
            }
            Value::Text(s)
        }
        "concat_ws" => {
            let sep = match sstr(a, 0) {
                Some(s) => s,
                None => return Ok(Some(Value::Null)),
            };
            let parts: Vec<String> = a[1.min(a.len())..].iter().filter_map(wire).collect();
            Value::Text(parts.join(&sep))
        }
        "upper" | "ucase" => str1(a, |s| s.to_uppercase()),
        "lower" | "lcase" => str1(a, |s| s.to_lowercase()),
        "length" | "char_length" | "character_length" => match sstr(a, 0) {
            Some(s) => Value::Int(s.chars().count() as i64),
            None => Value::Null,
        },
        "octet_length" => match sstr(a, 0) {
            Some(s) => Value::Int(s.len() as i64),
            None => Value::Null,
        },
        "reverse" => str1(a, |s| s.chars().rev().collect()),
        "trim" => str1(a, |s| s.trim().to_string()),
        "ltrim" => str1(a, |s| s.trim_start().to_string()),
        "rtrim" => str1(a, |s| s.trim_end().to_string()),
        "space" => match nnum(a, 0) {
            Some(n) => Value::Text(" ".repeat(n.max(0.0) as usize)),
            None => Value::Null,
        },
        "repeat" => match (sstr(a, 0), nnum(a, 1)) {
            (Some(s), Some(n)) => Value::Text(s.repeat(n.max(0.0) as usize)),
            _ => Value::Null,
        },
        "replace" => match (sstr(a, 0), sstr(a, 1), sstr(a, 2)) {
            (Some(s), Some(from), Some(to)) => Value::Text(if from.is_empty() {
                s
            } else {
                s.replace(&from, &to)
            }),
            _ => Value::Null,
        },
        "left" => match (sstr(a, 0), nnum(a, 1)) {
            (Some(s), Some(n)) => Value::Text(s.chars().take(n.max(0.0) as usize).collect()),
            _ => Value::Null,
        },
        "right" => match (sstr(a, 0), nnum(a, 1)) {
            (Some(s), Some(n)) => {
                let cs: Vec<char> = s.chars().collect();
                let start = cs.len().saturating_sub(n.max(0.0) as usize);
                Value::Text(cs[start..].iter().collect())
            }
            _ => Value::Null,
        },
        "substr" | "substring" | "mid" => substring(a),
        "instr" => match (sstr(a, 0), sstr(a, 1)) {
            (Some(s), Some(sub)) => Value::Int(match s.find(&sub) {
                Some(b) => s[..b].chars().count() as i64 + 1,
                None => 0,
            }),
            _ => Value::Null,
        },
        "locate" | "position" => match (sstr(a, 0), sstr(a, 1)) {
            (Some(sub), Some(s)) => Value::Int(match s.find(&sub) {
                Some(b) => s[..b].chars().count() as i64 + 1,
                None => 0,
            }),
            _ => Value::Null,
        },
        "lpad" => pad(a, true),
        "rpad" => pad(a, false),
        "ascii" => match sstr(a, 0) {
            Some(s) => Value::Int(s.bytes().next().unwrap_or(0) as i64),
            None => Value::Null,
        },
        // ---- math ----
        "abs" => match a.first() {
            Some(Value::Int(i)) => Value::Int(i.abs()),
            Some(v) => v
                .as_f64()
                .map(|x| Value::Float(x.abs()))
                .unwrap_or(Value::Null),
            None => Value::Null,
        },
        "ceil" | "ceiling" => math1i(a, f64::ceil),
        "floor" => math1i(a, f64::floor),
        "sign" => match nnum(a, 0) {
            Some(x) => Value::Int(if x > 0.0 {
                1
            } else if x < 0.0 {
                -1
            } else {
                0
            }),
            None => Value::Null,
        },
        "sqrt" => math1(a, f64::sqrt),
        "exp" => math1(a, f64::exp),
        "ln" | "log" if a.len() == 1 => math1(a, f64::ln),
        "log10" => math1(a, f64::log10),
        "log2" => math1(a, f64::log2),
        "pi" => Value::Float(std::f64::consts::PI),
        "power" | "pow" => math2(a, f64::powf),
        "mod" => match (nnum(a, 0), nnum(a, 1)) {
            (Some(x), Some(y)) if y != 0.0 => {
                if a.iter().all(|v| matches!(v, Value::Int(_))) {
                    Value::Int((x as i64) % (y as i64))
                } else {
                    Value::Float(x % y)
                }
            }
            _ => Value::Null,
        },
        "round" => match nnum(a, 0) {
            Some(x) => {
                let d = nnum(a, 1).unwrap_or(0.0) as i32;
                let m = 10f64.powi(d);
                let r = (x * m).round() / m;
                if d <= 0 {
                    Value::Int(r as i64)
                } else {
                    Value::Float(r)
                }
            }
            None => Value::Null,
        },
        "truncate" => match (nnum(a, 0), nnum(a, 1)) {
            (Some(x), Some(d)) => {
                let m = 10f64.powi(d as i32);
                let r = (x * m).trunc() / m;
                if (d as i32) <= 0 {
                    Value::Int(r as i64)
                } else {
                    Value::Float(r)
                }
            }
            _ => Value::Null,
        },
        "rand" => Value::Float(rand_f64()),
        "greatest" | "least" => {
            if a.is_empty() || a.iter().any(|v| v.is_null()) {
                Value::Null
            } else {
                let want_max = name == "greatest";
                let mut best = a[0].clone();
                for v in &a[1..] {
                    if let Some(ord) = v.compare(&best) {
                        if (want_max && ord == Ordering::Greater)
                            || (!want_max && ord == Ordering::Less)
                        {
                            best = v.clone();
                        }
                    }
                }
                best
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(out))
}

fn substring(a: &[Value]) -> Value {
    let s = match sstr(a, 0) {
        Some(s) => s,
        None => return Value::Null,
    };
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    let pos = match nnum(a, 1) {
        Some(p) => p as i64,
        None => return Value::Null,
    };
    let start = if pos < 0 {
        (len + pos).max(0)
    } else if pos > 0 {
        pos - 1
    } else {
        0
    };
    let start = start.clamp(0, len) as usize;
    let take = match nnum(a, 2) {
        Some(l) => (l as i64).max(0) as usize,
        None => chars.len().saturating_sub(start),
    };
    Value::Text(chars[start..].iter().take(take).collect())
}

fn pad(a: &[Value], left: bool) -> Value {
    let s = match sstr(a, 0) {
        Some(s) => s,
        None => return Value::Null,
    };
    let len = match nnum(a, 1) {
        Some(n) => n.max(0.0) as usize,
        None => return Value::Null,
    };
    let padstr = sstr(a, 2).unwrap_or_else(|| " ".to_string());
    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= len {
        return Value::Text(chars.into_iter().take(len).collect());
    }
    if padstr.is_empty() {
        return Value::Text(s);
    }
    let need = len - chars.len();
    let padding: String = padstr.chars().cycle().take(need).collect();
    Value::Text(if left {
        format!("{padding}{s}")
    } else {
        format!("{s}{padding}")
    })
}

fn cast_value(v: Value, ty: &sqlparser::ast::DataType) -> Result<Value> {
    use elyra_core::datetime as dt;
    if v.is_null() {
        return Ok(Value::Null);
    }
    let tn = ty.to_string().to_ascii_uppercase();
    let out = if tn.starts_with("CHAR")
        || tn.starts_with("VARCHAR")
        || tn.contains("TEXT")
        || tn == "STRING"
        || tn.starts_with("NCHAR")
        || tn.starts_with("NVARCHAR")
    {
        Value::Text(v.to_wire_string().unwrap_or_default())
    } else if tn.contains("INT") || tn.contains("SIGNED") {
        match &v {
            Value::Int(i) => Value::Int(*i),
            _ => v
                .as_f64()
                .map(|x| Value::Int(x as i64))
                .or_else(|| {
                    v.to_wire_string()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        .map(Value::Int)
                })
                .unwrap_or(Value::Null),
        }
    } else if tn.contains("DOUBLE")
        || tn.contains("FLOAT")
        || tn.contains("REAL")
        || tn.contains("DECIMAL")
        || tn.contains("NUMERIC")
        || tn.contains("DEC")
    {
        v.as_f64().map(Value::Float).unwrap_or(Value::Null)
    } else if tn.starts_with("DATETIME") || tn.starts_with("TIMESTAMP") {
        match &v {
            Value::DateTime(_) => v,
            Value::Date(d) => Value::DateTime(*d as i64 * 86_400_000_000),
            _ => v
                .to_wire_string()
                .and_then(|s| dt::parse_datetime(&s))
                .map(Value::DateTime)
                .unwrap_or(Value::Null),
        }
    } else if tn.starts_with("DATE") {
        match &v {
            Value::Date(_) => v,
            Value::DateTime(m) => Value::Date(m.div_euclid(86_400_000_000) as i32),
            _ => v
                .to_wire_string()
                .and_then(|s| dt::parse_date(&s))
                .map(Value::Date)
                .unwrap_or(Value::Null),
        }
    } else if tn.starts_with("TIME") {
        match &v {
            Value::Time(_) => v,
            _ => v
                .to_wire_string()
                .and_then(|s| dt::parse_time(&s))
                .map(Value::Time)
                .unwrap_or(Value::Null),
        }
    } else {
        return Err(Error::Unsupported(format!("unsupported CAST target: {tn}")));
    };
    Ok(out)
}

fn fill_random(buf: &mut [u8]) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEED: AtomicU64 = AtomicU64::new(0);
    let mut x = SEED.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
    if x == 0 {
        x = 0xdead_beef_cafe_babe;
    }
    for chunk in buf.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let bytes = x.to_le_bytes();
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = bytes[i];
        }
    }
}

fn rand_f64() -> f64 {
    let mut b = [0u8; 8];
    fill_random(&mut b);
    (u64::from_le_bytes(b) >> 11) as f64 / (1u64 << 53) as f64
}

fn gen_uuid() -> String {
    let mut b = [0u8; 16];
    fill_random(&mut b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
        b[14], b[15]
    )
}

fn function_arg_exprs(f: &sqlparser::ast::Function) -> Result<Vec<&Expr>> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let list = match &f.args {
        FunctionArguments::List(list) => list,
        FunctionArguments::None => return Ok(Vec::new()),
        FunctionArguments::Subquery(_) => {
            return Err(Error::Unsupported("subquery function argument".into()))
        }
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
                .map(|t| {
                    t.trim()
                        .parse::<f32>()
                        .map_err(|_| Error::Vector(format!("bad vector element: {t}")))
                })
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
    if let Some(i) = schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
    {
        return Ok(i);
    }
    // Match on the bare (unqualified) name on both sides, so a qualified
    // reference like `t.col` resolves against a single-table (bare) schema and
    // a bare `col` resolves against a joined (`alias.col`) schema.
    let bare = |n: &str| n.rsplit('.').next().unwrap_or(n).to_string();
    let target = bare(name);
    let hits: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| bare(&c.name).eq_ignore_ascii_case(&target))
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
        other => Err(Error::Unsupported(format!(
            "literal not supported: {other}"
        ))),
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
        Eq => Ok(Value::Bool(
            cmp(&l, &r)?.map(|o| o.is_eq()).unwrap_or(false),
        )),
        NotEq => Ok(Value::Bool(cmp(&l, &r)?.map(|o| o.is_ne()).unwrap_or(true))),
        Lt => Ok(Value::Bool(
            cmp(&l, &r)?.map(|o| o.is_lt()).unwrap_or(false),
        )),
        LtEq => Ok(Value::Bool(
            cmp(&l, &r)?.map(|o| o.is_le()).unwrap_or(false),
        )),
        Gt => Ok(Value::Bool(
            cmp(&l, &r)?.map(|o| o.is_gt()).unwrap_or(false),
        )),
        GtEq => Ok(Value::Bool(
            cmp(&l, &r)?.map(|o| o.is_ge()).unwrap_or(false),
        )),
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
