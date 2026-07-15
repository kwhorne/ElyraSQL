//! Row-aware expression evaluation for `WHERE` predicates.
//!
//! Unlike [`crate::eval`] (literals only), this evaluates expressions that
//! reference columns, resolved against a row + its schema.

use elyra_core::{Collation, Error, Result, Schema, Value};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value as SqlValue};

/// Evaluate `expr` against a row. Column identifiers resolve via `schema`.
pub fn eval_row(expr: &Expr, schema: &Schema, row: &[Value]) -> Result<Value> {
    match expr {
        Expr::Value(v) => literal(v),
        Expr::Nested(e) => eval_row(e, schema, row),
        Expr::Identifier(id) => {
            // System variables: `@@var`, `@@session.var`, `@@global.var`.
            if id.value.starts_with("@@") {
                return Ok(system_var(&id.value));
            }
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
            // `@@session.var` / `@@global.var` arrive as a compound identifier.
            if parts.first().is_some_and(|p| p.value.starts_with("@@")) {
                return Ok(system_var(
                    &parts.last().map(|p| p.value.clone()).unwrap_or_default(),
                ));
            }
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
            // Row/tuple form: `(a, b) IN ((x, y), (...))` -> element-wise equality.
            if let Expr::Tuple(le) = expr.as_ref() {
                let lhs: Vec<Value> = le
                    .iter()
                    .map(|e| eval_row(e, schema, row))
                    .collect::<Result<_>>()?;
                let mut found = false;
                for item in list {
                    if let Expr::Tuple(re) = item {
                        if re.len() == lhs.len() {
                            let mut all_eq = true;
                            for (lv, re_e) in lhs.iter().zip(re) {
                                let rv = eval_row(re_e, schema, row)?;
                                if lv.compare(&rv) != Some(std::cmp::Ordering::Equal) {
                                    all_eq = false;
                                    break;
                                }
                            }
                            if all_eq {
                                found = true;
                                break;
                            }
                        }
                    }
                }
                return Ok(Value::Bool(found != *negated));
            }
            let v = eval_row(expr, schema, row)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            // Three-valued: if no element equals `v` but the list contains a NULL,
            // the result is NULL (UNKNOWN), not FALSE -- e.g. `1 IN (NULL, 2)`.
            let mut found = false;
            let mut saw_null = false;
            for item in list {
                let rv = eval_row(item, schema, row)?;
                if rv.is_null() {
                    saw_null = true;
                    continue;
                }
                if v.compare(&rv) == Some(std::cmp::Ordering::Equal) {
                    found = true;
                    break;
                }
            }
            if found {
                return Ok(Value::Bool(!*negated));
            }
            if saw_null {
                return Ok(Value::Null);
            }
            Ok(Value::Bool(*negated))
        }
        Expr::IsNull(e) => Ok(Value::Bool(eval_row(e, schema, row)?.is_null())),
        Expr::IsNotNull(e) => Ok(Value::Bool(!eval_row(e, schema, row)?.is_null())),
        // `x IS [NOT] TRUE/FALSE/UNKNOWN` (NULL is UNKNOWN, never TRUE/FALSE).
        Expr::IsTrue(e) => Ok(Value::Bool(truthy(&eval_row(e, schema, row)?))),
        Expr::IsNotTrue(e) => Ok(Value::Bool(!truthy(&eval_row(e, schema, row)?))),
        Expr::IsFalse(e) => {
            let v = eval_row(e, schema, row)?;
            Ok(Value::Bool(!v.is_null() && !truthy(&v)))
        }
        Expr::IsNotFalse(e) => {
            let v = eval_row(e, schema, row)?;
            Ok(Value::Bool(v.is_null() || truthy(&v)))
        }
        Expr::IsUnknown(e) => Ok(Value::Bool(eval_row(e, schema, row)?.is_null())),
        Expr::IsNotUnknown(e) => Ok(Value::Bool(!eval_row(e, schema, row)?.is_null())),
        Expr::UnaryOp { op, expr } => {
            let v = eval_row(expr, schema, row)?;
            match (op, v) {
                (UnaryOperator::Not, v) => Ok(if v.is_null() {
                    Value::Null // NOT NULL = NULL (three-valued logic)
                } else {
                    Value::Bool(!truthy(&v))
                }),
                (UnaryOperator::Minus, Value::Int(i)) => {
                    i.checked_neg().map(Value::Int).ok_or_else(|| {
                        Error::OutOfRange(format!("BIGINT value is out of range in '-({i})'"))
                    })
                }
                (UnaryOperator::Minus, Value::Float(f)) => Ok(Value::Float(-f)),
                // Negating a large unsigned literal (e.g. `-9223372036854775808`,
                // parsed as UInt because it exceeds i64::MAX): valid iff it fits
                // the signed range once negated, else out of range like MySQL.
                (UnaryOperator::Minus, Value::UInt(u)) => {
                    if u <= (i64::MAX as u64) + 1 {
                        Ok(Value::Int((u as i128).wrapping_neg() as i64))
                    } else {
                        Err(Error::OutOfRange(format!(
                            "BIGINT value is out of range in '-({u})'"
                        )))
                    }
                }
                (UnaryOperator::Plus, v) => Ok(v),
                (UnaryOperator::PGBitwiseNot, v) => Ok(match v.as_f64() {
                    Some(x) => Value::Int(!(x as i64)),
                    None => Value::Null,
                }),
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
            let coll = expr_collation(expr, schema).unwrap_or(Collation::Ci);
            // `x BETWEEN a AND b` is `x >= a AND x <= b` with three-valued logic:
            // a NULL bound yields NULL unless the other test already forces FALSE.
            let ge = cmp(&v, &lo, coll)?.map(|o| o.is_ge());
            let le = cmp(&v, &hi, coll)?.map(|o| o.is_le());
            let inside = match (ge, le) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            };
            Ok(match inside {
                None => Value::Null,
                Some(b) => Value::Bool(if *negated { !b } else { b }),
            })
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Arrow => {
                json_path(eval_row(left, schema, row)?, right, schema, row, false)
            }
            BinaryOperator::LongArrow => {
                json_path(eval_row(left, schema, row)?, right, schema, row, true)
            }
            // Date +/- INTERVAL, and INTERVAL + Date.
            BinaryOperator::Plus | BinaryOperator::Minus
                if matches!(right.as_ref(), Expr::Interval(_)) =>
            {
                let Expr::Interval(iv) = right.as_ref() else {
                    unreachable!()
                };
                let sign = if matches!(op, BinaryOperator::Minus) {
                    -1
                } else {
                    1
                };
                let base = eval_row(left, schema, row)?;
                let n = eval_row(&iv.value, schema, row)?.as_f64().unwrap_or(0.0) as i64;
                let unit = iv
                    .leading_field
                    .as_ref()
                    .map(|u| u.to_string().to_ascii_uppercase())
                    .unwrap_or_else(|| "DAY".into());
                Ok(apply_interval(base, sign * n, &unit))
            }
            BinaryOperator::Plus if matches!(left.as_ref(), Expr::Interval(_)) => {
                let Expr::Interval(iv) = left.as_ref() else {
                    unreachable!()
                };
                let base = eval_row(right, schema, row)?;
                let n = eval_row(&iv.value, schema, row)?.as_f64().unwrap_or(0.0) as i64;
                let unit = iv
                    .leading_field
                    .as_ref()
                    .map(|u| u.to_string().to_ascii_uppercase())
                    .unwrap_or_else(|| "DAY".into());
                Ok(apply_interval(base, n, &unit))
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
        Expr::Extract { field, expr, .. } => {
            let v = eval_row(expr, schema, row)?;
            Ok(date_part(&v, &field.to_string()))
        }
        Expr::MatchAgainst {
            columns,
            match_value,
            opt_search_modifier,
        } => {
            use sqlparser::ast::SearchModifier;
            let boolean = matches!(opt_search_modifier, Some(SearchModifier::InBooleanMode));
            // Collect the searchable text from the named columns.
            let mut doc = String::new();
            for col in columns {
                if let Some(i) = schema
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(&col.value))
                {
                    if let Some(s) = row.get(i).and_then(|v| v.to_wire_string()) {
                        doc.push(' ');
                        doc.push_str(&s);
                    }
                }
            }
            let words: std::collections::HashSet<String> =
                crate::ft::tokenize(&doc).into_iter().collect();
            let query = match match_value {
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => s.clone(),
                other => other.to_string(),
            };
            let mut score = 0.0f64;
            let mut ok = true;
            for raw in query.split_whitespace() {
                let (required, excluded, term) = if boolean {
                    match raw.strip_prefix('+') {
                        Some(t) => (true, false, t),
                        None => match raw.strip_prefix('-') {
                            Some(t) => (false, true, t),
                            None => (false, false, raw),
                        },
                    }
                } else {
                    (false, false, raw)
                };
                let cleaned: String = term.chars().filter(|c| c.is_alphanumeric()).collect();
                if cleaned.is_empty() {
                    continue;
                }
                let term = crate::ft::stem(&cleaned);
                let present = words.contains(&term);
                if (excluded && present) || (required && !present) {
                    ok = false;
                } else if present {
                    score += 1.0;
                }
            }
            let relevance = if ok { score } else { 0.0 };
            Ok(Value::Float(relevance))
        }
        Expr::RLike {
            negated,
            expr,
            pattern,
            ..
        } => {
            let text = eval_row(expr, schema, row)?;
            let pat = eval_row(pattern, schema, row)?;
            if text.is_null() || pat.is_null() {
                return Ok(Value::Null);
            }
            let (t, p) = (
                text.to_wire_string().unwrap_or_default(),
                pat.to_wire_string().unwrap_or_default(),
            );
            let re = regex::Regex::new(&p)
                .map_err(|e| Error::Query(format!("invalid regular expression: {e}")))?;
            Ok(Value::Bool(re.is_match(&t) != *negated))
        }
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
        // CONVERT(expr, type) and CONVERT(expr USING charset). A charset
        // conversion is a no-op here (everything is UTF-8); a target type casts.
        Expr::Convert {
            expr, data_type, ..
        } => {
            let v = eval_row(expr, schema, row)?;
            match data_type {
                Some(ty) => cast_value(v, ty),
                None => Ok(v),
            }
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        }
        | Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
            ..
        } => {
            let text = eval_row(expr, schema, row)?;
            let pat = eval_row(pattern, schema, row)?;
            if text.is_null() || pat.is_null() {
                return Ok(Value::Null);
            }
            let esc = escape_char.as_ref().and_then(|s| s.chars().next());
            // Default collation is case-insensitive (utf8mb4_general_ci), as is
            // ILIKE, so both match case-insensitively.
            let m = like_eval(
                &text.to_wire_string().unwrap_or_default(),
                &pat.to_wire_string().unwrap_or_default(),
                esc,
                true,
            );
            Ok(Value::Bool(m != *negated))
        }
        other => Err(Error::Unsupported(format!(
            "expression not supported in WHERE: {other}"
        ))),
    }
}

enum LikeTok {
    Lit(char),
    Any,
    One,
}

/// SQL `LIKE` matching: `%` matches any run, `_` matches one character, an
/// optional escape character makes the next `%`/`_`/escape literal. Case-
/// insensitive when `ci` (the default collation). Iterative with `%`
/// backtracking (no per-row regex compilation).
fn like_eval(text: &str, pattern: &str, esc: Option<char>, ci: bool) -> bool {
    let fold = |c: char| if ci { c.to_ascii_lowercase() } else { c };
    let t: Vec<char> = text.chars().map(fold).collect();
    let esc = esc.map(fold);
    let pchars: Vec<char> = pattern.chars().map(fold).collect();

    let mut toks: Vec<LikeTok> = Vec::with_capacity(pchars.len());
    let mut i = 0;
    while i < pchars.len() {
        let c = pchars[i];
        if Some(c) == esc && i + 1 < pchars.len() {
            toks.push(LikeTok::Lit(pchars[i + 1]));
            i += 2;
            continue;
        }
        toks.push(match c {
            '%' => LikeTok::Any,
            '_' => LikeTok::One,
            _ => LikeTok::Lit(c),
        });
        i += 1;
    }

    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star, mut mark) = (usize::MAX, 0usize);
    while ti < t.len() {
        if pi < toks.len() {
            match toks[pi] {
                LikeTok::Any => {
                    star = pi;
                    mark = ti;
                    pi += 1;
                    continue;
                }
                LikeTok::One => {
                    ti += 1;
                    pi += 1;
                    continue;
                }
                LikeTok::Lit(c) if c == t[ti] => {
                    ti += 1;
                    pi += 1;
                    continue;
                }
                _ => {}
            }
        }
        if star != usize::MAX {
            pi = star + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < toks.len() && matches!(toks[pi], LikeTok::Any) {
        pi += 1;
    }
    pi == toks.len()
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
        "isnull" if args_exprs.len() == 1 => {
            let v = eval_row(args_exprs[0], schema, row)?;
            return Ok(Value::Int(v.is_null() as i64));
        }
        "strcmp" if args_exprs.len() == 2 => {
            let a = eval_row(args_exprs[0], schema, row)?;
            let b = eval_row(args_exprs[1], schema, row)?;
            if a.is_null() || b.is_null() {
                return Ok(Value::Null);
            }
            let o = a.compare_coll(&b, Collation::Ci);
            return Ok(match o {
                Some(std::cmp::Ordering::Less) => Value::Int(-1),
                Some(std::cmp::Ordering::Greater) => Value::Int(1),
                _ => Value::Int(0),
            });
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
        "timestampdiff" if args_exprs.len() == 3 => {
            let unit = unit_name(args_exprs[0]);
            let a = eval_row(args_exprs[1], schema, row)?;
            let b = eval_row(args_exprs[2], schema, row)?;
            return Ok(timestampdiff(&unit, &a, &b));
        }
        "timestampadd" if args_exprs.len() == 3 => {
            let unit = unit_name(args_exprs[0]);
            let n = eval_row(args_exprs[1], schema, row)?
                .as_f64()
                .unwrap_or(0.0) as i64;
            let base = eval_row(args_exprs[2], schema, row)?;
            return Ok(apply_interval(base, n, &unit));
        }
        "date_add" | "adddate" | "date_sub" | "subdate" if args_exprs.len() == 2 => {
            let base = eval_row(args_exprs[0], schema, row)?;
            let sub = matches!(name.as_str(), "date_sub" | "subdate");
            if let Expr::Interval(iv) = args_exprs[1] {
                let n = eval_row(&iv.value, schema, row)?.as_f64().unwrap_or(0.0) as i64;
                let unit = iv
                    .leading_field
                    .as_ref()
                    .map(|u| u.to_string().to_ascii_uppercase())
                    .unwrap_or_else(|| "DAY".into());
                return Ok(apply_interval(base, if sub { -n } else { n }, &unit));
            }
            let n = eval_row(args_exprs[1], schema, row)?
                .as_f64()
                .unwrap_or(0.0) as i64;
            return Ok(apply_interval(base, if sub { -n } else { n }, "DAY"));
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

/// Resolve a MySQL system variable reference (`@@var`, `@@session.var`,
/// `@@global.var`) to a value. Common variables that clients and ORMs probe on
/// connect are given sensible values; unknown ones return NULL (lenient, so an
/// unfamiliar `@@variable` never fails a session-setup query).
pub fn system_var(raw: &str) -> Value {
    let n = raw
        .trim_start_matches("@@")
        .trim_start_matches("session.")
        .trim_start_matches("global.")
        .trim_start_matches("local.")
        .to_ascii_lowercase();
    let text = |s: &str| Value::Text(s.to_string());
    match n.as_str() {
        "version" => text(elyra_core::SERVER_VERSION),
        "version_comment" => text("ElyraSQL"),
        "version_compile_os" => text("Linux"),
        "version_compile_machine" => text("x86_64"),
        "protocol_version" => Value::Int(10),
        "autocommit" => Value::Int(1),
        "sql_mode" => text("STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"),
        "tx_isolation" | "transaction_isolation" => text("REPEATABLE-READ"),
        "tx_read_only" | "transaction_read_only" => Value::Int(0),
        "max_allowed_packet" => Value::Int(67_108_864),
        "max_connections" => Value::Int(151),
        "wait_timeout" | "interactive_timeout" => Value::Int(28800),
        "net_write_timeout" => Value::Int(60),
        "net_read_timeout" => Value::Int(30),
        "lower_case_table_names" => Value::Int(0),
        "character_set_client"
        | "character_set_connection"
        | "character_set_results"
        | "character_set_server"
        | "character_set_database" => text("utf8mb4"),
        "collation_connection" | "collation_server" | "collation_database" => {
            text("utf8mb4_general_ci")
        }
        "time_zone" => text("SYSTEM"),
        "system_time_zone" => text("UTC"),
        "license" => text("MIT"),
        "have_query_cache" => text("NO"),
        "have_ssl" | "have_openssl" => text("DISABLED"),
        "hostname" => text("elyrasql"),
        "port" => Value::Int(3307),
        "sql_auto_is_null" => Value::Int(0),
        "foreign_key_checks" | "unique_checks" => Value::Int(1),
        "init_connect" => text(""),
        _ => Value::Null,
    }
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
        // A non-finite result (e.g. SQRT(-1), LN(0), LN(-1)) is out of domain;
        // MySQL returns NULL rather than NaN/inf.
        Some(x) => {
            let y = f(x);
            if y.is_finite() {
                Value::Float(y)
            } else {
                Value::Null
            }
        }
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
        // ---- server / session info ----
        "version" => Value::Text(elyra_core::SERVER_VERSION.into()),
        "database" | "schema" => Value::Text("elyra".into()),
        "user" | "current_user" | "session_user" | "system_user" => Value::Text("root@%".into()),
        "connection_id" => Value::Int(1),
        // Normally resolved from session state by the pre-pass before evaluation;
        // this fallback (0) only applies in positions the pre-pass doesn't reach.
        "last_insert_id" | "row_count" | "found_rows" => Value::Int(0),
        "current_role" => Value::Text("NONE".into()),
        // ---- date parts / arithmetic ----
        "year" | "month" | "day" | "dayofmonth" | "hour" | "minute" | "second" | "quarter"
        | "dayofweek" | "dayofyear" | "weekday" => date_part(&a[0], name),
        "date" => match to_micros(&a[0]) {
            Some(m) => Value::Date(m.div_euclid(86_400_000_000) as i32),
            None => Value::Null,
        },
        "time" => match to_micros(&a[0]) {
            Some(m) => Value::Time(m.rem_euclid(86_400_000_000)),
            None => Value::Null,
        },
        "datediff" => match (to_micros(&a[0]), to_micros(&a[1])) {
            (Some(x), Some(y)) => {
                Value::Int(x.div_euclid(86_400_000_000) - y.div_euclid(86_400_000_000))
            }
            _ => Value::Null,
        },
        "last_day" => match to_micros(&a[0]) {
            Some(m) => {
                let (y, mo, _) =
                    elyra_core::datetime::civil_from_days(m.div_euclid(86_400_000_000));
                Value::Date(
                    elyra_core::datetime::days_from_civil(y, mo, days_in_month(y, mo)) as i32,
                )
            }
            None => Value::Null,
        },
        "date_format" => match (to_micros(&a[0]), sstr(a, 1)) {
            (Some(m), Some(fmt)) => Value::Text(format_dt(m, &fmt)),
            _ => Value::Null,
        },
        "week" => match to_micros(&a[0]) {
            Some(m) => {
                let mode = nnum(a, 1).unwrap_or(0.0) as i64;
                Value::Int(calc_week(m.div_euclid(86_400_000_000), mode).1)
            }
            None => Value::Null,
        },
        "yearweek" => match to_micros(&a[0]) {
            Some(m) => {
                let mode = (nnum(a, 1).unwrap_or(0.0) as i64) | 2;
                let (y, w) = calc_week(m.div_euclid(86_400_000_000), mode);
                Value::Int(y * 100 + w)
            }
            None => Value::Null,
        },
        "str_to_date" => match (sstr(a, 0), sstr(a, 1)) {
            (Some(s), Some(fmt)) => str_to_date(&s, &fmt),
            _ => Value::Null,
        },
        "substring_index" => match (sstr(a, 0), sstr(a, 1), nnum(a, 2)) {
            (Some(s), Some(delim), Some(count)) => {
                Value::Text(substring_index(&s, &delim, count as i64))
            }
            _ => Value::Null,
        },
        "field" => {
            if a.is_empty() || a[0].is_null() {
                Value::Int(0)
            } else {
                let pos = a[1..]
                    .iter()
                    .position(|v| a[0].compare(v) == Some(std::cmp::Ordering::Equal));
                Value::Int(pos.map(|p| p as i64 + 1).unwrap_or(0))
            }
        }
        "elt" => match nnum(a, 0) {
            Some(n) => {
                let idx = n as usize;
                if idx >= 1 && idx < a.len() {
                    a[idx].clone()
                } else {
                    Value::Null
                }
            }
            None => Value::Null,
        },
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
        // MySQL: LENGTH/OCTET_LENGTH are byte lengths; CHAR_LENGTH is characters.
        "length" | "octet_length" => match sstr(a, 0) {
            Some(s) => Value::Int(s.len() as i64),
            None => Value::Null,
        },
        "char_length" | "character_length" => match sstr(a, 0) {
            Some(s) => Value::Int(s.chars().count() as i64),
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
        "bit_count" => match nnum(a, 0) {
            Some(x) => Value::Int((x as i64 as u64).count_ones() as i64),
            None => Value::Null,
        },
        // TO_DAYS: day number since year 0 (MySQL: TO_DAYS('1970-01-01') = 719528).
        "to_days" => match sstr(a, 0).and_then(|s| elyra_core::datetime::parse_date(&s)) {
            Some(d) => Value::Int(d as i64 + 719_528),
            None => Value::Null,
        },
        // INSERT(str, pos, len, newstr): replace `len` chars at 1-based `pos`.
        "insert" => string_insert(a),
        // CONV(n, from_base, to_base): base conversion, returned as text.
        "conv" => conv_fn(a),
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
        // ORD: for a multi-byte first character, MySQL sums the bytes big-endian;
        // for single-byte it equals ASCII.
        "ord" => match sstr(a, 0) {
            Some(s) => match s.chars().next() {
                Some(c) => {
                    let mut buf = [0u8; 4];
                    let bytes = c.encode_utf8(&mut buf).as_bytes();
                    let mut n: i64 = 0;
                    for &b in bytes {
                        n = n * 256 + b as i64;
                    }
                    Value::Int(n)
                }
                None => Value::Int(0),
            },
            None => Value::Null,
        },
        "bin" => match nnum(a, 0) {
            Some(x) => Value::Text(to_base_unsigned(x as i64 as u64, 2)),
            None => Value::Null,
        },
        "oct" => match nnum(a, 0) {
            Some(x) => Value::Text(to_base_unsigned(x as i64 as u64, 8)),
            None => Value::Null,
        },
        "crc32" => match sstr(a, 0) {
            Some(s) => Value::Int(crc32(s.as_bytes()) as i64),
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
        // ---- spatial (WKT POINT geometry) ----
        "point" => match (nnum(a, 0), nnum(a, 1)) {
            (Some(x), Some(y)) => Value::Text(format!("POINT({x} {y})")),
            _ => Value::Null,
        },
        "st_x" | "st_y" => match sstr(a, 0).as_deref().and_then(parse_point) {
            Some((x, y)) => Value::Float(if name == "st_x" { x } else { y }),
            None => Value::Null,
        },
        "st_distance" => {
            match (
                sstr(a, 0).as_deref().and_then(parse_point),
                sstr(a, 1).as_deref().and_then(parse_point),
            ) {
                (Some((x1, y1)), Some((x2, y2))) => {
                    Value::Float(((x1 - x2).powi(2) + (y1 - y2).powi(2)).sqrt())
                }
                _ => Value::Null,
            }
        }
        "st_astext" | "st_aswkt" => match sstr(a, 0) {
            Some(s) => Value::Text(s),
            None => Value::Null,
        },
        "st_geomfromtext" | "st_geometryfromtext" | "st_pointfromtext" | "st_geomfromwkt" => {
            match sstr(a, 0) {
                Some(s) if parse_point(&s).is_some() => Value::Text(s),
                Some(_) => return Err(Error::Query("invalid geometry WKT".into())),
                None => Value::Null,
            }
        }
        "radians" => match nnum(a, 0) {
            Some(x) => Value::Float(x.to_radians()),
            None => Value::Null,
        },
        "degrees" => match nnum(a, 0) {
            Some(x) => Value::Float(x.to_degrees()),
            None => Value::Null,
        },
        "char" => {
            let s: String = a
                .iter()
                .filter_map(|v| v.as_f64())
                .filter_map(|n| char::from_u32(n as u32))
                .collect();
            Value::Text(s)
        }
        "time_to_sec" => match &a[0] {
            Value::Time(m) => Value::Int(m / 1_000_000),
            Value::Null => Value::Null,
            v => match sstr(std::slice::from_ref(v), 0).and_then(|s| parse_hms(&s)) {
                Some(sec) => Value::Int(sec),
                None => Value::Null,
            },
        },
        "sec_to_time" => match nnum(a, 0) {
            Some(n) => Value::Time((n as i64).rem_euclid(86_400) * 1_000_000),
            None => Value::Null,
        },
        "soundex" => match sstr(a, 0) {
            Some(s) => Value::Text(soundex(&s)),
            None => Value::Null,
        },
        "regexp_replace" => match (sstr(a, 0), sstr(a, 1), sstr(a, 2)) {
            (Some(s), Some(pat), Some(rep)) => match regex::Regex::new(&pat) {
                Ok(re) => Value::Text(re.replace_all(&s, rep.as_str()).into_owned()),
                Err(_) => Value::Null,
            },
            _ => Value::Null,
        },
        "regexp_substr" => match (sstr(a, 0), sstr(a, 1)) {
            (Some(s), Some(pat)) => match regex::Regex::new(&pat) {
                Ok(re) => re
                    .find(&s)
                    .map(|m| Value::Text(m.as_str().to_string()))
                    .unwrap_or(Value::Null),
                Err(_) => Value::Null,
            },
            _ => Value::Null,
        },
        "from_unixtime" => match nnum(a, 0) {
            Some(secs) => {
                let m = (secs as i64).saturating_mul(1_000_000);
                match sstr(a, 1) {
                    Some(fmt) => Value::Text(format_dt(m, &fmt)),
                    None => Value::DateTime(m),
                }
            }
            None => Value::Null,
        },
        "dayname" => match to_micros(&a[0]) {
            Some(m) => {
                let wd = ((m.div_euclid(86_400_000_000).rem_euclid(7) + 3) % 7) as usize;
                Value::Text(
                    [
                        "Monday",
                        "Tuesday",
                        "Wednesday",
                        "Thursday",
                        "Friday",
                        "Saturday",
                        "Sunday",
                    ][wd]
                        .into(),
                )
            }
            None => Value::Null,
        },
        "monthname" => match to_micros(&a[0]) {
            Some(m) => {
                let (_, mo, _) =
                    elyra_core::datetime::civil_from_days(m.div_euclid(86_400_000_000));
                match [
                    "January",
                    "February",
                    "March",
                    "April",
                    "May",
                    "June",
                    "July",
                    "August",
                    "September",
                    "October",
                    "November",
                    "December",
                ]
                .get((mo as usize).wrapping_sub(1))
                {
                    Some(s) => Value::Text((*s).into()),
                    None => Value::Null,
                }
            }
            None => Value::Null,
        },
        "md5" => match sstr(a, 0) {
            Some(s) => {
                use md5::{Digest, Md5};
                Value::Text(hex_lower(&Md5::digest(s.as_bytes())))
            }
            None => Value::Null,
        },
        "sha" | "sha1" => match sstr(a, 0) {
            Some(s) => {
                use sha1::{Digest, Sha1};
                Value::Text(hex_lower(&Sha1::digest(s.as_bytes())))
            }
            None => Value::Null,
        },
        "sha2" => match (sstr(a, 0), nnum(a, 1)) {
            (Some(s), Some(bits)) => {
                use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
                let h = match bits as u32 {
                    0 | 256 => hex_lower(&Sha256::digest(s.as_bytes())),
                    224 => hex_lower(&Sha224::digest(s.as_bytes())),
                    384 => hex_lower(&Sha384::digest(s.as_bytes())),
                    512 => hex_lower(&Sha512::digest(s.as_bytes())),
                    _ => return Ok(Some(Value::Null)),
                };
                Value::Text(h)
            }
            _ => Value::Null,
        },
        "find_in_set" => match (sstr(a, 0), sstr(a, 1)) {
            (Some(needle), Some(set)) => {
                let pos = set.split(',').position(|x| x == needle);
                Value::Int(pos.map(|p| p as i64 + 1).unwrap_or(0))
            }
            _ => Value::Null,
        },
        "format" => match nnum(a, 0) {
            Some(n) => Value::Text(format_number(
                n,
                nnum(a, 1).unwrap_or(0.0).max(0.0) as usize,
            )),
            None => Value::Null,
        },
        "hex" => match &a[0] {
            Value::Null => Value::Null,
            Value::Int(i) => Value::Text(format!("{i:X}")),
            v => Value::Text(
                wire(v)
                    .unwrap_or_default()
                    .bytes()
                    .map(|b| format!("{b:02X}"))
                    .collect(),
            ),
        },
        "unhex" => match sstr(a, 0) {
            Some(s) => {
                let decoded: Option<Vec<u8>> = (0..s.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
                    .collect();
                match decoded {
                    Some(b) => match String::from_utf8(b.clone()) {
                        Ok(t) => Value::Text(t),
                        Err(_) => Value::Bytes(b),
                    },
                    None => Value::Null,
                }
            }
            None => Value::Null,
        },
        _ => return Ok(None),
    };
    Ok(Some(out))
}

/// Parse a WKT `POINT(x y)` into its coordinates.
fn parse_point(s: &str) -> Option<(f64, f64)> {
    let s = s.trim();
    if !s.to_ascii_lowercase().starts_with("point") {
        return None;
    }
    let open = s.find('(')?;
    let close = s.rfind(')')?;
    let inner = &s[open + 1..close];
    let mut it = inner.split_whitespace();
    let x = it.next()?.parse().ok()?;
    let y = it.next()?.parse().ok()?;
    Some((x, y))
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
    // MySQL: a 1-based position of 0 yields the empty string.
    if pos == 0 {
        return Value::Text(String::new());
    }
    let start = if pos < 0 { (len + pos).max(0) } else { pos - 1 };
    let start = start.clamp(0, len) as usize;
    let take = match nnum(a, 2) {
        Some(l) => (l as i64).max(0) as usize,
        None => chars.len().saturating_sub(start),
    };
    Value::Text(chars[start..].iter().take(take).collect())
}

/// SUBSTRING_INDEX(str, delim, count): substring before the count-th delimiter
/// (from the left if positive, from the right if negative).
fn string_insert(a: &[Value]) -> Value {
    let (Some(s), Some(pos), Some(len), Some(ins)) =
        (sstr(a, 0), nnum(a, 1), nnum(a, 2), sstr(a, 3))
    else {
        return Value::Null;
    };
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len() as i64;
    let pos = pos as i64;
    // MySQL: an out-of-range position returns the original string unchanged.
    if pos < 1 || pos > n {
        return Value::Text(s);
    }
    let start = (pos - 1) as usize;
    let len = len as i64;
    let del = if len < 0 {
        chars.len() - start
    } else {
        (len as usize).min(chars.len() - start)
    };
    let mut out: String = chars[..start].iter().collect();
    out.push_str(&ins);
    out.extend(chars[start + del..].iter());
    Value::Text(out)
}

fn conv_fn(a: &[Value]) -> Value {
    let (Some(s), Some(from), Some(to)) = (sstr(a, 0), nnum(a, 1), nnum(a, 2)) else {
        return Value::Null;
    };
    let from = from as i64;
    let to = to as i64;
    if !(2..=36).contains(&from.unsigned_abs()) || !(2..=36).contains(&to.unsigned_abs()) {
        return Value::Null;
    }
    let s = s.trim();
    // Parse in `from` base as an unsigned magnitude with an optional sign.
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let mut val: u64 = 0;
    let mut any = false;
    for c in digits.chars() {
        let d = c.to_digit(from.unsigned_abs() as u32);
        match d {
            Some(d) => {
                val = val.wrapping_mul(from.unsigned_abs()).wrapping_add(d as u64);
                any = true;
            }
            None => break, // MySQL stops at the first invalid digit
        }
    }
    if !any {
        return Value::Text("0".to_string());
    }
    // Signed interpretation only when the target base is negative.
    let out = if to < 0 {
        let signed = if neg {
            (val as i64).wrapping_neg()
        } else {
            val as i64
        };
        to_base_signed(signed, to.unsigned_abs())
    } else {
        let mag = if neg {
            (val as i64).wrapping_neg() as u64
        } else {
            val
        };
        to_base_unsigned(mag, to.unsigned_abs())
    };
    Value::Text(out)
}

/// CRC-32 (IEEE 802.3 / zlib), as used by MySQL's `CRC32()`.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn to_base_unsigned(mut n: u64, base: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let digits = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut out = Vec::new();
    while n > 0 {
        out.push(digits[(n % base) as usize]);
        n /= base;
    }
    out.reverse();
    String::from_utf8(out).unwrap()
}

fn to_base_signed(n: i64, base: u64) -> String {
    if n < 0 {
        format!("-{}", to_base_unsigned(n.unsigned_abs(), base))
    } else {
        to_base_unsigned(n as u64, base)
    }
}

fn substring_index(s: &str, delim: &str, count: i64) -> String {
    if delim.is_empty() || count == 0 {
        return String::new();
    }
    let parts: Vec<&str> = s.split(delim).collect();
    if count > 0 {
        let n = (count as usize).min(parts.len());
        parts[..n].join(delim)
    } else {
        let n = ((-count) as usize).min(parts.len());
        parts[parts.len() - n..].join(delim)
    }
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

/// MySQL string-to-integer coercion: an optional sign then leading ASCII digits;
/// anything non-numeric (or no digits) yields 0. Saturates on overflow.
fn str_to_int(v: &Value) -> i64 {
    let s = v.to_wire_string().unwrap_or_default();
    let b = s.trim().as_bytes();
    let mut i = 0;
    let neg = matches!(b.first(), Some(b'-'));
    if matches!(b.first(), Some(b'+') | Some(b'-')) {
        i = 1;
    }
    let start = i;
    let mut n: i64 = 0;
    while i < b.len() && b[i].is_ascii_digit() {
        n = n.saturating_mul(10).saturating_add((b[i] - b'0') as i64);
        i += 1;
    }
    if i == start {
        return 0;
    }
    if neg {
        -n
    } else {
        n
    }
}

fn cast_value(v: Value, ty: &sqlparser::ast::DataType) -> Result<Value> {
    use elyra_core::datetime as dt;
    use sqlparser::ast::{DataType as D, ExactNumberInfo};
    if v.is_null() {
        return Ok(Value::Null);
    }
    // Precise DECIMAL / binary casts need the DataType directly.
    match ty {
        D::Decimal(info) | D::Numeric(info) | D::Dec(info) => {
            let scale = match info {
                ExactNumberInfo::PrecisionAndScale(_, s) => *s as u8,
                _ => 0,
            };
            return Ok(match &v {
                Value::Decimal(u, s) => {
                    let rescaled = if *s <= scale {
                        u * 10i128.pow((scale - *s) as u32)
                    } else {
                        u / 10i128.pow((*s - scale) as u32)
                    };
                    Value::Decimal(rescaled, scale)
                }
                Value::Int(i) => Value::Decimal(*i as i128 * 10i128.pow(scale as u32), scale),
                other => match other
                    .to_wire_string()
                    .and_then(|s| elyra_core::value::parse_decimal(&s, scale))
                {
                    Some((u, s)) => Value::Decimal(u, s),
                    None => Value::Null,
                },
            });
        }
        D::Binary(_) | D::Varbinary(_) | D::Blob(_) | D::Bytea => {
            return Ok(match v {
                Value::Bytes(b) => Value::Bytes(b),
                other => Value::Bytes(other.to_wire_string().unwrap_or_default().into_bytes()),
            });
        }
        _ => {}
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
    } else if tn.contains("UNSIGNED") {
        // MySQL: CAST(x AS UNSIGNED) wraps into 64-bit unsigned (e.g. -1 ->
        // 18446744073709551615); floats round; non-numeric text -> 0.
        match &v {
            Value::UInt(u) => Value::UInt(*u),
            Value::Int(i) => Value::UInt(*i as u64),
            _ => match v.as_f64() {
                Some(x) => Value::UInt(x.round() as i64 as u64),
                None => Value::UInt(str_to_int(&v) as u64),
            },
        }
    } else if tn.contains("INT") || tn.contains("SIGNED") {
        // MySQL: casting a float to an integer rounds (half away from zero), not
        // truncates; a non-numeric string casts to its leading integer prefix
        // (or 0), not NULL.
        match &v {
            Value::Int(i) => Value::Int(*i),
            Value::UInt(u) => Value::Int(*u as i64),
            _ => match v.as_f64() {
                Some(x) => Value::Int(x.round() as i64),
                None => Value::Int(str_to_int(&v)),
            },
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

/// Convert a value to microseconds-since-epoch (dates midnight-aligned).
fn to_micros(v: &Value) -> Option<i64> {
    match v {
        Value::Date(d) => Some(*d as i64 * 86_400_000_000),
        Value::DateTime(m) => Some(*m),
        Value::Time(t) => Some(*t),
        Value::Null => None,
        _ => v.to_wire_string().and_then(|s| {
            elyra_core::datetime::parse_datetime(&s)
                .or_else(|| elyra_core::datetime::parse_date(&s).map(|d| d as i64 * 86_400_000_000))
        }),
    }
}

fn parts(v: &Value) -> Option<(i64, u32, u32, u32, u32, u32)> {
    let m = to_micros(v)?;
    let days = m.div_euclid(86_400_000_000);
    let secs = m.rem_euclid(86_400_000_000) / 1_000_000;
    let (y, mo, d) = elyra_core::datetime::civil_from_days(days);
    Some((
        y,
        mo,
        d,
        (secs / 3600) as u32,
        ((secs % 3600) / 60) as u32,
        (secs % 60) as u32,
    ))
}

fn date_part(v: &Value, unit: &str) -> Value {
    let (y, mo, d, h, mi, s) = match parts(v) {
        Some(p) => p,
        None => return Value::Null,
    };
    let days = match to_micros(v) {
        Some(m) => m.div_euclid(86_400_000_000),
        None => return Value::Null,
    };
    Value::Int(match unit.to_ascii_lowercase().as_str() {
        "year" => y,
        "month" => mo as i64,
        "day" | "dayofmonth" => d as i64,
        "hour" => h as i64,
        "minute" => mi as i64,
        "second" => s as i64,
        "quarter" => (mo as i64 - 1) / 3 + 1,
        "dayofweek" | "dow" => (days.rem_euclid(7) + 4) % 7 + 1,
        "weekday" => (days.rem_euclid(7) + 3) % 7,
        "dayofyear" | "doy" => days - elyra_core::datetime::days_from_civil(y, 1, 1) + 1,
        _ => return Value::Null,
    })
}

/// Parse an `[H]H:MM:SS` time string to seconds (for TIME_TO_SEC).
fn parse_hms(s: &str) -> Option<i64> {
    let neg = s.starts_with('-');
    let mut it = s.trim_start_matches('-').split(':');
    let h: i64 = it.next()?.trim().parse().ok()?;
    let m: i64 = it.next()?.trim().parse().ok()?;
    let sec: i64 = it.next().unwrap_or("0").trim().parse().ok()?;
    let total = h * 3600 + m * 60 + sec;
    Some(if neg { -total } else { total })
}

/// MySQL-compatible SOUNDEX code (letter + up to three digits).
fn soundex(input: &str) -> String {
    let code = |c: char| match c.to_ascii_uppercase() {
        'B' | 'F' | 'P' | 'V' => b'1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => b'2',
        'D' | 'T' => b'3',
        'L' => b'4',
        'M' | 'N' => b'5',
        'R' => b'6',
        _ => 0,
    };
    let letters: Vec<char> = input.chars().filter(|c| c.is_ascii_alphabetic()).collect();
    if letters.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    out.push(letters[0].to_ascii_uppercase());
    let mut last = code(letters[0]);
    for &c in &letters[1..] {
        let d = code(c);
        if d != 0 && d != last {
            out.push(d as char);
        }
        // H and W are transparent (don't reset the previous code); others do.
        if !matches!(c.to_ascii_uppercase(), 'H' | 'W') {
            last = d;
        }
    }
    // MySQL pads to at least 4 characters.
    while out.len() < 4 {
        out.push('0');
    }
    out
}

/// Lowercase hex encoding of a digest (for MD5/SHA* SQL functions).
fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// MySQL `FORMAT(n, d)`: fixed `d` decimals with thousands separators.
fn format_number(n: f64, d: usize) -> String {
    let neg = n < 0.0;
    // MySQL rounds half away from zero (Rust's default is half-to-even).
    let factor = 10f64.powi(d as i32);
    let rounded = (n.abs() * factor + 0.5).floor() / factor;
    let s = format!("{rounded:.d$}");
    let (int_part, frac) = match s.split_once('.') {
        Some((i, f)) => (i.to_string(), Some(f.to_string())),
        None => (s, None),
    };
    let len = int_part.chars().count();
    let mut grouped = String::new();
    for (i, ch) in int_part.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    let mut out = String::new();
    if neg {
        out.push('-');
    }
    out.push_str(&grouped);
    if let Some(f) = frac {
        out.push('.');
        out.push_str(&f);
    }
    out
}

fn days_in_month(y: i64, m: u32) -> u32 {
    let first = elyra_core::datetime::days_from_civil(y, m, 1);
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    (elyra_core::datetime::days_from_civil(ny, nm, 1) - first) as u32
}

fn add_months(micros: i64, months: i64) -> i64 {
    let day_micros = micros.rem_euclid(86_400_000_000);
    let days = micros.div_euclid(86_400_000_000);
    let (y, mo, d) = elyra_core::datetime::civil_from_days(days);
    let total = y * 12 + (mo as i64 - 1) + months;
    let ny = total.div_euclid(12);
    let nm = (total.rem_euclid(12) + 1) as u32;
    let nd = d.min(days_in_month(ny, nm));
    elyra_core::datetime::days_from_civil(ny, nm, nd) * 86_400_000_000 + day_micros
}

fn unit_name(e: &Expr) -> String {
    match e {
        Expr::Identifier(id) => id.value.clone(),
        Expr::Interval(iv) => iv
            .leading_field
            .as_ref()
            .map(|u| u.to_string())
            .unwrap_or_default(),
        other => other.to_string(),
    }
    .to_ascii_uppercase()
}

fn comps(m: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = m.div_euclid(86_400_000_000);
    let secs = m.rem_euclid(86_400_000_000) / 1_000_000;
    let (y, mo, d) = elyra_core::datetime::civil_from_days(days);
    (
        y,
        mo,
        d,
        (secs / 3600) as u32,
        ((secs % 3600) / 60) as u32,
        (secs % 60) as u32,
    )
}

/// Whole months between two instants (MySQL TIMESTAMPDIFF semantics).
fn months_between(ma: i64, mb: i64) -> i64 {
    let (ya, moa, da, ha, mia, sa) = comps(ma);
    let (yb, mob, db, hb, mib, sb) = comps(mb);
    let mut months = (yb - ya) * 12 + (mob as i64 - moa as i64);
    let a_tod = (da, ha, mia, sa);
    let b_tod = (db, hb, mib, sb);
    if months > 0 && b_tod < a_tod {
        months -= 1;
    } else if months < 0 && b_tod > a_tod {
        months += 1;
    }
    months
}

fn timestampdiff(unit: &str, a: &Value, b: &Value) -> Value {
    let (ma, mb) = match (to_micros(a), to_micros(b)) {
        (Some(x), Some(y)) => (x, y),
        _ => return Value::Null,
    };
    let diff = mb - ma;
    Value::Int(match unit {
        "MICROSECOND" => diff,
        "SECOND" => diff / 1_000_000,
        "MINUTE" => diff / 60_000_000,
        "HOUR" => diff / 3_600_000_000,
        "DAY" => diff / 86_400_000_000,
        "WEEK" => diff / (7 * 86_400_000_000),
        "MONTH" => months_between(ma, mb),
        "QUARTER" => months_between(ma, mb) / 3,
        "YEAR" => months_between(ma, mb) / 12,
        _ => return Value::Null,
    })
}

fn days_in_year(y: i64) -> i64 {
    if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
        366
    } else {
        365
    }
}

/// Weekday of `days` (since epoch): 0-based, Monday-first unless `sunday_first`.
fn calc_weekday(days: i64, sunday_first: bool) -> i64 {
    (days + 3 + if sunday_first { 1 } else { 0 }).rem_euclid(7)
}

/// MySQL `calc_week`: returns `(year, week)` for the given mode bits.
fn calc_week(days: i64, mode: i64) -> (i64, i64) {
    let (y, m, d) = elyra_core::datetime::civil_from_days(days);
    let monday_first = (mode & 1) != 0;
    let mut week_year = (mode & 2) != 0;
    let first_weekday = (mode & 4) != 0;
    let mut year = y;
    let mut first_daynr = elyra_core::datetime::days_from_civil(y, 1, 1);
    let mut weekday = calc_weekday(first_daynr, !monday_first);

    if m == 1 && (d as i64) <= 7 - weekday {
        if !week_year && ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4)) {
            return (year, 0);
        }
        week_year = true;
        year -= 1;
        let diy = days_in_year(year);
        first_daynr -= diy;
        weekday = (weekday + 53 * 7 - diy).rem_euclid(7);
    }

    let mut wdays = if (first_weekday && weekday != 0) || (!first_weekday && weekday >= 4) {
        days - (first_daynr + (7 - weekday))
    } else {
        days - (first_daynr - weekday)
    };

    if week_year && wdays >= 52 * 7 {
        weekday = (weekday + days_in_year(year)).rem_euclid(7);
        if (!first_weekday && weekday < 4) || (first_weekday && weekday == 0) {
            return (year + 1, 1);
        }
    }
    if wdays < 0 {
        wdays = 0;
    }
    (year, wdays / 7 + 1)
}

/// Parse a string per a MySQL `DATE_FORMAT`-style pattern (STR_TO_DATE).
fn str_to_date(s: &str, fmt: &str) -> Value {
    match parse_with_format(s, fmt) {
        Some((micros, has_time)) => {
            if has_time {
                Value::DateTime(micros)
            } else {
                Value::Date(micros.div_euclid(86_400_000_000) as i32)
            }
        }
        None => Value::Null,
    }
}

fn parse_with_format(s: &str, fmt: &str) -> Option<(i64, bool)> {
    const MON: [&str; 12] = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ];
    let sb: Vec<char> = s.chars().collect();
    let mut si = 0usize;
    let (mut y, mut mo, mut d) = (1970i64, 1u32, 1u32);
    let (mut h, mut mi, mut se) = (0u32, 0u32, 0u32);
    let mut has_time = false;
    let mut pm: Option<bool> = None;
    let read_num = |sb: &[char], si: &mut usize, maxlen: usize| -> Option<i64> {
        while *si < sb.len() && sb[*si].is_whitespace() {
            *si += 1;
        }
        let start = *si;
        let mut n = 0i64;
        let mut cnt = 0;
        while *si < sb.len() && sb[*si].is_ascii_digit() && cnt < maxlen {
            n = n * 10 + (sb[*si] as i64 - '0' as i64);
            *si += 1;
            cnt += 1;
        }
        if *si == start {
            None
        } else {
            Some(n)
        }
    };
    let mut fi = fmt.chars();
    while let Some(c) = fi.next() {
        if c != '%' {
            if c.is_whitespace() {
                while si < sb.len() && sb[si].is_whitespace() {
                    si += 1;
                }
            } else if si < sb.len() && sb[si] == c {
                si += 1;
            }
            continue;
        }
        match fi.next() {
            Some('Y') => y = read_num(&sb, &mut si, 4)?,
            Some('y') => {
                let v = read_num(&sb, &mut si, 2)?;
                y = if v < 70 { 2000 + v } else { 1900 + v };
            }
            Some('m') | Some('c') => mo = read_num(&sb, &mut si, 2)? as u32,
            Some('d') | Some('e') => d = read_num(&sb, &mut si, 2)? as u32,
            Some('H') | Some('k') => {
                h = read_num(&sb, &mut si, 2)? as u32;
                has_time = true;
            }
            Some('h') | Some('I') | Some('l') => {
                h = read_num(&sb, &mut si, 2)? as u32;
                has_time = true;
            }
            Some('i') => {
                mi = read_num(&sb, &mut si, 2)? as u32;
                has_time = true;
            }
            Some('s') | Some('S') => {
                se = read_num(&sb, &mut si, 2)? as u32;
                has_time = true;
            }
            Some('p') => {
                while si < sb.len() && sb[si].is_whitespace() {
                    si += 1;
                }
                let a: String = sb.iter().skip(si).take(2).collect();
                pm = match a.to_ascii_uppercase().as_str() {
                    "PM" => Some(true),
                    "AM" => Some(false),
                    _ => return None,
                };
                si += 2;
            }
            Some('M') | Some('b') => {
                while si < sb.len() && sb[si].is_whitespace() {
                    si += 1;
                }
                let start = si;
                while si < sb.len() && sb[si].is_alphabetic() {
                    si += 1;
                }
                let name: String = sb[start..si]
                    .iter()
                    .collect::<String>()
                    .to_ascii_lowercase();
                mo = (MON
                    .iter()
                    .position(|m| m.starts_with(&name) || name.starts_with(*m))?
                    + 1) as u32;
            }
            Some('%') if si < sb.len() && sb[si] == '%' => si += 1,
            _ => {}
        }
    }
    if let Some(is_pm) = pm {
        h %= 12;
        if is_pm {
            h += 12;
        }
    }
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let days = elyra_core::datetime::days_from_civil(y, mo, d);
    let micros = days * 86_400_000_000 + (h as i64 * 3600 + mi as i64 * 60 + se as i64) * 1_000_000;
    Some((micros, has_time))
}

/// Add/subtract an interval to a date/datetime value.
fn apply_interval(base: Value, n: i64, unit: &str) -> Value {
    let (mut micros, was_date) = match &base {
        Value::Date(d) => (*d as i64 * 86_400_000_000, true),
        Value::DateTime(m) => (*m, false),
        Value::Null => return Value::Null,
        // A string operand behaves as a DATE when it has no time part and as a
        // DATETIME when it does (MySQL infers the result type this way, so
        // `DATE_ADD('2024-01-31', INTERVAL 1 MONTH)` returns a DATE).
        _ => {
            let Some(s) = base.to_wire_string() else {
                return Value::Null;
            };
            if let Some(d) = elyra_core::datetime::parse_date(&s) {
                (d as i64 * 86_400_000_000, true)
            } else if let Some(m) = elyra_core::datetime::parse_datetime(&s) {
                (m, false)
            } else {
                return Value::Null;
            }
        }
    };
    let time_unit = matches!(unit, "HOUR" | "MINUTE" | "SECOND" | "MICROSECOND");
    match unit {
        "MICROSECOND" => micros += n,
        "SECOND" => micros += n * 1_000_000,
        "MINUTE" => micros += n * 60_000_000,
        "HOUR" => micros += n * 3_600_000_000,
        "DAY" => micros += n * 86_400_000_000,
        "WEEK" => micros += n * 7 * 86_400_000_000,
        "MONTH" => micros = add_months(micros, n),
        "QUARTER" => micros = add_months(micros, n * 3),
        "YEAR" => micros = add_months(micros, n * 12),
        _ => return Value::Null,
    }
    if was_date && !time_unit {
        Value::Date(micros.div_euclid(86_400_000_000) as i32)
    } else {
        Value::DateTime(micros)
    }
}

/// MySQL `DATE_FORMAT` for the common specifiers.
fn format_dt(m: i64, fmt: &str) -> String {
    let days = m.div_euclid(86_400_000_000);
    let (y, mo, d) = elyra_core::datetime::civil_from_days(days);
    let secs = m.rem_euclid(86_400_000_000) / 1_000_000;
    let (h, mi, s) = (
        (secs / 3600) as u32,
        ((secs % 3600) / 60) as u32,
        (secs % 60) as u32,
    );
    let dow = ((days.rem_euclid(7) + 4) % 7) as usize;
    let doy = days - elyra_core::datetime::days_from_civil(y, 1, 1) + 1;
    const MON: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    const DAYN: [&str; 7] = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];
    let h12 = {
        let x = h % 12;
        if x == 0 {
            12
        } else {
            x
        }
    };
    let mut out = String::new();
    let mut it = fmt.chars();
    while let Some(c) = it.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('Y') => out.push_str(&format!("{y:04}")),
            Some('y') => out.push_str(&format!("{:02}", y.rem_euclid(100))),
            Some('m') => out.push_str(&format!("{mo:02}")),
            Some('c') => out.push_str(&mo.to_string()),
            Some('d') => out.push_str(&format!("{d:02}")),
            Some('e') => out.push_str(&d.to_string()),
            Some('H') => out.push_str(&format!("{h:02}")),
            Some('k') => out.push_str(&h.to_string()),
            Some('h') | Some('I') => out.push_str(&format!("{h12:02}")),
            Some('l') => out.push_str(&h12.to_string()),
            Some('i') => out.push_str(&format!("{mi:02}")),
            Some('s') | Some('S') => out.push_str(&format!("{s:02}")),
            Some('p') => out.push_str(if h < 12 { "AM" } else { "PM" }),
            Some('M') => out.push_str(MON[(mo - 1) as usize]),
            Some('b') => out.push_str(&MON[(mo - 1) as usize][..3]),
            Some('W') => out.push_str(DAYN[dow]),
            Some('a') => out.push_str(&DAYN[dow][..3]),
            Some('j') => out.push_str(&format!("{doy:03}")),
            Some('%') => out.push('%'),
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
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
            .or_else(|_| n.parse::<u64>().map(Value::UInt))
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
    lexpr: &Expr,
    rexpr: &Expr,
    schema: &Schema,
    _row: &[Value],
) -> Result<Value> {
    use BinaryOperator::*;
    // Logical operators with MySQL three-valued logic: for AND, FALSE dominates
    // and the result is NULL only when no operand is FALSE and at least one is
    // NULL; symmetrically for OR with TRUE. (`truthy` alone would fold NULL to
    // FALSE, making `NULL AND 1` wrongly 0 instead of NULL.)
    match op {
        And => {
            if matches!(bool3(&l), Some(false)) {
                return Ok(Value::Bool(false)); // short-circuit
            }
            let r = eval_right()?;
            return Ok(match (bool3(&l), bool3(&r)) {
                (_, Some(false)) => Value::Bool(false),
                (Some(true), Some(true)) => Value::Bool(true),
                _ => Value::Null,
            });
        }
        Or => {
            if matches!(bool3(&l), Some(true)) {
                return Ok(Value::Bool(true)); // short-circuit
            }
            let r = eval_right()?;
            return Ok(match (bool3(&l), bool3(&r)) {
                (_, Some(true)) => Value::Bool(true),
                (Some(false), Some(false)) => Value::Bool(false),
                _ => Value::Null,
            });
        }
        // `<=>` null-safe equality: NULL<=>NULL is true, NULL<=>x is false.
        Spaceship => {
            let r = eval_right()?;
            return Ok(Value::Bool(match (l.is_null(), r.is_null()) {
                (true, true) => true,
                (true, false) | (false, true) => false,
                (false, false) => cmp(&l, &r, cmp_collation(lexpr, rexpr, schema))?
                    .map(|o| o.is_eq())
                    .unwrap_or(false),
            }));
        }
        _ => {}
    }
    let r = eval_right()?;
    // Three-valued logic: a comparison with a NULL operand is NULL (UNKNOWN).
    // In WHERE this is falsy (row excluded); in CHECK it passes; in SELECT it
    // shows NULL — all matching SQL semantics.
    if matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq) && (l.is_null() || r.is_null()) {
        return Ok(Value::Null);
    }
    // A binary-collation column operand makes the text comparison case-sensitive.
    let coll = cmp_collation(lexpr, rexpr, schema);
    match op {
        Eq => Ok(Value::Bool(
            cmp(&l, &r, coll)?.map(|o| o.is_eq()).unwrap_or(false),
        )),
        NotEq => Ok(Value::Bool(
            cmp(&l, &r, coll)?.map(|o| o.is_ne()).unwrap_or(true),
        )),
        Lt => Ok(Value::Bool(
            cmp(&l, &r, coll)?.map(|o| o.is_lt()).unwrap_or(false),
        )),
        LtEq => Ok(Value::Bool(
            cmp(&l, &r, coll)?.map(|o| o.is_le()).unwrap_or(false),
        )),
        Gt => Ok(Value::Bool(
            cmp(&l, &r, coll)?.map(|o| o.is_gt()).unwrap_or(false),
        )),
        GtEq => Ok(Value::Bool(
            cmp(&l, &r, coll)?.map(|o| o.is_ge()).unwrap_or(false),
        )),
        Plus | Minus | Multiply | Divide | Modulo | MyIntegerDivide => arith(l, op, r),
        BitwiseAnd | BitwiseOr | BitwiseXor | PGBitwiseShiftLeft | PGBitwiseShiftRight => {
            bitwise(l, op, r)
        }
        _ => Err(Error::Unsupported(format!("operator not supported: {op}"))),
    }
}

fn bitwise(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    // MySQL bitwise operators work on 64-bit **unsigned** integers and return
    // BIGINT UNSIGNED. Coerce each operand to u64 (preserving the bit pattern of
    // a signed value) and return a UInt so large results display correctly.
    let to_u64 = |v: &Value| -> Option<u64> {
        match v {
            Value::UInt(u) => Some(*u),
            Value::Int(i) => Some(*i as u64),
            Value::Bool(b) => Some(*b as u64),
            _ => v.as_f64().map(|f| f as i64 as u64),
        }
    };
    let (a, b) = match (to_u64(&l), to_u64(&r)) {
        (Some(a), Some(b)) => (a, b),
        _ => return Ok(Value::Null),
    };
    Ok(Value::UInt(match op {
        BitwiseAnd => a & b,
        BitwiseOr => a | b,
        BitwiseXor => a ^ b,
        PGBitwiseShiftLeft => a.wrapping_shl((b & 63) as u32),
        PGBitwiseShiftRight => a.wrapping_shr((b & 63) as u32),
        _ => return Err(Error::Unsupported(format!("operator not supported: {op}"))),
    }))
}

fn arith(l: Value, op: &BinaryOperator, r: Value) -> Result<Value> {
    use BinaryOperator::*;
    // Arithmetic with a NULL operand is NULL (MySQL), never an error.
    if l.is_null() || r.is_null() {
        return Ok(Value::Null);
    }
    // `a DIV b`: integer division truncating toward zero; `/0` is NULL; overflow
    // (i64::MIN DIV -1) is out of range like MySQL.
    if matches!(op, MyIntegerDivide) {
        if let (Value::Int(a), Value::Int(b)) = (&l, &r) {
            if *b == 0 {
                return Ok(Value::Null);
            }
            return match a.checked_div(*b) {
                Some(v) => Ok(Value::Int(v)),
                None => Err(Error::OutOfRange(format!(
                    "BIGINT value is out of range in '({a} DIV {b})'"
                ))),
            };
        }
        let (Some(a), Some(b)) = (num(&l), num(&r)) else {
            return Err(Error::Type("arithmetic on non-numeric value".into()));
        };
        if b == 0.0 {
            return Ok(Value::Null);
        }
        return Ok(Value::Int((a / b).trunc() as i64));
    }
    // Exact DECIMAL arithmetic for +, -, * (division/modulo fall back to float).
    if matches!(l, Value::Decimal(..)) || matches!(r, Value::Decimal(..)) {
        if let Some(v) = decimal_arith(&l, op, &r) {
            return Ok(v);
        }
    }
    // Exact unsigned 64-bit arithmetic when either operand is UInt and both are
    // integer-typed (MySQL: any unsigned operand makes the operation unsigned).
    // Division stays float (like signed integer `/`), which MySQL renders as a
    // decimal.
    if matches!(l, Value::UInt(_)) || matches!(r, Value::UInt(_)) {
        let as_u64 = |v: &Value| -> Option<u64> {
            match v {
                Value::UInt(u) => Some(*u),
                Value::Int(i) => Some(*i as u64),
                Value::Bool(b) => Some(*b as u64),
                _ => None,
            }
        };
        if let (Some(a), Some(b)) = (as_u64(&l), as_u64(&r)) {
            let v = match op {
                Plus => Some(a.wrapping_add(b)),
                Minus => Some(a.wrapping_sub(b)),
                Multiply => Some(a.wrapping_mul(b)),
                Modulo => {
                    if b == 0 {
                        return Ok(Value::Null);
                    }
                    Some(a % b)
                }
                _ => None, // Divide -> fall through to float
            };
            if let Some(v) = v {
                return Ok(Value::UInt(v));
            }
        }
    }
    // Exact signed 64-bit integer arithmetic. MySQL raises an out-of-range error
    // (1690) on overflow rather than silently saturating or wrapping, and `x % 0`
    // is NULL. Division stays on the float path below (MySQL renders integer
    // division as a decimal, e.g. `5/2` -> 2.5000).
    if let (Value::Int(ai), Value::Int(bi)) = (&l, &r) {
        let (ai, bi) = (*ai, *bi);
        if !matches!(op, Divide) {
            let res = match op {
                Plus => ai.checked_add(bi),
                Minus => ai.checked_sub(bi),
                Multiply => ai.checked_mul(bi),
                Modulo => {
                    if bi == 0 {
                        return Ok(Value::Null);
                    }
                    // `i64::MIN % -1` overflows `checked_rem` but the true
                    // remainder is 0.
                    Some(ai.checked_rem(bi).unwrap_or(0))
                }
                _ => unreachable!(),
            };
            return match res {
                Some(v) => Ok(Value::Int(v)),
                None => Err(Error::OutOfRange(format!(
                    "BIGINT value is out of range in '({ai} {} {bi})'",
                    arith_symbol(op)
                ))),
            };
        }
    }

    let (Some(a), Some(b)) = (num(&l), num(&r)) else {
        return Err(Error::Type("arithmetic on non-numeric value".into()));
    };
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
        Modulo => {
            if b == 0.0 {
                return Ok(Value::Null);
            }
            a % b
        }
        _ => unreachable!(),
    };
    // A non-finite result (DOUBLE overflow to +/-inf, or e.g. inf-inf -> NaN) is
    // out of range; MySQL yields NULL rather than inf/NaN.
    if !res.is_finite() {
        return Ok(Value::Null);
    }
    Ok(Value::Float(res))
}

/// The operator symbol for an arithmetic op, for error messages.
fn arith_symbol(op: &BinaryOperator) -> &'static str {
    match op {
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Multiply => "*",
        BinaryOperator::Divide => "/",
        BinaryOperator::Modulo => "%",
        _ => "?",
    }
}

/// Three-way compare with SQL cross-type coercion; `None` when either side is
/// NULL. Delegates to the shared [`Value::compare_coll`].
fn cmp(l: &Value, r: &Value, coll: Collation) -> Result<Option<std::cmp::Ordering>> {
    Ok(l.compare_coll(r, coll))
}

/// Collation of a bare/qualified column reference, if it is one.
fn expr_collation(e: &Expr, schema: &Schema) -> Option<Collation> {
    let name = match e {
        Expr::Identifier(id) => id.value.as_str(),
        Expr::CompoundIdentifier(parts) => parts.last()?.value.as_str(),
        _ => return None,
    };
    schema.column(name).map(|c| c.collation)
}

/// The comparison collation for two operands: case-sensitive if either is a
/// binary-collation column, else the default case-insensitive collation.
fn cmp_collation(l: &Expr, r: &Expr, schema: &Schema) -> Collation {
    if matches!(expr_collation(l, schema), Some(Collation::Bin))
        || matches!(expr_collation(r, schema), Some(Collation::Bin))
    {
        Collation::Bin
    } else {
        Collation::Ci
    }
}

fn num(v: &Value) -> Option<f64> {
    v.as_f64()
}

/// Exact decimal `+`/`-`/`*`. Returns `None` (fall back to float) for division,
/// modulo, or when a non-decimal/non-integer operand is involved.
fn decimal_arith(l: &Value, op: &BinaryOperator, r: &Value) -> Option<Value> {
    use BinaryOperator::*;
    let to_dec = |v: &Value| -> Option<(i128, u8)> {
        match v {
            Value::Decimal(u, s) => Some((*u, *s)),
            Value::Int(i) => Some((*i as i128, 0)),
            Value::Bool(b) => Some((*b as i128, 0)),
            _ => None,
        }
    };
    let (a, asc) = to_dec(l)?;
    let (b, bsc) = to_dec(r)?;
    Some(match op {
        Plus | Minus => {
            let sc = asc.max(bsc);
            let aa = a.checked_mul(10i128.pow((sc - asc) as u32))?;
            let bb = b.checked_mul(10i128.pow((sc - bsc) as u32))?;
            Value::Decimal(if matches!(op, Plus) { aa + bb } else { aa - bb }, sc)
        }
        Multiply => Value::Decimal(a.checked_mul(b)?, asc.saturating_add(bsc)),
        _ => return None,
    })
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Int(i) => *i != 0,
        Value::UInt(u) => *u != 0,
        Value::Float(f) => *f != 0.0,
        Value::Decimal(u, _) => *u != 0,
        Value::Null => false,
        other => other.as_f64().map(|f| f != 0.0).unwrap_or(false),
    }
}

/// Three-valued truthiness: `None` for NULL (UNKNOWN), else `Some(bool)`.
fn bool3(v: &Value) -> Option<bool> {
    if v.is_null() {
        None
    } else {
        Some(truthy(v))
    }
}
