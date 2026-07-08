//! Aggregation, grouping and ordering.
//!
//! `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, `GROUP BY`, and `ORDER BY`. These paths
//! materialise their working set (grouping and sorting inherently need it);
//! a future columnar OLAP backend takes over for very large aggregations.

use std::cmp::Ordering;
use std::collections::HashMap;

use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem,
};

#[derive(Clone, Copy, PartialEq)]
enum AggFunc {
    CountStar,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// One output column of an aggregating SELECT.
enum OutCol {
    /// A grouped-by (or representative) column value, by schema index.
    Column(usize),
    /// An aggregate over a column (or `*`).
    Agg { func: AggFunc, arg: Option<usize>, distinct: bool },
}

/// Recognise an aggregate function name, if any.
fn agg_of(expr: &Expr) -> Option<(AggFunc, &sqlparser::ast::Function)> {
    let Expr::Function(f) = expr else { return None };
    let name = f.name.0.last()?.value.to_ascii_lowercase();
    let func = match name.as_str() {
        "count" => {
            // COUNT(*) vs COUNT(expr) is resolved later from the args.
            AggFunc::Count
        }
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        _ => return None,
    };
    Some((func, f))
}

/// Does this projection contain any aggregate function?
pub fn projection_has_aggregate(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            agg_of(e).is_some()
        }
        _ => false,
    })
}

fn item_expr_and_alias(item: &SelectItem) -> Result<(&Expr, Option<String>)> {
    match item {
        SelectItem::UnnamedExpr(e) => Ok((e, None)),
        SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias.value.clone()))),
        other => Err(Error::Unsupported(format!(
            "projection item not supported with aggregation: {other}"
        ))),
    }
}

fn col_index(schema: &Schema, name: &str) -> Result<usize> {
    schema
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| Error::Catalog(format!("unknown column: {name}")))
}

fn ident_of(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(id) => Some(&id.value),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.as_str()),
        _ => None,
    }
}

/// Resolve a single aggregate's argument column and DISTINCT flag.
fn agg_arg(schema: &Schema, f: &sqlparser::ast::Function) -> Result<(Option<usize>, bool)> {
    let FunctionArguments::List(list) = &f.args else {
        return Ok((None, false));
    };
    let distinct = matches!(
        list.duplicate_treatment,
        Some(sqlparser::ast::DuplicateTreatment::Distinct)
    );
    let arg = match list.args.first() {
        None => None,
        Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)) => None,
        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => {
            let name = ident_of(e)
                .ok_or_else(|| Error::Unsupported("aggregate arg must be a column".into()))?;
            Some(col_index(schema, name)?)
        }
        _ => return Err(Error::Unsupported("unsupported aggregate argument".into())),
    };
    Ok((arg, distinct))
}

struct Acc {
    count: i64,
    sum: f64,
    sum_is_int: bool,
    extreme: Option<Value>,
    distinct: std::collections::HashSet<String>,
}

impl Acc {
    fn new() -> Self {
        Acc { count: 0, sum: 0.0, sum_is_int: true, extreme: None, distinct: Default::default() }
    }
}

/// Run aggregation/grouping over `rows`. Returns the output schema and rows.
pub fn run(
    schema: &Schema,
    projection: &[SelectItem],
    group_by: &[Expr],
    rows: Vec<Vec<Value>>,
) -> Result<(Schema, Vec<Vec<Value>>)> {
    // Resolve GROUP BY columns (identifiers only).
    let mut group_cols = Vec::new();
    for g in group_by {
        let name = ident_of(g)
            .ok_or_else(|| Error::Unsupported("GROUP BY must reference a column".into()))?;
        group_cols.push(col_index(schema, name)?);
    }

    // Build the output plan and schema.
    let mut plan = Vec::new();
    let mut out_cols = Vec::new();
    for item in projection {
        let (expr, alias) = item_expr_and_alias(item)?;
        if let Some((mut func, f)) = agg_of(expr) {
            let (arg, distinct) = agg_arg(schema, f)?;
            if func == AggFunc::Count && arg.is_none() {
                func = AggFunc::CountStar;
            }
            let ty = match func {
                AggFunc::CountStar | AggFunc::Count => ColumnType::Int,
                AggFunc::Avg => ColumnType::Float,
                AggFunc::Sum | AggFunc::Min | AggFunc::Max => arg
                    .map(|i| schema.columns[i].ty.clone())
                    .unwrap_or(ColumnType::Float),
            };
            out_cols.push(ColumnDef {
                name: alias.unwrap_or_else(|| expr.to_string()),
                ty,
                nullable: true,
            });
            plan.push(OutCol::Agg { func, arg, distinct });
        } else {
            let name = ident_of(expr)
                .ok_or_else(|| Error::Unsupported("non-aggregated column must be a plain column".into()))?;
            let idx = col_index(schema, name)?;
            out_cols.push(ColumnDef {
                name: alias.unwrap_or_else(|| schema.columns[idx].name.clone()),
                ty: schema.columns[idx].ty.clone(),
                nullable: schema.columns[idx].nullable,
            });
            plan.push(OutCol::Column(idx));
        }
    }

    // Group rows. No GROUP BY and aggregates present ⇒ one implicit group.
    let mut groups: HashMap<String, (Vec<Value>, Vec<Acc>)> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    let n_aggs = plan.iter().filter(|o| matches!(o, OutCol::Agg { .. })).count();

    for row in &rows {
        let key = group_key(&group_cols, row);
        let entry = groups.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            (row.clone(), (0..n_aggs).map(|_| Acc::new()).collect())
        });

        let mut ai = 0;
        for out in &plan {
            if let OutCol::Agg { func, arg, distinct } = out {
                let acc = &mut entry.1[ai];
                ai += 1;
                let val = arg.map(|i| row[i].clone());
                update_acc(acc, *func, val, *distinct);
            }
        }
    }

    // If there were no rows and no GROUP BY, aggregates still produce one row
    // (e.g. COUNT(*) → 0).
    if rows.is_empty() && group_by.is_empty() && n_aggs > 0 {
        let sample = vec![Value::Null; schema.columns.len()];
        groups.insert(String::new(), (sample, (0..n_aggs).map(|_| Acc::new()).collect()));
        order.push(String::new());
    }

    // Emit output rows.
    let mut out_rows = Vec::with_capacity(order.len());
    for key in &order {
        let (sample, accs) = &groups[key];
        let mut ai = 0;
        let mut out = Vec::with_capacity(plan.len());
        for o in &plan {
            match o {
                OutCol::Column(i) => out.push(sample[*i].clone()),
                OutCol::Agg { func, .. } => {
                    out.push(finish_acc(&accs[ai], *func));
                    ai += 1;
                }
            }
        }
        out_rows.push(out);
    }

    Ok((Schema::new(out_cols), out_rows))
}

fn group_key(cols: &[usize], row: &[Value]) -> String {
    if cols.is_empty() {
        return String::new();
    }
    cols.iter()
        .map(|&i| format!("{:?}", row[i]))
        .collect::<Vec<_>>()
        .join("\u{1}")
}

fn update_acc(acc: &mut Acc, func: AggFunc, val: Option<Value>, distinct: bool) {
    match func {
        AggFunc::CountStar => acc.count += 1,
        AggFunc::Count => {
            if let Some(v) = val {
                if !v.is_null() {
                    if distinct {
                        if acc.distinct.insert(format!("{v:?}")) {
                            acc.count += 1;
                        }
                    } else {
                        acc.count += 1;
                    }
                }
            }
        }
        AggFunc::Sum | AggFunc::Avg => {
            if let Some(v) = val {
                if let Some(n) = num(&v) {
                    if distinct && !acc.distinct.insert(format!("{v:?}")) {
                        return;
                    }
                    acc.sum += n;
                    acc.count += 1;
                    if !matches!(v, Value::Int(_)) {
                        acc.sum_is_int = false;
                    }
                }
            }
        }
        AggFunc::Min | AggFunc::Max => {
            if let Some(v) = val {
                if v.is_null() {
                    return;
                }
                let replace = match &acc.extreme {
                    None => true,
                    Some(cur) => {
                        let ord = value_cmp(&v, cur);
                        (func == AggFunc::Min && ord == Ordering::Less)
                            || (func == AggFunc::Max && ord == Ordering::Greater)
                    }
                };
                if replace {
                    acc.extreme = Some(v);
                }
            }
        }
    }
}

fn finish_acc(acc: &Acc, func: AggFunc) -> Value {
    match func {
        AggFunc::CountStar | AggFunc::Count => Value::Int(acc.count),
        AggFunc::Sum => {
            if acc.count == 0 {
                Value::Null
            } else if acc.sum_is_int && acc.sum.fract() == 0.0 {
                Value::Int(acc.sum as i64)
            } else {
                Value::Float(acc.sum)
            }
        }
        AggFunc::Avg => {
            if acc.count == 0 {
                Value::Null
            } else {
                Value::Float(acc.sum / acc.count as f64)
            }
        }
        AggFunc::Min | AggFunc::Max => acc.extreme.clone().unwrap_or(Value::Null),
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

/// Total order over values: NULL sorts first, then numbers, then text.
pub fn value_cmp(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        _ => {
            if let (Some(x), Some(y)) = (num(a), num(b)) {
                return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
            }
            match (a, b) {
                (Value::Text(x), Value::Text(y)) => x.cmp(y),
                _ => format!("{a:?}").cmp(&format!("{b:?}")),
            }
        }
    }
}
