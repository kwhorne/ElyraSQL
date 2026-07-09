//! SQL-level aggregation planning over the [`elyra_olap`] kernel.
//!
//! This module turns a projection + `GROUP BY` into an [`AggPlan`] that can
//! be executed either in memory (`run`) or by streaming batches into a
//! [`GroupAggregator`] (the OLAP path in `exec`). The mergeable aggregation
//! kernel itself lives in `elyra-olap`.

use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use elyra_olap::{AggFunc, AggSpec, GroupAggregator};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem};

pub use elyra_olap::value_cmp;

/// An output column: either a (grouped) source column or an aggregate result.
enum OutCol {
    Column(usize),
    Agg(usize),
}

/// A planned aggregation: group columns, aggregates, output layout and schema.
pub struct AggPlan {
    group_cols: Vec<usize>,
    aggs: Vec<AggSpec>,
    plan: Vec<OutCol>,
    out_schema: Schema,
}

impl AggPlan {
    /// A fresh, empty aggregator for this plan (used per parallel worker).
    pub fn new_aggregator(&self) -> GroupAggregator {
        GroupAggregator::new(self.group_cols.clone(), self.aggs.clone())
    }

    /// Finalise an aggregator into output rows in projection order.
    pub fn finalize(&self, agg: GroupAggregator) -> (Schema, Vec<Vec<Value>>) {
        let empty = agg.empty_result();
        let group_by_empty = self.group_cols.is_empty();
        let groups = agg.into_groups();

        let mut out = Vec::with_capacity(groups.len().max(1));
        if groups.is_empty() && group_by_empty && !self.aggs.is_empty() {
            // Bare aggregate over zero rows still yields one row (COUNT(*)->0).
            out.push(
                self.plan
                    .iter()
                    .map(|o| match o {
                        OutCol::Column(_) => Value::Null,
                        OutCol::Agg(j) => empty[*j].clone(),
                    })
                    .collect(),
            );
        } else {
            for (sample, results) in groups {
                out.push(
                    self.plan
                        .iter()
                        .map(|o| match o {
                            OutCol::Column(i) => sample[*i].clone(),
                            OutCol::Agg(j) => results[*j].clone(),
                        })
                        .collect(),
                );
            }
        }
        (self.out_schema.clone(), out)
    }
}

/// Recognise an aggregate function call.
fn agg_of(expr: &Expr) -> Option<(AggFunc, &sqlparser::ast::Function)> {
    let Expr::Function(f) = expr else { return None };
    let name = f.name.0.last()?.value.to_ascii_lowercase();
    let func = match name.as_str() {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        "group_concat" => AggFunc::GroupConcat,
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
    crate::predicate::resolve_index(name, schema)
}

fn ident_of(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(parts) => Some(
            parts
                .iter()
                .map(|i| i.value.as_str())
                .collect::<Vec<_>>()
                .join("."),
        ),
        _ => None,
    }
}

/// Extract a GROUP_CONCAT `SEPARATOR '...'` clause, if present.
fn agg_separator(f: &sqlparser::ast::Function) -> Option<String> {
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    for c in &list.clauses {
        if let sqlparser::ast::FunctionArgumentClause::Separator(v) = c {
            return match v {
                sqlparser::ast::Value::SingleQuotedString(s)
                | sqlparser::ast::Value::DoubleQuotedString(s) => Some(s.clone()),
                other => Some(other.to_string()),
            };
        }
    }
    None
}

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
            Some(col_index(schema, &name)?)
        }
        _ => return Err(Error::Unsupported("unsupported aggregate argument".into())),
    };
    Ok((arg, distinct))
}

/// Build an [`AggPlan`] from a projection and `GROUP BY` expressions.
pub fn build_plan(
    schema: &Schema,
    projection: &[SelectItem],
    group_by: &[Expr],
) -> Result<AggPlan> {
    let mut group_cols = Vec::new();
    for g in group_by {
        let name = ident_of(g)
            .ok_or_else(|| Error::Unsupported("GROUP BY must reference a column".into()))?;
        group_cols.push(col_index(schema, &name)?);
    }

    let mut aggs = Vec::new();
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
                AggFunc::GroupConcat => ColumnType::Text,
                AggFunc::Sum | AggFunc::Min | AggFunc::Max => arg
                    .map(|i| schema.columns[i].ty.clone())
                    .unwrap_or(ColumnType::Float),
            };
            out_cols.push(ColumnDef {
                name: alias.unwrap_or_else(|| expr.to_string()),
                ty,
                nullable: true,
            });
            let separator = agg_separator(f);
            let idx = aggs.len();
            aggs.push(AggSpec {
                func,
                arg_col: arg,
                distinct,
                separator,
            });
            plan.push(OutCol::Agg(idx));
        } else {
            let name = ident_of(expr).ok_or_else(|| {
                Error::Unsupported("non-aggregated column must be a plain column".into())
            })?;
            let idx = col_index(schema, &name)?;
            out_cols.push(ColumnDef {
                name: alias.unwrap_or_else(|| schema.columns[idx].name.clone()),
                ty: schema.columns[idx].ty.clone(),
                nullable: schema.columns[idx].nullable,
            });
            plan.push(OutCol::Column(idx));
        }
    }

    Ok(AggPlan {
        group_cols,
        aggs,
        plan,
        out_schema: Schema::new(out_cols),
    })
}

/// In-memory aggregation over a fully materialised row set.
pub fn run(
    schema: &Schema,
    projection: &[SelectItem],
    group_by: &[Expr],
    rows: Vec<Vec<Value>>,
) -> Result<(Schema, Vec<Vec<Value>>)> {
    let plan = build_plan(schema, projection, group_by)?;
    let mut agg = plan.new_aggregator();
    for row in &rows {
        agg.feed(row);
    }
    Ok(plan.finalize(agg))
}
