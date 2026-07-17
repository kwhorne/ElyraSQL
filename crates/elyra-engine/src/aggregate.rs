//! SQL-level aggregation planning over the [`elyra_olap`] kernel.
//!
//! This module turns a projection + `GROUP BY` into an [`AggPlan`] that can
//! be executed either in memory (`run`) or by streaming batches into a
//! [`GroupAggregator`] (the OLAP path in `exec`). The mergeable aggregation
//! kernel itself lives in `elyra-olap`.

use elyra_core::{ColumnDef, ColumnType, Error, Result, Schema, Value};
use elyra_olap::{AggFunc, AggSpec, GroupAggregator};
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem};

/// An output column: a (grouped) source column, an aggregate result, or a
/// scalar expression evaluated per group over the group columns + aggregate
/// results (e.g. `ROUND(SUM(x), 2)`, `SUM(a)/COUNT(*)`, `UPPER(status)`).
enum OutCol {
    Column(usize),
    Agg(usize),
    Computed(Box<Expr>),
}

/// A planned aggregation: group columns, aggregates, output layout and schema.
pub struct AggPlan {
    group_cols: Vec<usize>,
    /// Text collation per group column (parallel to `group_cols`), so GROUP BY on
    /// a `_bin` column distinguishes case.
    group_collations: Vec<elyra_core::Collation>,
    aggs: Vec<AggSpec>,
    plan: Vec<OutCol>,
    out_schema: Schema,
    /// Aggregate-argument expressions appended as virtual columns after the
    /// input schema (for conditional aggregates like `SUM(CASE ...)`).
    arg_exprs: Vec<Expr>,
    input_schema: Schema,
    /// Schema for evaluating `Computed` output columns: the input columns
    /// followed by one `__agg_i` column per aggregate result.
    eval_schema: Schema,
}

impl AggPlan {
    /// A fresh, empty aggregator for this plan (used per parallel worker).
    pub fn new_aggregator(&self) -> GroupAggregator {
        GroupAggregator::new(
            self.group_cols.clone(),
            self.aggs.clone(),
            self.group_collations.clone(),
        )
    }

    /// The aggregate-argument expressions to append as virtual columns.
    pub fn group_cols(&self) -> &[usize] {
        &self.group_cols
    }

    pub fn arg_exprs(&self) -> &[Expr] {
        &self.arg_exprs
    }

    /// True when this plan is exactly `COUNT(*)` with no GROUP BY and no other
    /// aggregates -- so its value is just the matching row count, which a
    /// covering index can supply without fetching any rows.
    pub fn is_count_star_only(&self) -> bool {
        self.group_cols.is_empty()
            && self.arg_exprs.is_empty()
            && self.aggs.len() == 1
            && matches!(self.aggs[0].func, elyra_olap::AggFunc::CountStar)
    }

    /// Eligibility for the vectorised (columnar) scalar-aggregate fast path:
    /// no GROUP BY, no argument expressions, no DISTINCT, and every aggregate
    /// is `COUNT(*)`/`COUNT`/`SUM`/`AVG`/`MIN`/`MAX` over a base `Int`/`Float`
    /// column. Returns `(func, arg column, is_integer_column)` per aggregate
    /// slot, or `None` to fall back.
    #[allow(clippy::type_complexity)]
    pub fn scalar_agg_plan(
        &self,
        schema: &Schema,
    ) -> Option<Vec<(elyra_olap::AggFunc, Option<usize>, bool)>> {
        use elyra_olap::AggFunc::*;
        if !self.group_cols.is_empty() || !self.arg_exprs.is_empty() {
            return None;
        }
        let base = self.input_schema.columns.len();
        let mut out = Vec::with_capacity(self.aggs.len());
        for spec in &self.aggs {
            if spec.distinct {
                return None;
            }
            match spec.func {
                CountStar => out.push((spec.func, None, true)),
                Count | Sum | Avg | Min | Max => {
                    let c = spec.arg_col?;
                    if c >= base {
                        return None;
                    }
                    let is_int = match schema.columns.get(c).map(|col| &col.ty) {
                        Some(ColumnType::Int) => true,
                        Some(ColumnType::Float) => false,
                        _ => return None,
                    };
                    out.push((spec.func, Some(c), is_int));
                }
                _ => return None,
            }
        }
        Some(out)
    }

    /// Eligibility for the vectorised (columnar) *grouped* aggregate fast path
    /// (OLAP phase 3): exactly one GROUP BY column, that column a base
    /// `Int`/`Float` (so its key can be kept exactly -- integer bits or
    /// canonical float bits), no argument expressions, and every aggregate a
    /// numeric `COUNT(*)`/`COUNT`/`SUM`/`AVG`/`MIN`/`MAX` (no DISTINCT). Returns
    /// the group column index and `(func, arg column, is_integer_column)` per
    /// aggregate slot, or `None` to fall back to the general grouping path.
    #[allow(clippy::type_complexity)]
    pub fn columnar_group_plan(
        &self,
        schema: &Schema,
    ) -> Option<(usize, Vec<(elyra_olap::AggFunc, Option<usize>, bool)>)> {
        use elyra_olap::AggFunc::*;
        if self.group_cols.len() != 1 || !self.arg_exprs.is_empty() {
            return None;
        }
        let gc = self.group_cols[0];
        match schema.columns.get(gc).map(|c| &c.ty) {
            Some(ColumnType::Int) | Some(ColumnType::Float) => {}
            _ => return None,
        }
        let base = self.input_schema.columns.len();
        let mut out = Vec::with_capacity(self.aggs.len());
        for spec in &self.aggs {
            if spec.distinct {
                return None;
            }
            match spec.func {
                CountStar => out.push((spec.func, None, true)),
                Count | Sum | Avg | Min | Max => {
                    let c = spec.arg_col?;
                    if c >= base {
                        return None;
                    }
                    let is_int = match schema.columns.get(c).map(|col| &col.ty) {
                        Some(ColumnType::Int) => true,
                        Some(ColumnType::Float) => false,
                        _ => return None,
                    };
                    out.push((spec.func, Some(c), is_int));
                }
                _ => return None,
            }
        }
        Some((gc, out))
    }

    /// Project pre-aggregated `(group sample row, aggregate results)` tuples
    /// (e.g. from the columnar grouped path) into output rows, reusing the
    /// normal per-group projection.
    pub fn project_grouped(
        &self,
        groups: Vec<(Vec<Value>, Vec<Value>)>,
    ) -> Result<(Schema, Vec<Vec<Value>>)> {
        let mut out = Vec::with_capacity(groups.len());
        for (sample, results) in groups {
            out.push(self.project_group(&sample, &results)?);
        }
        Ok((self.out_schema.clone(), out))
    }

    /// Project a single output row from pre-computed scalar aggregate results
    /// (one per aggregate slot), reusing the normal projection so
    /// `COUNT(*)+1`, `SUM(a)/COUNT(*)`, etc. still work.
    pub fn project_scalar(&self, results: Vec<Value>) -> Result<(Schema, Vec<Vec<Value>>)> {
        let sample = vec![Value::Null; self.input_schema.columns.len()];
        let row = self.project_group(&sample, &results)?;
        Ok((self.out_schema.clone(), vec![row]))
    }

    /// Base-table column indices that aggregators read *directly* (e.g. the
    /// `age` in `SUM(age)`), excluding appended virtual argument columns. Used
    /// to compute which columns a scan must decode.
    pub fn agg_input_cols(&self) -> Vec<usize> {
        let base = self.input_schema.columns.len();
        self.aggs
            .iter()
            .filter_map(|a| a.arg_col)
            .filter(|&c| c < base)
            .collect()
    }

    /// Append the evaluated argument expressions to a row (if any).
    pub fn extend_row(&self, row: &[Value]) -> Result<Vec<Value>> {
        let mut r = row.to_vec();
        for e in &self.arg_exprs {
            r.push(crate::predicate::eval_row(e, &self.input_schema, row)?);
        }
        Ok(r)
    }

    /// Finalise an aggregator into output rows in projection order.
    pub fn finalize(&self, agg: GroupAggregator) -> Result<(Schema, Vec<Vec<Value>>)> {
        let empty = agg.empty_result();
        let group_by_empty = self.group_cols.is_empty();
        let groups = agg.into_groups();
        let base_len = self.input_schema.columns.len();

        let mut out = Vec::with_capacity(groups.len().max(1));
        if groups.is_empty() && group_by_empty && !self.aggs.is_empty() {
            // Bare aggregate over zero rows still yields one row (COUNT(*)->0).
            let sample = vec![Value::Null; base_len];
            out.push(self.project_group(&sample, &empty)?);
        } else {
            for (sample, results) in groups {
                out.push(self.project_group(&sample, &results)?);
            }
        }
        Ok((self.out_schema.clone(), out))
    }

    /// Build one output row for a group from its sample row + aggregate results.
    fn project_group(&self, sample: &[Value], results: &[Value]) -> Result<Vec<Value>> {
        let base_len = self.input_schema.columns.len();
        self.plan
            .iter()
            .map(|o| match o {
                OutCol::Column(i) => Ok(sample.get(*i).cloned().unwrap_or(Value::Null)),
                OutCol::Agg(j) => Ok(results.get(*j).cloned().unwrap_or(Value::Null)),
                OutCol::Computed(expr) => {
                    // Row = base input columns of the sample, then the aggregate
                    // results (named `__agg_i` in `eval_schema`).
                    let mut row: Vec<Value> = if sample.len() >= base_len {
                        sample[..base_len].to_vec()
                    } else {
                        let mut r = sample.to_vec();
                        r.resize(base_len, Value::Null);
                        r
                    };
                    row.extend(results.iter().cloned());
                    crate::predicate::eval_row(expr, &self.eval_schema, &row)
                }
            })
            .collect()
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
        "stddev" | "std" | "stddev_pop" => AggFunc::StddevPop,
        "stddev_samp" => AggFunc::StddevSamp,
        "variance" | "var_pop" => AggFunc::VarPop,
        "var_samp" => AggFunc::VarSamp,
        "bit_or" => AggFunc::BitOr,
        "bit_and" => AggFunc::BitAnd,
        "bit_xor" => AggFunc::BitXor,
        "facet" => AggFunc::Facet,
        "percentile" | "quantile" | "median" => AggFunc::Percentile,
        _ => return None,
    };
    Some((func, f))
}

/// Does this projection contain any aggregate function (including nested in an
/// expression, e.g. `ROUND(SUM(x), 2)`)?
pub fn projection_has_aggregate(projection: &[SelectItem]) -> bool {
    projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
            contains_aggregate(e)
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
/// The optional top-N cap in `FACET(col, n)` (its second argument, a positive
/// integer literal). `None` = return every facet value.
fn facet_top_of(f: &sqlparser::ast::Function) -> Option<usize> {
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    match list.args.get(1) {
        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            sqlparser::ast::Value::Number(n, _),
        )))) => n.parse::<usize>().ok().filter(|&n| n > 0),
        _ => None,
    }
}

/// The fraction in `PERCENTILE(col, p)` / `QUANTILE(col, p)` (0..1), or 0.5 for
/// `MEDIAN(col)`. Defaults to 0.5 if the second argument is missing/unparseable.
fn percentile_of(f: &sqlparser::ast::Function) -> Option<f64> {
    let name = f
        .name
        .0
        .last()
        .map(|p| p.value.to_ascii_lowercase())
        .unwrap_or_default();
    if name == "median" {
        return Some(0.5);
    }
    if !(name == "percentile" || name == "quantile") {
        return None;
    }
    let FunctionArguments::List(list) = &f.args else {
        return Some(0.5);
    };
    match list.args.get(1) {
        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
            sqlparser::ast::Value::Number(n, _),
        )))) => Some(n.parse::<f64>().unwrap_or(0.5)),
        _ => Some(0.5),
    }
}

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

fn agg_arg(f: &sqlparser::ast::Function) -> (Option<&Expr>, bool) {
    let FunctionArguments::List(list) = &f.args else {
        return (None, false);
    };
    let distinct = matches!(
        list.duplicate_treatment,
        Some(sqlparser::ast::DuplicateTreatment::Distinct)
    );
    let arg = match list.args.first() {
        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) => Some(e),
        _ => None,
    };
    (arg, distinct)
}

/// Build an [`AggPlan`] from a projection and `GROUP BY` expressions.
pub fn build_plan(
    schema: &Schema,
    projection: &[SelectItem],
    group_by: &[Expr],
) -> Result<AggPlan> {
    let mut aggs = Vec::new();
    let mut agg_types: Vec<ColumnType> = Vec::new();
    let mut plan = Vec::new();
    let mut out_cols = Vec::new();
    let mut arg_exprs: Vec<Expr> = Vec::new();

    // Group columns: a plain column groups by its index; a non-column expression
    // (e.g. `DATE_FORMAT(ts, ...)` for time-bucketing, `status DIV 100`) is
    // registered as a computed column appended after the base columns, and the
    // group key is taken from that appended position. `extend_row` evaluates it
    // per row; the projection of the same expression re-evaluates it on the
    // group's sample row (all rows in a group share the value).
    let mut group_cols = Vec::new();
    for g in group_by {
        match ident_of(g).and_then(|n| col_index(schema, &n).ok()) {
            Some(idx) => group_cols.push(idx),
            None => {
                let idx = schema.columns.len() + arg_exprs.len();
                arg_exprs.push(g.clone());
                group_cols.push(idx);
            }
        }
    }
    let ci = elyra_core::Collation::Ci;

    for item in projection {
        let (expr, alias) = item_expr_and_alias(item)?;

        // 1) A bare aggregate: SUM(x), COUNT(*), ...
        if let Some((func, f)) = agg_of(expr) {
            let slot = register_agg(func, f, schema, &mut aggs, &mut arg_exprs, &mut agg_types)?;
            out_cols.push(ColumnDef {
                name: alias.unwrap_or_else(|| expr.to_string()),
                ty: agg_types[slot].clone(),
                nullable: true,
                collation: ci,
            });
            plan.push(OutCol::Agg(slot));
            continue;
        }

        // 2/3) No aggregate anywhere: a plain group column, or a scalar
        // expression over the group columns (e.g. UPPER(status)).
        if !contains_aggregate(expr) {
            if let Some(idx) = ident_of(expr).and_then(|n| col_index(schema, &n).ok()) {
                out_cols.push(ColumnDef {
                    name: alias.unwrap_or_else(|| schema.columns[idx].name.clone()),
                    ty: schema.columns[idx].ty.clone(),
                    nullable: schema.columns[idx].nullable,
                    collation: ci,
                });
                plan.push(OutCol::Column(idx));
            } else {
                out_cols.push(ColumnDef {
                    name: alias.unwrap_or_else(|| expr.to_string()),
                    ty: infer_computed_type(expr),
                    nullable: true,
                    collation: ci,
                });
                plan.push(OutCol::Computed(Box::new(expr.clone())));
            }
            continue;
        }

        // 4) A scalar expression *containing* aggregates: ROUND(SUM(x), 2),
        // SUM(a)/COUNT(*), COALESCE(SUM(x), 0), ... Rewrite each aggregate to a
        // `__agg_i` reference and evaluate the rest per group.
        let rewritten =
            rewrite_aggregates(expr, schema, &mut aggs, &mut arg_exprs, &mut agg_types)?;
        out_cols.push(ColumnDef {
            name: alias.unwrap_or_else(|| expr.to_string()),
            ty: infer_computed_type(expr),
            nullable: true,
            collation: ci,
        });
        plan.push(OutCol::Computed(Box::new(rewritten)));
    }

    // Evaluation schema for Computed columns: input columns, then one
    // `__agg_i` column per aggregate.
    let mut eval_cols = schema.columns.clone();
    for (i, t) in agg_types.iter().enumerate() {
        eval_cols.push(ColumnDef {
            name: format!("__agg_{i}"),
            ty: t.clone(),
            nullable: true,
            collation: ci,
        });
    }

    let group_collations: Vec<elyra_core::Collation> = group_cols
        .iter()
        .map(|&c| {
            // Appended computed group columns (index past the base schema) have
            // no stored collation; default to case-insensitive.
            schema
                .columns
                .get(c)
                .map(|col| col.collation)
                .unwrap_or(elyra_core::Collation::Ci)
        })
        .collect();
    Ok(AggPlan {
        group_cols,
        group_collations,
        aggs,
        plan,
        out_schema: Schema::new(out_cols),
        arg_exprs,
        input_schema: schema.clone(),
        eval_schema: Schema::new(eval_cols),
    })
}

/// Register an aggregate call, returning its slot in `aggs` (its type is pushed
/// to `agg_types`). Shared by bare aggregates and aggregates nested in
/// expressions.
fn register_agg(
    mut func: AggFunc,
    f: &sqlparser::ast::Function,
    schema: &Schema,
    aggs: &mut Vec<AggSpec>,
    arg_exprs: &mut Vec<Expr>,
    agg_types: &mut Vec<ColumnType>,
) -> Result<usize> {
    let (arg_expr, distinct) = agg_arg(f);
    let (arg, arg_ty): (Option<usize>, Option<ColumnType>) = match arg_expr {
        None => (None, None),
        Some(e) => match ident_of(e).and_then(|n| col_index(schema, &n).ok()) {
            Some(ci) => (Some(ci), Some(schema.columns[ci].ty.clone())),
            None => {
                let ci = schema.columns.len() + arg_exprs.len();
                arg_exprs.push(e.clone());
                (Some(ci), None)
            }
        },
    };
    if func == AggFunc::Count && arg.is_none() {
        func = AggFunc::CountStar;
    }
    let ty = match func {
        AggFunc::CountStar | AggFunc::Count => ColumnType::Int,
        AggFunc::Avg
        | AggFunc::StddevPop
        | AggFunc::StddevSamp
        | AggFunc::VarPop
        | AggFunc::VarSamp => ColumnType::Float,
        AggFunc::GroupConcat => ColumnType::Text,
        AggFunc::Facet => ColumnType::Json,
        AggFunc::Percentile => ColumnType::Float,
        AggFunc::BitOr | AggFunc::BitAnd | AggFunc::BitXor => ColumnType::UInt,
        AggFunc::Sum | AggFunc::Min | AggFunc::Max => arg_ty.unwrap_or(ColumnType::Float),
    };
    let slot = aggs.len();
    aggs.push(AggSpec {
        func,
        arg_col: arg,
        distinct,
        separator: agg_separator(f),
        facet_top: facet_top_of(f),
        percentile: percentile_of(f),
    });
    agg_types.push(ty);
    Ok(slot)
}

/// Does `expr` contain an aggregate function anywhere?
fn contains_aggregate(expr: &Expr) -> bool {
    if agg_of(expr).is_some() {
        return true;
    }
    match expr {
        Expr::Nested(e)
        | Expr::UnaryOp { expr: e, .. }
        | Expr::Cast { expr: e, .. }
        | Expr::IsNull(e)
        | Expr::IsNotNull(e) => contains_aggregate(e),
        Expr::BinaryOp { left, right, .. } => contains_aggregate(left) || contains_aggregate(right),
        Expr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        Expr::Function(f) => {
            if let FunctionArguments::List(list) = &f.args {
                list.args.iter().any(|a| match a {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(e),
                        ..
                    } => contains_aggregate(e),
                    _ => false,
                })
            } else {
                false
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || conditions.iter().any(contains_aggregate)
                || results.iter().any(contains_aggregate)
                || else_result.as_deref().is_some_and(contains_aggregate)
        }
        _ => false,
    }
}

/// Replace every aggregate call in `expr` with an `__agg_i` identifier
/// (registering the aggregate), leaving the surrounding scalar expression
/// intact so it can be evaluated per group.
fn rewrite_aggregates(
    expr: &Expr,
    schema: &Schema,
    aggs: &mut Vec<AggSpec>,
    arg_exprs: &mut Vec<Expr>,
    agg_types: &mut Vec<ColumnType>,
) -> Result<Expr> {
    if let Some((func, f)) = agg_of(expr) {
        let slot = register_agg(func, f, schema, aggs, arg_exprs, agg_types)?;
        return Ok(Expr::Identifier(sqlparser::ast::Ident::new(format!(
            "__agg_{slot}"
        ))));
    }
    if !contains_aggregate(expr) {
        return Ok(expr.clone());
    }
    let mut e = expr.clone();
    match &mut e {
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::Cast { expr: inner, .. } => {
            **inner = rewrite_aggregates(inner, schema, aggs, arg_exprs, agg_types)?;
        }
        Expr::BinaryOp { left, right, .. } => {
            **left = rewrite_aggregates(left, schema, aggs, arg_exprs, agg_types)?;
            **right = rewrite_aggregates(right, schema, aggs, arg_exprs, agg_types)?;
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(list) = &mut f.args {
                for a in &mut list.args {
                    match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                        | FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(x),
                            ..
                        } => {
                            *x = rewrite_aggregates(x, schema, aggs, arg_exprs, agg_types)?;
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(o) = operand {
                **o = rewrite_aggregates(o, schema, aggs, arg_exprs, agg_types)?;
            }
            for c in conditions.iter_mut() {
                *c = rewrite_aggregates(c, schema, aggs, arg_exprs, agg_types)?;
            }
            for r in results.iter_mut() {
                *r = rewrite_aggregates(r, schema, aggs, arg_exprs, agg_types)?;
            }
            if let Some(er) = else_result {
                **er = rewrite_aggregates(er, schema, aggs, arg_exprs, agg_types)?;
            }
        }
        _ => {
            return Err(Error::Unsupported(
                "aggregate nested in an unsupported expression".into(),
            ))
        }
    }
    Ok(e)
}

/// Best-effort column type for a computed output column (the runtime `Value`
/// carries the true type; this only affects result metadata).
fn infer_computed_type(expr: &Expr) -> ColumnType {
    match expr {
        Expr::BinaryOp { op, .. } => {
            use sqlparser::ast::BinaryOperator::*;
            match op {
                Plus | Minus | Multiply | Divide | Modulo => ColumnType::Float,
                MyIntegerDivide => ColumnType::Int,
                BitwiseAnd | BitwiseOr | BitwiseXor | PGBitwiseShiftLeft | PGBitwiseShiftRight => {
                    ColumnType::UInt
                }
                _ => ColumnType::Text,
            }
        }
        Expr::UnaryOp { expr: e, .. } | Expr::Nested(e) => infer_computed_type(e),
        Expr::Function(f) => {
            let n = f
                .name
                .0
                .last()
                .map(|i| i.value.to_ascii_lowercase())
                .unwrap_or_default();
            match n.as_str() {
                "round" | "abs" | "ceil" | "ceiling" | "floor" | "sqrt" | "pow" | "power"
                | "sum" | "avg" | "count" | "min" | "max" | "truncate" | "mod" => ColumnType::Float,
                _ => ColumnType::Text,
            }
        }
        _ => ColumnType::Text,
    }
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
    if plan.arg_exprs().is_empty() {
        for row in &rows {
            agg.feed(row);
        }
    } else {
        for row in &rows {
            agg.feed(&plan.extend_row(row)?);
        }
    }
    if agg.overflowed() {
        return Err(Error::Query(format!(
            "GROUP BY produced too many distinct groups (limit {}); add a more \
             selective WHERE or raise ELYRASQL_GROUP_MAX_GROUPS",
            elyra_olap::default_max_groups()
        )));
    }
    plan.finalize(agg)
}
