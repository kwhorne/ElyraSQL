//! Compiled predicates for the aggregation/scan hot path.
//!
//! [`predicate::matches`](crate::predicate::matches) interprets the filter
//! expression against every row, re-resolving column names (a linear,
//! case-insensitive schema scan) each time. For the common analytical filter --
//! a conjunction of `column <cmp> numeric-literal` -- that per-row name
//! resolution and expression walk dominate a filtered aggregation.
//!
//! [`CompiledPredicate`] pre-resolves each column to an index once and evaluates
//! with native `f64` comparisons. It only accepts that common shape; anything
//! else returns `None` from [`compile`] and the caller falls back to the full
//! interpreter, so semantics never diverge.

use std::collections::HashSet;

use elyra_core::{canonical_f64_bits, ColumnType, Schema, Value};
use sqlparser::ast::{BinaryOperator, Expr, Value as SqlValue};

use crate::predicate;

#[derive(Clone, Copy)]
enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Op {
    #[inline]
    fn test(self, a: f64, b: f64) -> bool {
        match self {
            Op::Eq => a == b,
            Op::Ne => a != b,
            Op::Lt => a < b,
            Op::Le => a <= b,
            Op::Gt => a > b,
            Op::Ge => a >= b,
        }
    }
    fn flip(self) -> Op {
        match self {
            Op::Lt => Op::Gt,
            Op::Le => Op::Ge,
            Op::Gt => Op::Lt,
            Op::Ge => Op::Le,
            other => other,
        }
    }
}

#[derive(Clone, Copy)]
struct Cmp {
    col: usize,
    op: Op,
    rhs: f64,
}

/// `column IN (numeric literals)` as an O(1) membership test.
///
/// Interpreting `IN` walks the list for every row, so a long list dominates a
/// filtered scan: 500 literals over 200k rows is 100M comparisons. Values are
/// hashed by [`canonical_f64_bits`] so `0.0`/`-0.0` agree and NaN (which can never
/// equal anything) is simply absent from the set.
#[derive(Clone)]
struct InSet {
    col: usize,
    set: HashSet<u64>,
    negated: bool,
    /// Range spanned by the set, so a zone map can still skip chunks that cannot
    /// contain any listed value. Unused when `negated` (the complement of a set is
    /// not an interval).
    lo: f64,
    hi: f64,
}

#[derive(Clone)]
enum Term {
    Cmp(Cmp),
    In(InSet),
}

/// One `column <op> literal` bound exposed for zone-map (min/max) chunk
/// skipping. `op`/`rhs` mirror an internal comparison; `col` is a base column.
#[derive(Clone, Copy)]
pub struct ColBound {
    pub col: usize,
    pub op: BoundOp,
    pub rhs: f64,
}

#[derive(Clone, Copy, PartialEq)]
pub enum BoundOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A conjunction of `column <cmp> numeric-literal` comparisons over numeric
/// (`Int`/`Float`) columns, with column indices pre-resolved.
#[derive(Clone)]
pub struct CompiledPredicate {
    conj: Vec<Term>,
}

impl CompiledPredicate {
    /// The conjunction's per-column numeric bounds, for zone-map chunk skipping.
    /// Because the predicate is a pure AND of these, a chunk whose [min, max]
    /// range for some column cannot satisfy its bound contains no matching row.
    pub fn bounds(&self) -> Vec<ColBound> {
        let mut out = Vec::with_capacity(self.conj.len());
        for t in &self.conj {
            match t {
                Term::Cmp(c) => out.push(ColBound {
                    col: c.col,
                    op: match c.op {
                        Op::Eq => BoundOp::Eq,
                        Op::Ne => BoundOp::Ne,
                        Op::Lt => BoundOp::Lt,
                        Op::Le => BoundOp::Le,
                        Op::Gt => BoundOp::Gt,
                        Op::Ge => BoundOp::Ge,
                    },
                    rhs: c.rhs,
                }),
                // `IN` constrains the column to the span of its values, which is
                // enough for chunk skipping. `NOT IN` constrains nothing.
                Term::In(s) if !s.negated => {
                    out.push(ColBound {
                        col: s.col,
                        op: BoundOp::Ge,
                        rhs: s.lo,
                    });
                    out.push(ColBound {
                        col: s.col,
                        op: BoundOp::Le,
                        rhs: s.hi,
                    });
                }
                Term::In(_) => {}
            }
        }
        out
    }

    /// True if every comparison holds. A NULL / non-numeric column value fails
    /// the comparison (matching the interpreter's numeric semantics).
    #[inline]
    pub fn matches(&self, row: &[Value]) -> bool {
        self.conj.iter().all(|t| match t {
            Term::Cmp(c) => match row.get(c.col).and_then(|v| v.as_f64()) {
                Some(x) => c.op.test(x, c.rhs),
                None => false,
            },
            Term::In(s) => match row.get(s.col).and_then(|v| v.as_f64()) {
                // A NULL operand makes `IN` and `NOT IN` alike UNKNOWN, so the row
                // is excluded either way -- the same as the interpreter.
                Some(x) => s.set.contains(&canonical_f64_bits(x)) != s.negated,
                None => false,
            },
        })
    }
}

/// Compile a filter into a [`CompiledPredicate`], or `None` if it isn't a pure
/// conjunction of numeric-column comparisons (caller then uses the interpreter).
pub fn compile(expr: &Expr, schema: &Schema) -> Option<CompiledPredicate> {
    let mut conj = Vec::new();
    collect(expr, schema, &mut conj)?;
    if conj.is_empty() {
        return None;
    }
    Some(CompiledPredicate { conj })
}

fn collect(e: &Expr, schema: &Schema, out: &mut Vec<Term>) -> Option<()> {
    match e {
        Expr::Nested(inner) => collect(inner, schema, out),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect(left, schema, out)?;
            collect(right, schema, out)
        }
        Expr::BinaryOp { left, op, right } => {
            let cmp = cmp_op(op)?;
            if let (Some(col), Some(rhs)) = (numeric_col(left, schema), num_lit(right)) {
                out.push(Term::Cmp(Cmp { col, op: cmp, rhs }));
                Some(())
            } else if let (Some(rhs), Some(col)) = (num_lit(left), numeric_col(right, schema)) {
                out.push(Term::Cmp(Cmp {
                    col,
                    op: cmp.flip(),
                    rhs,
                }));
                Some(())
            } else {
                None
            }
        }
        // `column IN (numeric literals)` / `NOT IN`. Anything else in the list -- a
        // column reference, an expression, NULL, a non-numeric literal -- is
        // declined so the interpreter keeps ownership of those semantics.
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let col = numeric_col(expr, schema)?;
            if list.is_empty() {
                return None;
            }
            let mut set = HashSet::with_capacity(list.len());
            let mut lo = f64::INFINITY;
            let mut hi = f64::NEG_INFINITY;
            for item in list {
                let v = num_lit(item)?;
                // NaN can never equal anything, so it contributes no membership and
                // must not widen the bounds either.
                if v.is_nan() {
                    continue;
                }
                set.insert(canonical_f64_bits(v));
                lo = lo.min(v);
                hi = hi.max(v);
            }
            if set.is_empty() {
                return None;
            }
            out.push(Term::In(InSet {
                col,
                set,
                negated: *negated,
                lo,
                hi,
            }));
            Some(())
        }
        _ => None,
    }
}

fn cmp_op(op: &BinaryOperator) -> Option<Op> {
    Some(match op {
        BinaryOperator::Eq => Op::Eq,
        BinaryOperator::NotEq => Op::Ne,
        BinaryOperator::Lt => Op::Lt,
        BinaryOperator::LtEq => Op::Le,
        BinaryOperator::Gt => Op::Gt,
        BinaryOperator::GtEq => Op::Ge,
        _ => return None,
    })
}

/// Resolve an identifier to a column index, but only if it is an `Int`/`Float`
/// column (so native f64 comparison matches the interpreter's semantics).
fn numeric_col(e: &Expr, schema: &Schema) -> Option<usize> {
    let name = match e {
        Expr::Identifier(id) => id.value.clone(),
        Expr::CompoundIdentifier(parts) => parts.last()?.value.clone(),
        _ => return None,
    };
    let i = schema
        .columns
        .iter()
        .position(|c| predicate::identifier_eq(&c.name, &name))?;
    match schema.columns[i].ty {
        ColumnType::Int | ColumnType::Float => Some(i),
        _ => None,
    }
}

fn num_lit(e: &Expr) -> Option<f64> {
    match e {
        Expr::Value(SqlValue::Number(n, _)) => n.parse::<f64>().ok(),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => num_lit(expr).map(|v| -v),
        _ => None,
    }
}

#[cfg(test)]
mod in_tests {
    use super::*;
    use elyra_core::ColumnDef;

    fn schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("g", ColumnType::Int, true),
            ColumnDef::new("s", ColumnType::Text, true),
        ])
    }

    fn parse(sql: &str) -> Expr {
        let stmt = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::MySqlDialect {},
            &format!("SELECT 1 WHERE {sql}"),
        )
        .unwrap();
        match &stmt[0] {
            sqlparser::ast::Statement::Query(q) => match q.body.as_ref() {
                sqlparser::ast::SetExpr::Select(s) => s.selection.clone().unwrap(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    fn row(g: Option<i64>) -> Vec<Value> {
        vec![g.map_or(Value::Null, Value::Int), Value::Null]
    }

    #[test]
    fn in_list_membership_matches_interpreter_semantics() {
        let sc = schema();
        let p = compile(&parse("g IN (1, 3, 5)"), &sc).expect("compiles");
        assert!(p.matches(&row(Some(1))));
        assert!(p.matches(&row(Some(5))));
        assert!(!p.matches(&row(Some(2))));
        // A NULL operand is UNKNOWN, so the row is excluded -- as `predicate` does.
        assert!(!p.matches(&row(None)));

        let p = compile(&parse("g NOT IN (1, 3)"), &sc).expect("compiles");
        assert!(p.matches(&row(Some(2))));
        assert!(!p.matches(&row(Some(1))));
        // NOT IN with a NULL operand is UNKNOWN too, so still excluded.
        assert!(!p.matches(&row(None)));
    }

    #[test]
    fn in_list_yields_span_bounds_for_zone_maps() {
        let sc = schema();
        let p = compile(&parse("g IN (7, 2, 5)"), &sc).unwrap();
        let b = p.bounds();
        // The set constrains the column to [2, 7]; a chunk outside that can be
        // skipped. Two bounds, one per end.
        assert_eq!(b.len(), 2);
        assert!(b.iter().any(|x| x.op == BoundOp::Ge && x.rhs == 2.0));
        assert!(b.iter().any(|x| x.op == BoundOp::Le && x.rhs == 7.0));
        // The complement of a set is not an interval, so NOT IN must not prune.
        let p = compile(&parse("g NOT IN (7, 2)"), &sc).unwrap();
        assert!(p.bounds().is_empty());
    }

    #[test]
    fn declines_shapes_whose_semantics_it_cannot_reproduce() {
        let sc = schema();
        // A NULL element changes the three-valued outcome; leave it to the
        // interpreter rather than approximate it.
        assert!(compile(&parse("g IN (1, NULL)"), &sc).is_none());
        // Non-numeric column, non-literal element, and an empty list.
        assert!(compile(&parse("s IN ('a')"), &sc).is_none());
        assert!(compile(&parse("g IN (1, g)"), &sc).is_none());
        // Mixed with a comparison: still a pure conjunction, so it compiles.
        let p = compile(&parse("g IN (1,2) AND g > 0"), &sc).expect("compiles");
        assert!(p.matches(&row(Some(1))));
        assert!(!p.matches(&row(Some(3))));
    }

    #[test]
    fn zero_and_negative_literals_compare_by_value() {
        let sc = schema();
        let p = compile(&parse("g IN (0, -3)"), &sc).unwrap();
        // -0.0 and 0.0 must be the same member.
        assert!(p.matches(&[Value::Float(-0.0), Value::Null]));
        assert!(p.matches(&row(Some(0))));
        assert!(p.matches(&row(Some(-3))));
        assert!(!p.matches(&row(Some(3))));
    }
}
