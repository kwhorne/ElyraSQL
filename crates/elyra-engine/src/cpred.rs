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

use elyra_core::{ColumnType, Schema, Value};
use sqlparser::ast::{BinaryOperator, Expr, Value as SqlValue};

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

/// A conjunction of `column <cmp> numeric-literal` comparisons over numeric
/// (`Int`/`Float`) columns, with column indices pre-resolved.
#[derive(Clone)]
pub struct CompiledPredicate {
    conj: Vec<Cmp>,
}

impl CompiledPredicate {
    /// True if every comparison holds. A NULL / non-numeric column value fails
    /// the comparison (matching the interpreter's numeric semantics).
    #[inline]
    pub fn matches(&self, row: &[Value]) -> bool {
        self.conj
            .iter()
            .all(|c| match row.get(c.col).and_then(|v| v.as_f64()) {
                Some(x) => c.op.test(x, c.rhs),
                None => false,
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

fn collect(e: &Expr, schema: &Schema, out: &mut Vec<Cmp>) -> Option<()> {
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
                out.push(Cmp { col, op: cmp, rhs });
                Some(())
            } else if let (Some(rhs), Some(col)) = (num_lit(left), numeric_col(right, schema)) {
                out.push(Cmp {
                    col,
                    op: cmp.flip(),
                    rhs,
                });
                Some(())
            } else {
                None
            }
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
        .position(|c| c.name.eq_ignore_ascii_case(&name))?;
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
