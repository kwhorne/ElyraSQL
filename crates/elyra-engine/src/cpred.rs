//! Compiled predicates for the aggregation/scan hot path.
//!
//! [`predicate::matches`](crate::predicate::matches) interprets the filter
//! expression against every row, re-resolving column names (a linear,
//! case-insensitive schema scan) each time. For the common analytical filter --
//! a conjunction of `column <cmp> numeric-literal` -- that per-row name
//! resolution and expression walk dominate a filtered aggregation.
//!
//! [`CompiledPredicate`] pre-resolves each column to an index once and evaluates
//! with native comparisons: an integer column against an integer literal as
//! integers, a floating-point column as `f64`. It only accepts that common
//! shape; anything else returns `None` from [`compile`] and the caller falls
//! back to the full interpreter, so semantics never diverge.
//!
//! An integer column was compared as `f64` too, which cannot tell integers
//! apart above 2^53: `cust = 9007199254740993` counted the rows holding
//! 9007199254740992 as well.

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
    fn test_ord(self, ord: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::*;
        match self {
            Op::Eq => ord == Equal,
            Op::Ne => ord != Equal,
            Op::Lt => ord == Less,
            Op::Le => ord != Greater,
            Op::Gt => ord == Greater,
            Op::Ge => ord != Less,
        }
    }

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

/// The literal side of a comparison: exact for an integer column, `f64` for a
/// floating-point one.
#[derive(Clone, Copy)]
enum Lit {
    Int(i128),
    Float(f64),
}

#[derive(Clone, Copy)]
struct Cmp {
    col: usize,
    op: Op,
    rhs: Lit,
}

/// The members of an `IN` list: integers for an integer column, `f64` bits
/// (canonical, so `0.0`/`-0.0` agree) for a floating-point one.
#[derive(Clone)]
enum Members {
    Ints(HashSet<i128>),
    Floats(HashSet<u64>),
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
    set: Members,
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
                Term::Cmp(Cmp {
                    col,
                    op,
                    rhs: Lit::Float(rhs),
                }) => out.push(ColBound {
                    col: *col,
                    op: match op {
                        Op::Eq => BoundOp::Eq,
                        Op::Ne => BoundOp::Ne,
                        Op::Lt => BoundOp::Lt,
                        Op::Le => BoundOp::Le,
                        Op::Gt => BoundOp::Gt,
                        Op::Ge => BoundOp::Ge,
                    },
                    rhs: *rhs,
                }),
                // An integer bound may have no exact `f64`, so it is widened the
                // safe way for each operator: a chunk is only skipped when it
                // certainly holds no match (the zone map widens its min/max
                // outward to the same end).
                Term::Cmp(Cmp {
                    col,
                    op,
                    rhs: Lit::Int(rhs),
                }) => {
                    let (below, above) = (f64_at_most(*rhs), f64_at_least(*rhs));
                    let col = *col;
                    match op {
                        Op::Gt | Op::Ge => out.push(ColBound {
                            col,
                            op: BoundOp::Ge,
                            rhs: below,
                        }),
                        Op::Lt | Op::Le => out.push(ColBound {
                            col,
                            op: BoundOp::Le,
                            rhs: above,
                        }),
                        Op::Eq => {
                            out.push(ColBound {
                                col,
                                op: BoundOp::Ge,
                                rhs: below,
                            });
                            out.push(ColBound {
                                col,
                                op: BoundOp::Le,
                                rhs: above,
                            });
                        }
                        Op::Ne if below == above => out.push(ColBound {
                            col,
                            op: BoundOp::Ne,
                            rhs: below,
                        }),
                        Op::Ne => {}
                    }
                }
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
            Term::Cmp(c) => match (row.get(c.col), c.rhs) {
                (Some(v), Lit::Int(rhs)) => match int_value(v) {
                    Some(x) => c.op.test_ord(x.cmp(&rhs)),
                    None => false,
                },
                (Some(v), Lit::Float(rhs)) => match v.as_f64() {
                    Some(x) => c.op.test(x, rhs),
                    None => false,
                },
                (None, _) => false,
            },
            // A NULL operand makes `IN` and `NOT IN` alike UNKNOWN, so the row
            // is excluded either way -- the same as the interpreter.
            Term::In(s) => match (row.get(s.col), &s.set) {
                (Some(v), Members::Ints(set)) => match int_value(v) {
                    Some(x) => set.contains(&x) != s.negated,
                    None => false,
                },
                (Some(v), Members::Floats(set)) => match v.as_f64() {
                    Some(x) => set.contains(&canonical_f64_bits(x)) != s.negated,
                    None => false,
                },
                (None, _) => false,
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
            if let Some((col, int)) = numeric_col(left, schema) {
                let rhs = lit_for(right, int)?;
                out.push(Term::Cmp(Cmp { col, op: cmp, rhs }));
                Some(())
            } else if let Some((col, int)) = numeric_col(right, schema) {
                let rhs = lit_for(left, int)?;
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
            let (col, int) = numeric_col(expr, schema)?;
            if list.is_empty() {
                return None;
            }
            let mut ints = HashSet::new();
            let mut floats = HashSet::new();
            let mut lo = f64::INFINITY;
            let mut hi = f64::NEG_INFINITY;
            for item in list {
                match lit_for(item, int)? {
                    Lit::Int(v) => {
                        ints.insert(v);
                        lo = lo.min(f64_at_most(v));
                        hi = hi.max(f64_at_least(v));
                    }
                    // NaN can never equal anything, so it contributes no
                    // membership and must not widen the bounds either.
                    Lit::Float(v) if v.is_nan() => {}
                    Lit::Float(v) => {
                        floats.insert(canonical_f64_bits(v));
                        lo = lo.min(v);
                        hi = hi.max(v);
                    }
                }
            }
            let set = if int && !ints.is_empty() {
                Members::Ints(ints)
            } else if !int && !floats.is_empty() {
                Members::Floats(floats)
            } else {
                return None;
            };
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

/// Resolve an identifier to a column index, but only if it is an `Int` or
/// `Float` column (so a native comparison matches the interpreter's
/// semantics), with whether it is the integer one.
fn numeric_col(e: &Expr, schema: &Schema) -> Option<(usize, bool)> {
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
        ColumnType::Int => Some((i, true)),
        ColumnType::Float => Some((i, false)),
        _ => None,
    }
}

/// The literal `e` as compared against an integer column (`int`) or a
/// floating-point one: an integer column takes only an integer literal
/// (compared exactly); a decimal or exponent literal there is left to the
/// interpreter, which compares it as MySQL does. A floating-point column takes
/// any numeric literal as `f64`.
fn lit_for(e: &Expr, int: bool) -> Option<Lit> {
    let (text, negative) = num_text(e)?;
    if int {
        if !text.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let v = text.parse::<i128>().ok()?;
        Some(Lit::Int(if negative { -v } else { v }))
    } else {
        let v = text.parse::<f64>().ok()?;
        Some(Lit::Float(if negative { -v } else { v }))
    }
}

/// The digits of a numeric literal and whether it is negated.
fn num_text(e: &Expr) -> Option<(&str, bool)> {
    match e {
        Expr::Value(SqlValue::Number(n, _)) => Some((n.as_str(), false)),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => num_text(expr).map(|(text, negative)| (text, !negative)),
        _ => None,
    }
}

/// An integer column's value as an exact integer.
fn int_value(v: &Value) -> Option<i128> {
    match v {
        Value::Int(i) => Some(i128::from(*i)),
        Value::UInt(u) => Some(i128::from(*u)),
        Value::Bool(b) => Some(i128::from(*b)),
        _ => None,
    }
}

/// The largest `f64` that is not above `v`.
pub(crate) fn f64_at_most(v: i128) -> f64 {
    let f = v as f64;
    if (f as i128) > v {
        f.next_down()
    } else {
        f
    }
}

/// The smallest `f64` that is not below `v`.
pub(crate) fn f64_at_least(v: i128) -> f64 {
    let f = v as f64;
    if (f as i128) < v {
        f.next_up()
    } else {
        f
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
            ColumnDef::new("f", ColumnType::Float, true),
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
        vec![g.map_or(Value::Null, Value::Int), Value::Null, Value::Null]
    }

    fn float_row(f: f64) -> Vec<Value> {
        vec![Value::Null, Value::Null, Value::Float(f)]
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
        assert!(p.matches(&row(Some(0))));
        assert!(p.matches(&row(Some(-3))));
        assert!(!p.matches(&row(Some(3))));
        // -0.0 and 0.0 must be the same member of a floating-point column.
        let p = compile(&parse("f IN (0, -3)"), &sc).unwrap();
        assert!(p.matches(&float_row(-0.0)));
        assert!(p.matches(&float_row(-3.0)));
    }

    /// Above 2^53 neighbouring integers share an `f64`: an integer column
    /// compared as `f64` counted 9007199254740992 as `= 9007199254740993`.
    #[test]
    fn an_integer_column_compares_exactly_above_two_to_the_fifty_third() {
        let sc = schema();
        const N: i64 = 9_007_199_254_740_993;
        for (sql, want) in [
            ("g = 9007199254740993", [false, true, false]),
            ("g <> 9007199254740993", [true, false, true]),
            ("g < 9007199254740993", [true, false, false]),
            ("g >= 9007199254740993", [false, true, true]),
            ("9007199254740993 < g", [false, false, true]),
            ("g IN (9007199254740993)", [false, true, false]),
            ("g NOT IN (9007199254740993)", [true, false, true]),
        ] {
            let p = compile(&parse(sql), &sc).expect(sql);
            for (value, want) in [N - 1, N, N + 1].into_iter().zip(want) {
                assert_eq!(p.matches(&row(Some(value))), want, "{sql} for {value}");
            }
        }
        // A decimal or exponent literal against an integer column is left to
        // the interpreter, which compares it as MySQL does.
        assert!(compile(&parse("g = 9007199254740992.5"), &sc).is_none());
        assert!(compile(&parse("g = 9007199254740993e0"), &sc).is_none());
        assert!(compile(&parse("g IN (1, 2.5)"), &sc).is_none());
    }

    /// Zone-map bounds for an integer with no exact `f64` are widened so a chunk
    /// holding the value is never skipped.
    #[test]
    fn integer_bounds_round_outward() {
        let sc = schema();
        const N: i128 = 9_007_199_254_740_993;
        let p = compile(&parse("g = 9007199254740993"), &sc).unwrap();
        let bounds = p.bounds();
        let ge = bounds.iter().find(|b| b.op == BoundOp::Ge).unwrap();
        let le = bounds.iter().find(|b| b.op == BoundOp::Le).unwrap();
        assert!((ge.rhs as i128) <= N && (le.rhs as i128) >= N);
        assert_eq!(f64_at_most(N) as i128, N - 1);
        assert_eq!(f64_at_least(N) as i128, N + 1);
        assert_eq!(f64_at_most(N - 1), f64_at_least(N - 1));
        // `<>` an inexact integer constrains no chunk.
        assert!(compile(&parse("g <> 9007199254740993"), &sc)
            .unwrap()
            .bounds()
            .is_empty());
    }
}
