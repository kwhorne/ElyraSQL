//! Expressions compiled once per statement, evaluated once per row.
//!
//! [`crate::predicate::eval_row`] walks the parsed AST for every row: a column
//! reference is resolved by name against the schema, a literal is re-parsed from
//! its text, and a comparison re-derives its collation from the operand
//! expressions. For a scan that is the same work repeated millions of times.
//! [`CExpr`] does that work once -- column references become indexes, literals
//! become values, a comparison's collation is fixed -- and keeps everything else
//! as the original AST node, evaluated exactly as before.
//!
//! Semantics are not reimplemented. Operators go through the evaluator's own
//! [`binary`](crate::predicate::binary) and
//! [`unary_value`](crate::predicate::unary_value), and any node without a
//! compiled form is evaluated by `eval_row` itself, so a compiled expression
//! returns what the interpreter returns, error for error.

use crate::predicate::{self, binary, cmp_collation, unary_value};
use elyra_core::{Collation, Result, Schema, Value};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

/// A compiled expression over rows of one schema. Owns its parts (fallback
/// nodes are cloned once at compile time), so a scan worker can carry it.
#[derive(Clone, Debug)]
pub(crate) enum CExpr {
    /// A column, resolved to its index.
    Col(usize),
    /// A literal, parsed once.
    Lit(Value),
    Binary {
        op: BinaryOperator,
        left: Box<CExpr>,
        right: Box<CExpr>,
        collation: Collation,
    },
    Unary {
        op: UnaryOperator,
        operand: Box<CExpr>,
    },
    IsNull {
        operand: Box<CExpr>,
        negated: bool,
    },
    /// `operand [NOT] IN (items)` where every item is a constant or NULL, so
    /// each is evaluated once here instead of once per row. Each item is kept
    /// as evaluated and as an integer column compares it (see
    /// `predicate::against_integer_column`), which depends on the operand's
    /// value and so is chosen per row.
    InList {
        operand: Box<CExpr>,
        /// Whether the operand is a column reference: only then does an
        /// integer operand compare the items as integers.
        column: bool,
        items: Vec<(Value, Value)>,
        collation: Collation,
        negated: bool,
    },
    /// No compiled form: evaluated by the interpreter.
    Ast(Box<Expr>),
}

impl CExpr {
    /// Compile `expr` for rows of `schema`. Never fails: anything that does not
    /// compile -- or would be an error to evaluate -- stays an [`CExpr::Ast`]
    /// node, so the interpreter reports it at the same point as before.
    pub(crate) fn compile(expr: &Expr, schema: &Schema) -> CExpr {
        match expr {
            Expr::Nested(e) => CExpr::compile(e, schema),
            Expr::Value(v) => match crate::eval::literal(v) {
                Ok(val) => CExpr::Lit(val),
                Err(_) => ast(expr),
            },
            Expr::Identifier(id) => {
                // System variables and niladic functions (CURRENT_TIMESTAMP as
                // a bare word) keep their interpreted meaning.
                if id.value.starts_with("@@") {
                    return ast(expr);
                }
                match predicate::bare_column(&id.value, schema) {
                    Some(idx) => CExpr::Col(idx),
                    None => ast(expr),
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.first().is_some_and(|p| p.value.starts_with("@@")) {
                    return ast(expr);
                }
                match predicate::resolve_index_parts(parts, schema) {
                    Ok(idx) => CExpr::Col(idx),
                    Err(_) => ast(expr),
                }
            }
            Expr::BinaryOp { left, op, right } if compilable_binary(op, left, right) => {
                let mut l = CExpr::compile(left, schema);
                let mut r = CExpr::compile(right, schema);
                // An integer column against a constant compares as integers
                // when the constant converts without loss, as in the
                // interpreter (`predicate::against_integer_column`): the
                // constant is converted once, here.
                if matches!(
                    op,
                    BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::Spaceship
                ) {
                    integer_column_constant(&l, right, &mut r, schema);
                    integer_column_constant(&r, left, &mut l, schema);
                }
                CExpr::Binary {
                    op: op.clone(),
                    left: Box::new(l),
                    right: Box::new(r),
                    collation: cmp_collation(left, right, schema),
                }
            }
            Expr::UnaryOp { op, expr: e } => CExpr::Unary {
                op: *op,
                operand: Box::new(CExpr::compile(e, schema)),
            },
            Expr::IsNull(e) => CExpr::IsNull {
                operand: Box::new(CExpr::compile(e, schema)),
                negated: false,
            },
            Expr::IsNotNull(e) => CExpr::IsNull {
                operand: Box::new(CExpr::compile(e, schema)),
                negated: true,
            },
            Expr::InList {
                expr: operand,
                list,
                negated,
            } => compile_in_list(expr, operand, list, *negated, schema),
            _ => ast(expr),
        }
    }

    /// Evaluate against one row of the schema it was compiled for.
    pub(crate) fn eval(&self, schema: &Schema, row: &[Value]) -> Result<Value> {
        match self {
            CExpr::Col(idx) => Ok(row.get(*idx).cloned().unwrap_or(Value::Null)),
            CExpr::Lit(v) => Ok(v.clone()),
            CExpr::Binary {
                op,
                left,
                right,
                collation,
            } => binary(
                left.eval(schema, row)?,
                op,
                || right.eval(schema, row),
                || *collation,
            ),
            CExpr::Unary { op, operand } => unary_value(op, operand.eval(schema, row)?),
            CExpr::IsNull { operand, negated } => Ok(Value::Bool(
                operand.eval(schema, row)?.is_null() != *negated,
            )),
            CExpr::InList {
                operand,
                column,
                items,
                collation,
                negated,
            } => {
                // As `predicate::eval_row` evaluates `Expr::InList`.
                let v = operand.eval(schema, row)?;
                if v.is_null() {
                    return Ok(Value::Null);
                }
                let as_integer = *column && matches!(v, Value::Int(_) | Value::UInt(_));
                let mut saw_null = false;
                for (value, integer) in items {
                    let rv = if as_integer { integer } else { value };
                    if rv.is_null() {
                        saw_null = true;
                        continue;
                    }
                    if v.compare_coll(rv, *collation) == Some(std::cmp::Ordering::Equal) {
                        return Ok(Value::Bool(!*negated));
                    }
                }
                if saw_null {
                    return Ok(Value::Null);
                }
                Ok(Value::Bool(*negated))
            }
            CExpr::Ast(e) => predicate::eval_row(e, schema, row),
        }
    }

    /// Whether the row passes, as a `WHERE`/`ON` clause: NULL is false.
    pub(crate) fn matches(&self, schema: &Schema, row: &[Value]) -> Result<bool> {
        Ok(predicate::truthy(&self.eval(schema, row)?))
    }
}

/// If `column` is an integer column and `constant` a literal, replace its
/// compiled form with the constant as the column compares it.
fn integer_column_constant(column: &CExpr, constant: &Expr, compiled: &mut CExpr, schema: &Schema) {
    let CExpr::Col(index) = column else {
        return;
    };
    let integer = schema.columns.get(*index).is_some_and(|c| {
        matches!(
            c.ty,
            elyra_core::ColumnType::Int | elyra_core::ColumnType::UInt
        )
    });
    if !integer || !predicate::is_constant(constant) {
        return;
    }
    if let Ok(value) = predicate::eval_row(constant, schema, &[]) {
        *compiled = CExpr::Lit(predicate::int_constant(value));
    }
}

/// Compile `operand [NOT] IN (list)` when every item is a constant or NULL and
/// evaluates without error; otherwise the interpreter keeps it (a row-tuple
/// operand, a column or expression in the list, or an item that would fail --
/// which the interpreter reports only when it reaches that item).
fn compile_in_list(
    whole: &Expr,
    operand: &Expr,
    list: &[Expr],
    negated: bool,
    schema: &Schema,
) -> CExpr {
    if matches!(operand, Expr::Tuple(_)) {
        return ast(whole);
    }
    let mut items = Vec::with_capacity(list.len());
    for item in list {
        let constant = predicate::is_constant(item);
        let null = matches!(item, Expr::Value(sqlparser::ast::Value::Null));
        if !constant && !null {
            return ast(whole);
        }
        let Ok(value) = predicate::eval_row(item, schema, &[]) else {
            return ast(whole);
        };
        // `against_integer_column` converts only a constant item.
        let integer = if constant {
            predicate::int_constant(value.clone())
        } else {
            value.clone()
        };
        items.push((value, integer));
    }
    // The column's collation, or an explicit COLLATE on either side, as the
    // interpreter picks it: the first item whose comparison is not `Ci`.
    let collation = list
        .iter()
        .map(|item| cmp_collation(operand, item, schema))
        .find(|c| *c != Collation::Ci)
        .unwrap_or(Collation::Ci);
    CExpr::InList {
        operand: Box::new(CExpr::compile(operand, schema)),
        column: predicate::is_column_ref(operand),
        items,
        collation,
        negated,
    }
}

fn ast(expr: &Expr) -> CExpr {
    CExpr::Ast(Box::new(expr.clone()))
}

/// The binary operators the interpreter sends straight to `binary()`. JSON
/// paths and `INTERVAL` arithmetic have their own handling in `eval_row` and
/// stay interpreted.
fn compilable_binary(op: &BinaryOperator, left: &Expr, right: &Expr) -> bool {
    match op {
        BinaryOperator::Arrow | BinaryOperator::LongArrow => false,
        BinaryOperator::Plus | BinaryOperator::Minus
            if matches!(right, Expr::Interval(_)) || matches!(left, Expr::Interval(_)) =>
        {
            false
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use elyra_core::{ColumnDef, ColumnType};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    fn schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("a", ColumnType::Int, true),
            ColumnDef::new("f", ColumnType::Float, true),
            ColumnDef::new("s", ColumnType::Text, true),
        ])
    }

    fn expr(sql: &str) -> Expr {
        Parser::new(&MySqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_expr()
            .unwrap()
    }

    /// Compiled and interpreted evaluation agree on every row, value for value
    /// and error for error, across operators, NULLs, collation and fallbacks.
    #[test]
    fn compiled_matches_interpreted() {
        let sch = schema();
        let rows = [
            vec![Value::Int(3), Value::Float(2.5), Value::Text("Abc".into())],
            vec![Value::Int(-3), Value::Float(0.0), Value::Text("x".into())],
            vec![Value::Null, Value::Float(-1.0), Value::Text("abc".into())],
            vec![Value::Int(i64::MAX), Value::Null, Value::Null],
            vec![Value::Int(0), Value::Float(0.0), Value::Text("".into())],
        ];
        let cases = [
            "a * 2 + 1",
            "f * 2 + 1",
            "a + f",
            "a / 2",
            "a DIV 2",
            "a % 2",
            "-a",
            "NOT a",
            "a > 1 AND f < 3",
            "a > 1 OR f IS NULL",
            "a IS NULL",
            "s IS NOT NULL",
            "s = 'ABC'",
            "s <=> NULL",
            "a <=> NULL",
            "a = 3",
            "a + 1",             // overflows on i64::MAX: same error
            "CONCAT(s, 'x')",    // function: interpreted fallback
            "a BETWEEN 1 AND 5", // interpreted fallback
            "a IN (1, 3, NULL)",
            "a IN (1, 3)",
            "a NOT IN (1, 3)",
            "a NOT IN (1, NULL)",
            "a IN ('3', '3.0', ' 3')", // integer column against text constants
            "a IN (3.0, 2.5)",
            "a IN (-3, +0)",
            "f IN (2.5, 0)",
            "f IN ('2.5')",
            "s IN ('abc', 'x')", // case-insensitive
            "s IN ('abc' COLLATE utf8mb4_bin)",
            "s NOT IN ('ABC', NULL)",
            "a + 0 IN (3, 0)", // not a column reference: no integer conversion
            "a IN (f, 3)",     // a column in the list: interpreted fallback
            "a IN (1 + 2)",    // an expression in the list: interpreted fallback
            "(a, f) IN ((3, 2.5))",
            "nope IN (1, 2)",
            "(a)",
            "nope + 1", // unknown column: same error
            "a & 6",
            "CURRENT_DATE IS NOT NULL",
        ];
        for c in cases {
            let e = expr(c);
            let ce = CExpr::compile(&e, &sch);
            for row in &rows {
                let want = predicate::eval_row(&e, &sch, row).map_err(|e| e.to_string());
                let got = ce.eval(&sch, row).map_err(|e| e.to_string());
                assert_eq!(got, want, "{c} on {row:?}");
            }
        }
    }

    /// A list of constants is compiled -- the point of it: the interpreter
    /// evaluated every item for every row -- and anything else is not.
    #[test]
    fn in_lists_of_constants_compile() {
        let sch = schema();
        let compiled = |sql: &str| matches!(CExpr::compile(&expr(sql), &sch), CExpr::InList { .. });
        assert!(compiled("a IN (1, 3, NULL)"));
        assert!(compiled("a NOT IN ('3', -4)"));
        assert!(compiled("s IN ('abc')"));
        assert!(!compiled("a IN (f, 3)"));
        assert!(!compiled("a IN (1 + 2)"));
        assert!(!compiled("(a, f) IN ((3, 2.5))"));
    }

    #[test]
    fn columns_and_literals_are_resolved_once() {
        let sch = schema();
        let e = expr("a * 2 + 1");
        match CExpr::compile(&e, &sch) {
            CExpr::Binary { left, right, .. } => {
                assert!(matches!(*right, CExpr::Lit(Value::Int(1))));
                match *left {
                    CExpr::Binary { left, right, .. } => {
                        assert!(matches!(*left, CExpr::Col(0)));
                        assert!(matches!(*right, CExpr::Lit(Value::Int(2))));
                    }
                    _ => panic!("inner product not compiled"),
                }
            }
            _ => panic!("sum not compiled"),
        }
    }
}
