//! MySQL's `DIV`, `MOD` and `XOR` operators, parsed with their real
//! precedence.
//!
//! sqlparser 0.53's MySQL dialect parses `a DIV b` by reading the whole rest of
//! the expression as `b` (`parse_expr`, the lowest precedence) and unwrapping
//! the result. Two things followed:
//!
//! - `7 DIV 2 + 1` was `7 DIV (2 + 1)`, and `a DIV b = 3` was `a DIV (b = 3)`:
//!   silently wrong answers wherever `DIV` met another operator;
//! - a rest it cannot parse (`SELECT 1 DIV 2 << 1`) panicked inside the parser,
//!   and with `panic = "abort"` that ended the server for every client.
//!
//! The binary `MOD` operator was not parsed at all, and sqlparser gives the
//! logical `XOR` a precedence above comparisons, where MySQL puts it between
//! `AND` and `OR`: `a = 1 XOR b = 2` would compare `a` with `1 XOR b`.
//!
//! Before parsing, each `DIV` keyword is rewritten to `/ __elyra_div__ /`.
//! `/` has the precedence and left associativity MySQL gives `DIV`, so the
//! parser builds the right tree, `(a / __elyra_div__) / b`, which [`restore`]
//! turns back into `a DIV b`. The dialect's own `DIV` parsing is never reached.
//! `MOD` becomes `%`, the same operator.
//!
//! `XOR` becomes `OR __elyra_xor__ AND`: the `AND` keeps everything that binds
//! more tightly than `XOR` on its right, and the `OR` everything on its left.
//! [`restore`] turns `L OR (__elyra_xor__ AND R)` into `L XOR R`; where the
//! `OR` has taken a plain `OR` on its left (`a OR b XOR c`), it rotates, since
//! `XOR` binds more tightly than `OR`: `a OR (b XOR c)`.

use std::ops::ControlFlow;

use sqlparser::ast::{BinaryOperator, Expr, Statement, VisitMut, VisitorMut};
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser, ParserError};

/// The placeholder `DIV` is rewritten around. Not a name a statement can use
/// by accident; if one survives a `restore`, it fails as an unknown column
/// rather than computing anything.
const MARK: &str = "__elyra_div__";

/// The placeholder `XOR` is rewritten around (see [`MARK`]).
const XOR_MARK: &str = "__elyra_xor__";

/// Parse `sql` with `DIV` and `MOD` at their MySQL precedence. Use this, not
/// `Parser::parse_sql`, for any SQL that may contain them.
pub(crate) fn parse_sql(dialect: &dyn Dialect, sql: &str) -> Result<Vec<Statement>, ParserError> {
    let rewritten = rewrite(sql);
    let mut statements = Parser::parse_sql(dialect, rewritten.as_deref().unwrap_or(sql))?;
    if rewritten.is_some() {
        restore(&mut statements);
    }
    Ok(statements)
}

/// `sql` with each `DIV` / `MOD` / `XOR` operator rewritten (see the module docs), or
/// `None` if it has neither. Literals, quoted identifiers and comments are left
/// alone, as is a word that is part of a longer one or follows a `.`
/// (`t.div`), and `MOD(` -- the function.
pub(crate) fn rewrite(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut edits: Vec<(usize, usize, &str)> = Vec::new();
    for code in crate::sqllex::code_bytes(sql) {
        let i = code.index;
        if !matches!(code.byte, b'd' | b'D' | b'm' | b'M' | b'x' | b'X') {
            continue;
        }
        if edits.last().is_some_and(|(start, len, _)| i < start + len) {
            continue;
        }
        let after_dot = bytes[..i]
            .iter()
            .rev()
            .find(|b| !b.is_ascii_whitespace())
            .is_some_and(|&b| b == b'.');
        if after_dot {
            continue;
        }
        if crate::sqllex::keyword_at(sql, i, "DIV") {
            edits.push((i, 3, "/ __elyra_div__ /"));
        } else if crate::sqllex::keyword_at(sql, i, "XOR") {
            edits.push((i, 3, "OR __elyra_xor__ AND"));
        } else if crate::sqllex::keyword_at(sql, i, "MOD") {
            let next = bytes[i + 3..].iter().find(|b| !b.is_ascii_whitespace());
            if next != Some(&b'(') {
                edits.push((i, 3, "%"));
            }
        }
    }
    if edits.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(sql.len() + edits.len() * 16);
    let mut at = 0;
    for (start, len, with) in edits {
        out.push_str(&sql[at..start]);
        out.push(' ');
        out.push_str(with);
        out.push(' ');
        at = start + len;
    }
    out.push_str(&sql[at..]);
    Some(out)
}

/// Turn every `(a / __elyra_div__) / b` that [`rewrite`] produced back into
/// `a DIV b`, and every `L OR (__elyra_xor__ AND R)` into `L XOR R`.
pub(crate) fn restore<V: VisitMut>(node: &mut V) {
    let _ = node.visit(&mut Restore);
}

struct Restore;

impl VisitorMut for Restore {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = expr
        {
            if let Some(rest) = strip_xor_mark(right) {
                let left =
                    std::mem::replace(left.as_mut(), Expr::Value(sqlparser::ast::Value::Null));
                *expr = xor(left, rest);
                return ControlFlow::Continue(());
            }
        }
        let Expr::BinaryOp {
            left,
            op: BinaryOperator::Divide,
            right,
        } = expr
        else {
            return ControlFlow::Continue(());
        };
        let Expr::BinaryOp {
            left: dividend,
            op: BinaryOperator::Divide,
            right: mark,
        } = left.as_mut()
        else {
            return ControlFlow::Continue(());
        };
        if !matches!(mark.as_ref(), Expr::Identifier(id) if id.value == MARK && id.quote_style.is_none())
        {
            return ControlFlow::Continue(());
        }
        let dividend =
            std::mem::replace(dividend.as_mut(), Expr::Value(sqlparser::ast::Value::Null));
        let divisor = std::mem::replace(right.as_mut(), Expr::Value(sqlparser::ast::Value::Null));
        *expr = Expr::BinaryOp {
            left: Box::new(dividend),
            op: BinaryOperator::MyIntegerDivide,
            right: Box::new(divisor),
        };
        ControlFlow::Continue(())
    }
}

fn is_mark(e: &Expr, mark: &str) -> bool {
    matches!(e, Expr::Identifier(id) if id.value == mark && id.quote_style.is_none())
}

/// `R` from `__elyra_xor__ AND R`, the mark being the leftmost operand of an
/// `AND` chain (`__elyra_xor__ AND c AND d` is `(mark AND c) AND d`).
fn strip_xor_mark(e: &Expr) -> Option<Expr> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = e
    else {
        return None;
    };
    if is_mark(left, XOR_MARK) {
        return Some((**right).clone());
    }
    strip_xor_mark(left).map(|left| Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: right.clone(),
    })
}

/// `left XOR right`, where `left` may be a plain `OR` the parser grouped
/// with: `XOR` binds more tightly, so it takes only that `OR`'s right operand.
fn xor(left: Expr, right: Expr) -> Expr {
    match left {
        Expr::BinaryOp {
            left: a,
            op: BinaryOperator::Or,
            right: b,
        } => Expr::BinaryOp {
            left: a,
            op: BinaryOperator::Or,
            right: Box::new(xor(*b, right)),
        },
        left => Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Xor,
            right: Box::new(right),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::MySqlDialect;

    fn select(sql: &str) -> String {
        parse_sql(&MySqlDialect {}, &format!("SELECT {sql}"))
            .unwrap()
            .remove(0)
            .to_string()
            .trim_start_matches("SELECT ")
            .to_string()
    }

    fn tree(sql: &str) -> Expr {
        match parse_sql(&MySqlDialect {}, &format!("SELECT {sql}"))
            .unwrap()
            .remove(0)
        {
            Statement::Query(q) => match *q.body {
                sqlparser::ast::SetExpr::Select(s) => match s.projection.into_iter().next() {
                    Some(sqlparser::ast::SelectItem::UnnamedExpr(e)) => e,
                    other => panic!("{other:?}"),
                },
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
    }

    fn op(e: &Expr) -> &BinaryOperator {
        match e {
            Expr::BinaryOp { op, .. } => op,
            other => panic!("not a binary operation: {other}"),
        }
    }

    #[test]
    fn div_binds_like_multiplication() {
        // `+` and `=` bind more loosely, so they are on top.
        assert_eq!(op(&tree("7 DIV 2 + 1")), &BinaryOperator::Plus);
        assert_eq!(op(&tree("7 DIV 2 = 3")), &BinaryOperator::Eq);
        assert_eq!(op(&tree("1 + 7 DIV 2")), &BinaryOperator::Plus);
        // Same level: left to right.
        assert_eq!(op(&tree("7 DIV 2 * 2")), &BinaryOperator::Multiply);
        assert_eq!(op(&tree("8 * 7 DIV 2")), &BinaryOperator::MyIntegerDivide);
        assert_eq!(op(&tree("7 DIV 2 DIV 2")), &BinaryOperator::MyIntegerDivide);
        // The round trip keeps the meaning: a nested division keeps its
        // parentheses, and `DIV` prints as `DIV`.
        assert_eq!(select("7 DIV (2 + 1)"), "7 DIV (2 + 1)");
        assert_eq!(select("a DIV b + 1"), "a DIV b + 1");
        assert_eq!(select("x MOD 4 = 3"), "x % 4 = 3");
    }

    #[test]
    fn xor_binds_between_and_and_or() {
        // Comparisons and AND bind more tightly; OR more loosely.
        assert_eq!(select("a = 1 XOR b = 2"), "a = 1 XOR b = 2");
        assert_eq!(op(&tree("a = 1 XOR b = 2")), &BinaryOperator::Xor);
        assert_eq!(op(&tree("a AND b XOR c AND d")), &BinaryOperator::Xor);
        assert_eq!(op(&tree("a OR b XOR c")), &BinaryOperator::Or);
        assert_eq!(op(&tree("a XOR b OR c")), &BinaryOperator::Or);
        assert_eq!(op(&tree("NOT a XOR b")), &BinaryOperator::Xor);
        // Left to right among themselves.
        let Expr::BinaryOp { left, .. } = tree("a XOR b XOR c") else {
            unreachable!()
        };
        assert_eq!(op(&left), &BinaryOperator::Xor);
        // `a OR b XOR c OR d` is `(a OR (b XOR c)) OR d`.
        assert_eq!(select("a OR b XOR c OR d"), "a OR b XOR c OR d",);
        let Expr::BinaryOp { left, .. } = tree("a OR b XOR c OR d") else {
            unreachable!()
        };
        let Expr::BinaryOp { right, .. } = *left else {
            unreachable!()
        };
        assert_eq!(op(&right), &BinaryOperator::Xor);
    }

    #[test]
    fn an_unparsable_rest_is_an_error_not_a_panic() {
        assert!(parse_sql(&MySqlDialect {}, "SELECT 1 DIV").is_err());
        assert!(parse_sql(&MySqlDialect {}, "SELECT 1 DIV 2 <<").is_err());
    }

    #[test]
    fn only_the_operators_are_rewritten() {
        for sql in [
            "SELECT 'a DIV b', `div` FROM t",
            "SELECT t.div, t.mod FROM t",
            "SELECT MOD(7, 4), divisor, modulo FROM t",
            "SELECT 1 -- DIV 2\n",
            "SELECT /* MOD */ 1",
        ] {
            assert_eq!(rewrite(sql), None, "{sql}");
        }
        assert!(rewrite("SELECT a div b, c mod d FROM t").is_some());
    }
}
