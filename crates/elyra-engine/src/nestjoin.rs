//! Parenthesised joins in `FROM`: `FROM (a JOIN b ON x) JOIN c ON y`.
//!
//! MySQL stores every view with joins in this form -- `from ((a join b
//! on(...)) join c on(...))` -- and mysqldump writes it back, so a restored view
//! with a join failed to be created ("unknown column a.x"): the executor has no
//! nested-join operator. Ordinary queries written that way failed the same.
//!
//! Where it does not change the result, the nesting is taken out before the
//! statement runs:
//!
//! - a parenthesised join on the **left** of a join list, or alone in `FROM`
//!   (after a comma too): `(a J1 b) J2 c` is `a J1 b J2 c` for every join type,
//!   since joins are evaluated left to right;
//! - one on the **right** of an inner join, when it holds only inner joins:
//!   `a JOIN (b JOIN c ON y) ON x` is `a CROSS JOIN b JOIN c ON (y) AND (x)`.
//!
//! An outer join inside a parenthesised right operand is left as it is: moving
//! it would change which rows are padded with NULLs.

use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Query, SetExpr, TableFactor,
    TableWithJoins, VisitMut, VisitorMut,
};

/// Flatten the parenthesised joins in every `FROM` of `node`, subqueries and
/// CTEs included.
pub fn flatten<V: VisitMut>(node: &mut V) {
    let _ = node.visit(&mut Flatten);
}

struct Flatten;

impl VisitorMut for Flatten {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<()> {
        flatten_set_expr(&mut query.body);
        ControlFlow::Continue(())
    }
}

fn flatten_set_expr(body: &mut SetExpr) {
    match body {
        SetExpr::Select(select) => {
            for from in &mut select.from {
                flatten_from(from);
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            flatten_set_expr(left);
            flatten_set_expr(right);
        }
        SetExpr::Query(query) => flatten_set_expr(&mut query.body),
        _ => {}
    }
}

/// The inner join list of an unaliased parenthesised join, cloned.
fn nested(factor: &TableFactor) -> Option<TableWithJoins> {
    match factor {
        TableFactor::NestedJoin {
            table_with_joins,
            alias: None,
        } => Some((**table_with_joins).clone()),
        _ => None,
    }
}

fn flatten_from(from: &mut TableWithJoins) {
    // On the left: `(a J1 b) J2 c` -> `a J1 b J2 c`.
    while let Some(mut inner) = nested(&from.relation) {
        flatten_from(&mut inner);
        inner.joins.append(&mut from.joins);
        *from = inner;
    }
    // On the right of an inner join, holding only inner joins.
    let mut joins = Vec::with_capacity(from.joins.len());
    for join in std::mem::take(&mut from.joins) {
        let Some(mut inner) = nested(&join.relation) else {
            joins.push(join);
            continue;
        };
        flatten_from(&mut inner);
        if inner.joins.is_empty() {
            // `a JOIN (b) ON x`: only parentheses.
            joins.push(Join {
                relation: inner.relation,
                ..join
            });
            continue;
        }
        let outer_on = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(on)) => Some(on.clone()),
            JoinOperator::CrossJoin => None,
            _ => {
                joins.push(join);
                continue;
            }
        };
        let all_inner = inner.joins.iter().all(|j| {
            matches!(
                j.join_operator,
                JoinOperator::Inner(JoinConstraint::On(_)) | JoinOperator::CrossJoin
            )
        });
        if !all_inner {
            joins.push(join);
            continue;
        }
        joins.push(Join {
            relation: inner.relation,
            global: false,
            join_operator: JoinOperator::CrossJoin,
        });
        joins.append(&mut inner.joins);
        if let Some(on) = outer_on {
            let last = joins.last_mut().expect("just pushed");
            last.join_operator =
                match std::mem::replace(&mut last.join_operator, JoinOperator::CrossJoin) {
                    JoinOperator::Inner(JoinConstraint::On(inner_on)) => {
                        JoinOperator::Inner(JoinConstraint::On(and(inner_on, on)))
                    }
                    _ => JoinOperator::Inner(JoinConstraint::On(on)),
                };
        }
    }
    from.joins = joins;
}

fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Nested(Box::new(left))),
        op: BinaryOperator::And,
        right: Box::new(Expr::Nested(Box::new(right))),
    }
}

#[cfg(test)]
mod tests {
    use super::flatten;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    fn flat(sql: &str) -> String {
        let mut stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
        flatten(&mut stmt);
        stmt.to_string()
    }

    #[test]
    fn left_nesting_is_taken_out_for_every_join_type() {
        assert_eq!(
            flat("SELECT 1 FROM (a JOIN b ON b.x = a.x)"),
            "SELECT 1 FROM a JOIN b ON b.x = a.x"
        );
        assert_eq!(
            flat("SELECT 1 FROM ((a LEFT JOIN b ON (b.x = a.x)) JOIN c ON (c.y = a.y))"),
            "SELECT 1 FROM a LEFT JOIN b ON (b.x = a.x) JOIN c ON (c.y = a.y)"
        );
        assert_eq!(
            flat("SELECT 1 FROM a, (b JOIN c ON c.y = b.y) WHERE a.x = b.x"),
            "SELECT 1 FROM a, b JOIN c ON c.y = b.y WHERE a.x = b.x"
        );
        // Inside a subquery and a CTE too.
        assert_eq!(
            flat("WITH w AS (SELECT 1 FROM (a JOIN b ON b.x = a.x)) SELECT * FROM (SELECT 1 FROM (a JOIN b ON b.x = a.x)) d"),
            "WITH w AS (SELECT 1 FROM a JOIN b ON b.x = a.x) SELECT * FROM (SELECT 1 FROM a JOIN b ON b.x = a.x) AS d"
        );
    }

    #[test]
    fn right_nesting_of_inner_joins_moves_its_condition() {
        assert_eq!(
            flat("SELECT 1 FROM a JOIN (b JOIN c ON c.y = b.y) ON b.x = a.x"),
            "SELECT 1 FROM a CROSS JOIN b JOIN c ON (c.y = b.y) AND (b.x = a.x)"
        );
    }

    /// Moving an outer join out of a right operand would change which rows are
    /// padded with NULLs.
    #[test]
    fn right_nesting_with_an_outer_join_is_left_alone() {
        for sql in [
            "SELECT 1 FROM a JOIN (b LEFT JOIN c ON c.y = b.y) ON b.x = a.x",
            "SELECT 1 FROM a LEFT JOIN (b JOIN c ON c.y = b.y) ON b.x = a.x",
        ] {
            let mut stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
            let before = stmt.to_string();
            flatten(&mut stmt);
            assert_eq!(stmt.to_string(), before, "{sql}");
        }
    }
}
