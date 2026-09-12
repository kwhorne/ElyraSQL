//! Statement pre-pass that resolves the session-state niladic functions
//! `LAST_INSERT_ID()`, `ROW_COUNT()`, `FOUND_ROWS()` and `DATABASE()` to literals.
//!
//! These depend on per-connection state (the last auto-generated id and the
//! rows changed by the previous statement), which the stateless row evaluator
//! cannot see. Substituting them for literals *before* execution -- like the
//! `ai_embed` pre-pass -- keeps evaluation stateless and works wherever the
//! function appears (projection, `WHERE`, `VALUES`, `SET`, `ORDER BY`).

use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value as SqlValue};

/// Rewrite session-state functions in `stmt` to literal values.
pub fn rewrite(
    stmt: &mut Statement,
    last_insert_id: i64,
    row_count: i64,
    database: &str,
    now_micros: i64,
) {
    let ctx = Ctx {
        last_insert_id,
        row_count,
        database,
        now_micros,
    };
    match stmt {
        Statement::Query(q) => ctx.query(q),
        Statement::Insert(ins) => {
            if let Some(src) = &mut ins.source {
                ctx.query(src);
            }
        }
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for a in assignments {
                ctx.expr(&mut a.value);
            }
            if let Some(w) = selection {
                ctx.expr(w);
            }
        }
        _ => {}
    }
}

struct Ctx<'a> {
    last_insert_id: i64,
    row_count: i64,
    database: &'a str,
    /// Wall-clock micros captured once for this statement, so every NOW()-family
    /// call in it agrees -- MySQL freezes them to statement start.
    now_micros: i64,
}

impl Ctx<'_> {
    /// The literal value for a session function, or `None` if `e` is not one.
    fn literal_for(&self, e: &Expr) -> Option<Expr> {
        let Expr::Function(f) = e else { return None };
        // Only the zero-argument forms are session reads; `LAST_INSERT_ID(x)`
        // (the setter form) is left for the evaluator.
        let empty = matches!(&f.args, sqlparser::ast::FunctionArguments::None)
            || matches!(
                &f.args,
                sqlparser::ast::FunctionArguments::List(l) if l.args.is_empty()
            );
        if !empty {
            return None;
        }
        let name = f.name.0.last()?.value.to_ascii_lowercase();
        let value = match name.as_str() {
            "last_insert_id" => {
                Expr::Value(SqlValue::Number(self.last_insert_id.to_string(), false))
            }
            "row_count" => Expr::Value(SqlValue::Number(self.row_count.to_string(), false)),
            "found_rows" => Expr::Value(SqlValue::Number(self.row_count.max(0).to_string(), false)),
            "database" | "schema" => {
                Expr::Value(SqlValue::SingleQuotedString(self.database.to_string()))
            }
            // The NOW() family is frozen to the statement's captured instant, so
            // two reads agree and an INSERT with several is consistent. Emitted
            // as a typed CAST literal so the result keeps its DATETIME/DATE/TIME
            // type rather than degrading to a string. SYSDATE() is deliberately
            // absent: MySQL leaves it live, so it falls through to the evaluator.
            "now" | "current_timestamp" | "localtime" | "localtimestamp" | "utc_timestamp" => {
                frozen_temporal(
                    elyra_core::datetime::format_datetime(self.now_micros),
                    datetime_type(),
                )
            }
            "curdate" | "current_date" | "utc_date" => {
                let days = self.now_micros.div_euclid(86_400_000_000) as i32;
                frozen_temporal(
                    elyra_core::datetime::format_date(days),
                    sqlparser::ast::DataType::Date,
                )
            }
            "curtime" | "current_time" | "utc_time" => {
                let tod = self.now_micros.rem_euclid(86_400_000_000);
                frozen_temporal(
                    elyra_core::datetime::format_time(tod),
                    sqlparser::ast::DataType::Time(None, sqlparser::ast::TimezoneInfo::None),
                )
            }
            // UNIX_TIMESTAMP() with no argument is the frozen epoch seconds; the
            // one-argument form interprets its value and is left to the evaluator.
            "unix_timestamp" => Expr::Value(SqlValue::Number(
                (self.now_micros / 1_000_000).to_string(),
                false,
            )),
            _ => return None,
        };
        Some(value)
    }

    fn expr(&self, e: &mut Expr) {
        if let Some(lit) = self.literal_for(e) {
            *e = lit;
            return;
        }
        match e {
            Expr::Function(f) => {
                for x in crate::aiembed::fn_arg_exprs_mut(f) {
                    self.expr(x);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr(left);
                self.expr(right);
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::Cast { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr) => self.expr(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr(expr);
                self.expr(low);
                self.expr(high);
            }
            Expr::InList { expr, list, .. } => {
                self.expr(expr);
                for x in list {
                    self.expr(x);
                }
            }
            Expr::Subquery(query)
            | Expr::Exists {
                subquery: query, ..
            } => self.query(query),
            Expr::InSubquery { expr, subquery, .. } => {
                self.expr(expr);
                self.query(subquery);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(o) = operand {
                    self.expr(o);
                }
                for c in conditions {
                    self.expr(c);
                }
                for r in results {
                    self.expr(r);
                }
                if let Some(er) = else_result {
                    self.expr(er);
                }
            }
            _ => {}
        }
    }

    fn query(&self, q: &mut Query) {
        if let SetExpr::Select(sel) = q.body.as_mut() {
            for item in &mut sel.projection {
                use sqlparser::ast::SelectItem;
                match item {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                        self.expr(e)
                    }
                    _ => {}
                }
            }
            if let Some(w) = &mut sel.selection {
                self.expr(w);
            }
        }
        if let SetExpr::Values(v) = q.body.as_mut() {
            for row in &mut v.rows {
                for e in row {
                    self.expr(e);
                }
            }
        }
        if let Some(ob) = &mut q.order_by {
            for o in &mut ob.exprs {
                self.expr(&mut o.expr);
            }
        }
    }
}

/// `DATETIME` target type for a frozen NOW() literal.
fn datetime_type() -> sqlparser::ast::DataType {
    sqlparser::ast::DataType::Datetime(None)
}

/// Wrap a rendered temporal string in `CAST(... AS <type>)`, so the evaluator
/// produces a `DateTime`/`Date`/`Time` value (not a string) -- preserving the
/// column type NOW()/CURDATE()/CURTIME() have in MySQL.
fn frozen_temporal(rendered: String, data_type: sqlparser::ast::DataType) -> Expr {
    Expr::Cast {
        kind: sqlparser::ast::CastKind::Cast,
        expr: Box::new(Expr::Value(SqlValue::SingleQuotedString(rendered))),
        data_type,
        format: None,
    }
}

#[cfg(test)]
mod frozen_now_tests {
    use super::rewrite;
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    fn rewritten(sql: &str, now: i64) -> String {
        let mut stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
        rewrite(&mut stmt, 0, 0, "elyra", now);
        stmt.to_string()
    }

    /// The NOW() family is replaced by a typed CAST literal built from the one
    /// captured instant, so every call in the statement renders the same time
    /// with its DATETIME/DATE/TIME type intact. SYSDATE() is left live.
    #[test]
    fn now_family_is_substituted_with_the_captured_instant() {
        // 2024-01-01 12:00:00 UTC.
        const NOW: i64 = 1_704_110_400_000_000;
        let out = rewritten("SELECT NOW(), NOW(), CURDATE(), CURTIME()", NOW);
        // Both NOW() become the same DATETIME cast.
        assert_eq!(
            out.matches("CAST('2024-01-01 12:00:00' AS DATETIME)")
                .count(),
            2,
            "{out}"
        );
        assert!(out.contains("CAST('2024-01-01' AS DATE)"), "{out}");
        assert!(out.contains("CAST('12:00:00' AS TIME)"), "{out}");

        // UNIX_TIMESTAMP() with no arg -> the frozen epoch seconds.
        assert!(rewritten("SELECT UNIX_TIMESTAMP()", NOW).contains("1704110400"));

        // SYSDATE() is NOT substituted (stays live for the evaluator).
        assert!(rewritten("SELECT SYSDATE()", NOW)
            .to_uppercase()
            .contains("SYSDATE"));
        // UNIX_TIMESTAMP(x) (the interpreting form) is left alone too.
        let one_arg = rewritten("SELECT UNIX_TIMESTAMP('2024-01-01 00:00:00')", NOW);
        assert!(
            one_arg.to_uppercase().contains("UNIX_TIMESTAMP("),
            "{one_arg}"
        );
    }

    /// Substitution reaches expressions everywhere, not just bare projections.
    #[test]
    fn substitution_reaches_where_and_insert() {
        const NOW: i64 = 1_704_110_400_000_000;
        assert!(rewritten("SELECT * FROM t WHERE created >= NOW()", NOW)
            .contains("CAST('2024-01-01 12:00:00' AS DATETIME)"));
        assert_eq!(
            rewritten("INSERT INTO t (a, b) VALUES (NOW(), NOW())", NOW)
                .matches("CAST('2024-01-01 12:00:00' AS DATETIME)")
                .count(),
            2
        );
    }
}
