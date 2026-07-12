//! Statement pre-pass that resolves the session-state niladic functions
//! `LAST_INSERT_ID()`, `ROW_COUNT()` and `FOUND_ROWS()` to literals.
//!
//! These depend on per-connection state (the last auto-generated id and the
//! rows changed by the previous statement), which the stateless row evaluator
//! cannot see. Substituting them for literals *before* execution -- like the
//! `ai_embed` pre-pass -- keeps evaluation stateless and works wherever the
//! function appears (projection, `WHERE`, `VALUES`, `SET`, `ORDER BY`).

use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value as SqlValue};

/// Rewrite session-state functions in `stmt` to literal values.
pub fn rewrite(stmt: &mut Statement, last_insert_id: i64, row_count: i64) {
    let ctx = Ctx {
        last_insert_id,
        row_count,
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

struct Ctx {
    last_insert_id: i64,
    row_count: i64,
}

impl Ctx {
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
        let v = match name.as_str() {
            "last_insert_id" => self.last_insert_id,
            "row_count" => self.row_count,
            "found_rows" => self.row_count.max(0),
            _ => return None,
        };
        Some(Expr::Value(SqlValue::Number(v.to_string(), false)))
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
