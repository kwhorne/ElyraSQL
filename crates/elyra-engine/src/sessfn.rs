//! Statement pre-pass that resolves the session-state niladic functions
//! `LAST_INSERT_ID()`, `ROW_COUNT()`, `FOUND_ROWS()` and `DATABASE()` to literals.
//!
//! These depend on per-connection state (the last auto-generated id and the
//! rows changed by the previous statement), which the stateless row evaluator
//! cannot see. Substituting them for literals *before* execution -- like the
//! `ai_embed` pre-pass -- keeps evaluation stateless and works wherever the
//! function appears (projection, `WHERE`, `VALUES`, `SET`, `ORDER BY`).

use sqlparser::ast::{Expr, Query, SetExpr, Statement, Value as SqlValue};

/// Who a connection is: what `USER()`, `CURRENT_USER()` and `CONNECTION_ID()`
/// answer. Set by the server once the client has authenticated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The account name the client logged in as.
    pub user: String,
    /// The client's address, as MySQL reports it in `USER()`.
    pub host: String,
    /// The connection's id, as `SHOW PROCESSLIST` lists it.
    pub connection_id: u64,
}

impl Default for Identity {
    /// An embedded session, with no client and no login.
    fn default() -> Self {
        Identity {
            user: "root".into(),
            host: "localhost".into(),
            connection_id: 1,
        }
    }
}

/// The account a view, procedure or trigger runs as. Only an administrator can
/// create one, and the definer is not recorded, so this is the account
/// `information_schema` has always reported for them.
pub const DEFINER: &str = "root@%";

/// Resolve the identity functions everywhere in `node` -- every expression, in
/// any clause, subquery, CTE or derived table -- to literals:
///
/// - `USER()`, `SESSION_USER()`, `SYSTEM_USER()`: `user@client-host`, the
///   connection's own, also inside a view or routine;
/// - `CURRENT_USER()` / `CURRENT_USER`: the account, `user@%`; but inside a
///   view, procedure or trigger the definer's (`definer`), as in MySQL;
/// - `CONNECTION_ID()`.
///
/// These used to answer `root@%` and `1` whoever was connected, so a view that
/// filtered rows on `USER()` matched nothing, and an audit column recorded root
/// for every change.
pub fn rewrite_identity<V: sqlparser::ast::VisitMut>(
    node: &mut V,
    id: &Identity,
    definer: Option<&str>,
) {
    use std::ops::ControlFlow;
    let user = format!("{}@{}", id.user, id.host);
    let account = format!("{}@%", id.user);
    let current = definer.unwrap_or(&account);
    let _ = sqlparser::ast::visit_expressions_mut(node, |e| {
        // `CURRENT_USER` without parentheses is the same function in MySQL
        // (a reserved word, never a column); the parser reads it as a name.
        if let Expr::Identifier(ident) = e {
            if ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("current_user") {
                *e = str_lit(current);
            }
            return ControlFlow::<()>::Continue(());
        }
        if let Some(name) = niladic_name(e) {
            let lit = match name.as_str() {
                "user" | "session_user" | "system_user" => Some(str_lit(&user)),
                "current_user" => Some(str_lit(current)),
                "connection_id" => Some(Expr::Value(SqlValue::Number(
                    id.connection_id.to_string(),
                    false,
                ))),
                _ => None,
            };
            if let Some(lit) = lit {
                *e = lit;
            }
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Statements whose expressions run now, so identity functions in them are
/// resolved. DDL is left alone: a view, routine, trigger or column default
/// stores its expressions and evaluates them later, as whoever runs them then.
pub fn resolves_identity(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Query(_)
            | Statement::Insert(_)
            | Statement::Update { .. }
            | Statement::Delete(_)
            | Statement::SetVariable { .. }
            | Statement::Call(_)
    )
}

/// The lowercased name of a call with no arguments (`f()`, or a bare keyword
/// such as `CURRENT_USER`), or `None` for anything else.
fn niladic_name(e: &Expr) -> Option<String> {
    let Expr::Function(f) = e else { return None };
    let empty = matches!(&f.args, sqlparser::ast::FunctionArguments::None)
        || matches!(
            &f.args,
            sqlparser::ast::FunctionArguments::List(l) if l.args.is_empty()
        );
    if !empty {
        return None;
    }
    Some(f.name.0.last()?.value.to_ascii_lowercase())
}

/// Rewrite session-state functions in `stmt` to literal values.
pub fn rewrite(
    stmt: &mut Statement,
    last_insert_id: i64,
    row_count: i64,
    database: &str,
    now_micros: i64,
    tz_offset_minutes: i32,
) {
    let ctx = Ctx {
        last_insert_id,
        row_count,
        database,
        now_micros,
        tz_offset_minutes,
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
    /// The session offset from UTC in minutes. The *local* now-family
    /// (`NOW()`, `CURDATE()`, `CURTIME()`) is shifted by it; the `UTC_*` forms
    /// are not. `UNIX_TIMESTAMP(x)` / `FROM_UNIXTIME(n)` are wrapped in
    /// `CONVERT_TZ` when it is non-zero so their argument or result is read in
    /// the session zone, matching MySQL.
    tz_offset_minutes: i32,
}

impl Ctx<'_> {
    /// The captured instant shifted into the session zone, for the *local*
    /// now-family. `UTC_*` reads `now_micros` directly instead.
    fn local_micros(&self) -> i64 {
        self.now_micros + self.tz_offset_minutes as i64 * 60_000_000
    }

    /// If `e` is one of the *interpreting* temporal functions, return it
    /// rewritten to honour the session offset; otherwise `None`. Called only
    /// when the offset is non-zero, after the arguments have been resolved.
    ///
    /// The stateless evaluator reads and writes these in UTC, so CONVERT_TZ
    /// bridges the session zone:
    /// - `FROM_UNIXTIME(n)` -> `CONVERT_TZ(FROM_UNIXTIME(n), '+00:00', <off>)`
    ///   (epoch seconds are absolute; the *result* is shown in the session zone).
    /// - `FROM_UNIXTIME(n, fmt)` -> `DATE_FORMAT(CONVERT_TZ(...), fmt)`.
    /// - `UNIX_TIMESTAMP(x)` -> `UNIX_TIMESTAMP(CONVERT_TZ(x, <off>, '+00:00'))`
    ///   (the argument is a session-zone datetime; convert it back to UTC before
    ///   the evaluator reads it as one).
    fn tz_wrap(&self, e: &Expr) -> Option<Expr> {
        let Expr::Function(f) = e else { return None };
        let name = f.name.0.last()?.value.to_ascii_lowercase();
        let args = fn_call_args(f)?;
        let off = render_offset(self.tz_offset_minutes);
        match name.as_str() {
            "from_unixtime" => match args.len() {
                1 => {
                    let inner = call_expr("from_unixtime", args);
                    Some(call_expr(
                        "convert_tz",
                        vec![inner, str_lit("+00:00"), str_lit(&off)],
                    ))
                }
                2 => {
                    let mut it = args.into_iter();
                    let n = it.next()?;
                    let fmt = it.next()?;
                    let local = call_expr(
                        "convert_tz",
                        vec![
                            call_expr("from_unixtime", vec![n]),
                            str_lit("+00:00"),
                            str_lit(&off),
                        ],
                    );
                    Some(call_expr("date_format", vec![local, fmt]))
                }
                _ => None,
            },
            "unix_timestamp" if args.len() == 1 => {
                let x = args.into_iter().next()?;
                let utc = call_expr("convert_tz", vec![x, str_lit(&off), str_lit("+00:00")]);
                Some(call_expr("unix_timestamp", vec![utc]))
            }
            _ => None,
        }
    }

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
            // type rather than degrading to a string. The *local* forms carry
            // the session offset; the `UTC_*` forms stay in UTC. SYSDATE() is
            // deliberately absent: MySQL leaves it live, so it falls through to
            // the evaluator.
            "now" | "current_timestamp" | "localtime" | "localtimestamp" => frozen_temporal(
                elyra_core::datetime::format_datetime(self.local_micros()),
                datetime_type(),
            ),
            "utc_timestamp" => frozen_temporal(
                elyra_core::datetime::format_datetime(self.now_micros),
                datetime_type(),
            ),
            "curdate" | "current_date" => {
                let days = self.local_micros().div_euclid(86_400_000_000) as i32;
                frozen_temporal(
                    elyra_core::datetime::format_date(days),
                    sqlparser::ast::DataType::Date,
                )
            }
            "utc_date" => {
                let days = self.now_micros.div_euclid(86_400_000_000) as i32;
                frozen_temporal(
                    elyra_core::datetime::format_date(days),
                    sqlparser::ast::DataType::Date,
                )
            }
            "curtime" | "current_time" => {
                let tod = self.local_micros().rem_euclid(86_400_000_000);
                frozen_temporal(
                    elyra_core::datetime::format_time(tod),
                    sqlparser::ast::DataType::Time(None, sqlparser::ast::TimezoneInfo::None),
                )
            }
            "utc_time" => {
                let tod = self.now_micros.rem_euclid(86_400_000_000);
                frozen_temporal(
                    elyra_core::datetime::format_time(tod),
                    sqlparser::ast::DataType::Time(None, sqlparser::ast::TimezoneInfo::None),
                )
            }
            // UNIX_TIMESTAMP() with no argument is the frozen epoch seconds --
            // an absolute instant, independent of the session zone. The
            // one-argument form interprets its value and is left to the
            // evaluator (wrapped in CONVERT_TZ by `tz_wrap` when an offset is
            // set).
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
                // With a non-UTC session offset, the interpreting temporal
                // functions read their argument (or produce their result) in
                // the session zone. The stateless evaluator only knows UTC, so
                // wrap them in CONVERT_TZ here where the offset is known. Args
                // are resolved above first, so any nested session function is
                // already a literal by now.
                if self.tz_offset_minutes != 0 {
                    if let Some(wrapped) = self.tz_wrap(e) {
                        *e = wrapped;
                    }
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

/// The unnamed-expression arguments of a function call, cloned, or `None` if
/// any argument is not a plain positional expression (so we leave it alone).
fn fn_call_args(f: &sqlparser::ast::Function) -> Option<Vec<Expr>> {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    let FunctionArguments::List(list) = &f.args else {
        return None;
    };
    let mut out = Vec::with_capacity(list.args.len());
    for a in &list.args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => out.push(x.clone()),
            _ => return None,
        }
    }
    Some(out)
}

/// Build a bare `name(args...)` function-call expression.
fn call_expr(name: &str, args: Vec<Expr>) -> Expr {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };
    Expr::Function(Function {
        name: ObjectName(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// A single-quoted string literal expression.
fn str_lit(s: &str) -> Expr {
    Expr::Value(SqlValue::SingleQuotedString(s.to_string()))
}

/// Render an offset in minutes as MySQL's `[+-]HH:MM` spelling.
fn render_offset(minutes: i32) -> String {
    let sign = if minutes < 0 { '-' } else { '+' };
    let m = minutes.abs();
    format!("{sign}{:02}:{:02}", m / 60, m % 60)
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
        rewritten_tz(sql, now, 0)
    }

    fn rewritten_tz(sql: &str, now: i64, tz_offset_minutes: i32) -> String {
        let mut stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
        rewrite(&mut stmt, 0, 0, "elyra", now, tz_offset_minutes);
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

    /// A non-UTC offset shifts the *local* now-family by the offset but leaves
    /// the `UTC_*` forms in UTC -- matching MySQL, where `NOW()` follows the
    /// session zone and `UTC_TIMESTAMP()` does not.
    #[test]
    fn local_now_family_carries_the_offset_utc_forms_do_not() {
        // 2024-01-01 12:00:00 UTC; +02:00 -> local 14:00:00, same date.
        const NOW: i64 = 1_704_110_400_000_000;
        let out = rewritten_tz(
            "SELECT NOW(), CURDATE(), CURTIME(), UTC_TIMESTAMP(), UTC_DATE(), UTC_TIME()",
            NOW,
            120,
        );
        assert!(
            out.contains("CAST('2024-01-01 14:00:00' AS DATETIME)"),
            "{out}"
        );
        assert!(out.contains("CAST('2024-01-01' AS DATE)"), "{out}");
        assert!(out.contains("CAST('14:00:00' AS TIME)"), "{out}");
        // UTC forms stay at 12:00.
        assert!(
            out.contains("CAST('2024-01-01 12:00:00' AS DATETIME)"),
            "{out}"
        );
        assert!(out.contains("CAST('12:00:00' AS TIME)"), "{out}");
    }

    /// A negative offset can roll the local date and time-of-day back a day.
    #[test]
    fn a_negative_offset_can_cross_midnight() {
        // 2024-01-01 01:00:00 UTC; -05:30 -> 2023-12-31 19:30:00 local.
        const NOW: i64 = 1_704_070_800_000_000;
        let out = rewritten_tz("SELECT NOW(), CURDATE(), CURTIME()", NOW, -330);
        assert!(
            out.contains("CAST('2023-12-31 19:30:00' AS DATETIME)"),
            "{out}"
        );
        assert!(out.contains("CAST('2023-12-31' AS DATE)"), "{out}");
        assert!(out.contains("CAST('19:30:00' AS TIME)"), "{out}");
    }

    /// UNIX_TIMESTAMP() with no argument is an absolute instant: the offset does
    /// not move it (only its display would differ, and it has none).
    #[test]
    fn niladic_unix_timestamp_ignores_the_offset() {
        const NOW: i64 = 1_704_110_400_000_000;
        assert!(rewritten_tz("SELECT UNIX_TIMESTAMP()", NOW, 120).contains("1704110400"));
    }

    /// With an offset, FROM_UNIXTIME/UNIX_TIMESTAMP(arg) are wrapped in
    /// CONVERT_TZ so the evaluator (which only knows UTC) yields session-zone
    /// results. With no offset they are left untouched.
    #[test]
    fn interpreting_temporal_functions_are_wrapped_only_under_an_offset() {
        const NOW: i64 = 0;
        let fu = rewritten_tz("SELECT FROM_UNIXTIME(0)", NOW, 120);
        assert!(
            fu.contains("convert_tz(from_unixtime(0), '+00:00', '+02:00')"),
            "{fu}"
        );
        let uts = rewritten_tz("SELECT UNIX_TIMESTAMP('1970-01-01 02:00:00')", NOW, 120);
        assert!(
            uts.contains("unix_timestamp(convert_tz('1970-01-01 02:00:00', '+02:00', '+00:00'))"),
            "{uts}"
        );
        // Two-argument FROM_UNIXTIME formats the local datetime.
        let fmt = rewritten_tz("SELECT FROM_UNIXTIME(0, '%Y')", NOW, 120);
        assert!(
            fmt.contains("date_format(convert_tz(from_unixtime(0), '+00:00', '+02:00'), '%Y')"),
            "{fmt}"
        );
        // No offset -> no wrapping.
        let plain = rewritten_tz("SELECT FROM_UNIXTIME(0), UNIX_TIMESTAMP('x')", NOW, 0);
        assert!(!plain.to_uppercase().contains("CONVERT_TZ"), "{plain}");
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

#[cfg(test)]
mod identity_tests {
    use super::{resolves_identity, rewrite_identity, Identity, DEFINER};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    fn alice() -> Identity {
        Identity {
            user: "alice".into(),
            host: "10.0.0.7".into(),
            connection_id: 42,
        }
    }

    fn resolved(sql: &str, definer: Option<&str>) -> String {
        let mut stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
        assert!(resolves_identity(&stmt), "{sql}");
        rewrite_identity(&mut stmt, &alice(), definer);
        stmt.to_string()
    }

    /// Every clause and nesting a query can put an expression in is reached,
    /// not only the projection and `WHERE` of the outermost `SELECT`.
    #[test]
    fn identity_functions_are_resolved_wherever_they_appear() {
        for sql in [
            "SELECT USER()",
            "SELECT 1 FROM t WHERE o = USER()",
            "SELECT 1 FROM t JOIN u ON u.o = USER()",
            "SELECT o FROM t GROUP BY o HAVING o = USER()",
            "SELECT 1 UNION ALL SELECT USER()",
            "WITH me AS (SELECT USER() AS u) SELECT u FROM me",
            "SELECT u FROM (SELECT USER() AS u) x",
            "SELECT (SELECT USER())",
            "DELETE FROM t WHERE o = USER()",
            "UPDATE t SET o = USER()",
            "INSERT INTO t SELECT USER()",
            "SET @u = USER()",
        ] {
            let out = resolved(sql, None);
            assert!(out.contains("'alice@10.0.0.7'"), "{sql} -> {out}");
            assert!(!out.to_uppercase().contains("USER()"), "{sql} -> {out}");
        }
    }

    #[test]
    fn each_function_answers_as_mysql_does() {
        let out = resolved(
            "SELECT USER(), SESSION_USER(), SYSTEM_USER(), CURRENT_USER(), CURRENT_USER, CONNECTION_ID()",
            None,
        );
        assert_eq!(
            out,
            "SELECT 'alice@10.0.0.7', 'alice@10.0.0.7', 'alice@10.0.0.7', 'alice@%', 'alice@%', 42"
        );
    }

    /// Inside a view, procedure or trigger `CURRENT_USER()` is the definer's
    /// account; `USER()` is still the connection's.
    #[test]
    fn current_user_is_the_definer_inside_a_definer_context() {
        let out = resolved("SELECT USER(), CURRENT_USER()", Some(DEFINER));
        assert_eq!(out, "SELECT 'alice@10.0.0.7', 'root@%'");
    }

    /// A column called `user` is a column, and `USER(x)` is not the niladic
    /// function.
    #[test]
    fn a_column_named_user_is_left_alone() {
        assert_eq!(resolved("SELECT user FROM t", None), "SELECT user FROM t");
        assert_eq!(
            resolved("SELECT `current_user` FROM t", None),
            "SELECT `current_user` FROM t"
        );
    }

    /// DDL stores its expressions and evaluates them later, as whoever runs
    /// them then: resolving them now would freeze today's user into a view,
    /// a routine or a column default.
    #[test]
    fn ddl_keeps_its_identity_functions() {
        for sql in [
            "CREATE VIEW v AS SELECT * FROM t WHERE o = USER()",
            "CREATE TABLE t (who VARCHAR(100) DEFAULT (CURRENT_USER()))",
        ] {
            let stmt = Parser::parse_sql(&MySqlDialect {}, sql).unwrap().remove(0);
            assert!(!resolves_identity(&stmt), "{sql}");
        }
    }
}
