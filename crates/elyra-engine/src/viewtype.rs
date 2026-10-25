//! The type of a view's column, as MySQL derives it: from the declaration of the
//! column it names, or from the expression that computes it -- without reading
//! a row.
//!
//! ElyraSQL types a computed result column by its values, so an empty view had
//! no type to report. `information_schema.COLUMNS` and `SHOW COLUMNS` need the
//! declared one, `decimal(11,2)` for `amt * 2` over a `DECIMAL(10,2)`. The rules
//! here are MySQL 8.4's, measured on it: decimal precision and scale through
//! arithmetic and aggregates, string lengths through string functions, the
//! common type of `COALESCE`/`IF`/`CASE`. An expression they do not cover is
//! `None`, and the caller describes it as `text`.

use elyra_core::Collation;
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments,
    UnaryOperator, Value as SqlValue,
};

use crate::catalog::ColumnDeclaration;

/// One column of a view, as `information_schema.COLUMNS` and `SHOW COLUMNS`
/// describe it.
#[derive(Debug, Clone)]
pub(crate) struct ViewColumn {
    pub name: String,
    pub declaration: ColumnDeclaration,
    pub nullable: bool,
    pub collation: Collation,
    /// A column taken as it is from a table keeps the table's default; a
    /// computed one that cannot be NULL shows its type's implicit default.
    pub default: Option<String>,
    /// For an integer literal, its digits: `0` is an `int` to show, but one
    /// digit to arithmetic (`IF(c, amt, 0)` is `decimal(10,2)`).
    pub literal_digits: Option<u64>,
}

/// What a numeric type means for arithmetic.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Num {
    /// An integer of this many decimal digits; `literal` for a constant, which
    /// MySQL keeps as `int` where a column would widen to `bigint`.
    Int {
        digits: u64,
        literal: bool,
    },
    Decimal {
        precision: u64,
        scale: u64,
    },
    Float,
}

fn decl(
    data_type: &str,
    column_type: String,
    length: Option<u64>,
    precision: Option<u64>,
    scale: Option<u64>,
) -> ColumnDeclaration {
    ColumnDeclaration {
        data_type: data_type.into(),
        column_type,
        character_maximum_length: length,
        numeric_precision: precision,
        numeric_scale: scale,
    }
}

fn column(declaration: ColumnDeclaration, nullable: bool) -> ViewColumn {
    ViewColumn {
        name: String::new(),
        declaration,
        nullable,
        collation: Collation::Ci,
        default: None,
        literal_digits: None,
    }
}

fn int(nullable: bool) -> ViewColumn {
    column(decl("int", "int".into(), None, Some(10), Some(0)), nullable)
}

fn bigint(nullable: bool) -> ViewColumn {
    column(
        decl("bigint", "bigint".into(), None, Some(19), Some(0)),
        nullable,
    )
}

fn bigint_unsigned(nullable: bool) -> ViewColumn {
    column(
        decl("bigint", "bigint unsigned".into(), None, Some(20), Some(0)),
        nullable,
    )
}

fn decimal(precision: u64, scale: u64, nullable: bool) -> ViewColumn {
    let scale = scale.min(30);
    let precision = precision.clamp(scale.max(1), 65);
    column(
        decl(
            "decimal",
            format!("decimal({precision},{scale})"),
            None,
            Some(precision),
            Some(scale),
        ),
        nullable,
    )
}

fn double(nullable: bool) -> ViewColumn {
    column(
        decl("double", "double".into(), None, Some(22), None),
        nullable,
    )
}

/// An aggregate over doubles reports one more digit of precision.
fn aggregate_double() -> ViewColumn {
    column(decl("double", "double".into(), None, Some(23), None), true)
}

fn varchar(length: u64, nullable: bool) -> ViewColumn {
    column(
        decl(
            "varchar",
            format!("varchar({length})"),
            Some(length),
            None,
            None,
        ),
        nullable,
    )
}

fn named(data_type: &str, nullable: bool) -> ViewColumn {
    let length = match data_type {
        "text" => Some(65_535),
        "longtext" => Some(4_294_967_295),
        _ => None,
    };
    column(
        decl(data_type, data_type.into(), length, None, None),
        nullable,
    )
}

fn numeric(c: &ViewColumn) -> Option<Num> {
    if let Some(digits) = c.literal_digits {
        return Some(Num::Int {
            digits,
            literal: true,
        });
    }
    let d = &c.declaration;
    match d.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" | "year" | "bit" => {
            Some(Num::Int {
                digits: d.numeric_precision.unwrap_or(10),
                literal: false,
            })
        }
        "decimal" => Some(Num::Decimal {
            precision: d.numeric_precision.unwrap_or(10),
            scale: d.numeric_scale.unwrap_or(0),
        }),
        "float" | "double" => Some(Num::Float),
        _ => None,
    }
}

fn is_string(c: &ViewColumn) -> bool {
    matches!(
        c.declaration.data_type.as_str(),
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set"
    )
}

/// How many characters a value of this column takes as a string -- what
/// `CONCAT` adds up.
fn char_length(c: &ViewColumn) -> u64 {
    let d = &c.declaration;
    if let Some(length) = d.character_maximum_length {
        return length;
    }
    match d.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" => d.numeric_precision.unwrap_or(10) + 1,
        "bigint" => 20,
        "decimal" => {
            let scale = d.numeric_scale.unwrap_or(0);
            d.numeric_precision.unwrap_or(10) + 1 + u64::from(scale > 0)
        }
        "float" | "double" => 22,
        "date" => 10,
        "datetime" | "timestamp" => 19,
        "time" => 10,
        "year" => 4,
        _ => 65_535,
    }
}

/// The implicit default MySQL shows for a computed column that cannot be NULL.
pub(crate) fn implicit_default(declaration: &ColumnDeclaration) -> Option<String> {
    Some(match declaration.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" | "year" => "0".into(),
        "decimal" => match declaration.numeric_scale.unwrap_or(0) {
            0 => "0".into(),
            scale => format!("0.{}", "0".repeat(scale as usize)),
        },
        "char" | "varchar" => String::new(),
        "date" => "0000-00-00".into(),
        "datetime" | "timestamp" => "0000-00-00 00:00:00".into(),
        "time" => "00:00:00".into(),
        _ => return None,
    })
}

/// The type of a numeric result.
fn of_num(num: Num, nullable: bool) -> ViewColumn {
    match num {
        Num::Int { digits, literal } => {
            if literal || digits <= 9 {
                int(nullable)
            } else {
                bigint(nullable)
            }
        }
        Num::Decimal { precision, scale } => decimal(precision, scale, nullable),
        Num::Float => double(nullable),
    }
}

fn as_decimal(num: Num) -> (u64, u64) {
    match num {
        Num::Int { digits, .. } => (digits, 0),
        Num::Decimal { precision, scale } => (precision, scale),
        Num::Float => (17, 0),
    }
}

/// `a + b`, `a - b`, `a * b`, `a / b`, `a % b`, as MySQL types them.
fn arithmetic(op: &BinaryOperator, a: Num, b: Num, nullable: bool) -> ViewColumn {
    if a == Num::Float || b == Num::Float {
        return double(nullable);
    }
    let (p1, s1) = as_decimal(a);
    let (p2, s2) = as_decimal(b);
    let both_int = matches!(a, Num::Int { .. }) && matches!(b, Num::Int { .. });
    let both_literal = matches!(
        (a, b),
        (
            Num::Int { literal: true, .. },
            Num::Int { literal: true, .. }
        )
    );
    match op {
        BinaryOperator::Divide => {
            let scale = s1 + 4;
            decimal(p1 - s1 + s2 + scale, scale, nullable)
        }
        BinaryOperator::MyIntegerDivide => bigint(nullable),
        _ if both_int => of_num(
            Num::Int {
                digits: match op {
                    BinaryOperator::Multiply => p1 + p2,
                    _ => p1.max(p2) + 1,
                },
                literal: both_literal,
            },
            nullable,
        ),
        BinaryOperator::Multiply => decimal(p1 + p2, s1 + s2, nullable),
        BinaryOperator::Modulo => {
            let scale = s1.max(s2);
            decimal((p1 - s1).max(p2 - s2) + scale, scale, nullable)
        }
        _ => {
            let scale = s1.max(s2);
            decimal((p1 - s1).max(p2 - s2) + scale + 1, scale, nullable)
        }
    }
}

/// The common type of the values `COALESCE`, `IF`, `CASE`, `GREATEST` and the
/// like may return.
fn common(values: &[ViewColumn], nullable: bool) -> Option<ViewColumn> {
    let first = values.first()?;
    // The same type throughout -- but a literal `0` beside an `int` column is
    // not, as the column widens to `bigint` and the literal does not.
    if values.iter().all(|v| {
        v.declaration == first.declaration
            && v.literal_digits.is_some() == first.literal_digits.is_some()
    }) {
        return Some(ViewColumn {
            nullable,
            default: None,
            literal_digits: None,
            ..first.clone()
        });
    }
    if values.iter().any(is_string) || values.iter().any(|v| numeric(v).is_none()) {
        if values.iter().all(|v| is_string(v) || numeric(v).is_some()) {
            let length = values.iter().map(char_length).max()?;
            let collation = values
                .iter()
                .find(|v| is_string(v))
                .map_or(Collation::Ci, |v| v.collation);
            return Some(ViewColumn {
                collation,
                ..varchar(length, nullable)
            });
        }
        return None;
    }
    let nums: Vec<Num> = values.iter().filter_map(numeric).collect();
    if nums.contains(&Num::Float) {
        return Some(double(nullable));
    }
    if nums.iter().all(|n| matches!(n, Num::Int { .. })) {
        let literal = nums
            .iter()
            .all(|n| matches!(n, Num::Int { literal: true, .. }));
        return Some(if literal {
            int(nullable)
        } else {
            bigint(nullable)
        });
    }
    let scale = nums.iter().map(|n| as_decimal(*n).1).max()?;
    let integer = nums
        .iter()
        .map(|n| as_decimal(*n))
        .map(|(p, s)| p - s)
        .max()?;
    Some(decimal(integer + scale, scale, nullable))
}

fn literal(value: &SqlValue) -> Option<ViewColumn> {
    Some(match value {
        SqlValue::Number(n, _) => {
            let digits = n.trim_start_matches('-');
            match digits.split_once('.') {
                Some((whole, fraction)) => {
                    let whole = whole.trim_start_matches('0').len() as u64;
                    let scale = fraction.len() as u64;
                    decimal(whole + scale, scale, false)
                }
                None => ViewColumn {
                    literal_digits: Some(digits.len() as u64),
                    ..if digits.len() > 9 {
                        bigint(false)
                    } else {
                        int(false)
                    }
                },
            }
        }
        SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
            varchar(s.chars().count() as u64, false)
        }
        SqlValue::Boolean(_) => int(false),
        SqlValue::Null => column(
            decl("varbinary", "varbinary(0)".into(), Some(0), None, None),
            true,
        ),
        _ => return None,
    })
}

/// A literal integer argument (`LEFT(name, 3)`).
fn literal_int(e: &Expr) -> Option<u64> {
    match e {
        Expr::Value(SqlValue::Number(n, _)) => n.parse().ok(),
        _ => None,
    }
}

fn function_args(f: &Function) -> Vec<&Expr> {
    match &f.args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|arg| match arg {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e))
                | FunctionArg::Named {
                    arg: FunctionArgExpr::Expr(e),
                    ..
                } => Some(e),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The type of `expr` in a view; `column_of` describes a column it names. A
/// computed column that cannot be NULL shows its type's implicit default, as
/// in MySQL.
pub(crate) fn expr_column(
    expr: &Expr,
    column_of: &dyn Fn(&Expr) -> Option<ViewColumn>,
) -> Option<ViewColumn> {
    let column = typed(expr, column_of)?;
    Some(match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => column,
        _ => with_implicit_default(column),
    })
}

fn typed(expr: &Expr, column_of: &dyn Fn(&Expr) -> Option<ViewColumn>) -> Option<ViewColumn> {
    let ty = |e: &Expr| typed(e, column_of);
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => column_of(expr),
        Expr::Nested(inner) => ty(inner),
        Expr::Value(value) => literal(value),
        Expr::UnaryOp { op, expr: inner } => match op {
            UnaryOperator::Not => Some(int(ty(inner)?.nullable)),
            UnaryOperator::Minus | UnaryOperator::Plus => {
                let operand = ty(inner)?;
                // `-2` stays an `int`; `-id` widens, as any arithmetic does.
                if operand.literal_digits.is_some() || matches!(inner.as_ref(), Expr::Value(_)) {
                    return Some(operand);
                }
                match numeric(&operand)? {
                    Num::Int { .. } => Some(bigint(operand.nullable)),
                    num => Some(of_num(num, operand.nullable)),
                }
            }
            _ => None,
        },
        Expr::BinaryOp { left, op, right } => {
            use BinaryOperator as B;
            match op {
                B::Plus | B::Minus | B::Multiply | B::Divide | B::Modulo | B::MyIntegerDivide => {
                    let (l, r) = (ty(left)?, ty(right)?);
                    let nullable = l.nullable
                        || r.nullable
                        || matches!(op, B::Divide | B::Modulo | B::MyIntegerDivide);
                    Some(arithmetic(op, numeric(&l)?, numeric(&r)?, nullable))
                }
                B::Eq
                | B::NotEq
                | B::Lt
                | B::LtEq
                | B::Gt
                | B::GtEq
                | B::Spaceship
                | B::And
                | B::Or
                | B::Xor => {
                    let nullable =
                        ty(left).is_none_or(|c| c.nullable) || ty(right).is_none_or(|c| c.nullable);
                    Some(int(nullable && !matches!(op, B::Spaceship)))
                }
                B::LongArrow => Some(named("longtext", true)),
                B::Arrow => Some(named("json", true)),
                _ => None,
            }
        }
        Expr::IsNull(_) | Expr::IsNotNull(_) | Expr::IsTrue(_) | Expr::IsFalse(_) => {
            Some(int(false))
        }
        Expr::Like { expr: e, .. }
        | Expr::ILike { expr: e, .. }
        | Expr::InList { expr: e, .. }
        | Expr::Between { expr: e, .. }
        | Expr::RLike { expr: e, .. } => Some(int(ty(e).is_none_or(|c| c.nullable))),
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        }
        | Expr::Convert {
            expr: inner,
            data_type: Some(data_type),
            ..
        } => cast(inner, data_type, column_of),
        Expr::Case {
            results,
            else_result,
            ..
        } => {
            let mut values = results.iter().map(ty).collect::<Option<Vec<_>>>()?;
            match else_result {
                Some(e) => values.push(ty(e)?),
                None => {
                    return common(&values, true);
                }
            }
            let nullable = values.iter().any(|v| v.nullable);
            common(&values, nullable)
        }
        Expr::Substring {
            expr: inner,
            substring_from,
            substring_for,
            ..
        } => {
            let s = ty(inner)?;
            let mut length = char_length(&s);
            if let Some(from) = substring_from.as_deref().and_then(literal_int) {
                length = length.saturating_sub(from.saturating_sub(1));
            }
            if let Some(n) = substring_for.as_deref().and_then(literal_int) {
                length = length.min(n);
            }
            Some(ViewColumn {
                collation: s.collation,
                ..varchar(length, true)
            })
        }
        Expr::Trim { expr: inner, .. } => {
            let s = ty(inner)?;
            Some(ViewColumn {
                collation: s.collation,
                ..varchar(char_length(&s), true)
            })
        }
        Expr::Ceil { expr: inner, .. } | Expr::Floor { expr: inner, .. } => {
            let c = ty(inner)?;
            match numeric(&c)? {
                Num::Float => Some(double(c.nullable)),
                _ => Some(bigint(c.nullable)),
            }
        }
        Expr::Extract { .. } => Some(int(true)),
        Expr::Function(f) => function(f, column_of),
        _ => None,
    }
}

fn with_implicit_default(c: ViewColumn) -> ViewColumn {
    let default = (!c.nullable)
        .then(|| implicit_default(&c.declaration))
        .flatten();
    ViewColumn { default, ..c }
}

fn cast(
    inner: &Expr,
    data_type: &DataType,
    column_of: &dyn Fn(&Expr) -> Option<ViewColumn>,
) -> Option<ViewColumn> {
    let text = data_type.to_string().to_ascii_uppercase();
    let source = typed(inner, column_of);
    // A number from a value that cannot be NULL cannot be NULL; a string or a
    // date can (a value that does not convert).
    let nullable = source.as_ref().is_none_or(|c| c.nullable);
    if text.starts_with("UNSIGNED") {
        return Some(bigint_unsigned(nullable));
    }
    if text.starts_with("SIGNED") {
        return Some(bigint(nullable));
    }
    if text == "CHAR" || text.starts_with("CHAR(") || text.starts_with("NCHAR") {
        let length = match data_type {
            DataType::Char(Some(sqlparser::ast::CharacterLength::IntegerLength {
                length, ..
            })) => *length,
            _ => source.as_ref().map(char_length)?,
        };
        return Some(varchar(length, true));
    }
    let declaration = crate::exec::declaration_from_data_type(data_type).ok()?;
    let number = matches!(
        declaration.data_type.as_str(),
        "tinyint" | "smallint" | "mediumint" | "int" | "bigint" | "decimal" | "float" | "double"
    );
    Some(column(declaration, nullable || !number))
}

fn function(f: &Function, column_of: &dyn Fn(&Expr) -> Option<ViewColumn>) -> Option<ViewColumn> {
    let ty = |e: &Expr| typed(e, column_of);
    let name = f.name.0.last()?.value.to_ascii_lowercase();
    let args = function_args(f);
    let arg = |i: usize| args.get(i).and_then(|e| ty(e));
    let star = matches!(&f.args, FunctionArguments::List(list)
        if list.args.iter().any(|a| matches!(a, FunctionArg::Unnamed(FunctionArgExpr::Wildcard))));
    let string_of = |c: ViewColumn, length: u64| ViewColumn {
        collation: c.collation,
        ..varchar(length, true)
    };
    Some(match name.as_str() {
        // Aggregates.
        "count" => bigint(false),
        "sum" | "avg" if star => return None,
        "sum" => match numeric(&arg(0)?)? {
            Num::Float => aggregate_double(),
            num => {
                let (p, s) = as_decimal(num);
                decimal(p + 22, s, true)
            }
        },
        "avg" => match numeric(&arg(0)?)? {
            Num::Float => aggregate_double(),
            num => {
                let (p, s) = as_decimal(num);
                decimal(p + 4, s + 4, true)
            }
        },
        "min" | "max" => ViewColumn {
            nullable: true,
            default: None,
            ..arg(0)?
        },
        "group_concat" => named("text", true),
        "bit_or" | "bit_and" | "bit_xor" => bigint_unsigned(false),
        "std" | "stddev" | "stddev_pop" | "stddev_samp" | "variance" | "var_pop" | "var_samp" => {
            aggregate_double()
        }
        // Strings.
        "upper" | "lower" | "ucase" | "lcase" | "ltrim" | "rtrim" | "trim" | "reverse"
        | "replace" | "quote" | "soundex" => {
            let s = arg(0)?;
            let length = char_length(&s);
            string_of(s, length)
        }
        "concat" => {
            let parts = args.iter().map(|e| ty(e)).collect::<Option<Vec<_>>>()?;
            let length = parts.iter().map(char_length).sum();
            let first = parts
                .into_iter()
                .find(is_string)
                .unwrap_or_else(|| int(true));
            string_of(first, length)
        }
        "concat_ws" => {
            let parts = args.iter().map(|e| ty(e)).collect::<Option<Vec<_>>>()?;
            let (separator, rest) = parts.split_first()?;
            let length = rest.iter().map(char_length).sum::<u64>()
                + char_length(separator) * (rest.len().saturating_sub(1) as u64);
            let first = rest
                .iter()
                .find(|c| is_string(c))
                .cloned()
                .unwrap_or_else(|| int(true));
            string_of(first, length)
        }
        "substring" | "substr" | "mid" => {
            let s = arg(0)?;
            let mut length = char_length(&s);
            if let Some(from) = args.get(1).and_then(|e| literal_int(e)) {
                length = length.saturating_sub(from.saturating_sub(1));
            }
            if let Some(n) = args.get(2).and_then(|e| literal_int(e)) {
                length = length.min(n);
            }
            string_of(s, length)
        }
        "left" | "right" => {
            let s = arg(0)?;
            let length = match args.get(1).and_then(|e| literal_int(e)) {
                Some(n) => n.min(char_length(&s)),
                None => char_length(&s),
            };
            string_of(s, length)
        }
        "lpad" | "rpad" => {
            let s = arg(0)?;
            let length = args.get(1).and_then(|e| literal_int(e))?;
            string_of(s, length)
        }
        "md5" => varchar(32, true),
        "sha1" | "sha" => varchar(40, true),
        "uuid" => varchar(36, true),
        "length" | "char_length" | "character_length" | "octet_length" | "bit_length" | "ascii"
        | "ord" | "crc32" => bigint(arg(0)?.nullable),
        "locate" | "instr" | "position" | "field" | "find_in_set" | "unix_timestamp"
        | "timestampdiff" | "to_days" | "to_seconds" => bigint(true),
        "strcmp" => int(true),
        // Dates.
        "now" | "current_timestamp" | "sysdate" | "localtime" | "localtimestamp"
        | "utc_timestamp" => named("datetime", false),
        "curdate" | "current_date" | "utc_date" => named("date", false),
        "curtime" | "current_time" | "utc_time" => named("time", false),
        "date" | "last_day" | "from_days" | "makedate" => named("date", true),
        "time" | "sec_to_time" | "maketime" | "timediff" => named("time", true),
        "timestamp" | "from_unixtime" => named("datetime", true),
        "year" => column(decl("year", "year".into(), None, None, None), true),
        "month" | "day" | "dayofmonth" | "dayofweek" | "dayofyear" | "hour" | "minute"
        | "second" | "week" | "weekday" | "weekofyear" | "quarter" | "yearweek" | "datediff"
        | "microsecond" | "period_diff" => int(true),
        "dayname" | "monthname" => varchar(9, true),
        "date_add" | "date_sub" | "adddate" | "subdate" => {
            let d = arg(0)?;
            match d.declaration.data_type.as_str() {
                "date" | "datetime" | "timestamp" => ViewColumn {
                    nullable: true,
                    default: None,
                    ..d
                },
                _ => return None,
            }
        }
        // Common types.
        "coalesce" | "ifnull" => {
            let values = args.iter().map(|e| ty(e)).collect::<Option<Vec<_>>>()?;
            let nullable = values.iter().all(|v| v.nullable);
            common(&values, nullable)?
        }
        "if" => {
            let values = vec![arg(1)?, arg(2)?];
            let nullable = values.iter().any(|v| v.nullable);
            common(&values, nullable)?
        }
        "nullif" => {
            let c = arg(0)?;
            match numeric(&c) {
                Some(Num::Int { .. }) => bigint(true),
                _ => ViewColumn {
                    nullable: true,
                    default: None,
                    ..c
                },
            }
        }
        "greatest" | "least" => {
            let values = args.iter().map(|e| ty(e)).collect::<Option<Vec<_>>>()?;
            let nullable = values.iter().any(|v| v.nullable);
            common(&values, nullable)?
        }
        // Numbers.
        "abs" => {
            let c = arg(0)?;
            match numeric(&c)? {
                Num::Int { .. } => bigint(c.nullable),
                num => of_num(num, c.nullable),
            }
        }
        "round" | "truncate" => {
            let c = arg(0)?;
            let nullable = c.nullable || args.get(1).is_some_and(|e| literal_int(e).is_none());
            match numeric(&c)? {
                Num::Decimal { precision, scale } => {
                    let digits = args.get(1).and_then(|e| literal_int(e)).unwrap_or(0);
                    decimal(precision - scale + digits + 1, digits.min(scale), nullable)
                }
                Num::Float => double(nullable),
                Num::Int { .. } => bigint(nullable),
            }
        }
        "ceil" | "ceiling" | "floor" => {
            let c = arg(0)?;
            match numeric(&c)? {
                Num::Float => double(c.nullable),
                _ => bigint(c.nullable),
            }
        }
        "sqrt" | "exp" | "ln" | "log" | "log2" | "log10" | "pow" | "power" | "sin" | "cos"
        | "tan" | "asin" | "acos" | "atan" | "atan2" | "cot" | "pi" | "rand" | "radians"
        | "degrees" => double(true),
        "sign" => bigint(arg(0)?.nullable),
        // JSON.
        "json_extract"
        | "json_object"
        | "json_array"
        | "json_set"
        | "json_insert"
        | "json_replace"
        | "json_remove"
        | "json_merge_patch"
        | "json_merge_preserve"
        | "json_arrayagg"
        | "json_objectagg"
        | "json_keys" => named("json", true),
        "json_unquote" => named("longtext", true),
        "json_length" | "json_depth" | "json_valid" | "json_contains" => int(true),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::{decl, expr_column, ViewColumn};
    use elyra_core::Collation;
    use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;

    /// `id INT NOT NULL`, `amt DECIMAL(10,2) NOT NULL`, `p DECIMAL(5,3)`,
    /// `name VARCHAR(20) NOT NULL`, `code CHAR(3)`.
    fn column_of(e: &Expr) -> Option<ViewColumn> {
        let Expr::Identifier(i) = e else { return None };
        let (declaration, nullable) = match i.value.as_str() {
            "id" => (decl("int", "int".into(), None, Some(10), Some(0)), false),
            "amt" => (
                decl("decimal", "decimal(10,2)".into(), None, Some(10), Some(2)),
                false,
            ),
            "p" => (
                decl("decimal", "decimal(5,3)".into(), None, Some(5), Some(3)),
                true,
            ),
            "name" => (
                decl("varchar", "varchar(20)".into(), Some(20), None, None),
                false,
            ),
            "code" => (decl("char", "char(3)".into(), Some(3), None, None), true),
            _ => return None,
        };
        Some(ViewColumn {
            name: i.value.clone(),
            declaration,
            nullable,
            collation: Collation::Ci,
            default: None,
            literal_digits: None,
        })
    }

    fn described(expr: &str) -> (String, bool, Option<String>) {
        let sql = format!("SELECT {expr}");
        let Statement::Query(q) = Parser::parse_sql(&MySqlDialect {}, &sql).unwrap().remove(0)
        else {
            unreachable!()
        };
        let SetExpr::Select(select) = *q.body else {
            unreachable!()
        };
        let SelectItem::UnnamedExpr(e) = &select.projection[0] else {
            unreachable!()
        };
        let c = expr_column(e, &column_of).unwrap_or_else(|| panic!("{expr}: no type"));
        (c.declaration.column_type, c.nullable, c.default)
    }

    /// Each answer is MySQL 8.4's for a view column of that expression.
    #[test]
    fn expressions_are_typed_as_mysql_types_them() {
        let d = |ty: &str, nullable: bool, default: Option<&str>| {
            (ty.to_string(), nullable, default.map(str::to_string))
        };
        for (expr, want) in [
            ("1.5", d("decimal(2,1)", false, Some("0.0"))),
            ("id + 1", d("bigint", false, Some("0"))),
            ("amt * 2", d("decimal(11,2)", false, Some("0.00"))),
            ("amt - p", d("decimal(12,3)", true, None)),
            ("amt * p", d("decimal(15,5)", true, None)),
            ("amt / p", d("decimal(17,6)", true, None)),
            ("id / 2", d("decimal(14,4)", true, None)),
            ("SUM(amt)", d("decimal(32,2)", true, None)),
            ("AVG(id)", d("decimal(14,4)", true, None)),
            ("COUNT(*)", d("bigint", false, Some("0"))),
            ("CONCAT(name, code)", d("varchar(23)", true, None)),
            ("CONCAT(name, id)", d("varchar(31)", true, None)),
            ("SUBSTRING(name, 2, 5)", d("varchar(5)", true, None)),
            ("IFNULL(code, 'zz')", d("varchar(3)", false, Some(""))),
            ("IFNULL(id, 0)", d("bigint", false, Some("0"))),
            (
                "IF(id > 1, amt, 0)",
                d("decimal(10,2)", false, Some("0.00")),
            ),
            (
                "CASE WHEN id > 1 THEN amt ELSE p END",
                d("decimal(11,3)", true, None),
            ),
            ("ROUND(amt, 1)", d("decimal(10,1)", false, Some("0.0"))),
            ("CAST(id AS CHAR)", d("varchar(11)", true, None)),
            ("NOW()", d("datetime", false, Some("0000-00-00 00:00:00"))),
        ] {
            assert_eq!(described(expr), want, "{expr}");
        }
    }
}
