# SQL Reference — Overview

ElyraSQL parses SQL with the MySQL dialect. This section documents the
statements and expressions the engine currently supports.

## Supported statements

| Category | Statements |
|----------|-----------|
| DDL | `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `ALTER TABLE` |
| DML | `INSERT`, `UPDATE`, `DELETE` |
| Query | `SELECT` with `WHERE`, `ORDER BY`, `LIMIT`/`OFFSET`, `JOIN`, `GROUP BY` |
| Transactions | `BEGIN` / `START TRANSACTION`, `COMMIT`, `ROLLBACK` |
| Session | `SET`, `USE` (accepted; single-catalog) |

## Expressions

- Literals: integers, floats, strings, booleans, `NULL`.
- Arithmetic: `+`, `-`, `*`, `/`, `%`.
- Comparison: `=`, `!=`/`<>`, `<`, `<=`, `>`, `>=`.
- Logical: `AND`, `OR`, `NOT`.
- Predicates: `BETWEEN`, `IS NULL`, `IS NOT NULL`.
- Functions: aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) and the vector
  distance family (`VEC_DISTANCE`, ...).

Comparisons use SQL three-valued logic (any comparison with `NULL` is unknown)
and coerce across compatible types (e.g. a `DATE` column against a
`'2024-01-01'` string literal, or `DECIMAL` against a numeric literal).

## A note on identifiers

Columns may be referenced bare (`name`) or qualified (`users.name`). In joins,
qualify columns that appear in more than one table to avoid ambiguity errors.

## Pages

- [Data Types](data-types.md)
- [Tables & DDL](ddl.md)
- [Insert, Update, Delete](dml.md)
- [Queries & Joins](queries.md)
- [Aggregation & OLAP](aggregation.md)
- [Transactions](transactions.md)
- [Vector Search](vector-search.md)
