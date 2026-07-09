# Changelog

All notable changes to ElyraSQL are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-07-09

A large expansion of SQL coverage on top of the 0.1.0 foundation, turning
ElyraSQL into a broadly MySQL-compatible engine.

### Queries

- Subqueries in `WHERE` and the SELECT list — uncorrelated and correlated,
  including correlated subqueries over joins (`IN`, scalar, `EXISTS`).
- Derived tables (`FROM (SELECT ...) AS t`).
- Common table expressions (`WITH`), including chained CTEs and
  `WITH RECURSIVE`.
- Window functions (`OVER`): `ROW_NUMBER`, `RANK`, `DENSE_RANK`, running and
  partition `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`, `LAG`/`LEAD`, and explicit
  `ROWS`/`RANGE` frames.
- `HAVING`.
- Set operations: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`.
- `FROM`-less `SELECT` (e.g. `SELECT 1`, `SELECT NOW()`).

### DML

- `INSERT ... SELECT`.
- Upserts: `REPLACE`, `INSERT IGNORE`, and `ON DUPLICATE KEY UPDATE`
  (with correct secondary-index maintenance and duplicate-key error `1062`).
- Subqueries in `UPDATE`/`DELETE` `WHERE` (uncorrelated and correlated).
- Multi-table `UPDATE` and `DELETE` (joins in mutations).

### DDL

- `CREATE TABLE ... AS SELECT`, `CREATE TABLE ... LIKE`, `TRUNCATE TABLE`.
- `CREATE VIEW` / `DROP VIEW` (including column lists and views over views).
- `ALTER TABLE ... MODIFY`/`CHANGE COLUMN`, and `ALTER COLUMN SET/DROP DEFAULT`
  and `SET/DROP NOT NULL` (with data re-coercion on type change).
- Column `DEFAULT` (constants and functions), `AUTO_INCREMENT`, and stored
  generated columns.
- `ENUM`/`SET`, `BINARY`/`VARBINARY`, and `BIT` column types.

### Functions

- Date/time: `NOW`/`CURRENT_TIMESTAMP`/`CURDATE`/`CURTIME`, `YEAR`/`MONTH`/`DAY`/
  `HOUR`/`MINUTE`/`SECOND`, `QUARTER`/`DAYOFWEEK`/`DAYOFYEAR`, `EXTRACT`,
  `DATE_ADD`/`DATE_SUB`/`TIMESTAMPADD`, `DATEDIFF`/`TIMESTAMPDIFF`, `WEEK`/
  `YEARWEEK`, `DATE_FORMAT`, `STR_TO_DATE`, `LAST_DAY`, and the
  `d + INTERVAL n UNIT` operator.
- String: `CONCAT`/`CONCAT_WS`, `UPPER`/`LOWER`, `SUBSTRING`/`SUBSTRING_INDEX`,
  `LEFT`/`RIGHT`, `TRIM` family, `REPLACE`/`REVERSE`/`REPEAT`, `LPAD`/`RPAD`,
  `INSTR`/`LOCATE`, `FIELD`/`ELT`, and `REGEXP`/`RLIKE`.
- Math, conditional (`COALESCE`/`IFNULL`/`NULLIF`/`IF`/`CASE`), `CAST`
  (including exact `DECIMAL` and `BINARY`), `UUID()`.
- JSON: `JSON_EXTRACT`/`->`/`->>`, `JSON_ARRAY`/`JSON_OBJECT`, `JSON_SET`/
  `JSON_INSERT`/`JSON_REPLACE`/`JSON_REMOVE`, `JSON_CONTAINS`/`JSON_LENGTH`/
  `JSON_KEYS`/`JSON_TYPE`/`JSON_VALID`/`JSON_QUOTE`.
- Aggregates: `GROUP_CONCAT`, conditional aggregates (`SUM(CASE ...)`),
  `COUNT(DISTINCT expr)`.
- Bitwise `&`, `|`, `^`.

### Transactions

- Write-conflict detection (first-committer-wins, MySQL error `1213`).
- Opt-in serializable isolation with read-set and scanned-range validation.

### Introspection

- `SHOW TABLES`, `SHOW COLUMNS`, `DESCRIBE`/`DESC`, `SHOW CREATE TABLE`,
  `SHOW INDEX`/`SHOW KEYS`.
- Queryable `INFORMATION_SCHEMA`: `tables`, `columns`, `statistics`,
  `key_column_usage`.

### Numerics & wire protocol

- Exact `DECIMAL` arithmetic (`+`, `-`, `*`) and exact `SUM(DECIMAL)`.
- Value-driven result column typing (computed columns report the right wire
  type; no spurious `.0`).
- `DATE`/`DATETIME`/`TIME` prepared-statement parameters decoded from the
  binary protocol.

### Fixed

- `DateTime` vs `DATE` comparison (previously always false).
- `DROP TABLE` left orphaned secondary-index entries.
- `INSERT` affected-row count included index-entry writes.

### Docs & project

- MkDocs Material documentation site, contributing guide, issue/PR templates,
  security and conduct policies, Dependabot configuration.

## [0.1.0]

Initial release: single-file ACID storage (`.edb`), MySQL wire protocol,
core CRUD with `WHERE`/`ORDER BY`/`LIMIT`, indexes, aggregation and `GROUP BY`,
joins, prepared statements, authentication and TLS, vector search (exact +
HNSW), parallel OLAP aggregation, and transactions with snapshot isolation.

[0.2.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.2.0
[0.1.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.1.0
