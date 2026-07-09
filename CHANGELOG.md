# Changelog

All notable changes to ElyraSQL are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [0.8.2] - 2026-07-09

High-availability & feature-completeness release.

### Automatic failover

- `cluster` mode: Raft-style leader election (terms, majority votes, heartbeats,
  step-down). The elected leader accepts writes and serves replication;
  followers are read-only and replicate from it. On leader failure a surviving
  node is automatically elected. Leader-only writes provide fencing; a majority
  quorum avoids split-brain. Data replication remains asynchronous.

### Stored procedures

- `IN`/`OUT`/`INOUT` parameters, session `@user` variables, and full control
  flow: `LOOP`, `REPEAT ... UNTIL`, labeled `LEAVE`/`ITERATE` (in addition to
  `IF`/`WHILE`).

### Full-text search

- `CREATE FULLTEXT INDEX` builds a persistent inverted index maintained on
  INSERT/UPDATE/DELETE and used to accelerate `MATCH ... AGAINST`; light English
  stemming folds regular word forms.

### Spatial

- `POINT`/`GEOMETRY` columns (WKT) with `POINT`, `ST_X`, `ST_Y`, `ST_Distance`,
  `ST_AsText`, `ST_GeomFromText`.

## [0.8.1] - 2026-07-09

Programmability release: triggers, procedural stored procedures, and full-text
search.

### Triggers

- Row-level `CREATE TRIGGER name {BEFORE|AFTER} {INSERT|UPDATE|DELETE} ON t FOR
  EACH ROW <body>` / `DROP TRIGGER`, with `NEW.col` / `OLD.col`. BEFORE bodies
  support `SET NEW.col = expr`; AFTER bodies run arbitrary DML per affected row.
  Firing is depth-guarded against runaway recursion.

### Stored procedures

- Parameters (`IN`), local variables (`DECLARE`, `SET`), and control flow
  (`IF`/`ELSEIF`/`ELSE`, `WHILE`), interpreted over the procedure body.

### Full-text search

- `MATCH(col, ...) AGAINST('terms' [IN BOOLEAN MODE])` — scan-based relevance
  scoring (natural-language OR-of-terms, or boolean `+`/`-`).

### Fixed

- The fast INSERT path now persists the `AUTO_INCREMENT` counter, so consecutive
  auto-increment inserts no longer reuse ids.

## [0.8.0] - 2026-07-09

Programmability & log-management release.

### Binary log management

- The binlog is now a directory of rotating segment files, rotating at
  `ELYRASQL_BINLOG_SEGMENT_MB` (default 128 MB).
- `SHOW BINARY LOGS` lists segments and sizes; `PURGE BINARY LOGS TO '<name>'`
  deletes older segments. `--binlog` and `binlog-replay` now take a directory.

### Stored procedures

- `CREATE [OR REPLACE] PROCEDURE name() BEGIN ...; END`, `CALL name()`, and
  `DROP PROCEDURE` — statement-list macros executed through the engine, with a
  recursion-depth guard. (Parameters, variables and control flow are not yet
  supported.)

## [0.7.0] - 2026-07-09

Durability & recovery release: point-in-time recovery, richer statistics, and
semi-synchronous replication.

### Point-in-time recovery

- Optional append-only **binlog** (`--binlog`) records every committed write-set
  with an LSN and timestamp.
- `elyrasql binlog-replay --data <f> --binlog <f> [--until-lsn N |
  --until-time-ms T]` replays onto a restored backup (or an empty file) up to a
  chosen point — exact, idempotent recovery.

### Statistics

- `ANALYZE TABLE` now collects per-column statistics (distinct-value count, null
  count, min/max), exposed via `information_schema.column_statistics`.
- The planner drives a comma cross-join from the smallest analyzed table.

### Replication

- **Semi-synchronous** mode (`--semi-sync-ms`): a commit waits for a replica to
  acknowledge before returning, degrading to asynchronous on timeout or when no
  replica is attached. Replication is now bidirectional (replicas acknowledge
  applied LSNs).

## [0.6.0] - 2026-07-09

Scale & availability release: replication, partitioned aggregation spill,
cost-based joins with statistics, and a Prometheus metrics endpoint.

### Replication & HA

- Asynchronous primary → replica replication. A primary streams LSN-tagged
  committed write-sets (`--replication-listen`); a replica bootstraps from a
  snapshot and applies the stream (`elyrasql replica`), serving read-only
  queries. Idempotent, ordered application means replicas never diverge; failover
  is manual (a replica file is a complete database).

### Aggregation

- `GROUP BY` with many distinct groups now falls back to **partitioned spill**
  aggregation (bounded memory) instead of erroring, completing the OOM-safety
  story alongside `ORDER BY` spill.

### Query planning

- Equi hash joins now cover **INNER / LEFT / RIGHT** with a cost-based build side
  (INNER builds the smaller relation; RIGHT no longer degrades to nested-loop).
- `ANALYZE TABLE` records row-count statistics, surfaced as
  `information_schema.tables.TABLE_ROWS`.

### Observability

- Prometheus/OpenMetrics endpoint (`--metrics-listen`, `GET /metrics`) exposing
  the server counters, plus a `/health` probe.

## [0.5.0] - 2026-07-09

Operations & data-model release: observability, memory-bounded sorts, per-column
collation, and scoped privileges.

### Observability

- `SHOW STATUS` / `SHOW GLOBAL STATUS` counters (uptime, connections,
  Questions/Queries, `Com_*`, Errors, Slow_queries), with `LIKE 'prefix%'`.
- `SHOW [FULL] PROCESSLIST` listing live connections and their current query.
- Slow-query log: `--slow-query-ms` / `ELYRASQL_SLOW_QUERY_MS` logs statements
  at or above the threshold with their duration.

### Memory safety

- `ORDER BY` is now memory-bounded: a top-N heap for `ORDER BY ... LIMIT`, and an
  external merge sort that spills to temp files for large sorts
  (`ELYRASQL_SORT_MAX_ROWS`).
- `GROUP BY` fails gracefully past `ELYRASQL_GROUP_MAX_GROUPS` instead of risking
  an out-of-memory crash.

### Collation

- Per-column `COLLATE ..._bin` / `BINARY` opt-in to case-sensitive behavior for
  `WHERE` comparisons, `UNIQUE`, `PRIMARY KEY` and secondary indexes (text is
  still case-insensitive by default). `ORDER BY`/`GROUP BY`/joins still use the
  default collation.

### Access control & integrity

- Per-table `GRANT`/`REVOKE` (`ON <table>`): raises a read-only account's level
  for specific tables; reads stay globally allowed. Deny-safe when a target is
  indeterminate. `SHOW GRANTS` lists global and per-table grants.
- `ON UPDATE` referential actions enforced (CASCADE / SET NULL / RESTRICT) when
  a parent's referenced key changes.

## [0.4.0] - 2026-07-09

Production-readiness release: backup, real user management, and a MySQL-style
case-insensitive default collation.

### Backup & restore

- **Hot backup** with `BACKUP TO '<path>'` (admin): copies the whole database
  from a consistent MVCC snapshot into a fresh file without blocking writers.
- **Offline** `elyrasql backup` and `elyrasql restore` CLI subcommands.
- The backup is a complete database file — start a server on it or copy it back.

### Users & access control

- Persistent accounts stored in the database file (survive restarts):
  `CREATE USER`, `DROP USER`, `ALTER USER` / `SET PASSWORD`, `GRANT`, `REVOKE`,
  `SHOW GRANTS`.
- New accounts start read-only; `GRANT` raises them, `REVOKE` lowers them.
  Privileges map to the coarse global read/write/admin levels (the object
  clause is parsed but not scoped). Passwords stored as `SHA1(SHA1(pw))`.
- Authentication consults startup bootstrap accounts plus persistent accounts;
  open dev mode applies only when no account exists.

### Collation

- **Default case-insensitive collation** for text, applied consistently across
  comparisons, `ORDER BY`, indexing, `GROUP BY`, `DISTINCT`, joins, set
  operations, and `UNIQUE`/`PRIMARY KEY`.
- **On-disk change:** text key encoding is now case-folded. Databases created
  before 0.4.0 that use text primary keys or text indexes should be reloaded.

## [0.3.0] - 2026-07-09

Data-integrity release: the constraints a production database must enforce.

### Constraints

- **UNIQUE** constraints are now enforced (previously stored but not checked).
  Column-level `UNIQUE`, table-level `UNIQUE(...)`, and `CREATE UNIQUE INDEX`
  all reject duplicates (error `1062`), including duplicates within a single
  statement; multiple `NULL`s are allowed.
- **FOREIGN KEY** constraints are enforced. INSERT/UPDATE require a matching
  parent row (primary key or unique index, error `1452`); DELETE on the parent
  applies `RESTRICT`/`NO ACTION` (block), `ON DELETE CASCADE` (delete children),
  or `ON DELETE SET NULL`.
- **CHECK** constraints (column- and table-level) are enforced on INSERT and
  UPDATE, passing on TRUE or NULL per SQL semantics.

### Transactions

- **SAVEPOINT**, **ROLLBACK TO SAVEPOINT**, and **RELEASE SAVEPOINT**.
- **SELECT ... FOR UPDATE / FOR SHARE**: optimistic row locking — a locked row
  changed by another transaction aborts the locking transaction at commit
  (lost-update prevention without blocking).

### Fixed

- Three-valued logic for comparisons: `NULL = x`, `x >= NULL`, etc. now evaluate
  to NULL (UNKNOWN) instead of false. WHERE still excludes them, CHECK passes,
  and SELECT shows NULL — matching SQL semantics.

## [0.2.1] - 2026-07-09

Performance and robustness pass, verified on Linux (1,000,000-row workloads).

### Performance

- **Bulk `INSERT` ~5-6x faster** (~33k → ~190k rows/s in a container, ~240k on
  fast-fsync storage). The 0.2.0 duplicate-key check did one storage read per
  row (each opening its own read transaction); it now:
  - detects duplicates inside the write transaction itself for plain `INSERT`
    (redb returns the previous value — no existence read), and
  - batches the existence check into a single read for `IGNORE`/`REPLACE`/
    `ON DUPLICATE KEY UPDATE`.
- **Group commit for `INSERT`**: the writer coalesces queued plain/insert jobs
  into one transaction (one fsync), falling back to per-statement application
  only when a group contains a duplicate — so concurrent write throughput is
  preserved.
- **`GROUP BY` ~3.4x faster** on low-cardinality groups (~927ms → ~273ms over
  1M rows): the group key is a compact binary encoding instead of
  `Debug`-formatting every row's key columns.
- Statement dispatch inspects only a short prefix instead of lowercasing the
  whole (possibly large) SQL text.

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

[0.8.2]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.2
[0.8.1]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.1
[0.8.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.0
[0.7.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.7.0
[0.6.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.6.0
[0.5.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.5.0
[0.4.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.4.0
[0.3.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.3.0
[0.2.1]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.2.1
[0.2.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.2.0
[0.1.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.1.0
