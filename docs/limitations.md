# Limitations & Roadmap

ElyraSQL is young. This page is an honest inventory of what is **not** yet
implemented, so you can judge fit.

## SQL surface

- Subqueries (`WHERE` and SELECT-list, uncorrelated **and** correlated,
  including over joins), derived tables, CTEs (`WITH`, including
  `WITH RECURSIVE`), `HAVING`, window functions with explicit `ROWS`/`RANGE`
  frames, set operations, and `FROM`-less `SELECT` are supported.
- Stored procedures support `IN`/`OUT`/`INOUT` parameters, session `@user`
  variables (`SET @x = ...`), local variables (`DECLARE`, `SET`), and control
  flow: `IF`/`ELSEIF`/`ELSE`, `WHILE`, `LOOP`, `REPEAT ... UNTIL`, with labeled
  `LEAVE`/`ITERATE`. `OUT`/`INOUT` arguments must be `@user` variables (written
  back on return). Cursors and condition handlers are not yet supported.
- Row-level triggers are supported: `CREATE TRIGGER name {BEFORE|AFTER}
  {INSERT|UPDATE|DELETE} ON t FOR EACH ROW <body>`, with `NEW.col`/`OLD.col`.
  BEFORE bodies support `SET NEW.col = expr`; AFTER bodies run arbitrary DML.
  Firing is depth-guarded against runaway recursion. Triggers fire on
  single-table INSERT/UPDATE/DELETE (not on multi-table or the upsert variants
  REPLACE/ON DUPLICATE/IGNORE).
- Not yet: named windows, `RANGE`/`GROUPS` numeric-offset frames, correlated
  subqueries combined with aggregation over a join, user-defined functions, and
  events.

## Constraints & integrity

- Enforced: `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `CHECK`, and `FOREIGN KEY`.
- Foreign keys reference a primary key or unique index; `ON DELETE`
  `RESTRICT`/`NO ACTION`/`CASCADE`/`SET NULL` are enforced.
- Not yet: `ON UPDATE` referential actions when a referenced key changes,
  multi-level (recursive) cascades, and deferred constraint checking.

## Query planning

- Range scans and index nested-loop joins are **single-column**; composite
  ranges fall back to a scan.
- Equi joins (INNER/LEFT/RIGHT) use a hash join with a cost-based build side
  (the smaller relation for INNER; an index nested-loop join when the driving
  side is small and the partner is indexed). `FULL` and non-equi joins use
  nested-loop; there is no merge join.
- `ANALYZE TABLE` records row-count and per-column statistics (NDV, null count,
  min/max), surfaced as `information_schema.tables.TABLE_ROWS` and
  `information_schema.column_statistics`. The planner drives a comma cross-join
  from the smallest analyzed table and picks hash-join build sides by live size;
  automatic reordering of explicit multi-table JOIN chains and full histogram-
  based cardinality estimation are not yet implemented.
- `ORDER BY` is memory-bounded: `ORDER BY ... LIMIT` uses a top-N heap, and
  large unbounded sorts spill sorted runs to temp files (external merge sort,
  `ELYRASQL_SORT_MAX_ROWS`). `GROUP BY` with many distinct groups falls back to
  **partitioned spill** aggregation (rows routed to partitions by group-key
  hash, spilled to temp files, aggregated per partition) so memory stays
  bounded; a single skewed partition past `ELYRASQL_GROUP_MAX_GROUPS` still
  errors. In-transaction reads materialize their working set.
- Spilled `GROUP BY` output is ordered per partition (add `ORDER BY` for a
  defined order).

## Transactions & locking

- **Snapshot** isolation (default, first-committer-wins) and **serializable**
  isolation (opt-in), both optimistic (validate-on-commit; conflicts abort with
  error `1213` rather than blocking).
- `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` are supported.
- `SELECT ... FOR UPDATE` / `FOR SHARE` provide **optimistic** row locking: a
  locked row that another transaction changes aborts your commit. Row locking is
  applied to single-table locking selects.
- **Pessimistic table locking** is also available: `LOCK TABLES t READ|WRITE` /
  `UNLOCK TABLES` take blocking table locks (a `WRITE` lock blocks other readers
  and writers; a `READ` lock blocks writers). While an explicit lock is held,
  conflicting statements from other sessions block until it is released, or fail
  with `1205` (lock wait timeout). `LOCK IN SHARE MODE` is accepted as a synonym
  for `FOR SHARE`. MVCC reads are not blocked by table locks (they read a
  consistent snapshot). When no explicit lock is held, locking adds no overhead.
- A single writer serializes all commits (redb); write concurrency is bounded
  by that writer plus group commit.

## Types & text

- Text is **case-insensitive by default**. A column can opt into case-sensitive
  behavior with `COLLATE ..._bin` / `BINARY`, which applies to equality/range
  comparisons (`WHERE`), `UNIQUE`, `PRIMARY KEY`, and secondary indexes. Not
  yet honoring per-column `_bin`: `ORDER BY`, `GROUP BY`, `DISTINCT` and join
  keys (these still use the default case-insensitive collation). Accent
  sensitivity and alternate charsets are not implemented.
- Full-text search: `MATCH(col, ...) AGAINST('terms' [IN BOOLEAN MODE])`
  (natural-language OR-of-terms, or boolean `+`/`-`, with relevance scoring).
  `CREATE FULLTEXT INDEX` builds a persistent inverted index that is maintained
  on INSERT/UPDATE/DELETE and used to accelerate MATCH; without one, MATCH falls
  back to a scan. Light stemming folds regular forms (dogs->dog, foxes->fox) but
  not irregular ones (wolves), and there are no synonyms. Vector (ANN) search is
  also available.
- `ENUM`/`SET` are stored as text and not value-checked.
- Basic spatial support: `POINT`/`GEOMETRY` columns are stored as WKT text, with
  `POINT(x,y)`, `ST_X`, `ST_Y`, `ST_Distance` (Euclidean), `ST_AsText`, and
  `ST_GeomFromText`. Only 2D points are supported; there is no spatial index or
  SRID/geodesic distance.

## Security & operations

- Multiple persistent accounts with `CREATE USER`/`GRANT`/`REVOKE`, with coarse
  (read/write/admin) privileges granted **globally** or **per table**. No
  per-database, per-column, or routine privileges; reads are always allowed at
  the global baseline (table grants only raise write/admin).
- Hot and offline backup/restore, plus an append-only binlog for point-in-time
  recovery (`--binlog` + `elyrasql binlog-replay`). Binlog rotation/pruning is
  manual; there is no incremental (block-level) backup.
- Primary → replica replication (read replicas, warm standby), asynchronous by
  default with an optional **semi-synchronous** mode (`--semi-sync-ms`).
  **Automatic failover** is available in `cluster` mode via Raft-style leader
  election (majority quorum, leader-only writes/fencing). Replication of data is
  still asynchronous (a new leader may lack the old leader's last writes); there
  is no synchronous/quorum commit or multi-primary.

## Wire protocol

- Prepared statements can desynchronize across repeated
  `COM_STMT_CLOSE` → `COM_STMT_PREPARE` cycles on one connection with strict
  clients (an upstream library limitation). Statement reuse and pooled clients
  are unaffected.
- No `LOAD DATA INFILE`.

## Roadmap

Candidate next steps, roughly in order of value:

1. Per-column `COLLATE` and a binary (case-sensitive) collation opt-out.
2. Scoped privileges (per-database / per-table `GRANT`).
3. Multi-level (recursive) cascades and deferred constraints.
4. Spill-to-disk for large sorts/aggregations.
5. Cost-based planning with statistics; hash/merge joins.
6. Observability: slow-query log and metrics.
7. Synchronous/quorum commit (zero-data-loss failover).

Have a need that isn't listed? Open an issue on
[GitHub](https://github.com/kwhorne/ElyraSQL/issues).
