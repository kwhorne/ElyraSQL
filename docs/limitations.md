# Limitations & Roadmap

ElyraSQL is young. This page is an honest inventory of what is **not** yet
implemented, so you can judge fit.

## SQL surface

- Subqueries (`WHERE` and SELECT-list, uncorrelated **and** correlated,
  including over joins), derived tables, CTEs (`WITH`, including
  `WITH RECURSIVE`), `HAVING`, window functions with explicit `ROWS`/`RANGE`
  frames, set operations, and `FROM`-less `SELECT` are supported.
- Not yet: named windows, `RANGE`/`GROUPS` numeric-offset frames, correlated
  subqueries combined with aggregation over a join, triggers, stored
  procedures, user-defined functions, and events.

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
- `ANALYZE TABLE` records row-count statistics (surfaced as
  `information_schema.tables.TABLE_ROWS`). Join build-side selection uses live
  materialized sizes; there is not yet a full cost-based optimizer with
  per-column histograms or automatic join reordering.
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
  locked row that another transaction changes aborts your commit. There is no
  pessimistic blocking; `LOCK IN SHARE MODE` and `LOCK TABLES` are not parsed
  (use `FOR SHARE`). Row locking is applied to single-table locking selects.
- A single writer serializes all commits (redb); write concurrency is bounded
  by that writer plus group commit.

## Types & text

- Text is **case-insensitive by default**. A column can opt into case-sensitive
  behavior with `COLLATE ..._bin` / `BINARY`, which applies to equality/range
  comparisons (`WHERE`), `UNIQUE`, `PRIMARY KEY`, and secondary indexes. Not
  yet honoring per-column `_bin`: `ORDER BY`, `GROUP BY`, `DISTINCT` and join
  keys (these still use the default case-insensitive collation). Accent
  sensitivity and alternate charsets are not implemented.
- `ENUM`/`SET` are stored as text and not value-checked; no spatial types;
  full-text search is vector-only.

## Security & operations

- Multiple persistent accounts with `CREATE USER`/`GRANT`/`REVOKE`, with coarse
  (read/write/admin) privileges granted **globally** or **per table**. No
  per-database, per-column, or routine privileges; reads are always allowed at
  the global baseline (table grants only raise write/admin).
- Hot and offline backup/restore exist, but there is no incremental backup or
  point-in-time recovery.
- Asynchronous primary → replica replication (read replicas, warm standby,
  manual failover) is supported. There is no synchronous/quorum commit,
  automatic leader election/failover, or multi-primary.

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
7. Synchronous/quorum replication and automatic failover.

Have a need that isn't listed? Open an issue on
[GitHub](https://github.com/kwhorne/ElyraSQL/issues).
