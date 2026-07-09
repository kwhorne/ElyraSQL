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
- `RIGHT`/`FULL` and non-equi joins use nested-loop (no hash/merge).
- There is no cost-based optimizer or statistics; the planner uses heuristic
  fast paths.
- `ORDER BY` is memory-bounded: `ORDER BY ... LIMIT` uses a top-N heap, and
  large unbounded sorts spill sorted runs to temp files (external merge sort,
  `ELYRASQL_SORT_MAX_ROWS`). `GROUP BY` holds groups in memory but fails
  gracefully past `ELYRASQL_GROUP_MAX_GROUPS` rather than risking OOM; it does
  not yet spill (partitioned aggregation is future work). In-transaction reads
  still materialize their working set.

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

- Text uses a **default case-insensitive collation**: comparisons, sorting,
  indexing, grouping, joins, `DISTINCT`, and `UNIQUE`/`PRIMARY KEY` all treat
  `'Foo'` and `'foo'` as equal. Not yet: accent-insensitivity, per-column
  `COLLATE`, a binary (`_bin`, case-sensitive) opt-out, or alternate charsets.
- `ENUM`/`SET` are stored as text and not value-checked; no spatial types;
  full-text search is vector-only.

## Security & operations

- Multiple persistent accounts with `CREATE USER`/`GRANT`/`REVOKE` exist, but
  privileges are **global and coarse** (read/write/admin) — no per-database or
  per-table scoping, and no column/routine privileges.
- Hot and offline backup/restore exist, but there is no incremental backup or
  point-in-time recovery.
- No replication/HA, and no metrics / slow-query log yet.

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
3. `ON UPDATE` referential actions and multi-level cascades.
4. Spill-to-disk for large sorts/aggregations.
5. Cost-based planning with statistics; hash/merge joins.
6. Observability: slow-query log and metrics.
7. Replication for HA.

Have a need that isn't listed? Open an issue on
[GitHub](https://github.com/kwhorne/ElyraSQL/issues).
