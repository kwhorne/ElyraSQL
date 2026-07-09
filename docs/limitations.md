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
- `ORDER BY`, grouped/aggregated output, and in-transaction reads materialize
  their working set in memory (no spill-to-disk).

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

- No charset/collation handling: text compares and sorts bytewise (no
  case-insensitive `_ci` collation, no `COLLATE`).
- `ENUM`/`SET` are stored as text and not value-checked; no spatial types;
  full-text search is vector-only.

## Security & operations

- One user with a role (read/write/admin); no `GRANT`/`REVOKE`, multiple users,
  or per-object privileges.
- No replication/HA, no built-in backup/restore tooling, and no metrics /
  slow-query log yet.

## Wire protocol

- Prepared statements can desynchronize across repeated
  `COM_STMT_CLOSE` → `COM_STMT_PREPARE` cycles on one connection with strict
  clients (an upstream library limitation). Statement reuse and pooled clients
  are unaffected.
- No `LOAD DATA INFILE`.

## Roadmap

Candidate next steps, roughly in order of value:

1. `charset`/`collation` (at least a case-insensitive default).
2. Backup/restore and a consistent logical dump.
3. `GRANT`/`REVOKE` and multiple users.
4. `ON UPDATE` referential actions and multi-level cascades.
5. Spill-to-disk for large sorts/aggregations.
6. Cost-based planning with statistics; hash/merge joins.
7. Replication for HA.

Have a need that isn't listed? Open an issue on
[GitHub](https://github.com/kwhorne/ElyraSQL/issues).
