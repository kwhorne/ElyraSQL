# Limitations & Roadmap

ElyraSQL is young. This page is an honest inventory of what is **not** yet
implemented, so you can judge fit.

## SQL surface

- Subqueries in `WHERE` (uncorrelated **and** correlated: `IN`, scalar,
  `EXISTS`), derived tables (`FROM (SELECT ...) AS t`), and `HAVING` are
  supported. Scalar subqueries in the **SELECT list**, CTEs (`WITH`), window
  functions, and correlated subqueries combined with joins are not.
- No views, triggers, stored procedures, or user-defined functions.
- `ALTER TABLE` covers add/drop/rename column and rename table — not
  `MODIFY`/`CHANGE` (column type changes) or `ADD/DROP INDEX` via `ALTER`.
- JSON is stored and validated but has no path operators
  (`->`, `->>`, `JSON_EXTRACT`) yet.
- Minimal `information_schema` / `SHOW` support.

## Query planning

- Range scans and index nested-loop joins are **single-column**; composite
  ranges fall back to a scan.
- `RIGHT`/`FULL` and non-equi joins use nested-loop (no hash/merge).
- `ORDER BY`, grouped/aggregated output, and in-transaction reads materialize
  their working set in memory.

## Transactions

- **Snapshot** isolation (default, first-committer-wins) and **serializable**
  isolation (opt-in via `SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE`,
  read-set + scanned-range validation). Serializable is optimistic
  (validate-on-commit), so it aborts conflicting transactions with error `1213`
  rather than blocking; range validation is conservative (any change in a
  scanned range aborts).
- `SET autocommit=0` is accepted but not honoured; use explicit `BEGIN`.

## Analytics

- The OLAP path is a **row-oriented parallel streaming** aggregator, not a
  columnar engine; there is no spill-to-disk for working sets that exceed
  memory (only per-group state is bounded, not sort/materialize buffers).

## Wire protocol

- Prepared statements can desynchronize across repeated
  `COM_STMT_CLOSE` → `COM_STMT_PREPARE` cycles on one connection with strict
  clients (an upstream library limitation). Statement reuse and pooled clients
  are unaffected.

## Roadmap

Candidate next steps, roughly in order of value:

1. Window functions and scalar subqueries in the SELECT list.
2. More JSON functions (`JSON_SET`, `JSON_ARRAY`, containment).
3. CTEs (`WITH`).
4. Secondary-index range on composite keys; merge joins.
5. Columnar OLAP with spill-to-disk.
6. Richer `information_schema` / `SHOW`.
7. The ElyraSQL client (Rust + Svelte).

Have a need that isn't listed? Open an issue on
[GitHub](https://github.com/kwhorne/ElyraSQL/issues).
