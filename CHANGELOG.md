# Changelog

All notable changes to ElyraSQL are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Persisted HNSW vector index (ESQL-27).** The built graph is now saved to a
  sibling cache directory `<data>.vidx/`, so a restart **loads** it and reconciles
  any changes since — no cold-start rebuild from a full table scan. It is kept
  outside the authoritative single file (like `<data>.raftstate`), so it is not
  replicated, not in backups, and does not touch the global write sequence that
  gates the column cache; a missing / corrupt / wrong-version snapshot safely
  falls back to a rebuild. The snapshot is written on first build and on
  compaction (not on every write). Verified end-to-end: an index survives a server
  restart and returns correct nearest-neighbours without rebuilding.
- **Incremental HNSW vector-index maintenance (ESQL-26).** A write to a
  vector-indexed table no longer forces the next ANN query to rebuild the whole
  graph. The cached index is reconciled against storage instead: only the rows
  inserted / updated / deleted since the last reconcile are applied to the
  existing graph (new vectors inserted via `Hnsw::insert_one`, removed or
  superseded ones soft-tombstoned and filtered from results), detected by a
  content hash so all of INSERT/UPDATE/DELETE are correct. A single insert into a
  500k-row index now adds one node instead of rebuilding 500k. A full rebuild is
  reserved for the first build, a change as large as the table, or compaction when
  too many nodes are tombstoned. Verified end-to-end (insert/update/delete
  reflected in `VEC_DISTANCE` search) with recall-preserving tests. The graph is
  still memory-only (rebuilt cold on restart — ESQL-27).

## [1.4.9] - 2026-07-21

Reliability & cache-efficiency pass from a second codebase review. Also documents
several architectural gaps honestly (with tracking issues) rather than rushing
correctness-sensitive rewrites. No on-disk format change.

### Fixed

- **Lock-poison recovery.** The vector-index registry, the OLAP column cache, and
  the HNSW scratch-buffer pool now recover a poisoned lock via `into_inner()`
  instead of `.unwrap()` panicking. These guard only self-healing caches / reusable
  scratch pools, so a panic in one query no longer cascades into a whole-process
  crash on the next lock acquisition (worst case: a stale/missing cache entry that
  is rebuilt).
- The MySQL connection handler no longer panics on a TLS-capability mismatch
  (carried from 1.4.8): the one connection is dropped with a clean error.

### Changed

- **Column cache eviction is now approximate-LRU** instead of arbitrary: each
  cached table carries an atomic `last_used` tick bumped on read (no lock
  upgrade), and eviction drops the least-recently-used entries to fit the budget.

### Documented (known limitations, with tracking issues)

- Vector (HNSW) index rebuilds fully on any table write and is not persisted
  (cold start) — best for read-heavy / batch-updated embedding workloads today
  (ESQL-26 incremental maintenance, ESQL-27 persistence).
- `WHERE col IN (SELECT ...)` and `DISTINCT` collection are in-memory (no spill);
  correlated subqueries run as `O(N×M)` nested loops (ESQL-28).
- Joins of more than two tables / complex expressions use the materialising path
  (ESQL-29).
- Intra-cluster Raft/replication traffic is authenticated (cluster secret) but not
  encrypted (ESQL-30).
- Isolation: all four standard levels are accepted; `SERIALIZABLE` and snapshot are
  the two engines (snapshot is at least as strong as `READ UNCOMMITTED`/
  `READ COMMITTED`/`REPEATABLE READ`), and `@@transaction_isolation` reports
  `REPEATABLE-READ`.

## [1.4.8] - 2026-07-21

Hardening pass from an external review — safer defaults, a query timeout, bounded
memory on an edge path, and more direct tests. No on-disk format change.

### Security

- **Safe-by-default open auth.** With no accounts configured (every client would
  be `Admin`), the server now *refuses to start* when bound to a non-loopback
  address. Override by configuring accounts, binding to localhost (the default,
  so local dev is unchanged), or setting `ELYRASQL_ALLOW_OPEN_AUTH=1`.
- **Replication exposure guard.** The replication endpoint (authenticated only
  when `ELYRASQL_CLUSTER_SECRET` is set) refuses a non-loopback bind without a
  secret unless `ELYRASQL_ALLOW_OPEN_AUTH=1`; warns loudly when unauthenticated.

### Added

- **Per-query timeout** `ELYRASQL_QUERY_TIMEOUT_MS` (0 = off): a statement running
  longer returns a clean error and unblocks the client.
- **`ELYRASQL_SERIALIZABLE_MAX_RANGE`** (default 5,000,000): a `SERIALIZABLE`
  commit whose validation would materialize a larger scanned range now aborts
  fail-safe instead of risking unbounded memory.

### Changed / Fixed

- The connection handler no longer panics on a TLS-capability mismatch; the one
  connection is dropped with a clean error.
- The replica restarts with a clear, deliberate `EX_TEMPFAIL` (75) exit on a
  resync-driven re-bootstrap instead of a bare `exit(1)`.

### Testing

- Direct unit tests for the `ORDER BY` planning helpers in `exec.rs` (previously
  covered only indirectly), plus unit tests for the bind-exposure classifier.
- Benchmarks now also run on a monthly CI schedule (still non-gating).

## [1.4.7] - 2026-07-20

Performance release: sorting on a **nullable** column now uses the secondary
index in both directions (the last grid-sort gap). Adds a companion `indexnull::`
keyspace for single-column indexes; **no change to existing on-disk data**, and
indexes built before 1.4.7 keep working (rebuild to pick up the new behaviour).

### Added

- **NULL-indexed ordered walks (removes the `ASC`-on-nullable full sort).**
  Single-column B-tree indexes built on 1.4.7+ now store NULL-keyed rows under a
  companion `indexnull::` keyspace (keyed by the clustered primary key, never
  unique). An `ORDER BY <nullable col> [ASC|DESC] LIMIT` — with or without a PK
  tiebreaker — is then a complete MySQL ordering by walking the value entries and
  the NULL entries in one snapshot: NULLs first for `ASC`, last for `DESC`, each
  ordered by the primary key. This closes the previous fallback where `ASC` on a
  nullable column with few/zero NULLs degraded to a full sort (now sub-millisecond
  at scale). Index maintenance (INSERT/UPDATE/DELETE, CREATE INDEX backfill,
  TRUNCATE/DROP/RENAME) keeps the NULL entries consistent; multiple NULLs are
  still allowed in a `UNIQUE` index. Indexes built before 1.4.7, and composite
  indexes with a nullable column, use the previous handling.

- **Primary-key tiebreaker on indexed `ORDER BY ... LIMIT`.** A non-unique
  secondary index stores `(value, clustered primary key)`, so walking it also
  orders by the trailing PK. `ORDER BY <indexed col> DESC, id DESC` — the usual
  stable-pagination sort a grid emits — is now served by the index walk instead of
  a full sort (dropped from ~6 s to sub-millisecond at scale). All order terms must
  share a direction and any trailing terms must be the primary-key columns in
  order. On a nullable column a tiebreaker only stays on the fast path when the
  NULL block is not reached (e.g. `DESC` with enough non-NULL rows); otherwise it
  falls back to the sorter, since the NULL block cannot be tiebroken cheaply.

## [1.4.6] - 2026-07-20

Performance release: deep `OFFSET` on paged grids no longer reads the skipped
rows. No on-disk format change.

### Added

- **Cheap deep `OFFSET` on indexed `ORDER BY ... LIMIT`.** With no residual
  filter, the leading `OFFSET` rows are now stepped over at the index/clustered
  level **without reading their rows** (the data key is not dereferenced), so
  paging deep into a result costs index steps rather than `offset` row reads. On
  600k rows `ORDER BY revenue LIMIT 40 OFFSET 500000` dropped from ~290 ms to
  ~18 ms; reverse-PK deep offset is likewise cheap. Applies to the primary-key and
  `NOT NULL` secondary-index walks; a residual filter still reads pre-offset rows
  (they must be counted).

### Notes

- Sorting **`ASC` on a nullable column that holds (almost) no NULLs** still falls
  back to a full sort: `ASC` places NULLs first, so the walk must establish the
  NULL block before emitting the head, and confirming an empty NULL set is not
  cheap without NULLs in the index. Declare such a column `NOT NULL` to keep it on
  the fast path in both directions. Indexing NULL keys (to remove this fallback)
  is planned as a separate, carefully-tested change.

## [1.4.5] - 2026-07-20

Performance release: nullable sort columns now use the secondary index for paged
grids (top-N without a full sort). No on-disk format change.

### Added

- **Nullable columns on indexed `ORDER BY ... LIMIT`.** A `NOT NULL` index is no
  longer required: a **nullable single-column** index now serves an ordered
  `LIMIT`, with the NULL-keyed rows (which indexes omit) spliced back in as a
  block — last for `DESC`, first for `ASC`, matching MySQL's NULL ordering. The
  NULL block is fetched by a budgeted clustered scan, so the common grid default
  (`ORDER BY <col> DESC LIMIT n` on a mostly-populated column) stays a
  sub-millisecond top-N instead of a full sort. Very rare NULLs on an `ASC` walk
  fall back to the sorter (bounded by `ELYRASQL_ORDER_SCAN_BUDGET`). Composite
  indexes still require every column to be `NOT NULL`.

## [1.4.4] - 2026-07-20

Performance release: filtered paged grids (`WHERE ... ORDER BY <col> LIMIT n`) are
now served without a full sort. No on-disk format change.

### Added

- **Filtered indexed `ORDER BY ... LIMIT`.** A `WHERE` filter is now applied as a
  residual **during** the ordered index/clustered walk, so a filtered grid page
  (`WHERE ... ORDER BY <indexed col> LIMIT n`) is served without a full sort too
  (previously only unfiltered ordered `LIMIT`s were accelerated). An examine
  budget (`ELYRASQL_ORDER_SCAN_BUDGET`) caps the walk so a very selective filter
  falls back to the memory-bounded sorter (cheap — few matches) instead of
  degrading into a near-full point-read scan. On 300k rows a `WHERE active=1
  ORDER BY revenue DESC LIMIT 40` runs in ~0.5 ms.

## [1.4.3] - 2026-07-20

Performance release: ordered `LIMIT` (paged grids) no longer sorts the whole
table. No on-disk format change.

### Added

- **Indexed `ORDER BY ... LIMIT` (top-N without a full sort).** A paged, ordered
  `LIMIT` with no `WHERE` filter is now served by an ordered walk that stops after
  `OFFSET + LIMIT` rows instead of sorting the whole table:
    - `ORDER BY <primary-key prefix> DESC LIMIT n` — reverse clustered scan
      (forward/ASC was already fast).
    - `ORDER BY <indexed column(s)> ASC|DESC LIMIT n` — ordered secondary-index
      walk, when every column of that index is `NOT NULL`.
  On a 300k-row table this took the three previously-unaccelerated grid sorts from
  ~5–8 s (full sort) to well under 1 ms. Nullable sort columns, filtered ordered
  `LIMIT`s, and ordered `LIMIT`s inside a transaction fall back to the existing
  memory-bounded sorter (correct, not yet index-accelerated).

## [1.4.2] - 2026-07-17

Analytics release: percentile aggregates and `GROUP BY` on an expression — the
pieces an observability/metrics workload needs (time-bucketed p50/p95/p99). No
on-disk format change.

### Added

- **Percentile aggregates** `PERCENTILE(col, p)` / `QUANTILE(col, p)` (fraction
  `p` in 0..1) and `MEDIAN(col)`, with exact `percentile_cont` (linear-
  interpolation) semantics — for latency percentiles (p50/p95/p99) in metrics
  workloads. Composes with `WHERE`/`GROUP BY`; an empty group is `NULL`.
- **`GROUP BY` an expression**, not just a plain column — e.g. time-bucketing
  `GROUP BY DATE_FORMAT(ts, '%Y-%m-%d %H:%i:00')` or `GROUP BY status DIV 100`.
  The projection of the same expression returns the group value. (Verified
  against MySQL in the differential suite.)

### Fixed

- Computed-column type inference now reports `DIV` as an integer and the bitwise
  operators as `BIGINT UNSIGNED` (previously text), so e.g. `SELECT n DIV 5 ...
  GROUP BY n DIV 5` returns an integer column.

## [1.4.1] - 2026-07-17

Join-streaming release. Streams two-table `RIGHT JOIN` (closing ESQL-6, the last
backlog item), so all equi-join shapes are now memory-bounded. No on-disk format
change.

### Changed

- **Streaming `RIGHT JOIN`.** A two-table `RIGHT JOIN` followed by `ORDER BY` or
  `GROUP BY` now streams (rewritten to the equivalent `LEFT JOIN` with the output
  columns reordered back to the query's `(A, B)` order), so it is bounded by the
  partner hash table plus the sorter/aggregator rather than the full join size —
  joining `INNER`/`LEFT`/`RIGHT` equi-joins on the streaming path. `FULL`,
  non-equi, derived-table, and multi-join-chain `RIGHT` joins still use the
  correct materialising path.

## [1.4.0] - 2026-07-16

Search release. Completes the search chapter with faceted counts, reusing the
same engine as full-text and vector search. No on-disk format change.

### Added

- **Faceted search** via a `FACET(col[, top_n])` aggregate. It returns a JSON
  object of `{value: count}` over the matched rows (ordered by count, optional
  top-N cap), computing every facet plus the hit count in a **single pass**. As
  an ordinary aggregate it composes with `WHERE`, full-text `MATCH ... AGAINST`,
  vector filters and `GROUP BY` — the counts side of a faceted search, reusing
  the same engine as full-text and vector search. Works in the server and the
  embedded engine.

## [1.3.0] - 2026-07-16

Access-control release. Enforces individual DML privileges per table, closing the
last documented gap in the privilege model. No on-disk format change (legacy
per-table grants upgrade in place).

### Changed (security)

- **Fine-grained privilege enforcement.** The individual DML privileges `INSERT`,
  `UPDATE` and `DELETE` are now enforced separately, per target table, instead of
  a single coarse "write" tier. A user granted only `INSERT` can no longer
  `UPDATE` or `DELETE`, and `REVOKE`ing one write privilege leaves the others
  intact. Per-table grants are stored as a privilege set (legacy grants migrate
  automatically); role-inherited grants are included. Admin/open-auth connections
  are unaffected (full access), reads remain allowed at the baseline, and DDL is
  still gated at the `ADMIN` tier.

## [1.2.0] - 2026-07-15

MySQL-semantics release. Adds an automated **differential test harness** that runs
180 edge-case queries against ElyraSQL and a real MySQL 8 in CI, and fixes the
correctness divergences it surfaced. No on-disk format change from 1.1.x.

### Added

- **MySQL differential harness** (`tests/compat/differential/mysql_diff.py`) and a
  CI workflow (`mysql:8.4` service) that fail on any non-allowlisted divergence in
  rows, NULLs, or error/no-error — a permanent guard against MySQL-semantics
  regressions.
- New functions: `ISNULL`, `STRCMP`, `BIT_COUNT`, `TO_DAYS`, `INSERT`, `CONV`,
  `ORD`, `BIN`, `OCT`, `CRC32`.
- The `DIV` integer-division operator and the `!` logical-NOT prefix operator.

### Fixed (MySQL semantics)

- **NULL propagation:** arithmetic with a NULL operand (`NULL + 1`) returned an
  error → now NULL. `NOT NULL` / `!NULL` → NULL.
- **Three-valued logic:** `AND`/`OR` (`NULL AND 1` → NULL, not 0), `IN`
  (`1 IN (NULL, 2)` → NULL), and `BETWEEN` (`1 BETWEEN NULL AND 5` → NULL) now
  follow SQL three-valued logic.
- **Math domain errors** (`SQRT(-1)`, `LN(0)`, `LN(-1)`) return NULL instead of
  NaN/inf.
- **`LENGTH`** now returns the byte length (`CHAR_LENGTH` stays characters);
  `SUBSTRING(s, 0)` returns `''`.
- **`CAST` to integer** rounds instead of truncating (`CAST(3.7 AS SIGNED)` = 4);
  `UNSIGNED` wraps (`CAST(-1 AS UNSIGNED)` = 18446744073709551615); non-numeric
  text casts to its leading integer prefix (or 0).
- **Invalid dates are rejected** rather than rolled over: `CAST('2024-02-30' AS
  DATE)` → NULL (also affects date parsing generally).
- **`DATE_ADD`/date + interval** on a time-less date yields a `DATE`, not a
  `DATETIME`.
- **Integer division `DIV`** truncates toward zero; `DIV 0` → NULL.
- **Bit aggregates** `BIT_OR`/`BIT_AND`/`BIT_XOR` return `BIGINT UNSIGNED`.

### Notes

- A few divergences are intentional and documented in the harness allowlist:
  ElyraSQL is stricter about implicit string→number coercion in arithmetic and
  comparison (`0 = 'abc'` is 0, not 1), and it does not replicate MySQL's bare
  `!!x` quirk (it treats `!!x` as consistent double negation). `DECIMAL`/`TIME`
  results are sent as text (values identical).

## [1.1.3] - 2026-07-14

Security release. Completes the expression-depth denial-of-service guard first
shipped in 1.1.1, which missed two attack shapes. No on-disk format change from
1.1.x. Upgrading from 1.1.0/1.1.1/1.1.2 is strongly recommended.

### Security

- **Completed the expression-depth DoS guard from 1.1.1.** The initial guard
  estimated AST depth from a hand-picked set of operator tokens and tracked only
  open-bracket nesting, so it **missed** two shapes that still overflowed the
  worker stack and aborted the process: JSON `->`/`->>` chains
  (`x -> '$' -> '$' ...`) and token-balanced *postfix* chains
  (`x[0][0]...`, `f()()...`). The guard now treats **every** operator token the
  tokenizer can emit as depth-contributing and accumulates depth when a
  group/subscript/call closes, so all deep-AST shapes are rejected before parsing.
  A statement separator (`;`) resets the estimate so multi-statement batches of
  shallow statements aren't falsely rejected. Verified against arrow, longarrow,
  subscript, call, paren, function-nesting, arithmetic, boolean and bitwise chains
  at 300k terms (all rejected; server stays alive).

## [1.1.2] - 2026-07-14

Correctness release for integer/floating-point arithmetic. No on-disk format
change from 1.1.x.

### Fixed

- **Integer arithmetic no longer silently saturates** ([#15]). Signed 64-bit
  arithmetic was evaluated in `f64` and cast back, so a result past the `BIGINT`
  range was silently clamped (e.g. `9223372036854775807 + 1` returned
  `9223372036854775807`) — a correctness/data-integrity foot-gun for computed
  writes. Integer `+`, `-`, `*` (and unary `-`) are now computed exactly and raise
  `ERROR 1690 (22003) BIGINT value is out of range` on overflow, matching MySQL,
  in both the scalar and row (`WHERE`/`UPDATE`) paths.
- **`x % 0` now returns `NULL`** (was `0`), matching MySQL, for both `%` and
  `MOD()` — consistent with `x / 0`, which already returned `NULL`.
- **`DOUBLE` overflow now returns `NULL`** instead of `inf`/`NaN` (e.g.
  `POW(10,308) * 10`), matching MySQL's out-of-range behaviour.

## [1.1.1] - 2026-07-14

Security release. Fixes two denial-of-service issues in the same class (unbounded
recursion on hostile input → worker-stack overflow → process abort). No on-disk
format change from 1.1.0. Upgrading is strongly recommended.

### Security

- **Fixed a remote denial-of-service** (reported privately). A single query with a
  deeply-nested flat expression — e.g. `SELECT 1+1+1...` or
  `... WHERE id=1 OR id=1 OR ...` with tens of thousands of terms — built a
  left-deep AST whose depth is O(N). Evaluating it, and even *dropping* it,
  recursed O(N) frames deep and overflowed the worker thread stack, which aborted
  the **entire server process** (dropping every client at once), not just the
  offending connection. Unauthenticated in the default open-auth (dev) mode; any
  authenticated user otherwise. ElyraSQL now rejects over-deep expressions with a
  normal SQL error **before parsing** (so the pathological AST is never built —
  the parser, evaluator, and AST destructor never recurse unboundedly). The limit
  is configurable via `ELYRASQL_MAX_EXPR_DEPTH` (default 2000). Wide-but-shallow
  queries (long `IN` lists, large multi-row `INSERT`s) are unaffected.
- **Fixed a related JSON denial-of-service** found while auditing for siblings of
  the above. A deeply-nested JSON document (`[[[[...]]]]`, ~200k levels) passed to
  a JSON function such as `JSON_VALID`/`JSON_EXTRACT` recursed through the JSON
  parser (and the value's recursive destructor) and overflowed the worker stack,
  again aborting the whole process. The JSON parser now enforces a maximum nesting
  depth (200 levels, matching the on-write validator); an over-deep document is
  treated as invalid JSON instead of crashing.

## [1.1.0] - 2026-07-14

Robustness release. Adds a soak/chaos test harness and, on its first run, fixes a
real isolation bug it uncovered. No on-disk format change from 1.0.

### Fixed

- **Snapshot-consistent autocommit aggregates.** A single autocommit aggregate
  (e.g. `SELECT SUM(x) FROM t`) could, under concurrent writes, return a value
  that never existed in any consistent state — because the parallel and batched
  aggregate scan paths each opened their *own* MVCC read snapshot, so different
  parts of one aggregate observed the table at different commit points. Every
  single statement now reads through **one** pinned snapshot: the parallel
  clustered-range scans, the `COUNT(*)` fast path, and the spilling
  (`partitioned`) aggregation all share a single point-in-time view. This
  restores snapshot isolation for autocommit aggregate reads. (In-transaction
  aggregation already read the session snapshot and was unaffected.)

### Testing

- **Soak / chaos harness** (`crates/elyra-cli/tests/soak.rs`). Many concurrent
  connections run atomic transfers against a fixed-total set of accounts while a
  global bank invariant — total balance conserved, never negative — is checked
  continuously. A second test repeatedly `SIGKILL`s and restarts the server
  mid-write and re-checks the invariant after every crash-recovery, exercising
  crash consistency under sustained load. Short by default so it runs per-PR;
  env-tunable (`ELYRASQL_SOAK_SECS`/`WORKERS`/`ACCOUNTS`/`KILL_MS`) with a nightly
  workflow for long runs. This harness found the aggregate-isolation bug above on
  its first CI run.

### Notes

- Cross-engine benchmarks were re-run on the fair native-Linux environment and
  are unchanged by the isolation fix — ElyraSQL remains fastest of the three on
  every aggregation query.

## [1.0.0] - 2026-07-13

First stable release. ElyraSQL is a robust, MySQL-compatible SQL server in Rust:
a single ACID file, a broad SQL surface, vector + full-text + hybrid search, and
parallel OLAP aggregation. This release closes a wave of correctness, robustness
and compatibility work and commits to Semantic Versioning from here on. No
on-disk format change from 0.9.x (`.edb` files upgrade in place).

### Correctness fixes

- **`SELECT DISTINCT` now deduplicates.** It was previously a no-op on the base
  scan path (returned duplicate rows); it now dedups on the projected output,
  before `OFFSET`/`LIMIT`, and is collation-aware.
- **Native (binary) prepared statements** no longer desync across repeated
  `COM_STMT_PREPARE` on one connection. Root-caused to a use-after-free and a
  buffer-padding bug in the wire packet reader when a client (e.g. PDO/mysqlnd)
  pipelines commands. PDO with `EMULATE_PREPARES=false` now works.
- **Process-global catalog cache** is keyed by `(database, table)`, so multiple
  databases in one process can't serve each other's schema.
- Fixed a UTF-8 slicing panic in `UPDATE`/`DELETE` `LIMIT` stripping (found by
  the new fuzzer).

### SQL surface

- **`BIGINT UNSIGNED`** is a first-class type (`Value::UInt`): columns store and
  read values above `i64::MAX` exactly, and all bitwise operators (`&` `|` `^`
  `<<` `>>` and unary `~`) return correct 64-bit unsigned results with exact
  unsigned arithmetic.
- **`GROUP BY ... WITH ROLLUP`** — subtotal + grand-total rows, re-aggregated per
  level so `AVG`/`MIN`/`MAX` stay correct.
- **`INSERT ... SET col = val`** and **comma-style multi-table `UPDATE`** are
  accepted (rewritten to the standard forms).
- **Per-column `_bin`/`BINARY` collation** is honored in `ORDER BY`, `GROUP BY`,
  `DISTINCT` and equi-join keys (not just `WHERE`/`UNIQUE`/indexes).
- **`ENUM`/`SET` value validation** — a non-member value is rejected.
- **Qualified wildcard `alias.*`** in the projection.

### Performance / robustness

- **Streaming joins.** `INNER`/`LEFT` joins — explicit, comma, and N-table
  left-deep chains — followed by `ORDER BY` or `GROUP BY` stream the driving
  table through a spilling sorter/aggregator, so a large fact-to-dimensions join
  is bounded by group/sort state, not the full join output size.
- **Native prepared statements**: `describe_query` reports an exact result-column
  count (incl. `*` over joins) at `PREPARE`.

### Testing

- End-to-end wire integration tests (independent `mysql_async` driver),
  crash-recovery/durability tests, a committed Laravel/Eloquent + PyMySQL +
  native-PDO compatibility harness in CI, property tests (value round-trips,
  aggregation/ORDER BY invariants), and a `cargo-fuzz` target for the
  preprocessing+parse pipeline. All gated in CI.

### Notes

- Deferred (documented in [limitations](docs/limitations.md)): streaming
  `RIGHT`/`FULL`/non-equi/derived-table joins (the materialising path is correct;
  only a rare OOM risk), and pre-commit 2-phase replication.

## [0.9.9] - 2026-07-12

Wire-protocol release. ElyraSQL now owns its MySQL wire layer, which unblocked
three things a third-party dependency held back. No on-disk format change.

### First-party wire layer

- Forked `opensrv-mysql` into the in-tree **`elyra-wire`** crate (Apache-2.0,
  attribution preserved). ElyraSQL now maintains and extends its own MySQL
  wire-protocol implementation instead of depending on an unmaintained upstream.

### TLS: rustls 0.23

- Server TLS moved from rustls 0.22 to **rustls 0.23** (via `tokio-rustls`
  0.26), using the pure-Rust *ring* provider (no aws-lc/OpenSSL; static musl
  builds keep working). `rustls-webpki` is now 0.103.13, so the four RUSTSEC
  webpki advisories no longer apply. Note: rustls 0.23 requires X.509 **v3**
  certificates (all modern/CA-issued certs qualify).

### Authentication: caching_sha2_password

- Implemented **`caching_sha2_password`** (MySQL 8's default auth plugin),
  opt-in via `ELYRASQL_AUTH_PLUGIN=caching_sha2_password`. Full authentication
  runs over TLS (cleartext) or a plaintext connection (RSA-OAEP public-key
  exchange, 2048-bit *ring* key generated on first use); the recovered password
  is checked against the existing `SHA1(SHA1(pw))` digest, so no credential
  storage change and the password is never persisted in the clear. The default
  stays `mysql_native_password` (works with every client). The full
  Laravel/Eloquent suite passes authenticating with caching_sha2_password.

### Native prepared statements

- `describe_query` is now **count-complete**: it reports an exact result-column
  count (with best-effort types) at `PREPARE` for any single SELECT with an
  explicit projection, so binary (native) prepared-statement drivers read the
  result set instead of desyncing. Emulated/client-side prepares remain the
  recommended setting for the widest compatibility.

### Notes

- The `rsa` crate's Marvin timing advisory (RUSTSEC-2023-0071, no fixed release)
  is documented and scoped in `.cargo/audit.toml`: RSA runs once per connection
  in the opt-in non-TLS caching_sha2 path only; TLS or native_password avoid it.
- dependabot now pins `nom`/`mysql_common` (the vendored wire crate uses their
  current APIs).

## [0.9.8] - 2026-07-12

MySQL-compatibility release, driven by running real MySQL clients and the
**Laravel/Eloquent** stack against ElyraSQL and closing every gap that surfaced.
No on-disk format change.

### Laravel / framework support

- A full Laravel Eloquent workload runs cleanly: migrations (`Schema::create`
  with `$table->id()`, `foreignId()->constrained()`, indexes), model CRUD with
  correct `lastInsertId`, `hasMany`/`belongsTo`, eager loading, `withCount`,
  query-builder joins/aggregates/`groupBy`+`having`, `updateOrInsert`,
  transactions, and cascading deletes.
- New **[Framework Integration](https://elyracode.com/sql/server/frameworks/)**
  guide with recommended settings for Laravel, PDO/Symfony, Python (PyMySQL/
  Django/SQLAlchemy), Rust (sqlx) and Node (mysql2).
- CREATE TABLE now tolerates trailing table options (`ENGINE=`, `DEFAULT
  CHARSET`/`CHARACTER SET`, `COLLATE '...'`, `AUTO_INCREMENT=`, `ROW_FORMAT`,
  `COMMENT`, ...) so Laravel/mysqldump/ORM DDL parses.
- `ALTER TABLE ADD FOREIGN KEY`/`ADD INDEX`/`KEY`/`UNIQUE` (with backfill).
- Unsigned and extended column types (`BIGINT UNSIGNED`, `MEDIUMINT`, `DOUBLE
  PRECISION`, `TINY/MEDIUM/LONGTEXT`+`BLOB`, `NVARCHAR`, ...).
- `information_schema.columns` reports `COLLATION_NAME`, `COLUMN_COMMENT`,
  `GENERATION_EXPRESSION`, `CHARACTER_SET_NAME` (schema introspection).
- The OK packet now sets `SERVER_STATUS_IN_TRANS`, so `PDO::inTransaction()` and
  `commit`/`rollBack` behave correctly (transactions were silently
  auto-committing before). The OK packet also carries `last_insert_id`, so
  driver `lastrowid`/`getGeneratedKeys` work after `INSERT`.

### SQL surface

- Session functions `LAST_INSERT_ID()`, `ROW_COUNT()`, `FOUND_ROWS()`.
- `@@`system variables (`@@version`, `@@session.*`, `@@global.*`, `sql_mode`,
  `character_set_*`, ...); unknown ones return NULL.
- Operators: `<=>` (null-safe equal), `IS [NOT] TRUE/FALSE/UNKNOWN`, row/tuple
  `IN` (`(a,b) IN ((...),(...))`).
- Subqueries in the SELECT list (scalar, `EXISTS`), including alongside `t.*`.
- `HAVING` referencing aggregates not in the SELECT list.
- Scalar functions: `MD5`/`SHA1`/`SHA2`, `HEX`/`UNHEX`, `FORMAT`, `FIND_IN_SET`,
  `FROM_UNIXTIME`, `DAYNAME`/`MONTHNAME`, `PI`/`RADIANS`/`DEGREES`, `CHAR`,
  `TIME_TO_SEC`/`SEC_TO_TIME`, `SOUNDEX`, `REGEXP_REPLACE`/`REGEXP_SUBSTR`,
  `CONVERT()`.
- Aggregates: `STDDEV`/`STDDEV_POP`/`STDDEV_SAMP`, `VARIANCE`/`VAR_POP`/
  `VAR_SAMP`, `BIT_OR`/`BIT_AND`/`BIT_XOR`.
- Window functions: `NTILE`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`.
- `UPDATE`/`DELETE ... LIMIT n` is accepted (the row limit is not enforced).

### Known limitations

- Binary (native) prepared-statement parameter binding is not yet reliable with
  PDO/mysqlnd; use client-side/emulated prepares (`PDO::ATTR_EMULATE_PREPARES
  => true`). PyMySQL and sqlx bind client-side and are unaffected.
- Parser-level: `INSERT ... SET`, comma multi-table `UPDATE`, `GROUP BY ... WITH
  ROLLUP`, and the `<<`/`>>`/`~` bitwise operators are not parsed.

## [0.9.7] - 2026-07-12

OLAP acceleration release. No on-disk format change; fully compatible with
0.9.3–0.9.6 data files. The default behaviour is unchanged — every new
accelerator below is opt-in.

### Query performance (always on)

- **Vectorised (columnar) grouped aggregation.** `GROUP BY` on a single numeric
  column with numeric aggregates keys each group exactly in an FxHash map and
  accumulates into flat per-group `f64`/`i64` arrays, decoding only the needed
  columns — no byte-key encoding or per-row `Value` dispatch. A pushed-down
  compiled predicate filters on the same path. On native Linux (1M rows):
  `GROUP BY` top-10 93→54 ms (≈1.6× ahead of PostgreSQL), low-cardinality
  64→46 ms, filtered aggregation 53→46 ms.
- **Single-pass hybrid `GROUP BY` spill.** Aggregation now keeps groups in memory
  and spills *only the rows whose group does not fit* to disk partitions,
  instead of routing every row through disk. When the working set fits, nothing
  spills.
- **Streaming index nested-loop join.** `FROM a JOIN b ON a.k = b.<indexed>
  [WHERE …] LIMIT n` (no GROUP BY/aggregate/ORDER BY/DISTINCT) scans the driving
  table incrementally, probes the indexed partner per row, and stops as soon as
  enough rows are produced — bounded memory, early termination (e.g. `LIMIT 5`
  over 100k driving rows in ~0.5 ms).

### Opt-in accelerators

- **`ELYRASQL_SYNC`** — commit durability. `full` (default) fsyncs every commit;
  `normal` returns before the fsync and flushes in the background
  (`ELYRASQL_SYNC_INTERVAL_MS`, default 200 ms), greatly increasing small-batch
  `INSERT` throughput (~14× on single-row autocommit inserts) for a bounded
  crash-loss window. Never risks corruption; same tradeoff as MySQL
  `innodb_flush_log_at_trx_commit=2` / PostgreSQL `synchronous_commit=off`.
- **`ELYRASQL_COLUMN_CACHE_MB`** — in-memory columnar cache (default 0 = off) for
  repeated **unfiltered** aggregations: a table's numeric columns are
  materialised once and reused, skipping the scan (cached 4-aggregate scalar
  over 200k rows ~0.8 ms).
- **`ELYRASQL_ZONE_MAPS`** — data-skipping for **filtered** aggregations (default
  off): per-chunk column min/max let a `WHERE col <op> value` skip blocks that
  cannot match. Big win for data with locality (time-series, monotonic ids);
  selective filter on 500k rows ~2.2× faster.

  All three are race-free by construction: a monotonic write sequence written
  *inside every write transaction* invalidates cached state on any committed
  write (insert/update/delete, COMMIT, replication, DDL), so they never serve
  stale data. Filtered aggregations still run the predicate on every surviving
  row, so zone maps never affect correctness.

### Security / tooling

- `cargo audit` now runs in CI, with `.cargo/audit.toml` documenting each
  reviewed advisory (the rustls-webpki chain is transitive via opensrv-mysql's
  rustls 0.22 and unreachable server-side).
- Compatible dependency updates.

## [0.9.6] - 2026-07-12

OLAP performance release. No on-disk format change; fully compatible with
0.9.3–0.9.5 data files.

Headline result (native Linux, all engines on one host — see
`benchmark_analyse.md`): on 1M rows, **ElyraSQL is the fastest of ElyraSQL,
PostgreSQL 17 and MySQL 8.4 on every OLAP query** — global aggregation, low- and
high-cardinality `GROUP BY`, top-N, and filtered aggregation — and 2–5× ahead of
MySQL.

### OLAP

- **Vectorised (columnar) scalar aggregation.** Multi-aggregate queries without
  `GROUP BY` over numeric columns extract each column into a contiguous `f64`
  array per batch and aggregate with tight, SIMD-friendly loops instead of
  per-row `Value` dispatch. `SUM/AVG/MIN/MAX` over 1M rows ≈ halved.
- **Compiled filter predicate.** A `WHERE` that is a conjunction of
  `column <cmp> numeric-literal` is compiled once with pre-resolved column
  indices and evaluated with native comparisons, instead of re-resolving column
  names and walking the expression per row. Filtered aggregation on 1M rows
  dropped from ~87 ms to ~53 ms (now ahead of PostgreSQL).
- **Fast bare `COUNT(*)`.** Counts keys across parallel clustered ranges without
  decoding row values, and seeds the result directly (~24 ms → ~8 ms locally).
- **`ELYRASQL_AGG_WORKERS`** tunes aggregation parallelism (default min(cores, 4);
  aggregation is memory-bandwidth bound, so more workers can be slower).

### Tooling

- `bench/olap.py` — OLAP benchmark harness (1M-row analytical queries).
- `.github/workflows/benchmark.yml` — native-Linux CI benchmark against MySQL
  and PostgreSQL; run with `gh workflow run benchmark.yml`. This is the fair,
  representative environment (a laptop hypervisor penalises ElyraSQL's parallel,
  memory-mapped scans).
- `benchmark_analyse.md` refreshed with the native-Linux OLAP + core-SQL results.

## [0.9.5] - 2026-07-12

Performance release focused on aggregation. No on-disk format change; fully
compatible with 0.9.3/0.9.4 data files.

Headline result (200k rows, each engine measured alone on the box): ElyraSQL is
now the **fastest of the four on full-table `COUNT` and `GROUP BY`** — ahead of
MySQL 8.4, Percona 8.4 and PostgreSQL 17. See `benchmark_analyse.md`.

### Aggregation

- **Bounded table-keyspace scan for parallel-split planning.** The planner that
  splits a table for parallel aggregation was walking backwards through the
  *entire* database (every secondary-index entry and other table) to find a
  table's last row, making full-scan `COUNT`/`SUM`/`GROUP BY` scale with total
  database size rather than table size. It now bounds the probe to the table's
  own keyspace. On a 200k-row table sharing the file with another 200k table and
  two secondary indexes, `GROUP BY` dropped from ~17 ms to ~4.4 ms.
- **Aggregation parallelism capped at 4 by default** (`ELYRASQL_AGG_WORKERS`
  overrides). Full-scan aggregation is memory-bandwidth bound; beyond ~4 workers
  the coordination overhead makes it slower.
- **Allocation-light grouping.** The group-by aggregator uses an insertion-
  ordered map (one key allocation per group instead of two) and moves group
  state during parallel merges instead of cloning it — markedly faster
  high-cardinality `GROUP BY`.

### Result transfer

- **Buffered wire writes** (plain connections). The MySQL protocol writer issued
  one `write_vectored` syscall per result row against an unbuffered socket; a
  64 KiB buffer now coalesces rows, helping any query that returns many rows to
  a fast client.

### Tuning

- `ELYRASQL_AGG_WORKERS` — degree of parallelism for full-scan aggregation
  (1 = single-threaded; default min(cores, 4)).

## [0.9.4] - 2026-07-12

Performance release: a focused campaign on scan, aggregation and per-query
overhead, benchmarked head-to-head against MySQL 8.4, Percona 8.4 and
PostgreSQL 17 (see `benchmark_analyse.md`). No on-disk format change; fully
compatible with 0.9.3 data files.

Headline result (200k rows, same host/client): ElyraSQL now **beats MySQL and
Percona on full-table `COUNT` and bulk insert**, **matches PostgreSQL on
full-scan `COUNT`**, and is competitive on indexed `COUNT` and range
`ORDER BY`. Full-scan `COUNT` improved ~10x (48 ms -> ~5 ms) and
`ORDER BY pk LIMIT` ~50x (29 ms -> ~0.6 ms) versus 0.9.3.

### Query engine

- **PK-ordered `LIMIT` fast path** — `ORDER BY <pk> LIMIT n` scans in clustered
  order and stops as soon as enough rows are collected, instead of
  materialising and sorting the whole result set.
- **Projection-aware decoding** — scans materialise only the columns a query
  actually reads, skipping (without allocating) `TEXT`/`JSON` columns it never
  touches.
- **Zero-copy scanning** — full-table scans decode straight from borrowed
  storage bytes inside a single read transaction, with a reused row buffer, so
  there is no per-row copy or allocation.
- **Parallel clustered aggregation** — for integer-primary-key tables, a
  full-scan aggregate splits the keyspace into ranges aggregated in parallel.
- **Covering-index `COUNT`** — `COUNT(*)` whose filter is an equality covered by
  an index is answered by counting index entries, with no row fetch.
- **Faster `GROUP BY`** — an allocation-free group-key hot path with a fast
  (FxHash) aggregation map.

### Per-query overhead

- **Table-definition cache** — autocommit queries resolve their schema from an
  in-memory cache (epoch-invalidated on DDL) instead of reading it from storage
  every time.
- **Common-path check elimination** — materialized-view refresh checks,
  per-column mask lookups, and redundant privilege lookups are skipped entirely
  unless the corresponding feature is actually in use.

### Tooling

- `bench/compare.py` — identical portable workload across ElyraSQL, MySQL,
  Percona and PostgreSQL.
- `benchmark_analyse.md` — the 0.9.4 cross-engine comparison and analysis.

## [0.9.3] - 2026-07-10

AI-native search release: hybrid full-text + vector retrieval and in-SQL
embedding generation — the RAG/AI-app stack in one MySQL-compatible file, no
external search engine. No on-disk format change.

### Hybrid search

- **`HYBRID(text_col, 'query', vec_col, vector)`** — a first-class ranking
  primitive that fuses a **vector** (HNSW) ranking and a **full-text** ranking
  with **Reciprocal Rank Fusion** (RRF, k=60), honouring the query's structured
  `WHERE` filter:

  ```sql
  SELECT id, title, HYBRID(body, 'data privacy', embedding, ?) AS score
  FROM docs WHERE lang = 'en' ORDER BY score DESC LIMIT 10;
  ```

  The text side uses a `FULLTEXT` index when present (otherwise a scan), the
  vector side uses the HNSW index, and the fused relevance is exposed via the
  projection alias. One query, one file — no Elasticsearch/pgvector/reranker.

### In-SQL embeddings

- **`ai_embed('text')`** — calls an OpenAI-compatible `/v1/embeddings` endpoint
  (cloud, or a local Ollama/LM Studio/llama.cpp/vLLM server) and returns the
  vector, so query vectors and stored values are generated directly in SQL:

  ```sql
  SELECT id, HYBRID(body, 'privacy', embedding, ai_embed('privacy')) AS score
  FROM docs ORDER BY score DESC LIMIT 10;
  INSERT INTO docs VALUES (1, 'some text', ai_embed('some text'));
  ```

  Resolved in an async pre-pass (each unique text embedded once and cached, then
  treated as a vector literal), so all downstream vector operations are
  unchanged. Configured via `ELYRASQL_AI_EMBED_URL` / `_KEY` / `_MODEL`. HTTP via
  `ureq` + `ring`, so the static musl builds keep working. Constant arguments
  only (`ai_embed('query')`); per-row `ai_embed(column)` is future work.

## [0.9.2] - 2026-07-10

MySQL client & driver compatibility release, driven by testing real GUI tools
and a Rust `sqlx` client against ElyraSQL. No on-disk format change.

### Query engine

- **`LIKE` / `ILIKE` in `WHERE`** are now supported (they were rejected before):
  `%`/`_` wildcards, `ESCAPE`, `NOT LIKE`, case-insensitive under the default
  collation — so contains/prefix search works.
- **Numeric/string comparison coercion**: comparing a numeric column to a string
  literal (`id = '5'`, `IN ('5','6')`, `price = '10.50'`, `id > '4'`) now coerces
  per MySQL rules. This also fixes **bound parameters not matching numeric
  columns** (drivers render params as string literals).
- **Expressions over aggregates**: `ROUND(SUM(x),2)`, `SUM(a)/COUNT(*)`,
  `SUM(qty*price)`, `COALESCE(SUM(x),0)+n`, and scalar expressions over group
  columns like `UPPER(status)` — with or without `GROUP BY`.
- **Positional `ORDER BY`** (`ORDER BY 2`, `ORDER BY 1 DESC`).
- **`VERSION()`, `DATABASE()`, `USER()`, `CURRENT_USER()`, `CONNECTION_ID()`,
  `CURRENT_ROLE()`** work as scalar functions in any context (not just as an
  exact-match intercept).
- `CREATE`/`DROP DATABASE` and `SCHEMA` are accepted as no-ops (single-file
  database), so tools and migrations that issue them proceed.

### Introspection

- **`information_schema`**: added `engines`, `schemata`, `views`, `events`,
  `routines`, `triggers`; `KEY_COLUMN_USAGE` gained `POSITION_IN_UNIQUE_
  CONSTRAINT` and `REFERENCED_TABLE_SCHEMA`/`NAME`/`COLUMN_NAME` (foreign-key
  discovery). Database name unified to `elyra`.
- **`SHOW`**: `VARIABLES`, `STATUS`, `COLLATION`, `DATABASES`, `WARNINGS`,
  `TABLE STATUS`, `FUNCTION`/`PROCEDURE STATUS` (incl. the `WHERE` form), and
  `PROCESSLIST` (now handled in-engine, so it works over the prepared path too).
- `mysql.user` lists accounts (always including the built-in `root`).

### Drivers

- **Opt-in prepared-statement column description** (`ELYRASQL_STMT_DESCRIBE`,
  default off): describes a simple `SELECT`'s result columns at `PREPARE` time
  so drivers like **sqlx** resolve result columns **by name**. Off by default
  because strict `libmysqlclient`-based clients mishandle it; verified with a
  real sqlx harness that it enables by-name resolution and survives multiple
  prepares on one connection.

## [0.9.1] - 2026-07-10

MySQL client compatibility release. Real GUI tools (DBeaver, Workbench) and
drivers fire a cluster of introspection queries on connect and to populate their
schema tree; ElyraSQL now answers them, and a few everyday query forms that
errored now work. No on-disk format change.

### Session / introspection queries

- `SHOW [GLOBAL|SESSION] VARIABLES [LIKE ...]` returns a MySQL 8.0-compatible
  system-variable set (character sets, collations, timeouts,
  `max_allowed_packet`, `lower_case_table_names`, `sql_mode`, `version*`, ...)
  with `LIKE` filtering.
- `SHOW STATUS`, `SHOW COLLATION`, `SHOW DATABASES`, `SHOW WARNINGS`/`ERRORS`,
  `SHOW TABLE STATUS`, and `SHOW FUNCTION`/`PROCEDURE STATUS` (including the
  `WHERE` form, which the SQL parser rejects, handled pre-parse).

### `information_schema` virtual tables

- Added `engines`, `schemata`, `views`, `events`, `routines`, and `triggers`
  (views/routines/triggers reflect the actual stored objects). The reported
  database name is now consistently `elyra` (matching `TABLE_SCHEMA` and
  `Tables_in_elyra`).

### Query engine

- **Expressions over aggregates**: `ROUND(SUM(x), 2)`, `SUM(a)/COUNT(*)`,
  `SUM(qty*price)`, `COALESCE(SUM(x), 0) + n`, and scalar expressions over group
  columns like `UPPER(status)` — with or without `GROUP BY`, and over an empty
  input (yields `NULL`). Previously these errored.
- **Positional `ORDER BY`** (`ORDER BY 2`, `ORDER BY 1 DESC`) in both the
  aggregated and plain paths.

### Dependencies

- Applied safe in-range dependency updates; pinned crates that define the
  on-disk format / SQL parsing / SIMD API (bincode, redb, sqlparser, wide) and
  the opensrv-pinned rustls stack against breaking Dependabot bumps. Bumped the
  Alpine runtime image and GitHub Actions. The four `rustls-webpki` advisories
  are not reachable (server-only TLS, no client-cert/CRL validation) and are
  transitively pinned by `opensrv-mysql`.

## [0.9.0] - 2026-07-10

Robustness, correctness & security hardening release. A broad review of the
query engine, transaction layer, vector search, privilege model and network/
disk I/O, tightening production safety without changing the on-disk format.

### Correctness

- **Signed-zero / NaN grouping.** `GROUP BY`, `DISTINCT` and hash joins now
  canonicalize float keys, so `-0.0` and `+0.0` group together (as SQL requires)
  and all NaNs collapse to one key.
- **Total ordering.** `Value::total_cmp`'s fallback no longer compares `Debug`
  strings (which allocated per comparison and sorted `10.0` before `2.0`); it
  uses a numeric / stable per-type order.
- **Full-text stemming.** Replaced the ad-hoc suffix stripper (which mangled
  `string`→`str`, `running`→`runn`) with the **Snowball** algorithms
  (`rust-stemmers`); multilingual via `ELYRASQL_FULLTEXT_LANGUAGE` (default
  `english`; `none` disables stemming).
- **Transaction ORDER BY** now uses the disk-spilling sorter *inside*
  transactions too (via the snapshot+overlay cursor), not just in autocommit.
- **GROUP BY** consults column statistics to go straight to the spilling path
  when a large group count is predicted, avoiding a wasted in-memory pass and
  re-scan (run `ANALYZE TABLE` to benefit).

### Stability

- **JSON validator depth limit** (`MAX_JSON_DEPTH`) stops deeply nested input
  from overflowing the thread stack.
- **O(1) savepoints.** `SAVEPOINT` records an undo-log marker instead of cloning
  the whole staged write set (previously O(writes × savepoints)); `ROLLBACK TO`
  reverts only changes since the savepoint.
- **Bounded transaction buffer.** Uncommitted writes past `ELYRASQL_TXN_MAX_BYTES`
  (default 1 GiB) are rejected with an error instead of exhausting memory.
- **Single-flight vector index rebuilds.** A burst of queries after a write now
  triggers exactly one HNSW rebuild while the rest await and share it, instead
  of a thundering-herd of parallel full-table scans.
- **Temp-file hygiene.** Sort/aggregation spill files are size-guarded on read
  (a corrupt file can't trigger a giant allocation) and stale files from a
  SIGKILLed process are reclaimed at startup (only confirmed-dead PIDs).

### Security

- **Fine-grained global privileges.** `GRANT`/`REVOKE ON *.*` now add/remove
  individual privileges as a set, so revoking one privilege no longer collapses
  an admin account to read-only. `SHOW GRANTS` lists the exact set.
- **DROP USER** purges the account's global, per-table, per-column and role
  grants, so a recreated same-name user can't inherit stale privileges.
- **Constant-time password comparison** (`ct_eq`) closes a hash timing side
  channel.
- **Bounded frame/record reads.** Every length-prefixed read (cluster,
  replication, binlog, spill files) rejects oversized lengths before allocating,
  via the configurable `ELYRASQL_MAX_FRAME_MB` (default 1024 MiB), turning a
  corrupt file or malicious packet into an error instead of an OOM crash.

### Performance

- **HNSW visited-set pooling** removes an O(N) heap allocation per vector search.
- **SIMD distance kernels** (`wide::f32x8`, 8-wide) accelerate L2 / inner-product
  / cosine on the hot ANN path.
- **Cooperative yielding** (`yield_now`) in stored-procedure `WHILE`/`LOOP`/
  `REPEAT` loops keeps a long procedure from starving the async runtime.

### Known limitations (documented)

- Multi-table joins still materialize before sort/group (streaming join output
  is planned); an unanalyzed high-cardinality `GROUP BY` may still fall back with
  a second scan; internal cluster/replication traffic is authenticated
  (`ELYRASQL_CLUSTER_SECRET`) but not yet encrypted (mTLS is planned — use a
  private network/VPN meanwhile).

## [0.8.10] - 2026-07-10

Consensus hardening & security release — making the Raft write path production-
viable and strengthening password handling.

### Raft write-path throughput

- The leader holds **persistent `AppendEntries` connections** to followers
  (reused across rounds, `TCP_NODELAY`) instead of a fresh connection per round.
- The Raft log is now **append-only** (fsync only new entries; the whole log is
  no longer rewritten per write), and the leader fsyncs a round's entries once.
- Committed entries are **applied together** through the DB group commit.
- Together these lift concurrent cluster write throughput from ~60/s to ~500/s
  (16 connections) / ~800/s (32); a single sequential write stays
  fsync-latency-bound.

### Leader lease (liveness + linearizable leader reads)

- The leader renews a **lease** each round it confirms a quorum and **steps
  down** if it cannot within the lease window (below the election timeout). A
  leader partitioned from its quorum now relinquishes leadership — in-flight
  writes fail fast and a healthy majority elects a new leader — rather than
  hanging. A lease-valid leader is guaranteed to be the leader, so its local
  reads are linearizable without a quorum round-trip.

### Raft log compaction

- The replicated log no longer grows unbounded: once entries are applied and
  replicated to every member, each node discards them (keeping the snapshot
  boundary term for the consistency check); the applied state machine is the
  snapshot. Compaction advances only to the slowest member's replicated index.

### Password hardening

- New passwords must satisfy a strength policy (`ELYRASQL_PASSWORD_MIN_LEN`,
  default 8; `ELYRASQL_PASSWORD_REQUIRE_MIXED`, default on;
  `ELYRASQL_PASSWORD_POLICY=off` to disable).
- Repeated failed logins trigger a temporary **account lockout**
  (`ELYRASQL_AUTH_MAX_FAILURES`, default 10; `ELYRASQL_AUTH_LOCKOUT_SECS`,
  default 60), logged.

## [0.8.9] - 2026-07-09

Consensus release: the Raft log is now on the live cluster write path.

### Raft replicated-log write path (pre-commit / 2-phase)

- In `cluster` mode, every write is proposed through the Raft log: the leader
  appends the entry, replicates it via `AppendEntries`, **commits it once a
  quorum has durably logged it**, and only then **applies** it and acknowledges
  the client. Followers append (with the AppendEntries consistency check +
  conflicting-suffix truncation) and apply up to the leader's commit index.
- Votes use the §5.4.1 election restriction on the log, so failover is
  **no-data-loss**: an acknowledged write is on a quorum's durable log and any
  elected leader already has it. A write cannot be acknowledged without a quorum.
- New plumbing: `elyra_storage::WriteOp` + `Consensus` trait + `Db.set_consensus`
  / `apply_op_local`; the single-node write path is unchanged when no consensus
  layer is installed.
- Known limitation: a leader partitioned from its quorum blocks writes (until it
  can replicate or the client times out) rather than stepping down proactively
  (no leader lease yet).

### Verified

- 3-node cluster: writes commit via quorum and replicate; followers reject
  writes; killing the leader preserves all acknowledged writes on the new leader
  (no data loss); no commit without a quorum.

## [0.8.8] - 2026-07-09

Partitioning release.

### Partitioning

- `CREATE TABLE ... PARTITION BY RANGE|LIST|HASH (<pk column>) (...)` records a
  partitioning scheme (managed primary-key ranges), exposed in
  `information_schema.partitions`.
- `ALTER TABLE t DROP PARTITION p` / `TRUNCATE PARTITION p` cheaply delete a
  partition's rows (range/`IN` delete with index cleanup). Queries with a PK
  predicate prune automatically via clustered range scans.
- Also fixed a stale docs line: `ON UPDATE` referential actions are enforced.

### Notes / deferred

- Partitioning is **single-node** (managed PK ranges, not physical files, not
  enforced on INSERT). **Horizontal write scale-out** across nodes would require
  distributed sharding and is out of scope by design.
- **Wiring the Raft log into the live cluster write path** (leader append →
  quorum commit → apply, for pre-commit 2-phase durability) is intentionally
  *not* bundled here: it is a correctness-critical change that warrants a
  dedicated release with partition/failover testing (the tested log core landed
  in 0.8.6). Today's HA remains async replication + quorum/`--sync-strict` + the
  LSN-aware election restriction (no data loss for acknowledged writes).

## [0.8.7] - 2026-07-09

SQL-surface & usability release.

### Named windows

- `SELECT ... OVER w ... WINDOW w AS (PARTITION BY ... ORDER BY ...)`, including
  `OVER (w ...)` that inherits a named window and adds local clauses.

### Materialized-view auto-refresh

- Materialized views now **auto-refresh on read** when a base table has changed
  since the last refresh (detected via per-table write counters). This is a full
  recompute, not incremental delta maintenance.

### Notes

- `caching_sha2_password` remains unimplemented: the latest published
  `opensrv-mysql` (0.7.0, what we use) does not drive its multi-round auth
  exchange. MySQL 8 clients negotiate down to `mysql_native_password`.

## [0.8.6] - 2026-07-09

Programmability, security & consensus-foundation release.

### Materialized views

- `CREATE MATERIALIZED VIEW v AS <select>` materializes the result into a real
  table; `REFRESH MATERIALIZED VIEW v` recomputes it; `DROP MATERIALIZED VIEW v`
  removes it. Refresh is explicit (no auto-refresh).

### Per-column privileges

- `GRANT SELECT(col, ...) ON t TO u` restricts a user to reading only those
  columns of `t`; querying an ungranted column (via the projection, `SELECT *`,
  or a `WHERE`/`ORDER BY` reference) is denied. Enforced for single-base-table
  selects; a restricted table in a join/subquery is denied (deny-safe).

### Raft log core (consensus foundation)

- New unit-tested `raftlog`: an ordered persistent log with the AppendEntries
  consistency check + conflicting-suffix truncation, the quorum/current-term
  commit rule, apply-only-when-committed, and the §5.4.1 election restriction.
  Routing the live cluster write path through it (for pre-commit 2-phase
  durability) is the remaining integration step.

### Notes

- `caching_sha2_password` remains unimplemented: the MySQL-protocol library does
  not drive its multi-round auth exchange. MySQL 8 clients negotiate down to
  `mysql_native_password`.

## [0.8.5] - 2026-07-09

Planner, security, and durability release.

### Histogram-based cardinality

- `ANALYZE TABLE` builds an equi-height histogram per column (reservoir sample),
  exposed as a JSON `HISTOGRAM` in `information_schema.column_statistics`. The
  planner estimates WHERE-predicate selectivity from histograms to order joins
  by estimated (not just raw) row counts.

### Roles, per-database grants & audit log

- `CREATE ROLE` / `DROP ROLE`, `GRANT <role> TO <user>` / `REVOKE <role> FROM
  <user>`; users inherit the global and per-table grants of their roles.
- `GRANT ... ON db.*` is accepted (maps to a global grant, single database).
- `--audit-log <path>` appends one line per executed statement
  (`timestamp  conn_id  user  OK|ERR  sql`).

### LOAD DATA INFILE & auth hardening

- `LOAD DATA INFILE '<server path>' INTO TABLE t [FIELDS/LINES TERMINATED BY]
  [IGNORE n LINES] [(cols)]` bulk-loads a server-side file (ADMIN required; `\N`
  = NULL).
- Connection salts now use the OS CSPRNG. (`caching_sha2_password` is not
  implemented — the wire library lacks its multi-round exchange; MySQL 8 clients
  negotiate down to `mysql_native_password`.)

### Crash-safe cluster elections

- Election state (current term + vote) is persisted to `<data>.raftstate`, so a
  restarted node never double-votes in a term (Raft safety). Full Raft log
  replication (pre-commit 2-phase durability) remains a dedicated milestone.

## [0.8.4] - 2026-07-09

High-availability & query-planner release.

### Zero-data-loss failover (election restriction)

- Cluster leader election now enforces the **Raft election restriction**: a node
  only votes for a candidate at least as up-to-date (by LSN) as itself, so an
  elected leader holds every quorum-acknowledged write. Together with
  `--sync-strict` this makes failover no-data-loss for acknowledged writes. (The
  sync barrier still runs after the local commit; this is not a pre-commit
  2-phase protocol.)

### Dynamic cluster membership

- Add/remove cluster members at runtime with `elyrasql cluster-ctl --node
  <addr> --action add|remove --peer id@host:port`. The leader advertises the
  membership in heartbeats and followers adopt it. Add one node at a time and
  start it before adding.

### Cost-based JOIN reordering + merge join

- Explicit INNER-join chains over base tables are reordered cost-based (drive
  from the smallest relation, extend along equi-join predicates; alias-aware).
- Large INNER equi-joins whose inputs are already sorted on the join key
  (clustered primary-key scans) use a streaming merge join.

### Stored procedures: cursors & handlers

- `DECLARE ... CURSOR FOR`, `OPEN`, `FETCH ... INTO`, `CLOSE`, and
  `DECLARE {CONTINUE|EXIT} HANDLER FOR {NOT FOUND | SQLEXCEPTION | SQLSTATE '...'
  | <code>} <action>`.

## [0.8.3] - 2026-07-09

Scalability & robustness release — hardening the write path and high availability.

### Pessimistic locking

- `LOCK TABLES t READ|WRITE` / `UNLOCK TABLES` take real **blocking** table
  locks (a `WRITE` lock blocks other readers and writers; a `READ` lock blocks
  writers). Conflicting statements from other sessions block until release, or
  fail with `1205` (lock wait timeout). `LOCK IN SHARE MODE` is accepted as a
  synonym for `FOR SHARE`. Zero overhead when no explicit lock is held.

### Quorum / synchronous replication

- `--sync-replicas N` makes each commit wait for `N` replica acknowledgements;
  `--sync-strict` fails the commit-confirmation on timeout instead of silently
  degrading to asynchronous (no silent data-loss window). Per-replica ack
  tracking replaces the single high-water mark.

### Incremental replica catch-up

- A reconnecting replica streams only the **binlog delta** since its last applied
  LSN instead of re-copying the whole database, falling back to a full snapshot
  only when the binlog is disabled or the needed segments were purged. Replicas
  reconnect transparently on stream drops. The LSN counter is resumed from the
  binlog across restarts (correct binlog ordering + working catch-up).

### Write throughput

- Validated **transactional** commits are now **group-committed**: many
  concurrent transactions fold into one write transaction (one fsync) instead of
  one fsync each, while preserving first-committer-wins ordering and write-write
  conflict detection. (The single writer remains inherent to the ACID
  single-file design; there are no parallel writers or sharding.)

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

[1.4.9]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.9
[1.4.8]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.8
[1.4.7]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.7
[1.4.6]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.6
[1.4.5]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.5
[1.4.4]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.4
[1.4.3]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.3
[1.4.2]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.2
[1.4.1]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.1
[1.4.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.0
[1.3.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.3.0
[1.2.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.2.0
[1.1.3]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.1.3
[1.1.2]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.1.2
[1.1.1]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.1.1
[#15]: https://github.com/kwhorne/ElyraSQL/issues/15
[1.1.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.1.0
[1.0.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.0.0
[0.9.9]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.9
[0.9.8]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.8
[0.9.7]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.7
[0.9.6]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.6
[0.9.5]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.5
[0.9.4]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.4
[0.9.3]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.3
[0.9.2]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.2
[0.9.1]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.1
[0.9.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.9.0
[0.8.10]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.10
[0.8.9]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.9
[0.8.8]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.8
[0.8.7]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.7
[0.8.6]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.6
[0.8.5]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.5
[0.8.4]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.4
[0.8.3]: https://github.com/kwhorne/ElyraSQL/releases/tag/v0.8.3
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
