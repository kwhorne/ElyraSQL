# Changelog

All notable changes to ElyraSQL are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Numeric `RANGE` and `GROUPS` window frames.** Aggregate windows now support
  exact numeric offsets in ascending and descending order, peer groups,
  partitions, NULL ordering, and empty frames. Integer and decimal boundaries
  use checked fixed-point arithmetic rather than lossy floating-point
  conversion; invalid, row-dependent, temporal, and incompatible offsets are
  rejected explicitly.
- **Composite secondary-index prefix ranges.** Predicates such as
  `tenant = ? AND status = ? AND created BETWEEN ? AND ?` can scan the matching
  left prefix of a composite index. Bounds honor each component's collation,
  repeated constraints are merged, residual predicates are rechecked, and
  transactional overlays remain visible. Covered `COUNT(*)` can count index
  entries without fetching table rows.
- **`ALTER TABLE ... ADD PRIMARY KEY` for populated tables.** Existing rows and
  secondary indexes are reclustered atomically, with serializable range
  validation preventing concurrent inserts from surviving in the old row-id
  keyspace.
- **Bounded spill-backed `SELECT DISTINCT`.** Large distinct sets now sort and
  stream through temporary files instead of requiring the entire result in
  memory, while preserving SQL collation, mixed-numeric equality, stable first
  representatives, offsets, limits, cancellation, and result metadata.

### Changed

- **Correlated `EXISTS` / `NOT EXISTS` can execute as one-time membership
  plans.** Safe single-table equality correlations are evaluated once with
  exact type/collation gates and correct NULL anti/semi-join semantics; other
  shapes retain the general correlated path. `EXPLAIN` reports the optimized
  plan only when it is guaranteed.
- **Selective inner joins delay partner-table materialization.** A selective
  point driver can probe a partner primary or secondary index directly,
  including transaction-local rows, instead of eagerly scanning every joined
  table.
- **Window aggregation is incremental where possible.** `SUM`, `COUNT`, and
  `AVG` over `RANGE`/`GROUPS` frames use precomputed bounds and prefix state;
  `MIN` and `MAX` retain their exact fallback while sharing the faster bound
  planning.
- **Bulk inserts and table rewrites do less allocation and redundant work.**
  Index keys encode selected columns without cloning, writes are ordered for
  the B-tree, unchanged rows reuse their serialized representation, and
  serializable scans coalesce overlapping validation ranges.
- **`LOAD DATA INFILE` uses bounded 50,000-row bulk units** to amortize SQL
  parsing and durable commits. Insert paths cache trigger definitions (including
  empty sets) with DDL-safe invalidation. On the 50,000-row comparison workload,
  ordinary 1,000-row batches improved from 1,072 ms to 781 ms, one bulk
  statement took 541 ms, and server-side `LOAD DATA` took 267 ms.
- **`ai_embed()` now uses ureq 3.** Two things change for anyone who has
  `ELYRASQL_AI_EMBED_URL` configured. ureq 3 reads `HTTP_PROXY`, `HTTPS_PROXY`
  and `ALL_PROXY` from the environment by default where ureq 2 did not, so the
  proxy is explicitly disabled to preserve the previous behaviour -- the request
  carries the provider API key in its `Authorization` header, and a proxy
  variable that happens to be set in the server's environment should not
  silently reroute it. The response body is also capped at ureq's 10 MiB
  default; ureq 2's `into_json()` was unbounded.

  TLS is unchanged: rustls with `ring` and bundled webpki roots, no
  `aws-lc-rs`, so static musl builds and the `FROM scratch` image are
  unaffected. Note that ureq 3 raises the effective MSRV floor to exactly the
  declared 1.88 (via `cookie_store` and `time`).

### Fixed

- Exact `RANGE` boundaries no longer merge distinct integers above `2^53`, and
  wholly out-of-partition frames return an empty frame instead of indexing with
  `usize::MAX` and crashing the connection.
- External sorting now returns no rows for `LIMIT 0` in every spill/top-N mode
  and reports truncated spill headers or bodies as storage corruption rather
  than clean EOF or a generic I/O failure.
- Spill-backed `DISTINCT` groups by its canonical SQL key rather than a broader
  sort comparison, preventing mixed numeric representations from producing
  duplicate output.

### Internal

- **Dependabot no longer rewrites the Rust toolchain pins, and no longer groups
  breaking updates with safe ones.** `dtolnay/rust-toolchain@1.88.0` pins the
  *Rust* version, not a version of the action -- that action publishes one tag
  per toolchain. Read as an action version, an "upgrade" rewrote both the MSRV
  gate in `ci.yml` and the release toolchain in `release.yml` to a Rust version
  that does not exist (#68). Only the `ci.yml` half failed, because
  `release.yml` runs on tags; merged, it would have dropped MSRV verification
  and broken the next release build. The action is now ignored and toolchain
  moves stay manual.

  The Cargo group is split into `cargo-patch` and `cargo-minor`. In Cargo a 0.x
  *minor* bump is a breaking change, so one group let a single breaking update
  block every safe one: #69 bundled `ureq` 2->3, `md-5`/`sha2` 0.10->0.11,
  `getrandom` 0.2->0.4 and `rand` 0.8->0.10 with ten routine updates and failed
  to compile as a whole. `sha2` and `md-5` also join the existing `sha1` rule --
  all three share one `digest` dependency, and moving any of them alone leaves
  two RustCrypto generations in the tree.

## [1.9.4] - 2026-08-03

Seven contributions, and for the first time in a while none of them is a
correctness bug. This release is about the machinery around the engine: how it
is built, how it is shipped, and what it tells clients about itself.

Two changes can affect a working deployment, and both are in the upgrade note
in the installation docs: **the container image no longer has a shell**, and
**result-column metadata reports different (correct) values.** Neither touches
your data.

### Fixed

- **Result columns advertise their declared width and the right collation.**
  Every character column reported the unbounded `TEXT` capacity under a utf8mb3
  collation, so clients computed nonsense widths: PyMySQL reported **21845**
  for a `VARCHAR(32)` where MySQL 8.4 reports **128**, because it divides the
  advertised byte length by the charset's bytes per character. `VARBINARY(16)`
  reported 65535 rather than 16.

  Columns now carry the declared width from the sidecar added in 1.9.1, and the
  collation is `utf8mb4_0900_ai_ci` on character columns and `binary` on
  everything else, which is what MySQL sends and what clients key their length
  arithmetic off. `SHOW VARIABLES` already reported utf8mb4 everywhere; the wire
  disagreed with it.

  Checked field by field against MySQL 8.4: collations match on all ten types
  tested, widths on nine. The remaining gap is `INT`, reported as `BIGINT`/20
  where MySQL says `INT`/11 — the storage type is a 64-bit integer, and
  changing the advertised type also changes binary-protocol encoding, so it is
  left for its own change. The full comparison is in
  `docs/mysql-compatibility.md`.

  Tables written before 1.9.1 have no sidecar and keep the unbounded width.
  Nothing needs to be rebuilt.
- **`elyrasql serve` exits on Ctrl-C.** SIGINT was previously ignored, so the
  only way to stop a foreground server was `SIGTERM` or `SIGKILL`. It is a clean
  exit rather than a connection drain — in-flight connections are dropped, and
  `redb` keeps the database consistent regardless. Installing the handler also
  overrides an inherited `SIG_IGN`: a server started with SIGINT ignored will
  now exit on it.

### Changed

- **The container image is built `FROM scratch`** instead of Alpine: only the
  static binary, `passwd`/`group` for the non-root user, the data directory, and
  a writable `/tmp` for sort and aggregate spills. **22.3 MB → 13.6 MB
  (−39%)**, and no OS packages means no OS CVEs to triage.

  **There is no shell in the image.** `docker exec <container> sh` and anything
  built on it — debugging one-liners, shell-based health checks, init wrappers
  — no longer works; use the MySQL protocol from outside the container. This is
  the one change here that can break a working deployment. See
  `docs/deployment.md`, which also covers where spill files now live.
- **Release binaries are built with fat LTO** via a new `[profile.dist]`.
  `[profile.release]` stays on thin LTO, so local and CI builds are unaffected.
  Measured: full scan `COUNT` 2.79 → 1.68 ms, `GROUP BY` 2.21 → 1.76 ms, other
  workloads within noise; binary 6.7% smaller.
- **Release binaries are additionally profile-guided (PGO).** The release
  workflow trains on generated SQL dumps and the benchmark suite, then rebuilds
  with the merged profile. Measured cold on aarch64, six runs per side with a
  fresh server and data directory each: **index nested-loop joins −14.6%**
  (20.6 → 17.6 ms) and **vector index build −4.9%** (985 → 937 ms), with
  sub-2 ms workloads unchanged. Binary a further 13% smaller.

  The training set is load-bearing: a deliberately thinned one made the same
  code **32% slower**, because a profile optimises for what it saw and
  pessimises the rest. The release step therefore verifies a profile was
  actually produced before using it, and annotates the run when it was not — a
  release built without a profile is still a supported release, it just is not
  silently one. Details in `CONTRIBUTING.md`.

  Cost: about 2m45s per target, so roughly 8 minutes added to a three-target
  release. Nothing on PR builds.

### Internal

- **The SQL dump differential harness runs nightly.** Added in 1.9.1, it found
  the `DECIMAL`/`UNSIGNED` range gap on its first run and was then never wired
  into CI — so nobody noticed it had been broken since 1.9.1, because the
  testbench is a separate Cargo workspace whose lockfile goes stale on every
  version bump while `run.sh` runs `cargo run --locked`. A tool nobody runs is a
  tool that quietly stops working. Five schema models now run nightly against
  MySQL 8.4, comparing full table digests; the release checklist in
  `CONTRIBUTING.md` now covers the lockfile.
- **CI runs tests with `cargo-nextest`**: 5m44s → 46s wall for the same tests,
  because the suite is dominated by wire tests that each stand up a server and
  wait. Note that nextest does not run doctests; the workspace has none today.

## [1.9.3] - 2026-08-02

A small release: no behaviour changes, no new SQL, nothing to check before
upgrading. It exists because the change it carries is worth having sooner than
the next feature release.

### Changed

- **`WHERE` filtering on the streaming join paths takes a fast path.** An
  `AND`-connected `WHERE` is split at plan time and every simple
  `col <op> col` comparison resolved to column indices once, instead of
  resolving each reference twice per row — once for the value and again for the
  collation, each walk ending in a string comparison. Profiling put
  `predicate::matches` at 95% of CPU in the streaming aggregate path.

  Measured on this release's tree: the heaviest wire test drops from **14.5s to
  1.9s** in debug builds and 2.2s to 1.7s in release, and the full wire suite
  from 22.3s to 17.2s. Together with the nested-loop fast path in 1.9.2 the
  suite has gone from 73s to 17s in debug, which is the difference between a
  test run people wait for and one they don't.

  Release-mode gains are smaller, because this removes interpretive overhead the
  optimiser already handled. Anything the shape detector does not recognise —
  `OR`, expressions, literals — falls through to the unchanged evaluator; all of
  those were checked against MySQL 8.4 along with collation-sensitive
  comparisons and the `LEFT JOIN ... IS NULL` anti-join.
- The fuzz workflow uses `actions/upload-artifact@v7`, so every artifact action
  in the repository is on the same major.

## [1.9.2] - 2026-08-02

Eleven contributions, and a theme: **the things we never exercised were the
things that did not work.** A spilling sort that no test had ever made spill.
`SET` statements accepted and discarded. Result metadata nothing read back.
Virtual catalog tables nothing queried. Each was fine right up to the first time
something real depended on it.

Two return or lose data silently and are the reason to upgrade: an anti-join
that dropped its `WHERE`, and `SET autocommit=0` that committed anyway.

**Three changes tighten validation**, so statements that used to succeed can now
fail — a string longer than its `VARCHAR(n)`, and the two error-code corrections
below. No on-disk format change; 1.5.x through 1.9.1 open unchanged.

### Fixed — wrong or lost data

- **`LEFT JOIN ... WHERE nullable IS NULL` returned the rows it was meant to
  exclude.** A `WHERE` predicate over the nullable side was pushed beneath the
  outer join, which changes what NULL-extension means, so the standard
  anti-join idiom silently returned every row. It needed a compound `ON`
  (`ON a.id = b.id AND b.k = 'x'`) and the plain execution path — adding
  `ORDER BY` or `GROUP BY` gave the right answer, so the shape that was wrong
  was the bare one WordPress and Drupal generate for "rows without this meta
  key".
- **`SET autocommit=0` was accepted and discarded, so work committed anyway.**
  A second connection could see the "uncommitted" rows immediately, and
  `ROLLBACK` did nothing. An application building a transaction with
  `autocommit=0` rather than `START TRANSACTION` — which several drivers do by
  default — had the appearance of a transaction and none of the substance.
  `sql_mode` (including `ANSI_QUOTES`), `NO_AUTO_VALUE_ON_ZERO`,
  `FOREIGN_KEY_CHECKS`, isolation level and `group_concat_max_len` were
  discarded the same way and are now applied, with session values kept
  independent of the globals.
- **A spilling `ORDER BY` failed outright with `Bad file descriptor`
  (ESQL-60).** The external merge sort wrote each run through a handle from
  `File::create` — which opens **write-only** — and the merge phase then seeked
  that same handle back to 0 and tried to read it, so every sort that exceeded
  `ELYRASQL_SORT_MAX_ROWS` returned an I/O error instead of rows. Since that
  budget defaults to a million rows, it took an unbounded `ORDER BY` over a
  large table to reach, which is why nothing in CI or the benchmarks had ever
  written a run file: the sweep tops out at 8193 rows and the benchmark's
  unbounded sort stays in memory at 200k. The memory-bounded `ORDER BY`
  described in `BENCHMARKS.md` and `docs/performance.md` therefore never
  worked. Run files are now opened read-write, and a unit test forces a small
  budget so the spill path is exercised on every run — including the
  single-run case, which skips the k-way merge.
- **`SELECT` without `FROM` ignored `WHERE`, `LIMIT` and `OFFSET`.**
  `SELECT 1 WHERE 1=0` and `SELECT 1 LIMIT 0` both returned a row. MySQL's
  unsigned all-ones `LIMIT` sentinel with `OFFSET` is also accepted now.
- **Numeric string coercion was inconsistent per call site.** `'12' + 1` raised
  1366, `SUM(text_column)` returned `NULL` rather than a total, and
  `CAST('4.5x' AS DOUBLE)`, `ABS('-3q')` and `ROUND('2.7t')` all returned
  `NULL`. One shared MySQL-compatible coercion now covers arithmetic, unary
  numerics, casts, scalar functions, `SUM`/`AVG`, window fallbacks and the OLAP
  paths, preserving `NULL` while coercing malformed non-NULL strings to zero.

### Fixed — schema and metadata

- **Inline index prefix lengths (`KEY meta_key (meta_key(191))`) are accepted.**
  This is the DDL WordPress ships, so installation stopped at the first
  `CREATE TABLE`; it also drove a large cluster of Drupal failures. The prefix
  is parsed and the index covers the whole column, which is *stricter* than
  MySQL rather than looser.
- **Declared column types survive.** `TINYINT(1)`, `VARCHAR(n)`, `CHAR(n)` and
  exact numeric declarations were collapsed to coarse storage types, so
  `SHOW COLUMNS` reported `BIGINT` where MySQL reports `tinyint(1)` — the
  declaration every ORM reads to decide a column is boolean. Type names,
  widths, character lengths, precision and scale are now kept in a
  catalog-compatible sidecar and reported through `SHOW COLUMNS`,
  `SHOW CREATE TABLE` and `information_schema.COLUMNS`, and carried through
  `LIKE`, CTAS, `ALTER`, rename and drop. **`CHAR`/`VARCHAR` limits are now
  enforced** in strict mode.
- **`information_schema` constraint tables are complete.**
  `TABLE_CONSTRAINTS` and `CHECK_CONSTRAINTS` did not exist — and a missing
  virtual table raises 1064, which reads as *your SQL is wrong* rather than
  *this server lacks the table*, so Rails schema dumping, Django introspection
  and Adminer stopped on valid queries. `REFERENTIAL_CONSTRAINTS` is complete,
  and `COLUMNS.PRIVILEGES` and `ROUTINES.DTD_IDENTIFIER` are populated.
- **Result columns carry real metadata.** Every column arrived as
  `VAR_STRING`, length 1024, with no flags. Native `DATE`, `DATETIME`, `TIME`,
  `NEWDECIMAL` and `JSON` types are reported now, along with `NOT_NULL`,
  `PRI_KEY`, `UNIQUE_KEY`, `AUTO_INCREMENT` and `UNSIGNED` flags, per-column
  lengths and decimal scales — so a typed client stops hydrating dates and
  decimals as strings. Composite-index members are no longer marked
  individually unique.

### Changed

- **Nested-loop joins over a simple cross-relation comparison take a fast
  path**, skipping the row clone and the general predicate evaluator for pairs
  the condition rejects. The wire suite drops from **73s to 22s in debug**
  builds (release is unchanged — the optimiser already handled it), which is
  where CI and contributors spend their time. Anything the shape detector does
  not recognise falls through to the unchanged evaluator.
- **63 transitive dependencies removed** by moving `mysql_common` 0.32 → 0.37,
  including `bindgen`, `clang-sys`, `cmake` and `zstd-sys` — so the build no
  longer needs a C toolchain for crates we never used.
- All CI workflows are on `actions/checkout@v7` (Node 24).

## [1.9.1] - 2026-08-02

Four fixes for the same underlying blind spot: **rows of the statement being
executed were invisible to the checks that guard it**. A foreign key looked at
the table as it stood before the statement, a cascade did the same, and neither
followed a key that pointed at its own table. Add a fourth, unrelated, that had
simply never been checked at all — integer width — and the effect was a database
that accepted schemas and data MySQL refuses, and refused inserts MySQL accepts.

All four were found by differential testing rather than by a report, and each is
verified against MySQL 8.4 on identical data.

**Three of these tighten validation**, so statements that used to succeed may now
fail: a value too wide for its column, a duplicate column name, and
`DELETE FROM t` on a self-referencing table without `ON DELETE CASCADE`. Nothing
is rewritten on upgrade — a database cannot start rejecting data it already
holds — and there is no on-disk format change: 1.5.x through 1.9.0 open
unchanged.

### Fixed

- **A multi-row `INSERT` could not satisfy a self-referencing foreign key from
  its own rows (ESQL-58).** `INSERT INTO emp VALUES (1,NULL),(2,1),(3,2)` was
  refused with 1452 while MySQL accepts it: the key was checked against the
  table as it stood *before* the statement, so a row referencing one earlier in
  the same batch found no parent. Every dump tool batches inserts, which made
  any table with a `parent_id`-shaped key impossible to restore. Rows earlier in
  the statement (and the row itself, for `(5,5)`) now count — while a *forward*
  reference still fails, as it does in MySQL, because that is a genuine ordering
  error rather than a batching artefact.
- **A self-referencing `ON DELETE CASCADE` did not cascade (ESQL-59).** Deleting
  the root of a hierarchy left its children behind, pointing at a row that no
  longer existed — a table violating its own foreign key. The helper that finds
  referencing tables skipped the table being deleted from, so a key pointing at
  its own table never fired. Cascades now also run to a **fixed point**, so they
  reach grandchildren instead of stopping after one level, with a depth bound
  and a visited set so a cycle terminates. Relatedly, `DELETE FROM t` on a
  self-referencing table without `CASCADE` is now refused with 1451 like MySQL,
  instead of quietly deleting rows that were still referenced.
- **Integer width is enforced (ESQL-56).** `TINYINT` accepted 300, `SMALLINT`
  32768, `INT` 2147483648 — storage is 64-bit for every integer type, and
  nothing carried the declared width, so MySQL's 1264 never happened. Widths are
  now recorded and checked for `TINYINT`/`SMALLINT`/`MEDIUMINT`/`INT`, signed and
  unsigned, and maintained through `ADD`/`DROP`/`MODIFY COLUMN`.

  They live in a **separate catalog key**, not on `ColMeta`: `TableDef` is
  bincode-encoded and bincode is positional, so a new field would make every
  existing catalog undecodable. A table created before this release simply has
  no widths recorded and keeps the old behaviour until it is recreated.
- **`ADD COLUMN` accepted a name the table already had (ESQL-57)**, producing
  two identically named columns — one unreachable, both occupying a slot in
  every stored row, and DDL that could not be replayed. Now 1060 `42S21`,
  matching MySQL, on both `ALTER TABLE ... ADD COLUMN` and `CREATE TABLE`, and
  compared case-insensitively as MySQL does.

## [1.9.0] - 2026-08-02

**Upgrade promptly.** This release fixes two bugs that could return or write the
wrong data with no error at all, and one that could take the server down.

Seven contributions from [@HelgeSverre](https://github.com/HelgeSverre), landed
as a reviewed stack. Every entry below was verified against MySQL 8.4 on
identical data before merging — in several cases the *table contents afterwards*
were compared, not just the statement's return code, because the failure mode
was a successful-looking statement.

The pattern across all seven is the same one 1.8.0 started: these are not
features we were missing, they are places where we answered confidently and
wrongly. None of them came from a bug report.

### Fixed — wrong data

- **`NATURAL JOIN` and `JOIN ... USING` executed as cross joins.** The join
  condition was ignored entirely, so the result was a cartesian product
  presented as a join — no error, plausible-looking rows, and a row count that
  only looks wrong if you already know the answer. On two 2-row tables sharing
  two columns, `SELECT * FROM a NATURAL JOIN b` returned **4 rows where MySQL
  returns 1**, with the shared columns duplicated in the output. Both forms now
  match MySQL exactly, including the parts that are easy to get half-right: a
  `NATURAL JOIN` coalesces every shared column, a `USING (k)` emits the join
  column once and moves it to the front of the select list, and `SELECT k` after
  `USING (k)` is unambiguous rather than an error. Collation and coercion are
  preserved identically on the optimized and fallback join paths, so the fast
  path cannot disagree with the slow one.
- **A database qualifier was ignored, so writes aimed elsewhere hit local
  tables.** `UPDATE nosuchdb.t SET ...` and `DELETE FROM nosuchdb.t` executed
  against the local `t` and **reported success** — verified by inspecting the
  table afterwards: the row really was updated and another really was deleted.
  Reads behaved the same way, in `FROM`, in joins and in subqueries. A migration
  runner pointed at the wrong environment, a dump replayed with its original
  `db.table` names, or an ORM with a second connection could all modify data
  they never addressed. Qualifiers are now validated consistently across SELECT,
  CTEs, views, DML, DDL, routines, triggers, introspection and prepared
  statements, and invalid single-table `UPDATE`/`DELETE`/upsert targets are
  rejected **before any row changes**. `GRANT`/`REVOKE` scopes are parsed
  structurally at the same time, so a quoted dotted or wildcard-looking table
  name can no longer redirect a privilege grant.
- **`UPDATE ... SET unknown_qualifier.col = ...` succeeded without writing
  anything.** A typo in a column qualifier produced a successful statement that
  changed nothing — an application would log a save and move on. It is now
  rejected with MySQL's 1054 before the mutation runs.

### Fixed — crashes and lost objects

- **A chain of nested views could kill the server process.** Around 600 levels
  of stored views overflowed the native stack and aborted the whole process,
  taking every other connection with it — reachable by any account that can
  `CREATE VIEW`. Deep-but-legitimate nesting now grows the stack near the guard
  page, and a hard `MAX_QUERY_NESTING` bound catches the rest, including cyclic
  view definitions that no amount of stack can survive. The result is an
  ordinary query error with the session still usable. Stored views compose
  *already-parsed* queries, which is why the text-level complexity guard never
  saw this.
- **A failed `ALTER TABLE` could leave the table half-changed — or gone.**
  `ALTER TABLE t ADD COLUMN b INT, DROP COLUMN nosuch` failed *and* added `b`;
  `ALTER TABLE t ADD COLUMN c INT, RENAME TO nosuchdb.t2` failed *and* renamed
  the table away, so the next statement got `no such table`. Every `ALTER TABLE`
  now runs inside a private transaction checkpoint that rolls back catalog,
  rows, indexes, counters, foreign keys and renames together, while preserving
  prior work and client savepoints inside an explicit transaction.

### Fixed — queries that should have worked

- **A CTE was invisible from inside a subquery or a set-operation branch**, and
  reported as `1146 no such table`, which sends you looking in entirely the
  wrong place. `WITH a AS (...) SELECT ... WHERE id IN (SELECT id FROM a)` and
  `WITH a AS (...) SELECT * FROM a UNION ALL ...` both work now. CTE rewriting
  models declaration-point scope, shadowing, forward references and
  recursive-reference rules explicitly instead of substituting text, bounds
  expansion depth and width, cleans up failed materialization, and no longer
  lets a CTE capture a physical table of the same name at the wrong point.
- **Relation aliases are case-sensitive again.** `SELECT ... FROM t AS T WHERE
  t.id = 1` was accepted; MySQL rejects it. Column names and output aliases stay
  case-insensitive, and the lookup is Unicode-aware rather than ASCII-folded.

### Fixed — result metadata

- **A quoted identifier containing a dot was split down the middle.** Result
  metadata recovered the source table by re-parsing a joined `"table.column"`
  string, so a column `` `a.b` `` in a table `` `we.ird` `` came back as table
  `a`, column `b`. Qualifiers are now kept structured through planning and
  execution rather than flattened into a string and re-split. This corrects the
  metadata work added in 1.8.0.

### Known gaps

- **`ALTER TABLE ... ADD COLUMN` accepts a name that already exists**, producing
  a table with two identically named columns where MySQL raises 1060. Found
  while probing the `ALTER` atomicity fix; it is an input-validation gap rather
  than an atomicity one and is tracked in ESQL-57.
- Integer width remains advisory (see 1.8.0); `UNSIGNED` is enforced.

## [1.8.0] - 2026-08-01

A correctness release, and an unusually direct demonstration of why differential
testing is worth the trouble: **every bug below was found by asking MySQL what it
does, rather than by a report**. Four came out of reviewing 1.7.0's own
contributions, one from a contributed test harness on its first run, and one
from re-reading a CI log that everybody had already given up on.

Three of them were silent — a `CREATE TABLE ... AS SELECT` that lost half its
query, a column constraint that was enforced on some columns and not others, and
result metadata a client could not use — which is the kind that survives a test
suite written by the people who wrote the engine.

A minor rather than a patch bump: `INT UNSIGNED` columns now refuse negative
values they previously accepted, and several error codes changed to the ones
MySQL uses. Both are observable, even though both are corrections. There is no
on-disk format change and no migration — 1.5.x, 1.6.x and 1.7.x databases open
unchanged.

### Fixed — silent wrong behaviour

- **`CREATE TABLE ... AS SELECT` over an aggregate was truncated at the first
  parenthesis.** The preprocessor that strips MySQL table options (`ENGINE=`,
  `DEFAULT CHARSET`, ...) located the column list by looking for the first `(`
  in the statement. In a CTAS that paren usually belongs to the *query* —
  `COUNT(*)`, a derived table, any function call — so the statement was cut at
  that paren's partner: `CREATE TABLE t AS SELECT g, COUNT(*) AS c FROM u GROUP
  BY g` became `CREATE TABLE t AS SELECT g, COUNT(*)`, which then failed with
  `unknown column: g`. The stripper now declines any statement that is a CTAS or
  `LIKE` before it looks for a column list. **Materialized views run through
  CTAS**, so `CREATE MATERIALIZED VIEW v AS SELECT ... GROUP BY ...` — the main
  reason to want one — works for the first time (ESQL-54).
- **`UNSIGNED` is enforced on every integer width (ESQL-56).** Only
  `BIGINT UNSIGNED` mapped to the unsigned type; `TINYINT`/`SMALLINT`/
  `MEDIUMINT`/`INT UNSIGNED` were stored as *signed*, so the same schema was
  enforced inconsistently — `INSERT INTO t (unsigned_int_col) VALUES (-1)`
  succeeded and read back `-1`, while the identical insert into a
  `BIGINT UNSIGNED` column was refused. Laravel generates both shapes
  (`foreignId()` is an unsigned big integer, `unsignedInteger()` is not), so an
  application got the constraint on some columns and not others. Width is still
  advisory — every integer is stored as 64 bits, and that is now stated plainly
  in the [data types](docs/sql/data-types.md) page rather than left to be
  discovered.

  A column created by an earlier version keeps the type it was created with, so
  it goes on accepting negatives until the table is recreated; there is no
  migration and no on-disk change.

  Found by the differential SQL-dump harness in #26, on its first fixture.
- **`SHOW CREATE TABLE` now echoes `CHECK` and `FOREIGN KEY` constraints.** They
  were enforced but invisible, so a schema dumped through `SHOW CREATE TABLE`
  silently lost them and schema-diff tools saw a table that did not match the
  one they had. Referential actions are included (`ON DELETE CASCADE`), MySQL's
  implicit `NO ACTION` is not, and the emitted DDL is accepted back with the
  constraints still live.

### Fixed — client-visible metadata and errors

- **Result metadata names the source table again (ESQL-55).** MySQL puts the
  bare column name in the name field and the relation in the `table` field, and
  1.7.0 adopted the first half without the second — so a client had no way at
  all to tell the two `id` columns of a join apart: not by name (they collide,
  as in MySQL) and not by metadata. The qualifier was available all along, in
  the internal `"alias.col"` names, and was simply dropped when the output
  schema shortened them. It is now carried through to the wire for joins,
  single-table scans (reporting the alias, as MySQL does) and projected
  columns, and left empty for expressions and aggregates — which is also what
  MySQL reports. Verified column-for-column against MySQL 8.4 on seven query
  shapes.

  `Schema` carries this in a `serde(skip)` field, so the catalog encoding is
  byte-identical and existing databases are untouched; a test asserts that.
- **`t.*` works over a single-table scan.** `SELECT np_a.* FROM np_a` was
  refused with `unknown table qualifier` because a single-table scan carries
  bare column names, so there was no qualifier to match — while the same
  wildcard over a join worked. Naming the relation being read is now accepted;
  naming a relation that is not in the query still errors.
- **Out-of-range column values report MySQL's code.** Storing a value a column
  cannot hold answered 1366 (`ER_TRUNCATED_WRONG_VALUE`, SQLSTATE `HY000`);
  MySQL answers **1264** (`ER_WARN_DATA_OUT_OF_RANGE`, SQLSTATE `22003`), which
  is what a client checking for an overflow looks for.
- **Catalog errors report the MySQL code clients branch on.** Everything the
  catalog refused answered 1146 (`ER_NO_SUCH_TABLE`), so an unknown column
  looked like a missing table to an ORM. Unknown columns are now 1054
  (`ER_BAD_FIELD_ERROR`, SQLSTATE 42S22), a duplicate index name 1061, an
  unknown index 1176, and "already exists" 1050; anything unrecognised keeps
  1146.

### Fixed — test infrastructure

- **The soak/chaos suite can no longer hang a CI job (ESQL-53).** A connect
  attempt had no timeout of its own, so a server that accepted the socket from
  the listen backlog while mid-restart and never wrote the handshake blocked the
  chaos loop indefinitely — observed once as a job that sat silent for 45
  minutes before being cancelled by hand. Connects are now bounded and treated
  as "not ready" (the caller already retries until its deadline), worker joins
  time out with the worker's index rather than blocking, and the workflow has a
  45-minute cap so a hang is reported within the hour instead of burning a
  runner until the six-hour default.

### Known gaps

- **Integer width is advisory.** Every integer type is stored as 64 bits, so a
  value too wide for its declared column (`300` into a `TINYINT`) is accepted
  where MySQL raises 1264. The signedness *is* enforced, on every width, as of
  this release. Persisting the declared width would change the catalog
  encoding, so it is tracked separately in ESQL-56 with an approach that avoids
  that; `docs/sql/data-types.md` documents the current behaviour.

## [1.7.0] - 2026-08-01

A compatibility release, and one that arrived from outside: both halves came in
as pull requests from [@HelgeSverre](https://github.com/HelgeSverre), who ran the
migration and test suites of **four commercial Laravel codebases** against
isolated ElyraSQL instances, reduced every failure to a generic MySQL
reproduction, and checked the ambiguous ones against MySQL 8.4 rather than
guessing. The largest of those suites had 469 migration files; two complete
application suites passed 278 tests / 764 assertions and 169 tests / 762
assertions against ElyraSQL.

The theme is that **almost none of these were engine bugs**. They were places
where ElyraSQL answered a question correctly but *differently* — a column label,
a coercion under a session SQL mode, a version string, a metadata field — and
every one of those differences becomes a driver-specific workaround in somebody's
application. There are 103 new end-to-end tests through the MySQL wire protocol
covering them.

Two genuine wrong-results bugs were found on the way, and both had been present
for some time. Neither came from a report: one fell out of running a join battery
differentially against MySQL, the other out of the threshold sweep finally being
able to run.

Also in this release: **native Apple Silicon binaries**, built and tested on an
arm64 macOS runner in CI and attached to every tagged release from this one on.

No on-disk format change and no migration — a 1.5.x or 1.6.x database opens
unchanged, and a 1.7.0 database still opens in either. It is a minor rather than
a patch bump because the advertised MySQL version changes what version-gated
clients generate, and because `CREATE DATABASE` now refuses instead of quietly
succeeding.

### Fixed — wrong results

- **A fractional literal was rounded into an integer index range, shifting the
  result by a row.** `k > 1024.5` on an `INT` primary key means `k >= 1025`, but
  the bound was coerced the way an `INSERT` value is coerced — rounded to the
  nearest representable value — and the strict `>` was then applied to the
  *rounded* bound, dropping row 1025. Four lookup paths reused `coerce()`, which
  prepares a value for **storage**, to prepare a literal for **comparison**;
  those are different jobs. A bound now keeps its meaning by flipping its
  inclusivity when coercion moved the value, which is exact rather than
  approximate: coercion rounds to the nearest representable value, so nothing
  lies strictly between the literal and its coerced form. `=`, `IN` and
  `BETWEEN` have nowhere to record a flipped inclusivity, so an inexact literal
  declines the index and lets the scan's filter answer — closing `k = 1024.5`,
  which returned a row it should not have. Only the index paths were affected
  (`k+0 > 1024.5` was always right), and both directions were wrong: `<` matched
  one row too few before this release, `=` since the point lookup was written.
  Found by the threshold sweep at n=2048, where `AVG(k)` lands on `.5` and
  `WHERE k > (SELECT AVG(k) FROM a)` counted one short.
- **A join `ON` condition spanning both tables was hashed as an equi key**
  (carried from 1.6.0; see that entry). Verified again here by the join battery
  that now runs on every pull request.

### Added

- **Laravel migrations run unmodified.** MySQL index and foreign-key drop forms,
  ordered and limited `UPDATE`, column backfills for added columns, removal of
  indexes with dependent columns, rename/introspection metadata, collation
  metadata, index prefixes, and catalog results that honour the selected
  database.
- **Native Apple Silicon support is release-gated.** CI now builds and runs the
  full workspace test suite on an arm64 macOS runner using the minimum supported
  Rust toolchain. Tagged releases add an `elyrasql-<version>-macos-aarch64`
  archive and checksum alongside the existing Linux artifacts. The macOS build
  targets macOS 11.0, verifies the Mach-O architecture/minimum OS/signature, and
  is executed again after archive extraction.
- **103 end-to-end tests through the MySQL wire protocol**, plus unit and
  property regressions, covering every compatibility path changed here.

### Changed

- **`CREATE DATABASE` refuses instead of silently succeeding.** ElyraSQL is a
  single logical schema in one file, so reporting success made callers believe
  they had an isolated database when every connection still shares `elyra`. The
  *conditional* forms are honest no-ops and still succeed — `CREATE DATABASE IF
  NOT EXISTS` means "make sure this exists", which is what Laravel's
  `MigrateCommand`, container entrypoints and our own benches ask for — as does
  `DROP DATABASE IF EXISTS` for a database that does not exist here. An
  unconditional `CREATE DATABASE other`, or dropping the live database, now
  fails loudly.
- **`SELECT *` over a join returns MySQL's column labels.** The bare column name
  is returned and the qualifier belongs in the result metadata; ElyraSQL was
  inventing labels like `np_a.id`. Clients that key rows by name now see the
  same collisions MySQL produces, which is what their code expects.

- **The MySQL compatibility version advertised to clients is now 8.0.12.** The
  previous 8.0.0 prefix made Laravel and other clients suppress window-function
  SQL they can safely send to ElyraSQL; version-gated clients may now generate
  different queries after connecting.
- **The documented minimum Rust version is now 1.88.** The locked dependency
  graph already required 1.88 (notably `time` through the wire-protocol stack),
  so the previous 1.82 claim could not reproduce a locked build. The macOS CI
  lane now tests the corrected minimum directly.

### Fixed

- **MySQL coercion is aligned across session SQL modes** — unsigned values,
  integer and decimal rounding, invalid temporal input in non-strict mode,
  numeric string prefixes in comparisons (`'123tail'` compares as `123`, a
  string with no numeric prefix as `0`), and exact-decimal overflow.
- **Correct wire results** for explicit auto-increment IDs, transaction status
  in the result-set flags, prepared and dynamic result types encoded from the
  declared column type, and MySQL-compatible column metadata.
- **Query fixes:** correlated subqueries (including over joins), aggregated
  correlated join filters, grouped join ordering and projections, representative
  rows for grouped wildcards, scalar subqueries in `INSERT ... VALUES`, wildcard
  expansion in window projections, ordering by hidden source values, unordered
  DML limits, upserts on unique secondary-key conflicts, and inner-relation
  shadowing.
- **SQL dumps ingest byte-for-byte.** Exact decimal and hexadecimal literals are
  preserved, substitutions no longer reach inside quoted text, and observed SQL
  is truncated only on UTF-8 boundaries.
- **Build metadata now reports the real target.** `@@version_compile_os`,
  `@@version_compile_machine`, and their `SHOW VARIABLES` equivalents no longer
  claim that Apple Silicon builds are Linux/x86_64.
- **Crash-leaked sort and aggregation files are reclaimed on macOS.** Unix
  process liveness now uses a non-signalling `kill(pid, 0)` check instead of the
  Linux-only `/proc` filesystem, while treating ambiguous results as live.

### Known gaps

- Result metadata still reports an **empty `table` field** where MySQL reports
  the source table, so a client cannot disambiguate duplicate column names
  except by position (ESQL-55).

## [1.6.0] - 2026-07-29

A performance release built on one finding: **two slow-join reports were the same
defect measured from two directions**, and fixing it once turned into a rewrite of
how row-oriented paths handle rows. Row shapes that used to scale with the *width*
of a row now scale with what the query actually reads.

Measured on 200,000 rows, both binaries run alternately against the **same data
file** (medians of 5, two rounds; MySQL 8.4 on the same host for reference):

| shape | 1.5.1 | 1.6.0 | | MySQL 8.4 |
|---|---:|---:|---|---:|
| `ORDER BY int LIMIT 100`, 12-column rows | 95 ms | **31 ms** | 3.0x | 55 ms |
| `ORDER BY int LIMIT 100`, 3-column rows | 44 ms | **22 ms** | 2.0x | 28 ms |
| `ORDER BY text LIMIT 100`, 12-column rows | 98 ms | **39 ms** | 2.5x | 57 ms |
| 1:1 join on a PK, `COUNT(*)`, 12-column | 492 ms | **150 ms** | 3.3x | 91 ms |
| 1:1 join on a PK, `COUNT(*)`, 3-column | 270 ms | **106 ms** | 2.5x | 71 ms |
| 1:N join emitting 40M rows, 12-column | 33 298 ms | **1130 ms** | **29x** | 535 ms |
| 1:N join emitting 40M rows, 3-column | 12 112 ms | **768 ms** | **16x** | 529 ms |
| join + `ORDER BY ... LIMIT 100` | 514 ms | **157 ms** | 3.3x | 31 ms |
| `ORDER BY` with no `LIMIT` (control) | 1951 ms | 1964 ms | — | 1869 ms |
| `COUNT(*)` scan (control) | 3.0 ms | 3.0 ms | — | 9.7 ms |

The controls are the point of the table as much as the gains: an unbounded sort and
a plain scan did not move, so nothing was traded away. The per-change numbers in
the entries below are deltas against the build each change landed on, which is why
they are smaller than the release-over-release figures here.

It also removes a class of query that used to be *refused*: a non-equi join
(`ON a.id < b.id`, a `BETWEEN` band join) now streams instead of materialising
into the memory ceilings. And it fixes a **wrong-results** bug in join planning
that was found by verifying the above differentially against MySQL rather than by
a report.

A minor rather than patch bump: non-equi joins answer queries that previously
errored, which is observable behaviour. There is no on-disk format change and no
migration — 1.5.x databases open unchanged, and 1.6.0 databases still open in
1.5.x.

### Fixed

- **A join `ON` condition spanning both tables was hashed as if it were an equi
  key, returning wrong results (ESQL-51).** `ON a.v + b.v = 4` returned
  2 500 000 rows where MySQL returns 1 250 000, and `ON a.id + b.id = 5000`
  returned one row too many — silently, with no error. Side attribution
  (`refs_in_schema`) resolved column references through the resolver's
  bare-name fallback, so `b.v` matched the left schema's `a.v` and the whole sum
  was judged left-only; the partner was then hashed under the constant `4` and
  probed with `a.v + b.v` evaluated on the left row alone, where `b.v` resolves
  back to `a.v`. A qualified reference now has to match the qualifier when the
  schema is a joined (qualified) one; bare references and single-table schemas
  are unchanged, so ordinary equi joins keep their hash path. Found by running a
  join battery differentially against MySQL 8.4 while working on ESQL-39;
  present since joins on expressions were supported.

### Added

- **Non-equi joins stream instead of being refused (ESQL-39).** `ON a.id < b.id`,
  a `BETWEEN` band join, or any `ON` with no equality to hash on had no key to
  build a hash table from, so the chain declined and the whole product was
  materialised — where it hit the fail-safe ceilings added in 1.4.13/1.5.0. The
  ceilings had to *fill* before they could refuse: on 20,000 rows, a three-way
  `ON a.id < b.id` under `COUNT(*)` grew the server to **2136 MB** and then
  failed after 1.3 s.

  Such a step is now the same unkeyed step a comma cross join uses, with the
  `ON` condition applied per pair, streamed into the spilling sorter/aggregator.
  The same query keeps the server at its **34 MB** idle footprint and answers —
  or, when the work is genuinely astronomical, is interrupted by
  `ELYRASQL_QUERY_TIMEOUT_MS` with the session still usable. Being honest about
  the trade: memory is now bounded but the *time* is not, and the join row
  ceilings no longer bound these shapes, so a timeout is the right control.

  When an `ON` mixes an equality with other conditions (`ON a.k = b.k AND
  a.x > b.x`), the equality is still hashed and the rest applied as a residual,
  so that shape stays `O(n+m)` rather than becoming a nested loop. A residual is
  an `ON` condition, not a `WHERE`: pairs it rejects are *unmatched*, so a LEFT
  join still NULL-extends the left row.

  Verified against MySQL 8.4 on 5000-row tables: 15 join shapes (non-equi,
  band, `LEFT` with residual, equality-plus-residual, `GROUP BY` and
  `ORDER BY ... LIMIT` over each) all identical, 0 divergences, plus the 203-case
  differential battery, the 15-size threshold sweep and the robustness scenario.

### Performance

- **One row decoder instead of two (ESQL-52).** Full-row decoding went through
  `bincode`/serde while projected decoding used the hand-written decoder added for
  ESQL-49. The hand-written one is ~17% faster on a wide row containing text
  (47 ms vs 56 ms per 200k 16-column rows), so both paths now share it and
  `bincode` is kept only as the authoritative fallback for anything it does not
  recognise. End to end this is modest, because ESQL-49 already routes the hot
  shapes through the projected decoder: `SELECT DISTINCT` over a text column
  112 → 99 ms, a text-key join 1775 → 1718 ms, everything else within noise.

- **Streaming joins no longer allocate per emitted row (ESQL-50).** With decoding
  fixed by ESQL-49, what was left was allocation and copying per combination: a
  200k-row 1:1 join spent a third of its time merely *tearing down* the partner
  hash table. The join path was rebuilt around reuse and borrowing:

  - The combined row is assembled in one scratch buffer per chain depth and the
    consumer *borrows* it, so an aggregate that keeps nothing (`COUNT(*)`, `SUM`)
    or a top-N row that loses admission costs no allocation at all.
  - The left half is copied once per driving row instead of once per combination.
  - Partner rows are stored flat — `n` values per row in one allocation per join
    key — rather than a `Vec` per row inside a `Vec` per key. For a unique key
    that inner `Vec` existed to hold exactly one row.
  - Join keys are encoded into a reusable buffer when probing and stored inline
    (up to 22 bytes, which covers every integer and short string key), so neither
    building nor probing the table allocates per row.
  - A wide partner whose columns the query mostly ignores has only the *read*
    positions written per combination, the rest staying at the `NULL` laid down
    once per driving row.
  - Join keys and qualified column references are resolved to a column index once
    at plan time instead of by name per row; `predicate::eval_row` no longer
    builds a `String` for a qualified reference on every row it evaluates, which
    helps filters and `ORDER BY` keys too, not just joins.

  Measured on 200k rows (medians of 5): 1:1 join on a primary key **214 → 132 ms**;
  a 1:N join emitting 40M rows **8621 → 981 ms** with 12-column rows and
  3741 → 765 ms with 3-column rows — so the width sensitivity that identified the
  original defect is gone (1.28x, from 2.3x). Joined `ORDER BY ... LIMIT`
  224 → 138 ms. Non-join shapes are unchanged, as intended.

  Two measurements changed the design and are worth recording. Choosing the copy
  strategy per row cost 24% on a 40M-row join *even when the branch never went the
  other way* (739 → 917 ms), so the strategy is a const parameter and chains that
  cannot benefit are compiled without the branch. And selective copying is only
  faster when it skips enough columns — a 7-column partner got slower with it —
  so the cheaper strategy is chosen per join from the widths.

- **Row paths no longer decode columns the query never reads (ESQL-49, covering
  ESQL-47 and ESQL-48).** Two reports — joins being slow, and `ORDER BY ... LIMIT`
  being slow — turned out to be the same defect measured from two directions:
  row-oriented paths materialised every column of every row. Since a row is one
  encoded blob, each unread `TEXT` column costs a `String` allocation, so the
  cost scaled with row *width* rather than with anything the query asked for.

  Both paths now decode only the columns a query references. Unread columns are
  skipped in place and left as `NULL` placeholders **at their original
  positions**, so filters, projections, `ORDER BY`, aggregates and nested join
  steps keep indexing rows exactly as before — no index remapping, and therefore
  no class of silently-wrong results.

  - `ORDER BY ... LIMIT k`: the sorter grew an admission test (`Sorter::admits`),
    which is the same comparison `push` makes. The scan evaluates the sort keys
    from a partial row, asks whether the row would be kept, and decodes the full
    row only if it would. With `LIMIT 100` over 200k rows that is ~199,900 rows
    that are no longer built and immediately dropped. Measured on 200k rows,
    12 columns: **94 ms → 29 ms** (3.2x); narrow rows 45 ms → 21 ms.
  - Streaming joins: the join chain is now resolved (catalog only) before any row
    is read, so the combined schema — and with it the set of columns the query
    reads — is known before both the partner hash tables and the driving scan
    decode anything. Join keys are always materialised, whatever the projection
    says. Measured on a 200k-row 1:1 join of two 12-column tables:
    `COUNT(*)` **488 ms → 226 ms**, and the width sensitivity that identified the
    defect flattened (wide/narrow 1.8x → 1.35x).

  `SELECT *`, rewritten `RIGHT` joins (whose output columns are permuted after
  the join) and any expression that can't be attributed to columns statically
  decode every column, exactly as before. Verified with the 203-case MySQL
  differential battery (0 divergences), the threshold sweep at all 15 sizes
  (51/51 identical at each), the robustness scenario (13/13 invariants) and the
  full workspace test suite.

### Notes

- **`ESQL-47` and `ESQL-48`, filed against 1.5.1, are both closed by this release.**
  They were reported as separate problems (joins 10-16x MySQL; `ORDER BY ... LIMIT k`
  ~2x) and turned out to be the same defect — row-oriented paths materialising every
  column of every row — which is why they were fixed once, as [ESQL-49], rather than
  patched twice.
- **The honest trade in non-equi joins:** memory is now bounded but *time* is not, and
  the join row/byte ceilings no longer bound those shapes. An `O(n x m)` join answers
  instead of being refused, so bound it with `ELYRASQL_QUERY_TIMEOUT_MS`, which
  interrupts one promptly and leaves the session usable. There is still no
  index-driven inequality (band) join, which is where MySQL wins these outright
  (`ON b.id BETWEEN a.id AND a.id + 2`: 32 ms there, 3507 ms here). Documented in
  [limitations](docs/limitations.md) rather than left to be discovered.
- **Attempted, measured and reverted:** `Value::Text(Arc<str>)`, to make text clones a
  refcount bump ([ESQL-52]). It is a trade, not a win — an `Arc<str>` costs ~25% more
  to build and ~60% more in a build/read/drop cycle, so a text-key join gained 19%
  while a plain `WHERE s LIKE '...'` scan lost 38%. Filters and scans are more common
  than fanout joins on text keys, so it was reverted rather than shipped on the
  strength of the one number that motivated it. The measurements are on the issue so
  the experiment is not repeated blindly. What survived is the decoder unification
  above.
- **Verification for the whole release**, all against a reference MySQL 8.4 on the same
  host: the 203-case differential battery (0 divergences), the threshold sweep (51/51
  queries identical at each of 15 row counts bracketing every internal threshold), a
  20-shape join battery written for this work (non-equi, band, `LEFT` with residual,
  equality-plus-residual, multi-step chains, NULL keys, `GROUP BY`/`ORDER BY` over
  each — 0 divergences), the robustness scenario (13/13 invariants including
  crash-recovery and resource exhaustion), the full workspace suite, clippy and fmt.
- **No migration.** No on-disk format change: 1.5.x databases open unchanged, and
  databases written by 1.6.0 still open in 1.5.x. Verified by round-tripping a
  database between the two builds, including non-ASCII text, JSON, NULLs and a text
  index lookup.

[ESQL-47]: https://wirelabs.youtrack.cloud/issue/ESQL-47
[ESQL-48]: https://wirelabs.youtrack.cloud/issue/ESQL-48
[ESQL-49]: https://wirelabs.youtrack.cloud/issue/ESQL-49
[ESQL-52]: https://wirelabs.youtrack.cloud/issue/ESQL-52

## [1.5.1] - 2026-07-29

A hardening patch found by auditing 1.5.0 rather than by a bug report: the collation
migration introduced in 1.5.0 held every write for a table in memory, which could
exhaust memory **at startup** on a large database -- a failure mode worse than any
query failing, because the server never comes up.

### Fixed

- **The collation migration could run out of memory on a large table (ESQL-44).**
  All index entries and re-keyed rows for a table were accumulated into one `Vec`
  before a single commit. Measured on a table with a non-ASCII text primary key and a
  text index: **441 MB at 300k rows**, growing linearly to **954 MB at 900k**. A table
  an order of magnitude larger would not have started at all.

  Writes are now flushed per scan batch, old index entries are deleted in batches
  before the rebuild, and the duplicate-detection set is scoped to the current batch
  (rows from earlier batches are already committed, so a storage probe finds those).
  Peak memory at 900k rows: **954 MB → 437 MB**, and now sub-linear — 3x the rows
  costs 1.27x the memory rather than 3x.

  Batching replaced per-table atomicity with **idempotent resume**: an already
  re-keyed row encodes to the key it is already stored under, and the version marker
  is only written once every table completes, so an interrupted upgrade is simply
  redone on the next start. The 1.5.0 documentation claimed per-table atomicity and
  has been corrected rather than left to mislead.

  Collision reporting is unchanged and was re-verified across the new batch boundary:
  a colliding pair seeded 6000 rows apart is still caught, names both keys, and
  refuses to start.

### Notes

Also audited and found **not** to be problems, recorded so the checks are not repeated
blindly: usernames are compared as raw bytes and are unaffected by the accent-
insensitive collation (`ådmin`, `admın`, `ADMIN` are all rejected against an `admin`
account); privilege enforcement, hostile-input handling and the durability invariants
are unchanged.

Two long-standing performance gaps were measured and filed rather than rushed:
joins that emit many rows are 10-16x MySQL ([ESQL-47]) and `ORDER BY ... LIMIT k` on
an unindexed column is ~2x ([ESQL-48]). Both were verified against a 1.4.15 binary and
are **not** regressions from 1.5.0. They sit in the hot join and sort paths, where
four wrong-results bugs have shipped before, so they get their own focused work.

[ESQL-47]: https://wirelabs.youtrack.cloud/issue/ESQL-47
[ESQL-48]: https://wirelabs.youtrack.cloud/issue/ESQL-48


## [1.5.0] - 2026-07-29

A behaviour-changing release: the default collation becomes MySQL's
`utf8mb4_0900_ai_ci`, so non-ASCII text sorts and compares by base letter. That
changes **which rows a query returns** for Nordic and other European text, and it
changes the bytes under which text is stored -- so existing databases are migrated
automatically on open. Alongside it, comma cross joins now stream instead of being
materialised, which was the last shape that could exhaust memory.

The threshold sweep against MySQL 8.4 now passes with an **empty allowlist**: all 51
queries at 15 row counts return byte-identical results. Every previously tolerated
divergence is gone.

**On-disk change:** text indexes and text primary keys are re-keyed on first open by
1.5.0. The migration runs before connections are accepted and commits one table at a
time, so a crash leaves each table fully on one side. Databases whose indexed text is
pure ASCII are not rewritten. Downgrading to 1.4.x after upgrading is not supported.

### Changed

- **The default collation is now `utf8mb4_0900_ai_ci`, matching MySQL 8 (ESQL-44).**
  The default character set was already `utf8mb4`. The default collation was
  `utf8mb4_general_ci`: case-insensitive but *accent-sensitive*, ordering non-ASCII
  text by codepoint. That put `Ærlig` after `zz` and made `WHERE s > 'cat'` match a
  different set of rows than MySQL — wrong answers for Nordic and other European
  text, not merely a different sort order.

  Text now folds to the base letters MySQL gives the same primary weight, including
  the expansions: `æ`→`ae`, `ß`→`ss`, `œ`→`oe`, `ø`→`o`, `å`→`a`, `é`→`e`. So
  `'café' = 'cafe'`, `'Straße' = 'Strasse'`, and ordering interleaves with ASCII:
  `Ærlig, ål, Ape, ape, cafe, café, cat, øl, Strasse, Straße, zz`.

  The folding table is **derived from MySQL's own weight strings** rather than
  written by hand, so it cannot disagree with the collation it implements. (Reading
  the spec through the `mysql` CLI, which connects as `latin1_swedish_ci`, initially
  reported `'Æ' = 'æ'` as false — measuring through a correctly configured client
  was necessary to get this right.)

  **The advertised collation string changed in the same release as the semantics,
  never before it.** Reporting `utf8mb4_0900_ai_ci` while implementing something
  else would mislead ORMs and migration tools that read `@@collation_server`.

  *Known limitation:* characters with their own primary weight in MySQL (`þ`, `ŋ`,
  `ı`) are case-folded but not reduced to ASCII, and non-Latin scripts order by
  codepoint rather than full UCA weights.

- **Automatic migration for existing databases (ESQL-44).** The collation folding
  feeds the on-disk key encoding, so this changes the bytes under which text is
  stored: secondary indexes, `UNIQUE` constraints and **text primary keys**. Without
  a migration a row written as `æble` would afterwards be looked up as `aeble` and
  not be found — silent row loss.

  Databases now carry a collation version. On open, an older database has its text
  index entries rebuilt and its text-primary-key rows re-keyed, before any connection
  is accepted, so no query can observe a half-migrated keyspace. The migration is
  idempotent and the version marker is written only once every table is done, so an
  interrupted upgrade resumes on the next start. Two rows
  whose keys become equal under the new folding (`æ` and `ae`) are reported as a
  collision naming both keys rather than one silently overwriting the other.

  **Pure-ASCII keys fold identically and are not rewritten**, so most databases
  migrate with no data movement at all.

- **Window functions are 25–35% faster (ESQL-46).** The projection was rebuilt for
  every row: `map_expr` cloned the whole expression tree, the window list was searched
  by *deep* `Expr` equality, a literal node was allocated from the value, and the
  result was re-interpreted — per row, per projection item. Each item is now classified
  once (is a window call / contains one / contains none), so the ordinary shapes do no
  expression work at all. Partition keys are also raw collation bytes instead of a
  `String` built by mapping each byte through `as char` (which re-encodes every byte
  ≥ 0x80 as multi-byte UTF-8) with a clone per row, and an unpartitioned window skips
  the hashing entirely. Measured on 200k rows: `LAG`/`LEAD` **48.5 ms → 32.9 ms**
  (parity with MySQL), `SUM` over a partition **64.5 ms → 40.9 ms**, partitioned
  `ROW_NUMBER` **77.0 ms → 54.0 ms**. The remaining gap is materialising every row
  before computing, which needs streaming windows.

### Added

- **Cross joins now stream, so their product is never buffered (ESQL-39).** A
  comma-separated cross join (`FROM a, b, c WHERE ...`) was the shape that grew the
  process to **97 GB** before the OS killed it, and after 1.4.13 it was merely refused
  by the row ceiling. It is now streamed: the join chain gained an *unkeyed* step, and
  one driving row's expansion recurses per step instead of being collected, so memory
  is O(chain depth) rather than O(product). Partners are still materialised — bounded
  by their table size, exactly as the existing keyed steps are — but the product is
  not, and the product is what explodes. Measured: `COUNT(*)` over 27 million
  combinations completes with resident memory **flat at 16 MB**, and the original
  4000³ workload peaks at **19 MB** instead of 97 GB. `GROUP BY`, `ORDER BY … LIMIT`,
  aggregates, and a comma entry mixed with an explicit `JOIN` all stream too, and all
  match MySQL 8.4 exactly.

  The shapes the chain still declines — non-equi joins, `FULL`, derived tables, a
  partner larger than the row cap — continue to materialise and remain bounded by the
  ceilings below.
- **Materialising joins are now bounded by memory, not just row count (ESQL-39).**
  The 1.4.13 ceilings counted rows, which is a poor proxy: 20M rows measured about
  5.4 GB for a narrow schema, but a wide row costs many times a narrow one for the
  same count, so the row ceiling could be satisfied while memory was not.
  `ELYRASQL_JOIN_MAX_BYTES` (default 2 GiB) bounds the bytes buffered across all
  concurrent joins, with each join sampling the width of the rows it actually emits
  (left ++ right, not just the driving row — sampling only the left side
  under-estimated a two-table join by roughly half). Verified with 1.5 KB rows and
  eight concurrent cross joins against a 512 MiB ceiling: the byte ceiling is what
  binds (14 604 refusals naming it, none naming the row ceiling), the process stays
  healthy, and a legitimate join afterwards still works, so nothing leaks.

  Documented honestly as **approximate**: reservations are taken in blocks and the
  allocator retains freed memory, so peak RSS ran ~2.3x the ceiling in that test.
  It is a safety net that keeps the process alive, not an accounting guarantee.
  Real spilling for the shapes that still materialise remains open on ESQL-39.

### Fixed

- **A statement containing non-ASCII text could panic the connection.** The keyword
  sniffers that detect user-management and procedure statements sliced the SQL by
  byte offset (`sql[..kw.len()]`), which panics when that offset lands inside a
  multi-byte character: `SELECT 'æ'='ae'` has a character spanning bytes 8..10 and
  `"drop user"` is 9 bytes long, so the worker aborted and the client saw a lost
  connection. Both sniffers now compare bytes. Found by running the release
  reproduction against the Docker image rather than trusting a clean test suite —
  the accent-insensitive collation makes such literals ordinary, so this moved from
  obscure to reachable.

- **`ORDER BY` on a non-projected column failed alongside a window function.**
  `SELECT amt, RANK() OVER (ORDER BY amt) FROM t ORDER BY id` was rejected with
  "ORDER BY references unknown output column", while the same query without the window
  function worked and MySQL accepts both. The window path only had the output columns
  to sort by; it now falls back to the base row, which is still index-aligned with the
  output. Found by a regression test written for the optimisation below — the test
  caught a compatibility gap rather than a regression.


## [1.4.15] - 2026-07-28

Query-planning release. Three related defects made ElyraSQL do far more work than
necessary on ordinary filters, all found by measuring against a reference MySQL
rather than by inspection:

- a range on a secondary index was executed as an index walk **regardless of how much
  of the table it matched**, so `WHERE amt > 0` cost 124 ms where the identical row
  set written as a non-index-usable `amt <> -1` cost 2.7 ms;
- `col IN (...)` was not recognised as index-usable **at all**, so even a five-element
  list scanned the whole table;
- and when a scan *was* right, `IN` was interpreted per row, so a 500-element list
  meant 100 million comparisons.

Measured on 200 000 rows: `amt > 0` **124 ms → 16.5 ms**, `IN (500 literals)`
**102 ms → 5.7 ms**, `IN (5 literals)` **4.5 ms → 1.3 ms**, and a scalar-subquery
filter **68.7 ms → 12.2 ms** — the last one had been recorded as a separate problem
and turned out to be the range defect all along. Several of these are now faster than
MySQL on the same data.

Because plan selection changed, correctness was verified more widely than usual: the
threshold sweep against MySQL 8.4, the 203-case differential battery, and the client
suites (Laravel/Eloquent, PHP native prepares, PyMySQL). That last group earned its
place — it caught a regression in this very work, described below. No on-disk format
change.

### Fixed

- **`col IN (...)` on an indexed column now uses the index (ESQL-46).** It was not
  recognised as index-usable at all, so even `g IN (1,2,3,4,5)` scanned the whole
  table and tested membership per row — 4.51 ms against MySQL's 0.33 ms, which does
  five index lookups. Values are now looked up through the index and unioned by
  storage key. Keys are collected before any row is fetched, so a list covering too
  much of the table (the same budget as a range) falls back to a scan having paid
  only for key lookups, and the rows that do qualify are fetched in one batched read
  rather than one per value. Measured on 200k rows: `IN (5 values)` **4.51 ms →
  1.26 ms**. Long lists are handled by the compiled predicate (below) rather than the
  index.
  List elements are coerced to the column's type before being encoded as keys —
  PDO binds integers as quoted strings, so `id IN ('1','2')` on an INT primary key is
  the ordinary shape from Laravel's `whereIn`; without coercion the lookup found
  nothing where a scan found the rows.
- **`IN` is now a set membership test in the compiled predicate (ESQL-46).** When a
  scan is the right plan, `column IN (numeric literals)` compiles to a hash-set test
  instead of walking the list for every row — 500 literals over 200k rows was 100M
  comparisons. Values are hashed canonically so `0.0`/`-0.0` agree, and the set's
  span is exposed as zone-map bounds so chunks outside it can still be skipped.
  Measured on 200k rows: `IN (500 literals)` **101.8 ms → 5.7 ms** (0.62x MySQL),
  `IN (subquery ~500)` 81.2 ms → 30.8 ms, and `IN` on an unindexed column 3.3 ms
  (0.35x MySQL). Shapes whose three-valued semantics the compiled form cannot
  reproduce — a `NULL` element, a non-numeric column, a non-literal element, an empty
  list — are declined so the interpreter keeps ownership of them.
- **A secondary-index range was used regardless of how much of the table it matched
  (ESQL-46).** An index range fetches every matching row by key, which is roughly an
  order of magnitude dearer per row than a sequential decode, so a wide range was far
  slower than simply scanning: `COUNT(*) FROM perf WHERE amt > 0` took **124 ms**,
  while the identical row set expressed as `amt <> -1` — not index-usable, therefore
  scanned — took 2.7 ms. Ranges now fall back to a scan past
  `ELYRASQL_INDEX_RANGE_MAX_FRACTION` (default 6%) of the table, decided *after* the
  index keys are walked but *before* any row is fetched, so a misjudged range costs
  only a key-only walk. Row counts come from `ANALYZE` statistics when present,
  otherwise from a key count cached per table and write epoch. Primary-key ranges are
  untouched, being sequential reads.

  Measured on 200k rows: `amt > 0` 124 ms → **16.5 ms**, `amt > 49999` 62.8 ms →
  9.2 ms, `g > 250` 50.1 ms → 9.0 ms, and selective ranges unchanged (`amt > 99000`
  1.2 ms). This also removed what looked like a separate scalar-subquery problem:
  `WHERE amt > (SELECT AVG(amt) FROM perf)` went 68.7 ms → **12.2 ms**, now slightly
  faster than MySQL — the subquery was never the cost, the wide range was.

### Added

- **Scenario suite runs in CI** (`.github/workflows/scenarios.yml`). The
  threshold-sweep, robustness and security scenarios added in 1.4.14 now gate every
  push and pull request, plus a nightly run:
    - *Threshold sweep* replays one query battery at row counts bracketing each
      internal threshold and diffs against a real MySQL 8.4 service. Known
      divergences are allowlisted by **exact SQL** with the issue that will remove
      them; a near-miss of an allowlisted query still fails, which was verified
      deliberately (`ORDER BY s ASC` is not covered by the entry for `ORDER BY s`).
    - *Robustness* asserts durability and concurrency invariants against a server it
      repeatedly `SIGKILL`s.
    - *Security* gates on per-action privilege enforcement and hostile input; the
      performance section is skipped there, since ratios on a shared runner are too
      noisy to gate a build.
  This closes the gap that let three wrong-result bugs reach released versions:
  every one of them hid *below* the thresholds the previous tests exercised.

## [1.4.14] - 2026-07-28

**Correctness release — upgrade from 1.4.13 is strongly recommended.** Two aggregate
bugs in 1.4.13 returned *wrong values* without any error. `COUNT(DISTINCT)` was
affected in both directions at once: it was multiplied by the parallel worker count
(so the answer depended on the machine's CPU count) and simultaneously deflated by
key collisions. The two defects partly cancelled, which is exactly why they survived
release. `GROUP_CONCAT` also ignored its `ORDER BY` entirely.

These were found by a new threshold-sweep scenario harness that replays one query
battery at sizes bracketing every internal threshold and diffs every result against
a reference MySQL 8.4 — built because the wrong-result bugs fixed in 1.4.13 had all
hidden *below* those thresholds. Robustness and security were verified in the same
pass: 13/13 durability and concurrency invariants hold (acknowledged commits survive
`SIGKILL`, no torn transactions after a mid-write kill with six concurrent writers,
totals conserved across ~3000 concurrent transfers) and 21/21 privilege and hostile-
input checks pass. No on-disk format change.

### Fixed

- **DISTINCT aggregates were multiplied by the parallel worker count (ESQL-42).**
  Present in 1.4.13 and earlier. `COUNT(DISTINCT g)` returned `8 x workers` for 8
  distinct values, so the answer **depended on the machine's CPU count** and was
  only correct with a single worker. Parallel aggregation merges partial results
  additively, which is right for COUNT/SUM/MIN/MAX but double-counts a value seen
  by two workers. `SUM(DISTINCT)` and `GROUP_CONCAT(DISTINCT)` were wrong the same
  way; `AVG(DISTINCT)` *looked* correct only because numerator and denominator were
  inflated equally, which is why spot checks missed it. DISTINCT counts are now
  derived from the merged set, and aggregations containing a DISTINCT stay on the
  single-pass path. Verified identical for 1/2/4/8/16 workers on 20k rows and
  differentially against MySQL 8.4.
- **`GROUP_CONCAT` ignored its `ORDER BY` (ESQL-43).** The clause parsed but was
  never applied, so values were concatenated in scan order --
  `GROUP_CONCAT(s ORDER BY s DESC)` returned the same string as with no ordering.
  Sort keys are now collected per value and applied when the aggregate finishes, so
  the result is well-defined even when partial aggregates are merged. Fixing it
  exposed a second defect: sort-key columns were not declared as columns the
  aggregate reads, so the scan decoded them as NULL, every key compared equal and
  the values silently stayed in scan order -- only visible when ordering by a column
  other than the concatenated one. Verified against MySQL 8.4 across 12 shapes
  (ASC/DESC, multiple keys, DISTINCT, `SEPARATOR`, and per group).
- **`COUNT(DISTINCT)` undercounted (ESQL-45).** Present in 1.4.13 and earlier, and
  the same root cause as the join-key bug fixed in 1.4.13 (ESQL-40) in a second
  place: the aggregator's distinct set was keyed by a collation key pushed through
  `from_utf8_lossy`, so values whose encoding contained a non-UTF-8 byte collided.
  `COUNT(DISTINCT g)` over 500 distinct integers returned **258**. It stayed hidden
  because ESQL-42 inflated the same results by the worker count while this deflated
  them, so the two bugs partly cancelled. The set is now keyed on raw bytes, which
  also means merging partials unions it correctly -- so `COUNT(DISTINCT)` keeps its
  parallelism (200k rows: 17.7ms single-threaded, 2.7ms on 16 workers), and only
  aggregates whose *value* merges additively (`SUM`/`AVG`/`GROUP_CONCAT`/`STDDEV`/
  `VAR` with DISTINCT) stay single-pass.
- **Integer-returning functions over an aggregate came back as text.**
  `LENGTH(GROUP_CONCAT(s))` reached the client as the string `"23"` instead of the
  number `23` (likewise `CHAR_LENGTH`, `ASCII`, `INSTR`, and the date-part
  functions), because computed-column type inference defaulted to `Text`. They are
  now typed as integers, so arithmetic and client-side decoding behave as in MySQL.

### Added

- **Threshold-sweep scenario harness** (`tests/scenarios/`). Both wrong-result bugs
  fixed in 1.4.13 (and ESQL-42 above) shipped because every existing test sat
  *below* the internal thresholds where they lived. The harness replays one query
  battery at sizes bracketing each threshold (1, 2, 127, 128, 129, 255, 256, 257,
  2047, 2048, 2049, 4095, 4097, 8193) and diffs every result against a reference
  MySQL 8.4, so a bug that only appears past a byte boundary, a join-strategy switch
  or a spill partition surfaces as a divergence.

## [1.4.13] - 2026-07-28

**Correctness release — upgrade from 1.4.12 is strongly recommended.** Two bugs in
1.4.12 returned *wrong results* for ordinary joins: hash-join keys collided for
integers in 128..255 (so a 1:1 join could return a cartesian product), and a bare
aggregate over a join appended 256 spurious zero rows. Neither raised an error, so
affected queries looked like they worked. Both are fixed and covered by the
differential battery, which grew from 183 to 203 cases against real MySQL 8.4.

The release also stops a single query from being able to kill the process
(unbounded join buffering), keeps the server responsive under CPU-heavy queries
without any configuration, and makes `ELYRASQL_QUERY_TIMEOUT_MS` genuinely
enforceable. No on-disk format change.

### Fixed

- **Wrong join results: the hash-join key was corrupted (ESQL-40).** Present in
  1.4.12. The key was a collation key pushed through `from_utf8_lossy`, but a
  collation key is an order-preserving **binary** encoding — so every byte that is
  not valid UTF-8 became U+FFFD and unrelated values collided into one key. Every
  integer in **128..255** hashed identically, so `SELECT COUNT(*) FROM t a JOIN t b
  ON a.id = b.id` on 300 rows returned **16556** instead of 300: those 128 ids
  formed a cartesian product. It affected ordinary equi-joins (hash join and the
  streaming N-table path) on integer keys in that range and on non-ASCII text keys;
  small tables escaped it only because the index nested-loop path handles them.
  The hash tables are now keyed on the raw collation-key bytes. Verified exact for
  n = 10…5000, zero mismatched pairs, and 9 join shapes matching real MySQL 8.4.
- **A bare aggregate over a join returned 256 spurious zero rows (ESQL-41).**
  Present in 1.4.12. `SELECT COUNT(*) FROM p JOIN q ON …` returned 257 rows — the
  right value, then 256 rows of `0`, constant regardless of table size.
  `SpillAgg::finalize` finalised all 256 spill partitions including the empty ones,
  and for an aggregate with **no GROUP BY** finalising an empty group set correctly
  means "zero rows in, one row out". Empty partitions are now skipped; an empty
  join still returns a single `0`, and `GROUP BY` over a join was never affected.
  Clients reading only the first row (most ORMs, for a scalar aggregate) saw the
  correct value, which is why this went unnoticed.

- **A materialising join could exhaust memory and kill the process (ESQL-39).**
  `FULL`, non-equi, derived-table and cross joins buffer their output, with no cap
  and no spilling: 8 concurrent 3-way cross joins over a 4000-row table took the
  process from 97 MB to **97 GB** RSS, after which the OS killed it — no panic and
  nothing logged. A single authenticated client could do this with one short query.
  Buffered rows are now bounded per join by `ELYRASQL_JOIN_MAX_ROWS` (default 10M)
  and across all concurrent joins by `ELYRASQL_JOIN_MAX_ROWS_TOTAL` (default 20M),
  both reserved through an RAII guard so a join that finishes, errors or unwinds
  returns its share. The per-join cap alone would not have bounded the server, since
  memory scales with concurrency. The same workload now plateaus at **5.4 GB** with
  the server healthy, and an unconstrained join fails with a message naming the
  limit rather than being killed. Streaming joins are unaffected — they never hold
  the join output.

- **A CPU-heavy query no longer makes the server unresponsive (ESQL-38).**
  Statement execution shares the async runtime's workers with the connection
  listener and every other session, so a long *synchronous* stretch — a join
  product, a sort, aggregation over materialised rows — monopolised a worker.
  With enough concurrent heavy queries the server stopped accepting connections
  entirely: measured with no query timeout configured, a fresh `SELECT 1` could
  not complete within 12 s. Those stretches now run through `block_in_place`, which
  hands the worker over and lets the runtime bring up a replacement, and the
  streaming join loops yield between driving rows. **No configuration is needed**,
  so this protects the default setup: 32 concurrent runaway queries (2× the core
  count) now leave a new connection answered in under 0.1 s. The query deadline
  from the previous entry still applies on top when configured.

- **`ELYRASQL_QUERY_TIMEOUT_MS` now actually stops a runaway statement
  (ESQL-32).** It previously wrapped execution in a wall-clock timeout, which can
  only take effect at an `.await` point — so a long stretch of synchronous CPU work
  ignored it entirely, and work already handed to a blocking thread ran to
  completion even after the client had given up. Measured before the fix: a 3-way
  cross join with a 2-second timeout gave the client nothing for 25 s and kept a
  core busy afterwards. The engine now carries a per-statement deadline and checks
  it inside its hot row loops — table scans, the nested-loop product, hash-join
  build and probe, join-chain expansion, sort-key evaluation, and **each parallel
  scan worker** (the case a wall-clock timeout can never reach). Every runaway
  shape tested (cross join, exploding equi-join with `ORDER BY`/`GROUP BY`,
  high-cardinality `GROUP BY`, expression sort over 800k rows) now aborts at the
  deadline with **zero** CPU left running, and with the timeout set the server kept
  answering new connections while 16 runaway queries were in flight. The deadline
  is armed per statement by a guard, so it is cleared on every exit path, and a
  trigger or procedure body inherits the outer statement's deadline rather than
  starting a fresh budget. `ELYRASQL_QUERY_TIMEOUT_MS` remains off by default, as in
  MySQL — with no timeout configured there is still no deadline to check
  (ESQL-38).
- **`REGEXP` compiled its pattern once per row.** Found by profiling the above: a
  `WHERE s REGEXP '...'` scan spent essentially all its time in `Regex::new`,
  recompiling the same constant pattern for every row. Compiled patterns are now
  cached (bounded, so patterns built from column values cannot grow it without
  limit), shared by `REGEXP`/`RLIKE`, `REGEXP_REPLACE` and `REGEXP_SUBSTR`. A
  `COUNT(*)` with a `REGEXP` filter over 800k rows went from **not finishing inside
  a 2-second budget to 0.12 s**.
- **`REGEXP` ignored the operand's collation (ESQL-37).** MySQL applies the
  operand's collation to `REGEXP`/`RLIKE`, and its default collation is
  case-insensitive, so `SELECT 'Hello' REGEXP 'h'` returns 1 there but returned 0
  here — silently different rows for any query relying on it. Case-sensitivity now
  follows the collation the same way comparisons already did: case-insensitive by
  default, case-sensitive for a `_bin` operand, with an inline `(?-i)` still
  overriding it. `REGEXP_REPLACE`/`REGEXP_SUBSTR` receive already-evaluated values,
  so they use MySQL's default (case-insensitive) behaviour, which is also what
  MySQL returns for `REGEXP_REPLACE('a1B2','[b]','x')` → `a1x2`. All expectations
  were taken from real MySQL 8.4 before implementing, and 12 REGEXP cases were
  added to the differential battery (now 195 cases, 0 divergences).

## [1.4.12] - 2026-07-27

Hardening release. A code audit found two denial-of-service vectors that a single
authenticated client could use to take the server down; both are fixed at the root.
The rest of the release closes the remaining unbounded-resource gaps — connections,
prepared statements and streamed parameters are now all bounded with
MySQL-compatible limits, errors and defaults — and completes inter-node encryption
by wrapping the Raft control plane in TLS. No on-disk format change.

### Added

- **TLS encryption for the Raft control plane (ESQL-31).** Leader election
  (`RequestVote`) and `AppendEntries` now run over TLS when the cluster TLS
  variables are set, completing inter-node encryption (replication was already
  covered by ESQL-30). It reuses the same `ELYRASQL_CLUSTER_TLS_CERT`/`_KEY`
  (the certificate each node presents), `ELYRASQL_CLUSTER_TLS_CA` (roots used to
  verify peers), and `ELYRASQL_CLUSTER_TLS_SERVER_NAME`. Each node **verifies**
  its peers' certificates, so a node that cannot verify a peer cannot form the
  cluster with it. The control-plane framing (`send`/`recv`), `handle_control`,
  and `append_rpc` were generalised over the transport, and peer connections are
  now a boxed TCP-or-TLS stream. Plaintext remains the default (with a warning).
  Verified end-to-end: a 3-node cluster elects a leader and replicates writes via
  Raft over TLS with the correct CA, and forms **no** cluster (no leader elected,
  all writes rejected) when peers present a certificate a wrong CA cannot verify.

- **Connection admission control (ESQL-33).** The listener accepted connections
  without limit, so a client could exhaust memory and file descriptors just by
  connecting. Concurrent connections are now bounded by
  `ELYRASQL_MAX_CONNECTIONS`, mirroring MySQL's `max_connections` **and its
  default of 151**. A surplus connection receives error **1040** (`Too many
  connections`) as its first packet — the same thing MySQL does, so clients report
  the real reason instead of a bare connection reset — and refusals are exposed as
  `elyrasql_connections_refused_total` for alerting. Slots use the same RAII
  permit pattern as prepared statements, so they are returned when a connection
  ends however it ends. `0` disables the limit.
- **A connection slot is reserved for administrators (ESQL-36).** As in MySQL, one
  connection *beyond* `ELYRASQL_MAX_CONNECTIONS` is admitted, and it is served only
  if it authenticates as an `Admin` account — so an operator can still connect to
  diagnose and `KILL` sessions on a saturated server. A non-admin that takes the
  slot is refused with **1040** *after* authenticating, deliberately reporting "too
  many connections" rather than an authentication failure, which would wrongly
  suggest bad credentials (verified: a wrong password still returns an auth error,
  not 1040). This required a decision that can only be made once the account is
  known, so `elyra-wire` gained a `post_auth_check` hook in its shim contract
  (defaulting to "allow", so other implementors are unaffected).
- **Streamed statement parameters are bounded (ESQL-35).**
  `COM_STMT_SEND_LONG_DATA` appended to a per-statement buffer that is only
  cleared by an execute, so a client that streamed data and never executed could
  grow memory without limit — side-stepping the statement cap below, since one
  statement was enough. Accumulated parameter data is now bounded by
  `ELYRASQL_MAX_ALLOWED_PACKET`; passing it releases the buffer **immediately**
  and fails the following `EXECUTE` with error **1153** (`Got a packet bigger than
  'max_allowed_packet' bytes`), after which the statement is reusable. The error
  is deliberately reported at execute time because `COM_STMT_SEND_LONG_DATA` has
  no reply in the protocol — which is also where MySQL reports it. Verified with a
  raw-protocol client: 3 MiB streamed against a 1 MiB budget left the server at
  14 MB RSS, and legitimate long data (300 KiB in three chunks) still assembles
  correctly.
- **Prepared statements are capped server-wide (ESQL-34).** A client could
  previously `COM_STMT_PREPARE` without ever closing, growing the per-connection
  statement map without limit. The number of live prepared statements is now
  bounded across all connections by `ELYRASQL_MAX_PREPARED_STMTS`, mirroring
  MySQL's `max_prepared_stmt_count` **and its default of 16382**; past the limit a
  prepare is answered with error **1461** (`Can't create more than
  max_prepared_stmt_count statements`) and the connection remains usable. The
  limit is global rather than per connection for the same reason MySQL's is: a
  per-connection cap bounds nothing while the connection count itself is unbounded
  (ESQL-33). Slots are tracked with an RAII permit, so they are returned when a
  statement is closed, when a connection ends *without* closing its statements,
  and on an unwind — a leaked global counter would otherwise eventually refuse
  every prepare server-wide. Wire behaviour differential-verified against real
  MySQL 8.4 (identical: three prepares accepted at a limit of three, then error
  1461 with SQLSTATE 42000).

### Fixed

- **Denial-of-service: `NTILE()` iterated its bucket argument.** `NTILE(N)`
  looped `N` times regardless of table size, so `NTILE(1000000000000)` on a
  10-row table pinned a CPU core **indefinitely** — and the work continued after
  the client disconnected. With one such query per core the server stopped
  answering *new* connections entirely (measured: 16 queries → 1449% CPU, a fresh
  `SELECT 1` timing out). Buckets are now assigned per row (O(rows)), which also
  matches MySQL for a bucket count larger than the row count (each row in its own
  bucket, surplus buckets empty). Distributions differential-verified against
  real MySQL 8.4 (`NTILE(3)`/`(4)`/`(7)`/`(20)`).
- **Denial-of-service: unbounded string expansion.** `REPEAT`, `SPACE`, `LPAD`
  and `RPAD` allocated whatever the arguments asked for, so `SPACE(10000000000)`
  requested 10 GB and `REPEAT('x', 200000000)` grew the process to 414 MB from a
  single query. They now return `NULL` past `ELYRASQL_MAX_ALLOWED_PACKET` (default
  64 MiB) — the same behaviour as MySQL past `max_allowed_packet`, whose default
  is also 64 MiB (verified against MySQL 8.4).

### Changed

- The string-expansion budget introduced in this same unreleased cycle is now
  named **`ELYRASQL_MAX_ALLOWED_PACKET`** (was `ELYRASQL_MAX_STRING_BYTES`). It is
  the same 64 MiB default, but it now also bounds streamed statement parameters,
  so one knob covers both — exactly as MySQL's `max_allowed_packet` does. No
  released version used the old name.
- **`ELYRASQL_QUERY_TIMEOUT_MS` is now documented accurately.** It can only
  interrupt a statement that yields; it does **not** preempt a long synchronous
  CPU loop (verified: a 2 s timeout did not stop the `NTILE` spin above). The
  previous wording implied the client was always unblocked. Treat it as a latency
  bound for I/O-waiting queries, not a CPU-DoS guard (ESQL-32).

## [1.4.11] - 2026-07-22

Robustness & cluster-security release: memory fail-safes for `IN (SELECT)` /
`DISTINCT`, and optional TLS encryption for the replication transport. No on-disk
format change.

### Added

- **TLS-encrypted replication (ESQL-30).** The primary↔replica stream (which
  carries the whole data set) can now be encrypted: set
  `ELYRASQL_CLUSTER_TLS_CERT`/`_KEY` on the primary and `ELYRASQL_CLUSTER_TLS_CA`
  on the replica. The replica **verifies** the primary's certificate (no
  accept-any mode, so it is not MITM-vulnerable), giving confidentiality + server
  authentication; combined with `ELYRASQL_CLUSTER_SECRET` this is mutual
  authentication. Plaintext remains the default (with a loud warning). The Raft
  control plane is not yet TLS-wrapped (ESQL-31).
- **Fail-safe memory bounds for `IN (SELECT ...)` and `DISTINCT` (ESQL-28).** An
  `IN (SELECT ...)` over more than `ELYRASQL_IN_SUBQUERY_MAX` rows (default
  1,000,000) or a `DISTINCT` over more than `ELYRASQL_DISTINCT_MAX` rows (default
  5,000,000) now errors with a clear message instead of buffering an unbounded set
  and risking OOM (rewrite `IN (SELECT)` as a `JOIN`/`EXISTS`).

### Testing

- Locked in **N-table left-deep join streaming** (ESQL-29) with a regression test:
  chains of two or more `INNER`/`LEFT` equi-joins feeding `ORDER BY`/`GROUP BY`
  stream through the spilling sorter/aggregator (the join output is never fully
  materialised). The remaining materialising cases (`FULL`, non-equi, derived
  table, `RIGHT`-in-a-chain) are correct, rare, and documented.

## [1.4.10] - 2026-07-22

Vector release: the HNSW index is now maintained **incrementally** and **persisted**
across restarts, so write-heavy and post-restart vector workloads no longer pay a
full graph rebuild. The on-disk cache lives in a sibling `<data>.vidx/` directory,
so the authoritative single file is unchanged.

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

[1.5.1]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.5.1
[1.5.0]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.5.0
[1.4.15]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.15
[1.4.14]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.14
[1.4.13]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.13
[1.4.12]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.12
[1.4.11]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.11
[1.4.10]: https://github.com/kwhorne/ElyraSQL/releases/tag/v1.4.10
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
