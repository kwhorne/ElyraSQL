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
  back on return). **Cursors** (`DECLARE ... CURSOR FOR`, `OPEN`, `FETCH ...
  INTO`, `CLOSE`) and **condition handlers** (`DECLARE {CONTINUE|EXIT} HANDLER
  FOR {NOT FOUND | SQLEXCEPTION | SQLSTATE '...' | <code>} <action>`) are
  supported; a handler action is a single statement (not a `BEGIN ... END`
  block), and handlers are scoped to the whole procedure body.
- Row-level triggers are supported: `CREATE TRIGGER name {BEFORE|AFTER}
  {INSERT|UPDATE|DELETE} ON t FOR EACH ROW <body>`, with `NEW.col`/`OLD.col`.
  BEFORE bodies support `SET NEW.col = expr`; AFTER bodies run arbitrary DML.
  Firing is depth-guarded against runaway recursion. Triggers fire on
  single-table INSERT/UPDATE/DELETE (not on multi-table or the upsert variants
  REPLACE/ON DUPLICATE/IGNORE).
- **Materialized views**: `CREATE MATERIALIZED VIEW v AS <select>` stores the
  result as a real table; `REFRESH MATERIALIZED VIEW v` recomputes it; `DROP
  MATERIALIZED VIEW v` removes it. Views **auto-refresh on read** when a base
  table has changed since the last refresh (detected via per-table write
  counters); this is a full recompute, not incremental delta maintenance.
- **Named windows** are supported: `... OVER w ... WINDOW w AS (PARTITION BY ...
  ORDER BY ...)`, including `OVER (w ...)` inheriting a named window.
- Not yet: `RANGE`/`GROUPS` numeric value-offset frames (only
  `UNBOUNDED PRECEDING .. CURRENT ROW`/`UNBOUNDED FOLLOWING` for `RANGE`),
  correlated subqueries combined with aggregation over a join, user-defined
  functions, and events.

## Constraints & integrity

- Enforced: `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `CHECK`, and `FOREIGN KEY`.
- Foreign keys reference a primary key or unique index; both `ON DELETE` and
  `ON UPDATE` `RESTRICT`/`NO ACTION`/`CASCADE`/`SET NULL` are enforced.
- Not yet: multi-level (recursive) cascades, and deferred constraint checking.

## Query planning

- Range scans and index nested-loop joins are **single-column**; composite
  ranges fall back to a scan.
- Equi joins (INNER/LEFT/RIGHT) use a hash join with a cost-based build side
  (the smaller relation for INNER; an index nested-loop join when the driving
  side is small and the partner is indexed). Large INNER equi-joins whose inputs
  are already sorted on the join key (e.g. clustered primary-key scans) use a
  streaming **merge join** (no hash table, ordered output). `FULL` and non-equi
  joins use nested-loop.
- Explicit **INNER-join chains over base tables are reordered cost-based**: the
  planner drives from the smallest relation and always extends along an
  equi-join predicate, keeping intermediate results small. Reordering is
  alias-aware and applies only when every join is a single equi-connector;
  outer joins, non-equi/multi-condition ON, and derived tables keep the
  written order.
- `ANALYZE TABLE` records row-count and per-column statistics (NDV, null count,
  min/max, and an **equi-height histogram** built from a reservoir sample),
  surfaced as `information_schema.tables.TABLE_ROWS` and
  `information_schema.column_statistics` (including a JSON `HISTOGRAM`). The
  planner estimates WHERE-predicate selectivity from the histograms to order
  comma cross-joins by estimated (not just raw) row counts, reorders explicit
  INNER-join chains cost-based, and picks hash-join build sides by live size.
  Multi-column/correlated histograms are not modelled.
- `ORDER BY` is memory-bounded: `ORDER BY ... LIMIT` uses a top-N heap, and
  large unbounded sorts spill sorted runs to temp files (external merge sort,
  `ELYRASQL_SORT_MAX_ROWS`). `GROUP BY` with many distinct groups falls back to
  **partitioned spill** aggregation (rows routed to partitions by group-key
  hash, spilled to temp files, aggregated per partition) so memory stays
  bounded; a single skewed partition past `ELYRASQL_GROUP_MAX_GROUPS` still
  errors. In-transaction reads materialize their working set.
- Spilled `GROUP BY` output is ordered per partition (add `ORDER BY` for a
  defined order).

## Partitioning

- `CREATE TABLE ... PARTITION BY RANGE|LIST|HASH (<pk column>) (...)` records a
  partitioning scheme over the primary key, exposed in
  `information_schema.partitions`. Partitions are **managed primary-key ranges**
  (metadata over the clustered PK), not physically separate files:
  `ALTER TABLE t DROP PARTITION p` / `TRUNCATE PARTITION p` cheaply delete a
  partition's rows (a range/`IN` delete, with index cleanup), and queries with a
  PK predicate prune automatically via clustered range scans. Boundaries are not
  enforced on INSERT, and this is single-node (partitioning does **not** shard
  writes across nodes — horizontal write scale-out would require distributed
  sharding, which is out of scope by design).

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
- A single writer serializes all commits (an inherent property of the ACID
  single-file engine — there are no parallel writers or write sharding).
  Throughput under high write concurrency comes from **group commit**: many
  pending writes — now including validated **transactional** commits — are folded
  into one transaction (one fsync), so N concurrent transactions cost one fsync
  rather than N. First-committer-wins ordering and write-write conflict detection
  are preserved within a batch. The expensive per-statement work (parsing,
  constraint checks, encoding, index maintenance) runs in the connection tasks
  in parallel; only the final commit is serialized.

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
  (read/write/admin) privileges granted **globally** or **per table**.
  **Roles** are supported: `CREATE ROLE` / `DROP ROLE`, `GRANT <role> TO <user>`
  / `REVOKE <role> FROM <user>`; a user inherits the global and per-table grants
  of every role granted to them. `GRANT ... ON db.*` is accepted and maps to a
  global grant (single default database). Reads are always allowed at the global
  baseline (grants only raise write/admin). **Per-column** SELECT grants are
  enforced (`GRANT SELECT(col, ...) ON t TO u`): a column-restricted user may
  only read those columns of `t` — querying an ungranted column (including via
  `SELECT *` or a `WHERE`/`ORDER BY` reference) is denied. Enforcement covers
  single-base-table selects; a column-restricted table used in a join or
  subquery is denied (deny-safe).
- An optional **audit log** (`--audit-log <path>`) appends one tab-separated
  line per executed statement (`timestamp  conn_id  user  OK|ERR  sql`).
- Hot and offline backup/restore, plus an append-only binlog for point-in-time
  recovery (`--binlog` + `elyrasql binlog-replay`). Binlog rotation/pruning is
  manual; there is no incremental (block-level) backup.
- Primary → replica replication (read replicas, warm standby), asynchronous by
  default with **semi-synchronous** (`--semi-sync-ms`) and **quorum /
  synchronous** modes (`--sync-replicas N`, optional `--sync-strict`). A commit
  waits until `N` replicas acknowledge; in strict mode a timeout fails the
  commit-confirmation instead of silently degrading. The barrier runs *after*
  the local commit (which is always durable), so it shrinks — but does not fully
  close — the failover data-loss window (there is no pre-commit 2-phase
  replication / multi-primary).
  **Automatic failover** is available in `cluster` mode via Raft-style leader
  election (majority quorum, leader-only writes/fencing) with the **election
  restriction**: a node only votes for a candidate at least as up-to-date (by
  LSN) as itself, so an elected leader has every quorum-acknowledged write.
  Together with `--sync-strict` this gives no-data-loss failover for
  acknowledged writes (the sync barrier still runs after the local commit, so
  it is not a pre-commit 2-phase protocol). A reconnecting replica catches up
  **incrementally from the binlog** (streaming only the delta since its last
  applied LSN), falling back to a full snapshot only when the binlog is disabled
  or the needed segments were purged; the LSN counter resumes from the binlog
  across restarts. **Cluster membership is dynamic**: `elyrasql cluster-ctl
  --action add|remove` changes membership at runtime (send to the leader, which
  propagates it to followers via heartbeats); add one node at a time and start a
  new node before adding it. An even-node cluster can, rarely, need an extra
  election round to break a tie — run an odd number of nodes. Election state
  (current term + vote) is persisted to a `<data>.raftstate` file so a restarted
  node never double-votes in a term (a Raft safety requirement). A unit-tested
  **Raft log core** (`raftlog`) implements the consensus-critical pieces — the
  AppendEntries consistency check with conflicting-suffix truncation, the
  quorum/current-term commit rule, apply-only-when-committed, and the §5.4.1
  election restriction. **Routing the live cluster write path through this log**
  (leader append → quorum commit → apply, followers applying to the leader's
  commit index) for pre-commit (2-phase) durability is the remaining
  integration step; today durability is async by default, with quorum/strict-
  sync plus the LSN-aware election restriction giving no-data-loss for
  acknowledged writes.

## Wire protocol

- Prepared statements can desynchronize across repeated
  `COM_STMT_CLOSE` → `COM_STMT_PREPARE` cycles on one connection with strict
  clients (an upstream library limitation). Statement reuse and pooled clients
  are unaffected.
- **`LOAD DATA INFILE`** reads a **server-side** file and bulk-inserts it
  (requires ADMIN, like MySQL's `FILE` privilege): `LOAD DATA INFILE '<path>'
  INTO TABLE t [FIELDS TERMINATED BY '...'] [ENCLOSED BY '...'] [LINES
  TERMINATED BY '...'] [IGNORE n LINES] [(cols)]`, with `\N` for NULL. Client-
  side `LOAD DATA LOCAL INFILE` (streaming the file over the wire) is not
  supported.
- Authentication uses `mysql_native_password`; connection salts now come from the
  OS CSPRNG. `caching_sha2_password` (MySQL 8's default) is **not** implemented —
  the wire library does not drive its multi-round fast/full-auth exchange — but
  MySQL 8 clients automatically negotiate down to `mysql_native_password`.

## Roadmap

Candidate next steps, roughly in order of value:

1. Pre-commit (2-phase) synchronous replication for true zero-data-loss failover
   (today's quorum barrier runs after the local commit).
2. Cost-based JOIN reordering for explicit join chains; a merge join.
3. Cursors and condition handlers in stored procedures.
4. Multi-level (recursive) cascades and deferred constraints.
5. Per-column `_bin` in `ORDER BY` / `GROUP BY` / `DISTINCT` / join keys.
6. A persistent spatial index (R-tree) and polygon/geodesic operations.
7. Dynamic cluster membership (online add/remove nodes).
8. Roles and per-database / per-column privileges; audit logging.
9. `caching_sha2_password` and `LOAD DATA INFILE`.

Many earlier roadmap items have shipped: per-column `COLLATE`/`_bin`, scoped
(per-table) privileges, spill-to-disk sorts/aggregations, cost-based hash joins
with statistics, slow-query log + Prometheus metrics, pessimistic table locking,
quorum/synchronous replication, and automatic failover with incremental
catch-up.

Have a need that isn't listed? Open an issue on
[GitHub](https://github.com/kwhorne/ElyraSQL/issues).
