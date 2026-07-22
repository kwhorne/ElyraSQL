# Limitations

ElyraSQL 1.0 is stable and broadly MySQL-compatible. This page is an honest
inventory of what is **not** yet implemented (or differs from MySQL), so you can
judge fit before deploying.

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
  block), and handlers are scoped to the whole procedure body. **`OPEN` buffers
  the cursor's full result set in memory** (it is not a streaming server-side
  cursor), so cursors are intended for the modest result sets typical of
  procedural logic, not for iterating huge tables.
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
- `INSERT ... SET col = val, ...` (MySQL shorthand) is supported — it is
  rewritten to `INSERT ... (cols) VALUES (...)` before parsing, including
  `ON DUPLICATE KEY UPDATE`.
- Comma-style multi-table `UPDATE t1, t2 SET ... WHERE ...` is supported — it is
  rewritten to `UPDATE t1 CROSS JOIN t2 SET ... WHERE ...` (the WHERE supplies
  the join condition) before parsing.
- `GROUP BY ... WITH ROLLUP` is supported: it adds a subtotal row for each
  grouping prefix and a grand-total row (dropped group columns are NULL),
  re-aggregating base rows per level so `AVG`/`MIN`/`MAX` stay correct. `ORDER
  BY`/`LIMIT` apply to the combined result (NULLs sort first).
- All the bitwise operators are supported: `&`, `|`, `^`, `<<`, `>>`, and unary
  `~`. They compute on 64-bit **unsigned** integers and return `BIGINT UNSIGNED`
  (`Value::UInt`), matching MySQL exactly — e.g. `~5` is `18446744073709551610`
  and `1 << 63` is `9223372036854775808`. Unsigned integer arithmetic (`+`,
  `-`, `*`, `%`) on such values is also exact. (`~` is bridged by rewriting
  `~x` to `(x ^ 18446744073709551615)` since no SQL dialect parses the prefix.)
- Supported beyond the basics: multi-table `UPDATE`/`DELETE` via `JOIN`,
  `INSERT ... SELECT`, `CREATE TABLE ... AS SELECT`, `COUNT(DISTINCT ...)`,
  `UNION ALL`/`INTERSECT`/`EXCEPT`, `WITH RECURSIVE`, row/tuple `IN`, window
  functions incl. `LAG`/`LEAD`/`NTILE`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`,
  the `<=>` null-safe operator, `IS [NOT] TRUE/FALSE/UNKNOWN`,
  `LAST_INSERT_ID()`/`ROW_COUNT()`, `@@`system variables, `CONVERT()`,
  `MD5`/`SHA*`/`SOUNDEX`/`REGEXP_REPLACE`, and statistical/bitwise aggregates.

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
- **Indexed `ORDER BY ... LIMIT` (top-N without a sort).** With no `WHERE`
  filter, an ordered `LIMIT` is served by an ordered index/clustered walk that
  stops after `OFFSET + LIMIT` rows — no full scan, no sort:
    - `ORDER BY <primary-key prefix> ASC|DESC LIMIT n` walks the clustered
      keyspace forward (ASC) or backward (DESC).
    - `ORDER BY <indexed column(s)> ASC|DESC LIMIT n` walks a secondary index in
      key order and follows each entry to its row. A `COLLATE` override or an
      expression key skips it. A **composite** index must have every column
      `NOT NULL` (indexes omit NULL tuples, so a NULL in any key column would drop
      a row from the walk).
    - A **primary-key tiebreaker** stays on the fast path: since a non-unique
      secondary index stores `(value, clustered pk)`, `ORDER BY <indexed col>
      [DESC], id [DESC]` (a grid's stable-sort tiebreaker) walks the index
      directly. All terms must share a direction and any trailing terms must be
      the primary-key columns in order.
    - A **nullable single-column** index is fully supported in **both directions**
      (including with a PK tiebreaker). Single-column indexes built on 1.4.7+ store
      NULL-keyed rows under a companion `indexnull::` keyspace, so the ordered walk
      is a complete MySQL ordering — NULLs first for `ASC`, last for `DESC`, each
      ordered by the clustered PK — with no data scan and no fallback. (Indexes
      built before 1.4.7 use the older NULL scan / sorter fallback until rebuilt;
      a **composite** index still requires every column to be `NOT NULL`.) The NULL block is fetched by
      a budgeted clustered scan; if NULLs are so rare that the budget
      (`ELYRASQL_ORDER_SCAN_BUDGET`) is exhausted before the block is known
      (mainly an `ASC` concern), it falls back to the sorter below.
    - A `WHERE` filter is applied as a residual **during** the ordered walk, so a
      filtered grid page (`WHERE ... ORDER BY <col> LIMIT n`) is still served
      without a full sort. To stay safe when the filter is very selective, the
      walk is capped by the same examine budget; if it cannot fill `n` rows within
      budget it falls back to the sorter below (a selective filter has few matches,
      so that sort is cheap).
    - **Deep `OFFSET`** on the fast path (no residual filter) steps over the
      leading `OFFSET` rows at the index/clustered level **without reading their
      rows**, so paging deep into a result stays cheap (index steps, not row
      reads). With a residual filter the pre-offset rows must still be read to be
      counted.
    - An ordered `LIMIT` **inside a transaction** falls back to the sorter below
      (correct, not yet index-accelerated).
- **Single-table** `ORDER BY` (the fallback) is memory-bounded: `ORDER BY ...
  LIMIT` uses a top-N heap and large unbounded sorts spill sorted runs to temp
  files (external merge sort, `ELYRASQL_SORT_MAX_ROWS`). This spilling path now
  runs **inside
  transactions too** (streaming the snapshot+overlay via the session cursor), not
  just in autocommit. `GROUP BY` with many distinct groups uses **partitioned
  spill** aggregation (rows routed to partitions by group-key hash, spilled to
  temp files); when column statistics predict a large group count the planner
  goes straight to the spilling path instead of running the in-memory pass,
  hitting the cap, and re-scanning (which previously cost two full scans). A
  single skewed partition past `ELYRASQL_GROUP_MAX_GROUPS` still errors. When a
  table has **no statistics** (never `ANALYZE`d) and turns out to have a huge
  group count, the in-memory pass can still overflow and fall back to the
  spilling path, costing a second scan; running `ANALYZE TABLE` avoids this.
  Spill files are read back with a size-guarded length prefix, so a corrupt
  temp file is rejected rather than triggering a giant allocation, and stale
  spill files left by a killed process (SIGKILL) are reclaimed at startup (only
  files owned by confirmed-dead PIDs are removed).
- **Join + `GROUP BY` on an indexed partner streams**: for the common shape
  `FROM driving JOIN partner ON driving.k = partner.<pk|indexed> [WHERE]
  GROUP BY ...` (INNER or LEFT), the driving table is scanned incrementally, the
  partner is probed by index, and joined rows feed the spilling aggregator
  directly -- so a large fact-to-dimension join with grouping is bounded by the
  group state (which spills), not the join output size. Also streamed: the same
  join shape with `LIMIT` and no grouping (early-stop index nested-loop).
- **Left-deep `JOIN` chains stream** (two or more tables): each partner is built
  into a hash table and the driving table is scanned incrementally; joined rows
  feed straight into the spilling sorter (`ORDER BY`, top-N heap / external merge)
  or the spilling aggregator (`GROUP BY`). The join output is never fully
  materialised, so a large fact-to-dimensions join with sorting or grouping is
  bounded by the partner hash tables plus the sorter/aggregator, not
  `|driving| x fanout`. INNER, LEFT and RIGHT are supported (autocommit only). A
  two-table `RIGHT JOIN` streams by rewriting it to the equivalent `LEFT JOIN`
  with the output columns reordered back to the query's order.
- **INNER comma joins stream too**: `FROM a, b, c WHERE a.k = b.k AND b.j = c.j`
  is normalised to an explicit `JOIN` chain (using the WHERE equi-predicates as
  `ON`), so it gets the same cost-based reordering and streaming as explicit
  joins when every table is connected by an equi-predicate.
- **Remaining materialising joins**: `FULL` joins, non-equi joins, derived-table
  joins, and `RIGHT` joins that are part of a multi-table chain (rather than a
  single two-table `RIGHT JOIN`) still build the full join result before
  `ORDER BY`/`GROUP BY` (correct, but not memory-bounded on very large such
  joins). `FULL` needs unmatched-build-side tracking; non-equi joins have no hash
  key to stream on; a derived table is materialised first. These shapes are rare
  and the materialising path is correct. Streaming left-deep chains of **more than
  two** tables (and complex join expressions) is tracked in ESQL-29; today they
  take the materialising `join_select` path.
- **`WHERE col IN (SELECT ...)` and `DISTINCT` collection are in-memory** (unlike
  `ORDER BY`/`GROUP BY`, which spill): the subquery's result set and the distinct
  set are buffered in RAM, so a query over an enormous such set can run out of
  memory. Bounding/spilling these is tracked in ESQL-28. Correlated subqueries
  execute as a nested loop (re-run per driving row, `O(N×M)`), not yet decorrelated.
- Uncommitted transaction writes are buffered in memory (not spilled to disk)
  until `COMMIT`/`ROLLBACK`. To keep this bounded, a transaction that stages more
  than `ELYRASQL_TXN_MAX_BYTES` (default 1 GiB) of writes has its next write
  rejected with an error rather than exhausting server memory. `SAVEPOINT` is
  cheap: it records an undo-log marker (O(1)) rather than copying the staged
  write set, and `ROLLBACK TO` reverts only the changes made since the savepoint
  (`reads`/locked rows are kept, which only makes commit-time validation more
  conservative, never incorrect).
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
  error `1213` rather than blocking). Serializable validates every range the
  transaction scanned by re-reading it at commit, so commit cost scales with the
  read set; a single scanned range over `ELYRASQL_SERIALIZABLE_MAX_RANGE` rows
  (default 5,000,000) aborts the commit (fail-safe against unbounded memory)
  rather than materializing without limit.
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
  comparisons (`WHERE`), `UNIQUE`, `PRIMARY KEY`, secondary indexes, and now
  **`ORDER BY`, `GROUP BY`, `DISTINCT` and equi-join keys** (a `_bin` column
  sorts, groups, de-duplicates and joins by exact bytes, case-sensitively; the
  default column stays case-insensitive). Accent sensitivity and alternate
  charsets are not implemented.
- Full-text search: `MATCH(col, ...) AGAINST('terms' [IN BOOLEAN MODE])`
  (natural-language OR-of-terms, or boolean `+`/`-`, with relevance scoring).
  `CREATE FULLTEXT INDEX` builds a persistent inverted index that is maintained
  on INSERT/UPDATE/DELETE and used to accelerate MATCH; without one, MATCH falls
  back to a scan. Stemming uses the **Snowball** algorithms (`rust-stemmers`),
  so it is linguistically correct (`running`->`run`, `studies`->`study`, while
  `string`/`sing` are left alone) and supports many languages via
  `ELYRASQL_FULLTEXT_LANGUAGE` (default `english`; `none` disables stemming).
  It still doesn't handle synonyms, and truly irregular forms (e.g. `wolves`)
  aren't unified. Changing the language invalidates an existing index
  (rebuild with `CREATE FULLTEXT INDEX`). Vector (ANN) search is also available.
- `ENUM` and `SET` values are validated against their declared members. A
  `CREATE TABLE` `ENUM('a','b',...)` column is enforced via a synthesized
  `CHECK col IN (...)`, and a `SET('a','b',...)` column via a synthesized REGEXP
  CHECK that accepts any comma-separated subset (or the empty set); a non-member
  INSERT/UPDATE is rejected, and NULL is allowed on a nullable column.
- Basic spatial support: `POINT`/`GEOMETRY` columns are stored as WKT text, with
  `POINT(x,y)`, `ST_X`, `ST_Y`, `ST_Distance` (Euclidean), `ST_AsText`, and
  `ST_GeomFromText`. Only 2D points are supported; there is no spatial index or
  SRID/geodesic distance.

## Security & operations

- Multiple persistent accounts with `CREATE USER`/`GRANT`/`REVOKE`. Privileges
  are tracked and **enforced per action**: the individual DML privileges
  `INSERT`, `UPDATE` and `DELETE` are checked separately, per target table, so a
  user granted only `INSERT` cannot `UPDATE`/`DELETE`, and revoking one write
  privilege leaves the others intact. Grants apply **globally** or **per table**.
  (Reads are still allowed at the global baseline — see below; DDL such as
  `CREATE`/`DROP`/`ALTER`/`CREATE INDEX` and administrative statements are gated
  at the `ADMIN` tier rather than by their individual `CREATE`/`DROP`/`ALTER`
  privileges.)
  **Roles** are supported: `CREATE ROLE` / `DROP ROLE`, `GRANT <role> TO <user>`
  / `REVOKE <role> FROM <user>`; a user inherits the global and per-table grants
  of every role granted to them. `GRANT ... ON db.*` is accepted and maps to a
  global grant (single default database). Reads are allowed at the global
  baseline (table-level `SELECT` is not required for an authenticated user).
  **Per-column** SELECT grants are
  enforced (`GRANT SELECT(col, ...) ON t TO u`): a column-restricted user may
  only read those columns of `t` — querying an ungranted column (including via
  `SELECT *` or a `WHERE`/`ORDER BY` reference) is denied. Enforcement covers
  single-base-table selects; a column-restricted table used in a join or
  subquery is denied (deny-safe).
- An optional **audit log** (`--audit-log <path>`) appends one tab-separated
  line per executed statement (`timestamp  conn_id  user  OK|ERR  sql`).
- **Cluster/replication authentication.** Set `ELYRASQL_CLUSTER_SECRET` (the same
  value on every node) to require a challenge-response handshake
  (`SHA1(secret‖nonce)`, constant-time) on every Raft control and replication
  connection, so an unauthenticated peer cannot inject fake writes or votes.
  Password hashes are compared in **constant time**. Exposing the replication
  endpoint on a non-loopback address **without** a secret is refused unless
  `ELYRASQL_ALLOW_OPEN_AUTH=1`. The internal Raft/replication traffic is not yet
  encrypted, so for **confidentiality** run it on a trusted/private network (or a
  VPN/WireGuard); mutual TLS for internal traffic is planned (ESQL-30).
- **Password hardening.** New passwords (`CREATE USER` / `ALTER USER` / `SET
  PASSWORD`) must satisfy a strength policy: minimum length
  (`ELYRASQL_PASSWORD_MIN_LEN`, default 8) and a letters+digits requirement
  (`ELYRASQL_PASSWORD_REQUIRE_MIXED`, default on); set
  `ELYRASQL_PASSWORD_POLICY=off` to disable. Repeated failed logins trigger a
  **temporary account lockout** (`ELYRASQL_AUTH_MAX_FAILURES`, default 10;
  `ELYRASQL_AUTH_LOCKOUT_SECS`, default 60) to blunt brute-force attacks;
  failures and lockouts are logged. Two auth plugins are supported:
  `mysql_native_password` (default, works with every client) and
  `caching_sha2_password` (MySQL 8's default; opt-in via `ELYRASQL_AUTH_PLUGIN`,
  full authentication over TLS or via an RSA public-key exchange on a plaintext
  connection). Both verify against the stored `SHA1(SHA1(password))` digest;
  the password is never persisted in the clear.
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
  node never double-votes in a term (a Raft safety requirement). In `cluster`
  mode the **live write path runs through the Raft replicated log**: the leader
  appends each write to the log, replicates it via `AppendEntries`, **commits it
  once a quorum has it**, and only then **applies** it and acknowledges the
  client (pre-commit / 2-phase). Followers append (with the consistency check +
  conflicting-suffix truncation) and apply up to the leader's commit index. With
  the §5.4.1 election restriction this is **no-data-loss failover**: an
  acknowledged write is on a quorum's durable log and any new leader has it.
  A write cannot be acknowledged without a quorum. The replication path is
  **batched for throughput**: the leader holds persistent `AppendEntries`
  connections to followers, appends are fsynced once per round (not per write),
  and committed entries are applied together through the DB's group commit — so
  concurrent writers reach hundreds of committed writes/second even though a
  single sequential write is fsync-latency-bound. The leader holds a **lease**:
  it renews leadership each round it confirms contact with a quorum, and **steps
  down** if it cannot for the lease window (below the minimum election timeout).
  A leader partitioned from its quorum therefore relinquishes leadership — its
  in-flight writes fail fast and a healthy majority elects a new leader — rather
  than hanging. Because the lease is shorter than the election timeout, a
  lease-valid leader is guaranteed to still be the leader, so its local reads are
  linearizable without a quorum round-trip. The Raft log is **compacted**: once
  entries are applied and replicated to every member, each node discards them
  (keeping only the snapshot boundary term for the consistency check), so the log
  does not grow unbounded — the applied state machine is the snapshot. Compaction
  advances only to the slowest member's replicated index, so a permanently
  lagging/dead member holds it back until the member catches up or is removed
  from membership. The older `primary`/`replica` mode remains asynchronous
  (semi-sync/quorum barrier).

## Wire protocol

- The MySQL wire layer is a **first-party crate (`elyra-wire`)**, forked from
  `opensrv-mysql`, so protocol behaviour is ours to fix and extend (this is what
  enabled rustls 0.23 and `caching_sha2_password`).
- **Binary (native) prepared statements** work for the common shapes, including
  repeated prepares on one connection (a packet-reader desync that affected
  drivers pipelining commands — e.g. PDO/mysqlnd with
  `PDO::ATTR_EMULATE_PREPARES => false` — is fixed). `describe_query` reports an
  exact result-column count at `PREPARE` (enable with `ELYRASQL_STMT_DESCRIBE`)
  for single **and** joined/multi-table SELECTs, so `SELECT *` over a join
  resolves its columns. Remaining gaps: `SELECT a.*` (qualified wildcard in the
  projection) and `SELECT *` over `information_schema` are not yet executed.
  Client-side (emulated) prepared statements remain the widest-compatibility
  default; PyMySQL and sqlx bind client-side and are unaffected.
- **`LOAD DATA INFILE`** reads a **server-side** file and bulk-inserts it
  (requires ADMIN, like MySQL's `FILE` privilege): `LOAD DATA INFILE '<path>'
  INTO TABLE t [FIELDS TERMINATED BY '...'] [ENCLOSED BY '...'] [LINES
  TERMINATED BY '...'] [IGNORE n LINES] [(cols)]`, with `\N` for NULL. Client-
  side `LOAD DATA LOCAL INFILE` (streaming the file over the wire) is not
  supported.
- Authentication offers `mysql_native_password` (default) and
  `caching_sha2_password` (MySQL 8's default; opt-in via `ELYRASQL_AUTH_PLUGIN`).
  Connection salts come from the OS CSPRNG. `caching_sha2_password` runs full
  authentication — cleartext over TLS, or an RSA public-key exchange on a
  plaintext connection.

Have a need that isn't listed? Open an issue on
[GitHub](https://github.com/kwhorne/ElyraSQL/issues).
