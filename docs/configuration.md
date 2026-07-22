# Configuration

ElyraSQL is configured entirely through CLI flags, each with an environment
variable fallback (handy for systemd and containers).

## `elyrasql serve`

| Flag | Environment | Default | Description |
|------|-------------|---------|-------------|
| `--data <path>` | `ELYRASQL_DATA` | `elyra.edb` | Path to the single database file. |
| `--listen <addr>` | `ELYRASQL_LISTEN` | `127.0.0.1:3307` | Bind address for the MySQL listener. |
| `--user <name>` | `ELYRASQL_USER` | — | A single admin user; enables authentication. |
| `--password <pw>` | `ELYRASQL_PASSWORD` | `""` | Password for `--user`. |
| `--auth <spec>` | — | — | Additional user `user:password:role` (repeatable). |
| `--tls-cert <file>` | `ELYRASQL_TLS_CERT` | — | PEM certificate; enables TLS (requires `--tls-key`). |
| `--tls-key <file>` | `ELYRASQL_TLS_KEY` | — | PEM private key. |
| `--slow-query-ms <n>` | `ELYRASQL_SLOW_QUERY_MS` | `0` | Log queries at or above this many ms (0 = off). See [Observability](observability.md). |
| `--metrics-listen <addr>` | `ELYRASQL_METRICS_LISTEN` | — | Serve Prometheus metrics at `http://<addr>/metrics`. |

## Memory limits (environment-only)

| Environment | Default | Description |
|-------------|---------|-------------|
| `ELYRASQL_SORT_MAX_ROWS` | `1000000` | Rows buffered before spilling to a temp file, for both `ORDER BY` (external merge sort) and partitioned `GROUP BY` spill. |
| `ELYRASQL_ORDER_SCAN_BUDGET` | `max(256 × (offset+limit), 50000)` | Rows examined during a **filtered** indexed `ORDER BY ... LIMIT` walk before falling back to the sorter. Guards against a very selective `WHERE` turning the ordered walk into a near-full scan; raise it if a moderately selective filtered grid is falling back too early. |
| `ELYRASQL_ALLOW_OPEN_AUTH` | unset | Override the safe-by-default refusal to run **open auth** (no accounts → every client is `Admin`) or an **unauthenticated replication endpoint** on a non-loopback bind. Set to `1` only when you deliberately want a credential-less server reachable from other hosts. |
| `ELYRASQL_QUERY_TIMEOUT_MS` | `0` (off) | Per-query wall-clock timeout. A statement running longer returns an error (`query exceeded ELYRASQL_QUERY_TIMEOUT_MS`) and the client is unblocked immediately. CPU-bound work already dispatched to a worker thread may finish in the background; the timeout bounds client-visible latency, not raw CPU. |
| `ELYRASQL_SERIALIZABLE_MAX_RANGE` | `5000000` | Max rows in a single scanned range that a `SERIALIZABLE` commit will materialize for phantom validation. A larger range aborts the commit (fail-safe against OOM) instead of buffering without limit — narrow the predicate or use a lower isolation level. |
| `ELYRASQL_IN_SUBQUERY_MAX` | `1000000` | Max rows a `WHERE col IN (SELECT ...)` may materialize into an in-memory value list. Beyond it the query errors fail-safe (rewrite as a `JOIN`/`EXISTS`) rather than buffering an unbounded list and evaluating it `O(N×M)`. |
| `ELYRASQL_DISTINCT_MAX` | `5000000` | Max distinct rows `SELECT DISTINCT` may buffer before erroring fail-safe instead of risking OOM. |
| `ELYRASQL_GROUP_MAX_GROUPS` | `5000000` | Distinct-group cap: in-memory `GROUP BY` past this falls back to partitioned spill; a single spill partition past it errors (0 = unlimited). |
| `ELYRASQL_AGG_WORKERS` | `min(cores, 4)` | Parallelism for full-scan aggregation (`COUNT`/`SUM`/`GROUP BY` over a whole table). Aggregation is memory-bandwidth bound, so the default caps at 4; set `1` for single-threaded, or raise it on hardware with high memory bandwidth. |
| `ELYRASQL_COLUMN_CACHE_MB` | `0` (off) | In-memory columnar cache budget, in MiB, for repeated **unfiltered** scalar/grouped aggregations: a table's numeric columns are materialised once and reused, skipping the storage scan. Ideal for read-heavy analytics/dashboards. Every committed write invalidates it (kept exact via a transactional write-sequence, never serves stale data). `0` disables it. Filtered aggregations always use the (already fast) scan path. |
| `ELYRASQL_ZONE_MAPS` | `off` | Set to `on` to enable data-skipping for **filtered** aggregations: per-chunk column min/max let a `WHERE col <op> value` skip blocks of rows that cannot match. A big win for data with locality (time-ordered rows, monotonic ids, sorted loads); no benefit for uniformly-shuffled columns. Results are always exact -- the filter still runs on every surviving row, and the map is invalidated by any committed write (transactional write-sequence). Off by default. |
| `ELYRASQL_SYNC` | `full` | Commit durability. `full` fsyncs on every commit (safest; a committed write survives an immediate crash). `normal` (aliases `relaxed`/`eventual`) lets commits return before the fsync and flushes in the background, greatly increasing small-batch / single-row `INSERT` throughput at the cost of a bounded crash-loss window. Never risks corruption: the file stays consistent and rolls back to the last durable commit. Equivalent to MySQL's `innodb_flush_log_at_trx_commit=2` or PostgreSQL's `synchronous_commit=off`. |
| `ELYRASQL_SYNC_INTERVAL_MS` | `200` | In `normal` durability, how often (ms, clamped 10..=10000) the background flush forces committed writes to disk -- roughly the worst-case crash-loss window. |
| `ELYRASQL_TXN_MAX_BYTES` | `1073741824` | Max bytes an uncommitted transaction may buffer (staged puts + deletes) before writes are rejected, so one huge transaction can't exhaust server memory. The statement errors; `COMMIT` or `ROLLBACK` to continue. |
| `ELYRASQL_MAX_EXPR_DEPTH` | `2000` | Max expression nesting/chain depth accepted from a client (in operator-token units), clamped to 64..5000. A deeply-nested flat expression (e.g. `1+1+1...` or a huge `OR` chain) is rejected with a SQL error *before* parsing, so it can't overflow the worker stack. Wide-but-shallow queries (long `IN` lists, big multi-row `INSERT`s) are unaffected. |
| `ELYRASQL_MAX_FRAME_MB` | `1024` | Max size of any single length-prefixed frame/record read from the network (cluster/replication), the binlog, or a spill file, before allocation. Rejects corrupt/malicious oversized lengths instead of crashing (OOM). Must stay ≥ the largest replicated/logged transaction. |

## Security & full-text (environment-only)

| Environment | Default | Description |
|-------------|---------|-------------|
| `ELYRASQL_PASSWORD_POLICY` | `on` | Set to `off` to disable the password-strength policy. |
| `ELYRASQL_PASSWORD_MIN_LEN` | `8` | Minimum password length when the policy is on. |
| `ELYRASQL_PASSWORD_REQUIRE_MIXED` | `on` | Require mixed character classes when the policy is on. |
| `ELYRASQL_AUTH_PLUGIN` | `mysql_native_password` | Authentication plugin advertised in the handshake. Set to `caching_sha2_password` for MySQL 8's default plugin: full authentication over TLS (cleartext) or a plaintext connection (RSA-encrypted password); no server-side password change required. `mysql_native_password` (the default) works with every client and is the safe choice. |
| `ELYRASQL_AUTH_MAX_FAILURES` | `10` | Failed logins before an account is temporarily locked out. |
| `ELYRASQL_AUTH_LOCKOUT_SECS` | `60` | Lockout duration after too many failed logins. |
| `ELYRASQL_CLUSTER_SECRET` | — | Shared secret authenticating cluster/replication connections (challenge-response). Strongly recommended for any multi-node deployment. |
| `ELYRASQL_FULLTEXT_LANGUAGE` | `english` | Snowball stemming language for `MATCH … AGAINST` and `FULLTEXT` indexes (`english`, `norwegian`, `german`, …, or `none` to disable stemming). Changing it invalidates existing full-text indexes (rebuild them). |
| `ELYRASQL_AUDIT_LOG` | — | Path to an append-only audit log (tab-separated: timestamp, connection, user, OK/ERR, SQL). |
| `ELYRASQL_AI_EMBED_URL` | u2014 | OpenAI-compatible embeddings endpoint for `ai_embed('text')` (e.g. OpenAI, or a local Ollama/LM Studio server). Set `ELYRASQL_AI_EMBED_KEY` (bearer token, optional locally) and `ELYRASQL_AI_EMBED_MODEL` (default `text-embedding-3-small`). |
| `ELYRASQL_STMT_DESCRIBE` | `off` | Describe prepared-statement result columns at `PREPARE` time (for `SELECT <cols> FROM <one table>`). Lets lenient drivers (e.g. **sqlx**) resolve result columns **by name**; strict `libmysqlclient`-based clients (mysql-connector) may mishandle a prepare response carrying result columns, so it is off by default. |

`RUST_LOG` controls log verbosity (`error`, `warn`, `info`, `debug`, `trace`;
default `info`).

## Examples

Local development (open, no auth):

```bash
elyrasql serve
```

Single admin user:

```bash
elyrasql serve --user root --password s3cret
```

Multiple users with roles, plus TLS:

```bash
elyrasql serve \
  --auth admin:adminpw:admin \
  --auth app:apppw:write \
  --auth analyst:ropw:read \
  --tls-cert server.crt --tls-key server.key
```

Roles are `admin` (DDL + everything), `write` (DML + reads), and `read`
(SELECT only). See [Security](security.md).

## Recommended settings

Sensible baselines by deployment profile. Start here and adjust to your
hardware and workload.

### Production (single node, exposed to applications)

Close the critical security and stability gaps first:

```bash
ELYRASQL_DATA=/var/lib/elyrasql/elyra.edb \
ELYRASQL_LISTEN=0.0.0.0:3307 \
ELYRASQL_USER=admin ELYRASQL_PASSWORD='<strong-password>' \
ELYRASQL_TLS_CERT=/etc/elyrasql/server.crt \
ELYRASQL_TLS_KEY=/etc/elyrasql/server.key \
ELYRASQL_SLOW_QUERY_MS=500 \
ELYRASQL_METRICS_LISTEN=127.0.0.1:9090 \
ELYRASQL_AUDIT_LOG=/var/log/elyrasql/audit.log \
elyrasql serve
```

- **Always** set credentials and **enable TLS** for client traffic — never
  expose an open (no-auth) listener to a network.
- Keep the password policy and login lockout at their defaults (on).
- If your transactions are small, lower `ELYRASQL_MAX_FRAME_MB` (e.g. `64`) for
  tighter denial-of-service defence. Keep it **≥ your largest transaction**
  (`ELYRASQL_TXN_MAX_BYTES`), or replication/binlog replay of that transaction
  will be rejected.
- Size the memory caps to your box: as a rough guide keep
  `ELYRASQL_SORT_MAX_ROWS` × average row size well under available RAM, and
  `ELYRASQL_TXN_MAX_BYTES` below the memory you can spare for one transaction.
- Run `ANALYZE TABLE` after bulk loads so the planner picks the spilling
  `GROUP BY` path directly (avoids a wasted scan) and orders joins well.

### Cluster / replication (high availability)

Everything from the production profile, plus:

```bash
ELYRASQL_CLUSTER_SECRET='<shared-secret>' \
ELYRASQL_REPLICATION_LISTEN=0.0.0.0:3317 \
ELYRASQL_SYNC_REPLICAS=1 \
ELYRASQL_BINLOG=/var/lib/elyrasql/binlog \
elyrasql serve   # (or: elyrasql cluster …)
```

- **Set `ELYRASQL_CLUSTER_SECRET`** on every node so peers authenticate.
- Internal cluster/replication traffic is authenticated but **not encrypted** —
  run nodes on a **private network or VPN** (mTLS is planned).
- Use `ELYRASQL_SYNC_REPLICAS` (and optionally `ELYRASQL_SYNC_STRICT`) to trade
  a little latency for stronger durability. See [Replication & HA](replication.md).

### Vector-search / OLAP heavy

- Build vector indexes **after** loading embeddings; the first query after a
  write pays a single (single-flight) rebuild, then queries are cached.
- Give the process plenty of RAM — HNSW graphs and OLAP aggregation are
  memory-resident up to the spill thresholds above.
- For very large `GROUP BY`/`ORDER BY`, raise `ELYRASQL_SORT_MAX_ROWS` /
  `ELYRASQL_GROUP_MAX_GROUPS` if you have the memory, or lower them to force
  earlier disk spilling on constrained hosts.

### Local development

Defaults are fine — `elyrasql serve` (open, no auth, `127.0.0.1:3307`).

## Other commands

```bash
elyrasql version    # print product and version
elyrasql --help
elyrasql serve --help
```

## Environment-only (systemd / Docker)

Because every flag has an env fallback, `elyrasql serve` with no arguments picks
everything up from the environment — exactly how the systemd unit and Docker
image invoke it:

```bash
ELYRASQL_DATA=/var/lib/elyrasql/elyra.edb \
ELYRASQL_LISTEN=0.0.0.0:3307 \
ELYRASQL_USER=root ELYRASQL_PASSWORD=secret \
elyrasql serve
```
