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
| `ELYRASQL_GROUP_MAX_GROUPS` | `5000000` | Distinct-group cap: in-memory `GROUP BY` past this falls back to partitioned spill; a single spill partition past it errors (0 = unlimited). |
| `ELYRASQL_TXN_MAX_BYTES` | `1073741824` | Max bytes an uncommitted transaction may buffer (staged puts + deletes) before writes are rejected, so one huge transaction can't exhaust server memory. The statement errors; `COMMIT` or `ROLLBACK` to continue. |
| `ELYRASQL_MAX_FRAME_MB` | `1024` | Max size of any single length-prefixed frame/record read from the network (cluster/replication), the binlog, or a spill file, before allocation. Rejects corrupt/malicious oversized lengths instead of crashing (OOM). Must stay ≥ the largest replicated/logged transaction. |

## Security & full-text (environment-only)

| Environment | Default | Description |
|-------------|---------|-------------|
| `ELYRASQL_PASSWORD_POLICY` | `on` | Set to `off` to disable the password-strength policy. |
| `ELYRASQL_PASSWORD_MIN_LEN` | `8` | Minimum password length when the policy is on. |
| `ELYRASQL_PASSWORD_REQUIRE_MIXED` | `on` | Require mixed character classes when the policy is on. |
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
