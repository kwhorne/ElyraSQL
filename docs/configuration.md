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
