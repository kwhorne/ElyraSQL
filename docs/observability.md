# Observability

ElyraSQL exposes runtime health through MySQL-compatible introspection plus a
slow-query log, so existing MySQL tooling works.

## Server status

`SHOW STATUS` (also `SHOW GLOBAL STATUS` / `SHOW SESSION STATUS`) returns
process-wide counters as `Variable_name` / `Value` rows:

```sql
SHOW STATUS;
SHOW STATUS LIKE 'Com_%';
```

| Variable | Meaning |
|----------|---------|
| `Uptime` | Seconds since the server started |
| `Threads_connected` | Current open connections |
| `Connections` | Total connections since start |
| `Questions` / `Queries` | Total statements executed |
| `Com_select` / `Com_insert` / `Com_update` / `Com_delete` / `Com_other` | Per-type statement counts |
| `Errors` | Statements that returned an error |
| `Slow_queries` | Statements at or above the slow threshold |

A trailing `LIKE 'prefix%'` filters by variable-name prefix.

## Process list

`SHOW PROCESSLIST` (or `SHOW FULL PROCESSLIST`) lists live connections with the
standard columns `Id, User, Host, db, Command, Time, State, Info`. A connection
running a statement shows `Command = Query`, the elapsed `Time` in seconds, and
the SQL in `Info`; an idle connection shows `Command = Sleep`.

```sql
SHOW PROCESSLIST;
```

## Slow-query log

Start the server with a millisecond threshold to log statements that take at
least that long:

```bash
elyrasql serve --data elyra.edb --slow-query-ms 500
# or
ELYRASQL_SLOW_QUERY_MS=500 elyrasql serve --data elyra.edb
```

Each slow statement is emitted at `WARN` level with its duration and (truncated)
SQL, and increments the `Slow_queries` counter:

```
WARN elyra_server::observ: slow query duration_ms=1636 sql=SELECT COUNT(*) FROM t a, t b
```

`--slow-query-ms 0` (the default) disables slow-query logging. Logs go through
the standard `tracing` subscriber; set `RUST_LOG` to adjust verbosity.

!!! note "Not yet available"
    There is no Prometheus/OpenMetrics endpoint or `performance_schema` yet;
    counters are exposed through `SHOW STATUS`. See [Limitations](limitations.md).
