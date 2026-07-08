# ElyraSQL

A robust, **MySQL-compatible** SQL server written in Rust. Single database
file, ACID storage, OLAP-ready and vector-native — all under one brand.

> Status: **early scaffold**. A working MySQL-protocol server that answers
> real queries is in place; the transactional executor, analytics and vector
> search land in the milestones below.

## Why ElyraSQL

- **MySQL wire protocol** — connect with `mysql`, DBeaver, Workbench, or any
  MySQL driver in any language. No custom client required.
- **One file** — the entire database lives in a single ACID file (`*.edb`),
  crash-safe with a single-writer / multi-reader model.
- **OLAP-ready** — large aggregations run through a parallel, streaming engine
  with memory proportional to the number of groups, not the table size.
- **Vector-native** — `VECTOR(n)` columns with ANN search, MySQL-flavoured
  distance functions.
- **MIT licensed**, targets **Ubuntu 24.04+** for production, develops
  anywhere Rust runs.

## Architecture

```
              MySQL clients / drivers
                       │  (MySQL wire protocol)
              ┌────────▼────────┐
              │  elyra-server   │   opensrv-mysql + tokio
              └────────┬────────┘
              ┌────────▼────────┐
              │  elyra-engine   │   sqlparser (MySQL dialect) → plan → execute
              └───┬────────┬────┘
      ┌───────────▼──┐  ┌──▼───────────┐  ┌──────────────┐
      │ elyra-storage│  │  elyra-olap  │  │ elyra-vector │
      │ single file  │  │  analytics   │  │  ANN / HNSW  │
      │ ACID (redb)  │  │  (columnar)  │  │              │
      └──────────────┘  └──────────────┘  └──────────────┘
                    all share  ▲  elyra-core (types, values, errors)
```

Crates:

| Crate           | Responsibility                                             |
|-----------------|------------------------------------------------------------|
| `elyra-core`    | Shared value/type model, errors, branding constants        |
| `elyra-storage` | Single-file ACID key/value engine, namespaced keyspace     |
| `elyra-engine`  | SQL parsing (MySQL dialect), planning, execution           |
| `elyra-olap`    | Analytical (columnar) query acceleration *(planned)*       |
| `elyra-vector`  | Vector column type + ANN search *(distance math today)*    |
| `elyra-server`  | MySQL-compatible wire protocol server                      |
| `elyra-cli`     | `elyrasql` binary (serve + admin)                          |

Third-party engines are internal dependencies only — nothing user-facing
(APIs, errors, CLI, wire handshake) exposes their names. Everything is
ElyraSQL.

## Quick start

```bash
# Build
cargo build --release

# Run the server (creates elyra.edb if missing)
./target/release/elyrasql serve --data elyra.edb --listen 127.0.0.1:3307

# With authentication + TLS
./target/release/elyrasql serve \
    --user root --password s3cret \
    --tls-cert server.crt --tls-key server.key

# Connect with any MySQL client
mysql -h 127.0.0.1 -P 3307 -u root -p
```

```sql
SELECT 1;
SELECT 1 + 1 AS two;
SELECT 'hei fra ElyraSQL' AS msg;
SELECT VERSION();   -- 8.0.0-ElyraSQL-0.1.0
```

## Configuration

| Flag / env               | Default             | Meaning                       |
|--------------------------|---------------------|-------------------------------|
| `--data` / `ELYRASQL_DATA`     | `elyra.edb`   | Path to the single DB file    |
| `--listen` / `ELYRASQL_LISTEN` | `127.0.0.1:3307` | Bind address (MySQL proto) |
| `RUST_LOG`               | `info`              | Log level                     |

## Performance

See [BENCHMARKS.md](BENCHMARKS.md) for a reproducible benchmark harness and
results. Highlights (release, 100k rows): PK lookup ~0.15 ms, selective
index-nested-loop join ~0.18 ms, cached vector ANN ~0.29 ms, bulk ingest
~180k rows/s.

## Deploying on Ubuntu 24.04+

```bash
sudo ./packaging/deploy.sh            # build, install, systemd, start
# or with credentials + TLS:
ELYRASQL_USER=root ELYRASQL_PASSWORD=secret \
  ELYRASQL_LISTEN=0.0.0.0:3307 sudo -E ./packaging/deploy.sh
journalctl -u elyrasql -f
```

## Roadmap

- [x] Cargo workspace + branded core types
- [x] Single-file ACID storage engine (`redb`)
- [x] MySQL wire-protocol server (handshake + text protocol)
- [x] SQL frontend (MySQL dialect) + literal/arithmetic `SELECT`
- [ ] Transactional executor: `CREATE/INSERT/UPDATE/DELETE/SELECT` over storage
- [x] JOINs (INNER / LEFT / CROSS, multi-table) with qualified columns
- [ ] Secondary indexes + query planner
- [x] Prepared statements (binary protocol): typed params, escaping
      (see caveat on repeated close→prepare cycles under opensrv 0.7)
- [x] `VECTOR(n)` columns + exact KNN via `VEC_DISTANCE` in `ORDER BY`
- [x] Secondary indexes (`CREATE INDEX`) with planner integration
- [x] Aggregation (`COUNT/SUM/AVG/MIN/MAX`), `GROUP BY`, `ORDER BY`
- [x] Vector ANN acceleration (HNSW), cached & rebuilt-when-stale
- [x] OLAP acceleration: parallel, bounded-memory streaming aggregation
- [x] Authentication (mysql_native_password) + TLS
- [ ] Roles / per-user privileges
- [x] systemd packaging for Ubuntu 24.04+
- [ ] ElyraSQL client (Rust + Svelte on Elyra Framework)

## License

MIT — see [LICENSE](LICENSE).
