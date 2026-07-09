# ElyraSQL

[![CI](https://github.com/kwhorne/ElyraSQL/actions/workflows/ci.yml/badge.svg)](https://github.com/kwhorne/ElyraSQL/actions/workflows/ci.yml)

A robust, **MySQL-compatible** SQL server written in Rust. Single database
file, ACID storage, OLAP-ready and vector-native — all under one brand.

> Status: **v0.5.0**. A broad, MySQL-compatible SQL engine: full DDL/DML,
> joins, subqueries (correlated too), CTEs (incl. `WITH RECURSIVE`), window
> functions, set operations, transactions (snapshot + serializable), a large
> function catalog, introspection (`SHOW` + `INFORMATION_SCHEMA`), vector search
> and parallel OLAP aggregation. See the [changelog](CHANGELOG.md).

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

## SQL support

- **DDL** — `CREATE`/`ALTER`/`DROP TABLE` (incl. `MODIFY`/`CHANGE`/`SET DEFAULT`),
  `CREATE TABLE ... AS SELECT`, `CREATE TABLE ... LIKE`, `TRUNCATE`,
  `CREATE`/`DROP VIEW`, `CREATE INDEX`; primary/composite keys, secondary and
  vector indexes; `DEFAULT`, `AUTO_INCREMENT`, generated columns, `ENUM`/`SET`.
- **Constraints** — enforced `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `CHECK`, and
  `FOREIGN KEY` (with `ON DELETE RESTRICT`/`CASCADE`/`SET NULL`).
- **DML** — `INSERT` (multi-row, `INSERT ... SELECT`), upserts (`REPLACE`,
  `INSERT IGNORE`, `ON DUPLICATE KEY UPDATE`), `UPDATE`/`DELETE` with subqueries
  and multi-table joins.
- **Queries** — all join types, `GROUP BY`/`HAVING`, `ORDER BY`/`LIMIT`,
  subqueries (uncorrelated **and** correlated, incl. over joins), derived
  tables, CTEs and `WITH RECURSIVE`, window functions with frames, set
  operations (`UNION`/`INTERSECT`/`EXCEPT`).
- **Functions** — string, math, date/time (incl. `INTERVAL` arithmetic),
  conditional, `CAST`, `REGEXP`, JSON, vector, and aggregates including
  `GROUP_CONCAT` and conditional aggregates. Exact `DECIMAL` arithmetic.
- **Transactions** — `BEGIN`/`COMMIT`/`ROLLBACK`, `SAVEPOINT`, snapshot and
  opt-in serializable isolation, and `SELECT ... FOR UPDATE`/`FOR SHARE`
  (optimistic row locking).
- **Backup** — hot, consistent `BACKUP TO '<path>'` while serving, plus offline
  `elyrasql backup`/`restore` CLI commands.
- **Users & access control** — persistent accounts via `CREATE USER`/`DROP USER`,
  `GRANT`/`REVOKE`, `SET PASSWORD`, `SHOW GRANTS`; coarse `read`/`write`/`admin`
  privileges; `mysql_native_password` auth; optional TLS.
- **Observability** — `SHOW STATUS` counters, `SHOW PROCESSLIST`, an optional
  slow-query log (`--slow-query-ms`), and a Prometheus `/metrics` endpoint.
- **Replication** — asynchronous primary → read-replica streaming for warm
  standbys and read scaling (`--replication-listen` / `elyrasql replica`).
- **Introspection** — `SHOW TABLES`/`COLUMNS`/`INDEX`, `SHOW CREATE TABLE`,
  `DESCRIBE`, and a queryable `INFORMATION_SCHEMA`.

See the [documentation site](https://kwhorne.github.io/ElyraSQL/) and
[limitations](docs/limitations.md) for the full, honest picture.

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
| `elyra-olap`    | Parallel, streaming group-aggregation kernel               |
| `elyra-vector`  | Vector column type + ANN search (exact + HNSW)             |
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
SELECT VERSION();   -- 8.0.0-ElyraSQL-0.5.0
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

## Install

Static Linux binaries (x86_64 and aarch64) are attached to each
[GitHub Release](https://github.com/kwhorne/ElyraSQL/releases):

```bash
curl -L -o elyrasql.tar.gz \
  https://github.com/kwhorne/ElyraSQL/releases/download/v0.5.0/elyrasql-0.5.0-linux-x86_64.tar.gz
tar xzf elyrasql.tar.gz && ./elyrasql-0.5.0-linux-x86_64/elyrasql serve
```

## Docker

Multi-arch image (amd64 + arm64) on GHCR:

```bash
docker run -p 3307:3307 -v elyra:/var/lib/elyrasql ghcr.io/kwhorne/elyrasql:0.5.0
# with auth + a persistent volume:
docker run -p 3307:3307 -v elyra:/var/lib/elyrasql \
  -e ELYRASQL_USER=root -e ELYRASQL_PASSWORD=secret \
  ghcr.io/kwhorne/elyrasql:latest
```

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
- [x] JOINs (INNER / LEFT / RIGHT / FULL / CROSS, multi-table)
- [x] Range index scans (`>`, `>=`, `<`, `<=`, `BETWEEN`)
- [x] Roles / per-user privileges (read / write / admin)
- [ ] Secondary indexes + query planner
- [x] Prepared statements (binary protocol): typed params, escaping
      (see caveat on repeated close→prepare cycles under opensrv 0.7)
- [x] `VECTOR(n)` columns + exact KNN via `VEC_DISTANCE` in `ORDER BY`
- [x] Secondary indexes (`CREATE INDEX`) with planner integration
- [x] Aggregation (`COUNT/SUM/AVG/MIN/MAX`), `GROUP BY`, `ORDER BY`
- [x] Vector ANN acceleration (HNSW), cached & rebuilt-when-stale
- [x] OLAP acceleration: parallel, bounded-memory streaming aggregation
- [x] Authentication (mysql_native_password) + TLS
- [x] Transactions: `BEGIN`/`COMMIT`/`ROLLBACK` with **snapshot isolation**
      (MVCC snapshot + buffered writes)
- [x] DATE / DATETIME / TIME / DECIMAL / JSON types
- [x] Composite (multi-column) PK & indexes
- [x] `ALTER TABLE` (ADD/DROP/RENAME COLUMN, RENAME TABLE)
- [x] systemd packaging for Ubuntu 24.04+
- [ ] ElyraSQL client (Rust + Svelte on Elyra Framework)

## License

MIT — see [LICENSE](LICENSE).
