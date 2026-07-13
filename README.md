<div align="center">

<img src="assets/png/icon-256.png" alt="ElyraSQL" width="128" height="128" />

# ElyraSQL

### The modern SQL database that speaks MySQL — one file, blazing fast, AI-ready.

**A robust, MySQL-compatible SQL server written in Rust.**

Single-file · ACID · OLAP-ready · vector-native · AI-native

[![CI](https://github.com/kwhorne/ElyraSQL/actions/workflows/ci.yml/badge.svg)](https://github.com/kwhorne/ElyraSQL/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/kwhorne/ElyraSQL?color=14B8A6&label=release)](https://github.com/kwhorne/ElyraSQL/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-14B8A6.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/ghcr.io-elyrasql-2496ED?logo=docker&logoColor=white)](https://github.com/kwhorne/ElyraSQL/pkgs/container/elyrasql)
[![Docs](https://img.shields.io/badge/docs-elyracode.com-14B8A6)](https://elyracode.com/docs/sql-server)

### [📖 Read the docs at elyracode.com →](https://elyracode.com/docs/sql-server)

[Documentation](https://elyracode.com/docs/sql-server) ·
[Quick start](#quick-start) ·
[Benchmarks](BENCHMARKS.md) ·
[Framework guide](https://elyracode.com/docs/sql-server/frameworks/) ·
[Changelog](CHANGELOG.md)

</div>

---

## Drop-in for MySQL. Built for what comes next.

ElyraSQL speaks the **MySQL wire protocol**, so your existing clients, drivers
and frameworks — **Laravel, Django, Rails, Node, Go, Rust** — connect *unchanged*.
But underneath it's a fresh, Rust-native engine built for 2026: the **entire
database lives in a single crash-safe file**, large analytical queries run
through a **parallel streaming engine**, and **vector search + AI embeddings are
first-class SQL** — no bolt-on extensions, no second system to operate.

One binary. One file. Your whole transactional, analytical and AI-search
workload — served over the protocol your stack already knows.

**[→ Get started in 60 seconds](#quick-start)** &nbsp;·&nbsp;
**[→ Full documentation](https://elyracode.com/docs/sql-server)**

> **Stable release: v1.0.0.** A broad, MySQL-compatible SQL engine: full
> DDL/DML, all join types (INNER/LEFT/RIGHT/FULL/CROSS, streamed for large
> analytical joins), subqueries (correlated too), CTEs (incl. `WITH RECURSIVE`),
> window functions, `GROUP BY ... WITH ROLLUP`, set operations, transactions
> (snapshot + serializable), a large function catalog with exact `DECIMAL` and
> `BIGINT UNSIGNED`, per-column collation, ENUM/SET validation, introspection
> (`SHOW` + `INFORMATION_SCHEMA`), vector + full-text + hybrid search, and
> parallel OLAP aggregation. Native `mysql_native_password` /
> `caching_sha2_password` auth, TLS, replication and Raft failover. See the
> [changelog](CHANGELOG.md).

## Why ElyraSQL

- **Framework-ready** — runs **Laravel/Eloquent** (migrations, models,
  relationships, transactions) and any MySQL-driver stack; see the
  [Framework Integration guide](https://elyracode.com/docs/sql-server/frameworks/).
- **MySQL wire protocol** — connect with `mysql`, DBeaver, Workbench, or any
  MySQL driver in any language. No custom client required.
- **Native client** — prefer something purpose-built? ElyraSQL ships its own
  native client: **[elyracode.com/sql/client](https://elyracode.com/sql/client)**.
- **One file** — the entire database lives in a single ACID file (`*.edb`),
  crash-safe with a single-writer / multi-reader model.
- **OLAP-ready** — large aggregations run through a parallel, streaming engine
  with memory proportional to the number of groups, not the table size.
- **Vector-native** — `VECTOR(n)` columns with ANN search, MySQL-flavoured
  distance functions.
- **AI-native search** — `HYBRID(...)` fuses full-text + vector ranking (RRF)
  in one query, and `ai_embed('text')` generates embeddings in SQL via an
  OpenAI-compatible endpoint: the RAG stack in one file, no external search
  engine.
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
- **Programmability** — row-level triggers (`BEFORE`/`AFTER` with `NEW`/`OLD`),
  stored procedures with parameters, variables and `IF`/`WHILE`, and full-text
  `MATCH ... AGAINST`.
- **Users & access control** — persistent accounts via `CREATE USER`/`DROP USER`,
  `GRANT`/`REVOKE`, `SET PASSWORD`, `SHOW GRANTS`; coarse `read`/`write`/`admin`
  privileges; `mysql_native_password` auth; optional TLS.
- **Observability** — `SHOW STATUS` counters, `SHOW PROCESSLIST`, an optional
  slow-query log (`--slow-query-ms`), and a Prometheus `/metrics` endpoint.
- **Replication & HA** — asynchronous primary → read-replica streaming, optional
  semi-sync, and **automatic failover** via Raft-style leader election
  (`elyrasql cluster`).
- **Introspection** — `SHOW TABLES`/`COLUMNS`/`INDEX`, `SHOW CREATE TABLE`,
  `DESCRIBE`, and a queryable `INFORMATION_SCHEMA`.

See the [documentation site](https://elyracode.com/docs/sql-server) and
[limitations](docs/limitations.md) for the full, honest picture.

## Architecture

```
              MySQL clients / drivers
                       │  (MySQL wire protocol)
              ┌────────▼────────┐
              │  elyra-server   │   elyra-wire (own MySQL protocol) + tokio
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
| `elyra-wire`    | First-party MySQL wire protocol (handshake, auth, TLS)      |
| `elyra-server`  | Connection handling, auth verification, prepared statements |
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
SELECT VERSION();   -- 8.0.0-ElyraSQL-1.0.0
```

## Configuration

| Flag / env               | Default             | Meaning                       |
|--------------------------|---------------------|-------------------------------|
| `--data` / `ELYRASQL_DATA`     | `elyra.edb`   | Path to the single DB file    |
| `--listen` / `ELYRASQL_LISTEN` | `127.0.0.1:3307` | Bind address (MySQL proto) |
| `RUST_LOG`               | `info`              | Log level                     |

## Performance

See [BENCHMARKS.md](BENCHMARKS.md) for a reproducible benchmark harness and
results, and [benchmark_analyse.md](benchmark_analyse.md) for a head-to-head
comparison against MySQL 8.4, Percona 8.4 and PostgreSQL 17.

Highlights (200k rows, same host): ElyraSQL **beats MySQL and Percona on
full-table `COUNT` and bulk insert** and matches PostgreSQL on full-scan
`COUNT`; PK lookup ~0.2 ms, selective join ~0.27 ms, cached vector ANN
~0.3 ms, bulk ingest ~183k rows/s.

## Install

Static Linux binaries (x86_64 and aarch64) are attached to each
[GitHub Release](https://github.com/kwhorne/ElyraSQL/releases):

```bash
curl -L -o elyrasql.tar.gz \
  https://github.com/kwhorne/ElyraSQL/releases/download/v1.0.0/elyrasql-1.0.0-linux-x86_64.tar.gz
tar xzf elyrasql.tar.gz && ./elyrasql-1.0.0-linux-x86_64/elyrasql serve
```

## Docker

Multi-arch image (amd64 + arm64) on GHCR:

```bash
docker run -p 3307:3307 -v elyra:/var/lib/elyrasql ghcr.io/kwhorne/elyrasql:1.0.0
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

## Learn more

- **Documentation** — [elyracode.com/docs/sql-server](https://elyracode.com/docs/sql-server)
- **Native client** — [elyracode.com/sql/client](https://elyracode.com/sql/client)
- **Framework integration** (Laravel, Django, Rails, …) — [elyracode.com/docs/sql-server/frameworks](https://elyracode.com/docs/sql-server/frameworks/)
- **Benchmarks & analysis** — [BENCHMARKS.md](BENCHMARKS.md) · [benchmark_analyse.md](benchmark_analyse.md)
- **Releases** — [github.com/kwhorne/ElyraSQL/releases](https://github.com/kwhorne/ElyraSQL/releases)

## License

MIT — see [LICENSE](LICENSE). Use it freely, in commercial and open projects alike.

---

<div align="center">

<img src="assets/png/icon-64.png" alt="ElyraSQL" width="40" height="40" />

**ElyraSQL** — part of the [Elyra](https://elyracode.com) family.

Developed with ❤️ from Norway 🇳🇴

</div>
