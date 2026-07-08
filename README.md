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
- **OLAP-ready** — heavy analytical queries route to a columnar engine over
  the same data.
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

# Connect with any MySQL client
mysql -h 127.0.0.1 -P 3307 -u root
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

## Roadmap

- [x] Cargo workspace + branded core types
- [x] Single-file ACID storage engine (`redb`)
- [x] MySQL wire-protocol server (handshake + text protocol)
- [x] SQL frontend (MySQL dialect) + literal/arithmetic `SELECT`
- [ ] Transactional executor: `CREATE/INSERT/UPDATE/DELETE/SELECT` over storage
- [ ] Secondary indexes + query planner
- [ ] Prepared statements (binary protocol)
- [x] `VECTOR(n)` columns + exact KNN via `VEC_DISTANCE` in `ORDER BY`
- [x] Secondary indexes (`CREATE INDEX`) with planner integration
- [x] Aggregation (`COUNT/SUM/AVG/MIN/MAX`), `GROUP BY`, `ORDER BY`
- [ ] Vector ANN acceleration (HNSW) for large collections
- [ ] OLAP acceleration (columnar analytics)
- [ ] Auth, roles, TLS
- [ ] systemd packaging for Ubuntu 24.04+
- [ ] ElyraSQL client (Rust + Svelte on Elyra Framework)

## License

MIT — see [LICENSE](LICENSE).
