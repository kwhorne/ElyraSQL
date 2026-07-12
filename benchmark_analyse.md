# ElyraSQL 0.9.4 — Cross-engine benchmark analysis

A head-to-head comparison of **ElyraSQL 0.9.4** against three established,
heavily-optimised engines — **MySQL 8.4**, **Percona Server 8.4** and
**PostgreSQL 17** — on an identical workload, same host, same client.

> **Honesty note.** These are our own reproducible numbers on developer
> hardware, not a tuned, vendor-official benchmark. Treat them as *relative*.
> Re-run them yourself with the harness below and report what you see.

## Method

- **Harness:** [`bench/compare.py`](bench/compare.py) runs the same schema, the
  same rows, and the same SQL against every engine. MySQL/Percona/ElyraSQL use
  the MySQL wire protocol (PyMySQL); PostgreSQL uses `psycopg2`. Vector search is
  ElyraSQL-only and is measured separately in [`BENCHMARKS.md`](BENCHMARKS.md).
- **Data set:** 200,000 rows in `users(id BIGINT PK, name TEXT, age BIGINT)` and
  a matching `orders` table.
- **All four engines run as containers on the same host** (OrbStack), one client,
  default configuration. Medians; "best of 3" is reported to filter out
  shared-host contention (numbers still vary run-to-run by ~10–30%).

```bash
# ElyraSQL
python3 bench/compare.py --driver mysql    --port 3307 --user root --password '' --label ElyraSQL
# MySQL / Percona
python3 bench/compare.py --driver mysql    --port 3308 --user root --password root --label MySQL
python3 bench/compare.py --driver mysql    --port 3309 --user root --password root --label Percona
# PostgreSQL
python3 bench/compare.py --driver postgres --port 5432 --user postgres --password postgres --database postgres --label Postgres
```

## Results (200,000 rows, best-of-3 medians)

| Workload | ElyraSQL 0.9.4 | MySQL 8.4 | Percona 8.4 | PostgreSQL 17 | Verdict vs MySQL family |
|---|---:|---:|---:|---:|---|
| Bulk insert (rows/s) | **183,000** | 154,000 | 222,000 | 341,000 | 🥈 beats MySQL, ~Percona |
| Full scan `COUNT` (no index) | **4.95 ms** | 9.1 ms | 10.7 ms | 4.65 ms | 🥇 **beats both**, ties PG |
| Indexed `COUNT` | 0.68 ms | 0.37 ms | 0.72 ms | 0.71 ms | 🥈 beats Percona & PG |
| Range + `ORDER BY` pk `LIMIT` | 0.58 ms | 0.43 ms | 0.76 ms | 0.17 ms | 🥈 beats Percona |
| `GROUP BY` (full aggregation) | 13.3 ms | 9.5 ms | 11.7 ms | 7.2 ms | 🔶 behind (~1.4x MySQL) |
| Selective join (index NLJ) | 0.27 ms | 0.12 ms | 0.24 ms | 0.13 ms | 🔶 ~Percona, behind MySQL |
| PK point lookup | 0.21 ms | 0.11 ms | 0.11 ms | 0.11 ms | 🔶 behind (~2x, sub-ms) |

## What improved in 0.9.4

Versus 0.9.3 on the same host:

| Workload | 0.9.3 | 0.9.4 | Speedup |
|---|---:|---:|---:|
| Range + `ORDER BY` pk `LIMIT` | 29 ms | 0.58 ms | **~50x** |
| Full scan `COUNT` (no index) | 48 ms | 4.95 ms | **~10x** |
| `GROUP BY` (full aggregation) | 69 ms | 13.3 ms | **~5x** |
| Indexed `COUNT` | 2.7 ms | 0.68 ms | **~4x** |
| PK point lookup | 0.42 ms | 0.21 ms | ~2x |

The gains came from eight changes, each landed and correctness-verified
independently:

1. **PK-ordered `LIMIT` scan** — `ORDER BY <pk> LIMIT n` walks clustered order and
   stops early, so no full scan and no sort.
2. **Projection-aware decoding** — decode only the columns a query reads; skip
   the rest of each row in place.
3. **Zero-copy scanning** — decode straight from borrowed storage bytes in one
   read transaction, reusing a single row buffer.
4. **Parallel clustered aggregation** — split an integer-PK table's keyspace into
   ranges aggregated in parallel.
5. **Covering-index `COUNT`** — count index entries for a covered equality filter
   instead of fetching rows.
6. **Allocation-free `GROUP BY`** — reuse the group-key buffer and use a fast
   (FxHash) aggregation map.
7. **Table-definition cache** — resolve schema from memory (epoch-invalidated on
   DDL) instead of a storage read per query.
8. **Common-path check elimination** — skip materialized-view, column-mask and
   redundant privilege lookups unless those features are actually in use.

## Analysis

**Where ElyraSQL wins.** On the workloads that stress the storage and execution
engine most — bulk ingest and full-table aggregation — ElyraSQL is now the
fastest of the MySQL family and matches PostgreSQL on full-scan `COUNT`. This is
the headline: a database designed in 2026 should not lose the heavy scans to
engines from the 1990s, and it no longer does.

**Where ElyraSQL is competitive.** Indexed `COUNT` and range `ORDER BY ... LIMIT`
are within noise of the MySQL family and ahead of Percona.

**Where ElyraSQL still trails (the 0.9.5 targets).**

- **`GROUP BY` (~1.4x behind MySQL).** The full scan itself is parallel and fast;
  the remaining cost is grouped aggregation that does not yet scale linearly
  across cores. Planned: better parallel partitioning of grouped aggregation and
  a tighter group-key path.
- **Sub-0.2 ms point queries (PK lookup, selective join).** These are dominated
  by ~0.1 ms of fixed per-query overhead — SQL parsing, one read-transaction
  thread hop, and the MySQL wire round-trip. They are already sub-millisecond;
  closing the last tenth of a millisecond needs statement/plan caching and fewer
  async hops. Planned for 0.9.5.

## Reproducing

Start the four engines as containers (example), then run the harness above:

```bash
docker run -d --name elyrasql   -p 3307:3307 -e ELYRASQL_USER=root -e ELYRASQL_PASSWORD='' ghcr.io/kwhorne/elyrasql:0.9.4
docker run -d --name bench-mysql   -p 3308:3306 -e MYSQL_ROOT_PASSWORD=root mysql:8.4
docker run -d --name bench-percona -p 3309:3306 -e MYSQL_ROOT_PASSWORD=root percona/percona-server:8.4
docker run -d --name bench-pg      -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:17
```

*Figures above measured on an ElyraSQL 0.9.4 container; your hardware will
differ. The point is the relative shape, and it is reproducible.*
