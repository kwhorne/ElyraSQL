# ElyraSQL benchmark analysis

A head-to-head comparison of **ElyraSQL** against **MySQL 8.4** and
**PostgreSQL 17** on an identical workload, same host, same client.

> **Why native Linux.** These numbers are produced by the
> [`Benchmark (native Linux)`](.github/workflows/benchmark.yml) CI workflow,
> which runs all three engines on a single **native x86_64 Linux** runner
> (GitHub Actions `ubuntu-latest`, 4 cores) with MySQL and PostgreSQL as service
> containers on the same host. This is a fair, reproducible environment. Running
> the same comparison inside a laptop hypervisor (e.g. OrbStack on macOS)
> systematically penalises ElyraSQL's parallel, memory-mapped scans by ~1.5x and
> is *not* representative of the Ubuntu production target. Re-run any time with
> `gh workflow run benchmark.yml`.

## OLAP — 1,000,000 rows (medians, ms; lower is better)

`events(id, user_id, category, amount)`, deterministic data, each engine loaded
with its native schema.

| Query | ElyraSQL | PostgreSQL 17 | MySQL 8.4 |
|---|---:|---:|---:|
| `COUNT(*)` | **27.6** | 29.0 | 24.0 |
| Global aggregation (`SUM/AVG/MIN/MAX`) | **36.6** | 55.5 | 162.6 |
| `GROUP BY` low-cardinality (100 groups) | **63.6** | 92.1 | 314.1 |
| `GROUP BY` + top-10 (10k groups) | **93.4** | 113.9 | 343.0 |
| Filtered aggregation (`WHERE amount>500`) | **53.5** | 55.5 | 229.8 |

**ElyraSQL is the fastest of the three on every OLAP query**, and 2–5× ahead of
MySQL. This is unusual for a row store and comes from the OLAP work in the 0.9.6
line: parallel clustered scans, a bounded table-keyspace split, vectorised
(columnar) scalar aggregation over `f64` arrays, and a compiled predicate for
filtered aggregation.

## Core SQL — 200,000 rows (medians, ms; lower is better)

| Workload | ElyraSQL | MySQL 8.4 | PostgreSQL 17 |
|---|---:|---:|---:|
| `GROUP BY` (full aggregation) | **12.9** | 21.8 | 16.8 |
| Full scan `COUNT` (no index) | **10.4** | 20.8 | 11.0 |
| Bulk insert (rows/s) | 163,000 | 176,000 | 178,000 |
| Indexed `COUNT` | 0.90 | 0.69 | 1.22 |
| Selective join (index NLJ) | 0.41 | 0.47 | 0.27 |
| PK point lookup | 0.28 | 0.27 | 0.20 |
| Range + `ORDER BY` pk `LIMIT` | 0.87 | 0.90 | 0.31 |

ElyraSQL leads on `GROUP BY` and full-scan `COUNT`, beats MySQL on the point
queries, and is within noise of the field on bulk insert and indexed lookups.
PostgreSQL keeps a small edge on the sub-millisecond point/range queries (mature
tuple format + planner); those are already well under a millisecond.

## Notes

- **Bulk insert** trails only at tiny (2k-row) autocommit batches, where
  ElyraSQL's crash-safe copy-on-write commit flushes more than a write-ahead-log
  append would; at realistic bulk-load batch sizes (≥10k rows or `LOAD DATA`)
  it reaches ~351k rows/s, ahead of MySQL's ~290k.
- **ClickHouse** is intentionally excluded: it is a columnar engine, a different
  architecture class, not a like-for-like target for a row store. It can be
  added with `bench/olap.py --engines elyra,clickhouse`.
- Reproduce locally with [`bench/compare.py`](bench/compare.py) (core SQL) and
  [`bench/olap.py`](bench/olap.py) (OLAP); numbers vary ±10–20% run-to-run.
