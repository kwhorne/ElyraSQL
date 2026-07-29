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
| `COUNT(*)` | 25.2 | 28.7 | **24.0** |
| Global aggregation (`SUM/AVG/MIN/MAX`) | **35.9** | 45.1 | 162.4 |
| `GROUP BY` low-cardinality (100 groups) | **48.5** | 75.0 | 312.2 |
| `GROUP BY` + top-10 (10k groups) | **53.5** | 95.9 | 344.6 |
| Filtered aggregation (`WHERE amount>500`) | **50.5** | 54.5 | 229.5 |

**ElyraSQL is the fastest of the three on every aggregation query** — global
aggregation, both `GROUP BY` shapes, and the filtered aggregation — typically
2–6× ahead of MySQL and up to ~1.8× ahead of PostgreSQL on high-cardinality
`GROUP BY`. On a bare `COUNT(*)` the three are within noise (MySQL a hair ahead).
This is unusual for a row store and comes from the OLAP work in the 0.9.x line,
carried into 1.0: parallel clustered scans, a bounded table-keyspace split,
vectorised (columnar) scalar *and grouped* aggregation over flat `f64` arrays,
and a compiled predicate for filtered aggregation.

## Core SQL — 200,000 rows (medians, ms; lower is better)

| Workload | ElyraSQL | MySQL 8.4 | PostgreSQL 17 |
|---|---:|---:|---:|
| `GROUP BY` (full aggregation) | **9.8** | 21.4 | 16.1 |
| Full scan `COUNT` (no index) | **9.3** | 20.8 | 10.4 |
| Bulk insert (rows/s) | 162,000 | 179,000 | 187,000 |
| Indexed `COUNT` | 0.90 | 0.65 | 1.21 |
| Selective join (index NLJ) | 0.39 | 0.45 | 0.24 |
| PK point lookup | 0.26 | 0.27 | 0.19 |
| Range + `ORDER BY` pk `LIMIT` | 0.85 | 0.85 | 0.30 |

ElyraSQL leads on `GROUP BY` and full-scan `COUNT`, beats MySQL on the point
queries, and is within noise of the field on bulk insert and indexed lookups.
PostgreSQL keeps a small edge on the sub-millisecond point/range queries (mature
tuple format + planner); those are already well under a millisecond.

## Row-oriented paths — the 1.6.0 work

The core-SQL table above measures *selective* shapes. The shapes that used to be
weak were the ones that emit or scan many rows, where the cost scaled with how
wide a row is rather than with what the query reads. 1.6.0 fixed that. Both
binaries were run **alternately against the same data file** (medians of 5, two
rounds, 200,000 rows; MySQL 8.4 on the same host for reference):

| Shape | 1.5.1 | 1.6.0 | | MySQL 8.4 |
|---|---:|---:|---|---:|
| `ORDER BY int LIMIT 100`, 12-column rows | 95 | **31** | 3.0x | 56 |
| `ORDER BY int LIMIT 100`, 3-column rows | 44 | **22** | 2.0x | 28 |
| `ORDER BY text LIMIT 100`, 12-column rows | 98 | **39** | 2.5x | 57 |
| 1:1 join on a PK, `COUNT(*)`, 12-column | 492 | **150** | 3.3x | 91 |
| 1:1 join on a PK, `COUNT(*)`, 3-column | 270 | **106** | 2.5x | 71 |
| 1:N join emitting 40M rows, 12-column | 33 298 | **1130** | **29x** | 535 |
| 1:N join emitting 40M rows, 3-column | 12 112 | **768** | **16x** | 529 |
| join + `ORDER BY ... LIMIT 100` | 514 | **157** | 3.3x | 31 |
| `ORDER BY` with no `LIMIT` (control) | 1951 | 1964 | — | 1869 |
| `COUNT(*)` scan (control) | 3.0 | 3.0 | — | 9.7 |

Three things to read from this rather than just the ratios. First, **the width
sensitivity is what identified the defect and it is what closed**: the 40M-row
join was 2.7x slower on 12-column rows than on 3-column ones and is now 1.5x
(MySQL: 1.01x). Second, **the controls did not move** — an unbounded sort and a
plain scan are the same as in 1.5.1, so nothing was traded away for these gains.
Third, the two join rows differ only in *fanout* (200k emitted vs 40M from the
same inputs), which is what separates per-key cost from per-emitted-row cost; a
single row count would have hidden which one was being fixed.

Where MySQL is still ahead: joins emitting very many rows (1.8x) and the joined
`ORDER BY ... LIMIT` shape (4.5x, where it appears not to materialise every
joined row before the top-N), plus index-driven inequality joins, which ElyraSQL
does not plan at all — a `BETWEEN` band join is 32 ms there against 3507 ms here.
Reproduce all of the above with [`bench/latemat.py`](bench/latemat.py), which
varies row width and join fanout deliberately.

## Notes

- **Bulk insert** trails only at tiny (2k-row) autocommit batches, where
  ElyraSQL's crash-safe copy-on-write commit flushes more than a write-ahead-log
  append would; at realistic bulk-load batch sizes (≥10k rows or `LOAD DATA`)
  it reaches ~351k rows/s, ahead of MySQL's ~290k.
- **ClickHouse** is intentionally excluded: it is a columnar engine, a different
  architecture class, not a like-for-like target for a row store. It can be
  added with `bench/olap.py --engines elyra,clickhouse`.
- Reproduce locally with [`bench/compare.py`](bench/compare.py) (core SQL),
  [`bench/olap.py`](bench/olap.py) (OLAP) and
  [`bench/latemat.py`](bench/latemat.py) (row width and join fanout); numbers vary
  ±10–20% run-to-run, so compare medians and re-run A/B against the *same* data
  file — comparing across freshly loaded files produced a 20% phantom difference
  during the 1.6.0 work.
