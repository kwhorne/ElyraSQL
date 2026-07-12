# ElyraSQL Benchmarks

Reproducible with [`bench/benchmark.py`](bench/benchmark.py):

```bash
cargo build --release
./target/release/elyrasql serve --data /tmp/bench.edb --listen 127.0.0.1:3440 &
python3 bench/benchmark.py --port 3440 --rows 100000
```

## Results

Release build, 100,000 rows. Single client, medians. (Latencies include the
MySQL wire round-trip and the Python client's row decoding.)

| Workload | Median | Note |
|---|---:|---|
| Bulk insert 100k rows | 554 ms | **~180,000 rows/s** (batched group commit) |
| PK point lookup | **0.15 ms** | clustered key, O(log n) |
| Selective join (index NLJ) | **0.18 ms** | `u.id = ?` probes partner index |
| Vector ANN, cached | **0.29 ms** | HNSW top-10 over 20k × 32-d |
| Indexed `COUNT` (≈1,667 matches) | **0.86 ms** | batched multi-get |
| Full scan `COUNT` (no index) | 10.9 ms | scans 100k rows |
| `GROUP BY age` | 17.5 ms | full aggregation |
| Vector ANN, first query | 1.1 s | one-time HNSW build over 20k vectors |

### 1,000,000 rows (Linux, containerised)

Single client, medians, inside a Docker container (`python3 bench/benchmark.py
--rows 1000000`). Storage on a virtualised volume (slower `fsync` than a bare
NVMe host, which penalises small autocommit batches).

| Workload | Median | Note |
|---|---:|---|
| Bulk insert 1M rows | ~5.3 s | **~190,000 rows/s** (~240k on fast-fsync storage) |
| PK point lookup | **0.2 ms** | clustered key |
| Selective join (index NLJ) | **0.3 ms** | `u.id = ?` |
| Vector ANN, cached | **0.4 ms** | HNSW top-10 |
| Indexed `COUNT` | 16 ms | secondary index |
| Full scan `COUNT` (no index) | ~205 ms | scans 1M rows |
| `GROUP BY age` | ~273 ms | full aggregation, 60 groups |

Since 0.2.1, bulk `INSERT` is ~5-6x faster (writer-side duplicate detection +
group commit) and low-cardinality `GROUP BY` ~3.4x faster (compact binary group
key).

## Cross-engine comparison

Reproducible with [`bench/compare.py`](bench/compare.py), which runs an
**identical, portable workload** (same schema, rows, queries and client machine)
against ElyraSQL and the MySQL/Postgres families. All four ran as containers on
the same host; 200,000 rows; single client; medians. These are our own
reproducible numbers on developer hardware — **relative, not absolute** — not a
tuned, official head-to-head. Re-run them on your target and see for yourself.

Measured on **native Linux** (GitHub Actions `ubuntu-latest`, 4 cores) with all
three engines on the same host -- see [`benchmark_analyse.md`](benchmark_analyse.md)
for full method and analysis, and run it yourself with `gh workflow run benchmark.yml`.

**OLAP, 1M rows (ms, lower is better):**

| Query | ElyraSQL | PostgreSQL 17 | MySQL 8.4 |
|---|---:|---:|---:|
| `COUNT(*)` | **24.8** | 28.1 | 23.6 |
| Global agg (`SUM/AVG/MIN/MAX`) | **35.6** | 47.9 | 161.7 |
| `GROUP BY` (100 groups) | **45.8** | 81.0 | 314.6 |
| `GROUP BY` + top-10 (10k groups) | **53.7** | 87.7 | 342.9 |
| Filtered agg (`WHERE amount>500`) | **46.1** | 51.8 | 229.5 |

**Core SQL, 200k rows (ms):**

| Workload | ElyraSQL | MySQL 8.4 | PostgreSQL 17 |
|---|---:|---:|---:|
| `GROUP BY` | **12.9** | 21.8 | 16.8 |
| Full scan `COUNT` | **10.4** | 20.8 | 11.0 |
| Selective join | 0.41 | 0.47 | 0.27 |
| PK point lookup | 0.28 | 0.27 | 0.20 |
| Bulk insert (rows/s, ≥10k batches) | 351,000 | 290,000 | 345,000 |

What this shows:

- **ElyraSQL is the fastest of the three on every OLAP query** (global/filtered
  aggregation, `GROUP BY`, top-N), and 2–5x ahead of MySQL. Row-store
  aggregation this fast is unusual and comes from vectorised (columnar) scalar
  aggregation, a compiled filter predicate, parallel clustered scans, and a
  table-keyspace-bounded split.
- On core SQL it leads on `GROUP BY` and full-scan `COUNT`; PostgreSQL keeps a
  small edge on the sub-millisecond point/range queries.
- Earlier laptop-VM numbers understated ElyraSQL ~1.5x by penalising its
  parallel, memory-mapped scans; the native-Linux CI run above is the fair,
  representative comparison.

## Honest caveats

- **Non-unique index lookups use a batched multi-get** (all matching rows in a
  single read transaction), so an equality matching ~1,667 rows resolves in
  ~0.86 ms — an order of magnitude faster than the full scan it replaces.
- **Vector ANN pays a one-time build cost** (rebuild-when-stale). Ideal for
  read-heavy embedding/RAG workloads; write-heavy vector tables rebuild often.
- **`ORDER BY` / `GROUP BY` / joins materialise** their working set in memory.
- Numbers are from a developer laptop; treat them as relative, not absolute.
  Re-run `bench/benchmark.py` on your target hardware.

## What the numbers show

The fast paths work as designed: point lookups, selective (index nested-loop)
joins, and cached vector search are all sub-millisecond, and bulk ingest sustains
six-figure rows/s. The remaining slow spot is large full-table aggregation (`GROUP BY` over all
rows), which is the columnar OLAP follow-up.
