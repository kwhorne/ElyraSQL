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

Best-of-3 medians (least host contention):

| Workload | ElyraSQL | MySQL 8.4 | Percona 8.4 | PostgreSQL 17 |
|---|---:|---:|---:|---:|
| Bulk insert (rows/s) | **183,000** | 154,000 | 222,000 | 341,000 |
| Full scan `COUNT` (no index) | **4.95 ms** | 9.1 ms | 10.7 ms | 4.65 ms |
| Indexed `COUNT` | 0.68 ms | 0.37 ms | 0.72 ms | 0.71 ms |
| Range + `ORDER BY` pk `LIMIT` | 0.58 ms | 0.43 ms | 0.76 ms | 0.17 ms |
| `GROUP BY` (full aggregation) | 13.3 ms | 9.5 ms | 11.7 ms | 7.2 ms |
| Selective join (index NLJ) | 0.27 ms | 0.12 ms | 0.24 ms | 0.13 ms |
| PK point lookup | 0.21 ms | 0.11 ms | 0.11 ms | 0.11 ms |

(Numbers vary run-to-run by ~10-30% under shared-host contention; the ordering
above is the consistent picture across repeated runs.)

What this shows, honestly:

- **ElyraSQL beats MySQL and Percona on full-table `COUNT` and bulk ingest**,
  matches PostgreSQL on full-scan `COUNT`, and beats Percona on indexed `COUNT`
  and range `ORDER BY` — a >10x improvement on scan aggregation over earlier
  releases (full-scan `COUNT` was ~48 ms).
- **`ORDER BY pk LIMIT` is fast** (~0.6 ms, was ~29 ms): a PK-ordered
  early-termination scan avoids materialising and sorting the whole result set.
- **Still behind on `GROUP BY`** (~1.4x vs MySQL, ~2x vs PostgreSQL) and on
  **sub-0.2 ms point queries** (PK lookup ~0.1 ms of fixed per-query overhead:
  SQL parse + one read-transaction hop + the MySQL wire round-trip). These, and
  parallel grouped aggregation, are the next targets.

The speedups came from: a PK-ordered early-terminating `ORDER BY ... LIMIT`
scan; projection-aware decoding (materialise only the columns a query reads);
zero-copy scanning (decode straight from borrowed storage bytes in one read
transaction); a parallel clustered-range split for integer-PK tables;
covering-index `COUNT`; an allocation-free FxHash group-by hot path; an
in-memory table-definition cache; and skipping materialized-view / column-mask
checks (and redundant privilege lookups) on the common query path.

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
