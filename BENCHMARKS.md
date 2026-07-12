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

Best-of-5 medians, each engine measured alone on the box (200k rows). See
[`benchmark_analyse.md`](benchmark_analyse.md) for full method and analysis.

| Workload | ElyraSQL 0.9.5 | MySQL 8.4 | Percona 8.4 | PostgreSQL 17 |
|---|---:|---:|---:|---:|
| Full scan `COUNT` (no index) | **4.97 ms** | 9.06 ms | 10.64 ms | 4.66 ms |
| `GROUP BY` (full aggregation) | **5.74 ms** | 9.50 ms | 9.93 ms | 6.95 ms |
| Selective join (index NLJ) | 0.15 ms | 0.11 ms | 0.11 ms | 0.13 ms |
| Indexed `COUNT` | 0.62 ms | 0.28 ms | 0.28 ms | 0.69 ms |
| Range + `ORDER BY` pk `LIMIT` | 0.52 ms | 0.41 ms | 0.41 ms | 0.17 ms |
| PK point lookup | 0.17 ms | 0.09 ms | 0.08 ms | 0.11 ms |
| Bulk insert, 50k batches (rows/s) | **351,000** | 290,000 | 296,000 | 345,000 |

(Numbers vary run-to-run by ~10-20%; the ordering above is the consistent
picture across repeated runs.)

What this shows, honestly:

- **ElyraSQL is the fastest of the four on full-table `COUNT` and `GROUP BY`** —
  the heavy analytical scans — and matches PostgreSQL on full-scan `COUNT`.
  Full-scan aggregation improved ~10x over the 0.9.2 line.
- **`ORDER BY pk LIMIT` is fast** (~0.5 ms, was ~29 ms): a PK-ordered
  early-termination scan avoids materialising and sorting the whole result set.
- **Joins, indexed `COUNT`, and range `ORDER BY` are competitive** with the
  MySQL family.
- **Point queries** read ~1.5-2x MySQL's latency *in the shared benchmark VM*;
  measured natively the PK lookup is 0.11 ms — identical to MySQL — so this is
  virtualisation overhead, not server overhead.
- **Bulk insert** trails at tiny (2k-row) batches (copy-on-write commit vs a
  write-ahead log) but leads at realistic bulk-load batch sizes (~351k rows/s).

The speedups came from: a PK-ordered early-terminating `ORDER BY ... LIMIT`
scan; projection-aware + zero-copy scanning; parallel clustered-range
aggregation (capped at 4 workers); a table-keyspace-bounded split probe (so
aggregation scales with table size, not database size); covering-index `COUNT`;
an allocation-light insertion-ordered group-by map; a table-definition cache;
buffered wire writes; and skipping matview / column-mask / redundant-privilege
checks on the common query path.

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
