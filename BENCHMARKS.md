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
