# Performance

Numbers below are from the reproducible harness in
[`bench/benchmark.py`](https://github.com/kwhorne/ElyraSQL/blob/main/bench/benchmark.py),
release build, 100,000 rows, single client, medians. Treat them as relative —
re-run on your hardware.

```bash
cargo build --release
./target/release/elyrasql serve --data /tmp/bench.edb --listen 127.0.0.1:3440 &
python3 bench/benchmark.py --port 3440 --rows 100000
```

| Workload | Median |
|----------|-------:|
| Bulk insert 100k rows | ~180,000 rows/s |
| PK point lookup | ~0.15 ms |
| Selective join (index nested-loop) | ~0.18 ms |
| Indexed `COUNT` (~1,667 matches) | ~0.9 ms |
| Vector ANN, cached (20k × 32-d) | ~0.3 ms |
| Full scan `COUNT` (no index) | ~11 ms |
| `GROUP BY` (full aggregation) | ~18 ms |

Selective join scaling (50k × 50k):

| Strategy | Time |
|----------|-----:|
| Index nested-loop (small driver, indexed partner) | ~0.3 ms |
| Hash join + predicate pushdown | ~12 ms |

Range scans (200k rows):

| Query | Time |
|-------|-----:|
| PK range `COUNT` (`id >= …`) | ~0.4 ms |
| Indexed range (`BETWEEN`, ~6k matches) | ~6 ms |
| Non-indexed range (full scan) | ~18 ms |

## Why it's fast

- **Clustered primary keys** and order-preserving encoding make point lookups
  and range scans B-tree operations.
- **Batched multi-get** fetches index matches in a single read transaction.
- **Index nested-loop joins** avoid materializing the partner for selective
  joins; **hash joins** handle the general equi-join case in `O(n+m)`.
- **Group commit** amortizes write durability across concurrent writers.
- **Streaming execution** keeps memory bounded on scans and aggregations.
- **HNSW** brings vector search from `O(n)` exact to sub-millisecond ANN.

## Honest caveats

- `ORDER BY`, grouped/aggregated output, and in-transaction reads materialize
  their working set.
- Range and index nested-loop paths are single-column.
- The vector HNSW index pays a one-time build cost after each table change.
