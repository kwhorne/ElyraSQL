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

Ordered `LIMIT` / paged grids (300k rows, no filter):

| Query | Time |
|-------|-----:|
| `ORDER BY <pk> ASC LIMIT 40` | <1 ms |
| `ORDER BY <pk> DESC LIMIT 40` | <1 ms |
| `ORDER BY <indexed col> ASC\|DESC LIMIT 40` (incl. nullable) | <1 ms |
| `WHERE active=1 ORDER BY <indexed col> DESC LIMIT 40` | ~0.5 ms |
| `WHERE region=3 ORDER BY <indexed col> LIMIT 40` (~10%) | ~1 ms |

These walk an index/clustered keyspace and stop after `OFFSET + LIMIT` rows, so
the cost is independent of table size (a full sort of the same data took several
seconds). A `WHERE` filter is applied as a residual during the walk; a very
selective filter falls back to the sorter (bounded by
`ELYRASQL_ORDER_SCAN_BUDGET`), which is cheap because it has few matches.

## Why it's fast

- **Clustered primary keys** and order-preserving encoding make point lookups
  and range scans B-tree operations.
- **Ordered `LIMIT`** (a paged grid: `ORDER BY <col> ASC|DESC LIMIT n OFFSET k`)
  walks the primary key (either direction) or a secondary index in order and
  stops after `k + n` rows -- top-N without sorting the table. Works on a nullable
  single-column index (NULL rows spliced in per MySQL ordering). A `WHERE` filter
  is applied as a residual during the walk (budget-guarded fallback for very
  selective filters).
- **Batched multi-get** fetches index matches in a single read transaction.
- **Index nested-loop joins** avoid materializing the partner for selective
  joins; **hash joins** handle the general equi-join case in `O(n+m)`.
- **Group commit** amortizes write durability across concurrent writers.
- **Streaming execution** keeps memory bounded on scans and aggregations.
- **HNSW** brings vector search from `O(n)` exact to sub-millisecond ANN.

## Honest caveats

- An **unaccelerated** `ORDER BY` (nullable sort column, a `WHERE` filter, an
  expression key, or inside a transaction), grouped/aggregated output, and
  in-transaction reads materialize their working set (memory-bounded: top-N heap
  or external merge sort). Indexed ordered `LIMIT` (above) is the fast path.
- Range and index nested-loop paths are single-column.
- The vector HNSW index pays a one-time build cost after each table change.
