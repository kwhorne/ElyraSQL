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

A deep `OFFSET` (no filter) steps over the leading rows at the index level
**without reading them**, so paging far into a result stays cheap (index steps,
not row reads). Sorting a **nullable** single-column index works on the fast path
in both directions (NULL rows are indexed under a companion keyspace); see
[limitations](limitations.md).

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
- **`IN` lists use the index, or a hash set.** `col IN (...)` on an indexed column is
  served by index lookups unioned by storage key (bounded by the same budget as a
  range, so a list covering most of the table falls back to a scan having paid only
  for key lookups). When a scan *is* the right plan, a numeric `IN` list compiles to
  an O(1) membership test rather than being walked per row, and the set's span is
  exposed to zone maps so chunks outside it are still skipped. Measured on 200k rows:
  `IN (5 values)` **4.5 ms → 1.3 ms**, `IN (500 values)` **102 ms → 5.7 ms**.
- **Index ranges are used only when they pay.** A secondary-index range fetches each
  matching row by key, which costs far more per row than a sequential decode, so a
  range matching more than `ELYRASQL_INDEX_RANGE_MAX_FRACTION` of the table falls back
  to a scan. Measured on 200k rows: `COUNT(*) WHERE amt > 0` went from **124 ms to
  16.5 ms**, `amt > 49999` from 62.8 ms to 9.2 ms, while a selective `amt > 99000`
  still takes the index at 1.2 ms. The check happens after the index keys are walked
  but before any row is fetched, so a misjudged range costs only a key-only walk.
- **CPU-heavy stretches run off the reactor** (`block_in_place`) and the streaming
  join loops yield periodically, so one expensive query cannot stall the listener or
  other sessions — with 32 concurrent runaway queries on 16 cores, a new connection
  is still answered in under 0.1 s.
- **Columns no query reads are never decoded.** Rows are stored as one encoded
  blob, so materialising a column costs an allocation for every `TEXT`/`JSON`
  value — which is pure waste when nothing reads it. Scans decode only the
  columns a query references (unread ones are skipped in place and stand in as
  `NULL` placeholders at their own position, so nothing downstream shifts):
  - `ORDER BY ... LIMIT k` decodes only the filter and sort-key columns, runs the
    top-N admission test, and pays for the full row only for the rows that make
    the cut. On 200k rows with 12 columns: **94 ms → 29 ms**.
  - Streaming joins decode each side with the same mask, so a `COUNT(*)` or a
    single-column `SUM` over a join stops copying columns nobody selected. On a
    200k-row 1:1 join of two 12-column tables: **488 ms → 226 ms**, and the cost
    of *widening* the rows drops with it (wide/narrow went from 1.8x to 1.35x).

  `SELECT *` genuinely reads every column and is unaffected, as is any query
  whose expressions can't be attributed to columns statically — those decode
  everything, exactly as before.
- **Compiled regular expressions are cached**, so `WHERE col REGEXP '...'` compiles
  the pattern once instead of once per row. Compilation costs far more than
  matching, so this dominates such scans: a `COUNT(*)` with a `REGEXP` filter over
  800k rows went from failing to finish inside a 2-second budget to **0.12 s**.

## Honest caveats

- An **unaccelerated** `ORDER BY` (nullable sort column, a `WHERE` filter, an
  expression key, or inside a transaction), grouped/aggregated output, and
  in-transaction reads materialize their working set (memory-bounded: top-N heap
  or external merge sort). Indexed ordered `LIMIT` (above) is the fast path.
- Range and index nested-loop paths are single-column.
- The vector HNSW index pays a one-time build cost after each table change.
