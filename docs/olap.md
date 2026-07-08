# Analytics (OLAP)

ElyraSQL runs analytical queries — large aggregations and scans — through a
dedicated engine designed for two properties that matter at scale: **bounded
memory** and **multi-core parallelism**. This page explains how it works, when
it kicks in, and how to get the most from it.

For the SQL syntax of aggregate functions and `GROUP BY`, see
[Aggregation](sql/aggregation.md). This page is about the execution engine
behind them.

## What "OLAP" means here

ElyraSQL is a single-file, row-oriented database that serves both transactional
(OLTP) and analytical (OLAP) workloads from the same data — no separate
warehouse, no ETL. The analytical path is a **parallel, streaming group
aggregator**: it reads the table in batches, aggregates each batch on a worker
thread, and merges partial results.

!!! note "Not a columnar engine"
    This is a row-oriented, parallel streaming aggregator. It delivers bounded
    memory and multi-core scaling, but it is **not** a columnar/vectorized store
    like DuckDB or Apache DataFusion, and it does not spill sort/hash buffers to
    disk. See [Limitations](#limitations-and-tradeoffs).

## When the OLAP path is used

Any `SELECT` that contains an aggregate function (`COUNT`, `SUM`, `AVG`, `MIN`,
`MAX`) or a `GROUP BY` is executed by the analytical engine:

```sql
SELECT COUNT(*) FROM events;
SELECT region, SUM(amount) FROM sales GROUP BY region;
SELECT COUNT(DISTINCT user_id) FROM sessions;
```

The planner then picks one of two strategies:

| Situation | Strategy |
|-----------|----------|
| Selective, indexed `WHERE` (equality or range on a PK/indexed column) | fetch only matching rows via the index, then aggregate them |
| Everything else (no filter, or non-indexed filter) | parallel streaming scan over the whole table |

```sql
-- index-driven: reads only the matching rows, then aggregates
SELECT SUM(amount) FROM sales WHERE region = 'north';
SELECT COUNT(*)   FROM sales WHERE ts >= '2024-01-01';   -- range via index

-- full parallel streaming aggregation
SELECT region, COUNT(*), SUM(amount) FROM sales GROUP BY region;
```

## Execution model

### Streaming, bounded memory

The table is scanned in fixed-size batches. Each batch updates a set of
**group accumulators**; the rows themselves are then discarded. Memory is
therefore proportional to the **number of distinct groups**, not the number of
rows.

- `SELECT COUNT(*) FROM t` keeps a single counter — constant memory over any
  table size.
- `SELECT k, SUM(v) FROM t GROUP BY k` keeps one accumulator per distinct `k`.

Aggregating a billion-row table does not load a billion rows into memory.

### Parallelism

Batches are dispatched to worker threads (one per CPU by default). Each worker
builds a **partial aggregator**; partials are then merged into the final
result. Merging is exact and function-aware:

| Function | Merge rule |
|----------|-----------|
| `COUNT` / `COUNT(*)` | sum of counts |
| `SUM` | sum of sums |
| `AVG` | sum of sums and counts, divided at the end |
| `MIN` / `MAX` | min / max of partial extremes |
| `DISTINCT` variants | union of the per-partition value sets |

Because reads use MVCC snapshots, workers scan concurrently without contention.

### Index-aware aggregation

When the `WHERE` clause is a selective equality or range on a primary-key or
indexed column, the engine retrieves just the matching rows through the index
(a batched multi-get) and aggregates those — avoiding a full scan entirely.

```sql
-- with an index on region, this touches only 'north' rows
CREATE INDEX sales_region ON sales (region);
SELECT AVG(amount) FROM sales WHERE region = 'north';
```

## Worked example

```sql
CREATE TABLE sales (id BIGINT PRIMARY KEY, region TEXT, amount BIGINT);
-- ... load 1,000,000 rows ...

SELECT region, COUNT(*) AS n, SUM(amount) AS total, MAX(amount) AS top
FROM sales
GROUP BY region
ORDER BY total DESC;
```

What happens:

1. The scan streams `sales` in batches across all CPU cores.
2. Each worker accumulates per-`region` counters, sums, and running maxima.
3. Partials merge into one result (one row per region).
4. `ORDER BY total DESC` sorts the small grouped result.

On a developer laptop this aggregates 1,000,000 rows into ~1,000 groups in
roughly 100 ms, with working memory bounded by the group count. See
[Performance](performance.md) for more numbers.

## Decimal and typed aggregation

Aggregates respect the value types:

- `SUM` over `DECIMAL` is **exact** (no float rounding).
- `MIN`/`MAX` work across `DATE`, `DATETIME`, `TIME`, `DECIMAL`, text, and
  numbers using the same ordering as `ORDER BY`.
- `AVG` returns a floating-point result.

```sql
SELECT MIN(order_date), MAX(order_date), SUM(price) FROM orders;
```

## Getting the best performance

- **Index the filter column.** A selective `WHERE` on an indexed column turns a
  full scan into an index read.
- **Aggregate at the source.** Push `GROUP BY`/aggregates into the query rather
  than pulling rows to the client.
- **Fewer groups = less memory.** High-cardinality `GROUP BY` (e.g. by a unique
  id) keeps one accumulator per group; prefer grouping on lower-cardinality
  columns where possible.
- **Reads scale out.** Concurrent analytical queries each get their own MVCC
  snapshot and run in parallel with writers and with each other.

## Limitations and tradeoffs

- **Row-oriented, not columnar.** There is no columnar/vectorized execution or
  late materialization; each row is decoded even if only one column is
  aggregated.
- **No spill-to-disk.** Group state and any `ORDER BY` buffer live in memory.
  Group *state* is bounded by cardinality, but a final `ORDER BY` over a very
  large grouped result materializes that result.
- **Single-column index paths.** Index-driven aggregation uses single-column
  equality/range; composite-key ranges fall back to a scan.
- **No `HAVING`, window functions, or `ROLLUP`/`CUBE`** yet.

A columnar store with spill-to-disk is on the [roadmap](limitations.md).
