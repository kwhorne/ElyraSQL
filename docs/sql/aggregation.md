# Aggregation

This page covers aggregate **SQL syntax**. For how large aggregations execute
(streaming, parallel, index-aware), see [Analytics (OLAP)](../olap.md).

## Aggregate functions

`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, each with an optional `DISTINCT`:

```sql
SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales;
SELECT COUNT(DISTINCT region) FROM sales;
```

- `COUNT(*)` over zero rows returns `0`.
- `SUM`/`MIN`/`MAX` preserve the argument's type where meaningful (e.g. `SUM`
  over `DECIMAL` is exact); `AVG` returns a float.

## GROUP BY

```sql
SELECT region, COUNT(*), SUM(amount) AS total
FROM sales
GROUP BY region
ORDER BY total DESC;
```

`GROUP BY` accepts one or more columns. Aggregation works over joined result
sets too.

## HAVING

Filter groups after aggregation. `HAVING` may reference aggregates or a
projection alias:

```sql
SELECT region, SUM(amount) AS total
FROM sales
GROUP BY region
HAVING SUM(amount) > 1000        -- or: HAVING total > 1000
ORDER BY total DESC;

SELECT region, COUNT(*) AS n FROM sales GROUP BY region HAVING COUNT(*) >= 3;
```

`HAVING` references must appear in the SELECT list (as an aggregate expression
or an alias) or be a grouped column.

## GROUP_CONCAT

```sql
SELECT region, GROUP_CONCAT(name) AS names FROM stores GROUP BY region;
SELECT region, GROUP_CONCAT(DISTINCT name SEPARATOR '; ') FROM stores GROUP BY region;
```

`GROUP_CONCAT` concatenates a group's values (default separator `,`), and
supports `DISTINCT` and a custom `SEPARATOR`. Ordering within the group follows
row order (an inner `ORDER BY` is not yet applied).

## Window functions

Window functions compute a value per row over a partition, without collapsing
rows:

```sql
SELECT id, region, amount,
       ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn,
       RANK()       OVER (ORDER BY amount)                          AS rk,
       SUM(amount)  OVER (PARTITION BY region ORDER BY id)         AS running,
       SUM(amount)  OVER (PARTITION BY region)                     AS region_total,
       LAG(amount)  OVER (ORDER BY id)                             AS prev
FROM sales;
```

Supported: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`
`OVER (...)`, and `LAG`/`LEAD`. With `ORDER BY` in the window, aggregates are
**running** (cumulative, peers share a value); without it they cover the whole
partition.

### Frames

Explicit `ROWS` frames (physical row offsets) are supported:

```sql
-- 3-row moving sum
SUM(v) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
-- centered average
AVG(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
-- suffix sum
SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
```

`RANGE` supports the whole-partition (`UNBOUNDED PRECEDING AND UNBOUNDED
FOLLOWING`) and running (`UNBOUNDED PRECEDING AND CURRENT ROW`) forms.

!!! note
    `RANGE`/`GROUPS` with numeric offsets, frame `EXCLUDE`, and named windows
    are not supported.

## The OLAP engine

Large aggregations run through a dedicated analytical path:

- **Streaming** — the table is scanned in batches; only per-group state is
  retained, so memory is proportional to the **number of groups**, not the
  table size. Aggregating a billion-row table does not exhaust memory.
- **Parallel** — batches are aggregated across worker threads and merged, using
  all cores.
- **Index-aware** — an aggregation with a selective, indexed `WHERE` (equality
  or range) reads only the matching rows via the index instead of scanning.

```sql
-- reads only matching rows via the index, then aggregates
SELECT SUM(amount) FROM sales WHERE region = 'north';

-- full parallel streaming aggregation
SELECT region, COUNT(*) FROM sales GROUP BY region;
```

!!! note
    This is a row-oriented, parallel streaming aggregator — not a columnar
    engine. It gives bounded memory and multi-core scaling; a columnar store
    with spill-to-disk is future work. The engine, strategies, and tuning are
    documented in detail under [Analytics (OLAP)](../olap.md).
