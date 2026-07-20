# Queries & Joins

## SELECT

```sql
SELECT id, name FROM users WHERE age >= 18 ORDER BY name LIMIT 10 OFFSET 20;
SELECT * FROM users;
SELECT id, age + 1 AS next_age FROM users;
```

Supported clauses: projection (columns, `*`, expressions, aliases), `WHERE`,
`ORDER BY` (multiple keys, `ASC`/`DESC`), and `LIMIT`/`OFFSET`.

`ORDER BY` may reference an output alias or any table column (even one not
projected).

## WHERE

```sql
WHERE age BETWEEN 18 AND 65
  AND status = 'active'
  AND (name IS NOT NULL)
```

Operators: `=`, `!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `BETWEEN`,
`IS [NOT] NULL`, plus arithmetic.

## The planner

ElyraSQL picks an access path automatically:

| Predicate | Access path | Cost |
|-----------|-------------|------|
| `pk = <literal>` (all key columns) | clustered point lookup | `O(log n)` |
| `indexed_col = <literal>` | secondary index | `O(log n + matches)` |
| `col >/>=/</<= <literal>`, `BETWEEN` on PK/indexed col | ordered range scan | proportional to matches |
| `ORDER BY <pk prefix> ASC\|DESC LIMIT n` (no filter) | clustered walk (forward/reverse), stop at `n` | `O(offset + n)` |
| `ORDER BY <indexed col> ASC\|DESC LIMIT n` | ordered index walk (+ NULL block), stop at `n` | `O(offset + n)` |
| anything else | full table scan (streaming) | `O(n)` |

Non-accelerated scans **stream** in bounded memory, so they never load the
whole table at once.

An ordered `LIMIT` (a paged grid: `ORDER BY <col> ASC|DESC LIMIT n OFFSET k`) is
served by an ordered index or clustered walk that stops after `k + n` rows —
constant work per page, independent of table size. This applies to the primary
key in **both** directions and to a secondary index — including a **nullable
single-column** index, where NULL-keyed rows are spliced in (last for `DESC`,
first for `ASC`, matching MySQL). Because a non-unique secondary index stores
`(value, clustered primary key)`, a **tiebreaker on the primary key** stays on the
fast path too: `ORDER BY <indexed col> DESC, id DESC` (the usual stable-pagination
sort) walks the index directly. All order terms must share a direction, and any
trailing terms must be the primary-key columns in order. A `WHERE` filter is applied as a residual during
the walk, so a filtered grid page stays on the fast path too; a very selective
filter (or very rare NULLs on an `ASC` walk) falls back to the sorter, bounded by
`ELYRASQL_ORDER_SCAN_BUDGET`. See [limitations](../limitations.md) for details.

## Joins

`INNER`, `LEFT`, `RIGHT`, `FULL`, and `CROSS` joins are supported, including
comma-style implicit joins and multi-table chains:

```sql
SELECT u.name, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount > 100
ORDER BY o.amount;

-- three tables
SELECT u.name, o.id, i.sku
FROM users u
JOIN orders o ON u.id = o.user_id
JOIN items  i ON o.id = i.order_id;

-- left join keeps unmatched left rows (right side NULL)
SELECT u.name, o.amount
FROM users u LEFT JOIN orders o ON u.id = o.user_id;
```

### Join execution

- **Equi-joins** (`a.x = b.y`) on `INNER`/`LEFT`/`RIGHT` use a **hash join** —
  `O(n+m)`.
- When the driving side is small and the partner is indexed on the join key,
  the planner uses an **index nested-loop join**, making selective joins
  sub-millisecond.
- **Memory-bounded streaming:** a large `INNER`/`LEFT`/`RIGHT` equi-join followed
  by `ORDER BY` or `GROUP BY` streams the driving side through the spilling
  sorter/aggregator (partner sides built into hash tables), so it is bounded by
  the result/hash state, not the full join output — including N-table left-deep
  chains and comma joins. A two-table `RIGHT JOIN` is rewritten to the equivalent
  `LEFT JOIN` (columns reordered back). `FULL` and non-equi joins use nested-loop
  and materialise before sorting/grouping.
- Single-table `WHERE` conjuncts are **pushed down** to each relation before
  the join to reduce work.

Qualify ambiguous columns (`u.id`, `o.id`); a bare column that exists in
multiple joined tables raises an "ambiguous column" error.

## Subqueries

Uncorrelated subqueries are supported in `WHERE`:

```sql
-- IN / NOT IN
SELECT name FROM users WHERE id IN (SELECT uid FROM orders);
SELECT name FROM users WHERE id NOT IN (SELECT uid FROM orders);

-- scalar subquery
SELECT name FROM users WHERE age = (SELECT MAX(age) FROM users);
SELECT name FROM users WHERE age > (SELECT AVG(age) FROM users);

-- EXISTS / NOT EXISTS
SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders);
```

Uncorrelated subqueries are executed once, before the outer query is planned.
A scalar subquery yields the first column of the first row (or `NULL` if empty).

### Correlated subqueries

Subqueries that reference the outer row are supported and evaluated per outer
row:

```sql
SELECT name FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.uid = u.id);

SELECT name FROM users u
WHERE (SELECT COUNT(*) FROM orders o WHERE o.uid = u.id) >= 2;
```

!!! note
    Correlated references must be **qualified** with the outer table's
    name/alias (`u.id`) so they are not confused with an inner column. This
    path materialises the outer rows and runs the subquery per row.

### Derived tables

```sql
SELECT x.region, x.total
FROM (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) x
WHERE x.total > 1000;
```

A derived table must have an alias. It works standalone and in joins.

### Scalar subqueries in the SELECT list

```sql
SELECT name,
       (SELECT COUNT(*) FROM orders o WHERE o.uid = u.id) AS order_count
FROM users u;
```

Both uncorrelated and correlated scalar subqueries are supported in the
projection.

### Common table expressions (WITH)

```sql
WITH regional AS (
    SELECT region, SUM(amount) AS total FROM sales GROUP BY region
)
SELECT region, total FROM regional WHERE total > 1000 ORDER BY total DESC;
```

CTEs are inlined as derived tables. Multiple, chained CTEs (a later CTE
referencing an earlier one) work.

### Recursive CTEs (WITH RECURSIVE)

```sql
WITH RECURSIVE seq(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 10
)
SELECT n FROM seq;

-- graph reachability (UNION deduplicates, so cycles terminate)
WITH RECURSIVE reach(node) AS (
    SELECT 1
    UNION
    SELECT e.dst FROM edges e JOIN reach r ON e.src = r.node
)
SELECT node FROM reach ORDER BY node;
```

The recursive body must be `anchor UNION [ALL] recursive`, with exactly one
self-referencing branch. `UNION` deduplicates (so cyclic graphs terminate);
`UNION ALL` does not and is capped at 1000 iterations. `FROM`-less anchors
(`SELECT 1`) are supported.

### Set operations (UNION / INTERSECT / EXCEPT)

```sql
SELECT v FROM a UNION     SELECT v FROM b;   -- distinct
SELECT v FROM a UNION ALL SELECT v FROM b;   -- keep duplicates
SELECT v FROM a INTERSECT SELECT v FROM b;
SELECT v FROM a EXCEPT    SELECT v FROM b;

SELECT v FROM a UNION SELECT v FROM b ORDER BY v DESC LIMIT 10;
```

`UNION`, `INTERSECT`, and `EXCEPT` are supported (with `ALL`). A trailing
`ORDER BY`/`LIMIT`/`OFFSET` applies to the combined result. Both sides must
produce the same number of columns.

### Correlated subqueries over joins

Correlated subqueries (in `WHERE` and the SELECT list) work over joins, too:

```sql
SELECT u.name, d.dname,
       (SELECT COUNT(*) FROM orders o WHERE o.uid = u.id) AS orders
FROM users u
JOIN departments d ON u.dept = d.id
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.uid = u.id);
```

The subquery is evaluated per joined row with the outer columns bound.
Correlated subqueries combined with **aggregation** over a join are not
supported.
