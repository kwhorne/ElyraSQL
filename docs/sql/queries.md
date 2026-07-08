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
| anything else | full table scan (streaming) | `O(n)` |

Non-accelerated scans **stream** in bounded memory, so they never load the
whole table at once.

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

- **Equi-joins** (`a.x = b.y`) on `INNER`/`LEFT` use a **hash join** — `O(n+m)`.
- When the driving side is small and the partner is indexed on the join key,
  the planner uses an **index nested-loop join**, making selective joins
  sub-millisecond.
- `RIGHT`/`FULL` and non-equi joins use nested-loop.
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

Subqueries are executed once, before the outer query is planned, and their
results are substituted in. A scalar subquery yields the first column of the
first row (or `NULL` if empty).

!!! note "Not yet supported"
    **Correlated** subqueries (that reference the outer row) and **derived
    tables** (`FROM (SELECT ...) AS t`) are not supported yet.
