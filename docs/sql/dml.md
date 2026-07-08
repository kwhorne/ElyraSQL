# Insert, Update, Delete

## INSERT

```sql
INSERT INTO users (id, name, email) VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob',   'bob@example.com');
```

- The column list is optional; omit it to supply values for all columns in
  definition order.
- Multi-row inserts commit as a single atomic batch (group commit).
- Values are coerced to the target column type; type mismatches and `NOT NULL`
  violations are rejected.

## UPDATE

```sql
UPDATE users SET name = 'Alice B.' WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 42;
```

- The assignment right-hand side may reference existing column values.
- Changing a primary-key value relocates the row's clustered key and updates
  index entries.
- Without a `WHERE` clause, all rows are updated.

## DELETE

```sql
DELETE FROM users WHERE id = 2;
DELETE FROM logs WHERE created < '2024-01-01';
DELETE FROM users;            -- all rows
```

`DELETE` supports `WHERE` and a MySQL-style `LIMIT`.

## Performance notes

`UPDATE` and `DELETE` use the same planner fast paths as `SELECT`: an equality
on the primary key is a point lookup, an equality or range on an indexed column
uses the index, and everything else is a full scan. See
[Queries & Joins](queries.md).
