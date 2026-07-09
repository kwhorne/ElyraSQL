# Tables & DDL

## CREATE TABLE

```sql
CREATE TABLE users (
  id     BIGINT PRIMARY KEY,
  name   TEXT NOT NULL,
  email  TEXT,
  age    BIGINT
);
```

- `PRIMARY KEY` may be a column option or a table constraint, and may be
  **composite**:

  ```sql
  CREATE TABLE enroll (
    student BIGINT,
    course  TEXT,
    grade   BIGINT,
    PRIMARY KEY (student, course)
  );
  ```

- Rows are **clustered** on the primary key: stored physically in key order,
  so PK lookups and range scans are `O(log n)`.
- A table without a primary key gets a hidden auto-incrementing row id.
- `CREATE TABLE IF NOT EXISTS` is supported.
- `NOT NULL` is enforced on insert and update.

## CREATE INDEX

Secondary B-tree indexes, single or composite:

```sql
CREATE INDEX users_email ON users (email);
CREATE INDEX ev_year_month ON events (year, month);
```

Indexes accelerate equality (`col = x`, and full-key equality for composite
indexes) and single-column range predicates (`>`, `>=`, `<`, `<=`, `BETWEEN`).
They are maintained automatically on `INSERT`, `UPDATE`, and `DELETE`.

Creating an index on a `VECTOR` column builds an HNSW index — see
[Vector Search](vector-search.md).

## ALTER TABLE

```sql
ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active';
ALTER TABLE users DROP COLUMN age;
ALTER TABLE users RENAME COLUMN email TO email_address;
ALTER TABLE users RENAME TO members;
```

- **ADD COLUMN** backfills existing rows with the default (or `NULL`). Adding a
  `NOT NULL` column requires a `DEFAULT`.
- **DROP COLUMN** rewrites rows and remaps key/index positions. A primary-key
  or indexed column cannot be dropped (drop the index first).
- **RENAME COLUMN** is a metadata-only change.
- **RENAME TABLE** re-keys the data and rebuilds index entries.

## Column defaults, AUTO_INCREMENT, and generated columns

```sql
CREATE TABLE items (
    id     BIGINT PRIMARY KEY AUTO_INCREMENT,
    name   TEXT,
    status TEXT   DEFAULT 'active',
    qty    BIGINT DEFAULT 1,
    price  BIGINT,
    total  BIGINT GENERATED ALWAYS AS (price * qty) STORED
);
```

- **`DEFAULT <expr>`** — used when the column is omitted from an `INSERT`.
- **`AUTO_INCREMENT`** — when the column is omitted, `NULL`, or `0`, the next
  value from a per-table counter is assigned. An explicit larger value advances
  the counter. The counter persists in the database file.
- **`GENERATED ALWAYS AS (<expr>) STORED`** — computed from other columns on
  every `INSERT` and `UPDATE`; any supplied value is ignored. `VIRTUAL`
  generated columns are treated as stored.

## CREATE TABLE ... AS SELECT

```sql
CREATE TABLE big_sales AS SELECT id, amount FROM sales WHERE amount >= 100;
CREATE TABLE totals AS SELECT region, SUM(amount) AS total FROM sales GROUP BY region;
```

The new table's columns are derived from the query's output (or an explicit
column list) and the result rows are copied in. The table has no primary key or
indexes.

## CREATE TABLE ... LIKE

```sql
CREATE TABLE sales_archive LIKE sales;
```

Copies the structure (columns, primary key, indexes) of an existing table
without copying any rows.

## TRUNCATE TABLE

```sql
TRUNCATE TABLE logs;
```

Removes all rows and index entries and resets the auto-increment counter,
keeping the table definition.

## Views

```sql
CREATE VIEW big_sales AS
    SELECT id, region, amount FROM sales WHERE amount >= 100;

CREATE VIEW region_totals(region, total) AS
    SELECT region, SUM(amount) FROM sales GROUP BY region;

CREATE OR REPLACE VIEW big_sales AS SELECT id FROM sales WHERE amount >= 200;

DROP VIEW big_sales;
```

Views are stored SELECT statements, expanded as derived tables when queried.
They support an optional column list, `OR REPLACE`, joins, aggregation over the
view, and views that reference other views. Views are read-only (no inserts or
updates through a view). Materialized views and `WITH CHECK OPTION` are not
supported.

## DROP TABLE

```sql
DROP TABLE members;
DROP TABLE IF EXISTS members;
```
