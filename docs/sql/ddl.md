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
ALTER TABLE users MODIFY COLUMN qty BIGINT;
ALTER TABLE users CHANGE COLUMN note remark TEXT;
ALTER TABLE users ALTER COLUMN status SET DEFAULT 'new';
ALTER TABLE users ALTER COLUMN status DROP DEFAULT;
ALTER TABLE users ALTER COLUMN status SET NOT NULL;
```

- **ADD COLUMN** backfills existing rows with the default (or `NULL`). Adding a
  `NOT NULL` column requires a `DEFAULT`.
- **DROP COLUMN** rewrites rows and remaps key/index positions. A primary-key
  or indexed column cannot be dropped (drop the index first).
- **RENAME COLUMN** is a metadata-only change.
- **MODIFY / CHANGE COLUMN** retypes a column (existing values are converted),
  renames it (`CHANGE`), and resets its options (nullability, default). The
  type of a primary-key column cannot be changed.
- **ALTER COLUMN** sets or drops a `DEFAULT`, or toggles `NOT NULL`.
- Type conversions follow MySQL-style leniency (e.g. `'10'` → `10`, `99` →
  `'99'`).
- **RENAME TABLE** re-keys the data and rebuilds index entries.

## Constraints

```sql
CREATE TABLE users (
    id    BIGINT PRIMARY KEY,
    email TEXT UNIQUE,
    age   BIGINT CHECK (age >= 0)
);

CREATE TABLE orders (
    id   BIGINT PRIMARY KEY,
    uid  BIGINT,
    FOREIGN KEY (uid) REFERENCES users(id) ON DELETE CASCADE
);
```

- **PRIMARY KEY**, **UNIQUE** (column, table, or `CREATE UNIQUE INDEX`), and
  **NOT NULL** are enforced (duplicate key → error 1062). Multiple `NULL`s are
  allowed in a unique index.
- **CHECK(expr)** (column- or table-level) is enforced on INSERT/UPDATE; it
  passes when the expression is TRUE or NULL and fails only when FALSE.
- **FOREIGN KEY** requires a matching parent row in a primary key or unique
  index (error 1452). `ON DELETE` supports `RESTRICT`/`NO ACTION` (default,
  blocks), `CASCADE` (deletes children), and `SET NULL`. Referencing columns
  are automatically indexed.

## Collation (case sensitivity)

Text is **case-insensitive by default** (`'Foo' = 'foo'`). Declare a column with
a binary collation to make it case-sensitive:

```sql
CREATE TABLE tokens (
    id    BIGINT PRIMARY KEY,
    token TEXT COLLATE utf8mb4_bin UNIQUE   -- case-sensitive unique
);
```

A `COLLATE ..._bin` (or `BINARY`) column compares case-sensitively in `WHERE`
(equality and ranges, with or without an index) and enforces case-sensitive
`UNIQUE` / `PRIMARY KEY`. Note: `ORDER BY`, `GROUP BY`, `DISTINCT` and join keys
currently still use the default case-insensitive collation even for `_bin`
columns — see [Limitations](../limitations.md).

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

## Introspection

```sql
SHOW TABLES;
SHOW COLUMNS FROM users;
DESCRIBE users;   -- or DESC users
SHOW CREATE TABLE users;
SHOW INDEX FROM users;   -- or SHOW KEYS FROM users
```

`SHOW CREATE TABLE` reconstructs the `CREATE TABLE` DDL (columns, defaults,
auto-increment, generated columns, primary key, and indexes). `SHOW INDEX` /
`SHOW KEYS` lists one row per index column
(`Table | Non_unique | Key_name | Seq_in_index | Column_name | ... | Index_type`).

`SHOW COLUMNS` / `DESCRIBE` return the familiar
`Field | Type | Null | Key | Default | Extra` layout, where `Key` is `PRI`
(primary key), `UNI` (unique index), or `MUL` (leading column of a secondary
index), and `Extra` shows `auto_increment` or `STORED GENERATED`.

### INFORMATION_SCHEMA

`information_schema.tables` and `information_schema.columns` are queryable
virtual tables, so you can filter, group, and aggregate over the catalog:

```sql
SELECT table_name FROM information_schema.tables;

SELECT column_name, data_type, is_nullable, column_key, extra
FROM information_schema.columns
WHERE table_name = 'users'
ORDER BY ordinal_position;

SELECT table_name, COUNT(*) AS columns
FROM information_schema.columns GROUP BY table_name;
```

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
