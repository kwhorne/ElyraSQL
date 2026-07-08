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

## DROP TABLE

```sql
DROP TABLE members;
DROP TABLE IF EXISTS members;
```
