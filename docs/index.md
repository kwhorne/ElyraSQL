# ElyraSQL

**A robust, MySQL-compatible SQL server written in Rust.**

ElyraSQL stores an entire database in a single ACID file, speaks the MySQL wire
protocol so existing clients and drivers work unchanged, and adds first-class
vector search and parallel analytical (OLAP) aggregation — all under one brand.

<div class="grid cards" markdown>

- :material-database: **Single file, ACID**
  The whole database is one crash-safe file with multi-version concurrency.

- :material-connection: **MySQL compatible**
  Connect with `mysql`, DBeaver, or any MySQL driver in any language.

- :material-vector-line: **Vector native**
  `VECTOR(n)` columns with exact and HNSW approximate nearest-neighbour search.

- :material-lightning-bolt: **Fast**
  Sub-millisecond point lookups, index nested-loop joins, and cached ANN.

</div>

## Highlights

- **Full SQL surface** — DDL, CRUD, `JOIN` (INNER/LEFT/RIGHT/FULL/CROSS),
  `WHERE`, `ORDER BY`, `LIMIT`/`OFFSET`, aggregation with `GROUP BY`.
- **Rich types** — `BIGINT`, `DOUBLE`, `TEXT`, `BLOB`, `BOOL`, `DATE`,
  `DATETIME`, `TIME`, `DECIMAL`, `JSON`, and `VECTOR`.
- **Indexing** — clustered primary keys (single or composite), secondary
  B-tree indexes, range scans, and HNSW vector indexes.
- **Transactions** — `BEGIN` / `COMMIT` / `ROLLBACK` with **snapshot isolation**.
- **Security** — `mysql_native_password` auth, TLS, and read/write/admin roles.
- **Operations** — single static binary, systemd unit, and a ~15 MB Docker image.

## Quick taste

```sql
CREATE TABLE docs (id BIGINT PRIMARY KEY, title TEXT, embedding VECTOR(3));
INSERT INTO docs VALUES (1, 'cat', '[1,0,0]'), (2, 'car', '[0,1,0]');

-- nearest neighbours to a query vector
SELECT id, title, VEC_DISTANCE(embedding, '[0.9,0.1,0]') AS dist
FROM docs ORDER BY dist LIMIT 5;
```

Continue to [Getting Started](getting-started.md).

!!! note "Project status"
    ElyraSQL is young and moving fast. The feature set below is implemented and
    tested, but see [Limitations](limitations.md) for known gaps
    before relying on it in production.
