# Vector Search

ElyraSQL treats vectors as a first-class column type for similarity search —
useful for embeddings, semantic search, and RAG.

## The VECTOR type

```sql
CREATE TABLE docs (
  id        BIGINT PRIMARY KEY,
  title     TEXT,
  embedding VECTOR(768)
);

INSERT INTO docs VALUES (1, 'cat', '[0.1, 0.2, ...]');
```

Vectors are written as a `'[a, b, c]'` string literal matching the declared
dimension.

## Distance functions

| Function | Metric |
|----------|--------|
| `VEC_DISTANCE(a, b)` / `VEC_L2_DISTANCE` | squared Euclidean (L2) |
| `VEC_COSINE_DISTANCE(a, b)` | cosine distance (`1 - cosine similarity`) |
| `VEC_INNER_PRODUCT(a, b)` | negative inner product |

Either argument may be a `VECTOR` column or a `'[...]'` literal.

## k-nearest-neighbour queries

```sql
SELECT id, title, VEC_DISTANCE(embedding, '[...]') AS dist
FROM docs
ORDER BY dist
LIMIT 10;
```

This returns the 10 nearest rows. It works combined with `WHERE` filters and
projections.

## HNSW acceleration

Creating an index on a `VECTOR` column builds an in-memory **HNSW** index:

```sql
CREATE INDEX docs_emb ON docs (embedding);
```

When a query matches the pattern `ORDER BY VEC_DISTANCE(col, q) LIMIT k` with no
`WHERE` (L2 metric), the planner uses the HNSW index for approximate
nearest-neighbour search — typically **sub-millisecond**, versus a full scan for
exact search.

- The index is **cached in memory** and **rebuilt when the table changes**
  (rebuild-when-stale), which suits read-heavy embedding workloads. Rebuilds are
  **single-flight**: if many queries arrive at once after a write, only one
  rebuilds the index while the others wait for and share its result, so a burst
  of concurrent queries can't trigger a stampede of parallel full-table scans.
- Without the pattern (e.g. with a `WHERE` filter, or cosine/inner-product),
  the query falls back to **exact** search, which is always correct.

!!! tip
    Build the index once your vectors are loaded. The first query after a
    change pays a one-time rebuild cost; subsequent queries are cached.
