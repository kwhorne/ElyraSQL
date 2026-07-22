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

!!! warning "Rebuild cost on write-heavy vector workloads"
    The cache is invalidated by **any** write to the table (tracked by a per-table
    write counter), and the next vector query then rebuilds the **whole** HNSW
    graph from a full table scan — it is not yet maintained incrementally. On a
    large, frequently-mutated vector table this is expensive (a single insert
    makes the next ANN query rebuild the entire graph). The graph is also **not
    persisted**, so it is rebuilt on the first query after a restart (cold start).
    ElyraSQL is best suited to **read-heavy / batch-updated** embedding workloads
    today; incremental maintenance and on-disk persistence are planned
    (ESQL-26 / ESQL-27). For steady high-write vector ingestion, batch writes and
    keep queries off the table during ingestion.
- Without the pattern (e.g. with a `WHERE` filter, or cosine/inner-product),
  the query falls back to **exact** search, which is always correct.

!!! tip
    Build the index once your vectors are loaded. The first query after a
    change pays a one-time rebuild cost; subsequent queries are cached.

## Hybrid search (full-text + vector, fused)

ElyraSQL fuses **full-text relevance** and **vector similarity** into a single
ranking with the `HYBRID(...)` primitive, honouring your structured `WHERE`
filter — no external search engine, one query, one file:

```sql
SELECT id, title,
       HYBRID(body, 'data privacy law', embedding, '[0.12, 0.03, ...]') AS score
FROM docs
WHERE lang = 'en'                 -- structured filter
ORDER BY score DESC
LIMIT 10;
```

`HYBRID(text_col, 'text query', vector_col, vector)`:

1. Ranks documents by **vector** nearest-neighbour (the HNSW index on
   `vector_col`).
2. Ranks documents by **full-text** term frequency over the stemmed query terms
   (using a `FULLTEXT` index on `text_col` when present, otherwise a scan).
3. Fuses the two rankings with **Reciprocal Rank Fusion** (RRF, `k = 60`), so a
   document ranked highly by *both* signals rises to the top.
4. Applies the query's `WHERE` filter and returns the top `LIMIT` rows, with the
   fused relevance exposed as the aliased column (`score` above).

Requirements and notes:

- The vector column needs a vector index (`CREATE INDEX ... ON t (embedding)`);
  a `FULLTEXT` index on the text column makes the text side index-accelerated.
- Weights are currently equal; the fan-out (candidates considered per side)
  scales with `LIMIT`. Reference the primitive by alias in `ORDER BY` /
  projection as shown.

## Faceted search: `FACET()`

The counts side of a faceted search is a normal aggregate, so it reuses the same
engine and runs in a single pass alongside the hit count. `FACET(col[, top_n])`
returns a `{value: count}` JSON object over the matched rows and composes with
`WHERE`, `MATCH ... AGAINST`, vector filters and `GROUP BY`:

```sql
SELECT FACET(category) AS categories, FACET(brand, 10) AS brands, COUNT(*) AS total
FROM docs
WHERE MATCH(title, body) AGAINST('rust database');
```

See [Aggregation → FACET](aggregation.md#facet-faceted-search-counts) for details.

## Generating embeddings in SQL: `ai_embed()`

`ai_embed('text')` calls an **OpenAI-compatible embeddings endpoint** and
returns the vector, so query vectors and stored values can be produced directly
in SQL — no separate embedding step in your app:

```sql
-- generate the query vector inline
SELECT id, title
FROM docs
ORDER BY VEC_DISTANCE(embedding, ai_embed('data privacy law'))
LIMIT 10;

-- ... and combine with hybrid search
SELECT id, HYBRID(body, 'privacy', embedding, ai_embed('privacy')) AS score
FROM docs ORDER BY score DESC LIMIT 10;

-- populate embeddings on insert
INSERT INTO docs VALUES (1, 'some text', ai_embed('some text'));
```

Configure the provider with environment variables:

| Variable | Description |
|----------|-------------|
| `ELYRASQL_AI_EMBED_URL` | Embeddings endpoint (e.g. `https://api.openai.com/v1/embeddings`, or a local `http://localhost:11434/v1/embeddings` for Ollama/LM Studio/llama.cpp/vLLM). |
| `ELYRASQL_AI_EMBED_KEY` | Bearer API key (optional for local servers). |
| `ELYRASQL_AI_EMBED_MODEL` | Model name (default `text-embedding-3-small`). |

- Each unique text is embedded **once** (resolved in an async pre-pass and
  cached per model+text), then treated as a normal vector literal.
- Only **constant** arguments are supported (`ai_embed('query')`); per-row
  `ai_embed(column)` is not yet supported for large scans.
