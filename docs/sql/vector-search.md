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

- The index is **cached in memory** and **incrementally reconciled** when the
  table changes: on the first query after a write, only the rows that were
  inserted, updated or deleted since the last reconcile are applied to the
  existing graph (new vectors inserted, removed/superseded ones soft-tombstoned),
  rather than rebuilding all N vectors. So a single `INSERT` into a 500k-row
  vector table adds one node instead of rebuilding 500k. The change set is
  detected content-wise (per-row vector hash), so `INSERT`/`UPDATE`/`DELETE` are
  all handled correctly. Reconciles are **single-flight**: a burst of concurrent
  queries after a write triggers exactly one reconcile, shared by all.
- A **full rebuild** is used only for the first build, for a change as large as
  the table itself, or to **compact** when too many nodes have been tombstoned
  (so the graph stays healthy). The scan cost of a reconcile is the same as the
  old rebuild scan; what is saved is the O(N) graph reconstruction.

- The built graph is **persisted** to a sibling cache directory `<data>.vidx/`
  (like `<data>.raftstate`), so a restart **loads** the graph and reconciles any
  changes since, instead of rebuilding from scratch (no cold start). The cache is
  regenerable: it lives outside the authoritative single file (so it is not
  replicated or in backups), and a missing / corrupt / wrong-version snapshot
  falls back to a rebuild. The snapshot is written on the first build and on
  compaction (not on every small write).

!!! note "Very-high-write workloads"
    Reconcile still reads all current rows to diff them, so an extremely high
    sustained write rate interleaved with queries pays that scan repeatedly; a
    write-log delta (to make reconcile O(delta) with no scan) is a further
    optimization. For steady bulk ingestion, batch writes.
- Without the pattern (e.g. with a `WHERE` filter, or cosine/inner-product),
  the query falls back to **exact** search, which is always correct.

!!! tip
    Build the index once your vectors are loaded. The first query builds and
    caches the graph; after later writes, the first query pays a one-time
    reconciliation scan and subsequent queries reuse the reconciled graph.

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
  `ai_embed(column)` is not supported. To embed a whole column, declare an
  [embedding index](#keeping-embeddings-in-step-create-embedding-index) and let
  the database maintain it.

## Keeping embeddings in step: `CREATE EMBEDDING INDEX`

`ai_embed()` embeds one value at a time, which leaves the application holding the
hard part: noticing that a row's text changed, re-embedding it, retrying when the
provider is down, and not doing any of it twice. An **embedding index** moves
that into the database.

```sql
CREATE EMBEDDING INDEX body_ix ON articles(body) INTO embedding
    USING MODEL 'text-embedding-3-small';
```

After that, writing `body` is the whole job:

```sql
INSERT INTO articles (body) VALUES ('data protection and personal privacy law');
-- no ai_embed(), no second statement, no application code
```

The `embedding` column fills in on its own, and `HYBRID()` and `VEC_DISTANCE()`
work on it exactly as on a hand-maintained vector column — because it is one.

### Syntax

```sql
CREATE EMBEDDING INDEX [IF NOT EXISTS] <name> ON <table>(<text_column>)
    INTO <vector_column> [USING MODEL '<model>'] [DIMENSION <n>];

DROP EMBEDDING INDEX [IF EXISTS] <name> ON <table>;

SHOW EMBEDDING INDEXES [ON <table>];
```

- The **source** is a `TEXT` or `JSON` column. Rows where it is `NULL` or blank
  are skipped — they never become a provider request.
- The **target** is an ordinary `VECTOR(n)` column, named explicitly. It has to
  be nameable: `HYBRID(body, 'q', embedding, …)` and `VEC_DISTANCE(embedding, …)`
  refer to it, which is where the value is. Creating a hidden column instead
  would also change what `SELECT *` returns and what `DESCRIBE` reports.
- `USING MODEL` is optional. Omitted, the index follows
  `ELYRASQL_AI_EMBED_MODEL`, so a deployment can change models without rewriting
  its DDL. Given, it pins the model for that index regardless of the server
  default.
- `DIMENSION` is a **cross-check**, not a second source of truth: the real
  dimension comes from the column's type, and a `DIMENSION` that disagrees with
  it is rejected rather than silently producing vectors the column cannot hold.

`HYBRID()` and the HNSW fast path still need a vector index on the target column:

```sql
CREATE INDEX articles_embedding ON articles (embedding);
```

### How it stays in step

The server sweeps on a timer (`--embedding-sweep-secs`, 30 by default; `0`
disables it). Each sweep works out what needs embedding **from the data itself**,
by comparing a hash of the `(model, text)` pair against the hash recorded when
the row's vector was written.

Deriving it rather than hooking `INSERT`/`UPDATE` means every path that writes
rows is covered — ordinary DML, bulk loads, `RESTORE`, binlog replay and
replication apply included. A write hook would see only the first of those.

Nothing happens while no provider is configured, and that is deliberately not
treated as failure: without `ELYRASQL_AI_EMBED_URL` every row would "fail"
without a single request having been made.

Writing back is transactional. An embedding call takes real time, and a row can
be updated inside that window; the sweep only stores a vector if the row is still
exactly as it read it, so a concurrent `UPDATE` is never overwritten — the row
simply stays pending and is picked up next sweep.

### Rows that are not embedded yet

A newly inserted row is searchable immediately, without its vector.

`HYBRID()` fuses a full-text ranking and a vector ranking over the **union** of
both, so a row missing from the vector side still ranks on its text. It gains the
semantic half when the sweep reaches it. Search degrades rather than hiding the
row — which is what you want, because a freshly inserted row silently absent from
results is indistinguishable from data loss.

Plain `VEC_DISTANCE()` has no text half, so a not-yet-embedded row is not
returned by it.

### When the provider fails

Failures are retried with exponential backoff (1s, 2s, 4s, 8s, 16s). After five
consecutive failures a row is **dead-lettered**: it stops being retried, so a
permanently unembeddable row — input the provider rejects, a quota that is not
coming back — stops costing requests and money.

It is not forgotten. `SHOW EMBEDDING INDEXES` reports both states:

```
+---------+----------+--------+-----------+------------+-----------+----------+--------+
| Name    | Table    | Source | Target    | Model      | Dimension | Retrying | Failed |
+---------+----------+--------+-----------+------------+-----------+----------+--------+
| body_ix | articles | body   | embedding | all-minilm |       384 |        0 |      2 |
+---------+----------+--------+-----------+------------+-----------+----------+--------+
```

`Retrying` is rows still inside their backoff; `Failed` is rows past the cap.
Editing a dead-lettered row's text changes its hash, which clears the state and
puts it back in the queue — the natural way to un-stick one after fixing the
input or the provider.

### Cost

Each sweep scans the indexed table to decide what needs work, and embeds at most
256 rows per index per pass, so a large backfill is spread over several sweeps
rather than arriving as one bill. The scan is the cost of not hooking the write
path; on a very large, rarely-changing table, raise `--embedding-sweep-secs`.
