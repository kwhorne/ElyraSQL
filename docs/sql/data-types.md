# Data Types

| Type | Aliases accepted | Stored as | Notes |
|------|------------------|-----------|-------|
| `BIGINT` | `INT`, `INTEGER`, `SMALLINT`, `TINYINT` | 64-bit signed | |
| `DOUBLE` | `FLOAT`, `REAL` | 64-bit float | |
| `BOOL` | `BOOLEAN` | boolean | rendered as `0`/`1` |
| `TEXT` | `VARCHAR`, `CHAR`, `STRING` | UTF-8 string | |
| `BLOB` | `BYTEA` | raw bytes | |
| `DATE` | | days since 1970-01-01 | `'YYYY-MM-DD'` |
| `DATETIME` | `TIMESTAMP` | microseconds since epoch | `'YYYY-MM-DD HH:MM:SS[.ffffff]'` |
| `TIME` | | microseconds since midnight | `'HH:MM:SS[.ffffff]'` |
| `DECIMAL(p,s)` | `NUMERIC(p,s)` | exact fixed-point | scale preserved |
| `JSON` | `JSONB` | validated text | structural validation on insert |
| `VECTOR(n)` | | `n` × float32 | ANN search, see [Vector Search](vector-search.md) |

## Literals and coercion

Values are written as string or numeric literals and coerced to the column
type on insert:

```sql
CREATE TABLE t (
  id     BIGINT PRIMARY KEY,
  price  DECIMAL(10,2),
  d      DATE,
  ts     DATETIME,
  clock  TIME,
  doc    JSON
);

INSERT INTO t VALUES
  (1, 19.99, '2024-01-15', '2024-01-15 09:30:00', '09:30:00', '{"a":1}');
```

- **DECIMAL** keeps its declared scale exactly: `19.9` stored in
  `DECIMAL(10,2)` reads back as `19.90`, and `SUM` over decimals is exact.
- **DATE/DATETIME/TIME** accept string literals and compare correctly against
  strings (`WHERE d >= '2024-01-01'`).
- **JSON** must be structurally valid; invalid JSON is rejected.
- **VECTOR** accepts a `'[a,b,c]'` string literal of the declared dimension.

## JSON access

Extract values from `JSON` columns with the `->` / `->>` operators or
`JSON_EXTRACT`, using MySQL-style paths (`$`, `$.key`, `$[0]`, chained):

```sql
SELECT doc->'$.name'        AS name_json,   -- returns JSON (quoted)
       doc->>'$.name'       AS name_text,   -- returns unquoted text
       doc->>'$.addr.city'  AS city,
       doc->>'$.tags[0]'    AS first_tag,
       JSON_EXTRACT(doc, '$.age') AS age
FROM docs;
```

`JSON_UNQUOTE` returns the raw scalar of a JSON value. A missing path yields
`NULL`.

### JSON functions

| Function | Description |
|----------|-------------|
| `JSON_ARRAY(v, ...)` | Build a JSON array |
| `JSON_OBJECT(k, v, ...)` | Build a JSON object from key/value pairs |
| `JSON_SET(doc, path, val, ...)` | Insert or update at each path |
| `JSON_INSERT(doc, path, val, ...)` | Set only paths that do not exist |
| `JSON_REPLACE(doc, path, val, ...)` | Set only paths that already exist |
| `JSON_REMOVE(doc, path, ...)` | Remove values at paths |
| `JSON_CONTAINS(doc, candidate[, path])` | Containment test (`1`/`0`) |
| `JSON_LENGTH(doc[, path])` | Element count (arrays/objects) |
| `JSON_KEYS(doc[, path])` | Object keys as a JSON array |
| `JSON_TYPE(val)` | `OBJECT`/`ARRAY`/`STRING`/`INTEGER`/`DOUBLE`/`BOOLEAN`/`NULL` |
| `JSON_VALID(str)` | Whether a string parses as JSON |
| `JSON_QUOTE(str)` | Wrap a string as a JSON string literal |

```sql
SELECT JSON_SET('{"a":1}', '$.a', 10, '$.b', 2);   -- {"a": 10, "b": 2}
UPDATE docs SET doc = JSON_SET(doc, '$.seen', 1) WHERE id = 5;
SELECT id FROM docs WHERE JSON_LENGTH(doc, '$.tags') >= 2;
```

Nested paths (`$.a.b`, `$.a[0]`) are supported for setting, removing, and
inspecting.

!!! warning "Parenthesize in `WHERE`/`ORDER BY`"
    The parser binds `=` tighter than `->>`, so wrap the extraction in
    parentheses when comparing:

    ```sql
    SELECT id FROM docs WHERE (doc->>'$.addr.city') = 'Bergen';
    ```

## Comparison semantics

Cross-type comparisons are coerced (date vs. text, decimal vs. numeric). `NULL`
compares as unknown and sorts first under `ORDER BY`.
