# Functions

ElyraSQL supports a broad set of MySQL-compatible scalar functions in
expressions (SELECT list, WHERE, DEFAULT, generated columns, etc.).

## Date & time

| Function | Result |
|----------|--------|
| `NOW()`, `CURRENT_TIMESTAMP`, `SYSDATE()`, `LOCALTIME` | current DATETIME |
| `CURDATE()`, `CURRENT_DATE` | current DATE |
| `CURTIME()`, `CURRENT_TIME` | current TIME |
| `UNIX_TIMESTAMP([dt])` | seconds since the epoch |

The niladic forms work with or without parentheses.

## String

`CONCAT`, `CONCAT_WS`, `UPPER`/`UCASE`, `LOWER`/`LCASE`, `LENGTH`/`CHAR_LENGTH`,
`OCTET_LENGTH`, `SUBSTRING`/`SUBSTR`/`MID`, `LEFT`, `RIGHT`, `TRIM`/`LTRIM`/`RTRIM`
(incl. `TRIM(LEADING/TRAILING 'x' FROM s)`), `REPLACE`, `REVERSE`, `REPEAT`,
`SPACE`, `LPAD`, `RPAD`, `INSTR`, `LOCATE`/`POSITION`, `ASCII`.

## Math

`ABS`, `CEIL`/`CEILING`, `FLOOR`, `ROUND(x[,d])`, `TRUNCATE(x,d)`, `SIGN`,
`SQRT`, `EXP`, `LN`/`LOG`, `LOG10`, `LOG2`, `POWER`/`POW`, `MOD`, `PI()`,
`RAND()`, `GREATEST`, `LEAST`.

## Conditional & null

`COALESCE`, `IFNULL`/`NVL`, `NULLIF`, `IF(cond, a, b)`, and `CASE` expressions
(both simple and searched).

## Other

- `UUID()` — a random version-4 UUID string.
- `CAST(x AS <type>)` / `CONVERT` — to `CHAR`/text, `SIGNED`/integer,
  `DECIMAL`/floating point, `DATE`, `DATETIME`, `TIME`.

## JSON

See [Data types](data-types.md#json) for `JSON_EXTRACT`, `JSON_SET`,
`JSON_ARRAY`, `JSON_OBJECT`, and the rest of the JSON family.

## Vector

See [Vector search](vector-search.md) for `VEC_DISTANCE`,
`VEC_COSINE_DISTANCE`, and `VEC_INNER_PRODUCT`.
