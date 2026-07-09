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

### Extracting and formatting

| Function | Result |
|----------|--------|
| `YEAR`, `MONTH`, `DAY`/`DAYOFMONTH`, `HOUR`, `MINUTE`, `SECOND` | component |
| `QUARTER`, `DAYOFWEEK`, `WEEKDAY`, `DAYOFYEAR` | component |
| `EXTRACT(unit FROM d)` | component |
| `DATE(dt)`, `TIME(dt)` | date / time part |
| `DATEDIFF(a, b)` | whole days between |
| `TIMESTAMPDIFF(unit, a, b)` | difference in the given unit |
| `WEEK(d[, mode])`, `YEARWEEK(d[, mode])` | week number (MySQL modes) |
| `LAST_DAY(d)` | last day of the month |
| `DATE_FORMAT(d, fmt)` | formatted string (`%Y %m %d %H %i %s %M %b %W %a %j %p ...`) |
| `STR_TO_DATE(s, fmt)` | parse a string with a format pattern |

### Date arithmetic

```sql
DATE_ADD('2024-01-31', INTERVAL 1 MONTH)   -- 2024-02-29 (day clamped)
DATE_SUB('2024-03-15', INTERVAL 10 DAY)
ADDDATE(d, 7)                              -- numeric day form
TIMESTAMPADD(HOUR, 5, dt)

-- INTERVAL also works as an operator
SELECT * FROM events WHERE ts > NOW() - INTERVAL 7 DAY;
SELECT DATE '2024-01-31' + INTERVAL 1 MONTH;
```

Units: `MICROSECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`,
`QUARTER`, `YEAR`. `INTERVAL` is supported both inside `DATE_ADD`/`DATE_SUB`/
`TIMESTAMPADD` and as a bare `d + INTERVAL n UNIT` / `d - INTERVAL n UNIT`
operator.

## String

`CONCAT`, `CONCAT_WS`, `UPPER`/`UCASE`, `LOWER`/`LCASE`, `LENGTH`/`CHAR_LENGTH`,
`OCTET_LENGTH`, `SUBSTRING`/`SUBSTR`/`MID`, `SUBSTRING_INDEX`, `LEFT`, `RIGHT`,
`TRIM`/`LTRIM`/`RTRIM` (incl. `TRIM(LEADING/TRAILING 'x' FROM s)`), `REPLACE`,
`REVERSE`, `REPEAT`, `SPACE`, `LPAD`, `RPAD`, `INSTR`, `LOCATE`/`POSITION`,
`ASCII`, `FIELD`, `ELT`.

Pattern matching: `str LIKE pattern`, and `str REGEXP pattern` / `str RLIKE
pattern` (POSIX-style regular expressions, with `NOT REGEXP`).

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
