# MySQL Compatibility

ElyraSQL speaks the MySQL wire protocol, so standard MySQL clients, GUIs, and
drivers connect without modification.

## What works

- **Text protocol** (`COM_QUERY`) — the common path for most clients and CLIs.
- **Prepared statements** (`COM_STMT_PREPARE`/`EXECUTE`) — typed parameters
  (including `DATE`/`DATETIME`/`TIME` from the binary protocol), value escaping,
  statement reuse; used by many ORMs and drivers.
- **Authentication** — `mysql_native_password`.
- **TLS** — clients may negotiate SSL.
- **Handshake** — reports a MySQL-looking version, e.g. `8.0.0-ElyraSQL-1.3.0`,
  and answers the session/introspection queries clients send on connect
  (`SELECT @@version_comment`, `SELECT VERSION()`, `SET ...`,
  `SHOW VARIABLES/STATUS/COLLATION/DATABASES/TABLE STATUS`, and the
  `information_schema` tables GUI tools read to build their schema tree).

## Laravel / Eloquent

ElyraSQL runs Laravel migrations (schema builder), Eloquent models and
relationships, the query builder, transactions, and pagination. Point the
`mysql` connection at ElyraSQL and set the database name to `elyra` (used as the
`information_schema` schema for `Schema::hasTable`/`hasColumn` and `SHOW`):

```php
// config/database.php  -> connections.mysql
'host'     => env('DB_HOST', '127.0.0.1'),
'port'     => env('DB_PORT', '3307'),
'database' => env('DB_DATABASE', 'elyra'),
'options'  => [
    // Recommended: client-side prepared statements. Native prepares work for
    // common shapes, but some (e.g. information_schema `SELECT *`) are not yet
    // reliable with strict drivers; emulation sends fully-formed queries.
    PDO::ATTR_EMULATE_PREPARES => true,
],
```

With that setting a full Eloquent workload -- `Schema::create` (including
`$table->id()`, `foreignId()->constrained()`, indexes), model CRUD with
`lastInsertId`, `hasMany`/`belongsTo`, eager loading, `withCount`, query-builder
joins/aggregates/`groupBy`+`having`, `updateOrInsert`, transactions and
cascading deletes -- runs cleanly.

## Verified clients

- `mysql` / `mariadb` command-line clients
- PyMySQL, mysql-connector-python
- DBeaver, MySQL Workbench (via the standard MySQL driver)
- Any language driver that speaks the MySQL protocol

## Differences and gaps

ElyraSQL implements a focused, growing subset of MySQL SQL. Notable current
gaps:

- Subqueries (`WHERE` + SELECT-list, correlated + uncorrelated), derived
  tables, CTEs (`WITH`), `HAVING`, and window functions are supported;
  `WITH RECURSIVE`, explicit window frames, and correlated subqueries with
  joins are not.
- Views, row-level triggers, and stored procedures are supported;
  user-defined functions and scheduled events are not.
- `ALTER TABLE` supports add/drop/rename/`MODIFY`/`CHANGE` column, rename table,
  and `ADD INDEX`/`KEY`/`UNIQUE` (with backfill); `ADD PRIMARY KEY`/`FOREIGN KEY`
  on an existing table must instead be declared in `CREATE TABLE`.
- A broad scalar function library (string, math, date/time, JSON, `MD5`/`SHA1`/
  `SHA2`, `HEX`/`UNHEX`, `FORMAT`, `FIND_IN_SET`, `FROM_UNIXTIME`, ...),
  statistical and bitwise aggregates (`STDDEV*`, `VAR*`, `BIT_OR`/`AND`/`XOR`),
  `LAST_INSERT_ID()`/`ROW_COUNT()`, `@@`system variables, and `CONVERT()`.
  `INSERT ... SET`, the `<<`/`>>`/`~` bitwise operators, and `LOAD DATA LOCAL`
  are not parsed (parser limitations); `&`, `|`, `^` bitwise operators work.
- Vector search and `VEC_DISTANCE(...)` are ElyraSQL extensions (they mirror
  MySQL 9's vector direction but are not identical).
- `SHOW` and `information_schema` cover what GUI tools and drivers need to
  connect and browse (`tables`, `columns`, `engines`, `schemata`, `views`,
  `routines`, `triggers`, `events`, `statistics`, `partitions`); it is not the
  complete MySQL catalog.

See [Limitations](limitations.md) for the full picture.

!!! note "Prepared-statement caveat"
    Binary (native) prepared statements work for common query shapes; a few
    (e.g. `SELECT *` over `information_schema` or a joined source) report no
    columns at `PREPARE`, which strict drivers may mishandle. For the widest
    compatibility, prefer client-side (emulated) prepared statements —
    `PDO::ATTR_EMULATE_PREPARES => true`, or the driver equivalent. Client-side-
    binding drivers like PyMySQL and sqlx are unaffected.
