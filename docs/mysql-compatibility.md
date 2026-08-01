# MySQL Compatibility

ElyraSQL speaks the MySQL wire protocol, so standard MySQL clients, GUIs, and
drivers connect without modification.

## What works

- **Text protocol** (`COM_QUERY`) — the common path for most clients and CLIs.
- **Prepared statements** (`COM_STMT_PREPARE`/`EXECUTE`) — typed parameters
  (including `DATE`/`DATETIME`/`TIME` from the binary protocol), value escaping,
  statement reuse; used by many ORMs and drivers.
- **Authentication** — `mysql_native_password` by **default** (widest driver
  compatibility), with `caching_sha2_password` available opt-in (see below).
- **TLS** — clients may negotiate SSL.
- **Handshake** — reports a MySQL-looking version, e.g. `8.0.12-ElyraSQL-1.7.0`,
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

Since 1.7.0 this is exercised against real applications rather than only
synthetic tests: the migration and test suites of four commercial Laravel
codebases run against ElyraSQL, the largest with 469 migration files. The
compatibility fixes that came out of that work are covered by 103 end-to-end
tests through the wire protocol, which run on every pull request alongside a
query battery compared differentially against MySQL 8.4.

!!! note "`php artisan migrate` and `CREATE DATABASE`"

    ElyraSQL has one logical database, `elyra`. `CREATE DATABASE IF NOT EXISTS`
    (which is what Laravel's `MigrateCommand` issues when the configured database
    is missing) succeeds as a no-op, as does `DROP DATABASE IF EXISTS` for a
    database that does not exist here. An **unconditional** `CREATE DATABASE`
    fails, because that caller is asking for an isolated database it would not
    get — before 1.7.0 it silently "succeeded" and every connection kept sharing
    `elyra`.

## Verified clients

- `mysql` / `mariadb` command-line clients
- PyMySQL, mysql-connector-python
- PHP PDO / Laravel Eloquent
- Rust drivers `sqlx` and `mysql_async` (the latter backs ElyraSQL's own
  observability sink — DB-verified end to end)
- DBeaver, MySQL Workbench (via the standard MySQL driver)
- Any language driver that speaks the MySQL protocol

### Authentication plugin

ElyraSQL advertises **`mysql_native_password`** by default. This is the pragmatic
choice for driver compatibility: the client completes the simple challenge/response
handshake (`SHA1(SHA1(password))`) with no key exchange, so PDO, PyMySQL,
mysql-connector, `sqlx`, and `mysql_async` all connect out of the box.

`caching_sha2_password` (MySQL 8's default) is available opt-in via
`ELYRASQL_AUTH_PLUGIN=caching_sha2_password`. ElyraSQL performs **full
authentication** every time (it keeps no fast-auth cache): the password is read
over the TLS channel, or — on a plaintext connection — RSA-encrypted with the
server's public key and decrypted server-side. Prefer **TLS** when using it, and
note that some drivers (e.g. `mysql_async`) are happiest on `mysql_native_password`
and may stumble on the full-auth exchange — the default already avoids this.

## Character set and collation

The default character set is `utf8mb4` and the default collation is
`utf8mb4_0900_ai_ci`, matching MySQL 8. The collation is case- **and**
accent-insensitive: `'café' = 'cafe'`, `'Straße' = 'Strasse'`, `'ae' = 'æ'`, and
ordering interleaves non-ASCII with ASCII (`Ærlig, ål, Ape, cafe, cat, øl, zz`).

The folding table is derived from MySQL's own `WEIGHT_STRING` output, so equality
and ordering agree with MySQL for Latin text. Characters that have their own
primary weight in MySQL (`þ`, `ŋ`, `ı`) are case-folded but not reduced to ASCII,
and non-Latin scripts (Greek, Cyrillic, CJK) order by codepoint rather than full
UCA weights.

A database created by ElyraSQL before 1.5.0 is migrated automatically on first
open: text index entries are rebuilt and text primary keys re-keyed, before any
connection is accepted. Databases whose indexed text is pure ASCII are unaffected.

## Differences and gaps

ElyraSQL implements a focused, growing subset of MySQL SQL. Notable current
gaps:

- Subqueries (`WHERE` + SELECT-list, correlated + uncorrelated, **including over
  joins**), derived tables, CTEs including **`WITH RECURSIVE`**, `HAVING`,
  window functions with **explicit `ROWS` frames** and named windows,
  `GROUP BY ... WITH ROLLUP` and set operations are supported. Not yet:
  `RANGE`/`GROUPS` **numeric value-offset** frames (only the `UNBOUNDED`/`CURRENT
  ROW` forms of `RANGE`).
- Views, **materialized views**, row-level triggers, and stored procedures
  (parameters, local and session variables, `IF`/`WHILE`/`LOOP`/`REPEAT`,
  cursors, condition handlers) are supported; user-defined functions and
  scheduled events are not.
- `ALTER TABLE` supports add/drop/rename/`MODIFY`/`CHANGE` column, rename table,
  `ADD INDEX`/`KEY`/`UNIQUE` (with backfill) and **`ADD FOREIGN KEY`** (enforced,
  though `SHOW CREATE TABLE` does not yet echo the constraint back);
  `ADD PRIMARY KEY` on an existing table must instead be declared in
  `CREATE TABLE`.
- A broad scalar function library (string, math, date/time, JSON, `MD5`/`SHA1`/
  `SHA2`, `HEX`/`UNHEX`, `FORMAT`, `FIND_IN_SET`, `FROM_UNIXTIME`, ...),
  statistical and bitwise aggregates (`STDDEV*`, `VAR*`, `BIT_OR`/`AND`/`XOR`),
  `LAST_INSERT_ID()`/`ROW_COUNT()`, `@@`system variables, and `CONVERT()`. The
  MySQL shorthands `INSERT ... SET`, the `<<`/`>>`/`~` bitwise operators and
  `LOAD DATA LOCAL INFILE` all work.
- Vector search and `VEC_DISTANCE(...)` are ElyraSQL extensions (they mirror
  MySQL 9's vector direction but are not identical).
- **One database.** `CREATE DATABASE`/`SCHEMA` is refused unless written with
  `IF NOT EXISTS`; see the note under *Laravel / Eloquent* above. `USE <name>`
  is accepted and changes what the catalog reports, but does not give a separate
  namespace.
- **Result metadata omits the source table.** MySQL fills the `table` field of
  each column definition; ElyraSQL leaves it empty. Column *names* match MySQL
  (a `SELECT *` over a join returns bare, possibly duplicated names), so a client
  that disambiguates duplicates via metadata cannot, and must use positional
  access or explicit aliases. Tracked in
  [ESQL-55](https://wirelabs.youtrack.cloud/issue/ESQL-55).
- **Isolation levels:** `SET TRANSACTION ISOLATION LEVEL ...` is accepted for all
  four standard levels, but only two engines exist — `SERIALIZABLE` (opt-in) and
  **snapshot** isolation, which backs everything else. Snapshot is *at least as
  strong* as `READ UNCOMMITTED`, `READ COMMITTED`, and `REPEATABLE READ` (no dirty
  reads, repeatable reads, no phantoms within a transaction), so a client that
  asks for `READ COMMITTED` gets more isolation, never less. The one behavioural
  difference: a long transaction under snapshot does **not** see other
  transactions' commits mid-flight (it reads a consistent snapshot from `BEGIN`),
  whereas true `READ COMMITTED` would. `@@transaction_isolation` reports
  `REPEATABLE-READ` (MySQL's default), which is what most ORMs expect.
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
