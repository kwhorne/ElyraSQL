# Framework Integration

ElyraSQL speaks the MySQL wire protocol, so application frameworks connect to it
with their standard **MySQL** driver — no special client. This page collects the
recommended connection settings per framework so a stock app runs cleanly.

The one setting that matters across drivers: prefer **client-side (emulated)
prepared statements**. ElyraSQL's binary (native) prepared-statement parameter
binding is not yet reliable with every driver (notably PDO/mysqlnd); emulated
prepares send fully-formed queries and are universally supported. Drivers that
already bind client-side (PyMySQL, sqlx) need no change.

---

## Laravel (Eloquent)

ElyraSQL runs Laravel migrations, Eloquent models and relationships, the query
builder, transactions, and pagination.

### Recommended configuration

`config/database.php` → `connections.mysql`:

```php
'mysql' => [
    'driver'    => 'mysql',
    'host'      => env('DB_HOST', '127.0.0.1'),
    'port'      => env('DB_PORT', '3307'),
    'database'  => env('DB_DATABASE', 'elyra'),
    'username'  => env('DB_USERNAME', 'root'),
    'password'  => env('DB_PASSWORD', ''),
    'charset'   => 'utf8mb4',
    'collation' => 'utf8mb4_unicode_ci',
    'prefix'    => '',
    'strict'    => false,
    'options'   => [
        // ElyraSQL: use client-side prepared statements.
        PDO::ATTR_EMULATE_PREPARES => true,
    ],
],
```

`.env`:

```ini
DB_CONNECTION=mysql
DB_HOST=127.0.0.1
DB_PORT=3307
DB_DATABASE=elyra
DB_USERNAME=root
DB_PASSWORD=
```

### Why these settings

- **`DB_DATABASE=elyra`** — ElyraSQL exposes a single database named `elyra`.
  Laravel uses this name as the `information_schema` schema for
  `Schema::hasTable()` / `hasColumn()` and for `SHOW` introspection, so it must
  match.
- **`PDO::ATTR_EMULATE_PREPARES => true`** — see the note above; required for
  parameter-bound queries to behave correctly today.
- **`'strict' => false`** — avoids Laravel issuing `SET SESSION sql_mode=...`
  with modes ElyraSQL does not model; functionally a no-op either way, but
  keeps the session-setup quiet.

### What works

A full Eloquent workload runs cleanly:

- **Migrations** — `Schema::create` with `$table->id()`, `string()`, `integer()`,
  `decimal()`, `boolean()`, `timestamps()`, `$table->foreignId()->constrained()
  ->onDelete('cascade')`, `unique()`, `index()`, and `Schema::table(...)` changes.
- **Models & CRUD** — `create()` (with correct `lastInsertId`), `find()`,
  `where()`, `update()`, `delete()`, `count()`, `whereIn()`, `orderBy()`,
  `pluck()`.
- **Relationships** — `hasMany`/`belongsTo`, eager loading (`with()`),
  `withCount()`, aggregates over relations.
- **Query builder** — `join()`, `select()`, `selectRaw()` aggregates, `groupBy()`
  + `havingRaw()`, `exists()`, `paginate()`, `updateOrInsert()`.
- **Transactions** — `DB::transaction()`, `beginTransaction()`/`commit()`/
  `rollBack()` (`PDO::inTransaction()` is reported correctly).

### Known caveats

- `GROUP BY ... WITH ROLLUP` and comma-style multi-table `UPDATE t1, t2 SET ...`
  are not parsed (use a `JOIN` for the latter). These are rare in Eloquent.
- Keep `PDO::ATTR_EMULATE_PREPARES => true` until native prepared-statement
  binding lands.

---

## PHP / PDO (framework-agnostic)

```php
$pdo = new PDO(
    'mysql:host=127.0.0.1;port=3307;dbname=elyra;charset=utf8mb4',
    'root',
    '',
    [
        PDO::ATTR_EMULATE_PREPARES => true,
        PDO::ATTR_ERRMODE          => PDO::ERRMODE_EXCEPTION,
    ]
);
```

Symfony (Doctrine DBAL) and any other PDO-based stack use the same
`PDO::ATTR_EMULATE_PREPARES => true` option under `driverOptions`.

---

## Python

**PyMySQL** and **mysqlclient** bind parameters client-side, so no special
option is needed:

```python
import pymysql
conn = pymysql.connect(host="127.0.0.1", port=3307, user="root",
                       password="", database="elyra", charset="utf8mb4",
                       autocommit=True)
```

**Django** — set `ENGINE` to `django.db.backends.mysql` with the standard
options; use `NAME = "elyra"`.

**SQLAlchemy** — `mysql+pymysql://root:@127.0.0.1:3307/elyra`.

---

## Rust

**sqlx** (MySQL) works out of the box (it binds client-side):

```rust
let pool = sqlx::mysql::MySqlPoolOptions::new()
    .connect("mysql://root:@127.0.0.1:3307/elyra").await?;
```

Set `ELYRASQL_STMT_DESCRIBE=on` on the server if a driver needs prepared-result
columns resolved by name at prepare time (sqlx benefits from this).

---

## Node.js

**mysql2** — use the standard connection; for parameterized queries prefer the
query API (client-side substitution) over the native prepared-statement
`execute()` path:

```js
const mysql = require('mysql2/promise');
const conn = await mysql.createConnection({
  host: '127.0.0.1', port: 3307, user: 'root', password: '', database: 'elyra',
});
```

---

## General checklist

1. Use the MySQL driver, host `127.0.0.1`, port `3307` (or your `--listen`).
2. Database name **`elyra`**.
3. Charset **`utf8mb4`**.
4. Prefer **emulated / client-side prepared statements** where the driver
   offers the choice.

See [MySQL Compatibility](mysql-compatibility.md) for the supported SQL surface
and [Limitations & Roadmap](limitations.md) for current gaps.
