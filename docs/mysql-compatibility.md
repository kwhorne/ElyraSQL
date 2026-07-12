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
- **Handshake** — reports a MySQL-looking version, e.g. `8.0.0-ElyraSQL-0.9.5`,
  and answers the session/introspection queries clients send on connect
  (`SELECT @@version_comment`, `SELECT VERSION()`, `SET ...`,
  `SHOW VARIABLES/STATUS/COLLATION/DATABASES/TABLE STATUS`, and the
  `information_schema` tables GUI tools read to build their schema tree).

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
- `ALTER TABLE` supports add/drop/rename column and rename table (not
  `MODIFY`/`CHANGE` type changes).
- Vector search and `VEC_DISTANCE(...)` are ElyraSQL extensions (they mirror
  MySQL 9's vector direction but are not identical).
- `SHOW` and `information_schema` cover what GUI tools and drivers need to
  connect and browse (`tables`, `columns`, `engines`, `schemata`, `views`,
  `routines`, `triggers`, `events`, `statistics`, `partitions`); it is not the
  complete MySQL catalog.

See [Limitations & Roadmap](limitations.md) for the full picture.

!!! note "Prepared-statement caveat"
    Repeated `COM_STMT_CLOSE` → `COM_STMT_PREPARE` cycles on a single connection
    can desynchronize with strict clients due to a limitation in the underlying
    wire library. Statement reuse and pooled clients (the common case), plus
    client-side-binding drivers like PyMySQL, are unaffected.
