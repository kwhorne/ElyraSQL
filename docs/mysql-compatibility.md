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
- **Handshake** — reports a MySQL-looking version, e.g. `8.0.0-ElyraSQL-0.8.10`,
  and answers the session/introspection queries clients send on connect
  (`SELECT @@version_comment`, `SELECT VERSION()`, `SET ...`, etc.).

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
- No views, triggers, stored procedures, or user-defined functions.
- `ALTER TABLE` supports add/drop/rename column and rename table (not
  `MODIFY`/`CHANGE` type changes).
- Vector search and `VEC_DISTANCE(...)` are ElyraSQL extensions (they mirror
  MySQL 9's vector direction but are not identical).
- `information_schema` / `SHOW` coverage is minimal.

See [Limitations & Roadmap](limitations.md) for the full picture.

!!! note "Prepared-statement caveat"
    Repeated `COM_STMT_CLOSE` → `COM_STMT_PREPARE` cycles on a single connection
    can desynchronize with strict clients due to a limitation in the underlying
    wire library. Statement reuse and pooled clients (the common case), plus
    client-side-binding drivers like PyMySQL, are unaffected.
