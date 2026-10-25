# Backup & Restore

ElyraSQL stores everything in a single file, which makes backups simple: a
complete, consistent copy of that file *is* the backup. There are two ways to
produce one.

## Hot backup (server running)

Run the SQL command while the server is serving. It copies the whole database
from a consistent point-in-time snapshot without blocking writers:

```sql
BACKUP TO '/var/backups/elyra-2026-07-09.edb';
```

- Requires the **admin** privilege.
- The target file must not already exist (ElyraSQL refuses to overwrite).
- The result reports the number of key/value records copied.
- Because it reads from an MVCC snapshot, concurrent writes during the backup
  are simply not included — the copy is a clean point-in-time image.

The produced file is itself a normal ElyraSQL database. Point a server at it to
inspect or use it directly:

```bash
elyrasql serve --data /var/backups/elyra-2026-07-09.edb --listen 127.0.0.1:3308
```

## Offline backup and restore (server stopped)

When the server is **not** running against the file, use the CLI. (redb takes
an exclusive lock on the open file, so offline tools require the server to be
stopped.)

```bash
# Back up
elyrasql backup --data /var/lib/elyrasql/elyra.edb --out /var/backups/elyra.edb

# Restore into place (refuses to overwrite unless --force)
elyrasql restore --input /var/backups/elyra.edb --data /var/lib/elyrasql/elyra.edb --force
```

`restore` validates that the backup opens as a valid ElyraSQL database before
copying it over the target.

## Restoring a hot backup

A file produced by `BACKUP TO` is complete, so restoring is just putting it in
place while the server is stopped:

```bash
systemctl stop elyrasql
cp /var/backups/elyra-2026-07-09.edb /var/lib/elyrasql/elyra.edb
systemctl start elyrasql
```

## Recommended routine

- Schedule a periodic `BACKUP TO` (e.g. via a cron job that connects and runs
  the command) to a timestamped path, then move it off-host.
- Keep several generations; the files compress well.
- Test restores regularly by starting a throwaway server against a backup.

## Point-in-time recovery (binlog)

Enable the binary log to record every committed write-set for point-in-time
recovery. `--binlog` is a **directory** of rotating segment files
(`binlog.000001`, ...):

```bash
elyrasql serve --data elyra.edb --binlog /var/lib/elyrasql/binlog
```

Segments rotate at `ELYRASQL_BINLOG_SEGMENT_MB` (default 128 MB). Inspect and
prune them with SQL:

```sql
SHOW BINARY LOGS;                       -- Log_name, File_size
PURGE BINARY LOGS TO 'binlog.000010';   -- delete segments before this one
```

Prune segments that predate your most recent full backup.

Each record carries an LSN and a millisecond timestamp. Because write-sets are
absolute key/value changes, replaying the log in order onto a base is
idempotent. To recover:

```bash
# 1. Restore the most recent full backup (or start from an empty file).
elyrasql restore --input /var/backups/elyra.edb --data /var/lib/elyrasql/recovered.edb

# 2. Replay the binlog up to the desired point (LSN or timestamp).
elyrasql binlog-replay --data recovered.edb --binlog /var/lib/elyrasql/binlog --until-time-ms 1783619354469
# or --until-lsn 421   (omit both to replay everything)
```

Replaying the whole binlog onto an empty file also reconstructs the full state
from genesis. Combine periodic full backups with a continuous binlog for
recovery to any moment.

!!! note "Not yet available"
    There is no incremental (block-level) backup. See
    [Limitations](limitations.md).

## Dumping with mysqldump

MySQL 8.4's `mysqldump` dumps an ElyraSQL database as it dumps a MySQL one,
and the dump restores into ElyraSQL or into MySQL:

```bash
mysqldump -h elyrasql.example.com -P 3307 -u root -p \
  --routines --triggers --single-transaction --set-gtid-purged=OFF \
  elyra > elyra.sql

# Into ElyraSQL, or into MySQL.
mysql -h elyrasql.example.com -P 3307 -u root -p elyra < elyra.sql
```

Tables and their rows (`BLOB` values included), indexes, foreign keys, `CHECK`
constraints, generated columns, partitioning, views (with their definer and
`SQL SECURITY`), triggers and stored procedures (with their parameter types)
come back as they were. A database that has been through `ANALYZE TABLE` has
its statistics collected again on restore: mysqldump writes them as `ANALYZE
TABLE ... UPDATE HISTOGRAM`, which ElyraSQL runs as `ANALYZE TABLE`. This was
checked with MySQL 8.4's `mysqldump`: a dump restored into an empty ElyraSQL
dumps again to the same file, and restores into MySQL 8.4 with the same rows.

`--single-transaction` dumps from one snapshot without blocking anyone.
`--lock-all-tables` works too: its `FLUSH TABLES WITH READ LOCK` takes a read
lock on every table until the dump ends, so writes to them wait, though a table
created meanwhile is not locked. `--databases elyra`, `--hex-blob`,
`--no-data`, `--compact` and `--skip-opt` work as they do against MySQL.

Things to know:

- **Name the database; do not use `--all-databases`.** mysqldump holds back
  every database it lists until it has seen one named `mysql`, which ElyraSQL
  does not have, so the dump comes out empty -- and mysqldump reports success.
- **`--add-drop-database` does not restore into ElyraSQL.** The dump then opens
  with `DROP DATABASE`, which ElyraSQL refuses for its single database.
- **A procedure created before 1.12.9 is dumped without its parameter types**,
  which were not recorded until then. ElyraSQL reads it back; MySQL does not.
  Drop and create it again to record them.
- **Triggers and procedures are dumped with `DEFINER=`root`@`%``** and the
  server's default `sql_mode`, which is what they run with.
- **Into MySQL:** a `DATETIME` column declared without fractional seconds keeps
  them in ElyraSQL, and MySQL rounds them away on restore.

## Importing a MySQL dump

A `mysqldump` of one MySQL database restores into ElyraSQL with the `mysql`
client:

```bash
# On the MySQL side: one database.
mysqldump --routines --triggers --single-transaction --set-gtid-purged=OFF \
  shop > shop.sql

# Into ElyraSQL.
mysql -h elyrasql.example.com -P 3307 -u root -p elyra < shop.sql
```

Tables, rows, indexes, foreign keys, views (with their definer and `SQL
SECURITY`), triggers and stored procedures arrive as they were; this was
checked with MySQL 8.4's `mysqldump`. The dump's version comments
(`/*!50001 ... */`) and session settings run as they do in MySQL.

`BLOB` values, which mysqldump writes as `_binary '...'` with their bytes as
they are, arrive byte for byte. With `--databases`, the dump's `CREATE DATABASE
IF NOT EXISTS ... DEFAULT CHARACTER SET ...` is accepted and changes nothing,
as in MySQL for a database that exists.

One thing to know:

- **Stored functions are not supported yet.** A dump with `--routines`
  contains every `CREATE FUNCTION`, and the restore stops at the first one.
  Leave them out of the dump, or restore with `mysql --force`, which reports
  each and carries on.
