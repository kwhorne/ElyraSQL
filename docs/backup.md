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
recovery:

```bash
elyrasql serve --data elyra.edb --binlog /var/lib/elyrasql/elyra.binlog
```

Each record carries an LSN and a millisecond timestamp. Because write-sets are
absolute key/value changes, replaying the log in order onto a base is
idempotent. To recover:

```bash
# 1. Restore the most recent full backup (or start from an empty file).
elyrasql restore --input /var/backups/elyra.edb --data /var/lib/elyrasql/recovered.edb

# 2. Replay the binlog up to the desired point (LSN or timestamp).
elyrasql binlog-replay --data recovered.edb --binlog elyra.binlog --until-time-ms 1783619354469
# or --until-lsn 421   (omit both to replay everything)
```

Replaying the whole binlog onto an empty file also reconstructs the full state
from genesis. Combine periodic full backups with a continuous binlog for
recovery to any moment.

!!! note "Not yet available"
    Binlog rotation/pruning is manual, and there is no incremental (block-level)
    backup. See [Limitations](limitations.md).
