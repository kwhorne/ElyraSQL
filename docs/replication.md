# Replication & High Availability

ElyraSQL supports **asynchronous primary → replica replication** for warm
standbys and read scaling. A replica bootstraps from a consistent snapshot of
the primary and then applies the primary's ongoing write stream, converging to
the primary's exact state.

## How it works

- The primary tags every committed write-set with a monotonic log sequence
  number (LSN) and streams them to connected replicas.
- A replica first receives a full snapshot of the keyspace, then applies each
  subsequent write-set in LSN order. Write-sets are absolute key/value changes,
  so applying them in order is idempotent — a replica never diverges.
- Replicas are **read-only**: they reject writes from clients.

## Running a primary

Add a replication endpoint to a normal server:

```bash
elyrasql serve \
  --data /var/lib/elyrasql/elyra.edb \
  --listen 0.0.0.0:3307 \
  --replication-listen 0.0.0.0:7000
```

## Running a replica

```bash
elyrasql replica \
  --primary primary-host:7000 \
  --data /var/lib/elyrasql/replica.edb \
  --listen 0.0.0.0:3307
```

The replica's `--data` file is **disposable**: it is recreated and
re-bootstrapped from the primary each time the replica starts. Point read-only
clients at the replica's MySQL port.

## Semi-synchronous replication

By default replication is asynchronous. Enable **semi-sync** on the primary to
wait for a replica to acknowledge each commit before returning success to the
client, shrinking the data-loss window on failover:

```bash
elyrasql serve \
  --data elyra.edb --listen 0.0.0.0:3307 \
  --replication-listen 0.0.0.0:7000 \
  --semi-sync-ms 2000
```

Each commit waits up to `--semi-sync-ms` for a replica to acknowledge the write.
If no replica acknowledges in time (or none is connected), the commit proceeds
anyway (degrading to asynchronous), so a lost replica never blocks the primary.

## Failover

Failover is manual. A replica's data file is a complete ElyraSQL database, so to
promote it, stop the replica and start it as a normal primary against the same
file:

```bash
elyrasql serve --data /var/lib/elyrasql/replica.edb --listen 0.0.0.0:3307
```

Then repoint clients (and any remaining replicas) at the new primary.

## Guarantees & limits

- **Asynchronous**: a replica lags the primary slightly; a committed write is
  not guaranteed to be on any replica yet (no synchronous/quorum commit).
- If a replica falls too far behind the primary's in-memory backlog, its stream
  is dropped and it re-bootstraps from a fresh snapshot on reconnect (run it
  under a supervisor such as systemd so it restarts automatically).
- There is no automatic leader election or failover, and no multi-primary /
  conflict resolution. See [Limitations](limitations.md).
