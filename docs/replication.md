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

## Automatic failover (cluster mode)

Run nodes in `cluster` mode for **automatic failover** via Raft-style leader
election. The elected leader accepts writes and serves the replication endpoint;
followers are read-only and replicate from the current leader. If the leader
fails, a surviving node is elected (given a majority) and starts accepting
writes — no manual intervention.

```bash
# 3-node cluster (each node lists the others as peers by control address)
elyrasql cluster --id 1 --data /var/lib/elyrasql/n1.edb \
  --listen 0.0.0.0:3307 --control-listen 0.0.0.0:4501 --replication-listen 0.0.0.0:5501 \
  --peer 2@node2:4502 --peer 3@node3:4503
```

A write is only accepted while a node believes it is the leader for the current
term (fencing). Election needs a **majority**, so run an odd number of nodes
(3 or 5) to tolerate 1 or 2 failures without split-brain.

Because replication is asynchronous, a newly elected leader may be missing the
old leader's last unreplicated writes; on a leadership change a follower
re-bootstraps from the new leader. There is no synchronous/quorum commit.

## Manual failover

A replica's data file is a complete ElyraSQL database, so to promote it manually,
stop the replica and start it as a normal primary against the same file:

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
