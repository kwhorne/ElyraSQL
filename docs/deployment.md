# Deployment

## Docker

```bash
docker run -d --name elyrasql \
  -p 3307:3307 \
  -v elyra-data:/var/lib/elyrasql \
  -e ELYRASQL_USER=root \
  -e ELYRASQL_PASSWORD=secret \
  ghcr.io/kwhorne/elyrasql:1.11.1
```

- Data persists in the `/var/lib/elyrasql` volume.
- The container runs as a non-root user and listens on `0.0.0.0:3307`.
- Configure via `ELYRASQL_*` environment variables (see
  [Configuration](configuration.md)).

### The image has no shell

Since 1.9.4 the image is built `FROM scratch`: it contains the static binary,
`passwd`/`group` for the non-root user, the data directory, and a writable
`/tmp` for sort and aggregate spills. That is 13.6 MB with no OS packages, and
so no OS CVEs to triage — but also no shell, no `mysql` client, no `ls`.

`docker exec <container> sh` will not work. Check liveness over the MySQL
protocol from outside the container:

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p"$PASSWORD" -e 'SELECT 1'
```

In Kubernetes, use a `tcpSocket` probe on 3307 rather than an `exec` probe. In
Compose, a `healthcheck` must run from another container.

One consequence worth knowing: spill files for large sorts and aggregations go
to `/tmp` **inside the container**, which is the writable layer, not the data
volume. A query that spills tens of gigabytes is bounded by container disk, not
by the volume. Set `TMPDIR` to a path on a mounted volume if that matters:

```bash
docker run -d -p 3307:3307 \
  -v elyra-data:/var/lib/elyrasql \
  -e TMPDIR=/var/lib/elyrasql/tmp \
  ghcr.io/kwhorne/elyrasql:1.11.1
```

For TLS, mount the certificate and key and point the env vars at them:

```bash
docker run -d -p 3307:3307 \
  -v elyra-data:/var/lib/elyrasql \
  -v $PWD/certs:/certs:ro \
  -e ELYRASQL_TLS_CERT=/certs/server.crt \
  -e ELYRASQL_TLS_KEY=/certs/server.key \
  ghcr.io/kwhorne/elyrasql:1.11.1
```

## systemd (Ubuntu 24.04+)

The repository ships `packaging/elyrasql.service` (hardened: `NoNewPrivileges`,
`ProtectSystem=strict`, `PrivateTmp`, dedicated user) and an install script:

```bash
sudo ./packaging/deploy.sh
```

Provide credentials/TLS/listen address via the environment; the script writes a
systemd drop-in:

```bash
ELYRASQL_USER=root ELYRASQL_PASSWORD=secret \
ELYRASQL_LISTEN=0.0.0.0:3307 \
sudo -E ./packaging/deploy.sh
```

Manage the service:

```bash
sudo systemctl status elyrasql
sudo systemctl restart elyrasql
journalctl -u elyrasql -f
```

## Backups

The entire database is a single file (default `/var/lib/elyrasql/elyra.edb`).
Because the storage engine is crash-safe and copy-on-write, you can snapshot the
file at the filesystem/volume level. For a consistent copy, prefer a moment of
low write activity or a volume snapshot.

## Restart policy

Release binaries abort on panic rather than unwinding, so **the process must be
supervised**. A panic is a crash, not a degraded connection.

This is deliberate. Unwinding a single connection task left the process running,
but if it unwound while holding one of the server's `Mutex` guards, that mutex
stayed poisoned for the process lifetime and every later query failed — a server
that answered a health check while serving nothing. Aborting is the same path the
crash-recovery and soak/chaos suites exercise on every run: the storage engine is
crash-safe, so a restart recovers with no data loss and no manual step.

`packaging/elyrasql.service` already sets `Restart=on-failure` and
`RestartSec=2`. Under Docker or an orchestrator, set an equivalent:

```bash
docker run -d --restart unless-stopped -p 3307:3307 \
  -v elyra-data:/var/lib/elyrasql \
  ghcr.io/kwhorne/elyrasql:1.11.1
```

Kubernetes restarts containers by default. If you run the binary under anything
that does **not** restart it, a panic becomes an outage that lasts until someone
notices.

## Operational notes

- **Concurrency** — reads scale across connections (MVCC snapshots); writes are
  serialized through a single group-commit writer for throughput.
- **Memory** — table scans and aggregations stream; per-connection memory stays
  bounded regardless of table size. In-transaction reads and `ORDER BY` /
  grouped results materialize their working set.
- **Single file** — keep it on durable, fast storage; the OS page cache handles
  data larger than RAM via memory mapping.
