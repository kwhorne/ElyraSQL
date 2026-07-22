# Deployment

## Docker

```bash
docker run -d --name elyrasql \
  -p 3307:3307 \
  -v elyra-data:/var/lib/elyrasql \
  -e ELYRASQL_USER=root \
  -e ELYRASQL_PASSWORD=secret \
  ghcr.io/kwhorne/elyrasql:1.4.10
```

- Data persists in the `/var/lib/elyrasql` volume.
- The container runs as a non-root user and listens on `0.0.0.0:3307`.
- Configure via `ELYRASQL_*` environment variables (see
  [Configuration](configuration.md)).

For TLS, mount the certificate and key and point the env vars at them:

```bash
docker run -d -p 3307:3307 \
  -v elyra-data:/var/lib/elyrasql \
  -v $PWD/certs:/certs:ro \
  -e ELYRASQL_TLS_CERT=/certs/server.crt \
  -e ELYRASQL_TLS_KEY=/certs/server.key \
  ghcr.io/kwhorne/elyrasql:1.4.10
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

## Operational notes

- **Concurrency** — reads scale across connections (MVCC snapshots); writes are
  serialized through a single group-commit writer for throughput.
- **Memory** — table scans and aggregations stream; per-connection memory stays
  bounded regardless of table size. In-transaction reads and `ORDER BY` /
  grouped results materialize their working set.
- **Single file** — keep it on durable, fast storage; the OS page cache handles
  data larger than RAM via memory mapping.
