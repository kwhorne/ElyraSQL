# Installation

ElyraSQL targets **Ubuntu 24.04+** for production but runs anywhere Rust does.

!!! warning "Upgrading to 1.5.0 or later"

    1.5.0 changed the default collation to `utf8mb4_0900_ai_ci`, which changes the
    bytes under which text is stored. A database created by an earlier version is
    migrated automatically the first time 1.5.0 or later opens it: text index entries are
    rebuilt and text primary keys re-keyed, before any connection is accepted.
    Databases whose indexed text is pure ASCII are not rewritten.

    The migration writes in batches so that a large table cannot exhaust memory at
    startup, and it is **idempotent**: an already re-keyed row encodes to the key it
    is already stored under. The version marker is written only once every table is
    done, so an interrupted upgrade simply resumes on the next start. **Take a backup
    first, and note that downgrading to 1.4.x afterwards is not supported.**

!!! info "Upgrading to 1.6.0"

    No on-disk format change and no migration: a 1.5.x database opens unchanged, and a
    database written by 1.6.0 still opens in 1.5.x. One behaviour change worth knowing
    before you upgrade: a **non-equi join** (`ON a.id < b.id`, a `BETWEEN` band join)
    used to be refused once its intermediate result hit
    `ELYRASQL_JOIN_MAX_ROWS`/`_BYTES`. It now streams, so such a query *answers* —
    with bounded memory but `O(n x m)` time. If you relied on the ceilings to stop
    runaway joins, set [`ELYRASQL_QUERY_TIMEOUT_MS`](configuration.md) instead; it
    interrupts one promptly and leaves the session usable.

## Static binaries

Each [release](https://github.com/kwhorne/ElyraSQL/releases) ships fully static
`musl` binaries for `x86_64` and `aarch64` — no libc or other runtime
dependency.

```bash
VER=1.6.0
ARCH=x86_64   # or aarch64
curl -L -o elyrasql.tar.gz \
  https://github.com/kwhorne/ElyraSQL/releases/download/v${VER}/elyrasql-${VER}-linux-${ARCH}.tar.gz
tar xzf elyrasql.tar.gz
cd elyrasql-${VER}-linux-${ARCH}
./elyrasql version
```

Each archive contains the `elyrasql` binary, `README`, `LICENSE`, and a sample
`elyrasql.service` systemd unit. Verify integrity with the published
`.sha256` file.

## Docker

Multi-arch image (`amd64` + `arm64`) on the GitHub Container Registry:

```bash
docker pull ghcr.io/kwhorne/elyrasql:1.6.0   # or :latest
docker run -p 3307:3307 -v elyra:/var/lib/elyrasql ghcr.io/kwhorne/elyrasql:1.6.0
```

The image is ~15 MB, runs as a non-root user, stores data in the
`/var/lib/elyrasql` volume, and is configured via environment variables (see
[Configuration](configuration.md)).

## Build from source

Requires Rust 1.82+.

```bash
git clone https://github.com/kwhorne/ElyraSQL.git
cd ElyraSQL
cargo build --release
./target/release/elyrasql serve
```

To build a static binary yourself:

```bash
rustup target add x86_64-unknown-linux-musl
sudo apt-get install -y musl-tools
cargo build --release --target x86_64-unknown-linux-musl -p elyra-cli
```

## Systemd (Ubuntu)

The repository ships a hardened unit and an install script:

```bash
sudo ./packaging/deploy.sh
# or with credentials + TLS:
ELYRASQL_USER=root ELYRASQL_PASSWORD=secret \
  ELYRASQL_LISTEN=0.0.0.0:3307 sudo -E ./packaging/deploy.sh
```

See [Deployment](deployment.md) for details.
