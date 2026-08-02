# Installation

ElyraSQL release builds target **Ubuntu 24.04+** and **Apple Silicon macOS
11+**. Intel Macs are not supported.

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

!!! danger "Upgrade to 1.9.0 promptly"

    1.9.0 fixes two bugs that could return or write the wrong data **with no
    error**, and one that could take the server down. If you are on 1.8.0 or
    earlier, treat this as more than routine:

    - **`NATURAL JOIN` and `JOIN ... USING` executed as cross joins.** Every
      such query returned a cartesian product instead of a join. If you have
      results derived from those shapes, recompute them.
    - **A database qualifier was ignored**, so `UPDATE otherdb.t ...` and
      `DELETE FROM otherdb.t ...` modified the *local* table and reported
      success. Worth auditing if anything in your stack issues qualified writes
      — a migration runner pointed at the wrong environment, or a dump replayed
      with its original `db.table` names.
    - **Nested views could crash the server**, killing every connection.

    No on-disk format change and no migration: a 1.5.x through 1.8.x database
    opens unchanged. Relation aliases also become case-sensitive again (MySQL's
    behaviour), so `FROM t AS T WHERE t.id = 1` now errors where it used to be
    accepted.

!!! info "Upgrading to 1.8.0"

    No on-disk format change and no migration: a 1.5.x, 1.6.x or 1.7.x database
    opens unchanged. Two behaviour changes worth knowing. **`UNSIGNED` is now
    enforced on every integer width** — a `TINYINT`/`SMALLINT`/`INT UNSIGNED`
    column used to accept negative values, and now refuses them like
    `BIGINT UNSIGNED` always has (columns *created* by an older version keep the
    type they were created with, so they go on accepting negatives until the
    table is recreated). And several **error codes now match MySQL**: an unknown
    column is 1054 rather than 1146, and an out-of-range value is 1264 rather
    than 1366, so a client that branches on the code sees what it expects.

!!! info "Upgrading to 1.7.0"

    No on-disk format change and no migration: a 1.5.x or 1.6.x database opens
    unchanged, and a database written by 1.7.0 still opens in either. Two behaviour
    changes worth knowing before you upgrade. **`CREATE DATABASE` now refuses**
    instead of quietly succeeding, because this server has one logical schema and
    reporting success made callers believe otherwise; the conditional forms
    (`CREATE DATABASE IF NOT EXISTS`, `DROP DATABASE IF EXISTS`) are still no-ops, so
    Laravel migrations and container entrypoints are unaffected. And the **advertised
    MySQL version is now 8.0.12** rather than 8.0.0, which lets version-gated clients
    generate window-function SQL they previously suppressed.

!!! info "Upgrading to 1.6.0"

    No on-disk format change and no migration: a 1.5.x database opens unchanged, and a
    database written by 1.6.0 still opens in 1.5.x. One behaviour change worth knowing
    before you upgrade: a **non-equi join** (`ON a.id < b.id`, a `BETWEEN` band join)
    used to be refused once its intermediate result hit
    `ELYRASQL_JOIN_MAX_ROWS`/`_BYTES`. It now streams, so such a query *answers* —
    with bounded memory but `O(n x m)` time. If you relied on the ceilings to stop
    runaway joins, set [`ELYRASQL_QUERY_TIMEOUT_MS`](configuration.md) instead; it
    interrupts one promptly and leaves the session usable.

## Release binaries

Each [release](https://github.com/kwhorne/ElyraSQL/releases) ships fully static
Linux `musl` binaries for `x86_64` and `aarch64`, and — since v1.7.0 — a native
Apple Silicon macOS binary. The macOS build links only Apple-provided system
libraries and supports macOS 11 or later.

```bash
# Linux
VER=X.Y.Z
ARCH=x86_64   # or aarch64
curl -L -o elyrasql.tar.gz \
  "https://github.com/kwhorne/ElyraSQL/releases/download/v${VER}/elyrasql-${VER}-linux-${ARCH}.tar.gz"
tar xzf elyrasql.tar.gz
cd elyrasql-${VER}-linux-${ARCH}
./elyrasql version
```

```bash
# Apple Silicon, macOS 11+
VER=X.Y.Z
curl -L -o elyrasql.tar.gz \
  "https://github.com/kwhorne/ElyraSQL/releases/download/v${VER}/elyrasql-${VER}-macos-aarch64.tar.gz"
tar xzf elyrasql.tar.gz
cd elyrasql-${VER}-macos-aarch64
./elyrasql version
```

Every archive contains `elyrasql`, `README`, and `LICENSE`; Linux archives also
contain a sample `elyrasql.service` systemd unit. Verify integrity with the
published `.sha256` file (`sha256sum -c` on Linux or `shasum -a 256 -c` on
macOS).

!!! note "macOS code signing"

    The macOS binary currently has the ad-hoc signature produced by the Apple
    linker, not a Developer ID signature or Apple notarization. Environments
    that require Gatekeeper publisher verification should build from source
    until signed and notarized release credentials are configured.

## Docker

Multi-arch image (`amd64` + `arm64`) on the GitHub Container Registry:

```bash
docker pull ghcr.io/kwhorne/elyrasql:1.9.0   # or :latest
docker run -p 3307:3307 -v elyra:/var/lib/elyrasql ghcr.io/kwhorne/elyrasql:1.9.0
```

The image is ~15 MB, runs as a non-root user, stores data in the
`/var/lib/elyrasql` volume, and is configured via environment variables (see
[Configuration](configuration.md)).

## Build from source

Requires Rust 1.88+ and the platform toolchain. On macOS, install Xcode Command
Line Tools first (`xcode-select --install`).

```bash
git clone https://github.com/kwhorne/ElyraSQL.git
cd ElyraSQL
cargo build --release
./target/release/elyrasql serve
```

For an explicit Apple Silicon build with the same deployment floor as the
release:

```bash
rustup target add aarch64-apple-darwin
MACOSX_DEPLOYMENT_TARGET=11.0 \
  cargo build --release --locked --target aarch64-apple-darwin -p elyra-cli
./target/aarch64-apple-darwin/release/elyrasql version
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
