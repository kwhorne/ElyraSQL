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

!!! danger "Upgrading to 1.9.9 — security release"

    **If you run replication, 1.9.8 and earlier expose your data. Upgrade.**

    Connecting to the replication port returned a full copy of the database:
    no credentials, no handshake, nothing sent by the peer. The guard only
    covered non-loopback binds, so a primary started with
    `--replication-listen 127.0.0.1:...` and no cluster secret was readable by
    any local process — or by anything that could get a TCP connection opened on
    its behalf.

    Two more, as serious: a replica never checked its primary's identity, so
    anything answering on the primary's address could feed it fabricated rows —
    **even with `ELYRASQL_CLUSTER_SECRET` set**, because authentication ran in
    one direction only. And `elyrasql replica` had no authentication flags at
    all, so its MySQL listener always accepted any username as `Admin` over
    replicated production data.

    **The fixes are breaking, deliberately.** After upgrading:

    - **A primary with `--replication-listen` needs `ELYRASQL_CLUSTER_SECRET`.**
      Without it the endpoint refuses to start and logs why; the server keeps
      serving, so watch for `replication endpoint stopped` in the log rather
      than assuming replication is running.
    - **A replica needs the same secret**, and refuses to start without it.
    - **Primary and replica must be upgraded together.** The handshake gained a
      step; a 1.9.9 replica will not accept a 1.9.8 primary.
    - **A replica needs accounts**: `--user`/`--password`/`--auth USER:PASS:ROLE`,
      exactly as `serve` takes them.

    `ELYRASQL_ALLOW_OPEN_AUTH=1` opts out of all four, and is the honest way to
    say "this port is on a network I control". It is not a default.

    If you ran an exposed replication endpoint, rotate anything the data would
    have revealed. The port left no access log.

!!! warning "Upgrading to 1.9.8"

    Arithmetic that used to be approximate is now exact. Results and result
    *types* both change. None of it touches your data, and every change moves
    toward MySQL — but if you have code that compensated for the old behaviour,
    that compensation is now wrong.

    **Rounding a decimal gives a different answer at the halfway point.**

    ```sql
    SELECT ROUND(1.005, 2);   -- was 1.00, now 1.01 (MySQL: 1.01)
    ```

    1.005 has no exact binary form, so the old path rounded it down. Anything
    that reconciled against MySQL, a spreadsheet or an accounting system was
    seeing our answer, not theirs. **If you stored the old results, they do not
    match what the same query returns now.**

    **Division, `MOD`, `ROUND`, `TRUNCATE`, `AVG` and `SUM` now return
    `DECIMAL`.** They returned `DOUBLE`, or `BIGINT` for an integer `SUM`.
    Drivers hand a `DECIMAL` to your language differently — Python gets
    `decimal.Decimal` where it got `float`, PHP and Go get a string where they
    got a float. Code that assumes a float from `SUM()` or `AVG()` needs a
    conversion it did not need before.

    The scales follow MySQL exactly: `10 / 3` is `3.3333` (the dividend's scale
    plus four), `AVG` over `DECIMAL(12,2)` has scale 6, and `SUM` over an `INT`
    column is `DECIMAL` because the total can exceed the column's range.

    **A range bound written as a string now returns the rows it should.**

    ```sql
    SELECT COUNT(*) FROM t WHERE k > '1.5';   -- k INT: was missing row 2
    ```

    The bound was coerced to 2 while the strict `>` was kept, so row 2 was
    silently dropped. Any query comparing an indexed numeric column against a
    string literal — or against a scalar subquery whose result is rendered as
    text — was affected. Queries that returned too few rows now return the
    right ones.

!!! warning "Upgrading to 1.9.6"

    Three changes alter answers your client already reads, and one changes how
    the process fails. None touches your data.

    **Affected-row counts now match MySQL.** They were wrong in five shapes, and
    the 1-vs-2 distinction is how a client tells an insert from an update — so
    `updateOrCreate`-style code was reading the wrong answer and may contain a
    workaround that now reads the wrong answer in the other direction:

    | statement | was | now (and MySQL) |
    |---|---:|---:|
    | `INSERT ... ON DUPLICATE KEY UPDATE`, row updated | 1 | 2 |
    | ... set to the values it already had | 1 | 0 |
    | `REPLACE` replacing an existing row | 1 | 2 |
    | `UPDATE` that matched a row but changed nothing | 1 | 0 |

    **Search your code for anything that compensated for the old counts** before
    upgrading. Code that treats "affected == 1" as "inserted" will now see 2 for
    an update, which is the correct signal but the opposite conclusion.

    **A boolean used as a number is now an integer.** `SELECT TRUE + 1` returns
    `2` as `BIGINT` where it returned `2.0` as `DOUBLE`; likewise `(1 = 1) + 1`
    and `(2 > 1) + (3 > 2)`. Code asserting on the column type, or round-tripping
    through a typed language, sees the new type.

    **`CREATE VIEW` over an existing table name now reports 1050**
    (`ER_TABLE_EXISTS_ERROR`) instead of 1146 (`ER_NO_SUCH_TABLE`). 1146 was
    simply wrong; a migration tool that treats 1050 as "already done" will now
    behave correctly here.

    **The server aborts on panic instead of unwinding, so it must be
    supervised.** A panic is now a crash and a restart rather than a degraded
    process — which is the point: a panic that unwound while holding an internal
    lock left the server answering health checks while failing every query. The
    shipped `packaging/elyrasql.service` already sets `Restart=on-failure`. If
    you run the container or the binary under anything that does **not** restart
    it, add a restart policy before upgrading; see
    [Deployment](deployment.md#restart-policy).

!!! warning "Upgrading to 1.9.5"

    Four changes can affect a working deployment. None touches your data.

    **A partial cluster TLS configuration now refuses to start.** Setting only
    one of `ELYRASQL_CLUSTER_TLS_CERT` / `ELYRASQL_CLUSTER_TLS_KEY`, or pointing
    them at a certificate that cannot be loaded, previously logged a warning and
    continued **in plaintext**. That is now a startup error. If a node fails to
    start after upgrading, it was running unencrypted before: fix the pair rather
    than working around the error.

    **An exposed Raft control plane now requires authentication.** A control
    listener bound to anything other than loopback refuses to start unless
    `ELYRASQL_CLUSTER_SECRET` is set. To keep the previous behaviour
    deliberately, set `ELYRASQL_ALLOW_OPEN_AUTH=1`.

    **`SHOW PROCESSLIST` is scoped.** A non-`Admin` account now sees only its own
    connections. Monitoring that reads the full process list must connect as an
    `Admin` account.

    **Slow-query and audit output is redacted.** String literals are replaced with
    `?` and `CREATE USER` / `ALTER USER` / `SET PASSWORD` statements are elided
    entirely, so log pipelines that parsed literal values out of these lines will
    no longer find them. The audit log file is created owner-only (`0600`) on
    Unix.

    Two smaller behaviour changes worth knowing: `ELYRASQL_AUTH_LOCKOUT_SECS` is
    no longer read (the account lockout was removed — it let unauthenticated
    traffic lock out valid accounts), and queries that read a column-restricted
    table through a subquery, CTE or `GROUP BY` are now correctly refused where
    the restriction was previously bypassed.

!!! warning "Upgrading to 1.9.4"

    Two changes can affect a working deployment. Neither touches your data.

    **The container image has no shell.** It is now built `FROM scratch` — 22.3 MB
    down to 13.6 MB, with no OS packages and so no OS CVEs. But `docker exec
    <container> sh` no longer works, and neither does anything built on it:
    shell-based health checks, debugging one-liners, init wrappers. Check
    liveness over the MySQL protocol from outside the container instead:

    ```bash
    mysql -h 127.0.0.1 -P 3307 -u root -e 'SELECT 1'
    ```

    If you run the image under an orchestrator with an `exec`-based probe,
    change the probe before upgrading.

    **Result-column metadata reports different values.** Character columns now
    advertise their declared width under a utf8mb4 collation, matching MySQL: a
    `VARCHAR(32)` reports 128 bytes where it previously reported the unbounded
    text capacity, and clients that divide by the charset width now show 32
    instead of 21845. This is a fix — the old values were wrong in a way that
    made every client's length arithmetic wrong — but if you have code asserting
    on column lengths or collation ids, it will see new numbers. Row data,
    types and flags are unchanged.

    Tables created before 1.9.1 have no stored width and keep the unbounded
    value. Nothing needs to be rebuilt.

    Also: **`elyrasql serve` now exits on Ctrl-C.** If you deliberately started
    it with SIGINT ignored, installing the handler overrides that, and it will
    exit on a signal it used to survive.

!!! info "Upgrading to 1.9.2"

    Two fixes are the reason to upgrade promptly, because both were silent:

    - **`LEFT JOIN ... WHERE nullable IS NULL` returned the rows it should have
      excluded** when the `ON` had more than one condition. If you have results
      derived from that anti-join idiom, recompute them.
    - **`SET autocommit=0` was ignored**, so work committed immediately and
      `ROLLBACK` did nothing. Anything relying on it for transaction scope had
      no transaction.

    One change tightens validation: **`CHAR`/`VARCHAR` length limits are now
    enforced** in strict mode, so a string longer than its declared column is
    refused (22001) instead of stored. Declared types are only recorded for
    tables created from 1.9.2 onward, so existing tables are unaffected until
    recreated.

    Also worth knowing: a spilling `ORDER BY` — one over more than
    `ELYRASQL_SORT_MAX_ROWS` rows without a `LIMIT` — previously failed with an
    I/O error and now works. If you worked around that with a larger sort
    budget, you can put it back.

!!! info "Upgrading to 1.9.1"

    A patch release, but three of its fixes **tighten validation**, so statements
    that used to succeed can now fail:

    - a value too wide for its column (`300` into a `TINYINT`) raises 1264
    - a duplicate column name raises 1060, on `CREATE TABLE` and `ADD COLUMN`
    - `DELETE FROM t` on a self-referencing table without `ON DELETE CASCADE`
      raises 1451 instead of deleting the referenced rows

    Each of those matches MySQL, and each was previously accepted silently.
    Nothing is rewritten on upgrade: **integer widths are only enforced for
    tables created from 1.9.1 onward**, so an existing database cannot start
    rejecting data it already holds. A batched `INSERT` into a table with a
    self-referencing key now works, and a self-referencing `ON DELETE CASCADE`
    now follows the chain instead of leaving orphans.

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
docker pull ghcr.io/kwhorne/elyrasql:1.9.9   # or :latest
docker run -p 3307:3307 -v elyra:/var/lib/elyrasql ghcr.io/kwhorne/elyrasql:1.9.9
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
