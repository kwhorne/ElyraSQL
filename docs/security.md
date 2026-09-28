# Security

## Authentication

ElyraSQL implements the MySQL `mysql_native_password` handshake (the default) and
`caching_sha2_password` (MySQL 8's default plugin, opt-in via
`ELYRASQL_AUTH_PLUGIN=caching_sha2_password`). Passwords are never stored in
plaintext — only `SHA1(SHA1(password))`, the same digest MySQL keeps — and each
connection uses a fresh salt.

- **`mysql_native_password`** verifies the challenge/response against the stored
  digest without reconstructing the password. Works with every MySQL client.
- **`caching_sha2_password`** runs full authentication: over TLS the client
  sends the password (protected by the TLS channel); on a plaintext connection
  the client encrypts it with the server's RSA public key (RSA-OAEP). The
  recovered cleartext is checked against the same `SHA1(SHA1(password))` digest —
  still never persisted in the clear. Prefer TLS so no RSA is involved.

Configure users on the command line:

```bash
# a single admin user
elyrasql serve --user root --password s3cret

# multiple users with roles
elyrasql serve \
  --auth admin:adminpw:admin \
  --auth app:apppw:write \
  --auth analyst:ropw:read
```

!!! danger "Open mode"
    With no users configured, ElyraSQL accepts **any** login as `Admin`. This is
    for local development only.

    **Safe by default:** the server *refuses to start* in open mode when its
    listener is bound to a non-loopback address (e.g. `0.0.0.0`, a routable IP, or
    a hostname) — the common way to accidentally expose a credential-less database.
    To run anyway you must either configure accounts (`--user`/`--password` or
    `--auth USER:PASS:ROLE`), keep the bind on `localhost` (the default), or set
    `ELYRASQL_ALLOW_OPEN_AUTH=1` to explicitly override. The default
    `127.0.0.1:3307` bind is unaffected, so local development is unchanged.

    The **replication endpoint** is likewise guarded, and more strictly: it hands
    a full copy of the database to every connecting peer, so it is **refused
    entirely** (any bind address, loopback included) unless
    `ELYRASQL_CLUSTER_SECRET` is set or `ELYRASQL_ALLOW_OPEN_AUTH=1` explicitly
    opts in. Replication authentication is **mutual**: the replica proves
    knowledge of the secret, and the primary must prove it back before the
    replica applies any snapshot or write-set — a replica refuses to start
    without a secret (same override applies), so a spoofed primary cannot feed
    it fabricated data.

    A **replica's MySQL listener** is guarded the same way: `elyrasql replica`
    refuses to start without accounts (`--user`/`--password` or
    `--auth USER:PASS:ROLE`) unless `ELYRASQL_ALLOW_OPEN_AUTH=1` explicitly
    opts in — a credential-less replica would hand Admin access to its entire
    replicated data set to anyone who can reach the port.

    **Encrypting replication:** set `ELYRASQL_CLUSTER_TLS_CERT` + `_KEY` on the
    primary and `ELYRASQL_CLUSTER_TLS_CA` on the replica. The replica then verifies
    the primary's certificate (a mismatched cert is rejected — not accept-any), so
    the stream is confidential and the primary is authenticated, while the shared
    secret authenticates the replica (mutual authentication). The **Raft control
    plane** (leader election + AppendEntries) uses the same `ELYRASQL_CLUSTER_TLS_*`
    variables and is encrypted the same way — each node presents its certificate
    and verifies its peers' certificates, so a node that cannot verify its peers
    cannot join the cluster. All inter-node traffic (consensus + replication) is
    therefore encrypted when the cluster TLS variables are set.

## Roles

Privileges are hierarchical: `read` < `write` < `admin`. The engine enforces the
minimum privilege per statement.

| Role | May run |
|------|---------|
| `read` | `SELECT`, transactions, session commands |
| `write` | the above + `INSERT`, `UPDATE`, `DELETE` |
| `admin` | the above + DDL (`CREATE`, `DROP`, `ALTER`, `CREATE INDEX`) |

A denied statement returns an access-denied error and is not executed.

## Managing users with SQL

Besides the startup `--auth` flags (which define bootstrap accounts that always
work), accounts can be created at runtime and are **persisted in the database
file**, so they survive restarts:

```sql
CREATE USER 'app'@'%' IDENTIFIED BY 's3cret';   -- no privileges yet (USAGE)
GRANT SELECT ON *.* TO 'app';                         -- read every table
GRANT SELECT, INSERT, UPDATE, DELETE ON *.* TO 'app';  -- read and write
GRANT SELECT ON orders TO 'report';                   -- one table only
GRANT SELECT (id, total) ON orders TO 'auditor';      -- two columns only
GRANT ALL PRIVILEGES ON *.* TO 'admin_user';          -- promote to admin
REVOKE ALL PRIVILEGES ON *.* FROM 'app';              -- back to none
SET PASSWORD FOR 'app' = 'newsecret';
SHOW GRANTS FOR 'app';
DROP USER 'app';
```

Notes and current limitations:

- New accounts start with **no privileges** (`USAGE`), as in MySQL: they can
  connect and run statements that touch no table, and read or write a table
  only once granted to. Accounts created before 1.12 have no stored privilege
  set and keep the global `SELECT` that was the default then; `REVOKE SELECT ON
  *.* FROM 'u'` brings one in line. Accounts configured at startup (`--auth`,
  `--user`) are governed by their configured tier.
- **Reads are checked per table**, in every statement that reads -- a `SELECT`
  and its subqueries and set operations, the source of an `INSERT ... SELECT`,
  the tables of an `UPDATE` or `DELETE` (including its own table when a `WHERE`
  or computed value reads it, as in MySQL). A table is readable with `SELECT`
  granted globally (`*.*`, `db.*`) or on that table, or -- for the granted
  columns only -- with column grants on it.
- **Global** grants track the individual privileges granted as a set, so
  `GRANT`/`REVOKE ON *.*` add/remove exactly the named privileges. Revoking one
  privilege no longer collapses the account: e.g. `REVOKE INSERT` from an admin
  keeps every other privilege. `SHOW GRANTS` lists the precise set.
- **DML privileges are enforced per action, per table.** `INSERT`, `UPDATE` and
  `DELETE` are checked individually against the target table's effective grant
  (global ∪ per-table ∪ role-inherited), so a user granted only `INSERT` cannot
  `UPDATE`/`DELETE`, and revoking one write privilege blocks *only* that action.
  Administrative statements and DDL (`CREATE`/`DROP`/`ALTER`/`CREATE INDEX`,
  triggers, procedures, `BACKUP`, `LOAD DATA`, ...) are gated at the `admin`
  tier (`GRANT ALL`/`GRANT OPTION`/`SUPER`).
- **Scope:** `GRANT ... ON *.*` (or `db.*`) sets the account's **global**
  privileges; `GRANT ... ON <table>` (or `db.table`) is a **per-table** grant of
  exactly the named privileges on that table only (stored as a privilege set, so
  `REVOKE ... ON <table>` removes just those). `REVOKE ON <table>` removes a
  table grant.
- **Column grants** (`GRANT SELECT (a, b) ON t`) allow reading those columns of
  `t` through a plain single-table `SELECT`; `t` in a join, subquery, set
  operation or `INSERT ... SELECT` is refused, since which columns it reads
  there cannot be verified. With `SELECT` on `t` itself as well, `t` is fully
  readable, as in MySQL. With only a global `SELECT`, column grants still
  restrict `t` (stricter than MySQL, which would allow every column), because
  that is how accounts that had read access by default use them.
- `DROP USER` purges the account's global, per-table, per-column, and role-
  membership grants, so recreating a user with the same name does not inherit
  stale privileges.
- Enforcement is deny-safe: a write/DDL statement whose target table can't be
  determined (e.g. a multi-table `UPDATE`) requires the **global** privilege.
  `SHOW GRANTS` lists the global grant and each table grant.
- The host part of `'user'@'host'` is accepted but ignored (accounts are
  host-independent).
- Passwords are stored only as `SHA1(SHA1(password))`.
- A privilege change takes effect on the account's **next statement**, on every
  connection -- replicas included.
- Managing users requires the **admin** privilege. Creating the first account
  (in an otherwise open/dev server) turns authentication on for subsequent
  connections — keep a bootstrap `--auth` admin so you don't lock yourself out.

## TLS

Provide a PEM certificate and key to enable TLS (rustls 0.23). Clients that
request SSL are upgraded to an encrypted connection; others continue in
plaintext.

```bash
elyrasql serve --tls-cert server.crt --tls-key server.key
```

Generate a self-signed certificate for testing. rustls requires an X.509 **v3**
certificate, so include a `subjectAltName` (a bare `-subj` alone produces a v1
certificate rustls will reject):

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout server.key -out server.crt -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p --ssl-mode=REQUIRED
```

## Resource limits (denial-of-service)

ElyraSQL bounds the recursion an untrusted query can trigger, so a single hostile
statement cannot exhaust the worker-thread stack and abort the process:

- **Expression depth.** Deeply-nested expressions of any shape — arithmetic/
  boolean/bitwise chains (`1+1+1...`, huge `OR` chains), parentheses and function
  nesting, JSON `->`/`->>` chains, and postfix subscript/call chains
  (`x[0][0]...`) — are rejected with a normal SQL error *before* parsing, so they
  can never build a deep AST that overflows the stack. The ceiling is
  `ELYRASQL_MAX_EXPR_DEPTH` (default 2000, clamped 64..5000); the effective
  expression-nesting limit is the smaller of it and what the calling thread's
  stack can hold, so a deep expression is refused rather than aborting the
  process on a small stack (an `elyra-embed` host thread or a test thread — the
  16 MiB server workers keep the full ceiling). Wide-but-shallow queries (long
  `IN` lists, large multi-row `INSERT`s, multi-statement batches) are unaffected.
- **JSON nesting.** JSON documents are parsed to a maximum nesting depth of 200
  (both on write and when read by JSON functions); a deeper document is treated as
  invalid JSON rather than crashing.
- Other resource bounds: `ELYRASQL_TXN_MAX_BYTES` (uncommitted transaction size),
  `ELYRASQL_SORT_MAX_ROWS` / `ELYRASQL_GROUP_MAX_GROUPS` (spill thresholds),
  `ELYRASQL_MAX_FRAME_MB` (max network/binlog/spill frame). See
  [Configuration](configuration.md).

To report a vulnerability, use GitHub's private vulnerability reporting on the
repository (Security tab); see `SECURITY.md`.

## Hardening checklist

- [ ] Configure `--user`/`--password` or `--auth` (never run open in production).
- [ ] Enable TLS with a real certificate.
- [ ] Bind to a private interface or firewall the port; only bind `0.0.0.0`
      when intended.
- [ ] Run under the dedicated `elyrasql` system user (the systemd unit does).
- [ ] Grant each application the least privilege it needs.
