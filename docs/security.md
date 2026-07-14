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
    With no users configured, ElyraSQL accepts **any** login and logs a loud
    warning. This is for local development only. Always configure credentials
    before exposing the server.

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
CREATE USER 'app'@'%' IDENTIFIED BY 's3cret';   -- created read-only
GRANT SELECT, INSERT, UPDATE, DELETE ON *.* TO 'app';  -- promote to write
GRANT ALL PRIVILEGES ON *.* TO 'admin_user';          -- promote to admin
REVOKE ALL PRIVILEGES ON *.* FROM 'app';              -- back to read-only
SET PASSWORD FOR 'app' = 'newsecret';
SHOW GRANTS FOR 'app';
DROP USER 'app';
```

Notes and current limitations:

- New accounts start **read-only**; use `GRANT` to raise them.
- **Global** grants track the individual privileges granted as a set, so
  `GRANT`/`REVOKE ON *.*` add/remove exactly the named privileges. Revoking one
  privilege no longer collapses the account: e.g. `REVOKE INSERT` from an admin
  keeps every other privilege. `SHOW GRANTS` lists the precise set.
- Enforcement itself is still evaluated at a coarse tier (`read`/`write`/`admin`)
  derived from that set: `GRANT ALL`/`GRANT OPTION`/`SUPER` → admin; any write
  action (`INSERT`, `UPDATE`, `DELETE`, `CREATE`, ...) present → write;
  `SELECT`-only → read. So revoking one of several write privileges keeps the
  others but does not yet block *only* that one action.
- **Scope:** `GRANT ... ON *.*` (or `db.*`) sets the account's **global**
  privileges; `GRANT ... ON <table>` (or `db.table`) is a **per-table** grant
  that *raises* the tier for that table only. Reads are always allowed at the
  global baseline, so table grants are used to give a read-only account
  write/admin on specific tables. `REVOKE ON <table>` removes a table grant.
- `DROP USER` purges the account's global, per-table, per-column, and role-
  membership grants, so recreating a user with the same name does not inherit
  stale privileges.
- Enforcement is deny-safe: a write/DDL statement whose target table can't be
  determined (e.g. a multi-table `UPDATE`) requires the **global** privilege.
  `SHOW GRANTS` lists the global grant and each table grant.
- The host part of `'user'@'host'` is accepted but ignored (accounts are
  host-independent).
- Passwords are stored only as `SHA1(SHA1(password))`.
- A privilege change takes effect on the account's **next connection**.
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
  can never build a deep AST that overflows the stack. Configurable via
  `ELYRASQL_MAX_EXPR_DEPTH` (default 2000, clamped 64..5000). Wide-but-shallow
  queries (long `IN` lists, large multi-row `INSERT`s, multi-statement batches)
  are unaffected.
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
