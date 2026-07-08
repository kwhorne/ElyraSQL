# Security

## Authentication

ElyraSQL implements the MySQL `mysql_native_password` handshake. Passwords are
never stored in plaintext — only `SHA1(SHA1(password))`, the same digest MySQL
keeps — and are verified via the challenge/response without reconstruction. Each
connection uses a fresh salt.

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

## TLS

Provide a PEM certificate and key to enable TLS. Clients that request SSL are
upgraded to an encrypted connection; others continue in plaintext.

```bash
elyrasql serve --tls-cert server.crt --tls-key server.key
```

Generate a self-signed certificate for testing:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout server.key -out server.crt -subj "/CN=localhost"
```

```bash
mysql -h 127.0.0.1 -P 3307 -u root -p --ssl-mode=REQUIRED
```

## Hardening checklist

- [ ] Configure `--user`/`--password` or `--auth` (never run open in production).
- [ ] Enable TLS with a real certificate.
- [ ] Bind to a private interface or firewall the port; only bind `0.0.0.0`
      when intended.
- [ ] Run under the dedicated `elyrasql` system user (the systemd unit does).
- [ ] Grant each application the least privilege it needs.
