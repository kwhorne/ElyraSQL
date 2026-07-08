# Getting Started

This guide gets you from zero to a running ElyraSQL server and your first query
in a couple of minutes.

## 1. Run the server

=== "Docker"

    ```bash
    docker run -p 3307:3307 -v elyra:/var/lib/elyrasql \
      ghcr.io/kwhorne/elyrasql:latest
    ```

=== "Static binary"

    Download the release for your architecture from the
    [releases page](https://github.com/kwhorne/ElyraSQL/releases), then:

    ```bash
    ./elyrasql serve --data elyra.edb --listen 127.0.0.1:3307
    ```

=== "From source"

    ```bash
    cargo run --release -p elyra-cli -- serve --listen 127.0.0.1:3307
    ```

The server creates the database file on first start if it does not exist.

## 2. Connect

ElyraSQL speaks the MySQL protocol, so any MySQL client works:

```bash
mysql -h 127.0.0.1 -P 3307 -u root
```

```python
import pymysql
conn = pymysql.connect(host="127.0.0.1", port=3307, user="root", password="")
```

!!! warning "Open by default"
    With no credentials configured the server accepts any login (and logs a
    warning). Set up [authentication](security.md) before exposing it.

## 3. Run some SQL

```sql
CREATE TABLE users (
    id    BIGINT PRIMARY KEY,
    name  TEXT,
    email TEXT,
    joined DATE
);

INSERT INTO users VALUES
  (1, 'Alice', 'alice@example.com', '2024-01-15'),
  (2, 'Bob',   'bob@example.com',   '2024-03-02');

CREATE INDEX users_email ON users (email);

SELECT id, name FROM users WHERE email = 'bob@example.com';
```

## 4. Try a transaction

```sql
BEGIN;
UPDATE users SET name = 'Alice B.' WHERE id = 1;
-- other connections still see the old value here
COMMIT;
```

## Next steps

- [Configuration](configuration.md) — flags, environment variables, TLS.
- [SQL Reference](sql/overview.md) — everything the query engine supports.
- [Deployment](deployment.md) — systemd and Docker in production.
