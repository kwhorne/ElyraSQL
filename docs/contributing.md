# Contributing

Contributions are welcome. ElyraSQL is a Cargo workspace; the layout is
described in [Architecture](architecture.md).

## Development setup

```bash
git clone https://github.com/kwhorne/ElyraSQL.git
cd ElyraSQL
cargo build
cargo test
```

## Before you push

CI runs formatting, linting, build, tests, and an end-to-end smoke test. Run
the same checks locally:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

## End-to-end testing

Many features are best validated against a real MySQL client. Start the server
and connect with PyMySQL or `mysql`:

```bash
cargo run --release -p elyra-cli -- serve --listen 127.0.0.1:3307 &
python3 - <<'PY'
import pymysql
c = pymysql.connect(host="127.0.0.1", port=3307, user="root", password="", autocommit=True)
cur = c.cursor()
cur.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)")
cur.execute("INSERT INTO t VALUES (1, 'hi')")
cur.execute("SELECT * FROM t")
print(cur.fetchall())
PY
```

## Guidelines

- Keep user-facing surfaces branded **ElyraSQL**; internal engine crate names
  should not leak into SQL, errors, the CLI, or the wire handshake.
- Prefer small, focused commits with clear messages.
- Add or update docs under `docs/` for user-visible changes.
- Be honest in docs about limitations — see [Limitations](limitations.md).

## Reporting issues

Open an issue on [GitHub](https://github.com/kwhorne/ElyraSQL/issues) with a
minimal reproduction (schema, statements, expected vs. actual).

## License

By contributing you agree that your contributions are licensed under the MIT
License.
