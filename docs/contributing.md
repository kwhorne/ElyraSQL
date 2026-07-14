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

CI runs formatting, linting, build, the full test suite, a client & framework
compatibility job (Laravel/Eloquent + PyMySQL against a live server), and a
security audit. Run the core checks locally:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --workspace
```

## Test suites

The test pyramid is regression-gated in CI (`cargo test --workspace` plus a
compatibility job):

- **Unit tests** — in each crate (`cargo test -p <crate>`).
- **Wire integration tests** (`crates/elyra-server/tests/wire.rs`) — start a real
  server in-process and drive it with the independent `mysql_async` driver
  (SQL correctness, native prepared statements, auth). Run with
  `cargo test -p elyra-server --test wire`.
- **Crash recovery** (`crates/elyra-cli/tests/durability.rs`) — spawns the real
  binary, commits rows, SIGKILLs it, restarts and verifies survival. Run with
  `cargo test -p elyra-cli --test durability`.
- **Soak / chaos** (`crates/elyra-cli/tests/soak.rs`) — many concurrent
  connections run atomic transfers while a global bank invariant (total balance
  conserved, never negative) is checked continuously; a second test repeatedly
  SIGKILLs and restarts the server mid-write and re-checks the invariant after
  every crash. Short by default (runs per-PR); tune with `ELYRASQL_SOAK_SECS`,
  `ELYRASQL_SOAK_WORKERS`, `ELYRASQL_SOAK_ACCOUNTS`, `ELYRASQL_SOAK_KILL_MS`. The
  nightly `Soak / chaos` workflow runs a long version. Run with
  `cargo test -p elyra-cli --test soak`.
- **Client & framework compatibility** (`tests/compat/`) — a full Laravel/
  Eloquent workload over PDO and a PyMySQL smoke test, run against a live
  server. See `tests/compat/README.md`.

When you add or change behaviour, add a test at the lowest layer that can catch a
regression — prefer the in-process wire tests for anything protocol/SQL-visible.

## Ad-hoc end-to-end checks

Many features are also quick to eyeball against a real MySQL client. Start the
server and connect with PyMySQL or `mysql`:

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
