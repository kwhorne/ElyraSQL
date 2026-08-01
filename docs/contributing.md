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

If you have [`just`](https://github.com/casey/just), `just check` runs these
workspace gates plus the isolated SQL dump testbench checks. Run `just` to see
the grouped build, run, test, stress, and Docker recipes. `just` is optional;
the commands above remain supported directly.

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
- **MySQL differential** (`tests/compat/differential/mysql_diff.py`) — runs an
  identical battery of edge-case queries (arithmetic, NULL/3VL, coercion, CAST,
  string/date functions, aggregates) against ElyraSQL **and a real MySQL 8** and
  fails on any non-allowlisted divergence. The `MySQL differential` workflow runs
  it in CI against a `mysql:8.4` service container; run it locally against any
  MySQL with `--ref-port`. Intentional/tracked differences are allowlisted in the
  harness with a rationale.
- **[SQL dump correctness stress test](../testbench/sql-dump/README.md)** —
  manually generates deterministic schemas and data, imports them into MySQL
  8.4 and an ephemeral ElyraSQL server, and compares metadata and typed
  contents. It is an isolated local investigation tool, not a CI gate,
  user-facing feature, or controlled performance benchmark. Run `just stress`,
  `just stress-data`, or `just stress-profile`; its guide also includes raw
  commands and artifact details.

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

## Scenario suite

Beyond the unit and wire tests, `tests/scenarios/` holds end-to-end scenarios that
run in CI (`.github/workflows/scenarios.yml`):

- **`s1_threshold_sweep.py`** replays one query battery at row counts that *bracket
  every internal threshold* (1, 2, 127, 128, 129, 255, 256, 257, 2047, 2048, 2049,
  4095, 4097, 8193) and diffs every result against a real MySQL 8.4. Three
  wrong-result bugs reached released versions because the existing tests all sat
  below those boundaries — the hash-join key only collided for integers ≥ 128, the
  spurious aggregate rows only appeared once the spill-partition path was used, and
  the `DISTINCT` inflation only appeared once parallel aggregation kicked in. **When
  adding a query shape, add it here too, not only as a small unit test.**
- **`s2_robustness.py`** asserts invariants that must hold no matter how the server
  is abused: every acknowledged commit survives `SIGKILL`, uncommitted work does
  not, concurrent transfers conserve their total, a mid-write kill leaves no torn
  transactions, and budgets and connection slots are reclaimed after exhaustion.
- **`s3_perf_security.py`** measures a performance profile against MySQL
  (informational — ratios on a shared runner are noise) and gates on security:
  per-action privilege enforcement, administrative statements being refused for
  non-admins, hostile input stored as data with byte-exact round-trip, and error
  messages leaking no internal names or paths.

Known divergences are allowlisted in `harness.py` by **exact SQL**, and each entry
must name the issue that will remove it. Exact matching is deliberate: a substring
pattern such as `"ORDER BY s"` would also hide a future bug in any unrelated query
that happens to order by that column.

Run them locally against a build under test:

```bash
cargo build --release -p elyra-cli
./target/release/elyrasql serve --data /tmp/scen.edb --listen 127.0.0.1:3400 &
cd tests/scenarios
ELYRA_PORT=3400 ELYRA_PASSWORD= MYSQL_PORT=3308 python3 s1_threshold_sweep.py
python3 s2_robustness.py 3400 /tmp/rb.edb ../../target/release/elyrasql
ELYRA_PORT=3400 python3 s3_perf_security.py
```
