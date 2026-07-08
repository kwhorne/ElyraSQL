# Contributing to ElyraSQL

Thanks for your interest in improving ElyraSQL! This document explains how to
set up, make changes, and get them merged.

By participating you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md), and your contributions are licensed under
the project's [MIT License](LICENSE).

## Ways to contribute

- **Report bugs** — open an issue with a minimal reproduction.
- **Request features** — describe the problem you want solved, not just a
  solution.
- **Improve docs** — everything under `docs/` and the crate rustdoc.
- **Write code** — fixes, features, tests, performance work.

Before starting significant work, please open an issue to discuss the design —
it saves everyone time.

## Project layout

ElyraSQL is a Cargo workspace. See
[docs/architecture.md](docs/architecture.md) for the full picture.

| Crate | Responsibility |
|-------|----------------|
| `elyra-core` | value/type model, errors, comparison, date/decimal, privileges |
| `elyra-storage` | single-file ACID engine (redb), `Db`, MVCC `Snapshot` |
| `elyra-engine` | parse → plan → execute, sessions/transactions, catalog, indexes |
| `elyra-olap` | streaming group-aggregation kernel |
| `elyra-vector` | vector metrics + HNSW index |
| `elyra-server` | MySQL wire protocol, auth, TLS, prepared statements |
| `elyra-cli` | the `elyrasql` binary |

## Development setup

Requires Rust 1.82+.

```bash
git clone https://github.com/kwhorne/ElyraSQL.git
cd ElyraSQL
cargo build
cargo test
```

## Before you open a pull request

CI runs formatting, linting, build, tests, and an end-to-end smoke test. Run
the same locally — they must pass:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --workspace
cargo test --workspace
```

### End-to-end testing

Many features are best validated against a real MySQL client. Start the server
and connect with PyMySQL or the `mysql` CLI:

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

## Coding guidelines

- **Branding.** Keep user-facing surfaces branded **ElyraSQL**. Internal engine
  crate/dependency names (redb, sqlparser, opensrv, …) must not leak into SQL,
  error messages, the CLI, or the wire handshake.
- **Correctness first.** Prefer a correct, simple implementation over a fast,
  subtly wrong one. Add fast paths deliberately and keep a correct fallback.
- **Bounded memory.** Favor streaming/batched processing over materializing
  whole tables where practical.
- **Be honest in docs.** If a feature has limits, document them (see
  [docs/limitations.md](docs/limitations.md)). Do not overclaim.
- **Tests.** Add unit tests for kernels (encoding, aggregation, HNSW, …) and,
  for user-visible behavior, an end-to-end check against the MySQL protocol.
- **Docs.** Update `docs/` for any user-visible change.

## Commit and PR conventions

- Write focused commits with clear, imperative messages
  (e.g. `Add range index scans`).
- Explain the *why* in the body when it isn't obvious.
- Keep PRs scoped to one logical change; open separate PRs for unrelated work.
- Fill out the pull request template and link the issue it addresses.
- Rebase on `main` and ensure CI is green.

## Reporting security issues

Please do **not** open public issues for vulnerabilities. See
[SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions are licensed under the MIT
License, and that you have the right to submit them.
