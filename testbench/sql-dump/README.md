# SQL dump correctness stress test

This is a developer-only, local stress tool for ElyraSQL. It generates a
deterministic MySQL dump from a YAML model, imports every statement through the
MySQL wire protocol into both MySQL 8.4 and an ephemeral in-process ElyraSQL
server, then compares schema metadata, row counts, and typed table contents.

It is not part of CI, a user-facing ElyraSQL feature, or a controlled performance
benchmark. Use it when changing SQL parsing, execution, type coercion, schema
metadata, or MySQL compatibility and you want broader coverage than a focused
regression test provides.

## Prerequisites

- Rust 1.88 or newer.
- Docker with the Compose plugin and a running Docker daemon.
- `just` is optional; every recipe below has a raw command equivalent.
- ShellCheck is needed only for `just stress-check` and `just check`.

The testbench is intentionally a separate Cargo workspace. Root-level
`cargo test --workspace` does not build or run it.

## Quick start

From the repository root, run the default schema comparison:

```bash
just stress
```

Without `just`:

```bash
./testbench/sql-dump/scripts/run.sh
```

The runner starts a fresh MySQL 8.4 container, waits for it to become healthy,
runs ElyraSQL in-process on an ephemeral port with a temporary database, and
removes the MySQL container, network, and temporary volume when it exits. Each
invocation uses a unique Compose project, so concurrent runs remain isolated.

The default model is `fixtures/models/car_dealership.yaml`. Schema-only mode
still makes SQL Splitter plan rows before rendering, so the default cap of three
rows avoids needless generation while retaining all 26 tables and 217 columns.

## Common stress profiles

Compare another bundled schema:

```bash
just stress-schema fixtures/models/odoo_erp.yaml 3
```

Generate data and compare every row using typed, order-independent digests:

```bash
just stress-data
```

The equivalent raw command is:

```bash
./testbench/sql-dump/scripts/run.sh \
  --model fixtures/local/02_relational_graph.yaml \
  --mode schema-and-data \
  --max-rows 1000 \
  --batch-size 127
```

Add warmed diagnostic timings for `COUNT(*)`, an ordered 100-row page, and a
point lookup on each table:

```bash
just stress-profile
```

These timings alternate query order and verify both systems returned the same
rows. `elyra_speedup` is `mysql_time / elyra_time`, so values above `1.0` mean
ElyraSQL was faster in that run. The results are troubleshooting signals, not
stable benchmark numbers.

Pass arbitrary harness options through the general recipe:

```bash
just stress \
  --model fixtures/local/01_scalar_matrix.yaml \
  --mode schema-and-data \
  --max-rows 100000 \
  --artifacts artifacts/scalar-100k
```

Run `just stress --help` for every option.

## Models and scale

- `fixtures/local/01_scalar_matrix.yaml` targets exact decimals, signed and
  unsigned integers, floating point, text and binary values, JSON, booleans,
  temporal values, nullability, defaults, Unicode, and escaping.
- `fixtures/local/02_relational_graph.yaml` targets foreign keys, composite
  keys, junctions, self-referential hierarchies, unique values, JSON, temporal
  planners, and multiple relationship shapes.
- `fixtures/local/03_commerce_graph.yaml` combines money, orders, line items,
  inventory, payments, binary payloads, JSON, enums, and relationships.
- `fixtures/models/` contains additional broad models, including the
  315-table Odoo-shaped ERP schema.

`--max-rows` is a cap per table, not for the entire run. The three local models
have been exercised at 100,000 generated rows per table: 100,000 scalar rows,
700,000 relational rows, and 600,000 commerce rows. The car-dealership model has
been exercised at 1,000 rows per table. Large runs collect each table's rows for
typed comparison, so use a dedicated artifact directory and allow substantial
time and memory.

## Artifacts and failures

The artifact directory defaults to `testbench/sql-dump/artifacts/latest/` and is
ignored by Git. Each run replaces its own `generated.sql`, `report.json`, and
`failure.sql` files so stale failures cannot be mistaken for current ones.

- `generated.sql` is the exact dump sent to both databases.
- `report.json` records arguments, model and dump hashes, generator diagnostics,
  validation results, engine versions, import timings, optional query profiles,
  and the final comparison outcome.
- `failure.sql` is written when MySQL or ElyraSQL rejects an import statement.

MySQL always executes a statement first. A MySQL rejection is an oracle
preflight failure rather than an ElyraSQL divergence. If MySQL accepts the
statement and ElyraSQL rejects it, the report and `failure.sql` identify the
first divergent statement. Metadata, row-count, data, and profiling failures
include a specific diagnostic in `report.json` and make the command exit
non-zero. Generation validation errors point to the retained `generated.sql`.

## Known model preflight results

- `cms_kitchensink.yaml` fails generation verification because `media.md5` is
  `CHAR(32)` while its file-metadata planner produces a 64-character SHA-256
  value by default.
- `everything.yaml --max-rows 3` violates planner minimum child counts and
  tenant cardinality before generation.
- `odoo_erp.yaml` passes schema validation and imports all 315 tables and 3,569
  columns into both databases. Its data mode currently produces 177 generation
  verification failures at seed 42 with a cap of three rows.

Treat those as known model/generator constraints. Use deterministic local model
changes when a failing data mode needs to become a correctness gate.

## Testbench quality checks

```bash
just stress-check
```

Without `just`:

```bash
cargo fmt --manifest-path testbench/sql-dump/Cargo.toml --all -- --check
cargo clippy --manifest-path testbench/sql-dump/Cargo.toml \
  --locked --all-targets -- -D warnings
cargo test --manifest-path testbench/sql-dump/Cargo.toml --locked
shellcheck testbench/sql-dump/scripts/run.sh
docker compose --file testbench/sql-dump/compose.yaml config --quiet
```
