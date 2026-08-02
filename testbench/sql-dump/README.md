# SQL dump correctness stress test

This is a developer-only, local correctness and investigation tool for
ElyraSQL. It generates a deterministic MySQL dump from a YAML model, imports
each statement into both MySQL and an ephemeral in-process ElyraSQL server, and
compares their schema metadata and typed contents.

It is not a user-facing feature, a CI gate, or a controlled performance
benchmark. Use it for broad coverage while changing SQL parsing, execution,
type coercion, schema metadata, or MySQL compatibility. When it finds a product
bug, add a focused workspace regression test as well.

## Prerequisites

- Rust 1.88 or newer.
- Docker with the Compose plugin and a running daemon.
- `just` is optional; raw equivalents are documented below.
- ShellCheck is needed for `just stress-check` or `just check-all`.

The testbench is a separate Cargo workspace. Root `cargo test --workspace` does
not build it.

## Quick start

From the repository root:

```bash
just stress
```

Without `just`:

```bash
./testbench/sql-dump/scripts/run.sh
```

The runner starts the pinned MySQL 8.4 patch image, waits for it to become
healthy, runs ElyraSQL in-process with a temporary database, and removes the
container, network, and disk-backed database volume on exit. Each invocation
uses a unique Compose project.

The runner changes into `testbench/sql-dump/` before invoking the harness.
Relative values passed to `--model` and `--artifacts` are therefore resolved
from that directory. The default model is `fixtures/car_dealership.yaml`.

The command prints the path to its `report.json`. By default, artifacts go into
a unique `artifacts/runs/<timestamp>-<pid>/` directory.

## Useful profiles

Compare a bundled schema:

```bash
just stress-schema fixtures/odoo_erp.yaml 3
```

Generate rows and compare all tables:

```bash
just stress-data
```

Equivalent raw command:

```bash
./testbench/sql-dump/scripts/run.sh \
  --model fixtures/02_relational_graph.yaml \
  --mode schema-and-data \
  --max-rows 1000 \
  --batch-size 1
```

Collect diagnostic query timings as well:

```bash
just stress-profile
```

The general recipe forwards arbitrary harness options without flattening shell
arguments:

```bash
just stress \
  --model fixtures/01_scalar_matrix.yaml \
  --mode schema-and-data \
  --max-rows 100000 \
  --timeout-seconds 300 \
  --artifacts artifacts/scalar-100k
```

Run `just stress --help` for the complete CLI.

Harness arguments override a model's generation seed, output dialect, output
mode, and batch size. This makes the selected command, rather than incidental
fixture defaults, the run contract.

## What is compared

Before either import, SQL Splitter compiles the selected model, renders directly
to `generated.sql`, and validates that dump. MySQL always executes each
statement first, so invalid generated SQL is reported as an oracle preflight
failure rather than an ElyraSQL divergence.

After import, the harness compares:

- table names and column order;
- normalized column types, signedness, nullability, defaults, generated and
  auto-increment attributes, character sets, and collation behavior;
- semantic primary, unique, and secondary indexes without relying on generated
  index names;
- foreign-key columns, referenced columns, and actions, treating MySQL's
  equivalent `RESTRICT` and `NO ACTION` spellings alike;
- row counts and order-independent typed row digests that preserve duplicate
multiplicity.

Automatically created non-unique indexes whose columns exactly match a foreign
key are omitted from the index snapshot because MySQL and ElyraSQL materialize
and name those implementation indexes differently. Other declared secondary
indexes remain part of the comparison.

Table rows are streamed while hashing. The harness retains fixed-size row
fingerprints for multiset comparison and formats only selected rows when
reporting a mismatch; it does not retain or format two complete tables at once.
Memory still grows by roughly one 32-byte fingerprint per row, plus collection
overhead.

Optional profiling warms each query, alternates database order, and verifies
order-sensitive results. Ordered-page and point-lookup samples are emitted only
when a table has a complete, non-null unique key with no prefix columns.
`elyra_speedup` is `mysql_time / elyra_time`; treat it only as a local
troubleshooting signal.

## Fixtures and scale

All checked-in YAML configurations live directly in `fixtures/`:

- `01_scalar_matrix.yaml` covers numeric, text, binary, JSON, boolean, temporal,
  null/default, Unicode, and escaping behavior.
- `02_relational_graph.yaml` covers primary and composite keys, foreign keys,
  junctions, self-references, uniqueness, JSON, and temporal planners.
- `03_commerce_graph.yaml` covers money, orders, line items, inventory,
  payments, binary payloads, enums, and relationships.
- `banking_ledger.yaml`, `car_dealership.yaml`, `cms_kitchensink.yaml`, and
  `everything.yaml` provide broader schema shapes.
- `odoo_erp.yaml` is the large-schema breadth fixture: an ERP-shaped graph with
  315 tables, 3,569 columns, and 1,376 relationships, including 28
  self-references and multiple foreign-key cycles.

These are static local stress configurations. They have no synchronization or
refresh workflow.

`--max-rows` is a cap per table, not for the whole run. Start with the smallest
profile that exercises the relevant path. Large profiles can require substantial
CPU, disk space, fingerprint memory, and time even though dump generation and
row reads are streamed.

Some broad fixtures intentionally remain useful only for selected modes:

- `cms_kitchensink.yaml` currently fails data generation because `media.md5` is
  `CHAR(32)` while its file-metadata planner emits a 64-character SHA-256 value.
- `everything.yaml --max-rows 3` violates planner minimum child and tenant
  cardinalities before generation.
- `odoo_erp.yaml` is primarily useful as a large schema profile; its data mode
  exposes known generation-verification failures at small row caps.

The relational presets use one-row inserts because ElyraSQL currently checks a
self-referencing foreign key row-by-row within a multi-row statement. Raising
`--batch-size` deliberately exposes that compatibility divergence.

## Artifacts and outcomes

Every successfully initialized run writes `report.json`, including setup,
generation, connection, timeout, import, and comparison failures.

- `generated.sql` is the exact streamed and hashed dump sent to both databases.
- `report.json` records normalized arguments, source revision and dirty state,
  executable, model, and dump hashes, host OS/architecture, validation, MySQL
  image and session environment, engine versions, timings, and a structured
  outcome.
- `failure.sql` contains the first import statement that is rejected, times out,
  or fails operationally.

SQL rejection, timeout, and infrastructure failure are distinct outcomes.
Metadata, row-count, data, and profiling divergences exit non-zero with a
diagnostic in `report.json`.

Passing `--artifacts <directory>` makes the location stable. A `.run.lock`
prevents two active runs from sharing and corrupting that directory. If a
process is killed without cleanup, remove a stale lock only after confirming no
run still owns it.

`--timeout-seconds` applies to each statement or query. A streamed table scan
uses one deadline for the complete scan rather than resetting the timeout for
each row.

## Testbench checks

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

`just check` runs the normal workspace gates. `just check-all` adds the isolated
testbench and strict documentation build without running a live differential
profile.
