# ElyraSQL development commands. Run `just` to list them.

set shell := ["bash", "-euo", "pipefail", "-c"]
set positional-arguments

docker_image := env('ELYRASQL_DOCKER_IMAGE', 'elyrasql:dev')
docker_port := env('ELYRASQL_DOCKER_PORT', '3307')
docker_publish := '127.0.0.1:' + docker_port + ':3307'

[private]
default:
    @just --list

# Build the Rust workspace.
[group('build')]
build:
    cargo build --workspace --locked

# Build the optimized Rust workspace.
[group('build')]
release:
    cargo build --workspace --release --locked

# Build and run the native ElyraSQL binary. Defaults to `serve`.
[group('run')]
run +args='serve': release
    ./target/release/elyrasql "$@"

# Run all workspace tests via nextest (faster). Install with:
#   cargo install cargo-nextest --locked
[group('test')]
test *args:
    cargo nextest run --workspace --locked "$@"

# Run tests for one workspace crate.
[group('test')]
test-crate crate *args:
    crate="$1"; shift; cargo nextest run --locked -p "$crate" "$@"

# Run the MySQL wire integration tests.
[group('test')]
test-wire:
    cargo nextest run --locked -p elyra-server -E 'test(/wire/)'

# Format Rust code.
[group('quality')]
fmt:
    cargo fmt --all

# Check Rust formatting without changing files.
[group('quality')]
fmt-check:
    cargo fmt --all --check

# Run Clippy with warnings denied.
[group('quality')]
clippy:
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

# Run the normal workspace quality gates.
[group('quality')]
check: fmt-check clippy test

# Run workspace, documentation, and isolated-testbench quality gates.
[group('quality')]
check-all: check stress-check docs-check

# Build the documentation with strict link and warning checks.
[group('quality')]
docs-check:
    mkdocs build --strict

# Run the SQL dump stress harness with arbitrary arguments.
[group('stress')]
stress *args:
    ./testbench/sql-dump/scripts/run.sh "$@"

# Compare a generated schema against MySQL.
[group('stress')]
stress-schema model='fixtures/car_dealership.yaml' rows='3':
    ./testbench/sql-dump/scripts/run.sh --model "$1" --mode schema-only --max-rows "$2"

# Compare generated schema and data against MySQL.
[group('stress')]
stress-data model='fixtures/02_relational_graph.yaml' rows='1000' batch='1':
    ./testbench/sql-dump/scripts/run.sh --model "$1" --mode schema-and-data --max-rows "$2" --batch-size "$3"

# Compare data and collect diagnostic query timings.
[group('stress')]
stress-profile model='fixtures/02_relational_graph.yaml' rows='100' batch='1' iterations='7':
    ./testbench/sql-dump/scripts/run.sh --model "$1" --mode schema-and-data --max-rows "$2" --batch-size "$3" --profile-iterations "$4"

# Exercise the large Odoo-shaped schema model.
[group('stress')]
stress-odoo rows='3':
    ./testbench/sql-dump/scripts/run.sh --model fixtures/odoo_erp.yaml --mode schema-only --max-rows "$1"

# Check the isolated stress-test workspace and its runner.
[group('stress')]
stress-check:
    cargo fmt --manifest-path testbench/sql-dump/Cargo.toml --all -- --check
    cargo clippy --manifest-path testbench/sql-dump/Cargo.toml --locked --all-targets -- -D warnings
    cargo test --manifest-path testbench/sql-dump/Cargo.toml --locked
    shellcheck testbench/sql-dump/scripts/run.sh
    docker compose --project-name elyra-sql-dump-config-check --file testbench/sql-dump/compose.yaml config --quiet

# Build the local development image.
[group('docker')]
docker-build:
    docker build --tag {{ quote(docker_image) }} .

# Run ElyraSQL in Docker with a persistent development volume.
[group('docker')]
docker-run *args: docker-build
    docker run --rm --init --publish {{ quote(docker_publish) }} --volume elyrasql-dev:/var/lib/elyrasql {{ quote(docker_image) }} "$@"
