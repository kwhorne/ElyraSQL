# ElyraSQL development commands. Run `just` to list them.

docker_image := env('ELYRASQL_DOCKER_IMAGE', 'elyrasql:dev')
docker_port := env('ELYRASQL_DOCKER_PORT', '3307')

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
    ./target/release/elyrasql {{ args }}

# Run all workspace tests, optionally forwarding Cargo arguments.
[group('test')]
test *args:
    cargo test --workspace --locked {{ args }}

# Run tests for one workspace crate.
[group('test')]
test-crate crate *args:
    cargo test --locked -p {{ crate }} {{ args }}

# Run the MySQL wire integration tests.
[group('test')]
test-wire:
    cargo test --locked -p elyra-server --test wire

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

# Run workspace and isolated-testbench quality gates.
[group('quality')]
check: fmt-check clippy test stress-check

# Run the SQL dump stress harness with arbitrary arguments.
[group('stress')]
stress *args:
    ./testbench/sql-dump/scripts/run.sh {{ args }}

# Compare a generated schema against MySQL.
[group('stress')]
stress-schema model='fixtures/models/car_dealership.yaml' rows='3':
    just stress --model "{{ model }}" --mode schema-only --max-rows "{{ rows }}"

# Compare generated schema and data against MySQL.
[group('stress')]
stress-data model='fixtures/local/02_relational_graph.yaml' rows='1000' batch='127':
    just stress --model "{{ model }}" --mode schema-and-data --max-rows "{{ rows }}" --batch-size "{{ batch }}"

# Compare data and collect diagnostic query timings.
[group('stress')]
stress-profile model='fixtures/local/03_commerce_graph.yaml' rows='10000' batch='1000' iterations='7':
    just stress --model "{{ model }}" --mode schema-and-data --max-rows "{{ rows }}" --batch-size "{{ batch }}" --profile-iterations "{{ iterations }}"

# Exercise the large Odoo-shaped schema model.
[group('stress')]
stress-odoo rows='3':
    just stress-schema fixtures/models/odoo_erp.yaml "{{ rows }}"

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
    docker build --tag "{{ docker_image }}" .

# Run ElyraSQL in Docker with a persistent development volume.
[group('docker')]
docker-run *args: docker-build
    docker run --rm --init --publish "127.0.0.1:{{ docker_port }}:3307" --volume elyrasql-dev:/var/lib/elyrasql "{{ docker_image }}" {{ args }}
