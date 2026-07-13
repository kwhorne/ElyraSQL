# Compatibility harnesses

Black-box compatibility tests that drive a **live ElyraSQL server** with real,
independent client stacks. They complement the in-process Rust integration tests
(`crates/elyra-server/tests/wire.rs`) by exercising the exact drivers real apps
use. All run automatically in the `compatibility` CI job.

## Laravel / Eloquent (PHP, PDO)

A realistic Eloquent workload: migrations (Schema builder), models,
relationships, the query builder, transactions and delete/cascade — over PDO
with emulated prepared statements, as a stock Laravel app is configured.

```bash
cd tests/compat/laravel
composer install
# start ElyraSQL on 127.0.0.1:3307 with database "elyra" first, then:
ELYRASQL_PORT=3307 php eloquent_test.php
```

## PyMySQL (Python)

A pure-Python driver (client-side parameter binding) smoke test.

```bash
pip install pymysql
ELYRASQL_PORT=3307 python3 tests/compat/python/pymysql_smoke.py
```

Both read `ELYRASQL_HOST`, `ELYRASQL_PORT`, `ELYRASQL_DB`, `ELYRASQL_USER`,
`ELYRASQL_PASS` from the environment.
