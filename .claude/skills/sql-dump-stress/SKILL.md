---
name: sql-dump-stress
description: Use when running, diagnosing, extending, or reviewing ElyraSQL's local SQL dump differential stress harness under testbench/sql-dump, especially for SQL parsing, execution, type coercion, schema metadata, MySQL compatibility, or fixture changes.
---

# Use the SQL dump stress harness

Read `testbench/sql-dump/README.md` before running or changing the harness. Treat
it as the canonical command, comparison-contract, and artifact reference.

- Treat this as a developer-only local correctness and investigation tool, not a
  user-facing feature, CI gate, or controlled performance benchmark.
- Start with the smallest relevant profile. Use `just stress` for schema work,
  `just stress-data` when generated rows matter, and `just stress-profile` only
  for diagnostic timings.
- Treat every YAML file in `testbench/sql-dump/fixtures/` as a static local test
  configuration. Do not add synchronization, provenance, or refresh machinery.
- Keep runs deterministic. Prefer an existing fixture when it covers the stress
  axis; add one only when reusable coverage is missing.
- Inspect `report.json` first after failure. Use `failure.sql` for the first
  failing import statement and `generated.sql` for reproduction.
- Distinguish outcomes correctly: MySQL rejection is an oracle preflight
  failure; ElyraSQL rejection after MySQL acceptance is a compatibility
  divergence; metadata, row-count, and data mismatches are comparison
  divergences; timeouts and infrastructure failures are operational failures.
- Never present profiling ratios as benchmark evidence.
- Preserve MySQL-first execution, deterministic generation, streaming comparison
  with fixed-size row fingerprints, per-query deadlines, unique artifact
  isolation, and the separate testbench Cargo workspace when editing the harness.
- Add a focused workspace regression test when a harness run uncovers a product
  bug; do not rely on the stress fixture alone.
- Run `just stress-check` after changing the harness, fixtures, runner, or
  Compose configuration.
- Keep generated artifacts untracked and do not open or update a PR unless the
  user explicitly asks.
