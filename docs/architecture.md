# Architecture

ElyraSQL is a Cargo workspace of focused crates. Third-party engines are
internal dependencies only — nothing user-facing (SQL, errors, CLI, wire
handshake) exposes their names.

```
              MySQL clients / drivers
                       │  (MySQL wire protocol)
              ┌────────▼────────┐
              │  elyra-server   │   auth, TLS, prepared statements
              └────────┬────────┘
              ┌────────▼────────┐
              │  elyra-engine   │   parse → plan → execute, sessions/txns
              └───┬────────┬────┘
      ┌───────────▼──┐  ┌──▼───────────┐  ┌──────────────┐
      │ elyra-storage│  │  elyra-olap  │  │ elyra-vector │
      │ single file  │  │  streaming   │  │  HNSW ANN    │
      │ ACID (redb)  │  │  aggregation │  │              │
      └──────────────┘  └──────────────┘  └──────────────┘
                    all share  ▲  elyra-core (types, values, errors)
```

## Crates

| Crate | Responsibility |
|-------|----------------|
| `elyra-core` | Value/type model, errors, comparison, date/decimal helpers, privileges. |
| `elyra-storage` | Single-file ACID key/value engine (redb); `Db` (group commit + concurrent reads) and MVCC `Snapshot`. |
| `elyra-engine` | SQL parsing (MySQL dialect), planning, execution, sessions/transactions, catalog, indexes. |
| `elyra-olap` | Mergeable streaming group-aggregation kernel. |
| `elyra-vector` | Vector distance metrics and the HNSW index. |
| `elyra-server` | MySQL wire protocol, auth, TLS, prepared statements. |
| `elyra-cli` | The `elyrasql` binary. |

## Storage model

Everything lives in one file, a `redb` B-tree, partitioned by key prefix:

| Prefix | Contents |
|--------|----------|
| `catalog::<table>` | table schema |
| `data::<table>::<key>` | rows, clustered on the primary key (or hidden rowid) |
| `index::<table>::<index>::…` | secondary index entries |
| `meta::…` | row-id counters, per-table write counters |

Keys use an order-preserving ("memcomparable") encoding so B-tree order matches
SQL order — including composite keys, where text components are escaped and
terminated to stay self-delimiting.

## Concurrency

- **Reads** open their own MVCC snapshot and run on a blocking pool — unlimited
  concurrent readers, no contention.
- **Writes** funnel through one dedicated writer thread that group-commits many
  pending writes into a single transaction, turning a potential write
  lock-convoy into a throughput win.
- **Transactions** take a snapshot at `BEGIN` and buffer writes in an overlay,
  giving snapshot isolation (see [Transactions](sql/transactions.md)).

## Query execution

The planner chooses per query: clustered point lookup, secondary index
(equality or range), index nested-loop / hash join, streaming scan, or the
parallel aggregation path. Result sets stream to the wire in bounded batches
where possible.
