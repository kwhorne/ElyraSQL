# Transactions

ElyraSQL supports explicit transactions with **snapshot isolation**.

```sql
BEGIN;                       -- or START TRANSACTION
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;                      -- or ROLLBACK to discard
```

## Semantics

- **Snapshot reads.** At `BEGIN` the transaction captures an MVCC snapshot.
  All subsequent reads see that consistent point in time — concurrent commits
  by other connections are invisible (repeatable reads).
- **Read-your-writes.** The transaction sees its own uncommitted changes.
- **Isolation.** Buffered writes are invisible to other connections until
  `COMMIT`. There are **no dirty reads**.
- **Atomicity.** `COMMIT` applies all buffered writes atomically; `ROLLBACK`
  discards them, leaving storage untouched.

## Example: isolation between connections

| Connection A | Connection B |
|--------------|--------------|
| `BEGIN` | |
| `INSERT ... (3, 30)` | |
| sees rows 1, 2, 3 | sees rows 1, 2 (not A's insert) |
| | `INSERT ... (4, 40)` (autocommit) |
| still sees 1, 2, 3 (snapshot) | sees 1, 2, 4 |
| `COMMIT` | |
| | sees 1, 2, 3, 4 |

## Autocommit

Statements outside an explicit transaction commit immediately. Each connection
has its own transaction state.

!!! warning "Known limitations"
    - There is **no write-conflict detection**: two transactions that modify
      the same row follow last-writer-wins on commit (write skew is possible).
      True serializable isolation is future work.
    - `SET autocommit=0` is accepted but not honoured; use explicit `BEGIN`.
