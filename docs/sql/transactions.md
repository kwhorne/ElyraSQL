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
- **Write-conflict detection.** On `COMMIT`, ElyraSQL verifies that every row
  the transaction wrote is unchanged since its snapshot. If another transaction
  committed a change first, the commit is **rejected** with a serialization
  failure (MySQL error `1213`) and the transaction is aborted
  (first-committer-wins). Under `SERIALIZABLE`, read rows and scanned ranges are
  validated too (see below).

```sql
-- If another connection commits a change to id = 1 first, this COMMIT fails
-- with error 1213 and must be retried.
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
COMMIT;
```

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

## Savepoints

```sql
BEGIN;
INSERT INTO t VALUES (1);
SAVEPOINT sp1;
INSERT INTO t VALUES (2);
ROLLBACK TO SAVEPOINT sp1;   -- undoes row 2, keeps row 1 and sp1
RELEASE SAVEPOINT sp1;       -- forgets sp1
COMMIT;
```

## Row locking

```sql
BEGIN;
SELECT balance FROM accounts WHERE id = 1 FOR UPDATE;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
COMMIT;
```

`SELECT ... FOR UPDATE` / `FOR SHARE` use **optimistic** locking: the locked
rows are validated at commit, so if another transaction changed one of them the
commit fails with error `1213` (retry). There is no pessimistic blocking, and
`LOCK IN SHARE MODE` is not parsed (use `FOR SHARE`).

## Isolation levels

The default is **snapshot** isolation (with first-committer-wins write-conflict
detection). A stronger **serializable** level is available per connection:

```sql
SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN;
-- ...
COMMIT;   -- may fail with error 1213 if a read row or scanned range changed
```

| Level | Guarantees |
|-------|-----------|
| `SNAPSHOT` (default) | Snapshot reads, no dirty reads, no lost updates. Write skew possible. |
| `SERIALIZABLE` | Additionally validates the **read set** and **scanned ranges** at commit, preventing write skew and phantoms. |

Under `SERIALIZABLE`, a commit is rejected (error `1213`) if any row the
transaction read, or any range it scanned, changed since its snapshot. This
costs more aborts under contention; retry the transaction.

Other levels (`READ COMMITTED`, `REPEATABLE READ`) are accepted and mapped to
snapshot isolation.

!!! note
    `SET autocommit=0` is accepted but not honoured; use explicit `BEGIN`.
