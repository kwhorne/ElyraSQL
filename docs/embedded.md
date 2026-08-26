# Embedded (in-process)

ElyraSQL runs as a library inside your own process, with no server, no socket
and no MySQL wire protocol. You open the `.edb` file directly and execute SQL
against it.

The SQL semantics are identical to the server's, because it is the same engine:
`elyra-engine` depends on the storage layer and on nothing above it, so the
server and the embedded library differ only in what wraps the engine. A file
written in-process opens in `elyrasql serve`, and a file the server wrote opens
in-process — the same file, either way.

```rust
use elyra_embed::{Database, Value};

let db = Database::open("app.edb")?;
let conn = db.connect();

conn.execute("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT)")?;
conn.execute("INSERT INTO users (name) VALUES ('Ada')")?;

let rows = conn.query("SELECT name FROM users")?;
assert_eq!(rows.get(0, "name"), Some(&Value::Text("Ada".into())));
```

## When to use it

Embedded mode suits the cases where a server is overhead rather than
infrastructure:

- **Test suites.** Real MySQL semantics with no container to start, no port to
  allocate and no fixture to tear down. `Database::temporary()` gives a fresh
  database per test, deleted when the handle drops.
- **Local development.** The same file format as production, so a database can
  be copied between the two without conversion.
- **CLI tools and desktop applications** that need SQL but not a service.
- **Edge and single-tenant deployments** where one process owns its data.

Keep the server where you need what a server gives you: concurrent clients from
other processes, replication, network access control, or any existing MySQL
client and ORM talking over the wire.

## One writer per file

The database file is locked exclusively, so exactly one process holds it at a
time — the same single-writer rule SQLite follows. Within that process, a
`Database` is shared freely across threads, and each unit of work takes its own
`connect()` for its own session state.

Opening a file another live handle holds fails immediately. Opening one whose
handle has just closed waits briefly first: the storage writer runs on a
detached thread that releases the file lock a moment after the handle is
dropped, so an immediate reopen would otherwise race it. `Config::lock_wait`
sets that budget, and `Some(Duration::ZERO)` turns it off for a caller who wants
to *detect* a conflict rather than wait one out.

## Everything blocks

The engine is async internally. `elyra-embed` owns a Tokio runtime and drives it,
so every method returns a plain value and callers need no runtime of their own.

The trade-off is that these methods cannot be called from inside an async
context — blocking on a runtime that is already entered on the thread would
deadlock, which Tokio reports by panicking. Every entry point checks for an
ambient runtime and returns an error instead. Async callers should use
`elyra-engine` directly; it is a normal async API and the facade adds nothing
they need.

## Reading values

`Value` carries the engine's own representation: `Decimal` is an unscaled
integer and a scale, `Date` is a day count. Match on it when you want the typed
value, and call `to_wire_string()` when you want the text a `mysql` client would
print — that is the exact rendering the server sends, with `None` for SQL NULL.

```rust
for row in rows.iter() {
    let text: Vec<String> = row.iter()
        .map(|v| v.to_wire_string().unwrap_or_else(|| "NULL".into()))
        .collect();
    println!("{}", text.join("\t"));
}
```

## From C, and from languages with an FFI

`elyra-embed-capi` exposes the same API over a C ABI, as a shared or static
library, for hosts that reach Rust through FFI — PHP, Python, Node, Ruby, Go.

```bash
cargo build -p elyra-embed-capi --release
cc app.c -I crates/elyra-embed-capi/include -L target/release -lelyrasql -o app
```

```c
#include "elyrasql.h"

ElyraDb *db = NULL;
if (elyra_db_open("app.edb", &db) != ELYRA_OK) {
    fprintf(stderr, "%s\n", elyra_last_error());
    return 1;
}

ElyraConn *conn = NULL;
elyra_db_connect(db, &conn);
elyra_conn_execute(conn, "INSERT INTO users (name) VALUES ('Ada')", NULL);

ElyraRows *rows = NULL;
elyra_conn_query(conn, "SELECT name FROM users", &rows);
for (size_t r = 0; r < elyra_rows_count(rows); r++) {
    printf("%s\n", elyra_rows_value(rows, r, 0));
}

elyra_rows_free(rows);
elyra_conn_free(conn);
elyra_db_free(db);
```

Fallible calls return `ELYRA_OK` or `ELYRA_ERR`, and `elyra_last_error()` carries
the message for the last failure on the calling thread. Handles are opaque and
each has a matching `*_free`; freeing `NULL` is a no-op, so cleanup paths need no
guard. Strings borrow from the handle they came from and are never freed
separately. A Rust panic cannot cross the boundary — the entry points abort
rather than unwind into the host.

A complete, compilable walkthrough lives in
`crates/elyra-embed-capi/examples/basic.c`.

!!! note "SQL NULL and out-of-range both read as `NULL`"
    `elyra_rows_value` returns `NULL` for a SQL NULL *and* for an index past the
    end of the result. Call `elyra_rows_is_null`, which answers 1, 0 or -1
    respectively, when the difference matters.

## What is not available in-process

- **Replication.** A replica connects to a primary over the network; the
  embedded library has no listener and no peer.
- **Accounts and grants as a security boundary.** `connect_as` exists so an
  application can exercise its own grant model, but a caller who holds the
  database file can open a second connection at any privilege level. In-process,
  privileges are a testing tool, not a control.
- **Concurrent access from other processes**, by the one-writer rule above.

## Limitations inherited from the server

Everything in [Limitations](limitations.md) applies unchanged, including the
single `elyra` database: `CREATE DATABASE` is rejected, and `USE` selects among
the schemas that exist rather than creating one.
