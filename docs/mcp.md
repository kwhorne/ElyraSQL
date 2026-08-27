# AI agents (MCP)

`elyrasql mcp` serves a database to an AI agent over the
[Model Context Protocol](https://modelcontextprotocol.io), speaking JSON-RPC on
stdin and stdout. The agent gets schema introspection and SQL, with no server to
run, no port to open and no driver to install.

```bash
elyrasql mcp --data app.edb
```

The database is opened **in-process**, the same way `backup` and `restore` open
it, so no server may be running against the same file at the same time.

## Registering it with a client

Most MCP clients take a command and arguments. For example:

```json
{
  "mcpServers": {
    "app-database": {
      "command": "elyrasql",
      "args": ["mcp", "--data", "/srv/app/app.edb"]
    }
  }
}
```

## Tools

| Tool | Arguments | Returns |
|------|-----------|---------|
| `list_tables` | — | The tables in the database |
| `describe_table` | `table` | Columns, types, nullability, keys, defaults |
| `query` | `sql`, optional `max_rows` | Result rows as JSON objects |

Results come back as JSON rather than a rendered table, because an agent parses
that more reliably than it parses column alignment. Numbers stay numbers;
`DECIMAL` is rendered as a string so its scale survives (`"24.50"`, not
`24.5`), which matters when the column is money.

## Read-only by default

The session runs at the `Read` privilege level, so a write is refused by the
engine, not by inspecting the SQL for dangerous-looking keywords — which is the
only way to refuse it reliably:

```
query error: access denied: statement requires WRITE privilege
```

`--allow-writes` raises the session to `Write`. That level is deliberate, not a
shorthand for "unrestricted":

| Statement | default | `--allow-writes` |
|-----------|:-------:|:----------------:|
| `SELECT` | ✅ | ✅ |
| `INSERT` / `UPDATE` / `DELETE` | ❌ | ✅ |
| `CREATE` / `ALTER` / `DROP TABLE` | ❌ | ❌ |
| `GRANT` / `CREATE USER` | ❌ | ❌ |

An agent that can be talked into deleting rows should not also be able to drop
the table they were in, or grant itself more. Schema changes and user management
require `Admin`, which this subcommand never takes.

!!! warning "It is still a live database"
    `--allow-writes` gives a language model write access to real data. Point it
    at a copy, or take a backup first — `elyrasql backup --data app.edb --out
    copy.edb` — and remember that an agent's SQL is only as careful as the
    prompt that produced it.

## Result size

`query` returns 200 rows by default, and at most 5,000 however large `max_rows`
is. A truncated result says so explicitly:

```json
{
  "row_count": 200,
  "truncated": true,
  "note": "stopped at 200 rows; add LIMIT or raise max_rows to see more"
}
```

Truncation is never silent, because a model that cannot tell a partial answer
from a complete one will reason from the partial one.

## Errors

A statement that fails comes back as a tool *result* marked `isError`, carrying
the engine's message, so the model can read what went wrong and try again:

```
catalog error: no such table: authorz
```

Only protocol-level mistakes — an unknown method, malformed JSON — return
JSON-RPC errors, because those are the client's to fix rather than the model's.

## Logging

stdout carries the protocol, so nothing else may be written to it. Log output
goes to stderr for this subcommand; `RUST_LOG` works as it does elsewhere.
