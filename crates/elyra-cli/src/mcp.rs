//! `elyrasql mcp` — a Model Context Protocol server over stdio.
//!
//! Points an AI agent at a database file and lets it introspect the schema and
//! run queries, without a server, a port or a driver. The engine is opened
//! in-process, the same way `backup` and `restore` open it.
//!
//! # Read-only unless told otherwise
//!
//! The session runs at [`Privilege::Read`], so writes are refused by the engine
//! itself rather than by pattern-matching the SQL — which is the only way to
//! refuse them reliably.
//!
//! `--allow-writes` raises it to `Write`, never `Admin`. That line is where it
//! is on purpose: `Write` permits `INSERT`/`UPDATE`/`DELETE`, while `DROP TABLE`,
//! schema changes and user management all still require `Admin` and stay
//! refused. An agent that can be talked into deleting rows should not also be
//! able to drop the table they were in, or grant itself more.
//!
//! # stdout belongs to the protocol
//!
//! Transport is newline-delimited JSON-RPC on stdin/stdout, so anything else
//! written to stdout corrupts the stream. Logging is redirected to stderr for
//! this subcommand in `main`.

use std::io::{BufRead, Write};

use elyra_core::{Privilege, Value};
use elyra_engine::{Engine, QueryResult, Session};
use serde_json::{json, Value as Json};

/// MCP revision this server implements.
///
/// The client sends the version it wants and we answer with one we speak; a
/// mismatch is the client's to resolve, per the spec. Bumping this is a
/// one-line change if a newer revision adds nothing we must implement.
const PROTOCOL_VERSION: &str = "2025-06-18";

/// Rows returned by `query` before the result is truncated.
///
/// An agent that asks for a million rows wants the shape of the data, not the
/// data; and the whole result would have to fit in its context regardless.
/// Truncation is always reported, never silent.
const DEFAULT_MAX_ROWS: usize = 200;

/// Hard ceiling on `max_rows`, whatever the caller asks for.
const ROW_LIMIT: usize = 5_000;

pub struct Server {
    engine: Engine,
    session: Session,
    privilege: Privilege,
}

impl Server {
    pub fn new(engine: Engine, allow_writes: bool) -> Self {
        let session = engine.session();
        Self {
            engine,
            session,
            privilege: if allow_writes {
                Privilege::Write
            } else {
                Privilege::Read
            },
        }
    }

    /// Serve until stdin closes.
    pub async fn run(&self) -> anyhow::Result<()> {
        let stdin = std::io::stdin();
        let mut stdout = std::io::stdout();
        let mut line = String::new();

        loop {
            line.clear();
            if stdin.lock().read_line(&mut line)? == 0 {
                return Ok(()); // client hung up
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let request: Json = match serde_json::from_str(trimmed) {
                Ok(v) => v,
                Err(e) => {
                    // No id to answer against, so this is the one case that uses
                    // the protocol-level error shape with a null id.
                    write_line(
                        &mut stdout,
                        &error_response(Json::Null, -32700, &format!("parse error: {e}")),
                    )?;
                    continue;
                }
            };

            // A notification has no `id` and must not be answered at all.
            let Some(id) = request.get("id").cloned() else {
                continue;
            };
            let method = request.get("method").and_then(Json::as_str).unwrap_or("");
            let params = request.get("params").cloned().unwrap_or(json!({}));

            let response = match self.dispatch(method, &params).await {
                Ok(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                Err(Failure::Protocol(code, message)) => error_response(id, code, &message),
                // A tool that ran and failed is a *result*, not a transport
                // error: the model needs to read the message and try again.
                Err(Failure::Tool(message)) => json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "isError": true,
                        "content": [{"type": "text", "text": message}],
                    }
                }),
            };
            write_line(&mut stdout, &response)?;
        }
    }

    async fn dispatch(&self, method: &str, params: &Json) -> Result<Json, Failure> {
        match method {
            "initialize" => Ok(json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": elyra_core::PRODUCT_NAME,
                    "version": elyra_core::SERVER_VERSION,
                },
                "instructions": format!(
                    "SQL access to an ElyraSQL database, MySQL-compatible. \
                     This session is {}. Start with list_tables.",
                    if self.privilege >= Privilege::Write {
                        "read-write"
                    } else {
                        "read-only"
                    }
                ),
            })),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({"tools": tool_definitions(self.privilege)})),
            "tools/call" => self.call_tool(params).await,
            other => Err(Failure::Protocol(
                -32601,
                format!("method not found: {other}"),
            )),
        }
    }

    async fn call_tool(&self, params: &Json) -> Result<Json, Failure> {
        let name = params
            .get("name")
            .and_then(Json::as_str)
            .ok_or_else(|| Failure::Protocol(-32602, "missing tool name".into()))?;
        let args = params.get("arguments").cloned().unwrap_or(json!({}));

        let text = match name {
            "list_tables" => self.sql_as_json("SHOW TABLES", DEFAULT_MAX_ROWS).await?,
            "describe_table" => {
                let table = string_arg(&args, "table")?;
                // Quoted so a table named like a keyword still resolves; the
                // engine rejects an unknown name, so this cannot reach past it.
                let quoted = table.replace('`', "``");
                self.sql_as_json(&format!("DESCRIBE `{quoted}`"), DEFAULT_MAX_ROWS)
                    .await?
            }
            "query" => {
                let sql = string_arg(&args, "sql")?;
                let max_rows = args
                    .get("max_rows")
                    .and_then(Json::as_u64)
                    .map(|n| (n as usize).min(ROW_LIMIT))
                    .unwrap_or(DEFAULT_MAX_ROWS);
                self.sql_as_json(&sql, max_rows).await?
            }
            other => return Err(Failure::Tool(format!("unknown tool: {other}"))),
        };

        Ok(json!({"content": [{"type": "text", "text": text}]}))
    }

    /// Run one statement and render its result as JSON text.
    async fn sql_as_json(&self, sql: &str, max_rows: usize) -> Result<String, Failure> {
        let mut results = self
            .engine
            .execute(sql, self.privilege, &self.session)
            .await
            .map_err(|e| Failure::Tool(e.to_string()))?;
        if results.is_empty() {
            return Ok(json!({"rows": [], "row_count": 0}).to_string());
        }

        match results.remove(0) {
            QueryResult::Rows(mut stream) => {
                let columns: Vec<String> = stream
                    .schema
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
                let mut rows = Vec::new();
                let mut truncated = false;
                loop {
                    let batch = stream
                        .next_batch(256)
                        .await
                        .map_err(|e| Failure::Tool(e.to_string()))?;
                    if batch.is_empty() {
                        break;
                    }
                    for row in batch {
                        if rows.len() >= max_rows {
                            truncated = true;
                            break;
                        }
                        let mut object = serde_json::Map::new();
                        for (name, value) in columns.iter().zip(&row) {
                            object.insert(name.clone(), value_to_json(value));
                        }
                        rows.push(Json::Object(object));
                    }
                    if truncated {
                        break;
                    }
                }
                let mut out = json!({"columns": columns, "row_count": rows.len(), "rows": rows});
                if truncated {
                    out["truncated"] = json!(true);
                    out["note"] = json!(format!(
                        "stopped at {max_rows} rows; add LIMIT or raise max_rows to see more"
                    ));
                }
                Ok(out.to_string())
            }
            QueryResult::Affected(n) => Ok(json!({"affected_rows": n}).to_string()),
            QueryResult::Insert {
                affected_rows,
                last_insert_id,
            } => Ok(json!({
                "affected_rows": affected_rows,
                "last_insert_id": last_insert_id,
            })
            .to_string()),
        }
    }
}

/// How a request failed: at the protocol level, or inside a tool.
enum Failure {
    Protocol(i64, String),
    Tool(String),
}

fn tool_definitions(privilege: Privilege) -> Vec<Json> {
    let query_description = if privilege >= Privilege::Write {
        "Run one SQL statement. This session may modify data."
    } else {
        "Run one read-only SQL statement. Writes are refused."
    };
    vec![
        json!({
            "name": "list_tables",
            "description": "List the tables in the database.",
            "inputSchema": {"type": "object", "properties": {}},
        }),
        json!({
            "name": "describe_table",
            "description": "Column names, types, nullability and keys for one table.",
            "inputSchema": {
                "type": "object",
                "properties": {"table": {"type": "string", "description": "Table name."}},
                "required": ["table"],
            },
        }),
        json!({
            "name": "query",
            "description": query_description,
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "One SQL statement, MySQL dialect."},
                    "max_rows": {
                        "type": "integer",
                        "description": "Rows to return before truncating (default 200).",
                    },
                },
                "required": ["sql"],
            },
        }),
    ]
}

/// Render a value the way an agent can use it: numbers as numbers, everything
/// else as the text a `mysql` client would print, and SQL NULL as JSON null.
fn value_to_json(value: &Value) -> Json {
    match value {
        Value::Null => Json::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) => json!(f),
        Value::Vector(v) => json!(v),
        other => match other.to_wire_string() {
            Some(s) => json!(s),
            None => Json::Null,
        },
    }
}

fn string_arg(args: &Json, name: &str) -> Result<String, Failure> {
    args.get(name)
        .and_then(Json::as_str)
        .map(str::to_string)
        .ok_or_else(|| Failure::Tool(format!("missing required argument: {name}")))
}

fn error_response(id: Json, code: i64, message: &str) -> Json {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn write_line(out: &mut std::io::Stdout, value: &Json) -> anyhow::Result<()> {
    serde_json::to_writer(&mut *out, value)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}
