use crate::{config::ProjectConfig, database, status};
use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::{
    io::{BufRead, Write},
    path::{Path, PathBuf},
};

pub fn run_stdio(root: PathBuf) -> Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = serde_json::from_str(&line)?;
        if request.get("id").is_none() {
            continue;
        }
        let response = dispatch(&root, request);
        serde_json::to_writer(&mut stdout, &response)?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}

pub fn dispatch(root: &Path, request: Value) -> Value {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let method = request.get("method").and_then(Value::as_str).unwrap_or("");
    let result = match method {
        "initialize" => Ok(json!({
            "protocolVersion": "2025-03-26",
            "capabilities": {"tools": {"listChanged": false}},
            "serverInfo": {"name": "streamling-blockchain", "version": env!("CARGO_PKG_VERSION")}
        })),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(tool_list()),
        "tools/call" => call_tool(
            root,
            request
                .pointer("/params/name")
                .and_then(Value::as_str)
                .unwrap_or(""),
            request
                .pointer("/params/arguments")
                .cloned()
                .unwrap_or_else(|| json!({})),
        ),
        _ => Err(anyhow::anyhow!("unknown MCP method: {method}")),
    };
    match result {
        Ok(value) => json!({"jsonrpc": "2.0", "id": id, "result": value}),
        Err(error) => {
            json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32000, "message": error.to_string()}})
        }
    }
}

fn tool_list() -> Value {
    json!({"tools": [
        {
            "name": "streamling_blockchain_schema",
            "description": "Read the live SQLite schema plus governed semantic descriptions before writing SQL.",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}
        },
        {
            "name": "streamling_blockchain_query",
            "description": "Run read-only SQL against locally indexed EVM event tables.",
            "inputSchema": {"type": "object", "properties": {
                "sql": {"type": "string"}, "max_rows": {"type": "integer", "minimum": 1, "maximum": 1000}
            }, "required": ["sql"], "additionalProperties": false}
        },
        {
            "name": "streamling_blockchain_status",
            "description": "Report indexed and target block heights, remaining blocks, catch-up state, contracts, and local database state.",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}
        }
    ]})
}

fn call_tool(root: &Path, name: &str, arguments: Value) -> Result<Value> {
    let payload = match name {
        "streamling_blockchain_schema" => {
            let config = ProjectConfig::load(root)?;
            let conn = database::open(&config.absolute_database(root))?;
            let semantics = std::fs::read_to_string(root.join("semantic.toml")).unwrap_or_default();
            json!({"schema": database::schema(&conn)?, "semantics": semantics})
        }
        "streamling_blockchain_query" => {
            let sql = arguments
                .get("sql")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("sql is required"))?;
            ensure_read_only(sql)?;
            let max_rows = arguments
                .get("max_rows")
                .and_then(Value::as_u64)
                .unwrap_or(200)
                .min(1000) as usize;
            let config = ProjectConfig::load(root)?;
            let conn = database::open(&config.absolute_database(root))?;
            database::query(&conn, sql, max_rows)?
        }
        "streamling_blockchain_status" => status::cached(root)?,
        _ => bail!("unknown tool: {name}"),
    };
    Ok(json!({"content": [{"type": "text", "text": serde_json::to_string_pretty(&payload)?}]}))
}

pub fn ensure_read_only(sql: &str) -> Result<()> {
    let keyword = sql
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if !matches!(keyword.as_str(), "select" | "with" | "pragma" | "explain") {
        bail!("MCP and HTTP query surfaces accept read-only SQL")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mcp_sql_is_read_only() {
        assert!(ensure_read_only(" SELECT 1").is_ok());
        assert!(ensure_read_only("WITH x AS (SELECT 1) SELECT * FROM x").is_ok());
        assert!(ensure_read_only("DELETE FROM events").is_err());
    }

    #[test]
    fn advertises_only_live_tools() {
        let tools = tool_list();
        assert_eq!(tools["tools"].as_array().unwrap().len(), 3);
    }
}
