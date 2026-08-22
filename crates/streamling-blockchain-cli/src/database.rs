use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use serde_json::{Map, Value, json};
use std::path::Path;

pub fn open(path: &Path) -> Result<Connection> {
    if !path.exists() {
        bail!("database does not exist yet: run `streamling-blockchain dev`");
    }
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open {} read-only", path.display()))
}

pub fn query(conn: &Connection, sql: &str, max_rows: usize) -> Result<Value> {
    let mut statement = conn.prepare(sql).context("prepare SQL")?;
    if statement.column_count() == 0 {
        let changed = statement.execute([])?;
        return Ok(json!({"columns": [], "rows": [], "changed": changed}));
    }
    let columns = statement
        .column_names()
        .iter()
        .map(|s| (*s).to_owned())
        .collect::<Vec<_>>();
    let mut rows = statement.query([])?;
    let mut output = Vec::new();
    let mut truncated = false;
    while let Some(row) = rows.next()? {
        if output.len() == max_rows {
            truncated = true;
            break;
        }
        let mut object = Map::new();
        for (index, name) in columns.iter().enumerate() {
            object.insert(name.clone(), value_to_json(row.get_ref(index)?));
        }
        output.push(Value::Object(object));
    }
    Ok(json!({"columns": columns, "rows": output, "truncated": truncated}))
}

fn value_to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(v) => v.into(),
        ValueRef::Real(v) => json!(v),
        ValueRef::Text(v) => String::from_utf8_lossy(v).into_owned().into(),
        ValueRef::Blob(v) => format!("0x{}", hex::encode(v)).into(),
    }
}

pub fn schema(conn: &Connection) -> Result<Value> {
    let mut statement = conn.prepare(
        "SELECT name, type, sql FROM sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )?;
    let entries = statement
        .query_map([], |row| {
            Ok(json!({
                "name": row.get::<_, String>(0)?,
                "type": row.get::<_, String>(1)?,
                "sql": row.get::<_, Option<String>>(2)?
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({"objects": entries}))
}
