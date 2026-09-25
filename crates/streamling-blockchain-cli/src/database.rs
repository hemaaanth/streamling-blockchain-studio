use crate::{clickhouse::ClickHouse, config::ProjectConfig};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use serde_json::{Map, Value, json};
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum BackendKind {
    Sqlite,
    Clickhouse,
}

/// The configured sink that read commands query. SQLite wins when both sinks are enabled.
pub fn resolve(config: &ProjectConfig, requested: Option<BackendKind>) -> Result<BackendKind> {
    match requested {
        Some(BackendKind::Sqlite) if !config.sinks.sqlite => {
            bail!("this project has no SQLite sink; use --backend clickhouse")
        }
        Some(BackendKind::Clickhouse) if config.sinks.clickhouse.is_none() => {
            bail!("this project has no ClickHouse sink; use --backend sqlite")
        }
        Some(kind) => Ok(kind),
        None if config.sinks.sqlite => Ok(BackendKind::Sqlite),
        None => Ok(BackendKind::Clickhouse),
    }
}

pub enum Backend {
    Sqlite(Connection),
    ClickHouse { client: ClickHouse, table: String },
}

impl Backend {
    pub fn open(
        root: &Path,
        config: &ProjectConfig,
        requested: Option<BackendKind>,
    ) -> Result<Self> {
        match resolve(config, requested)? {
            BackendKind::Sqlite => Ok(Self::Sqlite(open(&config.absolute_database(root))?)),
            BackendKind::Clickhouse => {
                let sink = config
                    .sinks
                    .clickhouse
                    .as_ref()
                    .context("ClickHouse sink is not configured")?;
                Ok(Self::ClickHouse {
                    client: ClickHouse::from_env(sink.database.as_deref())?,
                    table: sink.table.clone(),
                })
            }
        }
    }

    pub async fn query(&self, sql: &str, max_rows: usize) -> Result<Value> {
        match self {
            Self::Sqlite(conn) => query(conn, sql, max_rows),
            Self::ClickHouse { client, .. } => client.query(sql, max_rows).await,
        }
    }

    pub async fn schema(&self) -> Result<Value> {
        match self {
            Self::Sqlite(conn) => schema(conn),
            Self::ClickHouse { client, table } => {
                let mut value = client.schema().await?;
                value["events_table"] = client.qualified(table).into();
                value["dialect_notes"] = json!([
                    "Decoded event fields are JSON in fields_json; read them with JSONExtractString(fields_json, 'field').",
                    "Tables are ReplacingMergeTree; read current rows with FINAL WHERE is_deleted = 0.",
                    "Per-event <alias>__<event> tables in semantic.toml exist only in the SQLite sink; filter events by contract_alias and event_name instead."
                ]);
                Ok(value)
            }
        }
    }
}

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
    Ok(json!({"backend": "sqlite", "objects": entries}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClickHouseSinkConfig, ContractConfig, SinkConfig};
    use std::path::PathBuf;

    fn config(sqlite: bool, clickhouse: bool) -> ProjectConfig {
        ProjectConfig {
            chain: "ethereum".into(),
            chain_id: 1,
            rpc_url: "http://localhost".into(),
            rpc_url_env: None,
            database: PathBuf::from("events.db"),
            start_block: 0,
            end_block: None,
            confirmations: 12,
            window: 100,
            index_blocks: false,
            index_transactions: false,
            contracts: vec![ContractConfig {
                alias: "token".into(),
                address: "0x1111111111111111111111111111111111111111".into(),
                abi: PathBuf::from("abi.json"),
            }],
            discovery_rules: vec![],
            sinks: SinkConfig {
                sqlite,
                clickhouse: clickhouse.then(|| ClickHouseSinkConfig {
                    table: "events".into(),
                    compression: None,
                    database: None,
                }),
            },
        }
    }

    #[test]
    fn resolves_the_backend_from_configured_sinks() {
        assert_eq!(
            resolve(&config(true, true), None).unwrap(),
            BackendKind::Sqlite
        );
        assert_eq!(
            resolve(&config(false, true), None).unwrap(),
            BackendKind::Clickhouse
        );
        assert_eq!(
            resolve(&config(true, true), Some(BackendKind::Clickhouse)).unwrap(),
            BackendKind::Clickhouse
        );
        assert!(resolve(&config(true, false), Some(BackendKind::Clickhouse)).is_err());
        assert!(resolve(&config(false, true), Some(BackendKind::Sqlite)).is_err());
    }
}
