use arrow::{
    array::{RecordBatch, StringArray, UInt64Array},
    datatypes::SchemaRef,
};
use async_trait::async_trait;
use rusqlite::{Connection, params, types::Value as SqlValue};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use streamling_plugin::api::{PluginStateBackendFactory, SupportsGracefulShutdown};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, PluginInitializationError, SinkPlugin};

pub struct SqliteSink {
    connection: Mutex<Connection>,
    metrics: PluginMetricsRecorder,
    running: AtomicBool,
}

impl SqliteSink {
    pub fn new(
        _schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state: PluginStateBackendFactory,
        metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let path = options
            .get("db_path")
            .map(PathBuf::from)
            .ok_or_else(|| PluginInitializationError::Configuration("missing db_path".into()))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| PluginInitializationError::Execution(e.to_string().into()))?;
        }
        let connection = Connection::open(&path)
            .map_err(|e| PluginInitializationError::Execution(e.to_string().into()))?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| PluginInitializationError::Execution(e.to_string().into()))?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY, contract_alias TEXT NOT NULL, event_name TEXT NOT NULL,
            address TEXT NOT NULL, block_number INTEGER NOT NULL, block_hash TEXT NOT NULL,
            block_timestamp INTEGER NOT NULL, tx_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
            topic0 TEXT NOT NULL, data TEXT NOT NULL CHECK(json_valid(data))
        );
        CREATE INDEX IF NOT EXISTS events_block ON events(block_number, log_index);
        CREATE INDEX IF NOT EXISTS events_contract_event ON events(contract_alias, event_name);",
            )
            .map_err(|e| PluginInitializationError::Execution(e.to_string().into()))?;
        Ok(Self {
            connection: Mutex::new(connection),
            metrics,
            running: AtomicBool::new(true),
        })
    }
}

#[async_trait]
impl SupportsGracefulShutdown for SqliteSink {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl SinkPlugin for SqliteSink {
    async fn initialize(&self) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_batch(&self, batch: RecordBatch) -> Result<(), PluginError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let text = |name: &str| -> Result<&StringArray, PluginError> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| PluginError::Execution(format!("missing string column {name}")))
        };
        let numbers = |name: &str| -> Result<&UInt64Array, PluginError> {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| PluginError::Execution(format!("missing uint64 column {name}")))
        };
        let event_id = text("event_id")?;
        let alias = text("contract_alias")?;
        let event_name = text("event_name")?;
        let address = text("address")?;
        let block_number = numbers("block_number")?;
        let block_hash = text("block_hash")?;
        let timestamp = numbers("block_timestamp")?;
        let tx_hash = text("tx_hash")?;
        let log_index = numbers("log_index")?;
        let topic0 = text("topic0")?;
        let fields = text("fields_json")?;
        let op = text("_gs_op")?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PluginError::Internal("SQLite mutex poisoned".into()))?;
        let transaction = connection.transaction().map_err(db_error)?;
        for row in 0..batch.num_rows() {
            let table = safe_table(alias.value(row), event_name.value(row));
            transaction.execute_batch(&format!("CREATE TABLE IF NOT EXISTS \"{table}\" (
                event_id TEXT PRIMARY KEY, address TEXT NOT NULL, block_number INTEGER NOT NULL,
                block_hash TEXT NOT NULL, block_timestamp INTEGER NOT NULL, tx_hash TEXT NOT NULL,
                log_index INTEGER NOT NULL, data TEXT NOT NULL CHECK(json_valid(data))
            ); CREATE INDEX IF NOT EXISTS \"{table}_block\" ON \"{table}\"(block_number, log_index);" )).map_err(db_error)?;
            if op.value(row) == "d" {
                transaction
                    .execute(
                        "DELETE FROM events WHERE event_id = ?1",
                        [event_id.value(row)],
                    )
                    .map_err(db_error)?;
                transaction
                    .execute(
                        &format!("DELETE FROM \"{table}\" WHERE event_id = ?1"),
                        [event_id.value(row)],
                    )
                    .map_err(db_error)?;
            } else {
                let decoded: serde_json::Map<String, Value> =
                    serde_json::from_str(fields.value(row)).map_err(|error| {
                        PluginError::Execution(format!("invalid decoded event JSON: {error}"))
                    })?;
                let mut columns = {
                    let mut statement = transaction
                        .prepare(&format!("PRAGMA table_info(\"{table}\")"))
                        .map_err(db_error)?;
                    statement
                        .query_map([], |row| row.get::<_, String>(1))
                        .map_err(db_error)?
                        .collect::<rusqlite::Result<HashSet<_>>>()
                        .map_err(db_error)?
                };
                for field in decoded.keys() {
                    let column = event_column(field);
                    if columns.insert(column.clone()) {
                        transaction
                            .execute(
                                &format!(
                                    "ALTER TABLE \"{table}\" ADD COLUMN {} TEXT",
                                    quote_identifier(&column)
                                ),
                                [],
                            )
                            .map_err(db_error)?;
                    }
                }
                let values = params![
                    event_id.value(row),
                    alias.value(row),
                    event_name.value(row),
                    address.value(row),
                    block_number.value(row),
                    block_hash.value(row),
                    timestamp.value(row),
                    tx_hash.value(row),
                    log_index.value(row),
                    topic0.value(row),
                    fields.value(row)
                ];
                transaction
                    .execute(
                        "INSERT OR REPLACE INTO events VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                        values,
                    )
                    .map_err(db_error)?;
                transaction
                    .execute(
                        &format!(
                            "INSERT OR REPLACE INTO \"{table}\" (event_id,address,block_number,block_hash,block_timestamp,tx_hash,log_index,data) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)"
                        ),
                        params![
                            event_id.value(row),
                            address.value(row),
                            block_number.value(row),
                            block_hash.value(row),
                            timestamp.value(row),
                            tx_hash.value(row),
                            log_index.value(row),
                            fields.value(row)
                        ],
                    )
                    .map_err(db_error)?;
                for (field, value) in decoded {
                    let column = event_column(&field);
                    transaction
                        .execute(
                            &format!(
                                "UPDATE \"{table}\" SET {} = ?2 WHERE event_id = ?1",
                                quote_identifier(&column)
                            ),
                            params![event_id.value(row), sql_value(value)],
                        )
                        .map_err(db_error)?;
                }
            }
        }
        transaction.commit().map_err(db_error)?;
        self.metrics
            .record_count("streamling_blockchain_sqlite_rows", batch.num_rows() as u64);
        Ok(())
    }
    async fn process_checkpoint_marker(&self, _epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.connection
            .lock()
            .map_err(|_| PluginError::Internal("SQLite mutex poisoned".into()))?
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")
            .map_err(db_error)
    }
    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

fn safe_table(alias: &str, event: &str) -> String {
    format!("{}__{}", sanitize(alias), sanitize(event))
}
fn sanitize(value: &str) -> String {
    let mut out = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if out.is_empty() || out.as_bytes()[0].is_ascii_digit() {
        out.insert(0, '_');
    }
    out
}
fn event_column(field: &str) -> String {
    const RESERVED: &[&str] = &[
        "event_id",
        "address",
        "block_number",
        "block_hash",
        "block_timestamp",
        "tx_hash",
        "log_index",
        "data",
    ];
    if RESERVED.contains(&field) {
        format!("param_{field}")
    } else {
        field.to_owned()
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sql_value(value: Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(i64::from(value)),
        Value::Number(value) => SqlValue::Text(value.to_string()),
        Value::String(value) => SqlValue::Text(value),
        Value::Array(_) | Value::Object(_) => SqlValue::Text(value.to_string()),
    }
}

fn db_error(error: rusqlite::Error) -> PluginError {
    PluginError::Execution(format!("SQLite: {error}"))
}
