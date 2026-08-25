use arrow::{
    array::{Array, RecordBatch, StringArray, UInt64Array},
    datatypes::{DataType, SchemaRef},
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
    table: Option<String>,
    primary_key: Option<String>,
}

impl SqliteSink {
    pub fn new(
        schema: SchemaRef,
        _rt: PluginAsyncRuntimeObj,
        _state: PluginStateBackendFactory,
        metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let table = options.get("table").map(|value| sanitize(value));
        let primary_key = options
            .get("key_column")
            .or_else(|| options.get("primary_key"))
            .cloned();
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
        if let Some(table) = &table {
            ensure_table(&connection, &schema, table, primary_key.as_deref())
                .map_err(|e| PluginInitializationError::Execution(e.to_string().into()))?;
        } else {
            connection
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, chain_id INTEGER NOT NULL, contract_alias TEXT NOT NULL, event_name TEXT NOT NULL,
                address TEXT NOT NULL, block_number INTEGER NOT NULL, block_hash TEXT NOT NULL,
                block_timestamp INTEGER NOT NULL, tx_hash TEXT NOT NULL, log_index INTEGER NOT NULL,
                topic0 TEXT NOT NULL, data TEXT NOT NULL CHECK(json_valid(data))
            );
            CREATE INDEX IF NOT EXISTS events_block ON events(chain_id, block_number, log_index);
            CREATE INDEX IF NOT EXISTS events_contract_event ON events(chain_id, contract_alias, event_name);
            CREATE TABLE IF NOT EXISTS blocks (
                chain_id INTEGER NOT NULL, block_number INTEGER NOT NULL, block_hash TEXT NOT NULL, block_timestamp INTEGER NOT NULL,
                first_seen_at INTEGER NOT NULL DEFAULT (unixepoch()), last_seen_at INTEGER NOT NULL DEFAULT (unixepoch()),
                canonical INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (chain_id, block_number, block_hash)
            );
            CREATE INDEX IF NOT EXISTS blocks_canonical ON blocks(chain_id, block_number, canonical);
            CREATE TABLE IF NOT EXISTS event_revisions (
                event_id TEXT NOT NULL, op TEXT NOT NULL, chain_id INTEGER NOT NULL, contract_alias TEXT NOT NULL, event_name TEXT NOT NULL,
                block_number INTEGER NOT NULL, block_hash TEXT NOT NULL, observed_at INTEGER NOT NULL DEFAULT (unixepoch())
            );
            CREATE INDEX IF NOT EXISTS event_revisions_event ON event_revisions(event_id, observed_at);
            CREATE VIEW IF NOT EXISTS quality_reorgs AS
                SELECT chain_id, block_number, count(*) AS observed_hashes,
                       group_concat(block_hash, ',') AS block_hashes
                FROM blocks GROUP BY chain_id, block_number HAVING count(*) > 1;
            CREATE VIEW IF NOT EXISTS quality_event_counts_by_block AS
                SELECT chain_id, block_number, contract_alias, event_name, count(*) AS events
                FROM events GROUP BY chain_id, block_number, contract_alias, event_name;",
                )
                .map_err(|e| PluginInitializationError::Execution(e.to_string().into()))?;
        }
        Ok(Self {
            connection: Mutex::new(connection),
            metrics,
            running: AtomicBool::new(true),
            table,
            primary_key,
        })
    }
}

impl SqliteSink {
    async fn process_table_batch(
        &self,
        table: &str,
        batch: RecordBatch,
    ) -> Result<(), PluginError> {
        let primary_key = self.primary_key.as_deref().ok_or_else(|| {
            PluginError::Execution("generic SQLite sink requires key_column".into())
        })?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| PluginError::Internal("SQLite mutex poisoned".into()))?;
        ensure_table(&connection, batch.schema_ref(), table, Some(primary_key))
            .map_err(db_error)?;
        let transaction = connection.transaction().map_err(db_error)?;
        prune_replaced_transaction_blocks(&transaction, table, &batch).map_err(db_error)?;
        let fields = batch
            .schema()
            .fields()
            .iter()
            .filter(|field| field.name().as_str() != "_gs_op")
            .cloned()
            .collect::<Vec<_>>();
        let names = fields
            .iter()
            .map(|field| quote_identifier(field.name()))
            .collect::<Vec<_>>();
        let placeholders = (1..=fields.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>();
        let op = batch
            .column_by_name("_gs_op")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>());
        for row in 0..batch.num_rows() {
            let key = value_at(
                batch.column_by_name(primary_key).ok_or_else(|| {
                    PluginError::Execution(format!("missing column {primary_key}"))
                })?,
                row,
            )?;
            if matches!(op, Some(op) if op.value(row) == "d") {
                transaction
                    .execute(
                        &format!(
                            "DELETE FROM {} WHERE {} = ?1",
                            quote_identifier(table),
                            quote_identifier(primary_key)
                        ),
                        [key],
                    )
                    .map_err(db_error)?;
                continue;
            }
            let values = fields
                .iter()
                .map(|field| {
                    batch
                        .column_by_name(field.name())
                        .ok_or_else(|| {
                            PluginError::Execution(format!("missing column {}", field.name()))
                        })
                        .and_then(|column| value_at(column, row))
                })
                .collect::<Result<Vec<_>, _>>()?;
            transaction
                .execute(
                    &format!(
                        "INSERT OR REPLACE INTO {} ({}) VALUES ({})",
                        quote_identifier(table),
                        names.join(","),
                        placeholders.join(",")
                    ),
                    rusqlite::params_from_iter(values),
                )
                .map_err(db_error)?;
        }
        transaction.commit().map_err(db_error)?;
        self.metrics
            .record_count("streamling_blockchain_sqlite_rows", batch.num_rows() as u64);
        Ok(())
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
        if let Some(table) = &self.table {
            return self.process_table_batch(table, batch).await;
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
        let chain_id = numbers("chain_id")?;
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
                event_id TEXT PRIMARY KEY, chain_id INTEGER NOT NULL, address TEXT NOT NULL, block_number INTEGER NOT NULL,
                block_hash TEXT NOT NULL, block_timestamp INTEGER NOT NULL, tx_hash TEXT NOT NULL,
                log_index INTEGER NOT NULL, data TEXT NOT NULL CHECK(json_valid(data))
            ); CREATE INDEX IF NOT EXISTS \"{table}_block\" ON \"{table}\"(chain_id, block_number, log_index);" )).map_err(db_error)?;
            transaction
                .execute(
                    "UPDATE blocks SET canonical = 0, last_seen_at = unixepoch() WHERE chain_id = ?1 AND block_number = ?2 AND block_hash <> ?3",
                    params![
                        chain_id.value(row),
                        block_number.value(row),
                        block_hash.value(row)
                    ],
                )
                .map_err(db_error)?;
            transaction
                .execute(
                    "INSERT INTO blocks (chain_id, block_number, block_hash, block_timestamp) VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(chain_id, block_number, block_hash) DO UPDATE SET last_seen_at = unixepoch(), canonical = 1",
                    params![
                        chain_id.value(row),
                        block_number.value(row),
                        block_hash.value(row),
                        timestamp.value(row)
                    ],
                )
                .map_err(db_error)?;
            transaction
                .execute(
                    "INSERT INTO event_revisions (event_id, op, chain_id, contract_alias, event_name, block_number, block_hash) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        event_id.value(row),
                        if op.value(row) == "d" { "delete" } else { "insert" },
                        chain_id.value(row),
                        alias.value(row),
                        event_name.value(row),
                        block_number.value(row),
                        block_hash.value(row),
                    ],
                )
                .map_err(db_error)?;
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
                    chain_id.value(row),
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
                        "INSERT OR REPLACE INTO events VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
                        values,
                    )
                    .map_err(db_error)?;
                transaction
                    .execute(
                        &format!(
                            "INSERT OR REPLACE INTO \"{table}\" (event_id,chain_id,address,block_number,block_hash,block_timestamp,tx_hash,log_index,data) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
                        ),
                        params![
                            event_id.value(row),
                            chain_id.value(row),
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

fn ensure_table(
    connection: &Connection,
    schema: &SchemaRef,
    table: &str,
    primary_key: Option<&str>,
) -> rusqlite::Result<()> {
    let columns = schema
        .fields()
        .iter()
        .filter(|field| field.name().as_str() != "_gs_op")
        .map(|field| {
            let mut column = format!(
                "{} {}",
                quote_identifier(field.name()),
                sqlite_type(field.data_type())
            );
            if primary_key == Some(field.name().as_str()) {
                column.push_str(" PRIMARY KEY");
            }
            column
        })
        .collect::<Vec<_>>();
    connection.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_identifier(table),
        columns.join(",")
    ))?;
    let mut existing = connection
        .prepare(&format!("PRAGMA table_info({})", quote_identifier(table)))?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    for field in schema.fields() {
        if field.name() == "_gs_op" || existing.contains(field.name().as_str()) {
            continue;
        }
        connection.execute(
            &format!(
                "ALTER TABLE {} ADD COLUMN {} {}",
                quote_identifier(table),
                quote_identifier(field.name()),
                sqlite_type(field.data_type())
            ),
            [],
        )?;
        existing.insert(field.name().clone());
    }
    Ok(())
}

fn prune_replaced_transaction_blocks(
    transaction: &rusqlite::Transaction<'_>,
    table: &str,
    batch: &RecordBatch,
) -> rusqlite::Result<()> {
    if table != "evm_transactions" {
        return Ok(());
    }
    let Some(chain_ids) = batch
        .column_by_name("chain_id")
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
    else {
        return Ok(());
    };
    let Some(block_numbers) = batch
        .column_by_name("block_number")
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
    else {
        return Ok(());
    };
    let mut blocks = HashSet::new();
    for row in 0..batch.num_rows() {
        blocks.insert((chain_ids.value(row), block_numbers.value(row)));
    }
    for (chain_id, block_number) in blocks {
        transaction.execute(
            "DELETE FROM evm_transactions WHERE chain_id = ?1 AND block_number = ?2",
            params![chain_id, block_number],
        )?;
    }
    Ok(())
}

fn sqlite_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::UInt64 => "INTEGER",
        _ => "TEXT",
    }
}

fn value_at(column: &dyn Array, row: usize) -> Result<SqlValue, PluginError> {
    if column.is_null(row) {
        return Ok(SqlValue::Null);
    }
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(SqlValue::Text(values.value(row).to_owned()));
    }
    if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
        return Ok(SqlValue::Integer(values.value(row) as i64));
    }
    Err(PluginError::Execution(format!(
        "unsupported SQLite sink column type {:?}",
        column.data_type()
    )))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn pruning_transactions_replaces_every_row_for_touched_blocks() {
        let mut connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE evm_transactions (
                    transaction_id TEXT PRIMARY KEY,
                    chain_id INTEGER NOT NULL,
                    block_number INTEGER NOT NULL
                );
                INSERT INTO evm_transactions VALUES ('old_a', 1, 100);
                INSERT INTO evm_transactions VALUES ('old_b', 1, 100);
                INSERT INTO evm_transactions VALUES ('other', 1, 101);",
            )
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("transaction_id", DataType::Utf8, false),
            Field::new("chain_id", DataType::UInt64, false),
            Field::new("block_number", DataType::UInt64, false),
            Field::new("_gs_op", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["new_a"])),
                Arc::new(UInt64Array::from(vec![1])),
                Arc::new(UInt64Array::from(vec![100])),
                Arc::new(StringArray::from(vec!["i"])),
            ],
        )
        .unwrap();
        let transaction = connection.transaction().unwrap();

        prune_replaced_transaction_blocks(&transaction, "evm_transactions", &batch).unwrap();
        transaction.commit().unwrap();

        let remaining = connection
            .query_row(
                "SELECT group_concat(transaction_id, ',') FROM evm_transactions",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(remaining, "other");
    }
}
