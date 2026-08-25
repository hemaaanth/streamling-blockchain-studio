use abi_stable::std_types::RDuration;
use arrow::{
    array::{RecordBatch, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use ethers_core::{
    abi::{Abi, Event, RawLog, Token},
    types::H256,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use streamling_plugin::api::{
    PluginStateBackend, PluginStateBackendFactory, STREAMLING_COLUMN_NAME_OP,
    SupportsGracefulShutdown,
};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::ffi::PluginMetricsRecorder;
use streamling_plugin::{CheckpointEpoch, PluginError, PluginInitializationError, SourcePlugin};

#[derive(Clone, Deserialize)]
struct ContractSpec {
    alias: String,
    address: String,
    abi_path: PathBuf,
}
#[derive(Clone, Deserialize)]
struct DiscoverySpec {
    parent_contract: String,
    discovery_event: String,
    address_field: String,
    child_contract: String,
    child_abi_path: PathBuf,
}
#[derive(Deserialize)]
struct SourceSpec {
    contracts: Vec<ContractSpec>,
    #[serde(default)]
    discovery_rules: Vec<DiscoverySpec>,
}
#[derive(Clone)]
struct EventDescriptor {
    owner: String,
    event: Event,
}

pub struct EvmEventSource {
    rt: PluginAsyncRuntimeObj,
    metrics: PluginMetricsRecorder,
    client: reqwest::Client,
    rpc_url: String,
    confirmations: u64,
    window: AtomicU64,
    next_block: AtomicU64,
    progress_path: Option<PathBuf>,
    state: Arc<PluginStateBackend<SourceState>>,
    schema: SchemaRef,
    events: HashMap<H256, Vec<EventDescriptor>>,
    direct_addresses: HashMap<String, String>,
    discovery_rules: Vec<DiscoverySpec>,
    topics: Vec<String>,
    running: AtomicBool,
    discovered_rows: Mutex<Vec<PersistedRow>>,
    bootstrap_rows: Mutex<Option<Vec<PersistedRow>>>,
}

impl EvmEventSource {
    pub fn new(
        rt: PluginAsyncRuntimeObj,
        state_factory: PluginStateBackendFactory,
        metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let required = |name: &str| {
            options.get(name).cloned().ok_or_else(|| {
                PluginInitializationError::Configuration(format!("missing option {name}").into())
            })
        };
        let rpc_url = if let Some(url) = options.get("rpc_url") {
            url.clone()
        } else if let Some(name) = options.get("rpc_url_env") {
            std::env::var(name).map_err(|_| {
                PluginInitializationError::Configuration(format!("read ${name}").into())
            })?
        } else {
            return Err(PluginInitializationError::Configuration(
                "missing option rpc_url or rpc_url_env".into(),
            ));
        };
        let start_block = parse_option(&options, "start_block", 0)?;
        let confirmations = parse_option(&options, "confirmations", 12)?;
        let window = parse_option(&options, "window", 2_000)?;
        if window == 0 {
            return Err(PluginInitializationError::Configuration(
                "window must be greater than zero".into(),
            ));
        }
        let spec: SourceSpec = serde_json::from_str(&required("spec")?).map_err(config_error)?;
        let mut events: HashMap<H256, Vec<EventDescriptor>> = HashMap::new();
        let mut direct_addresses = HashMap::new();
        for contract in &spec.contracts {
            direct_addresses.insert(
                contract.address.to_ascii_lowercase(),
                contract.alias.clone(),
            );
            load_events(&contract.alias, &contract.abi_path, &mut events)?;
        }
        for rule in &spec.discovery_rules {
            load_events(&rule.child_contract, &rule.child_abi_path, &mut events)?;
        }
        let mut topics = events
            .keys()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        topics.sort();
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("contract_alias", DataType::Utf8, false),
            Field::new("event_name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_hash", DataType::Utf8, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("tx_hash", DataType::Utf8, false),
            Field::new("log_index", DataType::UInt64, false),
            Field::new("topic0", DataType::Utf8, false),
            Field::new("fields_json", DataType::Utf8, false),
            Field::new("discovered_address", DataType::Utf8, true),
            Field::new("discovery_rule", DataType::Utf8, true),
            Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        Ok(Self {
            rt,
            metrics,
            client: reqwest::Client::new(),
            rpc_url,
            confirmations,
            window: AtomicU64::new(window),
            next_block: AtomicU64::new(start_block),
            progress_path: options.get("progress_path").map(PathBuf::from),
            state: state_factory.create(),
            schema,
            events,
            direct_addresses,
            discovery_rules: spec.discovery_rules,
            topics,
            running: AtomicBool::new(true),
            discovered_rows: Mutex::new(Vec::new()),
            bootstrap_rows: Mutex::new(None),
        })
    }

    fn write_progress(
        &self,
        indexed_through: u64,
        observed_head: u64,
        safe_head: u64,
    ) -> Result<(), PluginError> {
        let Some(path) = &self.progress_path else {
            return Ok(());
        };
        let progress = BackfillProgress {
            indexed_through,
            observed_head,
            safe_head,
            confirmations: self.confirmations,
            caught_up: indexed_through >= safe_head,
            updated_at_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(progress_error)?;
        let temporary = path.with_extension("json.tmp");
        fs::write(
            &temporary,
            serde_json::to_vec(&progress).map_err(progress_error)?,
        )
        .map_err(progress_error)?;
        fs::rename(&temporary, path).map_err(progress_error)
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value, PluginError> {
        let body: Value = self
            .client
            .post(&self.rpc_url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .map_err(|e| PluginError::Execution(format!("{method}: {e}")))?
            .json()
            .await
            .map_err(|e| PluginError::Execution(format!("{method} response: {e}")))?;
        if let Some(error) = body.get("error") {
            return Err(PluginError::Execution(format!("{method}: {error}")));
        }
        body.get("result")
            .cloned()
            .ok_or_else(|| PluginError::Execution(format!("{method}: missing result")))
    }

    async fn block_timestamp(&self, block: u64) -> Result<u64, PluginError> {
        let value = self
            .rpc(
                "eth_getBlockByNumber",
                json!([format!("0x{block:x}"), false]),
            )
            .await?;
        parse_hex(
            value
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or("0x0"),
        )
    }
}

#[async_trait]
impl SupportsGracefulShutdown for EvmEventSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for EvmEventSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        if let Some(saved) = self.state.get().await.map_err(PluginError::State)? {
            self.next_block.store(saved.next_block, Ordering::Relaxed);
            *self
                .discovered_rows
                .lock()
                .map_err(|_| PluginError::Internal("discovery registry mutex poisoned".into()))? =
                saved.discovered_rows.clone();
            *self.bootstrap_rows.lock().map_err(|_| {
                PluginError::Internal("discovery bootstrap mutex poisoned".into())
            })? = Some(saved.discovered_rows);
        }
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.clone())
    }
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        if let Some(rows) = self
            .bootstrap_rows
            .lock()
            .map_err(|_| PluginError::Internal("discovery bootstrap mutex poisoned".into()))?
            .take()
        {
            return rows_to_batch(
                self.schema.clone(),
                rows.into_iter()
                    .map(|saved| (saved.row, saved.timestamp))
                    .collect(),
            );
        }
        let head = parse_hex(
            self.rpc("eth_blockNumber", json!([]))
                .await?
                .as_str()
                .unwrap_or("0x0"),
        )?;
        let safe_head = head.saturating_sub(self.confirmations);
        let from = self.next_block.load(Ordering::Relaxed);
        self.write_progress(from.saturating_sub(1), head, safe_head)?;
        if from > safe_head {
            self.rt.sleep(RDuration::from_millis(1_000)).await;
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let mut current_window = self.window.load(Ordering::Relaxed).max(1);
        let (to, logs) = loop {
            let to = safe_head.min(from.saturating_add(current_window - 1));
            let mut filter = json!({"fromBlock": format!("0x{from:x}"), "toBlock": format!("0x{to:x}"), "topics": [self.topics]});
            if self.discovery_rules.is_empty() {
                filter["address"] = Value::Array(
                    self.direct_addresses
                        .keys()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                );
            }
            match self.rpc("eth_getLogs", json!([filter])).await {
                Ok(value) => {
                    self.window.store(current_window, Ordering::Relaxed);
                    break (to, value.as_array().cloned().unwrap_or_default());
                }
                Err(error) if current_window > 1 && is_get_logs_range_error(&error) => {
                    current_window = (current_window / 2).max(1);
                    self.window.store(current_window, Ordering::Relaxed);
                    continue;
                }
                Err(error) => return Err(error),
            }
        };
        let mut timestamps = HashMap::new();
        let mut decoded = Vec::new();
        for log in logs {
            if let Some(row) = decode_row(
                &log,
                &self.events,
                &self.direct_addresses,
                &self.discovery_rules,
            )? {
                let timestamp = if let Some(value) = timestamps.get(&row.block_number) {
                    *value
                } else {
                    let value = self.block_timestamp(row.block_number).await?;
                    timestamps.insert(row.block_number, value);
                    value
                };
                decoded.push((row, timestamp));
            }
        }
        {
            let mut registry = self
                .discovered_rows
                .lock()
                .map_err(|_| PluginError::Internal("discovery registry mutex poisoned".into()))?;
            for (row, timestamp) in &decoded {
                if row.discovered_address.is_some()
                    && !registry
                        .iter()
                        .any(|saved| saved.row.event_id == row.event_id)
                {
                    registry.push(PersistedRow {
                        row: row.clone(),
                        timestamp: *timestamp,
                    });
                }
            }
        }
        let next_block = to.saturating_add(1);
        self.next_block.store(next_block, Ordering::Relaxed);
        self.write_progress(next_block.saturating_sub(1), head, safe_head)?;
        self.metrics
            .record_count("streamling_blockchain_logs", decoded.len() as u64);
        rows_to_batch(self.schema.clone(), decoded)
    }
    async fn process_checkpoint_marker(&self, _epoch: CheckpointEpoch) -> Result<(), PluginError> {
        Ok(())
    }
    async fn process_checkpoint_finalizer(
        &self,
        _epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let state = SourceState {
            next_block: self.next_block.load(Ordering::Relaxed),
            discovered_rows: self
                .discovered_rows
                .lock()
                .map_err(|_| PluginError::Internal("discovery registry mutex poisoned".into()))?
                .clone(),
        };
        self.state.put(state).await.map_err(PluginError::State)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BackfillProgress {
    indexed_through: u64,
    observed_head: u64,
    safe_head: u64,
    confirmations: u64,
    caught_up: bool,
    updated_at_unix: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SourceState {
    next_block: u64,
    discovered_rows: Vec<PersistedRow>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedRow {
    row: DecodedRow,
    timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DecodedRow {
    event_id: String,
    owner: String,
    event: String,
    address: String,
    block_number: u64,
    block_hash: String,
    tx_hash: String,
    log_index: u64,
    topic0: String,
    fields: String,
    discovered_address: Option<String>,
    discovery_rule: Option<String>,
}

fn decode_row(
    log: &Value,
    events: &HashMap<H256, Vec<EventDescriptor>>,
    direct: &HashMap<String, String>,
    discovery_rules: &[DiscoverySpec],
) -> Result<Option<DecodedRow>, PluginError> {
    let topics = log
        .get("topics")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let Some(topic0_text) = topics.first().and_then(Value::as_str) else {
        return Ok(None);
    };
    let topic0 = H256::from_str(topic0_text).map_err(internal)?;
    let Some(candidates) = events.get(&topic0) else {
        return Ok(None);
    };
    let address = log
        .get("address")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let preferred = direct.get(&address);
    let descriptor = candidates
        .iter()
        .find(|candidate| preferred == Some(&candidate.owner))
        .or_else(|| {
            candidates.iter().find(|candidate| {
                discovery_rules
                    .iter()
                    .any(|rule| rule.child_contract == candidate.owner)
            })
        })
        .unwrap_or(&candidates[0]);
    let raw_topics = topics
        .iter()
        .filter_map(Value::as_str)
        .map(H256::from_str)
        .collect::<Result<Vec<_>, _>>()
        .map_err(internal)?;
    let data = hex::decode(
        log.get("data")
            .and_then(Value::as_str)
            .unwrap_or("0x")
            .trim_start_matches("0x"),
    )
    .map_err(internal)?;
    let parsed = descriptor
        .event
        .parse_log(RawLog {
            topics: raw_topics,
            data,
        })
        .map_err(internal)?;
    let mut object = Map::new();
    let mut tokens = HashMap::new();
    for (index, param) in parsed.params.into_iter().enumerate() {
        let name = if param.name.is_empty() {
            format!("unnamed_{}", index + 1)
        } else {
            param.name
        };
        object.insert(name.clone(), token_json(&param.value));
        tokens.insert(name, param.value);
    }
    let discovery = discovery_rules.iter().find(|rule| {
        rule.parent_contract == descriptor.owner
            && rule.discovery_event == descriptor.event.name
            && direct.get(&address) == Some(&rule.parent_contract)
    });
    let (discovered_address, discovery_rule) = match discovery {
        Some(rule) => (
            tokens.get(&rule.address_field).and_then(token_address),
            Some(rule.child_contract.clone()),
        ),
        None => (None, None),
    };
    let block_number = parse_hex(
        log.get("blockNumber")
            .and_then(Value::as_str)
            .unwrap_or("0x0"),
    )?;
    let log_index = parse_hex(log.get("logIndex").and_then(Value::as_str).unwrap_or("0x0"))?;
    let block_hash = log
        .get("blockHash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let tx_hash = log
        .get("transactionHash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    Ok(Some(DecodedRow {
        event_id: format!("{block_hash}:{log_index}"),
        owner: descriptor.owner.clone(),
        event: descriptor.event.name.clone(),
        address,
        block_number,
        block_hash,
        tx_hash,
        log_index,
        topic0: topic0_text.to_owned(),
        fields: Value::Object(object).to_string(),
        discovered_address,
        discovery_rule,
    }))
}

fn rows_to_batch(
    schema: SchemaRef,
    rows: Vec<(DecodedRow, u64)>,
) -> Result<RecordBatch, PluginError> {
    let mut strings = (0..11).map(|_| StringBuilder::new()).collect::<Vec<_>>();
    let mut block_numbers = UInt64Builder::new();
    let mut timestamps = UInt64Builder::new();
    let mut indexes = UInt64Builder::new();
    for (row, timestamp) in rows {
        strings[0].append_value(row.event_id);
        strings[1].append_value(row.owner);
        strings[2].append_value(row.event);
        strings[3].append_value(row.address);
        block_numbers.append_value(row.block_number);
        strings[4].append_value(row.block_hash);
        timestamps.append_value(timestamp);
        strings[5].append_value(row.tx_hash);
        indexes.append_value(row.log_index);
        strings[6].append_value(row.topic0);
        strings[7].append_value(row.fields);
        strings[8].append_option(row.discovered_address);
        strings[9].append_option(row.discovery_rule);
        strings[10].append_value("i");
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(block_numbers.finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(timestamps.finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(indexes.finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
        ],
    )
    .map_err(PluginError::ArrowError)
}

fn load_events(
    owner: &str,
    path: &PathBuf,
    target: &mut HashMap<H256, Vec<EventDescriptor>>,
) -> Result<(), PluginInitializationError> {
    let abi: Abi = serde_json::from_slice(&fs::read(path).map_err(|e| {
        PluginInitializationError::Configuration(format!("read {}: {e}", path.display()).into())
    })?)
    .map_err(config_error)?;
    for events in abi.events.values() {
        for event in events {
            if event.anonymous {
                return Err(PluginInitializationError::Configuration(
                    format!(
                        "anonymous event {} in {} is unsupported; Streamling topic filters require topic0",
                        event.name,
                        path.display()
                    )
                    .into(),
                ));
            }
            let mut columns = HashSet::new();
            for (index, input) in event.inputs.iter().enumerate() {
                let raw = if input.name.is_empty() {
                    format!("unnamed_{}", index + 1)
                } else {
                    input.name.clone()
                };
                let column = event_column(&raw);
                if !columns.insert(column.clone()) {
                    return Err(PluginInitializationError::Configuration(
                        format!(
                            "event {} in {} maps multiple parameters to SQLite column {column}",
                            event.name,
                            path.display()
                        )
                        .into(),
                    ));
                }
            }
            target
                .entry(event.signature())
                .or_default()
                .push(EventDescriptor {
                    owner: owner.to_owned(),
                    event: event.clone(),
                });
        }
    }
    Ok(())
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

fn parse_option(
    options: &HashMap<String, String>,
    key: &str,
    default: u64,
) -> Result<u64, PluginInitializationError> {
    options
        .get(key)
        .map(|v| {
            v.parse().map_err(|_| {
                PluginInitializationError::Configuration(format!("invalid {key}").into())
            })
        })
        .unwrap_or(Ok(default))
}
fn config_error(error: impl std::fmt::Display) -> PluginInitializationError {
    PluginInitializationError::Configuration(error.to_string().into())
}
fn internal(error: impl std::fmt::Display) -> PluginError {
    PluginError::Internal(error.to_string())
}
fn progress_error(error: impl std::fmt::Display) -> PluginError {
    PluginError::Execution(format!("persist backfill progress: {error}"))
}
fn is_get_logs_range_error(error: &PluginError) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    text.contains("eth_getlogs")
        && (text.contains("exceeded max allowed range")
            || text.contains("block range")
            || text.contains("range too large"))
}
fn parse_hex(text: &str) -> Result<u64, PluginError> {
    u64::from_str_radix(text.trim_start_matches("0x"), 16).map_err(internal)
}
fn token_address(token: &Token) -> Option<String> {
    if let Token::Address(value) = token {
        Some(format!("{value:#x}"))
    } else {
        None
    }
}
fn token_json(token: &Token) -> Value {
    match token {
        Token::Address(v) => format!("{v:#x}").into(),
        Token::FixedBytes(v) | Token::Bytes(v) => format!("0x{}", hex::encode(v)).into(),
        Token::Int(v) | Token::Uint(v) => v.to_string().into(),
        Token::Bool(v) => (*v).into(),
        Token::String(v) => v.clone().into(),
        Token::Array(v) | Token::FixedArray(v) | Token::Tuple(v) => {
            Value::Array(v.iter().map(token_json).collect())
        }
    }
}
