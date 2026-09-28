use crate::rpc::RpcPool;
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
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
const MAX_ADDRESSES_PER_LOG_FILTER: usize = 500;
const MAX_CONCURRENT_LOG_REQUESTS: usize = 4;
const DEFAULT_BLOCK_CONCURRENCY: u64 = 8;
const DEFAULT_BATCH_SIZE: u64 = 1;

/// Fetch blocks at or below the safe head, in order. A load-balanced RPC can answer `null`
/// from a lagging node, so retry those instead of silently skipping the block.
async fn fetch_blocks(
    rt: &PluginAsyncRuntimeObj,
    rpc: &RpcPool,
    numbers: &[u64],
    full_transactions: bool,
) -> Result<Vec<Value>, PluginError> {
    let mut blocks = vec![Value::Null; numbers.len()];
    for attempt in 0..5 {
        let missing = (0..numbers.len())
            .filter(|&index| blocks[index].is_null())
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(blocks);
        }
        if attempt > 0 {
            rt.sleep(RDuration::from_millis(500 << attempt)).await;
        }
        let params = missing
            .iter()
            .map(|&index| json!([format!("0x{:x}", numbers[index]), full_transactions]))
            .collect();
        let values = rpc.batch(rt, "eth_getBlockByNumber", params).await?;
        for (index, value) in missing.into_iter().zip(values) {
            blocks[index] = value;
        }
    }
    match numbers
        .iter()
        .zip(&blocks)
        .find(|(_, block)| block.is_null())
    {
        Some((number, _)) => Err(PluginError::Execution(format!(
            "eth_getBlockByNumber: block {number} is unavailable below the safe head"
        ))),
        None => Ok(blocks),
    }
}

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
    rpc: RpcPool,
    chain_id: u64,
    confirmations: u64,
    window: AtomicU64,
    cursor: Cursor,
    progress_path: Option<PathBuf>,
    end_block: Option<u64>,
    state: Arc<PluginStateBackend<SourceState>>,
    schema: SchemaRef,
    events: HashMap<H256, Vec<EventDescriptor>>,
    direct_addresses: HashMap<String, String>,
    discovery_rules: Vec<DiscoverySpec>,
    direct_topics: Vec<String>,
    child_topics: Vec<String>,
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
        let rpc = RpcPool::from_options(&options, 1)?;
        let start_block = parse_option(&options, "start_block", 0)?;
        let confirmations = parse_option(&options, "confirmations", 12)?;
        let window = parse_option(&options, "window", 2_000)?;
        let chain_id = parse_option(&options, "chain_id", 0)?;
        let end_block = parse_optional(&options, "end_block")?;
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
        let mut direct_topics = events
            .keys()
            .map(|topic| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        direct_topics.sort();
        for rule in &spec.discovery_rules {
            load_events(&rule.child_contract, &rule.child_abi_path, &mut events)?;
        }
        let child_contracts = spec
            .discovery_rules
            .iter()
            .map(|rule| rule.child_contract.as_str())
            .collect::<HashSet<_>>();
        let mut child_topics = events
            .iter()
            .filter(|(_, descriptors)| {
                descriptors
                    .iter()
                    .any(|descriptor| child_contracts.contains(descriptor.owner.as_str()))
            })
            .map(|(topic, _)| format!("{topic:#x}"))
            .collect::<Vec<_>>();
        child_topics.sort();
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("chain_id", DataType::UInt64, false),
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
            rpc,
            chain_id,
            confirmations,
            window: AtomicU64::new(window),
            end_block,
            cursor: Cursor::new(start_block),
            progress_path: options.get("progress_path").map(PathBuf::from),
            state: state_factory.create(),
            schema,
            events,
            direct_addresses,
            discovery_rules: spec.discovery_rules,
            direct_topics,
            child_topics,
            running: AtomicBool::new(true),
            discovered_rows: Mutex::new(Vec::new()),
            bootstrap_rows: Mutex::new(None),
        })
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value, PluginError> {
        self.rpc.request(&self.rt, method, params).await
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

pub struct EvmBlockSource {
    rt: PluginAsyncRuntimeObj,
    metrics: PluginMetricsRecorder,
    rpc: RpcPool,
    confirmations: u64,
    window: u64,
    concurrency: usize,
    batch_size: usize,
    cursor: Cursor,
    progress_path: Option<PathBuf>,
    end_block: Option<u64>,
    chain_id: u64,
    state: Arc<PluginStateBackend<BlockSourceState>>,
    schema: SchemaRef,
    running: AtomicBool,
}

impl EvmBlockSource {
    pub fn new(
        rt: PluginAsyncRuntimeObj,
        state_factory: PluginStateBackendFactory,
        metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let rpc = RpcPool::from_options(&options, parse_batch_size(&options)?)?;
        let window = parse_option(&options, "window", 2_000)?;
        let chain_id = parse_option(&options, "chain_id", 0)?;
        let end_block = parse_optional(&options, "end_block")?;
        if window == 0 {
            return Err(PluginInitializationError::Configuration(
                "window must be greater than zero".into(),
            ));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_id", DataType::Utf8, false),
            Field::new("chain_id", DataType::UInt64, false),
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_hash", DataType::Utf8, false),
            Field::new("parent_hash", DataType::Utf8, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("miner", DataType::Utf8, true),
            Field::new("gas_limit", DataType::UInt64, false),
            Field::new("gas_used", DataType::UInt64, false),
            Field::new("base_fee_per_gas", DataType::Utf8, true),
            Field::new("transaction_count", DataType::UInt64, false),
            Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        Ok(Self {
            rt,
            metrics,
            rpc,
            chain_id,
            confirmations: parse_option(&options, "confirmations", 12)?,
            window,
            concurrency: parse_concurrency(&options)?,
            batch_size: parse_batch_size(&options)?,
            cursor: Cursor::new(parse_option(&options, "start_block", 0)?),
            progress_path: options.get("progress_path").map(PathBuf::from),
            end_block,
            state: state_factory.create(),
            schema,
            running: AtomicBool::new(true),
        })
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value, PluginError> {
        self.rpc.request(&self.rt, method, params).await
    }
}

#[async_trait]
impl SupportsGracefulShutdown for EvmBlockSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for EvmBlockSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        if let Some(saved) = self.state.get().await.map_err(PluginError::State)? {
            self.cursor.restore(saved.next_block);
        }
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.clone())
    }
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let head = parse_hex(
            self.rpc("eth_blockNumber", json!([]))
                .await?
                .as_str()
                .unwrap_or("0x0"),
        )?;
        let safe_head = capped_safe_head(head, self.confirmations, self.end_block);
        let from = self.cursor.begin(head, safe_head)?;
        if from > safe_head {
            self.rt.sleep(RDuration::from_millis(1_000)).await;
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let to = safe_head.min(from.saturating_add(self.window - 1));
        let rows = stream::iter(block_chunks(from, to, self.batch_size))
            .map(|chunk| async move {
                fetch_blocks(&self.rt, &self.rpc, &chunk, false)
                    .await?
                    .iter()
                    .map(|block| block_row(self.chain_id, block))
                    .collect::<Result<Vec<_>, _>>()
            })
            .buffered(self.concurrency)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        self.cursor.advance(to.saturating_add(1));
        self.metrics
            .record_count("streamling_blockchain_blocks", rows.len() as u64);
        block_rows_to_batch(self.schema.clone(), rows)
    }
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.cursor.mark(&epoch)
    }
    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let Some(mark) = self.cursor.finalize(&epoch)? else {
            return Ok(());
        };
        self.state
            .put(BlockSourceState {
                next_block: mark.next_block,
            })
            .await
            .map_err(PluginError::State)?;
        write_progress(self.progress_path.as_deref(), self.confirmations, mark)
    }
}

pub struct EvmTransactionSource {
    rt: PluginAsyncRuntimeObj,
    metrics: PluginMetricsRecorder,
    rpc: RpcPool,
    confirmations: u64,
    window: u64,
    concurrency: usize,
    batch_size: usize,
    /// When false, `input` is written empty; `method_id` still holds the selector.
    store_input: bool,
    cursor: Cursor,
    progress_path: Option<PathBuf>,
    end_block: Option<u64>,
    chain_id: u64,
    state: Arc<PluginStateBackend<BlockSourceState>>,
    schema: SchemaRef,
    running: AtomicBool,
}

impl EvmTransactionSource {
    pub fn new(
        rt: PluginAsyncRuntimeObj,
        state_factory: PluginStateBackendFactory,
        metrics: PluginMetricsRecorder,
        options: HashMap<String, String>,
    ) -> Result<Self, PluginInitializationError> {
        let rpc = RpcPool::from_options(&options, parse_batch_size(&options)?)?;
        let window = parse_option(&options, "window", 2_000)?;
        let end_block = parse_optional(&options, "end_block")?;
        if window == 0 {
            return Err(PluginInitializationError::Configuration(
                "window must be greater than zero".into(),
            ));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("transaction_id", DataType::Utf8, false),
            Field::new("chain_id", DataType::UInt64, false),
            Field::new("tx_hash", DataType::Utf8, false),
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_hash", DataType::Utf8, false),
            Field::new("block_timestamp", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt64, false),
            Field::new("from_address", DataType::Utf8, false),
            Field::new("to_address", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, false),
            Field::new("gas", DataType::UInt64, false),
            Field::new("gas_price", DataType::Utf8, true),
            Field::new("max_fee_per_gas", DataType::Utf8, true),
            Field::new("max_priority_fee_per_gas", DataType::Utf8, true),
            Field::new("input", DataType::Utf8, false),
            Field::new("method_id", DataType::Utf8, true),
            Field::new("nonce", DataType::UInt64, false),
            Field::new("receipt_status", DataType::UInt64, true),
            Field::new("receipt_gas_used", DataType::UInt64, true),
            Field::new("receipt_effective_gas_price", DataType::Utf8, true),
            Field::new("contract_address", DataType::Utf8, true),
            Field::new("logs_count", DataType::UInt64, true),
            Field::new(STREAMLING_COLUMN_NAME_OP, DataType::Utf8, false),
        ]));
        Ok(Self {
            rt,
            metrics,
            rpc,
            confirmations: parse_option(&options, "confirmations", 12)?,
            window,
            concurrency: parse_concurrency(&options)?,
            batch_size: parse_batch_size(&options)?,
            store_input: options
                .get("store_input")
                .map(|v| {
                    v.parse().map_err(|_| {
                        PluginInitializationError::Configuration("invalid store_input".into())
                    })
                })
                .unwrap_or(Ok(true))?,
            cursor: Cursor::new(parse_option(&options, "start_block", 0)?),
            progress_path: options.get("progress_path").map(PathBuf::from),
            end_block,
            chain_id: parse_option(&options, "chain_id", 0)?,
            state: state_factory.create(),
            schema,
            running: AtomicBool::new(true),
        })
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value, PluginError> {
        self.rpc.request(&self.rt, method, params).await
    }

    async fn transaction_rows(&self, numbers: &[u64]) -> Result<Vec<TransactionRow>, PluginError> {
        let blocks = fetch_blocks(&self.rt, &self.rpc, numbers, true).await?;
        let transactions = |block: &Value| {
            block
                .get("transactions")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        // Most blocks on fast chains are empty; fetch receipts only for the others.
        let busy = blocks
            .iter()
            .enumerate()
            .filter(|(_, block)| !transactions(block).is_empty())
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let batched = self
            .rpc
            .batch(
                &self.rt,
                "eth_getBlockReceipts",
                busy.iter()
                    .map(|&index| json!([format!("0x{:x}", numbers[index])]))
                    .collect(),
            )
            .await
            .unwrap_or_default();
        let mut rows = Vec::new();
        for (position, &index) in busy.iter().enumerate() {
            let block = &blocks[index];
            let txs = transactions(block);
            let receipts = match batched.get(position).and_then(Value::as_array) {
                Some(items) if items.len() == txs.len() => receipts_by_hash(items),
                // Unsupported method or a lagging node: fall back to the slower path.
                _ => self.receipts_by_tx(numbers[index], &txs).await?,
            };
            for (tx_index, tx) in txs.iter().enumerate() {
                let receipt = tx
                    .get("hash")
                    .and_then(Value::as_str)
                    .and_then(|hash| receipts.get(hash));
                let mut row = transaction_row(self.chain_id, block, tx, receipt, tx_index as u64)?;
                if !self.store_input {
                    row.input.clear();
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    async fn receipts_by_tx(
        &self,
        block_number: u64,
        txs: &[Value],
    ) -> Result<HashMap<String, Value>, PluginError> {
        if let Ok(receipts) = self
            .rpc(
                "eth_getBlockReceipts",
                json!([format!("0x{block_number:x}")]),
            )
            .await
            && let Some(items) = receipts.as_array()
            && items.len() == txs.len()
        {
            return Ok(receipts_by_hash(items));
        }
        let mut receipts = HashMap::new();
        for tx in txs {
            let Some(hash) = tx.get("hash").and_then(Value::as_str) else {
                continue;
            };
            let receipt = self.rpc("eth_getTransactionReceipt", json!([hash])).await?;
            if receipt.is_null() {
                return Err(PluginError::Execution(format!(
                    "eth_getTransactionReceipt: receipt for {hash} in block {block_number} is unavailable below the safe head"
                )));
            }
            receipts.insert(hash.to_owned(), receipt);
        }
        Ok(receipts)
    }
}

#[async_trait]
impl SupportsGracefulShutdown for EvmTransactionSource {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
    async fn terminate(&self) -> Result<(), PluginError> {
        self.running.store(false, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl SourcePlugin for EvmTransactionSource {
    async fn initialize(&self) -> Result<(), PluginError> {
        if let Some(saved) = self.state.get().await.map_err(PluginError::State)? {
            self.cursor.restore(saved.next_block);
        }
        Ok(())
    }
    fn output_schema(&self) -> Result<SchemaRef, PluginError> {
        Ok(self.schema.clone())
    }
    async fn generate_batch(&self) -> Result<RecordBatch, PluginError> {
        let head = parse_hex(
            self.rpc("eth_blockNumber", json!([]))
                .await?
                .as_str()
                .unwrap_or("0x0"),
        )?;
        let safe_head = capped_safe_head(head, self.confirmations, self.end_block);
        let from = self.cursor.begin(head, safe_head)?;
        if from > safe_head {
            self.rt.sleep(RDuration::from_millis(1_000)).await;
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let to = safe_head.min(from.saturating_add(self.window - 1));
        let rows = stream::iter(block_chunks(from, to, self.batch_size))
            .map(|chunk| async move { self.transaction_rows(&chunk).await })
            .buffered(self.concurrency)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        self.cursor.advance(to.saturating_add(1));
        self.metrics
            .record_count("streamling_blockchain_transactions", rows.len() as u64);
        transaction_rows_to_batch(self.schema.clone(), rows)
    }
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.cursor.mark(&epoch)
    }
    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let Some(mark) = self.cursor.finalize(&epoch)? else {
            return Ok(());
        };
        self.state
            .put(BlockSourceState {
                next_block: mark.next_block,
            })
            .await
            .map_err(PluginError::State)?;
        write_progress(self.progress_path.as_deref(), self.confirmations, mark)
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
            self.cursor.restore(saved.next_block);
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
        let safe_head = capped_safe_head(head, self.confirmations, self.end_block);
        let from = self.cursor.begin(head, safe_head)?;
        if from > safe_head {
            self.rt.sleep(RDuration::from_millis(1_000)).await;
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let mut current_window = self.window.load(Ordering::Relaxed).max(1);
        let (to, logs, child_contracts) = loop {
            let to = safe_head.min(from.saturating_add(current_window - 1));
            let direct_filter = json!({
                "fromBlock": format!("0x{from:x}"),
                "toBlock": format!("0x{to:x}"),
                "address": self.direct_addresses.keys().cloned().collect::<Vec<_>>(),
                "topics": [self.direct_topics]
            });
            let mut logs = match self.rpc("eth_getLogs", json!([direct_filter])).await {
                Ok(value) => value.as_array().cloned().unwrap_or_default(),
                Err(error) if current_window > 1 && is_get_logs_range_error(&error) => {
                    current_window = (current_window / 2).max(1);
                    self.window.store(current_window, Ordering::Relaxed);
                    continue;
                }
                Err(error) => return Err(error),
            };

            let mut child_contracts = self
                .discovered_rows
                .lock()
                .map_err(|_| PluginError::Internal("discovery registry mutex poisoned".into()))?
                .iter()
                .filter_map(|saved| {
                    Some((
                        saved.row.discovered_address.clone()?,
                        saved.row.discovery_rule.clone()?,
                    ))
                })
                .collect::<HashMap<_, _>>();
            for log in &logs {
                if let Some(row) = decode_row(
                    self.chain_id,
                    log,
                    &self.events,
                    &self.direct_addresses,
                    &self.discovery_rules,
                    &child_contracts,
                )? && let (Some(address), Some(rule)) =
                    (row.discovered_address, row.discovery_rule)
                {
                    child_contracts.insert(address, rule);
                }
            }
            let mut child_addresses = child_contracts.keys().cloned().collect::<Vec<_>>();
            child_addresses.sort_unstable();

            if !child_addresses.is_empty() && !self.child_topics.is_empty() {
                let address_batches = child_addresses
                    .chunks(MAX_ADDRESSES_PER_LOG_FILTER)
                    .map(<[String]>::to_vec)
                    .collect::<Vec<_>>();
                let requests = stream::iter(address_batches.into_iter().map(|addresses| {
                    let child_filter = json!({
                        "fromBlock": format!("0x{from:x}"),
                        "toBlock": format!("0x{to:x}"),
                        "address": addresses,
                        "topics": [self.child_topics]
                    });
                    async move { self.rpc("eth_getLogs", json!([child_filter])).await }
                }))
                .buffer_unordered(MAX_CONCURRENT_LOG_REQUESTS)
                .collect::<Vec<_>>()
                .await;
                let mut retry_with_smaller_window = false;
                for result in requests {
                    match result {
                        Ok(value) => logs.extend(value.as_array().cloned().unwrap_or_default()),
                        Err(error) if current_window > 1 && is_get_logs_range_error(&error) => {
                            current_window = (current_window / 2).max(1);
                            self.window.store(current_window, Ordering::Relaxed);
                            retry_with_smaller_window = true;
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if retry_with_smaller_window {
                    continue;
                }
            }

            let mut seen = HashSet::new();
            logs.retain(|log| {
                seen.insert(
                    json!([
                        log.get("blockHash"),
                        log.get("logIndex"),
                        log.get("address")
                    ])
                    .to_string(),
                )
            });
            self.window.store(current_window, Ordering::Relaxed);
            break (to, logs, child_contracts);
        };
        let mut timestamps = log_block_timestamps(&logs);
        let mut rows = Vec::new();
        for log in logs {
            if let Some(row) = decode_row(
                self.chain_id,
                &log,
                &self.events,
                &self.direct_addresses,
                &self.discovery_rules,
                &child_contracts,
            )? {
                rows.push(row);
            }
        }
        // Only blocks whose logs lacked `blockTimestamp` cost an extra RPC call.
        let blocks = rows
            .iter()
            .map(|row| row.block_number)
            .filter(|block| !timestamps.contains_key(block))
            .collect::<HashSet<_>>();
        let timestamp_results = stream::iter(blocks.into_iter().map(|block| async move {
            self.block_timestamp(block)
                .await
                .map(|timestamp| (block, timestamp))
        }))
        .buffer_unordered(8)
        .collect::<Vec<_>>()
        .await;
        for result in timestamp_results {
            let (block, timestamp) = result?;
            timestamps.insert(block, timestamp);
        }
        let decoded = rows
            .into_iter()
            .map(|row| {
                let timestamp = timestamps[&row.block_number];
                (row, timestamp)
            })
            .collect::<Vec<_>>();
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
        self.cursor.advance(to.saturating_add(1));
        self.metrics
            .record_count("streamling_blockchain_logs", decoded.len() as u64);
        rows_to_batch(self.schema.clone(), decoded)
    }
    async fn process_checkpoint_marker(&self, epoch: CheckpointEpoch) -> Result<(), PluginError> {
        self.cursor.mark(&epoch)
    }
    async fn process_checkpoint_finalizer(
        &self,
        epoch: CheckpointEpoch,
    ) -> Result<(), PluginError> {
        let Some(mark) = self.cursor.finalize(&epoch)? else {
            return Ok(());
        };
        let state = SourceState {
            next_block: mark.next_block,
            discovered_rows: self
                .discovered_rows
                .lock()
                .map_err(|_| PluginError::Internal("discovery registry mutex poisoned".into()))?
                .clone(),
        };
        self.state.put(state).await.map_err(PluginError::State)?;
        write_progress(self.progress_path.as_deref(), self.confirmations, mark)
    }
}

/// A source's position as the sinks see it. The host can process a checkpoint marker while
/// the batch that `generate_batch` last returned still waits to be sent, so that batch counts
/// as sent only when the next `generate_batch` call starts. The position recorded at each
/// marker is committed when its epoch is finalized, that is, once every sink has written it.
struct Cursor {
    /// First block after the last batch returned by `generate_batch`.
    returned: AtomicU64,
    /// First block after the last batch known to be sent downstream.
    sent: AtomicU64,
    /// Observed head and safe head from the latest `generate_batch` call.
    heads: Mutex<(u64, u64)>,
    marks: Mutex<BTreeMap<u64, Mark>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct Mark {
    next_block: u64,
    observed_head: u64,
    safe_head: u64,
}

impl Cursor {
    fn new(start_block: u64) -> Self {
        Self {
            returned: AtomicU64::new(start_block),
            sent: AtomicU64::new(start_block),
            heads: Mutex::new((0, 0)),
            marks: Mutex::new(BTreeMap::new()),
        }
    }

    fn restore(&self, next_block: u64) {
        self.returned.store(next_block, Ordering::Relaxed);
        self.sent.store(next_block, Ordering::Relaxed);
    }

    /// Start a `generate_batch` call and return the first block it should read.
    fn begin(&self, observed_head: u64, safe_head: u64) -> Result<u64, PluginError> {
        let next_block = self.returned.load(Ordering::Relaxed);
        self.sent.store(next_block, Ordering::Relaxed);
        *lock(&self.heads)? = (observed_head, safe_head);
        Ok(next_block)
    }

    /// Record that the batch being returned covers blocks before `next_block`.
    fn advance(&self, next_block: u64) {
        self.returned.store(next_block, Ordering::Relaxed);
    }

    fn mark(&self, epoch: &CheckpointEpoch) -> Result<(), PluginError> {
        let (observed_head, safe_head) = *lock(&self.heads)?;
        lock(&self.marks)?.insert(
            epoch.0,
            Mark {
                next_block: self.sent.load(Ordering::Relaxed),
                observed_head,
                safe_head,
            },
        );
        Ok(())
    }

    /// The position recorded at the latest marker up to `epoch`, which is now durable.
    fn finalize(&self, epoch: &CheckpointEpoch) -> Result<Option<Mark>, PluginError> {
        let mut marks = lock(&self.marks)?;
        let later = marks.split_off(&epoch.0.saturating_add(1));
        Ok(std::mem::replace(&mut *marks, later)
            .into_values()
            .next_back())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>, PluginError> {
    mutex
        .lock()
        .map_err(|_| PluginError::Internal("source cursor mutex poisoned".into()))
}

fn write_progress(path: Option<&Path>, confirmations: u64, mark: Mark) -> Result<(), PluginError> {
    let Some(path) = path else {
        return Ok(());
    };
    let indexed_through = mark.next_block.saturating_sub(1);
    let progress = BackfillProgress {
        indexed_through,
        observed_head: mark.observed_head,
        safe_head: mark.safe_head,
        confirmations,
        caught_up: indexed_through >= mark.safe_head,
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
struct BlockSourceState {
    next_block: u64,
}

#[derive(Clone, Debug)]
struct BlockRow {
    block_id: String,
    chain_id: u64,
    block_number: u64,
    block_hash: String,
    parent_hash: String,
    block_timestamp: u64,
    miner: Option<String>,
    gas_limit: u64,
    gas_used: u64,
    base_fee_per_gas: Option<String>,
    transaction_count: u64,
}

fn block_row(chain_id: u64, block: &Value) -> Result<BlockRow, PluginError> {
    let block_number = parse_hex(block.get("number").and_then(Value::as_str).unwrap_or("0x0"))?;
    Ok(BlockRow {
        block_id: format!("{chain_id}:{block_number}"),
        chain_id,
        block_number,
        block_hash: block
            .get("hash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        parent_hash: block
            .get("parentHash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        block_timestamp: parse_hex(
            block
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or("0x0"),
        )?,
        miner: block
            .get("miner")
            .or_else(|| block.get("author"))
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase),
        gas_limit: parse_hex(
            block
                .get("gasLimit")
                .and_then(Value::as_str)
                .unwrap_or("0x0"),
        )?,
        gas_used: parse_hex(
            block
                .get("gasUsed")
                .and_then(Value::as_str)
                .unwrap_or("0x0"),
        )?,
        base_fee_per_gas: block
            .get("baseFeePerGas")
            .and_then(Value::as_str)
            .map(str::to_owned),
        transaction_count: block
            .get("transactions")
            .and_then(Value::as_array)
            .map_or(0, |items| items.len() as u64),
    })
}

fn block_rows_to_batch(schema: SchemaRef, rows: Vec<BlockRow>) -> Result<RecordBatch, PluginError> {
    let mut block_numbers = UInt64Builder::new();
    let mut block_ids = StringBuilder::new();
    let mut chain_ids = UInt64Builder::new();
    let mut block_hashes = StringBuilder::new();
    let mut parent_hashes = StringBuilder::new();
    let mut timestamps = UInt64Builder::new();
    let mut miners = StringBuilder::new();
    let mut gas_limits = UInt64Builder::new();
    let mut gas_used = UInt64Builder::new();
    let mut base_fees = StringBuilder::new();
    let mut transaction_counts = UInt64Builder::new();
    let mut ops = StringBuilder::new();
    for row in rows {
        block_ids.append_value(row.block_id);
        chain_ids.append_value(row.chain_id);
        block_numbers.append_value(row.block_number);
        block_hashes.append_value(row.block_hash);
        parent_hashes.append_value(row.parent_hash);
        timestamps.append_value(row.block_timestamp);
        miners.append_option(row.miner);
        gas_limits.append_value(row.gas_limit);
        gas_used.append_value(row.gas_used);
        base_fees.append_option(row.base_fee_per_gas);
        transaction_counts.append_value(row.transaction_count);
        ops.append_value("i");
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(block_ids.finish()),
            Arc::new(chain_ids.finish()),
            Arc::new(block_numbers.finish()),
            Arc::new(block_hashes.finish()),
            Arc::new(parent_hashes.finish()),
            Arc::new(timestamps.finish()),
            Arc::new(miners.finish()),
            Arc::new(gas_limits.finish()),
            Arc::new(gas_used.finish()),
            Arc::new(base_fees.finish()),
            Arc::new(transaction_counts.finish()),
            Arc::new(ops.finish()),
        ],
    )
    .map_err(PluginError::ArrowError)
}

#[derive(Clone, Debug)]
struct TransactionRow {
    transaction_id: String,
    chain_id: u64,
    tx_hash: String,
    block_number: u64,
    block_hash: String,
    block_timestamp: u64,
    transaction_index: u64,
    from_address: String,
    to_address: Option<String>,
    value: String,
    gas: u64,
    gas_price: Option<String>,
    max_fee_per_gas: Option<String>,
    max_priority_fee_per_gas: Option<String>,
    input: String,
    method_id: Option<String>,
    nonce: u64,
    receipt_status: Option<u64>,
    receipt_gas_used: Option<u64>,
    receipt_effective_gas_price: Option<String>,
    contract_address: Option<String>,
    logs_count: Option<u64>,
}

fn block_chunks(from: u64, to: u64, size: usize) -> Vec<Vec<u64>> {
    (from..=to)
        .collect::<Vec<_>>()
        .chunks(size)
        .map(<[u64]>::to_vec)
        .collect()
}

fn receipts_by_hash(receipts: &[Value]) -> HashMap<String, Value> {
    receipts
        .iter()
        .filter_map(|receipt| {
            receipt
                .get("transactionHash")
                .and_then(Value::as_str)
                .map(|hash| (hash.to_owned(), receipt.clone()))
        })
        .collect()
}

fn transaction_row(
    chain_id: u64,
    block: &Value,
    tx: &Value,
    receipt: Option<&Value>,
    fallback_index: u64,
) -> Result<TransactionRow, PluginError> {
    let tx_hash = tx.get("hash").and_then(Value::as_str).unwrap_or_default();
    let input = tx
        .get("input")
        .or_else(|| tx.get("data"))
        .and_then(Value::as_str)
        .unwrap_or("0x")
        .to_owned();
    let method_id = (input.len() >= 10).then(|| input[..10].to_owned());
    Ok(TransactionRow {
        transaction_id: format!("{chain_id}:{tx_hash}"),
        chain_id,
        tx_hash: tx_hash.to_owned(),
        block_number: parse_hex(block.get("number").and_then(Value::as_str).unwrap_or("0x0"))?,
        block_hash: block
            .get("hash")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        block_timestamp: parse_hex(
            block
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or("0x0"),
        )?,
        transaction_index: tx
            .get("transactionIndex")
            .and_then(Value::as_str)
            .map(parse_hex)
            .transpose()?
            .unwrap_or(fallback_index),
        from_address: tx
            .get("from")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase(),
        to_address: tx
            .get("to")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase),
        value: tx
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("0x0")
            .to_owned(),
        gas: parse_hex(tx.get("gas").and_then(Value::as_str).unwrap_or("0x0"))?,
        gas_price: tx
            .get("gasPrice")
            .and_then(Value::as_str)
            .map(str::to_owned),
        max_fee_per_gas: tx
            .get("maxFeePerGas")
            .and_then(Value::as_str)
            .map(str::to_owned),
        max_priority_fee_per_gas: tx
            .get("maxPriorityFeePerGas")
            .and_then(Value::as_str)
            .map(str::to_owned),
        input,
        method_id,
        nonce: parse_hex(tx.get("nonce").and_then(Value::as_str).unwrap_or("0x0"))?,
        receipt_status: receipt
            .and_then(|value| value.get("status"))
            .and_then(Value::as_str)
            .map(parse_hex)
            .transpose()?,
        receipt_gas_used: receipt
            .and_then(|value| value.get("gasUsed"))
            .and_then(Value::as_str)
            .map(parse_hex)
            .transpose()?,
        receipt_effective_gas_price: receipt
            .and_then(|value| value.get("effectiveGasPrice"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        contract_address: receipt
            .and_then(|value| value.get("contractAddress"))
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase),
        logs_count: receipt
            .and_then(|value| value.get("logs"))
            .and_then(Value::as_array)
            .map(|items| items.len() as u64),
    })
}

fn transaction_rows_to_batch(
    schema: SchemaRef,
    rows: Vec<TransactionRow>,
) -> Result<RecordBatch, PluginError> {
    let mut strings = (0..13).map(|_| StringBuilder::new()).collect::<Vec<_>>();
    let mut chain_ids = UInt64Builder::new();
    let mut block_numbers = UInt64Builder::new();
    let mut timestamps = UInt64Builder::new();
    let mut indexes = UInt64Builder::new();
    let mut gas = UInt64Builder::new();
    let mut nonces = UInt64Builder::new();
    let mut receipt_statuses = UInt64Builder::new();
    let mut receipt_gas_used = UInt64Builder::new();
    let mut logs_counts = UInt64Builder::new();
    let mut ops = StringBuilder::new();
    for row in rows {
        strings[0].append_value(row.transaction_id);
        chain_ids.append_value(row.chain_id);
        strings[1].append_value(row.tx_hash);
        block_numbers.append_value(row.block_number);
        strings[2].append_value(row.block_hash);
        timestamps.append_value(row.block_timestamp);
        indexes.append_value(row.transaction_index);
        strings[3].append_value(row.from_address);
        strings[4].append_option(row.to_address);
        strings[5].append_value(row.value);
        gas.append_value(row.gas);
        strings[6].append_option(row.gas_price);
        strings[7].append_option(row.max_fee_per_gas);
        strings[8].append_option(row.max_priority_fee_per_gas);
        strings[9].append_value(row.input);
        strings[10].append_option(row.method_id);
        nonces.append_value(row.nonce);
        receipt_statuses.append_option(row.receipt_status);
        receipt_gas_used.append_option(row.receipt_gas_used);
        strings[11].append_option(row.receipt_effective_gas_price);
        strings[12].append_option(row.contract_address);
        logs_counts.append_option(row.logs_count);
        ops.append_value("i");
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(strings.remove(0).finish()),
            Arc::new(chain_ids.finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(block_numbers.finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(timestamps.finish()),
            Arc::new(indexes.finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(gas.finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(nonces.finish()),
            Arc::new(receipt_statuses.finish()),
            Arc::new(receipt_gas_used.finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(strings.remove(0).finish()),
            Arc::new(logs_counts.finish()),
            Arc::new(ops.finish()),
        ],
    )
    .map_err(PluginError::ArrowError)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedRow {
    row: DecodedRow,
    timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DecodedRow {
    chain_id: u64,
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
    chain_id: u64,
    log: &Value,
    events: &HashMap<H256, Vec<EventDescriptor>>,
    direct: &HashMap<String, String>,
    discovery_rules: &[DiscoverySpec],
    child_contracts: &HashMap<String, String>,
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
    let preferred = direct
        .get(&address)
        .or_else(|| child_contracts.get(&address));
    // A known contract's log decodes only with that contract's ABI; a topic its ABI does not
    // declare (for example an inherited `RoleGranted`) is skipped, not borrowed from another ABI.
    let descriptor = match preferred {
        Some(owner) => match candidates
            .iter()
            .find(|candidate| &candidate.owner == owner)
        {
            Some(descriptor) => descriptor,
            None => return Ok(None),
        },
        None if candidates.len() == 1 => &candidates[0],
        None => {
            return Err(PluginError::Execution(format!(
                "ambiguous event topic {topic0_text} for address {address}"
            )));
        }
    };
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
        event_id: format!("{chain_id}:{block_hash}:{log_index}"),
        chain_id,
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
    let mut chain_ids = UInt64Builder::new();
    let mut block_numbers = UInt64Builder::new();
    let mut timestamps = UInt64Builder::new();
    let mut indexes = UInt64Builder::new();
    for (row, timestamp) in rows {
        strings[0].append_value(row.event_id);
        chain_ids.append_value(row.chain_id);
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
            Arc::new(chain_ids.finish()),
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
fn parse_batch_size(options: &HashMap<String, String>) -> Result<usize, PluginInitializationError> {
    match parse_option(options, "rpc_batch_size", DEFAULT_BATCH_SIZE)? {
        0 => Err(PluginInitializationError::Configuration(
            "rpc_batch_size must be greater than zero".into(),
        )),
        value => Ok(value as usize),
    }
}
fn parse_concurrency(
    options: &HashMap<String, String>,
) -> Result<usize, PluginInitializationError> {
    match parse_option(options, "concurrency", DEFAULT_BLOCK_CONCURRENCY)? {
        0 => Err(PluginInitializationError::Configuration(
            "concurrency must be greater than zero".into(),
        )),
        value => Ok(value as usize),
    }
}
fn parse_optional(
    options: &HashMap<String, String>,
    key: &str,
) -> Result<Option<u64>, PluginInitializationError> {
    options
        .get(key)
        .map(|v| {
            v.parse().map(Some).map_err(|_| {
                PluginInitializationError::Configuration(format!("invalid {key}").into())
            })
        })
        .unwrap_or(Ok(None))
}
fn capped_safe_head(head: u64, confirmations: u64, end_block: Option<u64>) -> u64 {
    let safe_head = head.saturating_sub(confirmations);
    end_block.map_or(safe_head, |end| safe_head.min(end))
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
            || text.contains("range too large")
            || text.contains("log query timed out"))
}
/// Block timestamps carried on the logs themselves. Many providers add the
/// non-standard `blockTimestamp` field to `eth_getLogs` results.
fn log_block_timestamps(logs: &[Value]) -> HashMap<u64, u64> {
    logs.iter()
        .filter_map(|log| {
            let block = parse_hex(log.get("blockNumber")?.as_str()?).ok()?;
            let timestamp = parse_hex(log.get("blockTimestamp")?.as_str()?).ok()?;
            Some((block, timestamp))
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;

    #[test]
    fn cursor_commits_only_sent_batches_of_finalized_epochs() {
        let cursor = Cursor::new(100);
        assert_eq!(cursor.begin(1_000, 988).unwrap(), 100);
        cursor.advance(200);
        // The host processes a marker while the 100..199 batch still waits to be sent.
        cursor.mark(&CheckpointEpoch(1)).unwrap();
        assert_eq!(cursor.begin(1_010, 998).unwrap(), 200);
        cursor.advance(300);
        cursor.mark(&CheckpointEpoch(2)).unwrap();
        assert_eq!(cursor.begin(1_020, 1_008).unwrap(), 300);
        cursor.mark(&CheckpointEpoch(3)).unwrap();

        let first = cursor.finalize(&CheckpointEpoch(1)).unwrap().unwrap();
        assert_eq!(first.next_block, 100);
        assert_eq!((first.observed_head, first.safe_head), (1_000, 988));
        // Finalizing a later epoch covers any earlier ones still pending.
        let third = cursor.finalize(&CheckpointEpoch(3)).unwrap().unwrap();
        assert_eq!(third.next_block, 300);
        assert_eq!(cursor.finalize(&CheckpointEpoch(2)).unwrap(), None);
    }

    #[test]
    fn restored_cursor_resumes_from_saved_block() {
        let cursor = Cursor::new(0);
        cursor.restore(500);
        assert_eq!(cursor.begin(1_000, 988).unwrap(), 500);
        cursor.mark(&CheckpointEpoch(7)).unwrap();
        assert_eq!(
            cursor
                .finalize(&CheckpointEpoch(7))
                .unwrap()
                .unwrap()
                .next_block,
            500
        );
    }

    #[test]
    fn block_rows_include_chain_scoped_id() {
        let row = block_row(
            8453,
            &json!({
                "number": "0x7b",
                "hash": "0xabc",
                "parentHash": "0xdef",
                "timestamp": "0x65",
                "miner": "0x1111111111111111111111111111111111111111",
                "gasLimit": "0x10",
                "gasUsed": "0x08",
                "baseFeePerGas": "0x01",
                "transactions": ["0xaaa", "0xbbb"]
            }),
        )
        .unwrap();

        assert_eq!(row.block_id, "8453:123");
        assert_eq!(row.chain_id, 8453);
        assert_eq!(row.transaction_count, 2);
    }

    #[test]
    fn event_batches_include_chain_id() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Utf8, false),
            Field::new("chain_id", DataType::UInt64, false),
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
        let batch = rows_to_batch(
            schema,
            vec![(
                DecodedRow {
                    event_id: "8453:0xabc:0".into(),
                    chain_id: 8453,
                    owner: "token".into(),
                    event: "Transfer".into(),
                    address: "0x1111111111111111111111111111111111111111".into(),
                    block_number: 123,
                    block_hash: "0xabc".into(),
                    tx_hash: "0xtx".into(),
                    log_index: 0,
                    topic0: "0xtopic".into(),
                    fields: "{}".into(),
                    discovered_address: None,
                    discovery_rule: None,
                },
                101,
            )],
        )
        .unwrap();
        let chain_id = batch
            .column_by_name("chain_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(chain_id.value(0), 8453);
    }

    #[test]
    fn transaction_rows_include_receipt_fields() {
        let row = transaction_row(
            8453,
            &json!({
                "number": "0x7b",
                "hash": "0xblock",
                "timestamp": "0x65"
            }),
            &json!({
                "hash": "0xtx",
                "transactionIndex": "0x2",
                "from": "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                "to": null,
                "value": "0x10",
                "gas": "0x5208",
                "gasPrice": "0x3b9aca00",
                "input": "0xa9059cbb00000000",
                "nonce": "0x1"
            }),
            Some(&json!({
                "status": "0x1",
                "gasUsed": "0x5208",
                "effectiveGasPrice": "0x3b9aca00",
                "contractAddress": "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                "logs": [{}, {}]
            })),
            0,
        )
        .unwrap();

        assert_eq!(row.transaction_id, "8453:0xtx");
        assert_eq!(row.method_id.as_deref(), Some("0xa9059cbb"));
        assert_eq!(row.receipt_status, Some(1));
        assert_eq!(row.logs_count, Some(2));
        assert_eq!(
            row.contract_address.as_deref(),
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
    }

    #[test]
    fn log_block_timestamps_skip_logs_without_the_field() {
        let timestamps = log_block_timestamps(&[
            json!({"blockNumber": "0x7b", "blockTimestamp": "0x65"}),
            json!({"blockNumber": "0x7c"}),
            json!({"blockNumber": "0x7d", "blockTimestamp": null}),
        ]);

        assert_eq!(timestamps, HashMap::from([(123, 101)]));
    }

    #[test]
    fn log_query_timeouts_reduce_the_window() {
        let error = PluginError::Execution(
            "eth_getLogs: {\"code\":-32000,\"message\":\"log query timed out\"}".into(),
        );

        assert!(is_get_logs_range_error(&error));
    }

    #[test]
    fn child_address_selects_its_own_contract_descriptor() {
        let event: Event = serde_json::from_value(json!({
            "anonymous": false,
            "inputs": [],
            "name": "Ping",
            "type": "event"
        }))
        .unwrap();
        let topic = event.signature();
        let child_address = "0x2222222222222222222222222222222222222222";
        let events = HashMap::from([(
            topic,
            vec![
                EventDescriptor {
                    owner: "child_a".into(),
                    event: event.clone(),
                },
                EventDescriptor {
                    owner: "child_b".into(),
                    event,
                },
            ],
        )]);
        let child_contracts = HashMap::from([(child_address.to_owned(), "child_b".to_owned())]);
        let row = decode_row(
            4663,
            &json!({
                "address": child_address,
                "topics": [format!("{topic:#x}")],
                "data": "0x",
                "blockNumber": "0x1",
                "blockHash": "0xblock",
                "transactionHash": "0xtx",
                "logIndex": "0x0"
            }),
            &events,
            &HashMap::new(),
            &[],
            &child_contracts,
        )
        .unwrap()
        .unwrap();

        assert_eq!(row.owner, "child_b");
    }

    #[test]
    fn known_address_skips_topics_its_abi_does_not_declare() {
        let event: Event = serde_json::from_value(json!({
            "anonymous": false,
            "inputs": [],
            "name": "RoleGranted",
            "type": "event"
        }))
        .unwrap();
        let topic = event.signature();
        let flow = "0x3333333333333333333333333333333333333333";
        let log = json!({
            "address": flow,
            "topics": [format!("{topic:#x}")],
            "data": "0x",
            "blockNumber": "0x1",
            "blockHash": "0xblock",
            "transactionHash": "0xtx",
            "logIndex": "0x0"
        });
        let direct = HashMap::from([(flow.to_owned(), "flow".to_owned())]);
        let mine = EventDescriptor {
            owner: "mine".into(),
            event: event.clone(),
        };
        let reward = EventDescriptor {
            owner: "reward".into(),
            event,
        };
        for candidates in [vec![mine.clone()], vec![mine, reward]] {
            let events = HashMap::from([(topic, candidates)]);
            let row = decode_row(4663, &log, &events, &direct, &[], &HashMap::new()).unwrap();
            assert!(row.is_none());
        }
    }
}
