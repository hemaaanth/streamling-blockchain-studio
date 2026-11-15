use crate::{
    config::{DiscoveryRule, ProjectConfig},
    database::{Backend, BackendKind},
    output::{CodedError, ErrorCode},
    rpc, status,
};
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};
use streamling_blockchain_decode::{self as decode, B256, EventIndex, parse_hex};

struct DecodedEvent {
    event_id: String,
    contract_alias: String,
    event_name: String,
    block_number: u64,
    block_hash: String,
    data: String,
}

pub async fn diff(root: &Path, from: u64, to: u64, backend: Option<BackendKind>) -> Result<Value> {
    let config = ProjectConfig::load(root)?;
    let backend = Backend::open(root, &config, backend)?;
    diff_with(root, &config, &backend, from, to).await
}

async fn diff_with(
    root: &Path,
    config: &ProjectConfig,
    backend: &Backend,
    from: u64,
    to: u64,
) -> Result<Value> {
    if from > to {
        bail!(CodedError::new(
            ErrorCode::Validation,
            "--from must be less than or equal to --to"
        ))
    }
    let urls = config.rpc_urls_value()?;
    let client = rpc::client()?;
    let mut events = EventIndex::new();
    let mut direct = HashMap::new();
    let mut starts = HashMap::new();
    let mut direct_topics = HashSet::new();
    for contract in &config.contracts {
        let address = contract.address.to_ascii_lowercase();
        starts.insert(
            address.clone(),
            contract.start_block.unwrap_or(config.start_block),
        );
        direct.insert(address, contract.alias.clone());
        direct_topics.extend(load_events(
            &contract.alias,
            &root.join(&contract.abi),
            &mut events,
        )?);
    }
    let mut child_topics = HashSet::new();
    for rule in &config.discovery_rules {
        child_topics.extend(load_events(
            &rule.child_contract,
            &root.join(&rule.child_abi),
            &mut events,
        )?);
    }

    // The same two queries the event source makes: the configured contracts with their own
    // topics, then every discovered child with the child topics.
    let mut addresses = direct.keys().cloned().collect::<Vec<_>>();
    addresses.sort_unstable();
    let mut logs = Vec::new();
    for chunk in addresses.chunks(MAX_ADDRESSES_PER_LOG_FILTER) {
        let filter = json!({"address": chunk, "topics": [sorted_topics(&direct_topics)]});
        logs.extend(get_logs(&client, &urls, &filter, from, to).await?);
    }
    // The event source skips a configured contract's logs before its start block, also
    // when the contract is a discovered child too.
    let before_start = |log: &Value, event: &DecodedEvent| {
        let address = log["address"]
            .as_str()
            .unwrap_or_default()
            .to_ascii_lowercase();
        starts
            .get(&address)
            .is_some_and(|start| event.block_number < *start)
    };
    let mut children = stored_children(backend, config).await?;
    let mut decoded = Vec::new();
    for log in &logs {
        if let Some(event) = decode_log(config.chain_id, log, &events, &direct, &children)? {
            if before_start(log, &event) {
                continue;
            }
            if let Some((address, child)) = discovered_child(config, &direct, log, &event) {
                add_child(&mut children, address, log_position(log)?, child);
            }
            decoded.push(event);
        }
    }
    if !children.is_empty() && !child_topics.is_empty() {
        let mut child_addresses = children.keys().cloned().collect::<Vec<_>>();
        child_addresses.sort_unstable();
        let mut child_logs = Vec::new();
        for chunk in child_addresses.chunks(MAX_ADDRESSES_PER_LOG_FILTER) {
            let filter = json!({"address": chunk, "topics": [sorted_topics(&child_topics)]});
            child_logs.extend(get_logs(&client, &urls, &filter, from, to).await?);
        }
        for log in &child_logs {
            decoded.extend(
                decode_log(config.chain_id, log, &events, &direct, &children)?
                    .filter(|event| !before_start(log, event)),
            );
        }
    }
    let mut seen = HashSet::new();
    decoded.retain(|event| seen.insert(event.event_id.clone()));
    let stored = stored_events(backend, config.chain_id, from, to).await?;
    Ok(compare(stored, from, to, decoded))
}

const MAX_ADDRESSES_PER_LOG_FILTER: usize = 500;

fn sorted_topics(topics: &HashSet<B256>) -> Vec<String> {
    let mut topics = topics
        .iter()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    topics.sort();
    topics
}

/// `eth_getLogs` over `from..=to`, halving any range the provider refuses as too wide or
/// too large.
async fn get_logs(
    client: &reqwest::Client,
    urls: &str,
    filter: &Value,
    from: u64,
    to: u64,
) -> Result<Vec<Value>> {
    let mut pending = vec![(from, to)];
    let mut logs = Vec::new();
    while let Some((start, end)) = pending.pop() {
        let mut filter = filter.clone();
        filter["fromBlock"] = format!("0x{start:x}").into();
        filter["toBlock"] = format!("0x{end:x}").into();
        match rpc::rpc_any(client, urls, "eth_getLogs", json!([filter])).await {
            Ok(value) => logs.extend(value.as_array().cloned().unwrap_or_default()),
            Err(error) if start < end && is_too_wide(&error) => {
                let middle = start + (end - start) / 2;
                pending.push((middle + 1, end));
                pending.push((start, middle));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(logs)
}

/// Provider phrases for a log query that covers too many blocks or returns too many logs.
fn is_too_wide(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}").to_ascii_lowercase();
    if text.contains("rate limit") || text.contains("too many requests") {
        return false;
    }
    [
        "range",
        "more than",
        "response size",
        "too many results",
        "timed out",
        "timeout",
    ]
    .iter()
    .any(|phrase| text.contains(phrase))
}

/// The child address and alias a parent's discovery event names, if the event matches a rule.
fn discovered_child(
    config: &ProjectConfig,
    direct: &HashMap<String, String>,
    log: &Value,
    event: &DecodedEvent,
) -> Option<(String, String)> {
    let address = log.get("address")?.as_str()?.to_ascii_lowercase();
    let rule = config.discovery_rules.iter().find(|rule| {
        rule.parent_contract == event.contract_alias
            && rule.discovery_event == event.event_name
            && direct.get(&address) == Some(&rule.parent_contract)
    })?;
    let data: Value = serde_json::from_str(&event.data).ok()?;
    let address = data
        .get(&rule.address_field)?
        .as_str()?
        .to_ascii_lowercase();
    Some((address, rule.child_contract.clone()))
}

/// Child owners by lowercase address, with the (block, log index) of their discovery.
type Children = HashMap<String, ((u64, u64), String)>;

/// Keeps the earliest discovery of each child address. As in backfill, the first discovery
/// owns a child, even if another rule rediscovers it later.
fn add_child(children: &mut Children, address: String, position: (u64, u64), child: String) {
    match children.get(&address) {
        Some((first, _)) if *first <= position => {}
        _ => {
            children.insert(address, (position, child));
        }
    }
}

/// The block and log index of a log.
fn log_position(log: &Value) -> Result<(u64, u64)> {
    let field = |name: &str| parse_hex(log.get(name).and_then(Value::as_str).unwrap_or_default());
    Ok((field("blockNumber")?, field("logIndex")?))
}

/// Every stored discovery event, as backfill applies them: only the first rule for a parent
/// event counts.
async fn stored_children(backend: &Backend, config: &ProjectConfig) -> Result<Children> {
    let mut seen = HashSet::new();
    let mut children = Children::new();
    for rule in &config.discovery_rules {
        if !seen.insert((&rule.parent_contract, &rule.discovery_event)) {
            continue;
        }
        for (block_number, log_index, text) in
            discovery_rows(backend, config.chain_id, rule).await?
        {
            let address = serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|fields| fields.get(&rule.address_field)?.as_str().map(str::to_owned));
            if let Some(address) = address {
                add_child(
                    &mut children,
                    address.to_ascii_lowercase(),
                    (block_number, log_index),
                    rule.child_contract.clone(),
                );
            }
        }
    }
    Ok(children)
}

/// Block, log index, and fields of every stored discovery event for `rule`.
async fn discovery_rows(
    backend: &Backend,
    chain_id: u64,
    rule: &DiscoveryRule,
) -> Result<Vec<(u64, u64, String)>> {
    match backend {
        Backend::Sqlite(conn) => {
            let mut statement = conn.prepare(
                "SELECT block_number, log_index, data FROM events
                 WHERE chain_id = ?1 AND contract_alias = ?2 AND event_name = ?3",
            )?;
            Ok(statement
                .query_map(
                    params![chain_id, rule.parent_contract, rule.discovery_event],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        }
        Backend::ClickHouse { client, table } => {
            let rows = client
                .query(
                    &format!(
                        "SELECT block_number, log_index, fields_json FROM {} FINAL
                         WHERE is_deleted = 0 AND chain_id = {chain_id} AND contract_alias = '{}'
                           AND event_name = '{}'",
                        client.qualified(table),
                        sql_string(&rule.parent_contract),
                        sql_string(&rule.discovery_event),
                    ),
                    usize::MAX,
                )
                .await?;
            Ok(rows["rows"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|row| {
                    Some((
                        row["block_number"].as_u64()?,
                        row["log_index"].as_u64()?,
                        row["fields_json"].as_str()?.to_owned(),
                    ))
                })
                .collect())
        }
    }
}

fn sql_string(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\'', "\\'")
}

pub async fn audit_once(
    root: &Path,
    window_blocks: u64,
    backend: Option<BackendKind>,
) -> Result<Value> {
    let config = ProjectConfig::load(root)?;
    let backend = Backend::open(root, &config, backend)?;
    let snapshot = status::live(root).await?;
    let indexed = snapshot["backfill"]["indexed_through"]
        .as_u64()
        .ok_or_else(|| {
            CodedError::new(ErrorCode::NotFound, "backfill has not started")
                .next("streamling-blockchain dev")
        })?;
    let start = snapshot["backfill"]["start_block"].as_u64().unwrap_or(0);
    let window = window_blocks.max(1);
    let recent_to = indexed;
    let recent_from = recent_to.saturating_sub(window - 1).max(start);
    let mut checks =
        vec![audit_range(root, &config, &backend, "recent", recent_from, recent_to).await?];
    if recent_to > start + window {
        let span = recent_to - start - window;
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let random_from = start + seed.wrapping_mul(1_103_515_245).wrapping_add(12_345) % span;
        checks.push(
            audit_range(
                root,
                &config,
                &backend,
                "sample",
                random_from,
                random_from + window - 1,
            )
            .await?,
        );
    }
    Ok(
        json!({"ok": checks.iter().all(|check| check["ok"].as_bool() == Some(true)), "checks": checks}),
    )
}

/// Prints one JSON line per audit; in `--json` mode they go to stderr.
pub async fn audit_continuous(
    root: &Path,
    json: bool,
    interval: std::time::Duration,
    window_blocks: u64,
    backend: Option<BackendKind>,
) -> Result<()> {
    loop {
        // One failed audit (an RPC outage, a busy database) is reported, not fatal.
        let report = match audit_once(root, window_blocks, backend).await {
            Ok(report) => report,
            Err(error) => {
                let (code, _) = crate::output::classify(&error);
                json!({"ok": false, "error": format!("{error:#}"), "code": code})
            }
        };
        let line = serde_json::to_string(&report)?;
        if json {
            eprintln!("{line}");
        } else {
            println!("{line}");
        }
        tokio::time::sleep(interval).await;
    }
}

async fn audit_range(
    root: &Path,
    config: &ProjectConfig,
    backend: &Backend,
    label: &str,
    from: u64,
    to: u64,
) -> Result<Value> {
    let mut result = diff_with(root, config, backend, from, to).await?;
    if let Value::Object(object) = &mut result {
        object.insert(
            "indexed_tables".into(),
            compare_optional_indexed_tables(config, backend, from, to).await?,
        );
    }
    record_audit(root, config, backend, label, &result).await?;
    Ok(json!({
        "label": label,
        "from_block": from,
        "to_block": to,
        "ok": result["ok"],
        "chain_events": result["chain_events"],
        "stored_events": result["stored_events"],
        "missing": result["missing"].as_array().map_or(0, Vec::len),
        "extra": result["extra"].as_array().map_or(0, Vec::len),
        "changed": result["changed"].as_array().map_or(0, Vec::len),
        "indexed_tables": result["indexed_tables"],
    }))
}

/// Stores the audit summary next to the data it checked: `quality_replay_checks` in SQLite,
/// or `<events table>_quality_replay_checks` in ClickHouse.
async fn record_audit(
    root: &Path,
    config: &ProjectConfig,
    backend: &Backend,
    label: &str,
    result: &Value,
) -> Result<()> {
    let count = |key: &str| result[key].as_array().map_or(0, Vec::len) as u64;
    let ok = result["ok"].as_bool() == Some(true);
    let from_block = result["from_block"].as_u64().unwrap_or_default();
    let to_block = result["to_block"].as_u64().unwrap_or_default();
    let chain_events = result["chain_events"].as_u64().unwrap_or_default();
    let stored_events = result["stored_events"].as_u64().unwrap_or_default();
    let detail_json = serde_json::to_string(result)?;
    match backend {
        Backend::Sqlite(_) => {
            let conn = Connection::open(config.absolute_database(root))?;
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS quality_replay_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    checked_at INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    from_block INTEGER NOT NULL,
                    to_block INTEGER NOT NULL,
                    ok INTEGER NOT NULL,
                    chain_events INTEGER NOT NULL,
                    stored_events INTEGER NOT NULL,
                    missing_count INTEGER NOT NULL,
                    extra_count INTEGER NOT NULL,
                    changed_count INTEGER NOT NULL,
                    detail_json TEXT NOT NULL
                );",
            )?;
            conn.execute(
                "INSERT INTO quality_replay_checks (
                    checked_at, label, from_block, to_block, ok, chain_events, stored_events,
                    missing_count, extra_count, changed_count, detail_json
                ) VALUES (unixepoch(), ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    label,
                    from_block,
                    to_block,
                    ok,
                    chain_events,
                    stored_events,
                    count("missing"),
                    count("extra"),
                    count("changed"),
                    detail_json,
                ],
            )?;
        }
        Backend::ClickHouse { client, table } => {
            let audit_table = format!("{table}_quality_replay_checks");
            client
                .execute(&format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        id UUID DEFAULT generateUUIDv4(),
                        checked_at DateTime DEFAULT now(),
                        label LowCardinality(String),
                        from_block UInt64,
                        to_block UInt64,
                        ok UInt8,
                        chain_events UInt64,
                        stored_events UInt64,
                        missing_count UInt64,
                        extra_count UInt64,
                        changed_count UInt64,
                        detail_json String
                    )
                    ENGINE = MergeTree
                    ORDER BY (checked_at, label)",
                    client.qualified(&audit_table)
                ))
                .await?;
            client
                .insert_json_rows(
                    &audit_table,
                    &[json!({
                        "label": label,
                        "from_block": from_block,
                        "to_block": to_block,
                        "ok": u8::from(ok),
                        "chain_events": chain_events,
                        "stored_events": stored_events,
                        "missing_count": count("missing"),
                        "extra_count": count("extra"),
                        "changed_count": count("changed"),
                        "detail_json": detail_json,
                    })],
                )
                .await?;
        }
    }
    Ok(())
}

/// Stored events in the block range, keyed by event ID, in the shape `compare` expects.
async fn stored_events(
    backend: &Backend,
    chain_id: u64,
    from: u64,
    to: u64,
) -> Result<HashMap<String, Value>> {
    match backend {
        Backend::Sqlite(conn) => sqlite_stored_events(conn, from, to),
        Backend::ClickHouse { client, table } => {
            let rows = client
                .query(
                    &format!(
                        "SELECT event_id, contract_alias, event_name, block_number, block_hash, fields_json
                         FROM {} FINAL
                         WHERE is_deleted = 0 AND chain_id = {chain_id} AND block_number BETWEEN {from} AND {to}",
                        client.qualified(table)
                    ),
                    usize::MAX,
                )
                .await?;
            rows["rows"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|row| {
                    let event_id = row["event_id"]
                        .as_str()
                        .context("ClickHouse event row has no event_id")?;
                    Ok((
                        event_id.to_owned(),
                        json!({
                            "contract_alias": row["contract_alias"],
                            "event_name": row["event_name"],
                            "block_number": row["block_number"],
                            "block_hash": row["block_hash"],
                            "data": row["fields_json"],
                        }),
                    ))
                })
                .collect()
        }
    }
}

fn sqlite_stored_events(conn: &Connection, from: u64, to: u64) -> Result<HashMap<String, Value>> {
    let mut statement = conn.prepare(
        "SELECT event_id, contract_alias, event_name, block_number, block_hash, data FROM events WHERE block_number BETWEEN ?1 AND ?2",
    )?;
    let mut rows = statement.query([from, to])?;
    let mut stored = HashMap::new();
    while let Some(row) = rows.next()? {
        stored.insert(
            row.get::<_, String>(0)?,
            json!({
                "contract_alias": row.get::<_, String>(1)?,
                "event_name": row.get::<_, String>(2)?,
                "block_number": row.get::<_, u64>(3)?,
                "block_hash": row.get::<_, String>(4)?,
                "data": row.get::<_, String>(5)?,
            }),
        );
    }
    Ok(stored)
}

fn compare(
    stored: HashMap<String, Value>,
    from: u64,
    to: u64,
    decoded: Vec<DecodedEvent>,
) -> Value {
    let mut chain_ids = HashSet::new();
    let mut missing = Vec::new();
    let mut changed = Vec::new();
    for event in decoded {
        chain_ids.insert(event.event_id.clone());
        let expected = json!({
            "contract_alias": event.contract_alias,
            "event_name": event.event_name,
            "block_number": event.block_number,
            "block_hash": event.block_hash,
            "data": event.data,
        });
        match stored.get(&event.event_id) {
            Some(actual) if *actual == expected => {}
            Some(actual) => changed
                .push(json!({"event_id": event.event_id, "expected": expected, "actual": actual})),
            None => missing.push(json!({"event_id": event.event_id, "expected": expected})),
        }
    }
    let extra = stored
        .keys()
        .filter(|event_id| !chain_ids.contains(*event_id))
        .cloned()
        .collect::<Vec<_>>();
    json!({
        "from_block": from,
        "to_block": to,
        "chain_events": chain_ids.len(),
        "stored_events": stored.len(),
        "missing": missing,
        "extra": extra,
        "changed": changed,
        "ok": missing.is_empty() && extra.is_empty() && changed.is_empty(),
    })
}

async fn compare_optional_indexed_tables(
    config: &ProjectConfig,
    backend: &Backend,
    from: u64,
    to: u64,
) -> Result<Value> {
    let (blocks_table, transactions_table) = match backend {
        Backend::Sqlite(_) => ("evm_blocks".to_owned(), "evm_transactions".to_owned()),
        Backend::ClickHouse { table, .. } => {
            (format!("{table}_blocks"), format!("{table}_transactions"))
        }
    };
    let has_blocks = table_exists(backend, &blocks_table).await?;
    let has_transactions = table_exists(backend, &transactions_table).await?;
    if !has_blocks && !has_transactions {
        return Ok(Value::Null);
    }
    let urls = config.rpc_urls_value()?;
    let client = rpc::client()?;
    let mut chain_blocks = 0_u64;
    let mut chain_transactions = 0_u64;
    for block in from..=to {
        let value = rpc::rpc_any(
            &client,
            &urls,
            "eth_getBlockByNumber",
            json!([format!("0x{block:x}"), false]),
        )
        .await?;
        if value.is_null() {
            continue;
        }
        chain_blocks += 1;
        chain_transactions += value
            .get("transactions")
            .and_then(Value::as_array)
            .map_or(0, |items| items.len() as u64);
    }
    let stored_blocks = if has_blocks {
        Some(count_rows(backend, &blocks_table, config.chain_id, from, to).await?)
    } else {
        None
    };
    let stored_transactions = if has_transactions {
        Some(count_rows(backend, &transactions_table, config.chain_id, from, to).await?)
    } else {
        None
    };
    Ok(json!({
        "blocks": stored_blocks.map(|stored| json!({
            "chain": chain_blocks,
            "stored": stored,
            "ok": stored == chain_blocks,
        })),
        "transactions": stored_transactions.map(|stored| json!({
            "chain": chain_transactions,
            "stored": stored,
            "ok": stored == chain_transactions,
        })),
    }))
}

async fn table_exists(backend: &Backend, table: &str) -> Result<bool> {
    match backend {
        Backend::Sqlite(conn) => sqlite_table_exists(conn, table),
        Backend::ClickHouse { client, .. } => client.table_exists(table).await,
    }
}

async fn count_rows(
    backend: &Backend,
    table: &str,
    chain_id: u64,
    from: u64,
    to: u64,
) -> Result<u64> {
    match backend {
        Backend::Sqlite(conn) => sqlite_count_rows(conn, table, chain_id, from, to),
        Backend::ClickHouse { client, .. } => {
            let rows = client
                .query(
                    &format!(
                        "SELECT count() AS stored FROM {} FINAL WHERE is_deleted = 0 AND chain_id = {chain_id} AND block_number BETWEEN {from} AND {to}",
                        client.qualified(table)
                    ),
                    1,
                )
                .await?;
            rows["rows"][0]["stored"]
                .as_u64()
                .context("ClickHouse count returned no number")
        }
    }
}

fn sqlite_table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [table],
        |row| row.get::<_, bool>(0),
    )?)
}

fn sqlite_count_rows(
    conn: &Connection,
    table: &str,
    chain_id: u64,
    from: u64,
    to: u64,
) -> Result<u64> {
    let sql = format!(
        "SELECT count(*) FROM {table} WHERE chain_id = ?1 AND block_number BETWEEN ?2 AND ?3"
    );
    Ok(conn.query_row(&sql, params![chain_id, from, to], |row| row.get(0))?)
}

fn decode_log(
    chain_id: u64,
    log: &Value,
    events: &EventIndex,
    direct: &HashMap<String, String>,
    children: &Children,
) -> Result<Option<DecodedEvent>> {
    let address = log
        .get("address")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    // Every queried log comes from a known contract, and decodes only with that contract's
    // ABI, as in the event source.
    let Some(owner) = direct
        .get(&address)
        .or_else(|| children.get(&address).map(|(_, child)| child))
    else {
        return Ok(None);
    };
    let owners = HashMap::from([(address, owner.clone())]);
    let Some(decoded) = decode::decode_log(log, events, &[&owners])? else {
        return Ok(None);
    };
    Ok(Some(DecodedEvent {
        event_id: decoded.event_id(chain_id),
        contract_alias: decoded.descriptor.owner.clone(),
        event_name: decoded.descriptor.event.name.clone(),
        block_number: decoded.block_number,
        block_hash: decoded.block_hash,
        data: Value::Object(decoded.fields).to_string(),
    }))
}

fn load_events(owner: &str, path: &Path, target: &mut EventIndex) -> Result<Vec<B256>> {
    let events = decode::read_abi_events(path)?;
    Ok(decode::index_events(owner, events, target)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_reports_missing_extra_and_changed_events() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE events (event_id TEXT PRIMARY KEY, contract_alias TEXT, event_name TEXT, block_number INTEGER, block_hash TEXT, data TEXT);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO events VALUES ('kept', 'token', 'Transfer', 1, '0xaaa', '{\"value\":\"1\"}'), ('extra', 'token', 'Transfer', 1, '0xaaa', '{}')",
            [],
        )
        .unwrap();
        let result = compare(
            sqlite_stored_events(&conn, 1, 1).unwrap(),
            1,
            1,
            vec![DecodedEvent {
                event_id: "kept".into(),
                contract_alias: "token".into(),
                event_name: "Transfer".into(),
                block_number: 1,
                block_hash: "0xaaa".into(),
                data: "{\"value\":\"2\"}".into(),
            }],
        );
        assert_eq!(result["ok"], false);
        assert_eq!(result["changed"].as_array().unwrap().len(), 1);
        assert_eq!(result["extra"].as_array().unwrap(), &[json!("extra")]);
    }

    #[tokio::test]
    async fn clickhouse_backend_reads_current_rows_and_records_audits() {
        use crate::clickhouse::{
            apply_schema_with,
            tests::{clickhouse_config, event_row, test_client},
        };
        let database = format!("sbs_test_replay_{}", std::process::id());
        let Some(client) = test_client(&database) else {
            eprintln!("skipped: STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_URL is not set");
            return;
        };
        let config = clickhouse_config(&database, "events");
        apply_schema_with(&client, &config).await.unwrap();
        let mut other_chain = event_row("5:0xccc:0", 10);
        other_chain["chain_id"] = json!(5);
        let mut deleted = event_row("1:0xddd:0", 10);
        deleted["is_deleted"] = json!(1);
        client
            .insert_json_rows(
                "events",
                &[
                    event_row("1:0xaaa:0", 10),
                    event_row("1:0xaaa:0", 10),
                    event_row("1:0xaaa:1", 12),
                    other_chain,
                    deleted,
                ],
            )
            .await
            .unwrap();
        client
            .insert_json_rows(
                "events_blocks",
                &[json!({
                    "block_id": "1:10", "chain_id": 1, "block_number": 10, "block_hash": "0xaaa",
                    "parent_hash": "0x999", "block_timestamp": 1_700_000_000, "miner": null,
                    "gas_limit": 1, "gas_used": 1, "base_fee_per_gas": null,
                    "transaction_count": 1, "is_deleted": 0,
                })],
            )
            .await
            .unwrap();
        let backend = Backend::ClickHouse {
            client: client.clone(),
            table: "events".into(),
        };

        let stored = stored_events(&backend, 1, 10, 11).await.unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(
            stored["1:0xaaa:0"],
            json!({
                "contract_alias": "token",
                "event_name": "Transfer",
                "block_number": 10,
                "block_hash": "0xaaa",
                "data": "{\"value\":\"1\"}",
            })
        );
        assert!(table_exists(&backend, "events_blocks").await.unwrap());
        assert!(!table_exists(&backend, "events_transactions").await.unwrap());
        assert_eq!(
            count_rows(&backend, "events_blocks", 1, 10, 10)
                .await
                .unwrap(),
            1
        );

        let result = compare(stored, 10, 11, vec![]);
        record_audit(Path::new("."), &config, &backend, "recent", &result)
            .await
            .unwrap();
        let audits = client
            .query(
                "SELECT label, ok, stored_events, extra_count FROM events_quality_replay_checks",
                10,
            )
            .await
            .unwrap();
        assert_eq!(
            audits["rows"],
            json!([{"label": "recent", "ok": 0, "stored_events": 1, "extra_count": 1}])
        );

        client
            .execute(&format!("DROP DATABASE {database}"))
            .await
            .unwrap();
    }

    /// Runs `tests/mock_rpc.py` on a free port; the process stops when this drops.
    struct MockRpc(std::process::Child, String);

    impl Drop for MockRpc {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn start_mock_rpc(max_range: &str) -> MockRpc {
        use std::io::BufRead;
        let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut child = std::process::Command::new("python3")
            .arg(repo.join("tests/mock_rpc.py"))
            .args(["0", max_range])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("python3 runs tests/mock_rpc.py");
        let mut port = String::new();
        std::io::BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut port)
            .unwrap();
        let url = format!("http://127.0.0.1:{}", port.trim());
        MockRpc(child, url)
    }

    #[tokio::test]
    async fn diff_follows_discovered_children_and_splits_wide_ranges() {
        // The mock refuses ranges over 3 blocks, so 90..=110 must be split; the factory
        // log names a child whose log only the child query returns.
        let rpc = start_mock_rpc("3");
        let abi = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/transfer_abi.json");
        let config: ProjectConfig = toml::from_str(&format!(
            r#"
            chain = "ethereum"
            chain_id = 1
            rpc_url = "{}"
            database = "events.db"
            start_block = 90
            [[contracts]]
            alias = "factory"
            address = "0x1111111111111111111111111111111111111111"
            abi = "{abi}"
            [[discovery_rules]]
            parent_contract = "factory"
            discovery_event = "Transfer"
            address_field = "to"
            child_contract = "token"
            child_abi = "{abi}"
            "#,
            rpc.1,
            abi = abi.display(),
        ))
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE events (event_id TEXT PRIMARY KEY, chain_id INTEGER, contract_alias TEXT, event_name TEXT, block_number INTEGER, block_hash TEXT, log_index INTEGER, data TEXT);",
        )
        .unwrap();
        let backend = Backend::Sqlite(conn);

        let first = diff_with(Path::new("."), &config, &backend, 90, 110)
            .await
            .unwrap();
        assert_eq!(first["chain_events"], 2);
        let missing = first["missing"].as_array().unwrap();
        let owners = missing
            .iter()
            .map(|event| event["expected"]["contract_alias"].as_str().unwrap())
            .collect::<HashSet<_>>();
        assert_eq!(owners, HashSet::from(["factory", "token"]));

        let Backend::Sqlite(conn) = &backend else {
            unreachable!()
        };
        for event in missing {
            let expected = &event["expected"];
            conn.execute(
                "INSERT INTO events VALUES (?1, 1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    event["event_id"].as_str(),
                    expected["contract_alias"].as_str(),
                    expected["event_name"].as_str(),
                    expected["block_number"].as_u64(),
                    expected["block_hash"].as_str(),
                    event["event_id"].as_str().unwrap().rsplit(':').next(),
                    expected["data"].as_str(),
                ],
            )
            .unwrap();
        }
        let second = diff_with(Path::new("."), &config, &backend, 90, 110)
            .await
            .unwrap();
        assert_eq!(second["ok"], true, "{second}");
    }

    #[tokio::test]
    async fn stored_children_match_the_backfill_registry() {
        let abi = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/transfer_abi.json");
        let rule = |event: &str, field: &str, child: &str| {
            format!(
                "[[discovery_rules]]\nparent_contract = \"factory\"\ndiscovery_event = \"{event}\"\naddress_field = \"{field}\"\nchild_contract = \"{child}\"\nchild_abi = \"{}\"\n",
                abi.display()
            )
        };
        let config: ProjectConfig = toml::from_str(&format!(
            "chain = \"ethereum\"\nchain_id = 1\nrpc_url = \"http://127.0.0.1:1\"\ndatabase = \"events.db\"\nstart_block = 0\n[[contracts]]\nalias = \"factory\"\naddress = \"0x1111111111111111111111111111111111111111\"\nabi = \"{}\"\n{}{}{}",
            abi.display(),
            rule("Approval", "spender", "vault"),
            rule("Transfer", "to", "token"),
            // Backfill never applies a second rule for the same parent event.
            rule("Transfer", "from", "shadowed"),
        ))
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE events (event_id TEXT PRIMARY KEY, chain_id INTEGER, contract_alias TEXT, event_name TEXT, block_number INTEGER, block_hash TEXT, log_index INTEGER, data TEXT);
             INSERT INTO events VALUES
               ('1:0xb:0', 1, 'factory', 'Transfer', 500, '0xb', 0, '{\"from\":\"0xAAAA\",\"to\":\"0xBBBB\"}'),
               ('1:0xc:0', 1, 'factory', 'Approval', 900, '0xc', 0, '{\"spender\":\"0xbbbb\"}'),
               ('1:0xd:1', 1, 'factory', 'Transfer', 900, '0xd', 1, '{\"from\":\"0xcccc\",\"to\":\"0xdddd\"}'),
               ('2:0xe:0', 2, 'factory', 'Transfer', 50, '0xe', 0, '{\"from\":\"0xeeee\",\"to\":\"0xffff\"}');",
        )
        .unwrap();
        // Every discovery counts, wherever the replay window is, the first discovery of an
        // address owns it, and other chains are ignored.
        assert_eq!(
            stored_children(&Backend::Sqlite(conn), &config)
                .await
                .unwrap(),
            HashMap::from([
                ("0xbbbb".to_owned(), ((500, 0), "token".to_owned())),
                ("0xdddd".to_owned(), ((900, 1), "token".to_owned())),
            ])
        );
    }

    #[test]
    fn too_wide_errors_split_but_rate_limits_do_not() {
        let error = |text: &str| anyhow::anyhow!(text.to_owned());
        assert!(is_too_wide(&error(
            "eth_getLogs failed: {\"message\":\"exceed maximum block range: 3\"}"
        )));
        assert!(is_too_wide(&error(
            "query returned more than 10000 results"
        )));
        assert!(!is_too_wide(&error("429 Too Many Requests")));
        assert!(!is_too_wide(&error("execution reverted")));
    }

    #[test]
    fn count_rows_filters_by_chain_and_block_window() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE evm_transactions (
                transaction_id TEXT PRIMARY KEY,
                chain_id INTEGER NOT NULL,
                block_number INTEGER NOT NULL
            );
            INSERT INTO evm_transactions VALUES ('a', 1, 10);
            INSERT INTO evm_transactions VALUES ('b', 1, 11);
            INSERT INTO evm_transactions VALUES ('c', 2, 10);
            INSERT INTO evm_transactions VALUES ('d', 1, 12);",
        )
        .unwrap();

        assert!(sqlite_table_exists(&conn, "evm_transactions").unwrap());
        assert_eq!(
            sqlite_count_rows(&conn, "evm_transactions", 1, 10, 11).unwrap(),
            2
        );
    }
}
