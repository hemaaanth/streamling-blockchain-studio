use crate::{config::ProjectConfig, database, rpc, status};
use anyhow::{Context, Result, bail};
use ethers_core::{
    abi::{Abi, Event, RawLog, Token},
    types::H256,
};
use rusqlite::{Connection, params};
use serde_json::{Map, Value, json};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    str::FromStr,
};

struct EventDescriptor {
    owner: String,
    event: Event,
}

struct DecodedEvent {
    event_id: String,
    contract_alias: String,
    event_name: String,
    block_number: u64,
    block_hash: String,
    data: String,
}

pub async fn diff(root: &Path, from: u64, to: u64) -> Result<Value> {
    if from > to {
        bail!("--from must be less than or equal to --to")
    }
    let config = ProjectConfig::load(root)?;
    let rpc_url = config.rpc_url_value()?;
    let mut events = HashMap::new();
    let mut direct = HashMap::new();
    for contract in &config.contracts {
        direct.insert(
            contract.address.to_ascii_lowercase(),
            contract.alias.clone(),
        );
        load_events(&contract.alias, &root.join(&contract.abi), &mut events)?;
    }
    for rule in &config.discovery_rules {
        load_events(
            &rule.child_contract,
            &root.join(&rule.child_abi),
            &mut events,
        )?;
    }
    let mut topics = events
        .keys()
        .map(|topic| format!("{topic:#x}"))
        .collect::<Vec<_>>();
    topics.sort();
    let mut filter = json!({
        "fromBlock": format!("0x{from:x}"),
        "toBlock": format!("0x{to:x}"),
        "topics": [topics]
    });
    if config.discovery_rules.is_empty() {
        filter["address"] = Value::Array(direct.keys().cloned().map(Value::String).collect());
    }
    let logs = rpc_call(&rpc_url, "eth_getLogs", json!([filter])).await?;
    let decoded = logs
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|log| decode_log(config.chain_id, log, &events, &direct).transpose())
        .collect::<Result<Vec<_>>>()?;
    let conn = database::open(&config.absolute_database(root))?;
    compare(&conn, from, to, decoded)
}

pub async fn audit_once(root: &Path, window_blocks: u64) -> Result<Value> {
    let snapshot = status::live(root).await?;
    let indexed = snapshot["backfill"]["indexed_through"]
        .as_u64()
        .context("backfill has not started")?;
    let start = snapshot["backfill"]["start_block"].as_u64().unwrap_or(0);
    let window = window_blocks.max(1);
    let recent_to = indexed;
    let recent_from = recent_to.saturating_sub(window - 1).max(start);
    let mut checks = vec![audit_range(root, "recent", recent_from, recent_to).await?];
    if recent_to > start + window {
        let span = recent_to - start - window;
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let random_from = start + seed.wrapping_mul(1_103_515_245).wrapping_add(12_345) % span;
        checks.push(audit_range(root, "sample", random_from, random_from + window - 1).await?);
    }
    Ok(
        json!({"ok": checks.iter().all(|check| check["ok"].as_bool() == Some(true)), "checks": checks}),
    )
}

pub async fn audit_continuous(
    root: &Path,
    interval: std::time::Duration,
    window_blocks: u64,
) -> Result<()> {
    loop {
        let value = audit_once(root, window_blocks).await?;
        println!("{}", serde_json::to_string(&value)?);
        tokio::time::sleep(interval).await;
    }
}

async fn audit_range(root: &Path, label: &str, from: u64, to: u64) -> Result<Value> {
    let mut result = diff(root, from, to).await?;
    if let Value::Object(object) = &mut result {
        object.insert(
            "indexed_tables".into(),
            compare_optional_indexed_tables(root, from, to).await?,
        );
    }
    record_audit(root, label, &result)?;
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

fn record_audit(root: &Path, label: &str, result: &Value) -> Result<()> {
    let config = ProjectConfig::load(root)?;
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
            result["from_block"].as_u64().unwrap_or_default(),
            result["to_block"].as_u64().unwrap_or_default(),
            result["ok"].as_bool() == Some(true),
            result["chain_events"].as_u64().unwrap_or_default(),
            result["stored_events"].as_u64().unwrap_or_default(),
            result["missing"].as_array().map_or(0, Vec::len) as u64,
            result["extra"].as_array().map_or(0, Vec::len) as u64,
            result["changed"].as_array().map_or(0, Vec::len) as u64,
            serde_json::to_string(result)?,
        ],
    )?;
    Ok(())
}

async fn rpc_call(url: &str, method: &str, params: Value) -> Result<Value> {
    let response = rpc::client()?
        .post(url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .await
        .with_context(|| format!("call {method}"))?;
    let payload: Value = response
        .json()
        .await
        .with_context(|| format!("decode {method}"))?;
    if let Some(error) = payload.get("error") {
        bail!("{method}: {error}")
    }
    Ok(payload.get("result").cloned().unwrap_or(Value::Null))
}

fn compare(conn: &Connection, from: u64, to: u64, decoded: Vec<DecodedEvent>) -> Result<Value> {
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
    Ok(json!({
        "from_block": from,
        "to_block": to,
        "chain_events": chain_ids.len(),
        "stored_events": stored.len(),
        "missing": missing,
        "extra": extra,
        "changed": changed,
        "ok": missing.is_empty() && extra.is_empty() && changed.is_empty(),
    }))
}

async fn compare_optional_indexed_tables(root: &Path, from: u64, to: u64) -> Result<Value> {
    let config = ProjectConfig::load(root)?;
    let conn = database::open(&config.absolute_database(root))?;
    let has_blocks = table_exists(&conn, "evm_blocks")?;
    let has_transactions = table_exists(&conn, "evm_transactions")?;
    if !has_blocks && !has_transactions {
        return Ok(Value::Null);
    }
    let rpc_url = config.rpc_url_value()?;
    let mut chain_blocks = 0_u64;
    let mut chain_transactions = 0_u64;
    for block in from..=to {
        let value = rpc_call(
            &rpc_url,
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
        Some(count_rows(&conn, "evm_blocks", config.chain_id, from, to)?)
    } else {
        None
    };
    let stored_transactions = if has_transactions {
        Some(count_rows(
            &conn,
            "evm_transactions",
            config.chain_id,
            from,
            to,
        )?)
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

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [table],
        |row| row.get::<_, bool>(0),
    )?)
}

fn count_rows(conn: &Connection, table: &str, chain_id: u64, from: u64, to: u64) -> Result<u64> {
    let sql = format!(
        "SELECT count(*) FROM {table} WHERE chain_id = ?1 AND block_number BETWEEN ?2 AND ?3"
    );
    Ok(conn.query_row(&sql, params![chain_id, from, to], |row| row.get(0))?)
}

fn decode_log(
    chain_id: u64,
    log: &Value,
    events: &HashMap<H256, Vec<EventDescriptor>>,
    direct: &HashMap<String, String>,
) -> Result<Option<DecodedEvent>> {
    let topics = log
        .get("topics")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let Some(topic0_text) = topics.first().and_then(Value::as_str) else {
        return Ok(None);
    };
    let topic0 = H256::from_str(topic0_text)?;
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
        .unwrap_or(&candidates[0]);
    let raw_topics = topics
        .iter()
        .filter_map(Value::as_str)
        .map(H256::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    let data = hex::decode(
        log.get("data")
            .and_then(Value::as_str)
            .unwrap_or("0x")
            .trim_start_matches("0x"),
    )?;
    let parsed = descriptor.event.parse_log(RawLog {
        topics: raw_topics,
        data,
    })?;
    let mut object = Map::new();
    for (index, param) in parsed.params.into_iter().enumerate() {
        let name = if param.name.is_empty() {
            format!("unnamed_{}", index + 1)
        } else {
            param.name
        };
        object.insert(name, token_json(&param.value));
    }
    let block_hash = log
        .get("blockHash")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let log_index = parse_hex(log.get("logIndex").and_then(Value::as_str).unwrap_or("0x0"))?;
    Ok(Some(DecodedEvent {
        event_id: format!("{chain_id}:{block_hash}:{log_index}"),
        contract_alias: descriptor.owner.clone(),
        event_name: descriptor.event.name.clone(),
        block_number: parse_hex(
            log.get("blockNumber")
                .and_then(Value::as_str)
                .unwrap_or("0x0"),
        )?,
        block_hash,
        data: Value::Object(object).to_string(),
    }))
}

fn load_events(
    owner: &str,
    path: &PathBuf,
    target: &mut HashMap<H256, Vec<EventDescriptor>>,
) -> Result<()> {
    let abi: Abi = serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("read {}", path.display()))?,
    )?;
    for events in abi.events.values() {
        for event in events {
            if event.anonymous {
                bail!(
                    "anonymous event {} in {} is unsupported",
                    event.name,
                    path.display()
                )
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

fn parse_hex(text: &str) -> Result<u64> {
    Ok(u64::from_str_radix(text.trim_start_matches("0x"), 16)?)
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
            &conn,
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
        )
        .unwrap();
        assert_eq!(result["ok"], false);
        assert_eq!(result["changed"].as_array().unwrap().len(), 1);
        assert_eq!(result["extra"].as_array().unwrap(), &[json!("extra")]);
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

        assert!(table_exists(&conn, "evm_transactions").unwrap());
        assert_eq!(count_rows(&conn, "evm_transactions", 1, 10, 11).unwrap(), 2);
    }
}
