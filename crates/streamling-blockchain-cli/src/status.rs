use crate::{config::ProjectConfig, rpc};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{path::Path, time::Duration};

pub const PROGRESS_FILE: &str = "backfill-progress.json";
pub const BLOCKS_PROGRESS_FILE: &str = "blocks-progress.json";
pub const TRANSACTIONS_PROGRESS_FILE: &str = "transactions-progress.json";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackfillProgress {
    pub indexed_through: u64,
    pub observed_head: u64,
    pub safe_head: u64,
    pub confirmations: u64,
    pub caught_up: bool,
    pub updated_at_unix: u64,
}

pub fn cached(root: &Path) -> Result<Value> {
    let config = ProjectConfig::load(root)?;
    let database = config.absolute_database(root);
    let progress = read_progress(root, &config)?;
    Ok(payload(
        &config,
        &database,
        database.exists(),
        progress.as_ref(),
        None,
    ))
}

pub async fn live(root: &Path) -> Result<Value> {
    let config = ProjectConfig::load(root)?;
    let database = config.absolute_database(root);
    let progress = read_progress(root, &config)?;
    let rpc_url = config.rpc_url_value()?;
    let client = rpc::client()?;
    let head = rpc::hex_u64(&rpc::rpc(&client, &rpc_url, "eth_blockNumber", json!([])).await?)?;
    Ok(payload(
        &config,
        &database,
        database.exists(),
        progress.as_ref(),
        Some(head),
    ))
}

pub async fn wait(root: &Path, poll: Duration) -> Result<Value> {
    loop {
        let snapshot = live(root).await?;
        if snapshot["backfill"]["caught_up"].as_bool() == Some(true) {
            return Ok(snapshot);
        }
        tokio::time::sleep(poll).await;
    }
}

/// Progress of the slowest enabled source. Each source writes its file when a checkpoint
/// commits, so this is `None` until every enabled source has committed one.
pub fn read_progress(root: &Path, config: &ProjectConfig) -> Result<Option<BackfillProgress>> {
    let mut files = vec![PROGRESS_FILE];
    if config.index_blocks {
        files.push(BLOCKS_PROGRESS_FILE);
    }
    if config.index_transactions {
        files.push(TRANSACTIONS_PROGRESS_FILE);
    }
    let mut slowest: Option<BackfillProgress> = None;
    for file in files {
        let Some(progress) = read_progress_file(&root.join(file))? else {
            return Ok(None);
        };
        if slowest
            .as_ref()
            .is_none_or(|current| progress.indexed_through < current.indexed_through)
        {
            slowest = Some(progress);
        }
    }
    Ok(slowest)
}

fn read_progress_file(path: &Path) -> Result<Option<BackfillProgress>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse {}", path.display()))
        .map(Some)
}

fn payload(
    config: &ProjectConfig,
    database: &Path,
    database_ready: bool,
    progress: Option<&BackfillProgress>,
    live_head: Option<u64>,
) -> Value {
    let observed_head = live_head.or_else(|| progress.map(|item| item.observed_head));
    let safe_head = observed_head.map(|head| {
        let safe = head.saturating_sub(config.confirmations);
        config.end_block.map_or(safe, |end| safe.min(end))
    });
    let indexed_through = progress.map(|item| item.indexed_through);
    let caught_up = indexed_through
        .zip(safe_head)
        .map(|(indexed, safe)| indexed >= safe);
    let remaining_blocks = indexed_through
        .zip(safe_head)
        .map(|(indexed, safe)| safe.saturating_sub(indexed));
    let progress_percent = indexed_through.zip(safe_head).map(|(indexed, safe)| {
        let span = safe.saturating_sub(config.start_block);
        if span == 0 {
            100.0
        } else {
            let completed = indexed.saturating_sub(config.start_block).min(span);
            completed as f64 * 100.0 / span as f64
        }
    });
    let state = match caught_up {
        Some(true) => "caught_up",
        Some(false) => "backfilling",
        None => "not_started",
    };
    json!({
        "chain": config.chain,
        "chain_id": config.chain_id,
        "contracts": config.contracts,
        "discovery_rules": config.discovery_rules,
        "database": database,
        "database_ready": database_ready,
        "sinks": config.sinks,
        "backfill": {
            "state": state,
            "start_block": config.start_block,
            "indexed_through": indexed_through,
            "chain_head": observed_head,
            "safe_head": safe_head,
            "confirmations": config.confirmations,
            "remaining_blocks": remaining_blocks,
            "progress_percent": progress_percent,
            "caught_up": caught_up,
            "updated_at_unix": progress.map(|item| item.updated_at_unix),
            "live_head": live_head.is_some()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ContractConfig, SinkConfig};
    use std::path::PathBuf;

    fn config() -> ProjectConfig {
        ProjectConfig {
            chain: "ethereum".into(),
            chain_id: 1,
            rpc_url: "http://localhost".into(),
            rpc_url_env: None,
            database: PathBuf::from("events.db"),
            start_block: 100,
            end_block: None,
            confirmations: 10,
            window: 100,
            block_concurrency: 8,
            block_batch_size: 1,
            index_blocks: false,
            index_transactions: false,
            skip_calldata: false,
            contracts: vec![ContractConfig {
                alias: "token".into(),
                address: "0x1111111111111111111111111111111111111111".into(),
                abi: PathBuf::from("abi.json"),
            }],
            discovery_rules: vec![],
            sinks: SinkConfig::default(),
        }
    }

    #[test]
    fn calculates_completion_against_safe_head() {
        let progress = BackfillProgress {
            indexed_through: 990,
            observed_head: 1_000,
            safe_head: 990,
            confirmations: 10,
            caught_up: true,
            updated_at_unix: 1,
        };
        let value = payload(
            &config(),
            Path::new("events.db"),
            true,
            Some(&progress),
            Some(1_005),
        );
        assert_eq!(value["backfill"]["safe_head"], 995);
        assert_eq!(value["backfill"]["remaining_blocks"], 5);
        assert_eq!(value["backfill"]["caught_up"], false);
    }

    #[test]
    fn reports_the_slowest_enabled_source() {
        let root = std::env::temp_dir().join(format!(
            "streamling-blockchain-status-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let progress = |indexed_through: u64| {
            format!(
                r#"{{"indexed_through":{indexed_through},"observed_head":400,"safe_head":250,"confirmations":10,"caught_up":false,"updated_at_unix":1}}"#
            )
        };
        let mut config = config();
        config.index_blocks = true;
        config.index_transactions = true;
        std::fs::write(root.join(PROGRESS_FILE), progress(250)).unwrap();
        std::fs::write(root.join(BLOCKS_PROGRESS_FILE), progress(240)).unwrap();
        // The transaction source has not committed a checkpoint yet.
        assert!(read_progress(&root, &config).unwrap().is_none());

        std::fs::write(root.join(TRANSACTIONS_PROGRESS_FILE), progress(200)).unwrap();
        assert_eq!(
            read_progress(&root, &config)
                .unwrap()
                .unwrap()
                .indexed_through,
            200
        );
        config.index_transactions = false;
        assert_eq!(
            read_progress(&root, &config)
                .unwrap()
                .unwrap()
                .indexed_through,
            240
        );
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn caps_completion_against_end_block() {
        let mut config = config();
        config.end_block = Some(250);
        let progress = BackfillProgress {
            indexed_through: 250,
            observed_head: 1_000,
            safe_head: 250,
            confirmations: 10,
            caught_up: true,
            updated_at_unix: 1,
        };
        let value = payload(
            &config,
            Path::new("events.db"),
            true,
            Some(&progress),
            Some(1_005),
        );
        assert_eq!(value["backfill"]["safe_head"], 250);
        assert_eq!(value["backfill"]["remaining_blocks"], 0);
        assert_eq!(value["backfill"]["caught_up"], true);
    }
}
