use crate::config::{ContractConfig, DiscoveryRule, ProjectConfig};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use std::{collections::HashSet, path::Path};

pub fn write_generated_files(root: &Path, config: &ProjectConfig) -> Result<()> {
    write_pipeline(root, config)?;
    write_semantics(root, config)?;
    write_agent_files(root, config)?;
    Ok(())
}

fn write_pipeline(root: &Path, config: &ProjectConfig) -> Result<()> {
    let db = config
        .absolute_database(root)
        .canonicalize()
        .unwrap_or_else(|_| config.absolute_database(root));
    let contracts = config
        .contracts
        .iter()
        .map(|contract| {
            let abi = root
                .join(&contract.abi)
                .canonicalize()
                .unwrap_or_else(|_| root.join(&contract.abi));
            json!({"alias": contract.alias, "address": contract.address, "abi_path": abi})
        })
        .collect::<Vec<_>>();
    let discovery_rules = config
        .discovery_rules
        .iter()
        .map(|rule| {
            let child_abi = root
                .join(&rule.child_abi)
                .canonicalize()
                .unwrap_or_else(|_| root.join(&rule.child_abi));
            json!({
                "parent_contract": rule.parent_contract,
                "discovery_event": rule.discovery_event,
                "address_field": rule.address_field,
                "child_contract": rule.child_contract,
                "child_abi_path": child_abi
            })
        })
        .collect::<Vec<_>>();
    let plugin_options = serde_json::to_string(
        &json!({"contracts": contracts, "discovery_rules": discovery_rules}),
    )?;
    let mut transforms = serde_yaml::Mapping::new();
    let output_name = if config.discovery_rules.is_empty() {
        "raw_events".to_owned()
    } else {
        for rule in &config.discovery_rules {
            let table = format!("{}_contracts", rule.child_contract);
            transforms.insert(table.clone().into(), json_to_yaml(json!({
                "type": "dynamic_table", "backend_type": "InMemory", "backend_entity_name": table,
                "sql": format!("SELECT discovered_address FROM raw_events WHERE discovered_address IS NOT NULL AND discovery_rule = '{}'", rule.child_contract)
            }))?);
        }
        let direct = config
            .contracts
            .iter()
            .map(|contract| format!("lower(address) = lower('{}')", contract.address))
            .collect::<Vec<_>>();
        let discovered = config
            .discovery_rules
            .iter()
            .map(|rule| {
                format!(
                    "dynamic_table_check('{}_contracts', lower(address))",
                    rule.child_contract
                )
            })
            .collect::<Vec<_>>();
        let predicate = direct
            .into_iter()
            .chain(discovered)
            .collect::<Vec<_>>()
            .join(" OR ");
        transforms.insert(
            "indexed_events".into(),
            json_to_yaml(json!({
                "type": "sql", "primary_key": "event_id",
                "sql": format!("SELECT * FROM raw_events WHERE {predicate}")
            }))?,
        );
        "indexed_events".to_owned()
    };
    let mut sinks = serde_yaml::Mapping::new();
    if config.sinks.sqlite {
        sinks.insert(
            "local_sql".into(),
            json_to_yaml(json!({
                "type": "streamling_blockchain.sqlite_sink",
                "from": output_name,
                "options": {"db_path": db.to_string_lossy()}
            }))?,
        );
        if config.index_blocks {
            sinks.insert(
                "local_blocks".into(),
                json_to_yaml(json!({
                    "type": "streamling_blockchain.sqlite_sink",
                    "from": "evm_blocks",
                    "options": {
                        "db_path": db.to_string_lossy(),
                        "table": "evm_blocks",
                        "key_column": "block_id"
                    }
                }))?,
            );
        }
        if config.index_transactions {
            sinks.insert(
                "local_transactions".into(),
                json_to_yaml(json!({
                    "type": "streamling_blockchain.sqlite_sink",
                    "from": "evm_transactions",
                    "options": {
                        "db_path": db.to_string_lossy(),
                        "table": "evm_transactions",
                        "key_column": "transaction_id"
                    }
                }))?,
            );
        }
    }
    if let Some(clickhouse) = &config.sinks.clickhouse {
        let mut sink = json!({
            "type": "clickhouse",
            "from": output_name,
            "table": clickhouse.table,
            "primary_key": "event_id"
        });
        if let Some(compression) = &clickhouse.compression {
            sink["compression"] = compression.clone().into();
        }
        sinks.insert("clickhouse_events".into(), json_to_yaml(sink)?);
        if config.index_blocks {
            let table = format!("{}_blocks", clickhouse.table);
            sinks.insert(
                "clickhouse_blocks".into(),
                json_to_yaml(json!({
                    "type": "clickhouse",
                    "from": "evm_blocks",
                    "table": table,
                    "primary_key": "block_id"
                }))?,
            );
        }
        if config.index_transactions {
            let table = format!("{}_transactions", clickhouse.table);
            sinks.insert(
                "clickhouse_transactions".into(),
                json_to_yaml(json!({
                    "type": "clickhouse",
                    "from": "evm_transactions",
                    "table": table,
                    "primary_key": "transaction_id"
                }))?,
            );
        }
        write_clickhouse_schemas(
            root,
            clickhouse.database.as_deref(),
            &clickhouse.table,
            config.index_blocks,
            config.index_transactions,
        )?;
    }
    let mut source_options = json!({
        "start_block": config.start_block.to_string(),
        "confirmations": config.confirmations.to_string(),
        "window": config.window.to_string(),
        "chain_id": config.chain_id.to_string(),
        "progress_path": root.join("backfill-progress.json").to_string_lossy(),
        "spec": plugin_options
    });
    if let Some(end_block) = config.end_block {
        source_options["end_block"] = end_block.to_string().into();
    }
    if let Some(name) = &config.rpc_url_env {
        source_options["rpc_url_env"] = name.clone().into();
    } else {
        source_options["rpc_url"] = config.rpc_url.clone().into();
    }
    let mut sources = serde_yaml::Mapping::new();
    sources.insert(
        "raw_events".into(),
        json_to_yaml(json!({
            "type": "streamling_blockchain.evm_events",
            "primary_key": "event_id",
            "options": source_options
        }))?,
    );
    if config.index_blocks {
        let mut block_options = json!({
            "start_block": config.start_block.to_string(),
            "confirmations": config.confirmations.to_string(),
            "window": config.window.to_string(),
            "chain_id": config.chain_id.to_string(),
            "progress_path": root.join("blocks-progress.json").to_string_lossy(),
        });
        if let Some(end_block) = config.end_block {
            block_options["end_block"] = end_block.to_string().into();
        }
        if let Some(name) = &config.rpc_url_env {
            block_options["rpc_url_env"] = name.clone().into();
        } else {
            block_options["rpc_url"] = config.rpc_url.clone().into();
        }
        sources.insert(
            "evm_blocks".into(),
            json_to_yaml(json!({
                "type": "streamling_blockchain.evm_blocks",
                "primary_key": "block_id",
                "options": block_options
            }))?,
        );
    }
    if config.index_transactions {
        let mut transaction_options = json!({
            "start_block": config.start_block.to_string(),
            "confirmations": config.confirmations.to_string(),
            "window": config.window.to_string(),
            "chain_id": config.chain_id.to_string(),
            "progress_path": root.join("transactions-progress.json").to_string_lossy(),
        });
        if let Some(end_block) = config.end_block {
            transaction_options["end_block"] = end_block.to_string().into();
        }
        if let Some(name) = &config.rpc_url_env {
            transaction_options["rpc_url_env"] = name.clone().into();
        } else {
            transaction_options["rpc_url"] = config.rpc_url.clone().into();
        }
        sources.insert(
            "evm_transactions".into(),
            json_to_yaml(json!({
                "type": "streamling_blockchain.evm_transactions",
                "primary_key": "transaction_id",
                "options": transaction_options
            }))?,
        );
    }
    let pipeline = json!({
        "sources": sources,
        "transforms": transforms,
        "sinks": sinks
    });
    let pipeline_path = root.join("pipeline.yaml");
    std::fs::write(&pipeline_path, serde_yaml::to_string(&pipeline)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&pipeline_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn json_to_yaml(value: Value) -> Result<serde_yaml::Value> {
    Ok(serde_yaml::to_value(value)?)
}

fn write_clickhouse_schemas(
    root: &Path,
    database: Option<&str>,
    events_table: &str,
    include_blocks: bool,
    include_transactions: bool,
) -> Result<()> {
    let schema_dir = root.join("clickhouse");
    std::fs::create_dir_all(&schema_dir)?;
    if let Some(db) = database {
        std::fs::write(
            schema_dir.join("database.sql"),
            format!("CREATE DATABASE IF NOT EXISTS {db};\n"),
        )?;
    }
    write_clickhouse_schema_file(
        root,
        events_table,
        clickhouse_qualified(database, events_table),
        "  event_id String,\n  chain_id UInt64,\n  contract_alias String,\n  event_name String,\n  address String,\n  block_number UInt64,\n  block_hash String,\n  block_timestamp UInt64,\n  tx_hash String,\n  log_index UInt64,\n  topic0 String,\n  fields_json String,\n  discovered_address Nullable(String),\n  discovery_rule Nullable(String),\n  insert_time DateTime64(3),\n  is_deleted UInt8",
        "event_id",
    )?;
    if include_blocks {
        let table = format!("{events_table}_blocks");
        write_clickhouse_schema_file(
            root,
            &table,
            clickhouse_qualified(database, &table),
            "  block_id String,\n  chain_id UInt64,\n  block_number UInt64,\n  block_hash String,\n  parent_hash String,\n  block_timestamp UInt64,\n  miner Nullable(String),\n  gas_limit UInt64,\n  gas_used UInt64,\n  base_fee_per_gas Nullable(String),\n  transaction_count UInt64,\n  insert_time DateTime64(3),\n  is_deleted UInt8",
            "block_id",
        )?;
    }
    if include_transactions {
        let table = format!("{events_table}_transactions");
        write_clickhouse_schema_file(
            root,
            &table,
            clickhouse_qualified(database, &table),
            "  transaction_id String,\n  chain_id UInt64,\n  tx_hash String,\n  block_number UInt64,\n  block_hash String,\n  block_timestamp UInt64,\n  transaction_index UInt64,\n  from_address String,\n  to_address Nullable(String),\n  value String,\n  gas UInt64,\n  gas_price Nullable(String),\n  max_fee_per_gas Nullable(String),\n  max_priority_fee_per_gas Nullable(String),\n  input String,\n  method_id Nullable(String),\n  nonce UInt64,\n  receipt_status Nullable(UInt64),\n  receipt_gas_used Nullable(UInt64),\n  receipt_effective_gas_price Nullable(String),\n  contract_address Nullable(String),\n  logs_count Nullable(UInt64),\n  insert_time DateTime64(3),\n  is_deleted UInt8",
            "transaction_id",
        )?;
    }
    Ok(())
}

fn clickhouse_qualified(database: Option<&str>, table: &str) -> String {
    database.map_or_else(|| table.to_owned(), |db| format!("{db}.{table}"))
}

fn write_clickhouse_schema_file(
    root: &Path,
    file_stem: &str,
    qualified_table: String,
    columns: &str,
    order_by: &str,
) -> Result<()> {
    std::fs::write(
        root.join("clickhouse").join(format!("{file_stem}.sql")),
        format!(
            "CREATE TABLE IF NOT EXISTS {qualified_table} (\n{columns}\n)\nENGINE = ReplacingMergeTree(insert_time, is_deleted)\nORDER BY {order_by};\n"
        ),
    )?;
    Ok(())
}
pub fn write_semantics(root: &Path, config: &ProjectConfig) -> Result<()> {
    let mut out =
        String::from("# Generated from the live ABI registry. Edit descriptions freely.\n\n");
    for contract in &config.contracts {
        let abi_path = root.join(&contract.abi);
        let abi: Value = serde_json::from_slice(&std::fs::read(&abi_path)?)?;
        append_abi_semantics(&mut out, &contract.alias, &abi)?;
    }
    for rule in &config.discovery_rules {
        let abi: Value = serde_json::from_slice(&std::fs::read(root.join(&rule.child_abi))?)?;
        append_abi_semantics(&mut out, &rule.child_contract, &abi)?;
    }
    std::fs::write(root.join("semantic.toml"), out)?;
    Ok(())
}

fn append_abi_semantics(out: &mut String, alias: &str, abi: &Value) -> Result<()> {
    let items = abi.as_array().context("ABI must be a JSON array")?;
    for item in items
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("event"))
    {
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .context("event missing name")?;
        if item.get("anonymous").and_then(Value::as_bool) == Some(true) {
            bail!("anonymous event {name} is unsupported; Streamling topic filters require topic0");
        }
        let table = format!(
            "{}__{}",
            alias.to_ascii_lowercase(),
            name.to_ascii_lowercase()
        );
        let inputs = item
            .get("inputs")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let types = inputs
            .iter()
            .map(|input| {
                input
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let signature = format!("{name}({})", types.join(","));
        let topic0 = format!("0x{}", hex::encode(Keccak256::digest(signature.as_bytes())));
        out.push_str(&format!(
            "[tables.{table:?}]\ndescription = {:?}\ncontract = {:?}\nevent = {:?}\nsignature = {:?}\ntopic0 = {:?}\n",
            format!("{name} events emitted by {alias}"),
            alias,
            name,
            signature,
            topic0
        ));
        let mut columns = HashSet::new();
        for (index, input) in inputs.iter().enumerate() {
            let raw_field = input
                .get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("unnamed_{}", index + 1));
            let field = event_column(&raw_field);
            if !columns.insert(field.clone()) {
                bail!("event {name} maps multiple ABI parameters to SQLite column {field}");
            }
            let kind = input
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let indexed = input
                .get("indexed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            out.push_str(&format!(
                "[tables.{table:?}.columns.{field:?}]\ndescription = {:?}\nsolidity_type = {:?}\nindexed = {indexed}\nsource_name = {:?}\n",
                format!("Solidity {kind} event parameter"),
                kind,
                raw_field
            ));
        }
        out.push('\n');
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

fn write_agent_files(root: &Path, config: &ProjectConfig) -> Result<()> {
    let aliases = config
        .contracts
        .iter()
        .map(|c| c.alias.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    std::fs::write(
        root.join("llms.txt"),
        format!(
            "# Streamling Blockchain Studio project\n\nContracts: {aliases}\n\nKeep this project on persistent storage. Streamling checkpoints and the SQLite database live inside the project directory; restarting `streamling-blockchain dev` with the same project resumes from the last committed checkpoint. Use `streamling-blockchain status --json` to inspect backfill progress and `streamling-blockchain status --wait` to block until the safe chain head is indexed. Use `streamling-blockchain schema` before writing SQL. Query with `streamling-blockchain sql --json <QUERY>`. Event tables are named `<alias>__<event>`. Quote Solidity names such as `from`. Discovered child contracts are filtered by Streamling dynamic tables before persistence.\n"
        ),
    )?;
    std::fs::create_dir_all(root.join("skills/streamling-blockchain-query"))?;
    std::fs::write(
        root.join("skills/streamling-blockchain-query/SKILL.md"),
        "---\nname: streamling-blockchain-query\ndescription: Query a local Streamling Blockchain Studio event database.\n---\n\nKeep the project on persistent storage and reuse it after restarts so Streamling resumes its checkpoint. Run `streamling-blockchain status --json` to inspect backfill progress or `streamling-blockchain status --wait` when work must begin only after catch-up. Run `streamling-blockchain schema` before querying, then use `streamling-blockchain sql --json '<SQL>'`. Prefer explicit columns, quote Solidity identifiers, and include LIMIT for exploratory queries.\n",
    )?;
    Ok(())
}

pub fn add_contract(
    root: &Path,
    mut config: ProjectConfig,
    contract: ContractConfig,
) -> Result<ProjectConfig> {
    config.contracts.push(contract);
    config.save(root)?;
    write_generated_files(root, &config)?;
    Ok(config)
}

pub fn add_discovery_rule(
    root: &Path,
    mut config: ProjectConfig,
    rule: DiscoveryRule,
) -> Result<ProjectConfig> {
    config.discovery_rules.push(rule);
    config.save(root)?;
    write_generated_files(root, &config)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClickHouseSinkConfig, ContractConfig, SinkConfig};
    use std::path::PathBuf;

    #[test]
    fn generated_pipeline_can_write_sqlite_and_clickhouse_sinks() {
        let root = std::env::temp_dir().join(format!(
            "streamling-blockchain-project-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("abis")).unwrap();
        std::fs::write(
            root.join("abis/token.json"),
            r#"[{"type":"event","name":"Transfer","inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        let config = ProjectConfig {
            chain: "base".into(),
            chain_id: 8453,
            rpc_url: "http://localhost:8545".into(),
            rpc_url_env: None,
            database: PathBuf::from(".streamling-blockchain/events.db"),
            start_block: 1,
            confirmations: 12,
            end_block: Some(25),
            window: 2_000,
            index_blocks: true,
            index_transactions: true,
            contracts: vec![ContractConfig {
                alias: "token".into(),
                address: "0x1111111111111111111111111111111111111111".into(),
                abi: PathBuf::from("abis/token.json"),
            }],
            discovery_rules: vec![],
            sinks: SinkConfig {
                sqlite: true,
                clickhouse: Some(ClickHouseSinkConfig {
                    table: "events".into(),
                    database: Some("fwa_analytics".into()),
                    compression: Some("gzip".into()),
                }),
            },
        };

        write_generated_files(&root, &config).unwrap();

        let pipeline: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(root.join("pipeline.yaml")).unwrap())
                .unwrap();
        assert_eq!(
            pipeline["sinks"]["local_sql"]["type"],
            "streamling_blockchain.sqlite_sink"
        );
        assert_eq!(pipeline["sinks"]["clickhouse_events"]["type"], "clickhouse");
        assert_eq!(
            pipeline["sinks"]["clickhouse_events"]["primary_key"],
            "event_id"
        );
        assert_eq!(pipeline["sinks"]["clickhouse_blocks"]["type"], "clickhouse");
        assert_eq!(
            pipeline["sinks"]["clickhouse_blocks"]["primary_key"],
            "block_id"
        );
        assert_eq!(
            pipeline["sinks"]["clickhouse_blocks"]["table"],
            "events_blocks"
        );
        assert_eq!(
            pipeline["sinks"]["clickhouse_transactions"]["type"],
            "clickhouse"
        );
        assert_eq!(
            pipeline["sinks"]["clickhouse_transactions"]["primary_key"],
            "transaction_id"
        );
        assert_eq!(
            pipeline["sinks"]["clickhouse_transactions"]["table"],
            "events_transactions"
        );
        assert_eq!(
            pipeline["sources"]["evm_blocks"]["type"],
            "streamling_blockchain.evm_blocks"
        );
        assert_eq!(
            pipeline["sources"]["raw_events"]["options"]["chain_id"],
            "8453"
        );
        assert_eq!(pipeline["sources"]["evm_blocks"]["primary_key"], "block_id");
        assert_eq!(
            pipeline["sources"]["evm_blocks"]["options"]["chain_id"],
            "8453"
        );
        assert_eq!(
            pipeline["sinks"]["local_blocks"]["options"]["key_column"],
            "block_id"
        );
        assert_eq!(
            pipeline["sources"]["evm_transactions"]["type"],
            "streamling_blockchain.evm_transactions"
        );
        assert_eq!(
            pipeline["sources"]["evm_transactions"]["primary_key"],
            "transaction_id"
        );
        assert_eq!(
            pipeline["sources"]["evm_transactions"]["options"]["chain_id"],
            "8453"
        );
        assert_eq!(
            pipeline["sinks"]["local_transactions"]["options"]["key_column"],
            "transaction_id"
        );
        assert_eq!(
            pipeline["sources"]["raw_events"]["options"]["end_block"],
            "25"
        );
        assert_eq!(
            pipeline["sources"]["evm_blocks"]["options"]["end_block"],
            "25"
        );
        assert_eq!(
            pipeline["sources"]["evm_transactions"]["options"]["end_block"],
            "25"
        );
        let clickhouse_schema =
            std::fs::read_to_string(root.join("clickhouse/events.sql")).unwrap();
        assert!(clickhouse_schema.contains("chain_id UInt64"));
        assert!(clickhouse_schema.contains("ReplacingMergeTree"));
        let clickhouse_block_schema =
            std::fs::read_to_string(root.join("clickhouse/events_blocks.sql")).unwrap();
        assert!(clickhouse_block_schema.contains("block_id String"));
        assert!(clickhouse_block_schema.contains("transaction_count UInt64"));
        let clickhouse_transaction_schema =
            std::fs::read_to_string(root.join("clickhouse/events_transactions.sql")).unwrap();
        assert!(clickhouse_transaction_schema.contains("transaction_id String"));
        assert!(clickhouse_transaction_schema.contains("receipt_gas_used Nullable(UInt64)"));
        let semantics = std::fs::read_to_string(root.join("semantic.toml")).unwrap();
        assert!(semantics.contains("signature = \"Transfer(address,address,uint256)\""));
        assert!(semantics.contains("topic0 = \"0xddf252ad"));
        assert!(semantics.contains("solidity_type = \"address\""));
        assert!(semantics.contains("indexed = true"));
        let _ = std::fs::remove_dir_all(&root);
    }
}
