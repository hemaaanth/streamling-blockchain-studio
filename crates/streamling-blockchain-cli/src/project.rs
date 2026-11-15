use crate::config::{ContractConfig, DiscoveryRule, ProjectConfig};
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use std::path::Path;

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
            json!({
                "alias": contract.alias,
                "address": contract.address,
                "abi_path": abi,
                "start_block": contract.start_block
            })
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
        "log_concurrency": config.log_concurrency.to_string(),
        "chain_id": config.chain_id.to_string(),
        "progress_path": root.join(crate::status::PROGRESS_FILE).to_string_lossy(),
        "spec": plugin_options
    });
    if let Some(end_block) = config.end_block {
        source_options["end_block"] = end_block.to_string().into();
    }
    if let Some(max_window) = config.max_window {
        source_options["max_window"] = max_window.to_string().into();
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
            "concurrency": config.block_concurrency.to_string(),
            "rpc_batch_size": config.block_batch_size.to_string(),
            "chain_id": config.chain_id.to_string(),
            "progress_path": root.join(crate::status::BLOCKS_PROGRESS_FILE).to_string_lossy(),
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
            "concurrency": config.block_concurrency.to_string(),
            "rpc_batch_size": config.block_batch_size.to_string(),
            "chain_id": config.chain_id.to_string(),
            "progress_path": root.join(crate::status::TRANSACTIONS_PROGRESS_FILE).to_string_lossy(),
        });
        if let Some(end_block) = config.end_block {
            transaction_options["end_block"] = end_block.to_string().into();
        }
        if config.skip_calldata {
            transaction_options["store_input"] = "false".into();
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
    for statement in
        clickhouse_statements(database, events_table, include_blocks, include_transactions)
    {
        std::fs::write(
            schema_dir.join(format!("{}.sql", statement.name)),
            format!("{};\n", statement.sql),
        )?;
    }
    Ok(())
}

pub struct ClickHouseStatement {
    /// `database` for the database statement, otherwise the unqualified table name.
    pub name: String,
    pub sql: String,
    /// Expected `system.tables.sorting_key` for table statements.
    pub sorting_key: Option<&'static str>,
}

/// DDL for the tables Streamling's ClickHouse sink writes. The tables must exist before
/// Streamling starts: otherwise the sink creates them itself, ordered by the primary key
/// alone. ReplacingMergeTree(insert_time, is_deleted) is what the sink expects; it makes
/// at-least-once redelivery idempotent. The sorting key is also the deduplication key, so
/// each one ends with the row's unique ID; the leading columns never change for an ID.
pub fn clickhouse_statements(
    database: Option<&str>,
    events_table: &str,
    include_blocks: bool,
    include_transactions: bool,
) -> Vec<ClickHouseStatement> {
    let mut statements = Vec::new();
    if let Some(db) = database {
        statements.push(ClickHouseStatement {
            name: "database".into(),
            sql: format!("CREATE DATABASE IF NOT EXISTS {db}"),
            sorting_key: None,
        });
    }
    let mut table = |name: String, columns: &str, sorting_key: &'static str| {
        statements.push(ClickHouseStatement {
            sql: format!(
                "CREATE TABLE IF NOT EXISTS {} (\n{columns},\n  insert_time DateTime64(3) DEFAULT now64(3),\n  is_deleted UInt8\n)\nENGINE = ReplacingMergeTree(insert_time, is_deleted)\nPARTITION BY toYYYYMM(toDateTime(block_timestamp))\nORDER BY ({sorting_key})",
                database.map_or_else(|| name.clone(), |db| format!("{db}.{name}"))
            ),
            name,
            sorting_key: Some(sorting_key),
        });
    };
    table(
        events_table.to_owned(),
        "  event_id String,\n  chain_id UInt64,\n  contract_alias LowCardinality(String),\n  event_name LowCardinality(String),\n  address String,\n  block_number UInt64,\n  block_hash String,\n  block_timestamp UInt64,\n  tx_hash String,\n  log_index UInt64,\n  topic0 LowCardinality(String),\n  fields_json String,\n  discovered_address Nullable(String),\n  discovery_rule Nullable(String)",
        "chain_id, contract_alias, event_name, block_number, event_id",
    );
    if include_blocks {
        table(
            format!("{events_table}_blocks"),
            "  block_id String,\n  chain_id UInt64,\n  block_number UInt64,\n  block_hash String,\n  parent_hash String,\n  block_timestamp UInt64,\n  miner Nullable(String),\n  gas_limit UInt64,\n  gas_used UInt64,\n  base_fee_per_gas Nullable(String),\n  transaction_count UInt64",
            "chain_id, block_number, block_id",
        );
    }
    if include_transactions {
        table(
            format!("{events_table}_transactions"),
            "  transaction_id String,\n  chain_id UInt64,\n  tx_hash String,\n  block_number UInt64,\n  block_hash String,\n  block_timestamp UInt64,\n  transaction_index UInt64,\n  from_address String,\n  to_address Nullable(String),\n  value String,\n  gas UInt64,\n  gas_price Nullable(String),\n  max_fee_per_gas Nullable(String),\n  max_priority_fee_per_gas Nullable(String),\n  input String,\n  method_id Nullable(String),\n  nonce UInt64,\n  receipt_status Nullable(UInt64),\n  receipt_gas_used Nullable(UInt64),\n  receipt_effective_gas_price Nullable(String),\n  contract_address Nullable(String),\n  logs_count Nullable(UInt64)",
            "chain_id, block_number, transaction_index, transaction_id",
        );
    }
    statements
}
pub fn write_semantics(root: &Path, config: &ProjectConfig) -> Result<()> {
    // A file that no longer parses has no descriptions worth keeping; regenerate it.
    let existing = read_semantics(root).ok().flatten().unwrap_or_default();
    std::fs::write(
        root.join("semantic.toml"),
        render_semantics(root, config, &existing)?,
    )?;
    Ok(())
}

/// Lists what `semantics generate` would change in semantic.toml; empty means it is current.
pub fn check_semantics(root: &Path, config: &ProjectConfig) -> Result<Vec<String>> {
    let existing = match read_semantics(root) {
        Ok(Some(existing)) => existing,
        Ok(None) => return Ok(vec!["semantic.toml is missing".into()]),
        Err(error) => return Ok(vec![format!("{error:#}")]),
    };
    let expected: toml::Table = toml::from_str(&render_semantics(root, config, &existing)?)?;
    let tables = |table: &toml::Table| table.get("tables").and_then(toml::Value::as_table).cloned();
    let (expected, existing) = (
        tables(&expected).unwrap_or_default(),
        tables(&existing).unwrap_or_default(),
    );
    let mut problems = Vec::new();
    for (name, value) in &expected {
        match existing.get(name) {
            None => problems.push(format!("missing table {name}")),
            Some(current) if current != value => {
                problems.push(format!("table {name} does not match its ABI"))
            }
            Some(_) => {}
        }
    }
    for name in existing.keys().filter(|name| !expected.contains_key(*name)) {
        problems.push(format!("table {name} has no configured ABI event"));
    }
    Ok(problems)
}

fn read_semantics(root: &Path) -> Result<Option<toml::Table>> {
    let path = root.join("semantic.toml");
    match std::fs::read_to_string(&path) {
        Ok(text) => Ok(Some(
            toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn render_semantics(root: &Path, config: &ProjectConfig, existing: &toml::Table) -> Result<String> {
    let mut out = String::from(
        "# Generated from the live ABI registry. Edit descriptions freely; regeneration keeps them.\n",
    );
    if let Some(clickhouse) = &config.sinks.clickhouse {
        out.push_str(&format!(
            "# Tables below are SQLite sink tables. In ClickHouse, filter `{}` by contract_alias and event_name and read fields with JSONExtractString(fields_json, '<source_name>').\n",
            clickhouse.table
        ));
    }
    out.push('\n');
    let mut tables = SemanticTables::default();
    for contract in &config.contracts {
        tables.add_abi(&contract.alias, &read_abi(&root.join(&contract.abi))?)?;
    }
    for rule in &config.discovery_rules {
        tables.add_abi(
            &rule.child_contract,
            &read_abi(&root.join(&rule.child_abi))?,
        )?;
    }
    tables.render(&mut out, existing);
    Ok(out)
}

fn read_abi(path: &Path) -> Result<Value> {
    serde_json::from_slice(
        &std::fs::read(path).with_context(|| format!("read {}", path.display()))?,
    )
    .with_context(|| format!("parse ABI {}", path.display()))
}

/// The user's description for a table, or for one of its columns, from the current semantic.toml.
fn kept_description<'a>(
    existing: &'a toml::Table,
    table: &str,
    column: Option<&str>,
) -> Option<&'a str> {
    let mut entry = existing.get("tables")?.get(table)?;
    if let Some(column) = column {
        entry = entry.get("columns")?.get(column)?;
    }
    entry.get("description")?.as_str()
}

/// Semantic tables keyed like the SQLite sink's `<alias>__<event>` tables. Overloaded events and
/// child aliases shared by several factories land in one table there, so they share one entry here.
#[derive(Default)]
struct SemanticTables {
    tables: Vec<SemanticTable>,
}

struct SemanticTable {
    name: String,
    alias: String,
    event: String,
    /// Each ABI definition of the event as (signature, inputs); the first is the main one.
    variants: Vec<(String, Vec<SemanticColumn>)>,
}

#[derive(Clone, PartialEq)]
struct SemanticColumn {
    field: String,
    kind: String,
    indexed: bool,
    source_name: String,
}

impl SemanticTables {
    fn add_abi(&mut self, alias: &str, abi: &Value) -> Result<()> {
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
                bail!(
                    "anonymous event {name} is unsupported; Streamling topic filters require topic0"
                );
            }
            let inputs = item
                .get("inputs")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut columns = Vec::new();
            for (index, input) in inputs.iter().enumerate() {
                let source_name = input
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("unnamed_{}", index + 1));
                let field = event_column(&source_name);
                if columns
                    .iter()
                    .any(|column: &SemanticColumn| column.field == field)
                {
                    bail!("event {name} maps multiple ABI parameters to SQLite column {field}");
                }
                columns.push(SemanticColumn {
                    field,
                    kind: input
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_owned(),
                    indexed: input
                        .get("indexed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    source_name,
                });
            }
            let types = columns
                .iter()
                .map(|column| column.kind.as_str())
                .collect::<Vec<_>>();
            let signature = format!("{name}({})", types.join(","));
            let table = format!(
                "{}__{}",
                alias.to_ascii_lowercase(),
                name.to_ascii_lowercase()
            );
            let Some(entry) = self.tables.iter_mut().find(|entry| entry.name == table) else {
                self.tables.push(SemanticTable {
                    name: table,
                    alias: alias.to_owned(),
                    event: name.to_owned(),
                    variants: vec![(signature, columns)],
                });
                continue;
            };
            let variant = (signature, columns);
            if !entry.variants.contains(&variant) {
                entry.variants.push(variant);
            }
        }
        Ok(())
    }

    fn render(&self, out: &mut String, existing: &toml::Table) {
        let topic0 = |signature: &str| format!("0x{}", hex::encode(Keccak256::digest(signature)));
        for table in &self.tables {
            let (name, alias, event) = (&table.name, &table.alias, &table.event);
            out.push_str(&format!(
                "[tables.{name:?}]\ndescription = {}\ncontract = {alias:?}\nevent = {event:?}\nsignature = {:?}\ntopic0 = {:?}\n",
                toml_string(
                    kept_description(existing, name, None)
                        .map_or_else(|| format!("{event} events emitted by {alias}"), str::to_owned)
                ),
                table.variants[0].0,
                topic0(&table.variants[0].0),
            ));
            // Overloads keep their own inputs, so `check` still sees a change to any one of them.
            if table.variants.len() > 1 {
                let overloads = table.variants[1..]
                    .iter()
                    .map(|(signature, inputs)| {
                        let inputs = inputs
                            .iter()
                            .map(|input| {
                                format!(
                                    "{{ source_name = {:?}, solidity_type = {:?}, indexed = {} }}",
                                    input.source_name, input.kind, input.indexed
                                )
                            })
                            .collect::<Vec<_>>();
                        format!(
                            "{{ signature = {signature:?}, topic0 = {:?}, inputs = [{}] }}",
                            topic0(signature),
                            inputs.join(", ")
                        )
                    })
                    .collect::<Vec<_>>();
                out.push_str(&format!("overloads = [{}]\n", overloads.join(", ")));
            }
            // Columns follow the sink: one per field name, described by its first definition.
            let mut columns = Vec::<&SemanticColumn>::new();
            for column in table.variants.iter().flat_map(|(_, inputs)| inputs) {
                if !columns.iter().any(|known| known.field == column.field) {
                    columns.push(column);
                }
            }
            for column in columns {
                let (field, kind) = (&column.field, &column.kind);
                out.push_str(&format!(
                    "[tables.{name:?}.columns.{field:?}]\ndescription = {}\nsolidity_type = {kind:?}\nindexed = {}\nsource_name = {:?}\n",
                    toml_string(
                        kept_description(existing, name, Some(field))
                            .map_or_else(|| format!("Solidity {kind} event parameter"), str::to_owned)
                    ),
                    column.indexed,
                    column.source_name
                ));
            }
            out.push('\n');
        }
    }
}

/// Descriptions may hold user text, so quote them as TOML rather than with Rust's `{:?}`.
fn toml_string(text: String) -> String {
    toml::Value::String(text).to_string()
}

fn event_column(field: &str) -> String {
    const RESERVED: &[&str] = &[
        "event_id",
        "chain_id",
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
    let backends = backend_guidance(config);
    std::fs::write(
        root.join("llms.txt"),
        format!(
            "# Streamling Blockchain Studio project\n\nContracts: {aliases}\n\nKeep this project on persistent storage. Streamling checkpoints live inside the project directory; restarting `streamling-blockchain dev` with the same project resumes from the last committed checkpoint. Use `streamling-blockchain status --json` to inspect backfill progress and `streamling-blockchain status --wait` to block until the safe chain head is indexed. Use `streamling-blockchain schema` before writing SQL. Query with `streamling-blockchain sql --json <QUERY>`. With `--json`, every command prints one JSON envelope with `ok`, `data`, `warnings`, and `error` (`code`, `message`, `retryable`, `suggested_next`); read results from `data` and branch on `error.code`. Quote Solidity names such as `from`. Discovered child contracts are filtered by Streamling dynamic tables before persistence.\n\n{backends}"
        ),
    )?;
    std::fs::create_dir_all(root.join("skills/streamling-blockchain-query"))?;
    std::fs::write(
        root.join("skills/streamling-blockchain-query/SKILL.md"),
        format!(
            "---\nname: streamling-blockchain-query\ndescription: Query a Streamling Blockchain Studio event database.\n---\n\nKeep the project on persistent storage and reuse it after restarts so Streamling resumes its checkpoint. Run `streamling-blockchain status --json` to inspect backfill progress or `streamling-blockchain status --wait` when work must begin only after catch-up. Run `streamling-blockchain schema` before querying, then use `streamling-blockchain sql --json '<SQL>'`. With `--json`, every command prints one JSON envelope with `ok`, `data`, `warnings`, and `error` (`code`, `message`, `retryable`, `suggested_next`); read results from `data` and branch on `error.code`. Prefer explicit columns, quote Solidity identifiers, and include LIMIT for exploratory queries.\n\n{backends}"
        ),
    )?;
    Ok(())
}

fn backend_guidance(config: &ProjectConfig) -> String {
    let mut out = String::new();
    if config.sinks.sqlite {
        out.push_str("SQLite sink: `events` holds every decoded event with fields as JSON in `data`, and `<alias>__<event>` tables hold one column per event field. Use SQLite SQL, for example `json_extract(data, '$.from')`. Integer fields are decimal strings; sort or compare them with `int_sortkey(value)`, not `CAST(value AS INTEGER)` or `CAST(value AS REAL)`, which silently break above i64 or lose precision on int256/uint256.\n");
    }
    if let Some(clickhouse) = &config.sinks.clickhouse {
        let table = clickhouse.database.as_deref().map_or_else(
            || clickhouse.table.clone(),
            |db| format!("{db}.{}", clickhouse.table),
        );
        out.push_str(&format!(
            "ClickHouse sink: `{table}` holds every decoded event with fields as JSON in `fields_json`; there are no per-event tables. Use ClickHouse SQL, for example `JSONExtractString(fields_json, 'from')`, and read current rows with `FROM {table} FINAL WHERE is_deleted = 0`. Connection settings come from the `STREAMLING__CLICKHOUSE_SINK__*` environment variables. Sort or do arithmetic on integer fields with `toUInt256(JSONExtractString(fields_json, 'value'))` or `toInt256(...)`, not a plain string or float comparison.\n"
        ));
    }
    if config.sinks.sqlite && config.sinks.clickhouse.is_some() {
        out.push_str("Commands read SQLite by default; pass `--backend clickhouse` to `sql`, `schema`, `replay`, `audit`, or `mcp` to read ClickHouse.\n");
    }
    out
}

pub fn add_contract(
    root: &Path,
    mut config: ProjectConfig,
    contract: ContractConfig,
    abi_source: &Path,
) -> Result<ProjectConfig> {
    let target = root.join(&contract.abi);
    config.contracts.push(contract);
    install_abi(root, config, abi_source, &target)
}

pub fn add_discovery_rule(
    root: &Path,
    mut config: ProjectConfig,
    rule: DiscoveryRule,
    abi_source: &Path,
) -> Result<ProjectConfig> {
    let target = root.join(&rule.child_abi);
    config.discovery_rules.push(rule);
    install_abi(root, config, abi_source, &target)
}

/// Validates the new config and ABI before copying, so a rejected add never replaces an existing ABI file.
fn install_abi(
    root: &Path,
    config: ProjectConfig,
    abi_source: &Path,
    target: &Path,
) -> Result<ProjectConfig> {
    config.validate()?;
    let abi = read_abi(abi_source)?;
    // The runtime and replay decode with the shared decoder, so reject what it rejects.
    streamling_blockchain_decode::parse_abi(
        &serde_json::to_vec(&abi)?,
        &abi_source.display().to_string(),
    )?;
    SemanticTables::default().add_abi("check", &abi)?;
    std::fs::create_dir_all(target.parent().context("ABI path has no parent")?)?;
    std::fs::copy(abi_source, target).with_context(|| format!("copy {}", abi_source.display()))?;
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
            max_window: None,
            log_concurrency: crate::config::default_log_concurrency(),
            block_concurrency: 8,
            block_batch_size: 1,
            index_blocks: true,
            index_transactions: true,
            skip_calldata: false,
            contracts: vec![ContractConfig {
                alias: "token".into(),
                address: "0x1111111111111111111111111111111111111111".into(),
                abi: PathBuf::from("abis/token.json"),
                start_block: None,
            }],
            discovery_rules: vec![],
            sinks: SinkConfig {
                sqlite: true,
                clickhouse: Some(ClickHouseSinkConfig {
                    table: "events".into(),
                    database: Some("analytics".into()),
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
            pipeline["sources"]["evm_blocks"]["options"]["concurrency"],
            "8"
        );
        assert_eq!(
            pipeline["sources"]["evm_transactions"]["options"]["concurrency"],
            "8"
        );
        // The Streamling host does not pass a source option named `batch_size` to plugins.
        assert_eq!(
            pipeline["sources"]["evm_blocks"]["options"]["rpc_batch_size"],
            "1"
        );
        assert!(
            pipeline["sources"]["evm_blocks"]["options"]
                .get("batch_size")
                .is_none()
        );
        assert!(
            pipeline["sources"]["evm_transactions"]["options"]
                .get("store_input")
                .is_none()
        );
        let skipping = ProjectConfig {
            skip_calldata: true,
            ..config.clone()
        };
        write_generated_files(&root, &skipping).unwrap();
        let skipped: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(root.join("pipeline.yaml")).unwrap())
                .unwrap();
        assert_eq!(
            skipped["sources"]["evm_transactions"]["options"]["store_input"],
            "false"
        );
        write_generated_files(&root, &config).unwrap();
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
        assert!(clickhouse_schema.starts_with("CREATE TABLE IF NOT EXISTS analytics.events ("));
        assert!(clickhouse_schema.contains("ENGINE = ReplacingMergeTree(insert_time, is_deleted)"));
        assert!(clickhouse_schema.contains("insert_time DateTime64(3) DEFAULT now64(3)"));
        assert!(clickhouse_schema.contains("PARTITION BY toYYYYMM(toDateTime(block_timestamp))"));
        assert!(
            clickhouse_schema.contains(
                "ORDER BY (chain_id, contract_alias, event_name, block_number, event_id);"
            )
        );
        assert_eq!(
            std::fs::read_to_string(root.join("clickhouse/database.sql")).unwrap(),
            "CREATE DATABASE IF NOT EXISTS analytics;\n"
        );
        let clickhouse_block_schema =
            std::fs::read_to_string(root.join("clickhouse/events_blocks.sql")).unwrap();
        assert!(clickhouse_block_schema.contains("block_id String"));
        assert!(clickhouse_block_schema.contains("transaction_count UInt64"));
        assert!(clickhouse_block_schema.contains("ORDER BY (chain_id, block_number, block_id);"));
        let clickhouse_transaction_schema =
            std::fs::read_to_string(root.join("clickhouse/events_transactions.sql")).unwrap();
        assert!(clickhouse_transaction_schema.contains("transaction_id String"));
        assert!(clickhouse_transaction_schema.contains("receipt_gas_used Nullable(UInt64)"));
        assert!(
            clickhouse_transaction_schema
                .contains("ORDER BY (chain_id, block_number, transaction_index, transaction_id);")
        );
        let llms = std::fs::read_to_string(root.join("llms.txt")).unwrap();
        assert!(llms.contains("`<alias>__<event>` tables"));
        assert!(llms.contains("FROM analytics.events FINAL WHERE is_deleted = 0"));
        assert!(llms.contains("--backend clickhouse"));
        assert!(llms.contains("branch on `error.code`"));
        let semantics = std::fs::read_to_string(root.join("semantic.toml")).unwrap();
        assert!(semantics.contains("signature = \"Transfer(address,address,uint256)\""));
        assert!(semantics.contains("topic0 = \"0xddf252ad"));
        assert!(semantics.contains("solidity_type = \"address\""));
        assert!(semantics.contains("indexed = true"));
        let _ = std::fs::remove_dir_all(&root);
    }

    const TRANSFER_ABI: &str = r#"[{"type":"event","name":"Transfer","inputs":[{"name":"from","type":"address","indexed":true},{"name":"to","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#;
    const APPROVAL_ABI: &str = r#"[{"type":"event","name":"Approval","inputs":[{"name":"owner","type":"address","indexed":true},{"name":"value","type":"uint256","indexed":false}]}]"#;

    fn token_project(name: &str) -> (PathBuf, ProjectConfig) {
        let root = std::env::temp_dir().join(format!(
            "streamling-blockchain-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("abis")).unwrap();
        std::fs::write(root.join("abis/token.json"), TRANSFER_ABI).unwrap();
        let config = ProjectConfig {
            chain: "base".into(),
            chain_id: 8453,
            rpc_url: "http://localhost:8545".into(),
            rpc_url_env: None,
            database: PathBuf::from(".streamling-blockchain/events.db"),
            start_block: 1,
            confirmations: 12,
            end_block: None,
            window: 2_000,
            max_window: None,
            log_concurrency: crate::config::default_log_concurrency(),
            block_concurrency: 8,
            block_batch_size: 1,
            index_blocks: false,
            index_transactions: false,
            skip_calldata: false,
            contracts: vec![ContractConfig {
                alias: "token".into(),
                address: "0x1111111111111111111111111111111111111111".into(),
                abi: PathBuf::from("abis/token.json"),
                start_block: None,
            }],
            discovery_rules: vec![],
            sinks: SinkConfig::default(),
        };
        config.save(&root).unwrap();
        write_generated_files(&root, &config).unwrap();
        (root, config)
    }

    #[test]
    fn rejected_add_contract_keeps_the_existing_abi() {
        let (root, config) = token_project("duplicate-alias");
        let other = root.join("other.json");
        std::fs::write(&other, APPROVAL_ABI).unwrap();

        let duplicate = ContractConfig {
            alias: "token".into(),
            address: "0x2222222222222222222222222222222222222222".into(),
            abi: PathBuf::from("abis/token.json"),
            start_block: None,
        };
        assert!(add_contract(&root, config.clone(), duplicate, &other).is_err());
        assert_eq!(
            std::fs::read_to_string(root.join("abis/token.json")).unwrap(),
            TRANSFER_ABI
        );

        let child = DiscoveryRule {
            parent_contract: "token".into(),
            discovery_event: "Transfer".into(),
            address_field: "to".into(),
            child_contract: "token".into(),
            child_abi: PathBuf::from("abis/token.json"),
        };
        assert!(add_discovery_rule(&root, config, child, &other).is_err());
        assert_eq!(
            std::fs::read_to_string(root.join("abis/token.json")).unwrap(),
            TRANSFER_ABI
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn regeneration_keeps_descriptions_and_check_reports_drift_without_writing() {
        let (root, config) = token_project("semantics");
        let path = root.join("semantic.toml");
        let edited = std::fs::read_to_string(&path)
            .unwrap()
            .replace(
                "description = \"Transfer events emitted by token\"",
                "description = \"Token moves \\\"with quotes\\\"\"",
            )
            .replace(
                "description = \"Solidity uint256 event parameter\"",
                "description = \"Amount in base units\"",
            );
        std::fs::write(&path, &edited).unwrap();
        assert!(check_semantics(&root, &config).unwrap().is_empty());

        write_semantics(&root, &config).unwrap();
        let regenerated = std::fs::read_to_string(&path).unwrap();
        assert!(regenerated.contains("Token moves"));
        assert!(regenerated.contains("description = \"Amount in base units\""));
        let parsed: toml::Table = toml::from_str(&regenerated).unwrap();
        assert_eq!(
            parsed["tables"]["token__transfer"]["description"].as_str(),
            Some("Token moves \"with quotes\"")
        );

        std::fs::write(root.join("abis/token.json"), APPROVAL_ABI).unwrap();
        let problems = check_semantics(&root, &config).unwrap();
        assert!(problems.contains(&"missing table token__approval".to_owned()));
        assert!(problems.contains(&"table token__transfer has no configured ABI event".to_owned()));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), regenerated);

        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            check_semantics(&root, &config).unwrap(),
            vec!["semantic.toml is missing".to_owned()]
        );
        assert!(!path.exists());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn add_contract_rejects_an_abi_the_runtime_cannot_parse() {
        let (root, config) = token_project("bad-abi-type");
        let broken = root.join("broken.json");
        std::fs::write(
            &broken,
            r#"[{"type":"event","name":"Broken","inputs":[{"name":"x","type":"notatype","indexed":false}]}]"#,
        )
        .unwrap();
        let contract = ContractConfig {
            alias: "broken".into(),
            address: "0x2222222222222222222222222222222222222222".into(),
            abi: PathBuf::from("abis/broken.json"),
            start_block: None,
        };
        assert!(add_contract(&root, config, contract, &broken).is_err());
        assert!(!root.join("abis/broken.json").exists());
        assert_eq!(ProjectConfig::load(&root).unwrap().contracts.len(), 1);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_child_alias_renders_once_and_broken_semantics_regenerates() {
        let (root, mut config) = token_project("shared-child");
        std::fs::write(root.join("abis/pool.json"), APPROVAL_ABI).unwrap();
        config.contracts.push(ContractConfig {
            alias: "factory_b".into(),
            address: "0x2222222222222222222222222222222222222222".into(),
            abi: PathBuf::from("abis/token.json"),
            start_block: None,
        });
        for parent in ["token", "factory_b"] {
            config.discovery_rules.push(DiscoveryRule {
                parent_contract: parent.into(),
                discovery_event: "Transfer".into(),
                address_field: "to".into(),
                child_contract: "pool".into(),
                child_abi: PathBuf::from("abis/pool.json"),
            });
        }
        std::fs::write(
            root.join("semantic.toml"),
            "[tables.\"x\"]\n[tables.\"x\"]\n",
        )
        .unwrap();
        assert!(check_semantics(&root, &config).unwrap()[0].contains("parse"));

        write_semantics(&root, &config).unwrap();
        let text = std::fs::read_to_string(root.join("semantic.toml")).unwrap();
        assert_eq!(text.matches("[tables.\"pool__approval\"]").count(), 1);
        assert!(check_semantics(&root, &config).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_alias_keeps_every_abi_and_overloads_share_one_table() {
        let (root, mut config) = token_project("overloads");
        std::fs::write(root.join("abis/pool.json"), APPROVAL_ABI).unwrap();
        std::fs::write(
            root.join("abis/pool_v2.json"),
            r#"[
              {"type":"event","name":"Changed","anonymous":false,"inputs":[{"name":"amount","type":"uint256","indexed":false}]},
              {"type":"event","name":"Changed","anonymous":false,"inputs":[{"name":"owner","type":"address","indexed":true}]},
              {"type":"event","name":"Changed","anonymous":false,"inputs":[{"name":"amount","type":"address","indexed":true}]}
            ]"#,
        )
        .unwrap();
        for child_abi in ["abis/pool.json", "abis/pool_v2.json"] {
            config.discovery_rules.push(DiscoveryRule {
                parent_contract: "token".into(),
                discovery_event: "Transfer".into(),
                address_field: "to".into(),
                child_contract: "pool".into(),
                child_abi: PathBuf::from(child_abi),
            });
        }

        write_semantics(&root, &config).unwrap();
        let text = std::fs::read_to_string(root.join("semantic.toml")).unwrap();
        let semantics: toml::Table = toml::from_str(&text).unwrap();
        let tables = semantics["tables"].as_table().unwrap();
        assert!(tables.contains_key("pool__approval"));
        let changed = &tables["pool__changed"];
        assert_eq!(changed["signature"].as_str(), Some("Changed(uint256)"));
        assert_eq!(
            changed["overloads"][0]["signature"].as_str(),
            Some("Changed(address)")
        );
        let columns = changed["columns"].as_table().unwrap();
        assert!(columns.contains_key("amount") && columns.contains_key("owner"));
        let overload = &changed["overloads"][1]["inputs"][0];
        assert_eq!(overload["solidity_type"].as_str(), Some("address"));
        assert_eq!(overload["indexed"].as_bool(), Some(true));
        assert!(check_semantics(&root, &config).unwrap().is_empty());

        // Only the shadowed overload changes; check must still see it.
        let abi = std::fs::read_to_string(root.join("abis/pool_v2.json")).unwrap();
        std::fs::write(
            root.join("abis/pool_v2.json"),
            abi.replace(
                r#"{"name":"amount","type":"address","indexed":true}"#,
                r#"{"name":"amount","type":"address","indexed":false}"#,
            ),
        )
        .unwrap();
        assert_eq!(
            check_semantics(&root, &config).unwrap(),
            vec!["table pool__changed does not match its ABI".to_owned()]
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
