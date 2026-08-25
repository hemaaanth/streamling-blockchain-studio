mod config;
mod database;
mod doctor;
mod goldsky;
mod mcp;
mod project;
mod replay;
mod rpc;
mod status;
mod web;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use config::{
    ClickHouseSinkConfig, ContractConfig, DiscoveryRule, ProjectConfig, SinkConfig,
    validate_address, validate_alias,
};

use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::Command;

#[derive(Parser)]
#[command(
    name = "streamling-blockchain",
    version,
    about = "Own, query, and publish blockchain data with Streamling"
)]
struct Cli {
    #[arg(long, global = true, default_value = ".")]
    project: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init {
        address: String,
        #[arg(long)]
        alias: String,
        #[arg(long, conflicts_with = "goldsky_chain_id")]
        rpc: Option<String>,
        #[arg(long, conflicts_with_all = ["rpc", "goldsky_chain_id"])]
        rpc_env: Option<String>,
        #[arg(long, conflicts_with = "rpc_env")]
        goldsky_chain_id: Option<u64>,
        #[arg(long)]
        goldsky_endpoint: Option<String>,
        #[arg(long, default_value = "goldsky")]
        goldsky_cli: PathBuf,
        #[arg(long)]
        abi: Option<PathBuf>,
        #[arg(long)]
        start_block: Option<u64>,
        #[arg(long)]
        end_block: Option<u64>,
        #[arg(long, default_value_t = 12)]
        confirmations: u64,
        #[arg(long, default_value_t = 2_000)]
        window: u64,
        #[arg(long)]
        index_blocks: bool,
        #[arg(long)]
        index_transactions: bool,
        #[arg(long, value_enum, default_value_t = SinkMode::Sqlite)]
        sink: SinkMode,
        #[arg(long, default_value = "events")]
        clickhouse_table: String,
        #[arg(long)]
        clickhouse_database: Option<String>,
        #[arg(long)]
        clickhouse_compression: Option<String>,
    },
    InitRobinhood {
        #[arg(long, default_value = "https://api.robinhood.com/rhj/assets")]
        assets_url: String,
        #[arg(
            long,
            default_value = "https://rpc.mainnet.chain.robinhood.com",
            conflicts_with = "rpc_env"
        )]
        rpc: String,
        #[arg(long, conflicts_with = "rpc")]
        rpc_env: Option<String>,
        #[arg(long, default_value_t = 0)]
        start_block: u64,
        #[arg(long, default_value_t = 12)]
        confirmations: u64,
        #[arg(long, default_value_t = 2_000)]
        window: u64,
        #[arg(long)]
        index_blocks: bool,
        #[arg(long)]
        index_transactions: bool,
        #[arg(long, value_enum, default_value_t = SinkMode::Sqlite)]
        sink: SinkMode,
        #[arg(long, default_value = "events")]
        clickhouse_table: String,
        #[arg(long)]
        clickhouse_database: Option<String>,
        #[arg(long)]
        clickhouse_compression: Option<String>,
    },
    AddContract {
        address: String,
        #[arg(long)]
        alias: String,
        #[arg(long)]
        abi: PathBuf,
    },
    AddDiscovery {
        #[arg(long)]
        parent_contract: String,
        #[arg(long)]
        discovery_event: String,
        #[arg(long)]
        address_field: String,
        #[arg(long)]
        child_contract: String,
        #[arg(long)]
        child_abi: PathBuf,
    },
    Dev {
        #[arg(long, default_value = "streamling")]
        streamling: PathBuf,
        #[arg(long)]
        plugin: Option<PathBuf>,
        #[arg(long)]
        no_build: bool,
        #[arg(long)]
        exit_when_caught_up: bool,
        #[arg(long, default_value_t = 5)]
        exit_poll_seconds: u64,
    },
    Sql {
        query: String,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 500)]
        max_rows: usize,
        #[arg(long = "attach", value_name = "ALIAS=DB")]
        attach: Vec<String>,
    },
    Schema,
    Status {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 5)]
        poll_seconds: u64,
    },
    Mcp,
    Serve {
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: String,
    },
    Doctor {
        #[arg(long, default_value = "streamling")]
        streamling: PathBuf,
        #[arg(long)]
        plugin: Option<PathBuf>,
    },
    Replay {
        #[arg(long)]
        from: u64,
        #[arg(long)]
        to: u64,
    },
    Audit {
        #[arg(long, default_value_t = 1_000)]
        window_blocks: u64,
        #[arg(long, default_value_t = 300)]
        interval_seconds: u64,
        #[arg(long)]
        once: bool,
    },
    Semantics {
        #[command(subcommand)]
        command: SemanticsCommand,
    },
    Abi {
        #[command(subcommand)]
        command: AbiCommand,
    },
    RpcDoctor {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        apply: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SinkMode {
    Sqlite,
    Clickhouse,
    Both,
}

#[derive(Subcommand)]
enum SemanticsCommand {
    Generate,
    Check,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum AbiSource {
    Sourcify,
    Etherscan,
    #[value(name = "etherscan-v1")]
    EtherscanV1,
    Blockscout,
}

#[derive(Subcommand)]
enum AbiCommand {
    Fetch {
        address: String,
        #[arg(long)]
        chain_id: u64,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, value_enum, default_value_t = AbiSource::Sourcify)]
        source: AbiSource,
        #[arg(long)]
        etherscan_base_url: Option<String>,
        #[arg(long)]
        etherscan_api_key_env: Option<String>,
        #[arg(long)]
        etherscan_v2_base_url: Option<String>,
        #[arg(long)]
        blockscout_base_url: Option<String>,
    },
}

struct InitRequest {
    address: String,
    alias: String,
    rpc_url: String,
    rpc_url_env: Option<String>,
    abi: Option<PathBuf>,
    start_block: Option<u64>,
    end_block: Option<u64>,
    confirmations: u64,
    window: u64,
    index_blocks: bool,
    index_transactions: bool,
    sink: SinkMode,
    clickhouse_table: String,
    clickhouse_database: Option<String>,
    clickhouse_compression: Option<String>,
}

struct AttachedDatabase {
    alias: String,
    path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let root = absolute(&cli.project)?;
    match cli.command {
        Commands::Init {
            address,
            alias,
            rpc: explicit_rpc,
            rpc_env,
            goldsky_chain_id,
            goldsky_endpoint,
            goldsky_cli,
            abi,
            start_block,
            end_block,
            confirmations,
            window,
            index_blocks,
            index_transactions,
            sink,
            clickhouse_table,
            clickhouse_compression,
            clickhouse_database,
        } => {
            let (rpc_url, rpc_url_env) = if let Some(chain_id) = goldsky_chain_id {
                let endpoint = goldsky_endpoint
                    .as_deref()
                    .unwrap_or("streamling-blockchain");
                let url = goldsky::ensure_rpc(&goldsky_cli, endpoint, chain_id).await?;
                println!("✓ Goldsky Edge endpoint ready: {endpoint} · chain {chain_id}");
                (url, None)
            } else {
                if goldsky_endpoint.is_some() {
                    bail!("--goldsky-endpoint requires --goldsky-chain-id")
                }
                let url = if let Some(name) = &rpc_env {
                    std::env::var(name).with_context(|| format!("read ${name}"))?
                } else {
                    explicit_rpc.unwrap_or_default()
                };
                (url, rpc_env)
            };
            init(
                &root,
                InitRequest {
                    address,
                    alias,
                    rpc_url,
                    rpc_url_env,
                    abi,
                    start_block,
                    end_block,
                    confirmations,
                    window,
                    index_blocks,
                    index_transactions,
                    sink,
                    clickhouse_table,
                    clickhouse_compression,
                    clickhouse_database,
                },
            )
            .await
        }
        Commands::InitRobinhood {
            assets_url,
            rpc,
            rpc_env,
            start_block,
            confirmations,
            window,
            index_blocks,
            index_transactions,
            sink,
            clickhouse_table,
            clickhouse_database,
            clickhouse_compression,
        } => {
            init_robinhood(
                &root,
                assets_url,
                rpc,
                rpc_env,
                start_block,
                confirmations,
                window,
                index_blocks,
                index_transactions,
                sink,
                clickhouse_table,
                clickhouse_database,
                clickhouse_compression,
            )
            .await
        }
        Commands::AddContract {
            address,
            alias,
            abi,
        } => {
            validate_address(&address)?;
            validate_alias(&alias)?;
            let config = ProjectConfig::load(&root)?;
            let target = root.join("abis").join(format!("{alias}.json"));
            std::fs::create_dir_all(target.parent().unwrap())?;
            std::fs::copy(&abi, &target).with_context(|| format!("copy {}", abi.display()))?;
            project::add_contract(
                &root,
                config,
                ContractConfig {
                    alias: alias.clone(),
                    address,
                    abi: PathBuf::from(format!("abis/{alias}.json")),
                },
            )?;
            println!("✓ contract added · alias {alias}");
            Ok(())
        }
        Commands::AddDiscovery {
            parent_contract,
            discovery_event,
            address_field,
            child_contract,
            child_abi,
        } => {
            validate_alias(&child_contract)?;
            let config = ProjectConfig::load(&root)?;
            let target = root.join("abis").join(format!("{child_contract}.json"));
            std::fs::create_dir_all(target.parent().unwrap())?;
            std::fs::copy(&child_abi, &target)
                .with_context(|| format!("copy {}", child_abi.display()))?;
            project::add_discovery_rule(
                &root,
                config,
                DiscoveryRule {
                    parent_contract,
                    discovery_event,
                    address_field,
                    child_contract: child_contract.clone(),
                    child_abi: PathBuf::from(format!("abis/{child_contract}.json")),
                },
            )?;
            println!("✓ discovery rule added · dynamic table {child_contract}_contracts");
            Ok(())
        }
        Commands::Dev {
            streamling,
            plugin,
            no_build,
            exit_when_caught_up,
            exit_poll_seconds,
        } => {
            run_streamling(
                &root,
                &streamling,
                plugin,
                no_build,
                false,
                exit_when_caught_up,
                std::time::Duration::from_secs(exit_poll_seconds.max(1)),
            )
            .await
        }
        Commands::Sql {
            query,
            json: as_json,
            max_rows,
            attach,
        } => {
            let config = ProjectConfig::load(&root)?;
            let conn = database::open(&config.absolute_database(&root))?;
            attach_databases(&conn, attach)?;
            let value = database::query(&conn, &query, max_rows.min(10_000))?;
            if as_json {
                println!("{}", serde_json::to_string(&value)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&value)?);
            }
            Ok(())
        }
        Commands::Schema => {
            let config = ProjectConfig::load(&root)?;
            let conn = database::open(&config.absolute_database(&root))?;
            println!(
                "{}",
                serde_json::to_string_pretty(&database::schema(&conn)?)?
            );
            let semantics = std::fs::read_to_string(root.join("semantic.toml")).unwrap_or_default();
            println!("\n{semantics}");
            Ok(())
        }
        Commands::Status {
            json: as_json,
            wait,
            poll_seconds,
        } => {
            let value = if wait {
                status::wait(&root, std::time::Duration::from_secs(poll_seconds.max(1))).await?
            } else {
                status::live(&root).await?
            };
            if as_json {
                println!("{}", serde_json::to_string(&value)?);
            } else {
                let backfill = &value["backfill"];
                println!(
                    "chain: {} ({})\ncontracts: {}\ndiscovery rules: {}\ndatabase: {} ({})\nbackfill: {}\nindexed through: {}\nsafe head: {}\nremaining blocks: {}\nprogress: {:.2}%",
                    value["chain"].as_str().unwrap_or("unknown"),
                    value["chain_id"],
                    value["contracts"].as_array().map_or(0, Vec::len),
                    value["discovery_rules"].as_array().map_or(0, Vec::len),
                    value["database"].as_str().unwrap_or("unknown"),
                    if value["database_ready"].as_bool() == Some(true) {
                        "ready"
                    } else {
                        "not created"
                    },
                    backfill["state"].as_str().unwrap_or("unknown"),
                    backfill["indexed_through"]
                        .as_u64()
                        .map_or_else(|| "not started".into(), |height| height.to_string()),
                    backfill["safe_head"]
                        .as_u64()
                        .map_or_else(|| "unknown".into(), |height| height.to_string()),
                    backfill["remaining_blocks"]
                        .as_u64()
                        .map_or_else(|| "unknown".into(), |blocks| blocks.to_string()),
                    backfill["progress_percent"].as_f64().unwrap_or(0.0),
                );
            }
            Ok(())
        }
        Commands::Mcp => mcp::run_stdio(root),
        Commands::Serve { bind } => web::serve(root, bind).await,
        Commands::Replay { from, to } => {
            let value = replay::diff(&root, from, to).await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
            Ok(())
        }
        Commands::Audit {
            window_blocks,
            interval_seconds,
            once,
        } => {
            if once {
                let value = replay::audit_once(&root, window_blocks).await?;
                println!("{}", serde_json::to_string_pretty(&value)?);
                Ok(())
            } else {
                replay::audit_continuous(
                    &root,
                    std::time::Duration::from_secs(interval_seconds.max(1)),
                    window_blocks,
                )
                .await
            }
        }
        Commands::Semantics { command } => {
            let config = ProjectConfig::load(&root)?;
            project::write_semantics(&root, &config)?;
            match command {
                SemanticsCommand::Generate => {
                    println!("✓ semantic.toml generated");
                    Ok(())
                }
                SemanticsCommand::Check => {
                    println!("✓ semantic.toml is valid for configured ABIs");
                    Ok(())
                }
            }
        }
        Commands::Abi { command } => match command {
            AbiCommand::Fetch {
                address,
                chain_id,
                out,
                source,
                etherscan_base_url,
                etherscan_api_key_env,
                etherscan_v2_base_url,
                blockscout_base_url,
            } => {
                validate_address(&address)?;
                let client = rpc::client()?;
                let api_key = etherscan_api_key_env
                    .as_deref()
                    .map(std::env::var)
                    .transpose()
                    .context("read Etherscan API key env")?;
                let abi = match source {
                    AbiSource::Sourcify => {
                        rpc::fetch_sourcify_abi(&client, chain_id, &address).await?
                    }
                    AbiSource::Etherscan => {
                        rpc::fetch_etherscan_v2_abi(
                            &client,
                            chain_id,
                            &address,
                            api_key.as_deref(),
                            etherscan_v2_base_url.as_deref(),
                        )
                        .await?
                    }
                    AbiSource::EtherscanV1 => {
                        rpc::fetch_etherscan_compatible_abi(
                            &client,
                            chain_id,
                            &address,
                            etherscan_base_url.as_deref(),
                            api_key.as_deref(),
                        )
                        .await?
                    }
                    AbiSource::Blockscout => {
                        let base = blockscout_base_url.as_deref().context(
                            "--blockscout-base-url is required with --source blockscout",
                        )?;
                        rpc::fetch_blockscout_abi(&client, &address, base).await?
                    }
                };
                if let Some(parent) = out.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&out, serde_json::to_vec_pretty(&abi)?)?;
                println!("✓ ABI written: {}", out.display());
                Ok(())
            }
        },
        Commands::RpcDoctor {
            json: as_json,
            apply,
        } => {
            let report = doctor::rpc_doctor(&root, apply).await?;
            if as_json {
                println!("{}", serde_json::to_string(&report)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            Ok(())
        }
        Commands::Doctor { streamling, plugin } => {
            run_streamling(
                &root,
                &streamling,
                plugin,
                false,
                true,
                false,
                std::time::Duration::from_secs(5),
            )
            .await
        }
    }
}

async fn init(root: &Path, request: InitRequest) -> Result<()> {
    let InitRequest {
        address,
        alias,
        rpc_url: explicit_rpc,
        rpc_url_env,
        abi: abi_path,
        start_block,
        end_block,
        confirmations,
        window,
        index_blocks,
        index_transactions,
        sink,
        clickhouse_table,
        clickhouse_database,
        clickhouse_compression,
    } = request;
    validate_address(&address)?;
    validate_alias(&alias)?;
    if matches!(sink, SinkMode::Clickhouse | SinkMode::Both) {
        validate_alias(&clickhouse_table).context("ClickHouse table")?;
    }
    if root.join("streamling-blockchain.toml").exists() {
        bail!("project already exists at {}", root.display())
    }
    std::fs::create_dir_all(root.join("abis"))?;
    std::fs::create_dir_all(root.join(".streamling-blockchain"))?;
    let client = rpc::client()?;
    let (chain, chain_id, rpc_url) = rpc::detect_chain(
        &client,
        &address,
        (!explicit_rpc.is_empty()).then_some(explicit_rpc.as_str()),
    )
    .await?;
    println!("✓ chain detected: {chain}");
    let abi: Value = if let Some(path) = abi_path {
        serde_json::from_slice(
            &std::fs::read(&path).with_context(|| format!("read {}", path.display()))?,
        )?
    } else {
        rpc::fetch_abi(&client, chain_id, &address).await?
    };
    let abi_target = root.join("abis").join(format!("{alias}.json"));
    std::fs::write(&abi_target, serde_json::to_vec_pretty(&abi)?)?;
    let from = match start_block {
        Some(value) => value,
        None => {
            let value = rpc::deployment_block(&client, &rpc_url, &address).await?;
            println!("✓ deployment block: {value}");
            value
        }
    };
    let event_count = abi
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter(|item| item.get("type").and_then(Value::as_str) == Some("event"))
                .count()
        })
        .unwrap_or(0);
    if event_count == 0 {
        bail!("ABI declares no events")
    }
    let sinks = SinkConfig {
        sqlite: matches!(sink, SinkMode::Sqlite | SinkMode::Both),
        clickhouse: matches!(sink, SinkMode::Clickhouse | SinkMode::Both).then_some(
            ClickHouseSinkConfig {
                table: clickhouse_table,
                database: clickhouse_database,
                compression: clickhouse_compression,
            },
        ),
    };
    let config = ProjectConfig {
        chain,
        chain_id,
        rpc_url: if rpc_url_env.is_some() {
            String::new()
        } else {
            rpc_url
        },
        rpc_url_env,
        database: PathBuf::from(".streamling-blockchain/events.db"),
        start_block: from,
        end_block,
        confirmations,
        window,
        index_blocks,
        index_transactions,
        contracts: vec![ContractConfig {
            alias: alias.clone(),
            address,
            abi: PathBuf::from(format!("abis/{alias}.json")),
        }],
        discovery_rules: vec![],
        sinks,
    };
    config.save(root)?;
    project::write_generated_files(root, &config)?;
    println!("✓ {event_count} events · pipeline, semantic layer, MCP skill scaffolded");
    println!("✓ project state is restart-safe on persistent storage");
    println!(
        "next: cargo build --release && streamling-blockchain --project {} dev",
        root.display()
    );
    Ok(())
}

fn parse_attach(value: &str) -> Result<AttachedDatabase> {
    let (alias, path) = value
        .split_once('=')
        .context("--attach must use ALIAS=DB")?;
    validate_alias(alias)?;
    Ok(AttachedDatabase {
        alias: alias.to_owned(),
        path: PathBuf::from(path),
    })
}

fn attach_databases(conn: &rusqlite::Connection, values: Vec<String>) -> Result<()> {
    for value in values {
        let database = parse_attach(&value)?;
        conn.execute(
            &format!(
                "ATTACH DATABASE ?1 AS {}",
                quote_identifier(&database.alias)
            ),
            [database.path.to_string_lossy().as_ref()],
        )
        .with_context(|| format!("attach {}", database.path.display()))?;
    }
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
async fn init_robinhood(
    root: &Path,
    assets_url: String,
    rpc_url: String,
    rpc_url_env: Option<String>,
    start_block: u64,
    confirmations: u64,
    window: u64,
    index_blocks: bool,
    index_transactions: bool,
    sink: SinkMode,
    clickhouse_table: String,
    clickhouse_database: Option<String>,
    clickhouse_compression: Option<String>,
) -> Result<()> {
    if root.join("streamling-blockchain.toml").exists() {
        bail!("project already exists at {}", root.display())
    }
    let client = rpc::client()?;
    let actual_rpc = if let Some(name) = &rpc_url_env {
        std::env::var(name).with_context(|| format!("read ${name}"))?
    } else {
        rpc_url.clone()
    };
    let chain_id =
        rpc::hex_u64(&rpc::rpc(&client, &actual_rpc, "eth_chainId", serde_json::json!([])).await?)?;
    if chain_id != 4663 {
        bail!("Robinhood Stock Tokens expected chain 4663, got {chain_id}")
    }
    let payload: Value = client
        .get(&assets_url)
        .send()
        .await
        .with_context(|| format!("fetch {assets_url}"))?
        .json()
        .await
        .with_context(|| format!("decode {assets_url}"))?;
    let mut contracts = Vec::new();
    if let Some(assets) = payload.get("assets").and_then(Value::as_array) {
        for asset in assets {
            if asset.get("status").and_then(Value::as_str) != Some("ASSET_STATUS_ACTIVE") {
                continue;
            }
            let Some(symbol) = asset.get("tokenSymbol").and_then(Value::as_str) else {
                continue;
            };
            let Some(deployments) = asset.get("deployments").and_then(Value::as_array) else {
                continue;
            };
            for deployment in deployments {
                if deployment.get("chainId").and_then(Value::as_u64) != Some(4663) {
                    continue;
                }
                let Some(address) = deployment.get("contractAddress").and_then(Value::as_str)
                else {
                    continue;
                };
                validate_address(address)?;
                let alias = symbol.to_ascii_lowercase().replace(['.', '-'], "_");
                validate_alias(&alias)?;
                contracts.push(ContractConfig {
                    alias,
                    address: address.to_owned(),
                    abi: PathBuf::from("abis/erc20.json"),
                });
            }
        }
    }
    if contracts.is_empty() {
        bail!("Robinhood assets response did not include active chain 4663 deployments")
    }
    std::fs::create_dir_all(root.join("abis"))?;
    std::fs::create_dir_all(root.join(".streamling-blockchain"))?;
    std::fs::write(root.join("abis/erc20.json"), ERC20_TRANSFER_ABI)?;
    let sinks = SinkConfig {
        sqlite: matches!(sink, SinkMode::Sqlite | SinkMode::Both),
        clickhouse: matches!(sink, SinkMode::Clickhouse | SinkMode::Both).then_some(
            ClickHouseSinkConfig {
                table: clickhouse_table,
                database: clickhouse_database,
                compression: clickhouse_compression,
            },
        ),
    };
    let config = ProjectConfig {
        chain: "robinhood-chain".into(),
        chain_id,
        rpc_url: if rpc_url_env.is_some() {
            String::new()
        } else {
            rpc_url
        },
        rpc_url_env,
        database: PathBuf::from(".streamling-blockchain/events.db"),
        start_block,
        end_block: None,
        confirmations,
        window,
        index_blocks,
        index_transactions,
        contracts,
        discovery_rules: vec![],
        sinks,
    };
    config.save(root)?;
    project::write_generated_files(root, &config)?;
    println!(
        "✓ {} Robinhood Stock Token contracts · unbounded project scaffolded",
        config.contracts.len()
    );
    println!("✓ start block: {start_block}; end block: none");
    println!(
        "next: cargo build --release && streamling-blockchain --project {} dev",
        root.display()
    );
    Ok(())
}

const ERC20_TRANSFER_ABI: &str = r#"[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;

async fn run_streamling(
    root: &Path,
    streamling: &Path,
    plugin: Option<PathBuf>,
    no_build: bool,
    validate: bool,
    exit_when_caught_up: bool,
    exit_poll: std::time::Duration,
) -> Result<()> {
    let plugin = match plugin {
        Some(path) => absolute_from(root, &path),
        None => default_plugin_path()?,
    };
    if !plugin.exists() && !no_build {
        let status = Command::new("cargo")
            .args(["build", "--release", "-p", "streamling-blockchain-plugin"])
            .status()
            .await?;
        if !status.success() {
            bail!("plugin build failed")
        }
    }
    if !plugin.exists() {
        bail!("plugin library not found: {}", plugin.display())
    }
    let config = ProjectConfig::load(root)?;
    let mut command = Command::new(streamling);
    command
        .current_dir(root)
        .env("STREAMLING__PLUGIN__PATH", &plugin)
        .arg(root.join("pipeline.yaml"));
    if let Some(clickhouse) = &config.sinks.clickhouse
        && let Some(database) = &clickhouse.database
    {
        command.env("STREAMLING__CLICKHOUSE_SINK__DATABASE", database);
    }
    if validate {
        command.arg("--validate");
    }
    command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut child = command
        .spawn()
        .with_context(|| format!("run {}", streamling.display()))?;
    if !exit_when_caught_up {
        let status = child
            .wait()
            .await
            .with_context(|| format!("wait for {}", streamling.display()))?;
        if !status.success() {
            bail!("Streamling exited with {status}")
        }
        return Ok(());
    }
    if config.end_block.is_none() {
        bail!("--exit-when-caught-up requires a bounded project with end_block")
    }
    loop {
        tokio::select! {
            status = child.wait() => {
                let status = status.with_context(|| format!("wait for {}", streamling.display()))?;
                if !status.success() {
                    bail!("Streamling exited with {status}")
                }
                return Ok(());
            }
            _ = tokio::time::sleep(exit_poll) => {
                if status::live(root).await?["backfill"]["caught_up"].as_bool() == Some(true) {
                    tokio::time::sleep(exit_poll).await;
                    if status::live(root).await?["backfill"]["caught_up"].as_bool() == Some(true) {
                        child.kill().await.with_context(|| format!("stop {}", streamling.display()))?;
                        let _ = child.wait().await;
                        return Ok(());
                    }
                }
            }
        }
    }
}

fn default_plugin_path() -> Result<PathBuf> {
    let exe = std::env::current_exe()?;
    let exe_dir = exe.parent().context("executable has no parent")?;
    let name = if cfg!(target_os = "macos") {
        "libstreamling_blockchain_plugin.dylib"
    } else if cfg!(target_os = "windows") {
        "streamling_blockchain_plugin.dll"
    } else {
        "libstreamling_blockchain_plugin.so"
    };
    let sibling = exe_dir.join(name);
    if sibling.exists() {
        return Ok(sibling);
    }
    Ok(PathBuf::from("target/release").join(name))
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}
fn absolute_from(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}
