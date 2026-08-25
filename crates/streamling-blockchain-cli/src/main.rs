mod config;
mod database;
mod goldsky;
mod mcp;
mod project;
mod rpc;
mod status;
mod web;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use config::{
    ClickHouseSinkConfig, ContractConfig, DiscoveryRule, ProjectConfig, SinkConfig,
    validate_address, validate_alias,
};
use reqwest::Client;
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
        #[arg(long, default_value_t = 12)]
        confirmations: u64,
        #[arg(long, default_value_t = 2_000)]
        window: u64,
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
    },
    Sql {
        query: String,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 500)]
        max_rows: usize,
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
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SinkMode {
    Sqlite,
    Clickhouse,
    Both,
}

struct InitRequest {
    address: String,
    alias: String,
    rpc_url: String,
    rpc_url_env: Option<String>,
    abi: Option<PathBuf>,
    start_block: Option<u64>,
    confirmations: u64,
    window: u64,
    sink: SinkMode,
    clickhouse_table: String,
    clickhouse_database: Option<String>,
    clickhouse_compression: Option<String>,
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
            confirmations,
            window,
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
                    confirmations,
                    window,
                    sink,
                    clickhouse_table,
                    clickhouse_compression,
                    clickhouse_database,
                },
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
        } => run_streamling(&root, &streamling, plugin, no_build, false).await,
        Commands::Sql {
            query,
            json: as_json,
            max_rows,
        } => {
            let config = ProjectConfig::load(&root)?;
            let conn = database::open(&config.absolute_database(&root))?;
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
        Commands::Doctor { streamling, plugin } => {
            run_streamling(&root, &streamling, plugin, false, true).await
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
        confirmations,
        window,
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
    let client = Client::builder()
        .user_agent("streamling-blockchain/0.1")
        .build()?;
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
        confirmations,
        window,
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

async fn run_streamling(
    root: &Path,
    streamling: &Path,
    plugin: Option<PathBuf>,
    no_build: bool,
    validate: bool,
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
    let status = command
        .status()
        .await
        .with_context(|| format!("run {}", streamling.display()))?;
    if !status.success() {
        bail!("Streamling exited with {status}")
    }
    Ok(())
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
