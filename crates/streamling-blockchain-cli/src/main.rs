mod clickhouse;
mod config;
mod database;
mod doctor;
mod goldsky;
mod mcp;
mod output;
mod project;
mod publish;
mod replay;
mod rpc;
mod status;

use anyhow::{Context, Result, bail};
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum, error::ErrorKind};
use config::{
    ClickHouseSinkConfig, ContractConfig, DiscoveryRule, ProjectConfig, SinkConfig,
    validate_address, validate_alias,
};
use database::{Backend, BackendKind};
use output::{CodedError, ErrorCode, Output};

use serde_json::{Value, json};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{ExitCode, Stdio},
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
    /// Print one JSON envelope on stdout: {schema_version, ok, command, data, warnings, error}.
    #[arg(long, global = true)]
    json: bool,
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
        #[arg(long, default_value_t = 500)]
        max_rows: usize,
        #[arg(long = "attach", value_name = "ALIAS=DB")]
        attach: Vec<String>,
        /// Sink to query; defaults to SQLite when enabled, otherwise ClickHouse.
        #[arg(long, value_enum)]
        backend: Option<BackendKind>,
    },
    Schema {
        #[arg(long, value_enum)]
        backend: Option<BackendKind>,
    },
    Status {
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 5)]
        poll_seconds: u64,
    },
    Mcp {
        #[arg(long, value_enum)]
        backend: Option<BackendKind>,
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
        #[arg(long, value_enum)]
        backend: Option<BackendKind>,
    },
    Audit {
        #[arg(long, default_value_t = 1_000)]
        window_blocks: u64,
        #[arg(long, default_value_t = 300)]
        interval_seconds: u64,
        #[arg(long)]
        once: bool,
        #[arg(long, value_enum)]
        backend: Option<BackendKind>,
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
        apply: bool,
    },
    /// Manage the ClickHouse tables that the Streamling ClickHouse sink writes.
    Clickhouse {
        #[command(subcommand)]
        command: ClickhouseCommand,
    },
    /// Build a demo's Evidence site from this project's SQLite data and publish it.
    Publish(publish::PublishArgs),
}

#[derive(Subcommand)]
enum ClickhouseCommand {
    /// Print the ClickHouse DDL for a sink table set; no project is needed.
    Schema {
        #[arg(long, default_value = "events")]
        table: String,
        #[arg(long)]
        database: Option<String>,
        #[arg(long)]
        index_blocks: bool,
        #[arg(long)]
        index_transactions: bool,
    },
    /// Create the project's ClickHouse database and tables; `dev` also does this.
    Apply,
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
async fn main() -> ExitCode {
    let run = execute(std::env::args_os()).await;
    if run.json {
        println!("{}", run.envelope());
    } else if let Err(error) = &run.result {
        // Same text as returning the error from `main`.
        eprintln!("Error: {error:?}");
    }
    if run.result.is_ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

struct Run {
    json: bool,
    command: String,
    result: Result<Value>,
    warnings: Vec<String>,
}

impl Run {
    fn envelope(&self) -> Value {
        output::envelope(&self.command, &self.result, &self.warnings)
    }
}

/// Parses `args` and runs the command. Human output is printed as it happens; JSON mode
/// prints nothing on stdout and leaves the envelope to the caller.
async fn execute(args: impl IntoIterator<Item = impl Into<OsString>>) -> Run {
    let args = args.into_iter().map(Into::into).collect::<Vec<OsString>>();
    let matches = match Cli::command().try_get_matches_from(&args) {
        Ok(matches) => matches,
        Err(error) => {
            let json = args.iter().any(|arg| arg == "--json");
            if !json
                || matches!(
                    error.kind(),
                    ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
                )
            {
                error.exit()
            }
            let command = raw_command_path(&args);
            // clap's text without the usage block, on one line.
            let text = error.to_string();
            let message = text.split("\n\nUsage:").next().unwrap_or_default();
            let message = message.strip_prefix("error: ").unwrap_or(message);
            let error = CodedError::new(
                ErrorCode::Validation,
                message.split_whitespace().collect::<Vec<_>>().join(" "),
            )
            .next(if command.is_empty() {
                "streamling-blockchain --help".to_owned()
            } else {
                format!("streamling-blockchain {command} --help")
            });
            return Run {
                json,
                command,
                result: Err(error.into()),
                warnings: Vec::new(),
            };
        }
    };
    let mut path = Vec::new();
    let mut sub = &matches;
    while let Some((name, next)) = sub.subcommand() {
        path.push(name);
        sub = next;
    }
    let command = path.join(" ");
    let cli = Cli::from_arg_matches(&matches).expect("matches come from the Cli definition");
    let mut out = Output {
        json: cli.json,
        warnings: Vec::new(),
    };
    let result = dispatch(cli, &mut out).await;
    Run {
        json: out.json,
        command,
        result,
        warnings: out.warnings,
    }
}

/// The subcommand path in arguments that clap rejected, e.g. `clickhouse schema`.
fn raw_command_path(args: &[OsString]) -> String {
    let mut command = Cli::command();
    let mut path = Vec::new();
    for arg in args.iter().skip(1).filter_map(|arg| arg.to_str()) {
        if let Some(sub) = command.find_subcommand(arg).cloned() {
            path.push(sub.get_name().to_owned());
            command = sub;
        }
    }
    path.join(" ")
}

async fn dispatch(cli: Cli, out: &mut Output) -> Result<Value> {
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
            let (rpc_url, rpc_url_env, edge_endpoint) = if let Some(chain_id) = goldsky_chain_id {
                let endpoint = goldsky_endpoint
                    .as_deref()
                    .unwrap_or("streamling-blockchain");
                let url = goldsky::ensure_rpc(&goldsky_cli, endpoint, chain_id).await?;
                out.line(format!(
                    "✓ Goldsky Edge endpoint ready: {endpoint} · chain {chain_id}"
                ));
                (url, None, Some(endpoint.to_owned()))
            } else {
                if goldsky_endpoint.is_some() {
                    bail!(CodedError::new(
                        ErrorCode::Validation,
                        "--goldsky-endpoint requires --goldsky-chain-id"
                    ))
                }
                let url = if let Some(name) = &rpc_env {
                    std::env::var(name).with_context(|| format!("read ${name}"))?
                } else {
                    explicit_rpc.unwrap_or_default()
                };
                (url, rpc_env, None)
            };
            let mut data = init(
                &root,
                out,
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
            .await?;
            data["goldsky_endpoint"] = json!(edge_endpoint);
            Ok(data)
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
                out,
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
                    address: address.clone(),
                    abi: PathBuf::from(format!("abis/{alias}.json")),
                },
            )?;
            out.line(format!("✓ contract added · alias {alias}"));
            Ok(json!({"alias": alias, "address": address, "abi": target}))
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
                    parent_contract: parent_contract.clone(),
                    discovery_event: discovery_event.clone(),
                    address_field,
                    child_contract: child_contract.clone(),
                    child_abi: PathBuf::from(format!("abis/{child_contract}.json")),
                },
            )?;
            let table = format!("{child_contract}_contracts");
            out.line(format!("✓ discovery rule added · dynamic table {table}"));
            Ok(json!({
                "parent_contract": parent_contract,
                "discovery_event": discovery_event,
                "child_contract": child_contract,
                "table": table,
                "abi": target,
            }))
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
                out,
                &streamling,
                plugin,
                no_build,
                false,
                exit_when_caught_up,
                std::time::Duration::from_secs(exit_poll_seconds.max(1)),
            )
            .await?;
            status::cached(&root)
        }
        Commands::Sql {
            query,
            max_rows,
            attach,
            backend,
        } => {
            let config = ProjectConfig::load(&root)?;
            let backend = Backend::open(&root, &config, backend)?;
            match &backend {
                Backend::Sqlite(conn) => attach_databases(conn, attach)?,
                Backend::ClickHouse { .. } if !attach.is_empty() => {
                    bail!(
                        CodedError::new(
                            ErrorCode::Validation,
                            "--attach works only with the SQLite backend"
                        )
                        .next("streamling-blockchain sql --backend sqlite")
                    )
                }
                Backend::ClickHouse { .. } => {}
            }
            let value = backend.query(&query, max_rows.min(10_000)).await?;
            out.line(serde_json::to_string_pretty(&value)?);
            Ok(value)
        }
        Commands::Schema { backend } => {
            let config = ProjectConfig::load(&root)?;
            let backend = Backend::open(&root, &config, backend)?;
            let schema = backend.schema().await?;
            out.line(serde_json::to_string_pretty(&schema)?);
            let semantics = std::fs::read_to_string(root.join("semantic.toml")).unwrap_or_default();
            out.line(format!("\n{semantics}"));
            Ok(json!({"schema": schema, "semantics": semantics}))
        }
        Commands::Status { wait, poll_seconds } => {
            let value = if wait {
                status::wait(&root, std::time::Duration::from_secs(poll_seconds.max(1))).await?
            } else {
                status::live(&root).await?
            };
            if !out.json {
                let backfill = &value["backfill"];
                println!(
                    "chain: {} ({})\ncontracts: {}\ndiscovery rules: {}\ndatabase: {} ({})\nclickhouse: {}\nbackfill: {}\nindexed through: {}\nsafe head: {}\nremaining blocks: {}\nprogress: {:.2}%",
                    value["chain"].as_str().unwrap_or("unknown"),
                    value["chain_id"],
                    value["contracts"].as_array().map_or(0, Vec::len),
                    value["discovery_rules"].as_array().map_or(0, Vec::len),
                    value["database"].as_str().unwrap_or("unknown"),
                    if value["sinks"]["sqlite"].as_bool() == Some(false) {
                        "SQLite sink disabled"
                    } else if value["database_ready"].as_bool() == Some(true) {
                        "ready"
                    } else {
                        "not created"
                    },
                    match &value["sinks"]["clickhouse"] {
                        Value::Object(sink) => match sink.get("database").and_then(Value::as_str) {
                            Some(database) =>
                                format!("{database}.{}", sink["table"].as_str().unwrap_or("events")),
                            None => sink["table"].as_str().unwrap_or("events").to_owned(),
                        },
                        _ => "not configured".to_owned(),
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
            Ok(value)
        }
        Commands::Mcp { .. } if out.json => bail!(
            CodedError::new(
                ErrorCode::Validation,
                "mcp speaks the MCP protocol on stdout and does not accept --json"
            )
            .next("streamling-blockchain mcp")
        ),
        Commands::Mcp { backend } => {
            mcp::run_stdio(root, backend).await?;
            Ok(Value::Null)
        }
        Commands::Replay { from, to, backend } => {
            let value = replay::diff(&root, from, to, backend).await?;
            out.line(serde_json::to_string_pretty(&value)?);
            Ok(value)
        }
        Commands::Audit {
            window_blocks,
            interval_seconds,
            once,
            backend,
        } => {
            if once {
                let value = replay::audit_once(&root, window_blocks, backend).await?;
                out.line(serde_json::to_string_pretty(&value)?);
                Ok(value)
            } else {
                replay::audit_continuous(
                    &root,
                    out.json,
                    std::time::Duration::from_secs(interval_seconds.max(1)),
                    window_blocks,
                    backend,
                )
                .await?;
                Ok(Value::Null)
            }
        }
        Commands::Semantics { command } => {
            let config = ProjectConfig::load(&root)?;
            project::write_semantics(&root, &config)?;
            out.line(match command {
                SemanticsCommand::Generate => "✓ semantic.toml generated",
                SemanticsCommand::Check => "✓ semantic.toml is valid for configured ABIs",
            });
            Ok(json!({"path": root.join("semantic.toml"), "valid": true}))
        }
        Commands::Abi { command } => match command {
            AbiCommand::Fetch {
                address,
                chain_id,
                out: path,
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
                        let base = blockscout_base_url.as_deref().ok_or_else(|| {
                            CodedError::new(
                                ErrorCode::Validation,
                                "--blockscout-base-url is required with --source blockscout",
                            )
                        })?;
                        rpc::fetch_blockscout_abi(&client, &address, base).await?
                    }
                };
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&path, serde_json::to_vec_pretty(&abi)?)?;
                out.line(format!("✓ ABI written: {}", path.display()));
                Ok(json!({"path": path, "address": address, "chain_id": chain_id}))
            }
        },
        Commands::RpcDoctor { apply } => {
            let report = doctor::rpc_doctor(&root, apply).await?;
            out.line(serde_json::to_string_pretty(&report)?);
            Ok(report)
        }
        Commands::Clickhouse { command } => match command {
            ClickhouseCommand::Schema {
                table,
                database,
                index_blocks,
                index_transactions,
            } => {
                validate_alias(&table).context("ClickHouse table")?;
                if let Some(database) = &database {
                    validate_alias(database).context("ClickHouse database")?;
                }
                let statements = project::clickhouse_statements(
                    database.as_deref(),
                    &table,
                    index_blocks,
                    index_transactions,
                );
                for statement in &statements {
                    out.line(format!("{};\n", statement.sql));
                }
                Ok(json!(
                    statements
                        .iter()
                        .map(|s| s.sql.as_str())
                        .collect::<Vec<_>>()
                ))
            }
            ClickhouseCommand::Apply => {
                let config = ProjectConfig::load(&root)?;
                for warning in clickhouse::apply_schema(&config).await? {
                    out.warn(warning);
                }
                out.line("✓ ClickHouse tables ready");
                Ok(clickhouse_tables(&config))
            }
        },
        Commands::Publish(args) => {
            publish::run(&root, out, args, &std::env::vars().collect::<Vec<_>>()).await
        }
        Commands::Doctor { streamling, plugin } => {
            run_streamling(
                &root,
                out,
                &streamling,
                plugin,
                false,
                true,
                false,
                std::time::Duration::from_secs(5),
            )
            .await?;
            Ok(json!({"valid": true, "pipeline": root.join("pipeline.yaml")}))
        }
    }
}

async fn init(root: &Path, out: &Output, request: InitRequest) -> Result<Value> {
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
        bail!(CodedError::new(
            ErrorCode::Validation,
            format!("project already exists at {}", root.display())
        ))
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
    out.line(format!("✓ chain detected: {chain}"));
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
            out.line(format!("✓ deployment block: {value}"));
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
        bail!(CodedError::new(
            ErrorCode::Validation,
            "ABI declares no events"
        ))
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
    out.line(format!(
        "✓ {event_count} events · pipeline, semantic layer, MCP skill scaffolded"
    ));
    out.line("✓ project state is restart-safe on persistent storage");
    out.line(format!(
        "next: cargo build --release && streamling-blockchain --project {} dev",
        root.display()
    ));
    let mut data = project_summary(root, &config);
    data["event_count"] = event_count.into();
    Ok(data)
}

/// The `--json` result of `init` and `init-robinhood`.
fn project_summary(root: &Path, config: &ProjectConfig) -> Value {
    json!({
        "project": root,
        "config": root.join("streamling-blockchain.toml"),
        "pipeline": root.join("pipeline.yaml"),
        "semantics": root.join("semantic.toml"),
        "chain": config.chain,
        "chain_id": config.chain_id,
        "contracts": config.contracts.iter().map(|c| &c.alias).collect::<Vec<_>>(),
        "start_block": config.start_block,
        "end_block": config.end_block,
        "sinks": config.sinks,
        "next": format!("streamling-blockchain --project {} dev", root.display()),
    })
}

/// The `--json` result of `clickhouse apply`: the tables that now exist.
fn clickhouse_tables(config: &ProjectConfig) -> Value {
    let Some(sink) = &config.sinks.clickhouse else {
        return Value::Null;
    };
    let tables = project::clickhouse_statements(
        sink.database.as_deref(),
        &sink.table,
        config.index_blocks,
        config.index_transactions,
    )
    .into_iter()
    .filter(|statement| statement.sorting_key.is_some())
    .map(|statement| statement.name)
    .collect::<Vec<_>>();
    json!({"database": sink.database, "tables": tables})
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
#[allow(clippy::too_many_arguments)]
async fn init_robinhood(
    root: &Path,
    out: &Output,
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
) -> Result<Value> {
    if root.join("streamling-blockchain.toml").exists() {
        bail!(CodedError::new(
            ErrorCode::Validation,
            format!("project already exists at {}", root.display())
        ))
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
        bail!(CodedError::new(
            ErrorCode::Validation,
            format!("Robinhood Stock Tokens expected chain 4663, got {chain_id}")
        ))
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
    out.line(format!(
        "✓ {} Robinhood Stock Token contracts · unbounded project scaffolded",
        config.contracts.len()
    ));
    out.line(format!("✓ start block: {start_block}; end block: none"));
    out.line(format!(
        "next: cargo build --release && streamling-blockchain --project {} dev",
        root.display()
    ));
    Ok(project_summary(root, &config))
}

const ERC20_TRANSFER_ABI: &str = r#"[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"}]"#;

#[allow(clippy::too_many_arguments)]
async fn run_streamling(
    root: &Path,
    out: &mut Output,
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
            .stdout(out.child_stdout())
            .status()
            .await?;
        if !status.success() {
            bail!("plugin build failed")
        }
    }
    if !plugin.exists() {
        bail!(
            CodedError::new(
                ErrorCode::NotFound,
                format!("plugin library not found: {}", plugin.display())
            )
            .next("cargo build --release -p streamling-blockchain-plugin")
        )
    }
    let config = ProjectConfig::load(root)?;
    if !validate && config.sinks.clickhouse.is_some() {
        prepare_clickhouse(&config, out).await?;
    }
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
        .stdout(out.child_stdout())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
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
        bail!(CodedError::new(
            ErrorCode::Validation,
            "--exit-when-caught-up requires a bounded project with end_block"
        ))
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
                        stop_streamling(&mut child, streamling).await?;
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Creates the ClickHouse tables with the project's sorting key before the sink can create
/// them with its default key.
async fn prepare_clickhouse(config: &ProjectConfig, out: &mut Output) -> Result<()> {
    if std::env::var_os(clickhouse::URL_ENV).is_none() {
        out.warn(format!(
            "{} is not set, so ClickHouse tables were not prepared; Streamling will create missing tables ordered by their primary key only",
            clickhouse::URL_ENV
        ));
        return Ok(());
    }
    for warning in clickhouse::apply_schema(config).await? {
        out.warn(warning);
    }
    out.line("✓ ClickHouse tables ready");
    Ok(())
}

async fn stop_streamling(child: &mut tokio::process::Child, streamling: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let pid = child.id().context("Streamling process has no PID")?;
        let result = unsafe { libc::kill(pid as i32, libc::SIGINT) };
        if result != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("signal {}", streamling.display()));
        }
    }
    #[cfg(not(unix))]
    child
        .start_kill()
        .with_context(|| format!("stop {}", streamling.display()))?;

    let status = match tokio::time::timeout(std::time::Duration::from_secs(30), child.wait()).await
    {
        Ok(status) => status.with_context(|| format!("wait for {}", streamling.display()))?,
        Err(_) => {
            child
                .kill()
                .await
                .with_context(|| format!("force-stop {}", streamling.display()))?;
            let _ = child.wait().await;
            bail!(
                "{} did not checkpoint and stop within 30 seconds",
                streamling.display()
            )
        }
    };
    if !status.success() {
        bail!("Streamling exited with {status} during graceful shutdown")
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal project whose SQLite sink database the test writes itself.
    fn sqlite_project(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "streamling-blockchain-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join(".streamling-blockchain")).unwrap();
        std::fs::write(
            root.join("streamling-blockchain.toml"),
            "chain = \"ethereum\"\nchain_id = 1\nrpc_url = \"http://127.0.0.1:9\"\ndatabase = \".streamling-blockchain/events.db\"\nstart_block = 0\n\n[[contracts]]\nalias = \"token\"\naddress = \"0x1111111111111111111111111111111111111111\"\nabi = \"abis/token.json\"\n",
        )
        .unwrap();
        root
    }

    async fn run_json(args: &[&str]) -> Value {
        let run =
            execute(std::iter::once("streamling-blockchain").chain(args.iter().copied())).await;
        assert!(run.json);
        run.envelope()
    }

    #[tokio::test]
    async fn sql_json_wraps_rows_and_codes_failures() {
        let root = sqlite_project("sql-json");
        let project = root.to_str().unwrap();
        let database = root.join(".streamling-blockchain/events.db");

        let missing = run_json(&["--project", project, "sql", "SELECT 1", "--json"]).await;
        assert_eq!(missing["ok"], false);
        assert_eq!(missing["error"]["code"], "not_found");
        assert_eq!(
            missing["error"]["suggested_next"],
            "streamling-blockchain dev"
        );

        rusqlite::Connection::open(&database)
            .unwrap()
            .execute_batch(
                "CREATE TABLE events (event_name TEXT); INSERT INTO events VALUES ('Transfer'), ('Approval');",
            )
            .unwrap();
        let rows = run_json(&[
            "--project",
            project,
            "sql",
            "--json",
            "SELECT event_name FROM events ORDER BY event_name",
        ])
        .await;
        assert_eq!(
            rows,
            json!({
                "schema_version": 1,
                "ok": true,
                "command": "sql",
                "data": {
                    "columns": ["event_name"],
                    "rows": [{"event_name": "Approval"}, {"event_name": "Transfer"}],
                    "truncated": false
                },
                "warnings": [],
                "error": null
            })
        );

        let bad_sql = run_json(&["--json", "--project", project, "sql", "SELEC 1"]).await;
        assert_eq!(bad_sql["error"]["code"], "validation");
        let write = run_json(&["--json", "--project", project, "sql", "DELETE FROM events"]).await;
        assert_eq!(write["error"]["code"], "validation");

        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn mcp_refuses_json() {
        let value = run_json(&["mcp", "--json"]).await;
        assert_eq!(value["command"], "mcp");
        assert_eq!(value["error"]["code"], "validation");
    }

    #[tokio::test]
    async fn json_argument_errors_are_validation_envelopes() {
        let value = run_json(&["--json", "clickhouse", "schema", "--bogus"]).await;
        assert_eq!(value["command"], "clickhouse schema");
        assert_eq!(value["error"]["code"], "validation");
        assert!(
            value["error"]["message"]
                .as_str()
                .unwrap()
                .starts_with("unexpected argument '--bogus' found")
        );
        assert_eq!(
            value["error"]["suggested_next"],
            "streamling-blockchain clickhouse schema --help"
        );
    }

    #[tokio::test]
    async fn clickhouse_schema_data_is_the_statement_list() {
        let value = run_json(&["clickhouse", "schema", "--database", "analytics", "--json"]).await;
        assert_eq!(value["command"], "clickhouse schema");
        let statements = value["data"].as_array().unwrap();
        assert!(
            statements[0]
                .as_str()
                .unwrap()
                .starts_with("CREATE DATABASE")
        );
    }
}
