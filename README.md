# Streamling Blockchain Studio

Streamling Blockchain Studio turns EVM contract events into a local SQLite database. It provides a Rust CLI for project setup and operation, a native Streamling source/sink plugin, read-only SQL over CLI/HTTP/MCP, live backfill status, dynamic contract discovery, and optional Goldsky Edge RPC provisioning.

This is an independent Goldsky project built on the general-purpose [Streamling](https://www.streamling.dev/) runtime.

## Prerequisites

- Rust toolchain compatible with edition 2024
- A separately installed `streamling` executable compatible with `streamling-plugin` 0.2.x
- An EVM RPC URL, or the Goldsky CLI 13.9.0 or newer for automatic Edge endpoint creation
- A contract address and ABI; the CLI can fetch verified ABI metadata when the RPC supports it

Evidence is optional. The example under `evidence/` requires a separate Evidence installation and ClickHouse.

## Build

```sh
cargo build --release --workspace
```

The CLI is `target/release/streamling-blockchain`. The native plugin is built beside it as `libstreamling_blockchain_plugin` with the platform library extension.

## Create and run a project

With an existing RPC URL:

```sh
streamling-blockchain --project ./my-project init \
  0x1111111111111111111111111111111111111111 \
  --alias protocol \
  --rpc https://example-rpc.invalid \
  --abi ./protocol.json \
  --start-block 12345678

streamling-blockchain --project ./my-project dev
```

With Goldsky Edge provisioning:

```sh
streamling-blockchain --project ./my-project init \
  0x1111111111111111111111111111111111111111 \
  --alias protocol \
  --goldsky-chain-id 1 \
  --abi ./protocol.json \
  --start-block 12345678
```

For an analytics warehouse or Evidence dashboard, generate a ClickHouse sink in the same Streamling pipeline:

```sh
streamling-blockchain --project ./my-project init \
  0x1111111111111111111111111111111111111111 \
  --alias protocol \
  --rpc https://example-rpc.invalid \
  --abi ./protocol.json \
  --start-block 12345678 \
  --sink both \
  --clickhouse-table events
```

`--sink sqlite` is the default local mode. `--sink clickhouse` writes only through Streamling's built-in ClickHouse sink; `--sink both` keeps the local SQLite database and adds a ClickHouse sidecar sink. Configure the ClickHouse connection with Streamling's environment variables, including `STREAMLING__CLICKHOUSE_SINK__URL`, `STREAMLING__CLICKHOUSE_SINK__USER`, `STREAMLING__CLICKHOUSE_SINK__PASSWORD`, and `STREAMLING__CLICKHOUSE_SINK__DATABASE`. The CLI also writes `clickhouse/<table>.sql` as a readable schema for manual setup or review.

Goldsky setup checks the installed CLI contract, reuses a named Edge endpoint when present, creates it otherwise, reveals its API key through the authenticated CLI, and validates chain bytecode before creating the project.

Add more fixed contracts to the same pipeline:

```sh
streamling-blockchain --project ./my-project add-contract \
  0x2222222222222222222222222222222222222222 \
  --alias helper \
  --abi ./helper.json
```

Add contracts discovered from an indexed parent event:

```sh
streamling-blockchain --project ./my-project add-discovery \
  --parent-contract protocol \
  --discovery-event ContractCreated \
  --address-field contractAddress \
  --child-contract deployment \
  --child-abi ./deployment.json
```

## Query and inspect

```sh
streamling-blockchain --project ./my-project status --json
streamling-blockchain --project ./my-project status --wait
streamling-blockchain --project ./my-project schema
streamling-blockchain --project ./my-project sql --json \
  'SELECT event_name, count(*) AS events FROM events GROUP BY event_name LIMIT 20'
streamling-blockchain --project ./my-project serve
streamling-blockchain --project ./my-project mcp
```

`serve` exposes the Studio SQL page and JSON endpoints on `127.0.0.1:8787` by default. MCP is available over stdio with the tools `streamling_blockchain_schema`, `streamling_blockchain_query`, and `streamling_blockchain_status`.

## Project state and persistence

Each initialized project contains:

- `streamling-blockchain.toml`: source configuration; may contain an RPC credential
- `pipeline.yaml`: generated Streamling pipeline; may also contain the RPC credential
- `semantic.toml`: generated descriptions for agent-safe query construction
- `backfill-progress.json`: observed, safe, and indexed block heights
- `abis/`: copied contract ABIs
- `.streamling-blockchain/events.db`: decoded events when the SQLite sink is enabled
- `clickhouse/<table>.sql`: generated ClickHouse schema when the ClickHouse sink is enabled
- `state.db`: Streamling's committed source cursor and discovered-contract registry
- `web/index.html`, `llms.txt`, and `skills/`: generated query surfaces for the local SQLite database

Keep the entire project directory on persistent storage. Restarting `dev` with the same directory resumes from Streamling's last committed checkpoint. Copying only the SQLite event database is insufficient: it omits the source cursor and can cause a replay from the configured start block.

The CLI writes credential-bearing configuration with mode `0600` on Unix. Do not commit generated projects, RPC URLs containing keys, `access/`, `preprocessed_configs/`, Evidence connection files, databases, or WAL files.

## Development checks

```sh
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --release --workspace
```

For a behavioral smoke test, initialize against a known RPC fixture, run `dev`, wait for `status --wait`, then query the indexed events through CLI, HTTP, and MCP.

## Repository layout

- `crates/streamling-blockchain-cli/`: CLI, Goldsky provisioning, SQL/MCP/HTTP surfaces
- `crates/streamling-blockchain-plugin/`: native Streamling EVM source and SQLite sink
- `web/`: Studio SQL page copied into generated projects
- `evidence/`: optional FWA dashboard example
- `tests/`: deterministic EVM RPC and ABI fixtures

## License and provenance

Project code is licensed under Apache License 2.0; see `LICENSE`. Third-party components retain their own licenses; see `THIRD_PARTY_NOTICES`. The repository was implemented as an independent Streamling Blockchain Studio project.
