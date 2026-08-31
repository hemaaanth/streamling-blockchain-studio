# Streamling Blockchain Studio

Streamling Blockchain Studio turns EVM contract events into a local SQLite database. It provides a Rust CLI for project setup and operation, a native Streamling source/sink plugin, read-only SQL over CLI/HTTP/MCP, live backfill status, dynamic contract discovery, and optional Goldsky Edge RPC provisioning.

This is an independent Goldsky project built on the general-purpose [Streamling](https://www.streamling.dev/) runtime.

## Prerequisites

- Rust toolchain compatible with edition 2024
- A separately installed `streamling` executable compatible with `streamling-plugin` 0.2.x
- An EVM RPC URL, or the Goldsky CLI 13.9.0 or newer for automatic Edge endpoint creation
- A contract address and ABI; the CLI can fetch verified ABI metadata when the RPC supports it

Evidence is optional. The example under `evidence/` reads the local SQLite event database through Evidence's official `@evidence-dev/sqlite` connector. The Evidence project also keeps `evidence-connector-clickhouse` installed and registered so warehouse-backed pages can be added without changing project plumbing.

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
  --start-block 12345678 \
  --index-blocks

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

For Robinhood Stock Tokens, initialize from Robinhood's official asset registry. This creates one contract entry per active Robinhood Chain deployment and leaves `end_block` unset so the backfill runs from `--start-block` through the live safe head:

```sh
streamling-blockchain --project ./robinhood-stock-tokens init-robinhood \
  --start-block 0
```

Use `--rpc-env ROBINHOOD_RPC_URL` with an archive/provider endpoint for full historical indexing. Robinhood's public RPC is useful for setup checks but rate-limited for full-chain backfills.


For an analytics warehouse, generate a ClickHouse sink in the same Streamling pipeline:

```sh
streamling-blockchain --project ./my-project init \
  0x1111111111111111111111111111111111111111 \
  --alias protocol \
  --rpc https://example-rpc.invalid \
  --abi ./protocol.json \
  --start-block 12345678 \
  --index-transactions \
  --end-block 12346678 \
  --sink both \
  --clickhouse-table events
```

`--sink sqlite` is the default local mode and is enough for the included Evidence pages. `--sink clickhouse` writes only through Streamling's built-in ClickHouse sink; `--sink both` keeps the local SQLite database and adds a ClickHouse sidecar sink. The Evidence app supports both SQLite and ClickHouse connectors: use SQLite sources for local project files, and add ClickHouse sources when the project is intentionally writing to a warehouse. Configure the ClickHouse sink with Streamling's environment variables, including `STREAMLING__CLICKHOUSE_SINK__URL`, `STREAMLING__CLICKHOUSE_SINK__USER`, `STREAMLING__CLICKHOUSE_SINK__PASSWORD`, and `STREAMLING__CLICKHOUSE_SINK__DATABASE`; create that database before starting `dev`. The CLI writes readable schema files under `clickhouse/`: `<table>.sql` for decoded events, and `<table>_blocks.sql` / `<table>_transactions.sql` when block or transaction indexing is enabled.

Use `--end-block` for bounded demos and reproducible backfills. `streamling-blockchain dev --exit-when-caught-up` turns a bounded project into a batch job: once the indexed height reaches the safe `end_block`, the wrapper stops Streamling and exits. Use `--index-blocks` when dashboards need chain context beyond decoded logs. Use `--index-transactions` when dashboards need transaction inputs, sender/recipient/value, and receipt fields such as status, gas used, effective gas price, contract creation address, and log count. These flags add `streamling_blockchain.evm_blocks` and `streamling_blockchain.evm_transactions` sources and write rows with `chain_id` to the same SQLite database through the existing `streamling_blockchain.sqlite_sink`.

For multi-chain analytics, keep one generated project per chain and union the resulting SQLite or ClickHouse tables by `chain_id`. The project directory remains the checkpoint and RPC boundary, while the row model is chain-scoped so downstream dashboards can federate safely without mixing provider credentials, cursors, or safe-head progress across chains. The `sql --attach ALIAS=DB` option attaches other project databases read-only for ad hoc cross-chain queries.

Fetch a verified ABI before initializing or adding a contract. Sourcify is the default; Etherscan v2 works across Etherscan-supported chains with `--chain-id`; old Etherscan-compatible explorers and Blockscout remain available when needed:

```sh
streamling-blockchain abi fetch \
  0x1111111111111111111111111111111111111111 \
  --chain-id 8453 \
  --out ./protocol.json
streamling-blockchain abi fetch \
  0x1111111111111111111111111111111111111111 \
  --chain-id 8453 \
  --source etherscan \
  --etherscan-api-key-env ETHERSCAN_API_KEY \
  --out ./protocol.json
streamling-blockchain abi fetch \
  0x1111111111111111111111111111111111111111 \
  --chain-id 8453 \
  --source etherscan-v1 \
  --etherscan-base-url https://api.basescan.org/api \
  --etherscan-api-key-env BASESCAN_API_KEY \
  --out ./protocol.json
streamling-blockchain abi fetch \
  0x1111111111111111111111111111111111111111 \
  --chain-id 8453 \
  --source blockscout \
  --blockscout-base-url https://base.blockscout.com/api \
  --out ./protocol.json
```

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
streamling-blockchain --project ./my-project audit --once --window-blocks 1000
streamling-blockchain --project ./my-project audit --window-blocks 1000 --interval-seconds 300
streamling-blockchain --project ./my-project schema
streamling-blockchain --project ./my-project sql --json \
  'SELECT event_name, count(*) AS events FROM events GROUP BY event_name LIMIT 20'
streamling-blockchain --project ./my-project sql --json \
  'SELECT chain_id, block_number, block_timestamp, transaction_count FROM evm_blocks ORDER BY block_number DESC LIMIT 20'
streamling-blockchain --project ./my-project sql --json \
  'SELECT chain_id, block_number, transaction_index, from_address, to_address, receipt_status, receipt_gas_used FROM evm_transactions ORDER BY block_number DESC, transaction_index DESC LIMIT 20'
streamling-blockchain --project ./my-project sql --json \
  --attach base=../base/.streamling-blockchain/events.db \
  --attach arbitrum=../arbitrum/.streamling-blockchain/events.db \
  'SELECT chain_id, count(*) AS events FROM events GROUP BY chain_id
   UNION ALL
   SELECT chain_id, count(*) AS events FROM base.events GROUP BY chain_id
   UNION ALL
   SELECT chain_id, count(*) AS events FROM arbitrum.events GROUP BY chain_id'
streamling-blockchain --project ./my-project replay --from 1000000 --to 1000100
streamling-blockchain --project ./my-project semantics check
streamling-blockchain --project ./my-project rpc-doctor --json --apply
streamling-blockchain --project ./my-project mcp
```

To refresh the included Evidence dashboard from a generated SQLite project database:

```sh
STREAMLING_BLOCKCHAIN_DB=./my-project/.streamling-blockchain/events.db \
  npm --prefix evidence run sources:sqlite
npm --prefix evidence run build
```

`sources:sqlite` symlinks Evidence's local `events.db` source file to the generated project database, then lets Evidence's SQLite connector build its normal Parquet extracts. Evidence reads SQLite directly; no SQLite-to-ClickHouse sync path is involved.

MCP is available over stdio with the tools `streamling_blockchain_schema`, `streamling_blockchain_query`, and `streamling_blockchain_status`. `replay` refetches a closed block range and reports missing, extra, or changed local events without mutating the database. `audit` continuously replays the latest closed window and one older sampled window, then stores summaries in `quality_replay_checks`. `rpc-doctor --apply` writes a lower working `window` when the configured `eth_getLogs` range is too wide for the RPC provider.


Each initialized project contains:

- `streamling-blockchain.toml`: source configuration; may contain an RPC credential
- `pipeline.yaml`: generated Streamling pipeline; may also contain the RPC credential
- `semantic.toml`: generated ABI catalog for agent-safe query construction, including event signatures, topic0 values, Solidity field types, and indexed flags
- `backfill-progress.json`: observed, safe, and indexed block heights
- `abis/`: copied contract ABIs
- `.streamling-blockchain/events.db`: decoded events, optional `evm_blocks`, optional `evm_transactions`, and quality views/tables such as `quality_reorgs`, `quality_event_counts_by_block`, and `quality_replay_checks` when the SQLite sink is enabled
- `clickhouse/<table>.sql`, `clickhouse/<table>_blocks.sql`, and `clickhouse/<table>_transactions.sql`: generated ClickHouse schemas for enabled ClickHouse sinks
- `state.db`: Streamling's committed source cursor and discovered-contract registry
- `llms.txt` and `skills/`: generated agent query guidance for the local SQLite database

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

- `crates/streamling-blockchain-cli/`: CLI, Goldsky provisioning, SQL, and MCP surfaces
- `crates/streamling-blockchain-plugin/`: native Streamling EVM source and SQLite sink
- `evidence/`: optional FWA dashboard example
- `tests/`: deterministic EVM RPC and ABI fixtures

## License and provenance

Project code is licensed under Apache License 2.0; see `LICENSE`. Third-party components retain their own licenses; see `THIRD_PARTY_NOTICES`. The repository was implemented as an independent Streamling Blockchain Studio project.
