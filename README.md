# Streamling Blockchain Studio

Streamling Blockchain Studio turns EVM contract events into a local SQLite database. It provides a Rust CLI for project setup and operation, a native Streamling source/sink plugin, read-only SQL over CLI/HTTP/MCP, live backfill status, dynamic contract discovery, and optional Goldsky Edge RPC provisioning.

This is an independent Goldsky project built on the general-purpose [Streamling](https://www.streamling.dev/) runtime.

## Prerequisites

- Rust toolchain compatible with edition 2024
- A separately installed `streamling` executable compatible with `streamling-plugin` 0.2.x
- An EVM RPC URL, or the Goldsky CLI 13.9.0 or newer for automatic Edge endpoint creation
- A contract address and ABI; the CLI can fetch verified ABI metadata when the RPC supports it

Demo dashboards are optional Evidence projects under `demos/`. They read Streamling-generated data through Evidence connectors without committing generated databases, extracts, or credentials.

## Build

```sh
cargo build --release --workspace
```

The CLI is `target/release/streamling-blockchain`. The native plugin is built beside it as `libstreamling_blockchain_plugin` with the platform library extension.

## Demos

Demo dashboards live under `demos/`. Each directory is a complete Evidence project for one specific dataset or protocol; none of them is the generic template for the others.

Current demos:

- `demos/megapot/`: shipped Megapot v2 dashboard for Base activity, backed by the ClickHouse table used for the public demo.
- `demos/robinhood-stock-tokens/`: Robinhood Stock Tokens dashboard source for a Streamling-managed SQLite project. Full historical indexing needs an archive-capable Robinhood Chain RPC.
- `demos/pons-family/`: Pons Family V2 launch and bonding-curve analytics on Robinhood Chain, backed by a Streamling-managed SQLite project that discovers each curve from the factory.

The demo source is committed. Generated data, local Evidence extracts, builds, and credential-bearing connector files are not. To recreate a demo, follow that demo's README and keep the generated Streamling project outside the committed demo directory.

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

`--sink sqlite` is the default local mode and is enough for the included Evidence pages. `--sink clickhouse` writes only through Streamling's built-in ClickHouse sink; `--sink both` keeps the local SQLite database and adds a ClickHouse sidecar sink. The Evidence app supports both SQLite and ClickHouse connectors: use SQLite sources for local project files, and add ClickHouse sources when the project is intentionally writing to a warehouse. Configure the ClickHouse sink with Streamling's environment variables, including `STREAMLING__CLICKHOUSE_SINK__URL`, `STREAMLING__CLICKHOUSE_SINK__USER`, `STREAMLING__CLICKHOUSE_SINK__PASSWORD`, and `STREAMLING__CLICKHOUSE_SINK__DATABASE`. The CLI writes readable schema files under `clickhouse/`: `<table>.sql` for decoded events, and `<table>_blocks.sql` / `<table>_transactions.sql` when block or transaction indexing is enabled.

Before starting Streamling, `dev` creates the configured database and these tables when they do not exist; `streamling-blockchain --project <dir> clickhouse apply` does the same on its own. This matters because Streamling's sink otherwise creates missing tables ordered by their primary key alone. The tables use `ReplacingMergeTree(insert_time, is_deleted)`, which makes Streamling's at-least-once redelivery idempotent. They are partitioned by month and sorted by chain, contract alias, event name, and block, ending with the row ID, so filtered and `FINAL` reads touch fewer parts. Read current rows with `FROM <table> FINAL WHERE is_deleted = 0`. `CREATE TABLE IF NOT EXISTS` never changes an existing table, so `dev` warns when an existing table has a different sorting key; recreate it to adopt the new key. `streamling-blockchain clickhouse schema --database <db> --table <table> [--index-blocks] [--index-transactions]` prints the same DDL without a project.

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
streamling-blockchain --project ./my-project status
streamling-blockchain --project ./my-project status --wait
streamling-blockchain --project ./my-project audit --once --window-blocks 1000
streamling-blockchain --project ./my-project audit --window-blocks 1000 --interval-seconds 300
streamling-blockchain --project ./my-project schema
streamling-blockchain --project ./my-project sql \
  'SELECT event_name, count(*) AS events FROM events GROUP BY event_name LIMIT 20'
streamling-blockchain --project ./my-project sql \
  'SELECT chain_id, block_number, block_timestamp, transaction_count FROM evm_blocks ORDER BY block_number DESC LIMIT 20'
streamling-blockchain --project ./my-project sql \
  "SELECT json_extract(data, '$.value') AS value FROM events ORDER BY int_sortkey(json_extract(data, '$.value')) DESC LIMIT 20"
streamling-blockchain --project ./my-project sql \
  'SELECT chain_id, block_number, transaction_index, from_address, to_address, receipt_status, receipt_gas_used FROM evm_transactions ORDER BY block_number DESC, transaction_index DESC LIMIT 20'
streamling-blockchain --project ./my-project sql \
  --attach base=../base/.streamling-blockchain/events.db \
  --attach arbitrum=../arbitrum/.streamling-blockchain/events.db \
  'SELECT chain_id, count(*) AS events FROM events GROUP BY chain_id
   UNION ALL
   SELECT chain_id, count(*) AS events FROM base.events GROUP BY chain_id
   UNION ALL
   SELECT chain_id, count(*) AS events FROM arbitrum.events GROUP BY chain_id'
streamling-blockchain --project ./my-project replay --from 1000000 --to 1000100
streamling-blockchain --project ./my-project semantics check
streamling-blockchain --project ./my-project rpc-doctor --apply
streamling-blockchain --project ./my-project mcp
```

`sql`, `schema`, `replay`, `audit`, and `mcp` read the SQLite sink by default and the ClickHouse sink when SQLite is disabled. Pass `--backend sqlite` or `--backend clickhouse` to choose when both sinks are enabled. ClickHouse commands use the `STREAMLING__CLICKHOUSE_SINK__*` connection settings. Every `sql` and MCP query runs with ClickHouse's `readonly=1` setting, so the server rejects writes; this also works for users whose profile is already read-only. SQL uses the backend's dialect: `json_extract(data, '$.from')` in SQLite and `JSONExtractString(fields_json, 'from')` in ClickHouse. The per-event `<alias>__<event>` tables exist only in SQLite. On ClickHouse, `audit` stores its summaries in `<table>_quality_replay_checks`, and `--attach` is SQLite-only.

Decoded `uint256`/`int256` event fields are stored as decimal strings, so a plain `ORDER BY` sorts them as text (`"9"` after `"10"`) and `CAST(... AS INTEGER)` silently breaks above `i64`. On SQLite, sort or compare them with the built-in `int_sortkey(value)` scalar function instead, as in the `sql` example above; on ClickHouse, use `toUInt256(JSONExtractString(fields_json, 'value'))` or `toInt256(...)`.

```sh
streamling-blockchain --project ./my-project schema --backend clickhouse
streamling-blockchain --project ./my-project sql --backend clickhouse \
  "SELECT event_name, count() AS events FROM events FINAL WHERE is_deleted = 0 GROUP BY event_name"
streamling-blockchain --project ./my-project audit --backend clickhouse --once --window-blocks 1000
```

To refresh a demo dashboard from a generated SQLite project database:

```sh
STREAMLING_BLOCKCHAIN_DB=./my-project/.streamling-blockchain/events.db \
  npm --prefix demos/robinhood-stock-tokens run sources:sqlite
npm --prefix demos/robinhood-stock-tokens run build
```

`sources:sqlite` symlinks Evidence's local `events.db` source file to the generated project database, then lets Evidence's SQLite connector build its normal Parquet extracts. Evidence reads SQLite directly; no SQLite-to-ClickHouse sync path is involved.

MCP is available over stdio with the tools `streamling_blockchain_schema`, `streamling_blockchain_query`, and `streamling_blockchain_status`. `replay` refetches a closed block range and reports missing, extra, or changed local events without mutating the database. `audit` continuously replays the latest closed window and one older sampled window, then stores summaries in `quality_replay_checks`. `rpc-doctor --apply` writes a lower working `window` when the configured `eth_getLogs` range is too wide for the RPC provider.

## Publish a dashboard

`publish` builds a SQLite-backed demo's Evidence site from a project's database, writes `build/release.json`, checks the build, and ships it:

```sh
streamling-blockchain --project ./my-project publish demos/pons-family --target dir --out ./site
streamling-blockchain --project ./my-project publish demos/pons-family --target herenow --name "Pons Family"
streamling-blockchain --project ./my-project publish demos/pons-family --target herenow --name "Pons Family" --yes
```

The build runs `npm ci` when `node_modules/` is missing, then `npm run sources:sqlite` with `STREAMLING_BLOCKCHAIN_DB` set to the project database, then `npm run build`. `release.json` records the CLI version, build time, chain, contracts, discovery rules, block range, indexed height, event counts per contract and event, and the size and SHA-256 of every other file. It never contains the RPC URL or credentials.

Before anything leaves the machine, `publish` searches every built file for the project's RPC URL (and the key-bearing part of it), the value of `rpc_url_env`, and `STREAMLING__CLICKHOUSE_SINK__PASSWORD`, and refuses on any match. It then reports the total size, file count, and five largest files. here.now allows 2,500 files and 10 GB per site, and 5 GB per file (250 MB for anonymous sites).

`--target dir` copies the site to `--out`. It refuses a non-empty directory unless that directory holds an earlier `release.json`.

`--target herenow` is a dry run until you pass `--yes`: it reports the file count, size, and whether it would create or update a site. Publishing uses `HERENOW_API_KEY` (create one at [here.now](https://here.now)). `--anonymous` publishes without a key; the site expires after 24 hours, and the output includes the expiry time and a claim URL. The CLI saves the site in `<project>/.streamling-blockchain/publish/<demo>.json` (mode `0600`; it may hold the claim token) and updates that site on the next publish, so the URL stays the same. `--new-site` creates a new site instead.

A published site includes every row that the demo's source queries select. Aggregate large sources before publishing. The site is public unless you restrict access in here.now. We don't target Cloudflare Pages because each Evidence build bundles DuckDB WASM files of 34 MB and 39 MB, and Pages rejects files over 25 MiB.

## JSON output

Every command accepts `--json` in any position. stdout then carries exactly one JSON object, and the exit code is non-zero on failure:

```json
{"schema_version":1,"ok":true,"command":"sql","data":{"columns":["n"],"rows":[{"n":1}],"truncated":false},"warnings":[],"error":null}
{"schema_version":1,"ok":false,"command":"sql","data":null,"warnings":[],"error":{"code":"not_found","message":"database does not exist yet: run `streamling-blockchain dev`","retryable":false,"suggested_next":"streamling-blockchain dev"}}
```

`command` is the subcommand path, such as `clickhouse schema`. `data` holds the result: rows for `sql`, the status object for `status`, the report for `rpc-doctor`, the statement list for `clickhouse schema`, and the paths and facts that the `✓` lines state for commands such as `init` and `abi fetch`. `warnings` collects what human mode prints as `warning:` on stderr. `error.code` is one of:

- `validation`: bad arguments, configuration, or SQL, or a write refused by a read-only surface.
- `not_found`: a missing project, database, ABI, contract, or binary.
- `missing_credentials`: an unset credential environment variable, a Goldsky CLI that is not logged in, or rejected ClickHouse credentials.
- `transient_dependency`: an RPC, ClickHouse, ABI explorer, or Goldsky call that failed, timed out, or was rate limited. Only this code sets `retryable: true`.
- `internal`: anything else.

`dev`, `doctor`, `status --wait`, and `audit` without `--once` send Streamling's output and progress lines to stderr in JSON mode and print the envelope when they exit; `dev` returns the final status in `data`. `mcp` owns stdout for the MCP protocol and refuses `--json`.


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

ClickHouse integration tests run when `STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_URL` names a disposable ClickHouse HTTP endpoint, with optional `STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_USER` and `STREAMLING_BLOCKCHAIN_TEST_CLICKHOUSE_PASSWORD`. They create and drop their own `sbs_test_*` databases, and they skip when the URL is unset.

For a behavioral smoke test, initialize against a known RPC fixture, run `dev`, wait for `status --wait`, then query the indexed events through CLI, HTTP, and MCP.

## Repository layout

- `crates/streamling-blockchain-cli/`: CLI, Goldsky provisioning, SQL, and MCP surfaces
- `crates/streamling-blockchain-plugin/`: native Streamling EVM source and SQLite sink
- `demos/`: specific Evidence dashboard demos backed by Streamling-generated project databases
- `tests/`: deterministic EVM RPC and ABI fixtures

## License and provenance

Project code is licensed under Apache License 2.0; see `LICENSE`. Third-party components retain their own licenses; see `THIRD_PARTY_NOTICES`. The repository was implemented as an independent Streamling Blockchain Studio project.
