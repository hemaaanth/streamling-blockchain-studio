# Megapot demo

Evidence dashboard for Megapot v2 activity on Base. The shipped public instance is available at https://megapot.goldsky.sh/.

## Reproducible data source

`contracts.json` is the non-secret source manifest. It records Base chain ID `8453`, the two production contract addresses and aliases, the first deployment block, confirmations, RPC window, and verified ABI source. `scripts/init_project.py` validates the RPC chain, fetches both ABIs from Base Blockscout, and initializes a bounded Streamling project for SQLite, ClickHouse, or both.

The full range starts at block `43197068`. Without `--end-block`, the initializer fixes the end to the current head minus 12 confirmations. Pass the same explicit end block when comparing backends.

Published builds remain fixed at their configured end block until the demo is rebuilt.

Prerequisites from the repository root:

```sh
cargo build --release --workspace
streamling --version
cd demos/megapot
npm ci
```

Install Streamling from its [official installation documentation](https://www.streamling.dev/docs) rather than piping an unpinned network response into a shell.

Set a Base mainnet RPC without putting it in shell history or a committed file:

```sh
export BASE_RPC_URL='<Base RPC URL>'
```

The initializer fails before generating a project if the endpoint is not Base chain ID `8453`.

## Run with SQLite

```sh
python3 scripts/init_project.py /tmp/megapot-sqlite --sink sqlite

BASE_RPC_URL="$BASE_RPC_URL" \
  ../../target/release/streamling-blockchain \
  --project /tmp/megapot-sqlite \
  dev --no-build --exit-when-caught-up

STREAMLING_BLOCKCHAIN_DB=/tmp/megapot-sqlite/.streamling-blockchain/events.db \
  npm run sources:sqlite
npm run build
npm run preview
```

`sources:sqlite` points Evidence directly at Streamling's generated SQLite database. It does not copy or fabricate blockchain rows.

## Run with ClickHouse

The local Compose stack exposes ClickHouse only on loopback and creates `megapot_analytics.events`:

```sh
npm run clickhouse:up

set -a
. ./.env.example
set +a

python3 scripts/init_project.py /tmp/megapot-clickhouse --sink clickhouse

BASE_RPC_URL="$BASE_RPC_URL" \
  ../../target/release/streamling-blockchain \
  --project /tmp/megapot-clickhouse \
  dev --no-build --exit-when-caught-up

npm run sources:clickhouse
npm run build
npm run preview
```

Use `npm run clickhouse:down` to stop the server without deleting data. Use `npm run clickhouse:reset` to remove the local volume, including schemas left by an older demo configuration.

For an existing Streamling SQLite project, the one-time importer remains available:

```sh
python3 sync_clickhouse.py /tmp/megapot-sqlite/.streamling-blockchain/events.db
```

## Evidence backends

`scripts/configure_source.py` materializes one ignored `sources/megapot/` directory from the selected backend under `backends/`. Both sources expose the same `events` columns to the pages:

- SQLite maps Streamling's `data` JSON column to `fields_json`.
- ClickHouse reads `megapot_analytics.events FINAL`.

For remote ClickHouse, override the generated `megapot` source with `EVIDENCE_SOURCE__megapot__url`, `EVIDENCE_SOURCE__megapot__username`, and `EVIDENCE_SOURCE__megapot__password`. Never commit production credentials.

Generated and ignored: `sources/megapot/`, `.env`, `.evidence/`, `build/`, `node_modules/`, Streamling project directories, databases, copied ABIs, state, and progress files.
