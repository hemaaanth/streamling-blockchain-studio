# Robinhood Stock Tokens demo

Evidence dashboard for Robinhood Stock Token transfer activity indexed from Robinhood Chain by Streamling Blockchain Studio.

## Recreate locally

From the repository root:

```sh
curl -fsSL https://streamling.dev/install.sh | bash
cargo build --release --workspace
export ROBINHOOD_RPC_URL='https://your-archive-rpc.example'

target/release/streamling-blockchain \
  --project .local/demos/robinhood-stock-tokens \
  init-robinhood \
  --rpc-env ROBINHOOD_RPC_URL \
  --start-block 0 \
  --index-blocks

target/release/streamling-blockchain \
  --project .local/demos/robinhood-stock-tokens \
  dev \
  --streamling streamling
```

Use an archive-capable RPC. Public Robinhood RPC endpoints are fine for setup checks but are not reliable for full historical `eth_getLogs` backfills.

In another shell, inspect progress:

```sh
target/release/streamling-blockchain \
  --project .local/demos/robinhood-stock-tokens \
  status --json
```

Create the audit source table used by the Quality page:

```sh
target/release/streamling-blockchain \
  --project .local/demos/robinhood-stock-tokens \
  audit --once --window-blocks 1000
```

Refresh the Evidence extracts and build the static site:

```sh
STREAMLING_BLOCKCHAIN_DB=.local/demos/robinhood-stock-tokens/.streamling-blockchain/events.db \
  npm --prefix demos/robinhood-stock-tokens run sources:sqlite

npm --prefix demos/robinhood-stock-tokens run build
npm --prefix demos/robinhood-stock-tokens run preview
```

## Committed vs generated

Committed:

- `pages/`
- `sources/sqlite/*.sql`
- Evidence config, theme, package files, and access policy

Generated and ignored:

- `.local/demos/robinhood-stock-tokens/`
- `sources/sqlite/events.db`
- `.evidence/`
- `build/`
- `node_modules/`
