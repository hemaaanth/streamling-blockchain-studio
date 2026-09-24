# Pons Family Analytics

Evidence dashboard for Pons Family V2 launch and bonding-curve activity on Robinhood Chain. Streamling discovers each curve from the verified V2 factory and indexes its buy, sell, fee, completion, and graduation events into SQLite.

## Data source

`contracts.json` records Robinhood Chain ID `4663`, the V2 factory address and deployment block `26841846`, and the `TokenLaunched` discovery rule that registers each bonding curve. The factory and curve event ABIs live in `abis/`.

`scripts/init_project.py` turns that manifest into a Streamling project with the generic `init` and `add-discovery` commands. It uses the public Robinhood Chain RPC unless `ROBINHOOD_RPC_URL` is set.

## Recreate locally

From the repository root:

```sh
cargo build --release --workspace
python3 demos/pons-family/scripts/init_project.py

./target/release/streamling-blockchain \
  --project .local/demos/pons-family \
  dev --no-build --exit-when-caught-up

cd demos/pons-family
npm ci
STREAMLING_BLOCKCHAIN_DB=../../.local/demos/pons-family/.streamling-blockchain/events.db \
  npm run sources:sqlite
npm run build
npm run preview
```

Without `--end-block`, the initializer fixes the end to the current head minus 12 confirmations, and the dashboard stays at that block until it is rebuilt from a new project. Pass `--start-block` only for an explicitly partial local dataset; do not publish that as a complete Pons history.

The dashboard shows the latest indexed block and does not refresh automatically.

Generated project state lives under `.local/demos/pons-family/` and is not committed.

The dashboard reports native-quote volume only when `pairToken` is the zero address. ERC-20 quote assets remain separate so incompatible units are never summed.
