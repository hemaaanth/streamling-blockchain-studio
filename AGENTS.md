# Streamling Blockchain Studio

This repo is the Streamling Blockchain Studio toolkit plus specific demo dashboards.

## Data flow

Use Streamling for blockchain demo data. The intended path is:

1. `cargo build --release --workspace` builds the local `streamling-blockchain` CLI and native plugin.
2. A generated project under `.local/` or `/tmp/` is initialized with `streamling-blockchain --project <dir> init ...` or `init-robinhood ...`.
3. `streamling-blockchain --project <dir> dev` runs the separate `streamling` runtime and writes checkpoints plus `.streamling-blockchain/events.db`.
4. A demo under `demos/<name>/` reads that generated database via `STREAMLING_BLOCKCHAIN_DB=<dir>/.streamling-blockchain/events.db npm --prefix demos/<name> run sources:sqlite`.

Do not hand-create SQLite databases for demos. Manual SQLite fixtures belong only in tests. Do not commit generated Streamling projects, databases, WAL files, `state.db`, progress files, copied ABIs, generated skills, or credential-bearing configs.

## Demos

`demos/robinhood-stock-tokens/` is a specific Robinhood Stock Tokens Evidence dashboard, not a generic Evidence template. Add new demos as separate directories under `demos/`; do not overwrite another demo in place.

Before changing a demo, read its local `AGENTS.md` and README if present.
