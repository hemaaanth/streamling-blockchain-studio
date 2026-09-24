# Streamling Blockchain Studio

This repo is the Streamling Blockchain Studio toolkit plus specific demo dashboards.

## Data flow

Use Streamling for blockchain demo data. The intended path is:

1. `cargo build --release --workspace` builds the local `streamling-blockchain` CLI and native plugin.
2. A generated project under `.local/` or `/tmp/` is initialized with `streamling-blockchain --project <dir> init ...` or `init-robinhood ...`.
3. `streamling-blockchain --project <dir> dev` runs the separate `streamling` runtime, persists checkpoints, and writes the configured SQLite and/or ClickHouse sinks.
4. SQLite demos point Evidence at `<dir>/.streamling-blockchain/events.db`; ClickHouse demos point Evidence at the same ClickHouse database receiving the Streamling sink.

Do not hand-create blockchain demo data. Manual database fixtures belong only in tests. Local infrastructure such as the Megapot ClickHouse Compose stack may create empty schemas, but Streamling or the documented importer must populate them. Do not commit generated Streamling projects, databases, WAL files, `state.db`, progress files, copied ABIs, generated skills, or production credentials.

## Demos

`demos/megapot/` is the shipped Megapot v2 public dashboard backed by ClickHouse. `demos/robinhood-stock-tokens/` is a Robinhood Stock Tokens dashboard source backed by a Streamling-managed SQLite project. `demos/pons-family/` is a Pons Family V2 dashboard backed by a Streamling-managed SQLite project with factory discovery. Add new demos as separate directories under `demos/`; do not overwrite another demo in place.

Before changing a demo, read its local `AGENTS.md` and README if present.
