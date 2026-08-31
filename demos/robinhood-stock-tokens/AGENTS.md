# Robinhood Stock Tokens Demo

This directory is a specific Evidence dashboard for Robinhood Stock Token transfer analytics on Robinhood Chain.

## Data source

The dashboard reads a Streamling-managed SQLite database. Refresh sources with:

```sh
STREAMLING_BLOCKCHAIN_DB=<generated-project>/.streamling-blockchain/events.db npm --prefix demos/robinhood-stock-tokens run sources:sqlite
```

`STREAMLING_BLOCKCHAIN_DB` must point at a database created by `streamling-blockchain dev`. Do not create or edit `sources/sqlite/events.db` by hand.

## Evidence notes

- Pages live in `pages/` and use Evidence Markdown with SQL blocks.
- SQL runs through the configured SQLite connector for this demo.
- `sources/sqlite/events.sql` adapts Streamling's `events` table to the shape used by these pages.
- `sources/sqlite/quality_replay_checks.sql` reads Streamling audit output. Run `streamling-blockchain --project <generated-project> audit --once --window-blocks 1000` before refreshing Evidence sources.
- `sources/sqlite/*.db`, `.evidence/`, `build/`, and `node_modules/` are generated and ignored.
