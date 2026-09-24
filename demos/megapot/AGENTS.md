# Megapot Demo

This directory is the specific Megapot v2 Evidence dashboard for Base activity. It powers the shipped public demo at https://megapot.goldsky.sh/.

## Data source

`contracts.json` is the authoritative non-secret source manifest. `scripts/init_project.py` must remain the executable path from that manifest to a generated Streamling project. It validates Base chain ID `8453`, fetches verified ABIs, starts at the earliest contract deployment block, and supports SQLite, ClickHouse, or both.

Do not replace this demo with another protocol. Add new demos as sibling directories under `demos/`.

SQLite and ClickHouse must expose the same page-facing columns. Backend templates live under `backends/`; `scripts/configure_source.py` materializes the selected source under ignored `sources/megapot/`. ClickHouse uses `megapot_analytics.events`. Streamling remains the canonical data producer.

The local ClickHouse username and password are loopback-only development defaults. Use Evidence source environment overrides for remote deployments. Do not commit `.env`, generated Evidence sources, `.evidence/`, `build/`, `node_modules/`, generated Streamling projects, copied ABIs, data extracts, state, progress, or credentials.

## Evidence notes

- Pages live in `pages/` and use Evidence Markdown with SQL blocks.
- Run `npm run sources:sqlite` with `STREAMLING_BLOCKCHAIN_DB` for SQLite.
- Run `npm run clickhouse:up`, then `npm run sources:clickhouse`, for ClickHouse.
