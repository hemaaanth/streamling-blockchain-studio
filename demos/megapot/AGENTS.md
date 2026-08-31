# Megapot Demo

This directory is the specific Megapot v2 Evidence dashboard for Base activity. It powers the shipped public demo at https://megapot.goldsky.sh/.

## Data source

The dashboard uses Evidence's ClickHouse connector. `sources/clickhouse/events.sql` reads `fwa_analytics.events FINAL` and expects Streamling-style decoded event rows.

Do not replace this demo with another protocol. Add new demos as sibling directories under `demos/`.

Do not commit `connection.yaml`, `.evidence/`, `build/`, `node_modules/`, copied data extracts, or credentials.

## Evidence notes

- Pages live in `pages/` and use Evidence Markdown with SQL blocks.
- SQL runs through ClickHouse for this demo.
- Run `npm run sources` before `npm run build` when credentials are available.
