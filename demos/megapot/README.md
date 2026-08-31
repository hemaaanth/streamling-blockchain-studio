# Megapot demo

Evidence dashboard for Megapot v2 activity on Base.

This is the shipped public demo currently served at https://megapot.goldsky.sh/. It is a specific dashboard, not a generic Evidence template.

## Data source

The demo reads ClickHouse through `evidence-connector-clickhouse`:

- source SQL: `sources/clickhouse/events.sql`
- table: `fwa_analytics.events FINAL`
- data shape: decoded Streamling event rows with `fields_json`, `contract_alias`, `event_name`, block metadata, transaction metadata, and soft-delete fields

The committed repo does not include ClickHouse credentials or generated Evidence extracts.

## Run locally

From this directory, provide `sources/clickhouse/connection.yaml` with the ClickHouse connection available to Evidence, then run:

```sh
npm run sources
npm run build
npm run preview
```

`sources/clickhouse/connection.yaml`, `.evidence/`, `build/`, and `node_modules/` are generated/local and ignored.
