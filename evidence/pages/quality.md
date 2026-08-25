---
title: Quality
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 6
icon: shield-check
---

Data-quality checks over the local SQLite event table. Replay audit rows come from the same `.streamling-blockchain/events.db` file through Evidence's SQLite source.

```sql quality_summary
WITH base AS (
  SELECT *
  FROM events
  WHERE is_deleted = 0
)
SELECT
  count(*) AS decoded_events,
  count(DISTINCT event_id) AS unique_event_ids,
  count(*) - count(DISTINCT event_id) AS duplicate_event_ids,
  count(DISTINCT contract_alias) AS symbols_seen,
  count(DISTINCT block_number) AS event_blocks,
  count(*) FILTER (WHERE tx_hash IS NULL OR tx_hash = '') AS missing_tx_hashes,
  count(*) FILTER (WHERE fields_json IS NULL OR fields_json = '') AS missing_decoded_fields,
  count(*) FILTER (WHERE json_extract_string(fields_json, '$.from') IS NULL OR json_extract_string(fields_json, '$.to') IS NULL OR json_extract_string(fields_json, '$.value') IS NULL) AS malformed_transfers,
  min(block_number) AS first_block,
  max(block_number) AS latest_block
FROM base
```

```sql block_density
SELECT
  epoch_ms(CAST(floor(block_timestamp / 86400) * 86400000 AS BIGINT)) AS day,
  count(DISTINCT block_number) AS event_blocks,
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols
FROM events
WHERE is_deleted = 0
GROUP BY day
ORDER BY day
```

```sql symbol_gaps
SELECT
  contract_alias AS symbol,
  min(block_number) AS first_block,
  max(block_number) AS latest_block,
  count(DISTINCT block_number) AS event_blocks,
  count(*) AS transfers,
  count(*) - count(DISTINCT event_id) AS duplicate_event_ids,
  count(*) FILTER (WHERE tx_hash IS NULL OR tx_hash = '') AS missing_tx_hashes,
  count(*) FILTER (WHERE fields_json IS NULL OR fields_json = '') AS missing_decoded_fields
FROM events
WHERE is_deleted = 0
GROUP BY contract_alias
ORDER BY transfers DESC
LIMIT 50
```

<Grid cols=4>
  <BigValue data={quality_summary} value="decoded_events" title="Decoded events" fmt="num0" />
  <BigValue data={quality_summary} value="duplicate_event_ids" title="Duplicate IDs" fmt="num0" />
  <BigValue data={quality_summary} value="missing_tx_hashes" title="Missing tx hashes" fmt="num0" />
  <BigValue data={quality_summary} value="malformed_transfers" title="Malformed transfers" fmt="num0" />
</Grid>

<Grid cols=4>
  <BigValue data={quality_summary} value="symbols_seen" title="Symbols seen" fmt="num0" />
  <BigValue data={quality_summary} value="event_blocks" title="Event blocks" fmt="num0" />
  <BigValue data={quality_summary} value="first_block" title="First block" fmt="num0" />
  <BigValue data={quality_summary} value="latest_block" title="Latest block" fmt="num0" />
</Grid>

## Event blocks by day

<AreaChart data={block_density} x="day" y="transfers" title="Transfers by day" />

## Symbol-level checks

<DataTable data={symbol_gaps} rows=50 rowShading=true sortable=true search=true downloadable=true>
  <Column id="symbol" title="Symbol" chip=true />
  <Column id="transfers" title="Transfers" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="event_blocks" title="Event blocks" fmt="num0" />
  <Column id="duplicate_event_ids" title="Duplicate IDs" fmt="num0" />
  <Column id="missing_tx_hashes" title="Missing tx hashes" fmt="num0" />
  <Column id="missing_decoded_fields" title="Missing fields" fmt="num0" />
  <Column id="first_block" title="First" fmt="num0" />
  <Column id="latest_block" title="Latest" fmt="num0" />
</DataTable>
