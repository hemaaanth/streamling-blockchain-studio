---
title: Data room
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 5
icon: database
---

Decoded transfer-level view for traceability and follow-up modeling. Every row is one indexed ERC-20 `Transfer` log from a Robinhood Stock Token contract.

```sql event_mix
SELECT
  contract_alias,
  event_name,
  count(*) AS events,
  min(block_number) AS first_block,
  max(block_number) AS latest_block,
  count(*) * 1.0 / sum(count(*)) OVER () AS event_share
FROM events
WHERE is_deleted = 0
GROUP BY contract_alias, event_name
ORDER BY events DESC
```

```sql block_coverage
SELECT
  min(block_number) AS first_block,
  max(block_number) AS latest_block,
  count(DISTINCT block_number) AS blocks_with_events,
  count(*) AS decoded_events,
  min(to_timestamp(block_timestamp)) AS first_seen_at,
  max(to_timestamp(block_timestamp)) AS latest_seen_at
FROM events
WHERE is_deleted = 0
```

```sql quality_checks
WITH base AS (
  SELECT *
  FROM events
  WHERE is_deleted = 0
)
SELECT
  count(*) AS decoded_events,
  count(DISTINCT event_id) AS unique_event_ids,
  count(*) - count(DISTINCT event_id) AS duplicate_event_ids,
  count(DISTINCT contract_alias) AS contracts_indexed,
  count(DISTINCT event_name) AS event_types,
  count(*) FILTER (WHERE tx_hash IS NULL OR tx_hash = '') AS missing_tx_hashes,
  count(*) FILTER (WHERE fields_json IS NULL OR fields_json = '') AS missing_decoded_fields
FROM base
```

```sql raw_transfers
SELECT
  contract_alias AS symbol,
  block_number,
  log_index,
  to_timestamp(block_timestamp) AS event_time,
  concat(substr(json_extract_string(fields_json, '$.from'), 1, 6), '…', substr(json_extract_string(fields_json, '$.from'), -4)) AS sender,
  concat('https://robinhoodchain.blockscout.com/address/', json_extract_string(fields_json, '$.from')) AS sender_url,
  concat(substr(json_extract_string(fields_json, '$.to'), 1, 6), '…', substr(json_extract_string(fields_json, '$.to'), -4)) AS recipient,
  concat('https://robinhoodchain.blockscout.com/address/', json_extract_string(fields_json, '$.to')) AS recipient_url,
  round(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0) / 1e18, 6) AS units,
  concat('https://robinhoodchain.blockscout.com/tx/', tx_hash) AS tx_url,
  concat(substr(tx_hash, 1, 10), '…', substr(tx_hash, -6)) AS tx
FROM events
WHERE is_deleted = 0
ORDER BY block_number DESC, log_index DESC
LIMIT 200
```

<Grid cols=4>
  <BigValue data={block_coverage} value="decoded_events" title="Transfers" fmt="num0" />
  <BigValue data={block_coverage} value="blocks_with_events" title="Event blocks" fmt="num0" />
  <BigValue data={block_coverage} value="first_block" title="First block" fmt="num0" />
  <BigValue data={block_coverage} value="latest_block" title="Latest block" fmt="num0" />
</Grid>

## Event mix

<Grid cols=4>
  <BigValue data={quality_checks} value="duplicate_event_ids" title="Duplicate IDs" fmt="num0" />
  <BigValue data={quality_checks} value="missing_tx_hashes" title="Missing tx hashes" fmt="num0" />
  <BigValue data={quality_checks} value="missing_decoded_fields" title="Missing fields" fmt="num0" />
  <BigValue data={quality_checks} value="contracts_indexed" title="Contracts indexed" fmt="num0" />
</Grid>

<DataTable data={event_mix} rows=50 rowShading=true sortable=true search=true downloadable=true>
  <Column id="contract_alias" title="Symbol" chip=true />
  <Column id="event_name" title="Event" chip=true />
  <Column id="events" title="Events" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="event_share" title="Share" contentType="bar" fmt="pct1" barColor="#f59e0b" />
  <Column id="first_block" title="First" fmt="num0" />
  <Column id="latest_block" title="Latest" fmt="num0" />
</DataTable>

## Latest transfers

<DataTable data={raw_transfers} rows=100 rowShading=true sortable=true search=true downloadable=true compact=true>
  <Column id="symbol" title="Symbol" chip=true />
  <Column id="block_number" title="Block" fmt="num0" />
  <Column id="log_index" title="Log" fmt="num0" />
  <Column id="event_time" title="Time" fmt="date" />
  <Column id="sender_url" title="Sender" contentType="link" linkLabel="sender" openInNewTab=true />
  <Column id="recipient_url" title="Recipient" contentType="link" linkLabel="recipient" openInNewTab=true />
  <Column id="units" title="Units" contentType="bar" fmt="num2" barColor="#16a34a" />
  <Column id="tx_url" title="Tx" contentType="link" linkLabel="tx" openInNewTab=true />
</DataTable>
