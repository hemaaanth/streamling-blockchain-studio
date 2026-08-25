---
title: Data room
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 5
icon: database
---

Decoded-event view for traceability, QA, and follow-up modeling. Use this page to inspect contract coverage, block coverage, raw order events, and LP manager events.

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


```sql raw_orders
SELECT
  block_number,
  log_index,
  to_timestamp(block_timestamp) AS event_time,
  concat(substr(json_extract_string(fields_json, '$.buyer'), 1, 6), '…', substr(json_extract_string(fields_json, '$.buyer'), -4)) AS buyer,
  concat(substr(json_extract_string(fields_json, '$.recipient'), 1, 6), '…', substr(json_extract_string(fields_json, '$.recipient'), -4)) AS recipient,
  coalesce(try_cast(json_extract_string(fields_json, '$.currentDrawingId') AS BIGINT), 0) AS drawing,
  coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) AS tickets,
  round(coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6, 2) AS lp_earnings_usdc,
  round(coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6, 2) AS referral_fees_usdc,
  concat('https://basescan.org/tx/', tx_hash) AS tx_url,
  concat(substr(tx_hash, 1, 10), '…', substr(tx_hash, -6)) AS tx
FROM events
WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
ORDER BY block_number DESC, log_index DESC
LIMIT 100
```

```sql raw_lp_events
SELECT
  block_number,
  log_index,
  to_timestamp(block_timestamp) AS event_time,
  event_name,
  concat(substr(json_extract_string(fields_json, '$.lpAddress'), 1, 6), '…', substr(json_extract_string(fields_json, '$.lpAddress'), -4)) AS backer,
  round(coalesce(try_cast(json_extract_string(fields_json, '$.amount') AS DOUBLE), 0) / 1e6, 2) AS amount_usdc,
  concat('https://basescan.org/tx/', tx_hash) AS tx_url,
  concat(substr(tx_hash, 1, 10), '…', substr(tx_hash, -6)) AS tx
FROM events
WHERE is_deleted = 0 AND contract_alias = 'lp_manager'
ORDER BY block_number DESC, log_index DESC
LIMIT 100
```


```sql drawing_distribution
SELECT
  coalesce(try_cast(json_extract_string(fields_json, '$.currentDrawingId') AS BIGINT), try_cast(json_extract_string(fields_json, '$.drawingId') AS BIGINT), 0) AS drawing,
  count(*) AS decoded_events,
  count(*) FILTER (WHERE event_name = 'TicketOrderProcessed') AS orders,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) ELSE 0 END) AS tickets
FROM events
WHERE is_deleted = 0
GROUP BY drawing
HAVING drawing > 0
ORDER BY drawing DESC
LIMIT 50
```

<Grid cols=4>
  <BigValue data={block_coverage} value="decoded_events" title="Decoded" fmt="num0" />
  <BigValue data={block_coverage} value="blocks_with_events" title="Blocks" fmt="num0" />
  <BigValue data={block_coverage} value="first_block" title="First block" fmt="num0" />
  <BigValue data={block_coverage} value="latest_block" title="Latest block" fmt="num0" />
</Grid>


## Event mix

<Grid cols=4>
  <BigValue data={quality_checks} value="duplicate_event_ids" title="Duplicate IDs" fmt="num0" />
  <BigValue data={quality_checks} value="missing_tx_hashes" title="Missing tx hashes" fmt="num0" />
  <BigValue data={quality_checks} value="missing_decoded_fields" title="Missing fields" fmt="num0" />
  <BigValue data={quality_checks} value="event_types" title="Event types" fmt="num0" />
</Grid>

<DataTable data={event_mix} rows=20 rowShading=true sortable=true downloadable=true>
  <Column id="event_name" title="Event" chip=true />
  <Column id="contract_alias" title="Contract" chip=true />
  <Column id="events" title="Events" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="event_share" title="Share" contentType="bar" fmt="pct1" barColor="#f59e0b" />
  <Column id="first_block" title="First" fmt="num0" />
  <Column id="latest_block" title="Latest" fmt="num0" />
</DataTable>

## Drawings

<DataTable data={drawing_distribution} rows=50 rowShading=true sortable=true downloadable=true>
  <Column id="drawing" title="Drawing" fmt="num0" />
  <Column id="decoded_events" title="Events" contentType="bar" fmt="num0" barColor="#64748b" />
  <Column id="orders" title="Orders" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#f59e0b" />
</DataTable>

## Orders

<DataTable data={raw_orders} rows=50 rowShading=true sortable=true search=true downloadable=true compact=true>
  <Column id="block_number" title="Block" fmt="num0" />
  <Column id="event_time" title="Time" fmt="date" />
  <Column id="buyer" title="Buyer" />
  <Column id="recipient" title="Recipient" />
  <Column id="drawing" title="Drawing" fmt="num0" />
  <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="lp_earnings_usdc" title="LP" fmt="usd2" />
  <Column id="referral_fees_usdc" title="Referral" fmt="usd2" />
  <Column id="tx_url" title="Tx" contentType="link" linkLabel="tx" openInNewTab=true />
</DataTable>

## LP events

<DataTable data={raw_lp_events} rows=50 rowShading=true sortable=true search=true downloadable=true compact=true>
  <Column id="block_number" title="Block" fmt="num0" />
  <Column id="event_time" title="Time" fmt="date" />
  <Column id="event_name" title="Event" chip=true />
  <Column id="backer" title="Backer" />
  <Column id="amount_usdc" title="Amount" contentType="bar" fmt="usd2" barColor="#16a34a" />
  <Column id="tx_url" title="Tx" contentType="link" linkLabel="tx" openInNewTab=true />
</DataTable>
