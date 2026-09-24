---
title: Launches
page_width: full
cards: true
table_of_contents: false
sidebar_position: 2
icon: rocket
---

# Launches

Every token emitted by the verified Pons V2 factory, with its deployer, bonding curve, quote asset, and graduation state.

```sql launch_summary
WITH launches AS (
  SELECT
    lower(json_extract_string(fields_json, '$.token')) AS token,
    lower(json_extract_string(fields_json, '$.pairToken')) AS pair_token
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
),
graduated AS (
  SELECT DISTINCT lower(json_extract_string(fields_json, '$.token')) AS token
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'PoolGraduated'
)
SELECT
  count(*) AS launches,
  sum(CASE WHEN pair_token = '0x0000000000000000000000000000000000000000' THEN 1 ELSE 0 END) AS native_launches,
  count(DISTINCT pair_token) AS quote_assets,
  sum(CASE WHEN g.token IS NOT NULL THEN 1 ELSE 0 END) AS graduated
FROM launches l
LEFT JOIN graduated g ON l.token = g.token
```

```sql launches_by_hour
WITH bounds AS (
  SELECT max(block_timestamp) - min(block_timestamp) AS span
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
),
launches AS (
  SELECT e.*, CASE WHEN b.span > 1209600 THEN 86400 WHEN b.span > 43200 THEN 3600 ELSE 60 END AS bucket_seconds
  FROM events e CROSS JOIN bounds b
  WHERE e.is_deleted = 0 AND e.contract_alias = 'pons_factory' AND e.event_name = 'TokenLaunched'
)
SELECT
  epoch_ms(CAST(floor(block_timestamp / bucket_seconds) * bucket_seconds * 1000 AS BIGINT)) AS period,
  count(*) AS launches
FROM launches
GROUP BY period
ORDER BY period
```

```sql quote_assets
SELECT
  CASE
    WHEN lower(json_extract_string(fields_json, '$.pairToken')) = '0x0000000000000000000000000000000000000000' THEN 'Native ETH'
    ELSE substr(json_extract_string(fields_json, '$.pairToken'), 1, 8) || '…' || substr(json_extract_string(fields_json, '$.pairToken'), -6)
  END AS quote_asset,
  count(*) AS launches
FROM events
WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
GROUP BY quote_asset
ORDER BY launches DESC
LIMIT 12
```

```sql recent_launches
WITH graduated AS (
  SELECT DISTINCT lower(json_extract_string(fields_json, '$.token')) AS token
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'PoolGraduated'
)
SELECT
  epoch_ms(CAST(e.block_timestamp * 1000 AS BIGINT)) AS launch_time,
  substr(json_extract_string(e.fields_json, '$.token'), 1, 8) || '…' || substr(json_extract_string(e.fields_json, '$.token'), -6) AS token_label,
  'https://robinhoodchain.blockscout.com/address/' || json_extract_string(e.fields_json, '$.token') AS token_url,
  substr(json_extract_string(e.fields_json, '$.curve'), 1, 8) || '…' || substr(json_extract_string(e.fields_json, '$.curve'), -6) AS curve_label,
  'https://robinhoodchain.blockscout.com/address/' || json_extract_string(e.fields_json, '$.curve') AS curve_url,
  substr(json_extract_string(e.fields_json, '$.deployer'), 1, 8) || '…' || substr(json_extract_string(e.fields_json, '$.deployer'), -6) AS deployer_label,
  'https://robinhoodchain.blockscout.com/address/' || json_extract_string(e.fields_json, '$.deployer') AS deployer_url,
  CASE
    WHEN lower(json_extract_string(e.fields_json, '$.pairToken')) = '0x0000000000000000000000000000000000000000' THEN 'Native ETH'
    ELSE substr(json_extract_string(e.fields_json, '$.pairToken'), 1, 8) || '…' || substr(json_extract_string(e.fields_json, '$.pairToken'), -6)
  END AS quote_asset,
  CASE WHEN g.token IS NULL THEN 'Bonding' ELSE 'Graduated' END AS status,
  e.block_number,
  'https://robinhoodchain.blockscout.com/tx/' || e.tx_hash AS tx_url
FROM events e
LEFT JOIN graduated g ON lower(json_extract_string(e.fields_json, '$.token')) = g.token
WHERE e.is_deleted = 0 AND e.contract_alias = 'pons_factory' AND e.event_name = 'TokenLaunched'
ORDER BY e.block_number DESC, e.log_index DESC
LIMIT 100
```

<Grid cols=4>
  <BigValue data={launch_summary} value="launches" title="Launches" fmt="num0" />
  <BigValue data={launch_summary} value="native_launches" title="Native-quoted" fmt="num0" />
  <BigValue data={launch_summary} value="quote_assets" title="Quote assets" fmt="num0" />
  <BigValue data={launch_summary} value="graduated" title="Graduated" fmt="num0" />
</Grid>

<Grid cols=2>
  <AreaChart data={launches_by_hour} x="period" y="launches" title="Launch cadence" emptySet=pass />
  <BarChart data={quote_assets} x="quote_asset" y="launches" title="Launches by quote asset" emptySet=pass />
</Grid>

<DataTable data={recent_launches} rows=25 rowShading=true sortable=true search=true downloadable=true emptySet=pass>
  <Column id="launch_time" title="Launched" fmt="longdate" />
  <Column id="token_url" title="Token" contentType="link" linkLabel="token_label" openInNewTab=true />
  <Column id="curve_url" title="Curve" contentType="link" linkLabel="curve_label" openInNewTab=true />
  <Column id="deployer_url" title="Deployer" contentType="link" linkLabel="deployer_label" openInNewTab=true />
  <Column id="quote_asset" title="Quote" chip=true />
  <Column id="status" title="State" chip=true />
  <Column id="block_number" title="Block" fmt="num0" />
  <Column id="tx_url" title="Tx" contentType="link" linkLabel="open" openInNewTab=true />
</DataTable>