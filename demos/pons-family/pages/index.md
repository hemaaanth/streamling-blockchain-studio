---
title: Pons Family Analytics
hide_title: true
page_width: full
cards: true
table_of_contents: false
sidebar_position: 1
icon: layout-dashboard
---

<h1 class="title">Pons Family Analytics</h1>

Pons V2 launch and bonding-curve trading snapshot on Robinhood Chain, indexed from verified contract events by Streamling through the latest block shown below.

```sql overview
WITH launches AS (
  SELECT
    lower(json_extract_string(fields_json, '$.curve')) AS curve,
    lower(json_extract_string(fields_json, '$.pairToken')) AS pair_token
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
),
trades AS (
  SELECT
    e.event_name,
    CASE WHEN e.event_name = 'CurveBuy'
      THEN json_extract_string(e.fields_json, '$.buyer')
      ELSE json_extract_string(e.fields_json, '$.seller')
    END AS trader,
    CASE WHEN e.event_name = 'CurveBuy'
      THEN coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteIn') AS DOUBLE), 0)
      ELSE coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteOut') AS DOUBLE), 0)
    END AS quote_amount,
    l.pair_token
  FROM events e
  JOIN launches l ON lower(e.address) = l.curve
  WHERE e.is_deleted = 0 AND e.event_name IN ('CurveBuy', 'CurveSell')
)
SELECT
  (SELECT count(*) FROM launches) AS launches,
  count(*) AS trades,
  count(DISTINCT lower(trader)) AS traders,
  round(sum(CASE WHEN pair_token = '0x0000000000000000000000000000000000000000' THEN quote_amount ELSE 0 END) / 1e18, 2) AS native_volume,
  (SELECT max(block_number) FROM events WHERE is_deleted = 0) AS latest_block
FROM trades
```

```sql hourly_activity
WITH bounds AS (
  SELECT max(block_timestamp) - min(block_timestamp) AS span
  FROM events
  WHERE is_deleted = 0 AND event_name IN ('CurveBuy', 'CurveSell')
),
activity AS (
  SELECT e.*, CASE WHEN b.span > 1209600 THEN 86400 WHEN b.span > 43200 THEN 3600 ELSE 60 END AS bucket_seconds
  FROM events e CROSS JOIN bounds b
  WHERE e.is_deleted = 0 AND e.event_name IN ('CurveBuy', 'CurveSell')
)
SELECT
  epoch_ms(CAST(floor(block_timestamp / bucket_seconds) * bucket_seconds * 1000 AS BIGINT)) AS period,
  count(*) AS trades,
  sum(CASE WHEN event_name = 'CurveBuy' THEN 1 ELSE 0 END) AS buys,
  sum(CASE WHEN event_name = 'CurveSell' THEN 1 ELSE 0 END) AS sells
FROM activity
GROUP BY period
ORDER BY period
```

```sql top_launches
WITH launches AS (
  SELECT
    lower(json_extract_string(fields_json, '$.curve')) AS curve,
    json_extract_string(fields_json, '$.token') AS token,
    lower(json_extract_string(fields_json, '$.pairToken')) AS pair_token,
    block_timestamp AS launched_at
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
),
trade_stats AS (
  SELECT
    lower(e.address) AS curve,
    count(*) AS trades,
    count(DISTINCT CASE WHEN e.event_name = 'CurveBuy' THEN lower(json_extract_string(e.fields_json, '$.buyer')) ELSE lower(json_extract_string(e.fields_json, '$.seller')) END) AS traders,
    sum(CASE
      WHEN l.pair_token = '0x0000000000000000000000000000000000000000' AND e.event_name = 'CurveBuy'
        THEN coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteIn') AS DOUBLE), 0)
      WHEN l.pair_token = '0x0000000000000000000000000000000000000000'
        THEN coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteOut') AS DOUBLE), 0)
      ELSE 0
    END) / 1e18 AS native_volume
  FROM events e
  JOIN launches l ON lower(e.address) = l.curve
  WHERE e.is_deleted = 0 AND e.event_name IN ('CurveBuy', 'CurveSell')
  GROUP BY lower(e.address)
)
SELECT
  substr(l.token, 1, 8) || '…' || substr(l.token, -6) AS token_label,
  substr(l.curve, 1, 8) || '…' || substr(l.curve, -6) AS curve_label,
  'https://robinhoodchain.blockscout.com/address/' || l.token AS token_url,
  'https://robinhoodchain.blockscout.com/address/' || l.curve AS curve_url,
  coalesce(s.trades, 0) AS trades,
  coalesce(s.traders, 0) AS traders,
  round(coalesce(s.native_volume, 0), 3) AS native_volume
FROM launches l
LEFT JOIN trade_stats s ON l.curve = s.curve
ORDER BY trades DESC
LIMIT 15
```

<Grid cols=5>
  <BigValue data={overview} value="launches" title="Launches" fmt="num0" />
  <BigValue data={overview} value="trades" title="Curve trades" fmt="num0" />
  <BigValue data={overview} value="traders" title="Unique traders" fmt="num0" />
  <BigValue data={overview} value="native_volume" title="Native quote volume" fmt="num2" />
  <BigValue data={overview} value="latest_block" title="Latest block" fmt="num0" />
</Grid>

<Grid cols=2>
  <AreaChart data={hourly_activity} x="period" y="trades" title="Trading velocity" emptySet=pass />
  <BarChart data={top_launches} x="token_label" y="trades" title="Most active launches" emptySet=pass />
</Grid>

<DataTable data={top_launches} rows=15 rowShading=true sortable=true emptySet=pass>
  <Column id="token_url" title="Token" contentType="link" linkLabel="token_label" openInNewTab=true />
  <Column id="curve_url" title="Curve" contentType="link" linkLabel="curve_label" openInNewTab=true />
  <Column id="trades" title="Trades" contentType="bar" fmt="num0" barColor="#7c3aed" />
  <Column id="traders" title="Traders" fmt="num0" />
  <Column id="native_volume" title="Native volume" fmt="num3" />
</DataTable>

## Explore

- [Launches](/launches/) — new tokens, quote assets, and graduation activity.
- [Trading](/trading/) — buy/sell flow, volume, and recent executions.
- [Participants](/participants/) — the most active Pons wallets.
- [Data room](/data-room/) — decoded factory and curve events for verification.