---
title: Trading
page_width: full
cards: true
table_of_contents: false
sidebar_position: 3
icon: chart-candlestick
---

# Bonding-curve trading

Decoded `CurveBuy` and `CurveSell` events. Native volume is shown separately; ERC-20 quote assets are never mixed into that total.

```sql trading_summary
WITH launches AS (
  SELECT lower(json_extract_string(fields_json, '$.curve')) AS curve,
         lower(json_extract_string(fields_json, '$.pairToken')) AS pair_token
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
),
trades AS (
  SELECT
    e.event_name,
    CASE WHEN e.event_name = 'CurveBuy' THEN coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteIn') AS DOUBLE), 0)
         ELSE coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteOut') AS DOUBLE), 0) END AS quote_amount,
    coalesce(try_cast(json_extract_string(e.fields_json, '$.fee') AS DOUBLE), 0) AS fee,
    coalesce(try_cast(json_extract_string(e.fields_json, '$.tax') AS DOUBLE), 0) AS tax,
    l.pair_token
  FROM events e
  JOIN launches l ON lower(e.address) = l.curve
  WHERE e.is_deleted = 0 AND e.event_name IN ('CurveBuy', 'CurveSell')
)
SELECT
  count(*) AS trades,
  sum(CASE WHEN event_name = 'CurveBuy' THEN 1 ELSE 0 END) AS buys,
  sum(CASE WHEN event_name = 'CurveSell' THEN 1 ELSE 0 END) AS sells,
  round(sum(CASE WHEN pair_token = '0x0000000000000000000000000000000000000000' THEN quote_amount ELSE 0 END) / 1e18, 3) AS native_volume,
  round(sum(CASE WHEN pair_token = '0x0000000000000000000000000000000000000000' THEN fee + tax ELSE 0 END) / 1e18, 3) AS native_fees_tax
FROM trades
```

```sql activity_by_side
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
  CASE WHEN event_name = 'CurveBuy' THEN 'Buys' ELSE 'Sells' END AS side,
  count(*) AS trades
FROM activity
GROUP BY period, side
ORDER BY period, side
```

```sql active_curves
WITH launches AS (
  SELECT lower(json_extract_string(fields_json, '$.curve')) AS curve,
         json_extract_string(fields_json, '$.token') AS token,
         lower(json_extract_string(fields_json, '$.pairToken')) AS pair_token
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
)
SELECT
  substr(l.token, 1, 8) || '…' || substr(l.token, -6) AS token_label,
  'https://robinhoodchain.blockscout.com/address/' || l.token AS token_url,
  count(*) AS trades,
  sum(CASE WHEN e.event_name = 'CurveBuy' THEN 1 ELSE 0 END) AS buys,
  sum(CASE WHEN e.event_name = 'CurveSell' THEN 1 ELSE 0 END) AS sells,
  count(DISTINCT CASE WHEN e.event_name = 'CurveBuy' THEN lower(json_extract_string(e.fields_json, '$.buyer')) ELSE lower(json_extract_string(e.fields_json, '$.seller')) END) AS traders,
  round(sum(CASE
    WHEN l.pair_token = '0x0000000000000000000000000000000000000000' AND e.event_name = 'CurveBuy' THEN coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteIn') AS DOUBLE), 0)
    WHEN l.pair_token = '0x0000000000000000000000000000000000000000' THEN coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteOut') AS DOUBLE), 0)
    ELSE 0 END) / 1e18, 3) AS native_volume
FROM events e
JOIN launches l ON lower(e.address) = l.curve
WHERE e.is_deleted = 0 AND e.event_name IN ('CurveBuy', 'CurveSell')
GROUP BY l.token, l.pair_token
ORDER BY trades DESC
LIMIT 25
```

```sql recent_trades
WITH launches AS (
  SELECT lower(json_extract_string(fields_json, '$.curve')) AS curve,
         json_extract_string(fields_json, '$.token') AS token,
         lower(json_extract_string(fields_json, '$.pairToken')) AS pair_token
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
)
SELECT
  epoch_ms(CAST(e.block_timestamp * 1000 AS BIGINT)) AS trade_time,
  CASE WHEN e.event_name = 'CurveBuy' THEN 'Buy' ELSE 'Sell' END AS side,
  substr(l.token, 1, 8) || '…' || substr(l.token, -6) AS token_label,
  'https://robinhoodchain.blockscout.com/address/' || l.token AS token_url,
  CASE WHEN e.event_name = 'CurveBuy'
    THEN substr(json_extract_string(e.fields_json, '$.buyer'), 1, 8) || '…' || substr(json_extract_string(e.fields_json, '$.buyer'), -6)
    ELSE substr(json_extract_string(e.fields_json, '$.seller'), 1, 8) || '…' || substr(json_extract_string(e.fields_json, '$.seller'), -6)
  END AS trader_label,
  'https://robinhoodchain.blockscout.com/address/' || CASE WHEN e.event_name = 'CurveBuy'
    THEN json_extract_string(e.fields_json, '$.buyer') ELSE json_extract_string(e.fields_json, '$.seller') END AS trader_url,
  CASE WHEN l.pair_token = '0x0000000000000000000000000000000000000000' THEN round((CASE WHEN e.event_name = 'CurveBuy'
    THEN try_cast(json_extract_string(e.fields_json, '$.quoteIn') AS DOUBLE)
    ELSE try_cast(json_extract_string(e.fields_json, '$.quoteOut') AS DOUBLE) END) / 1e18, 6) END AS native_quote,
  e.block_number,
  'https://robinhoodchain.blockscout.com/tx/' || e.tx_hash AS tx_url
FROM events e
JOIN launches l ON lower(e.address) = l.curve
WHERE e.is_deleted = 0 AND e.event_name IN ('CurveBuy', 'CurveSell')
ORDER BY e.block_number DESC, e.log_index DESC
LIMIT 100
```

<Grid cols=5>
  <BigValue data={trading_summary} value="trades" title="Trades" fmt="num0" />
  <BigValue data={trading_summary} value="buys" title="Buys" fmt="num0" />
  <BigValue data={trading_summary} value="sells" title="Sells" fmt="num0" />
  <BigValue data={trading_summary} value="native_volume" title="Native volume" fmt="num3" />
  <BigValue data={trading_summary} value="native_fees_tax" title="Native fees + tax" fmt="num3" />
</Grid>

<Grid cols=2>
  <AreaChart data={activity_by_side} x="period" y="trades" series="side" title="Buy and sell velocity" emptySet=pass />
  <BarChart data={active_curves} x="token_label" y="trades" title="Most traded launches" emptySet=pass />
</Grid>

<DataTable data={active_curves} rows=25 rowShading=true sortable=true emptySet=pass>
  <Column id="token_url" title="Token" contentType="link" linkLabel="token_label" openInNewTab=true />
  <Column id="trades" title="Trades" contentType="bar" fmt="num0" barColor="#7c3aed" />
  <Column id="buys" title="Buys" fmt="num0" />
  <Column id="sells" title="Sells" fmt="num0" />
  <Column id="traders" title="Traders" fmt="num0" />
  <Column id="native_volume" title="Native volume" fmt="num3" />
</DataTable>

## Latest executions

<DataTable data={recent_trades} rows=25 rowShading=true sortable=true search=true downloadable=true emptySet=pass>
  <Column id="trade_time" title="Time" fmt="longdate" />
  <Column id="side" title="Side" chip=true />
  <Column id="token_url" title="Token" contentType="link" linkLabel="token_label" openInNewTab=true />
  <Column id="trader_url" title="Trader" contentType="link" linkLabel="trader_label" openInNewTab=true />
  <Column id="native_quote" title="Native quote" fmt="num6" />
  <Column id="block_number" title="Block" fmt="num0" />
  <Column id="tx_url" title="Tx" contentType="link" linkLabel="open" openInNewTab=true />
</DataTable>