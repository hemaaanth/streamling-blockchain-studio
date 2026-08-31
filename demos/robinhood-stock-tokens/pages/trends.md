---
title: Trends
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 3
icon: book-open
---

Transfer velocity, symbol breadth, and wallet concentration for indexed Robinhood Stock Token activity.

```sql headline
SELECT
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols,
  count(DISTINCT tx_hash) AS transactions,
  count(DISTINCT json_extract_string(fields_json, '$.from')) AS senders,
  count(DISTINCT json_extract_string(fields_json, '$.to')) AS recipients,
  round(sum(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0)) / 1e18, 2) AS units_moved
FROM events
WHERE is_deleted = 0
```

```sql daily_transfers
SELECT
  epoch_ms(CAST(floor(block_timestamp / 86400) * 86400000 AS BIGINT)) AS day,
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols,
  count(DISTINCT tx_hash) AS transactions,
  round(sum(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0)) / 1e18, 2) AS units_moved
FROM events
WHERE is_deleted = 0
GROUP BY day
ORDER BY day
```

```sql symbol_concentration
WITH ranked AS (
  SELECT
    contract_alias,
    count(*) AS transfers,
    row_number() OVER (ORDER BY count(*) DESC) AS rank
  FROM events
  WHERE is_deleted = 0
  GROUP BY contract_alias
)
SELECT
  sum(transfers) AS all_transfers,
  sum(transfers) FILTER (WHERE rank <= 5) AS top_5_transfers,
  sum(transfers) FILTER (WHERE rank <= 20) AS top_20_transfers,
  sum(transfers) FILTER (WHERE rank <= 5) * 1.0 / sum(transfers) AS top_5_share,
  sum(transfers) FILTER (WHERE rank <= 20) * 1.0 / sum(transfers) AS top_20_share
FROM ranked
```

```sql mint_burn_flow
SELECT
  contract_alias AS symbol,
  count(*) FILTER (WHERE json_extract_string(fields_json, '$.from') = '0x0000000000000000000000000000000000000000') AS mints,
  count(*) FILTER (WHERE json_extract_string(fields_json, '$.to') = '0x0000000000000000000000000000000000000000') AS burns,
  count(*) FILTER (WHERE json_extract_string(fields_json, '$.from') != '0x0000000000000000000000000000000000000000' AND json_extract_string(fields_json, '$.to') != '0x0000000000000000000000000000000000000000') AS transfers
FROM events
WHERE is_deleted = 0
GROUP BY contract_alias
ORDER BY transfers DESC
LIMIT 20
```

<Grid cols=5>
  <BigValue data={headline} value="transfers" title="Transfers" fmt="num0" />
  <BigValue data={headline} value="symbols" title="Symbols" fmt="num0" />
  <BigValue data={headline} value="transactions" title="Transactions" fmt="num0" />
  <BigValue data={headline} value="senders" title="Senders" fmt="num0" />
  <BigValue data={headline} value="recipients" title="Recipients" fmt="num0" />
</Grid>

## Daily activity

<Grid cols=2>
  <AreaChart data={daily_transfers} x="day" y="transfers" title="Daily transfers" />
  <AreaChart data={daily_transfers} x="day" y="symbols" title="Active symbols by day" />
</Grid>

## Concentration

<Grid cols=2>
  <BigValue data={symbol_concentration} value="top_5_share" title="Top 5 symbol share" fmt="pct1" />
  <BigValue data={symbol_concentration} value="top_20_share" title="Top 20 symbol share" fmt="pct1" />
</Grid>

<BarChart data={mint_burn_flow} x="symbol" y="transfers" title="Symbol transfer volume" />

<DataTable data={mint_burn_flow} rows=20 rowShading=true sortable=true downloadable=true>
  <Column id="symbol" title="Symbol" chip=true />
  <Column id="transfers" title="Transfers" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="mints" title="Mints" fmt="num0" />
  <Column id="burns" title="Burns" fmt="num0" />
</DataTable>
