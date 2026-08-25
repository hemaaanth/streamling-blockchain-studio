---
title: Dashboard
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 2
icon: chart-column
---

Robinhood Stock Token transfer activity indexed from Robinhood Chain. This dashboard is live and partial while the backfill is running.

```sql overview
SELECT
  count(*) AS decoded_events,
  count(DISTINCT contract_alias) AS symbols_seen,
  count(DISTINCT block_number) AS event_blocks,
  count(DISTINCT tx_hash) AS transactions,
  count(DISTINCT json_extract_string(fields_json, '$.from')) AS senders,
  count(DISTINCT json_extract_string(fields_json, '$.to')) AS recipients,
  count(*) FILTER (WHERE json_extract_string(fields_json, '$.from') = '0x0000000000000000000000000000000000000000') AS mint_events,
  count(*) FILTER (WHERE json_extract_string(fields_json, '$.to') = '0x0000000000000000000000000000000000000000') AS burn_events,
  round(sum(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0)) / 1e18, 2) AS token_units_moved,
  min(block_number) AS first_block,
  max(block_number) AS latest_block,
  min(to_timestamp(block_timestamp)) AS first_seen_at,
  max(to_timestamp(block_timestamp)) AS latest_seen_at
FROM events
WHERE is_deleted = 0
```

```sql symbol_leaders
SELECT
  contract_alias AS symbol,
  count(*) AS transfers,
  count(DISTINCT tx_hash) AS transactions,
  count(DISTINCT json_extract_string(fields_json, '$.from')) AS senders,
  count(DISTINCT json_extract_string(fields_json, '$.to')) AS recipients,
  round(sum(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0)) / 1e18, 2) AS units_moved,
  min(block_number) AS first_block,
  max(block_number) AS latest_block,
  concat('https://robinhoodchain.blockscout.com/address/', min(address)) AS contract_url
FROM events
WHERE is_deleted = 0
GROUP BY contract_alias
ORDER BY transfers DESC
LIMIT 25
```

```sql transfer_flow
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

```sql counterparty_leaders
WITH parties AS (
  SELECT 'sender' AS side, json_extract_string(fields_json, '$.from') AS wallet, contract_alias, tx_hash
  FROM events
  WHERE is_deleted = 0
  UNION ALL
  SELECT 'recipient' AS side, json_extract_string(fields_json, '$.to') AS wallet, contract_alias, tx_hash
  FROM events
  WHERE is_deleted = 0
)
SELECT
  side,
  concat(substr(wallet, 1, 6), '…', substr(wallet, -4)) AS wallet,
  concat('https://robinhoodchain.blockscout.com/address/', wallet) AS wallet_url,
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols,
  count(DISTINCT tx_hash) AS transactions
FROM parties
WHERE wallet != '' AND wallet != '0x0000000000000000000000000000000000000000'
GROUP BY side, wallet
ORDER BY transfers DESC
LIMIT 20
```

<Grid cols=4>
  <BigValue data={overview} value="decoded_events" title="Transfers" fmt="num0" />
  <BigValue data={overview} value="symbols_seen" title="Symbols seen" fmt="num0" />
  <BigValue data={overview} value="transactions" title="Transactions" fmt="num0" />
  <BigValue data={overview} value="token_units_moved" title="Token units moved" fmt="num2" />
</Grid>

<Grid cols=4>
  <BigValue data={overview} value="mint_events" title="Mint events" fmt="num0" />
  <BigValue data={overview} value="burn_events" title="Burn events" fmt="num0" />
  <BigValue data={overview} value="first_block" title="First block" fmt="num0" />
  <BigValue data={overview} value="latest_block" title="Latest block" fmt="num0" />
</Grid>

<Grid cols=2>
  <AreaChart data={transfer_flow} x="day" y="transfers" title="Transfers by day" />
  <AreaChart data={transfer_flow} x="day" y="units_moved" title="Token units moved by day" />
</Grid>

## Symbols

<DataTable data={symbol_leaders} rows=25 rowShading=true sortable=true search=true downloadable=true>
  <Column id="symbol" title="Symbol" chip=true />
  <Column id="transfers" title="Transfers" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="transactions" title="Txs" fmt="num0" />
  <Column id="senders" title="Senders" fmt="num0" />
  <Column id="recipients" title="Recipients" fmt="num0" />
  <Column id="units_moved" title="Units moved" fmt="num2" />
  <Column id="first_block" title="First" fmt="num0" />
  <Column id="latest_block" title="Latest" fmt="num0" />
  <Column id="contract_url" title="Contract" contentType="link" linkLabel="open" openInNewTab=true />
</DataTable>

## Active wallets

<DataTable data={counterparty_leaders} rows=20 rowShading=true sortable=true downloadable=true>
  <Column id="side" title="Side" chip=true />
  <Column id="wallet_url" title="Wallet" contentType="link" linkLabel="wallet" openInNewTab=true />
  <Column id="transfers" title="Transfers" contentType="bar" fmt="num0" barColor="#f59e0b" />
  <Column id="symbols" title="Symbols" fmt="num0" />
  <Column id="transactions" title="Txs" fmt="num0" />
</DataTable>
