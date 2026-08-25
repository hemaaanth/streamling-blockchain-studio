---
title: Robinhood Stock Token Analytics
hide_title: true
page_width: full
cards: true
table_of_contents: false
auto_refresh: 60000
sidebar_position: 1
icon: layout-dashboard
---

<h1 class="title" data-pp="pp_mt7qvai2bbmc">Robinhood Stock Token Analytics</h1>

Live transfer analytics for Robinhood's Stock Tokens on Robinhood Chain. The current view is intentionally partial while the historical backfill runs.

```sql overview
SELECT
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols,
  count(DISTINCT tx_hash) AS transactions,
  count(DISTINCT block_number) AS event_blocks,
  round(sum(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0)) / 1e18, 2) AS units_moved,
  min(block_number) AS first_block,
  max(block_number) AS latest_block
FROM events
WHERE is_deleted = 0
```

```sql top_symbols
SELECT
  contract_alias AS symbol,
  count(*) AS transfers,
  round(sum(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0)) / 1e18, 2) AS units_moved,
  count(DISTINCT tx_hash) AS transactions
FROM events
WHERE is_deleted = 0
GROUP BY contract_alias
ORDER BY transfers DESC
LIMIT 12
```

```sql daily_flow
SELECT
  epoch_ms(CAST(floor(block_timestamp / 86400) * 86400000 AS BIGINT)) AS day,
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols
FROM events
WHERE is_deleted = 0
GROUP BY day
ORDER BY day
```

<Grid cols=4>
  <BigValue data={overview} value="transfers" title="Transfers" fmt="num0" />
  <BigValue data={overview} value="symbols" title="Symbols seen" fmt="num0" />
  <BigValue data={overview} value="transactions" title="Transactions" fmt="num0" />
  <BigValue data={overview} value="latest_block" title="Latest indexed block" fmt="num0" />
</Grid>

<Grid cols=2>
  <AreaChart data={daily_flow} x="day" y="transfers" title="Transfer velocity" />
  <BarChart data={top_symbols} x="symbol" y="transfers" title="Most active symbols" />
</Grid>

<DataTable data={top_symbols} rows=12 rowShading=true sortable=true>
  <Column id="symbol" title="Symbol" chip=true />
  <Column id="transfers" title="Transfers" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="units_moved" title="Units moved" fmt="num2" />
  <Column id="transactions" title="Txs" fmt="num0" />
</DataTable>

## Explore

- [Dashboard](/dashboard/) — symbol, wallet, and transfer flow monitoring.
- [Trends](/trends/) — velocity and concentration charts.
- [Participants](/participants/) — wallet explorer.
- [Data room](/data-room/) — raw transfers and contract coverage.
- [Quality](/quality/) — replay audit status and gaps.
