---
title: Participants
page_width: full
cards: true
table_of_contents: false
sidebar_position: 4
icon: users
---

# Participants

Wallet activity across launches and bonding-curve trades. Wallet links open the Robinhood Chain explorer.

```sql participant_summary
WITH actors AS (
  SELECT lower(json_extract_string(fields_json, '$.buyer')) AS wallet
  FROM events WHERE is_deleted = 0 AND event_name = 'CurveBuy'
  UNION ALL
  SELECT lower(json_extract_string(fields_json, '$.seller')) AS wallet
  FROM events WHERE is_deleted = 0 AND event_name = 'CurveSell'
)
SELECT
  count(DISTINCT wallet) AS traders,
  count(*) AS trade_actions,
  (SELECT count(DISTINCT lower(json_extract_string(fields_json, '$.deployer'))) FROM events WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched') AS deployers,
  (SELECT count(*) FROM events WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'CreatorFeeRecipientUpdated') AS fee_recipient_updates
FROM actors
```

```sql top_traders
WITH launches AS (
  SELECT lower(json_extract_string(fields_json, '$.curve')) AS curve,
         lower(json_extract_string(fields_json, '$.pairToken')) AS pair_token
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
),
actions AS (
  SELECT
    lower(json_extract_string(e.fields_json, '$.buyer')) AS wallet,
    1 AS buys,
    0 AS sells,
    CASE WHEN l.pair_token = '0x0000000000000000000000000000000000000000' THEN coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteIn') AS DOUBLE), 0) ELSE 0 END AS native_quote
  FROM events e JOIN launches l ON lower(e.address) = l.curve
  WHERE e.is_deleted = 0 AND e.event_name = 'CurveBuy'
  UNION ALL
  SELECT
    lower(json_extract_string(e.fields_json, '$.seller')) AS wallet,
    0 AS buys,
    1 AS sells,
    CASE WHEN l.pair_token = '0x0000000000000000000000000000000000000000' THEN coalesce(try_cast(json_extract_string(e.fields_json, '$.quoteOut') AS DOUBLE), 0) ELSE 0 END AS native_quote
  FROM events e JOIN launches l ON lower(e.address) = l.curve
  WHERE e.is_deleted = 0 AND e.event_name = 'CurveSell'
)
SELECT
  substr(wallet, 1, 8) || '…' || substr(wallet, -6) AS wallet_label,
  'https://robinhoodchain.blockscout.com/address/' || wallet AS wallet_url,
  count(*) AS trades,
  sum(buys) AS buys,
  sum(sells) AS sells,
  round(sum(native_quote) / 1e18, 3) AS native_volume
FROM actions
GROUP BY wallet
ORDER BY trades DESC
LIMIT 50
```

```sql active_deployers
WITH launches AS (
  SELECT
    lower(json_extract_string(fields_json, '$.deployer')) AS deployer,
    lower(json_extract_string(fields_json, '$.curve')) AS curve
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'pons_factory' AND event_name = 'TokenLaunched'
),
launch_counts AS (
  SELECT deployer, count(*) AS launches FROM launches GROUP BY deployer
),
trade_counts AS (
  SELECT l.deployer, count(*) AS curve_trades
  FROM events e JOIN launches l ON lower(e.address) = l.curve
  WHERE e.is_deleted = 0 AND e.event_name IN ('CurveBuy', 'CurveSell')
  GROUP BY l.deployer
)
SELECT
  substr(c.deployer, 1, 8) || '…' || substr(c.deployer, -6) AS deployer_label,
  'https://robinhoodchain.blockscout.com/address/' || c.deployer AS deployer_url,
  c.launches,
  coalesce(t.curve_trades, 0) AS curve_trades
FROM launch_counts c
LEFT JOIN trade_counts t ON c.deployer = t.deployer
ORDER BY launches DESC, curve_trades DESC
LIMIT 30
```

<Grid cols=4>
  <BigValue data={participant_summary} value="traders" title="Unique traders" fmt="num0" />
  <BigValue data={participant_summary} value="trade_actions" title="Trade actions" fmt="num0" />
  <BigValue data={participant_summary} value="deployers" title="Deployers" fmt="num0" />
  <BigValue data={participant_summary} value="fee_recipient_updates" title="Fee recipient updates" fmt="num0" />
</Grid>

<Grid cols=2>
  <BarChart data={top_traders} x="wallet_label" y="trades" title="Most active traders" emptySet=pass />
  <BarChart data={active_deployers} x="deployer_label" y="launches" title="Most active deployers" emptySet=pass />
</Grid>

## Trader leaderboard

<DataTable data={top_traders} rows=25 rowShading=true sortable=true search=true downloadable=true emptySet=pass>
  <Column id="wallet_url" title="Wallet" contentType="link" linkLabel="wallet_label" openInNewTab=true />
  <Column id="trades" title="Trades" contentType="bar" fmt="num0" barColor="#7c3aed" />
  <Column id="buys" title="Buys" fmt="num0" />
  <Column id="sells" title="Sells" fmt="num0" />
  <Column id="native_volume" title="Native volume" fmt="num3" />
</DataTable>

## Launch creators

<DataTable data={active_deployers} rows=20 rowShading=true sortable=true search=true emptySet=pass>
  <Column id="deployer_url" title="Deployer" contentType="link" linkLabel="deployer_label" openInNewTab=true />
  <Column id="launches" title="Launches" contentType="bar" fmt="num0" barColor="#ec4899" />
  <Column id="curve_trades" title="Resulting trades" fmt="num0" />
</DataTable>