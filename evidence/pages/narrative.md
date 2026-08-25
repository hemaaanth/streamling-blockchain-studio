---
title: Narrative read
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 3
icon: book-open
---

Compact analyst read for Megapot v2: demand quality, channel mix, participant concentration, referral economics, and LP backer flow.

```sql headline
SELECT
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) ELSE 0 END) AS tickets_sold,
  count(DISTINCT CASE WHEN event_name = 'TicketOrderProcessed' THEN json_extract_string(fields_json, '$.buyer') END) AS buyers,
  count(DISTINCT CASE WHEN event_name IN ('TicketOrderProcessed', 'TicketPurchased') THEN json_extract_string(fields_json, '$.recipient') END) AS recipients,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS lp_earnings_usdc,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS referral_fees_usdc,
  sum(CASE WHEN event_name = 'TicketWinningsClaimed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.winningsAmount') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS winnings_usdc,
  count(DISTINCT CASE WHEN contract_alias = 'lp_manager' AND event_name IN ('LpDeposited', 'LpWithdrawInitiated', 'LpWithdrawFinalized') THEN json_extract_string(fields_json, '$.lpAddress') END) AS lp_backers,
  sum(CASE WHEN event_name = 'LpDeposited' THEN coalesce(try_cast(json_extract_string(fields_json, '$.amount') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS lp_deposits_usdc
FROM events
WHERE is_deleted = 0
```

```sql weekly_tickets
SELECT
  epoch_ms(CAST(floor(block_timestamp / 604800) * 604800000 AS BIGINT)) AS week,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0)) AS tickets,
  count(*) AS orders,
  count(DISTINCT json_extract_string(fields_json, '$.buyer')) AS buyers
FROM events
WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
GROUP BY week
ORDER BY week
```

```sql source_share
WITH ticket_sources AS (
  SELECT
    CASE json_extract_string(fields_json, '$.source')
      WHEN '0x6d656761706f743a776562000000000000000000000000000000000000000000' THEN 'megapot:web'
      WHEN '0x62616c6b616e6c6f74746f000000000000000000000000000000000000000000' THEN 'balkanlotto'
      WHEN '0x6c6f74706f740000000000000000000000000000000000000000000000000000' THEN 'lotpot'
      WHEN '0x6d656761706f743a636c61696d5f77696e5f636f6d706f756e64000000000000' THEN 'megapot:claim_win_compound'
      WHEN '0x6d656761706f743a636c61696d5f67756172616e746565000000000000000000' THEN 'megapot:claim_guarantee'
      WHEN '0x6d656761706f743a636c61696d5f77696e000000000000000000000000000000' THEN 'megapot:claim_win'
      WHEN '0x6665656c2e636173680000000000000000000000000000000000000000000000' THEN 'feel.cash'
      ELSE 'unknown'
    END AS source
  FROM events
  WHERE is_deleted = 0 AND event_name = 'TicketPurchased'
)
SELECT
  source,
  count(*) AS tickets,
  count(*) * 1.0 / sum(count(*)) OVER () AS share
FROM ticket_sources
GROUP BY source
ORDER BY tickets DESC
```

```sql concentration
WITH orders AS (
  SELECT
    json_extract_string(fields_json, '$.recipient') AS recipient,
    coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) AS tickets
  FROM events
  WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
), ranked AS (
  SELECT recipient, sum(tickets) AS tickets
  FROM orders
  GROUP BY recipient
)
SELECT
  sum(tickets) AS all_tickets,
  sum(tickets) FILTER (WHERE rank <= 10) AS top_10_tickets,
  sum(tickets) FILTER (WHERE rank <= 50) AS top_50_tickets,
  sum(tickets) FILTER (WHERE rank <= 10) * 1.0 / sum(tickets) AS top_10_share,
  sum(tickets) FILTER (WHERE rank <= 50) * 1.0 / sum(tickets) AS top_50_share
FROM (
  SELECT recipient, tickets, row_number() OVER (ORDER BY tickets DESC) AS rank
  FROM ranked
)
```

```sql referral_vs_lp
SELECT
  coalesce(try_cast(json_extract_string(fields_json, '$.currentDrawingId') AS BIGINT), 0) AS drawing,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6) AS lp_earnings_usdc,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6) AS referral_fees_usdc
FROM events
WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
GROUP BY drawing
HAVING drawing > 0
ORDER BY drawing
```

```sql lp_backer_flow
WITH lp_events AS (
  SELECT
    event_name,
    coalesce(try_cast(json_extract_string(fields_json, '$.amount') AS DOUBLE), 0) / 1e6 AS amount_usdc
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'lp_manager'
)
SELECT
  event_name,
  count(*) AS events,
  round(sum(amount_usdc), 2) AS amount_usdc
FROM lp_events
WHERE event_name IN ('LpDeposited', 'LpWithdrawInitiated', 'LpWithdrawFinalized')
GROUP BY event_name
ORDER BY amount_usdc DESC
```


<Grid cols=5>
  <BigValue data={headline} value="tickets_sold" title="Tickets sold" fmt="num0" />
  <BigValue data={headline} value="lp_earnings_usdc" title="LP earnings" fmt="usd2" />
  <BigValue data={headline} value="lp_backers" title="LP backers" fmt="num0" />
  <BigValue data={headline} value="lp_deposits_usdc" title="LP deposits" fmt="usd2" />
  <BigValue data={headline} value="winnings_usdc" title="Claimed winnings" fmt="usd2" />
</Grid>

## 1. Demand is real enough to segment

With coverage from deployment through the latest indexed Base block, Megapot has enough purchase and backer history to separate organic web activity, integrations, compound flows, referrals, repeat buyers, whale-like recipients, and LP treasury movement.

<AreaChart data={weekly_tickets} x="week" y="tickets" title="Weekly ticket demand" />

## 2. Channels are product strategy, not metadata

<DataTable data={source_share} rows=10 rowShading=true sortable=true totalRow=true>
  <Column id="source" title="Source" chip=true />
  <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="share" title="Share" contentType="bar" fmt="pct1" barColor="#f59e0b" />
</DataTable>

The labeled source field is the most actionable surface in the decoded Jackpot logs. It points to where distribution is happening: Megapot web, lottery partners, claim/compound loops, and unclassified traffic that should be decoded further.

## 3. Concentration explains quality of demand

<Grid cols=2>
  <BigValue data={concentration} value="top_10_share" title="Top 10 recipient share" fmt="pct1" />
  <BigValue data={concentration} value="top_50_share" title="Top 50 recipient share" fmt="pct1" />
</Grid>

A broad recipient base is healthier for a consumer jackpot story; concentration is still useful if it reveals integrators, vaults, syndicates, or high-conviction repeat players.

## 4. Referral economics and backer flow are visible

<Grid cols=2>
  <AreaChart data={referral_vs_lp} x="drawing" y="lp_earnings_usdc" series="referral_fees_usdc" title="LP earnings and referral fees by drawing" />
  <BarChart data={lp_backer_flow} x="event_name" y="amount_usdc" title="LP manager deposits and withdrawals" />
</Grid>

Coverage includes the Jackpot and LP manager contracts. That covers demand, referrals, winnings, deposits, and finalized withdrawals. Backer ROI and current pool-value marks require share-accounting state, so they should be calculated separately rather than inferred from event logs.
