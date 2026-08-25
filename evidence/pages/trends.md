---
title: Trends
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 3
icon: book-open
---


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

## Weekly demand

<AreaChart data={weekly_tickets} x="week" y="tickets" title="Weekly ticket demand" />


## Participant concentration

<Grid cols=2>
  <BigValue data={concentration} value="top_10_share" title="Top 10 recipient share" fmt="pct1" />
  <BigValue data={concentration} value="top_50_share" title="Top 50 recipient share" fmt="pct1" />
</Grid>

## LP earnings and referral fees

<Grid cols=2>
  <AreaChart data={referral_vs_lp} x="drawing" y="lp_earnings_usdc" series="referral_fees_usdc" title="LP earnings and referral fees by drawing" />
  <BarChart data={lp_backer_flow} x="event_name" y="amount_usdc" title="LP manager deposits and withdrawals" />
</Grid>
