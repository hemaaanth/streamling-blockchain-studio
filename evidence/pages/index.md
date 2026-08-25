---
title: Megapot Analytics
hide_title: true
page_width: full
cards: true
table_of_contents: false
auto_refresh: 60000
sidebar_position: 1
icon: layout-dashboard
---

<h1 class="title" data-pp="pp_mt7qvai2bbmc">Megapot Analytics</h1>

Megapot v2 activity from contract deployment through the latest indexed Base block: demand, channels, drawings, participant concentration, referrals, winnings, and LP backer flow.

```sql overview
SELECT
  count(*) AS decoded_events,
  count(*) FILTER (WHERE event_name = 'TicketOrderProcessed') AS order_events,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) ELSE 0 END) AS tickets_sold,
  count(DISTINCT CASE WHEN event_name = 'TicketOrderProcessed' THEN json_extract_string(fields_json, '$.buyer') END) AS buyers,
  count(DISTINCT CASE WHEN event_name IN ('TicketOrderProcessed', 'TicketPurchased') THEN json_extract_string(fields_json, '$.recipient') END) AS recipients,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS lp_earnings_usdc,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS referral_fees_usdc,
  min(block_number) AS first_block,
  max(block_number) AS latest_block
FROM events
WHERE is_deleted = 0
```

```sql flow
SELECT
  epoch_ms(CAST(floor(block_timestamp / 3600) * 3600000 AS BIGINT)) AS hour,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0)) AS tickets,
  count(*) AS orders,
  count(DISTINCT json_extract_string(fields_json, '$.buyer')) AS buyers
FROM events
WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
GROUP BY hour
ORDER BY hour
```

```sql source_mix
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
    END AS source,
    json_extract_string(fields_json, '$.recipient') AS recipient
  FROM events
  WHERE is_deleted = 0 AND event_name = 'TicketPurchased'
)
SELECT
  source,
  count(*) AS tickets,
  count(DISTINCT recipient) AS recipients,
  count(*) * 1.0 / sum(count(*)) OVER () AS share
FROM ticket_sources
GROUP BY source
ORDER BY tickets DESC
```

```sql latest_drawings
SELECT
  coalesce(try_cast(json_extract_string(fields_json, '$.currentDrawingId') AS BIGINT), try_cast(json_extract_string(fields_json, '$.drawingId') AS BIGINT), 0) AS drawing,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) ELSE 0 END) AS tickets,
  count(DISTINCT CASE WHEN event_name = 'TicketOrderProcessed' THEN json_extract_string(fields_json, '$.buyer') END) AS buyers,
  count(DISTINCT CASE WHEN event_name IN ('TicketOrderProcessed', 'TicketPurchased') THEN json_extract_string(fields_json, '$.recipient') END) AS recipients,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS lp_earnings_usdc,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS referral_fees_usdc
FROM events
WHERE is_deleted = 0
GROUP BY drawing
HAVING drawing > 0
ORDER BY drawing DESC
LIMIT 8
```

<Grid cols=4>
  <BigValue data={overview} value="tickets_sold" title="Tickets sold" fmt="num0" />
  <BigValue data={overview} value="buyers" title="Buyers" fmt="num0" />
  <BigValue data={overview} value="recipients" title="Recipients" fmt="num0" />
  <BigValue data={overview} value="decoded_events" title="Decoded events" fmt="num0" />
</Grid>

<Grid cols=3>
  <BigValue data={overview} value="lp_earnings_usdc" title="LP earnings" fmt="usd2" />
  <BigValue data={overview} value="referral_fees_usdc" title="Referral fees" fmt="usd2" />
  <BigValue data={overview} value="latest_block" title="Latest indexed block" fmt="num0" />
</Grid>


<Grid cols=2>
  <AreaChart data={flow} x="hour" y="tickets" title="Ticket velocity by hour" />
  <BarChart data={source_mix} x="source" y="tickets" title="Channel mix" />
</Grid>

<DataTable data={latest_drawings} rows=8 rowShading=true sortable=true>
  <Column id="drawing" title="Drawing" fmt="num0" />
  <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="buyers" title="Buyers" fmt="num0" align="right" />
  <Column id="recipients" title="Recipients" fmt="num0" align="right" />
  <Column id="lp_earnings_usdc" title="LP" fmt="usd2" />
  <Column id="referral_fees_usdc" title="Referral" fmt="usd2" />
</DataTable>

## Explore

- [Dashboard](/dashboard/) — compact monitoring view.
- [Trends](/trends/) — demand and participant charts.
- [Participants](/participants/) — wallet and channel explorer.
- [Data room](/data-room/) — raw events and QA tables.
