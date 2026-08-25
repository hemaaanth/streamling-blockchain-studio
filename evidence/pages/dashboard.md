---
title: Dashboard
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 2
icon: chart-column
---


Megapot v2 activity from contract deployment through the latest indexed Base block.

```sql overview
SELECT
  count(*) AS decoded_events,
  count(*) FILTER (WHERE event_name = 'TicketPurchased') AS ticket_events,
  count(*) FILTER (WHERE event_name = 'TicketOrderProcessed') AS order_events,
  count(DISTINCT CASE WHEN event_name = 'TicketOrderProcessed' THEN json_extract_string(fields_json, '$.buyer') END) AS buyers,
  count(DISTINCT CASE WHEN event_name IN ('TicketOrderProcessed', 'TicketPurchased') THEN json_extract_string(fields_json, '$.recipient') END) AS recipients,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) ELSE 0 END) AS tickets_sold,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS lp_earnings_usdc,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS referral_fees_usdc,
  sum(CASE WHEN event_name = 'TicketWinningsClaimed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.winningsAmount') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS claimed_winnings_usdc,
  count(DISTINCT CASE WHEN event_name = 'TicketWinningsClaimed' THEN json_extract_string(fields_json, '$.userAddress') END) AS winning_claimants,
  min(to_timestamp(block_timestamp)) AS first_seen_at,
  max(to_timestamp(block_timestamp)) AS latest_seen_at,
  min(block_number) AS first_indexed_block,
  max(block_number) AS latest_indexed_block
FROM events
WHERE is_deleted = 0
```

```sql lp_summary
WITH lp_events AS (
  SELECT
    event_name,
    json_extract_string(fields_json, '$.lpAddress') AS lp_address,
    coalesce(try_cast(json_extract_string(fields_json, '$.amount') AS DOUBLE), 0) / 1e6 AS amount_usdc
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'lp_manager'
)
SELECT
  count(DISTINCT CASE WHEN event_name IN ('LpDeposited', 'LpWithdrawInitiated', 'LpWithdrawFinalized') THEN lp_address END) AS lp_backers,
  sum(CASE WHEN event_name = 'LpDeposited' THEN amount_usdc ELSE 0 END) AS deposits_usdc,
  sum(CASE WHEN event_name = 'LpWithdrawFinalized' THEN amount_usdc ELSE 0 END) AS finalized_withdrawals_usdc,
  sum(CASE WHEN event_name = 'LpWithdrawInitiated' THEN amount_usdc ELSE 0 END) AS initiated_withdrawals_usdc,
  sum(CASE WHEN event_name = 'LpDeposited' THEN amount_usdc WHEN event_name = 'LpWithdrawFinalized' THEN -amount_usdc ELSE 0 END) AS net_deposits_usdc
FROM lp_events
```

```sql lp_backers
WITH lp_events AS (
  SELECT
    json_extract_string(fields_json, '$.lpAddress') AS lp_address,
    event_name,
    coalesce(try_cast(json_extract_string(fields_json, '$.amount') AS DOUBLE), 0) / 1e6 AS amount_usdc
  FROM events
  WHERE is_deleted = 0 AND contract_alias = 'lp_manager'
    AND event_name IN ('LpDeposited', 'LpWithdrawInitiated', 'LpWithdrawFinalized')
), backers AS (
  SELECT
    lp_address,
    sum(CASE WHEN event_name = 'LpDeposited' THEN amount_usdc ELSE 0 END) AS deposits_usdc,
    sum(CASE WHEN event_name = 'LpWithdrawFinalized' THEN amount_usdc ELSE 0 END) AS withdrawals_usdc,
    count(*) FILTER (WHERE event_name = 'LpDeposited') AS deposit_events,
    count(*) FILTER (WHERE event_name IN ('LpWithdrawInitiated', 'LpWithdrawFinalized')) AS withdrawal_events
  FROM lp_events
  GROUP BY lp_address
)
SELECT
  concat(substr(lp_address, 1, 6), '…', substr(lp_address, -4)) AS backer,
  concat('https://basescan.org/address/', lp_address) AS backer_url,
  round(deposits_usdc, 2) AS deposits_usdc,
  round(withdrawals_usdc, 2) AS withdrawals_usdc,
  round(deposits_usdc - withdrawals_usdc, 2) AS net_usdc,
  deposit_events,
  withdrawal_events
FROM backers
ORDER BY net_usdc DESC
LIMIT 15
```

```sql data_quality
WITH base AS (
  SELECT *
  FROM events
  WHERE is_deleted = 0
)
SELECT
  count(*) AS decoded_events,
  count(DISTINCT event_id) AS unique_event_ids,
  count(*) - count(DISTINCT event_id) AS duplicate_event_ids,
  count(DISTINCT contract_alias) AS contracts_indexed,
  count(DISTINCT event_name) AS event_types,
  min(block_number) AS first_block,
  max(block_number) AS latest_block,
  count(DISTINCT block_number) AS blocks_with_events
FROM base
```



```sql round_snapshot
SELECT
  coalesce(try_cast(json_extract_string(fields_json, '$.currentDrawingId') AS BIGINT), try_cast(json_extract_string(fields_json, '$.drawingId') AS BIGINT), 0) AS drawing_id,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) ELSE 0 END) AS tickets,
  count(DISTINCT CASE WHEN event_name = 'TicketOrderProcessed' THEN json_extract_string(fields_json, '$.buyer') END) AS buyers,
  count(DISTINCT CASE WHEN event_name IN ('TicketOrderProcessed', 'TicketPurchased') THEN json_extract_string(fields_json, '$.recipient') END) AS recipients,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS lp_earnings_usdc,
  sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS referral_fees_usdc
FROM events
WHERE is_deleted = 0
  AND event_name IN ('NewDrawingInitialized', 'JackpotSettled', 'TicketOrderProcessed', 'TicketPurchased')
GROUP BY drawing_id
HAVING drawing_id > 0
ORDER BY drawing_id DESC
LIMIT 10
```

```sql unit_economics
WITH totals AS (
  SELECT
    sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) ELSE 0 END) AS tickets_sold,
    sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS lp_earnings_usdc,
    sum(CASE WHEN event_name = 'TicketOrderProcessed' THEN coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6 ELSE 0 END) AS referral_fees_usdc
  FROM events
  WHERE is_deleted = 0
)
SELECT
  tickets_sold,
  lp_earnings_usdc,
  referral_fees_usdc,
  CASE WHEN tickets_sold > 0 THEN lp_earnings_usdc / tickets_sold END AS lp_earnings_per_ticket,
  CASE WHEN tickets_sold > 0 THEN referral_fees_usdc / tickets_sold END AS referral_fee_per_ticket,
  CASE WHEN tickets_sold > 0 THEN (lp_earnings_usdc + referral_fees_usdc) / tickets_sold END AS protocol_take_per_ticket,
  CASE WHEN (lp_earnings_usdc + referral_fees_usdc) > 0 THEN referral_fees_usdc / (lp_earnings_usdc + referral_fees_usdc) END AS referral_take_rate
FROM totals
```

```sql ticket_flow
SELECT
  epoch_ms(CAST(floor(block_timestamp / 86400) * 86400000 AS BIGINT)) AS bucket,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0)) AS tickets,
  count(*) AS orders,
  count(DISTINCT json_extract_string(fields_json, '$.buyer')) AS buyers,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6) AS lp_earnings_usdc,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6) AS referral_fees_usdc
FROM events
WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
GROUP BY bucket
ORDER BY bucket
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
      WHEN '0x6d656761706f743a696e633a626f6e7573000000000000000000000000000000' THEN 'megapot:inc:bonus'
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
  count(*) * 1.0 / sum(count(*)) OVER () AS ticket_share
FROM ticket_sources
GROUP BY source
ORDER BY tickets DESC
```

```sql participant_leaders
WITH orders AS (
  SELECT
    json_extract_string(fields_json, '$.recipient') AS recipient,
    json_extract_string(fields_json, '$.buyer') AS buyer,
    coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) AS tickets
  FROM events
  WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
)
SELECT
  concat(substr(recipient, 1, 6), '…', substr(recipient, -4)) AS recipient,
  sum(tickets) AS tickets,
  sum(tickets) * 1.0 / sum(sum(tickets)) OVER () AS ticket_share,
  count(*) AS orders,
  count(DISTINCT buyer) AS buyers,
  concat('https://basescan.org/address/', recipient) AS recipient_url
FROM orders
GROUP BY recipient
ORDER BY tickets DESC
LIMIT 15
```

```sql referrer_leaders
SELECT
  concat(substr(json_extract_string(fields_json, '$.referrer'), 1, 6), '…', substr(json_extract_string(fields_json, '$.referrer'), -4)) AS referrer,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.amount') AS DOUBLE), 0) / 1e6) AS fees_usdc,
  count(*) AS fee_events
FROM events
WHERE is_deleted = 0 AND event_name = 'ReferralFeeCollected'
GROUP BY referrer
ORDER BY fees_usdc DESC
LIMIT 10
```

```sql winning_claims
SELECT
  concat(substr(json_extract_string(fields_json, '$.userAddress'), 1, 6), '…', substr(json_extract_string(fields_json, '$.userAddress'), -4)) AS claimant,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.winningsAmount') AS DOUBLE), 0) / 1e6) AS winnings_usdc,
  count(*) AS claims,
  max(coalesce(try_cast(json_extract_string(fields_json, '$.matchedNormals') AS BIGINT), 0)) AS best_normal_matches
FROM events
WHERE is_deleted = 0 AND event_name = 'TicketWinningsClaimed'
GROUP BY claimant
ORDER BY winnings_usdc DESC
LIMIT 10
```

```sql recent_orders
SELECT
  to_timestamp(block_timestamp) AS purchased_at,
  concat(substr(json_extract_string(fields_json, '$.buyer'), 1, 6), '…', substr(json_extract_string(fields_json, '$.buyer'), -4)) AS buyer,
  concat('https://basescan.org/address/', json_extract_string(fields_json, '$.buyer')) AS buyer_url,
  concat(substr(json_extract_string(fields_json, '$.recipient'), 1, 6), '…', substr(json_extract_string(fields_json, '$.recipient'), -4)) AS recipient,
  concat('https://basescan.org/address/', json_extract_string(fields_json, '$.recipient')) AS recipient_url,
  json_extract_string(fields_json, '$.currentDrawingId') AS drawing_id,
  coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) AS tickets,
  round(coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6, 2) AS lp_earnings_usdc,
  round(coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6, 2) AS referral_fees_usdc,
  concat('https://basescan.org/tx/', tx_hash) AS tx_url,
  concat(substr(tx_hash, 1, 10), '…', substr(tx_hash, -6)) AS tx
FROM events
WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
ORDER BY block_number DESC, log_index DESC
LIMIT 25
```

## Jackpot activity

<Grid cols=4>
  <BigValue data={overview} value="tickets_sold" title="Tickets sold" fmt="num0" />
  <BigValue data={overview} value="recipients" title="Recipients" fmt="num0" />
  <BigValue data={overview} value="lp_earnings_usdc" title="LP earnings" fmt="usd2" />
  <BigValue data={overview} value="referral_fees_usdc" title="Referral fees" fmt="usd2" />
</Grid>

## LP backer flow

<Grid cols=4>
  <BigValue data={lp_summary} value="lp_backers" title="LP backers" fmt="num0" />
  <BigValue data={lp_summary} value="deposits_usdc" title="Deposits" fmt="usd2" />
  <BigValue data={lp_summary} value="finalized_withdrawals_usdc" title="Withdrawals" fmt="usd2" />
  <BigValue data={lp_summary} value="net_deposits_usdc" title="Net deposits" fmt="usd2" />
</Grid>

<DataTable data={lp_backers} rows=15 rowShading=true sortable=true search=true downloadable=true>
  <Column id="backer_url" title="Backer" contentType="link" linkLabel="backer" openInNewTab=true />
  <Column id="deposits_usdc" title="Deposits" contentType="bar" fmt="usd2" barColor="#2563eb" />
  <Column id="withdrawals_usdc" title="Withdrawals" fmt="usd2" />
  <Column id="net_usdc" title="Net" contentType="bar" fmt="usd2" barColor="#16a34a" />
  <Column id="deposit_events" title="Deposits" fmt="num0" align="right" />
  <Column id="withdrawal_events" title="Withdrawals" fmt="num0" align="right" />
</DataTable>

## Tickets and channels

<Grid cols=2>
  <AreaChart
    data={ticket_flow}
    x="bucket"
    y="tickets"
    title="Daily tickets"
  />
  <BarChart
    data={source_mix}
    x="source"
    y="tickets"
    title="Tickets by source label"
  />
</Grid>


<DataTable data={source_mix} rows=10 rowShading=true sortable=true totalRow=true downloadable=true>
  <Column id="source" title="Source" chip=true />
  <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="recipients" title="Recipients" fmt="num0" align="right" />
  <Column id="ticket_share" title="Share" contentType="bar" fmt="pct1" barColor="#f59e0b" />
</DataTable>

## Participants

<Grid cols=2>
  <DataTable data={participant_leaders} rows=15 rowShading=true sortable=true downloadable=true>
    <Column id="recipient_url" title="Recipient" contentType="link" linkLabel="recipient" openInNewTab=true />
    <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
    <Column id="ticket_share" title="Share" contentType="bar" fmt="pct1" barColor="#f59e0b" />
    <Column id="orders" title="Orders" fmt="num0" align="right" />
    <Column id="buyers" title="Buyers" fmt="num0" align="right" />
  </DataTable>
  <DataTable data={round_snapshot} rows=10 rowShading=true sortable=true>
    <Column id="drawing_id" title="Drawing" fmt="num0" />
    <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
    <Column id="buyers" title="Buyers" fmt="num0" />
    <Column id="recipients" title="Recipients" fmt="num0" />
    <Column id="lp_earnings_usdc" title="LP earnings" fmt="usd2" />
    <Column id="referral_fees_usdc" title="Referral fees" fmt="usd2" />
  </DataTable>
</Grid>


## Referrals

<Grid cols=2>
  <DataTable data={referrer_leaders} rows=10 rowShading=true sortable=true totalRow=true>
    <Column id="referrer" title="Referrer" />
    <Column id="fees_usdc" title="Fees" contentType="bar" fmt="usd2" barColor="#16a34a" />
    <Column id="fee_events" title="Fee events" fmt="num0" align="right" />
  </DataTable>
  <DataTable data={winning_claims} rows=10 rowShading=true sortable=true totalRow=true>
    <Column id="claimant" title="Claimant" />
    <Column id="winnings_usdc" title="Winnings" contentType="bar" fmt="usd2" barColor="#16a34a" />
    <Column id="claims" title="Claims" fmt="num0" align="right" />
    <Column id="best_normal_matches" title="Best match" fmt="num0" align="right" />
  </DataTable>
</Grid>


## Orders

<DataTable data={recent_orders} rows=25 rowShading=true sortable=true search=true downloadable=true compact=true>
  <Column id="purchased_at" title="Purchased" fmt="date" />
  <Column id="buyer_url" title="Buyer" contentType="link" linkLabel="buyer" openInNewTab=true />
  <Column id="recipient_url" title="Recipient" contentType="link" linkLabel="recipient" openInNewTab=true />
  <Column id="drawing_id" title="Drawing" />
  <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="lp_earnings_usdc" title="LP" fmt="usd2" />
  <Column id="referral_fees_usdc" title="Referral" fmt="usd2" />
  <Column id="tx_url" title="Tx" contentType="link" linkLabel="tx" openInNewTab=true />
</DataTable>

## Data quality

<Grid cols=4>
  <BigValue data={data_quality} value="decoded_events" title="Decoded" fmt="num0" />
  <BigValue data={data_quality} value="duplicate_event_ids" title="Duplicate IDs" fmt="num0" />
  <BigValue data={data_quality} value="contracts_indexed" title="Contracts" fmt="num0" />
  <BigValue data={data_quality} value="event_types" title="Event types" fmt="num0" />
</Grid>

