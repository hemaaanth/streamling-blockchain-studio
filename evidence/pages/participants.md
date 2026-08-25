---
title: Participants
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 4
icon: users
---


```sql buyer_leaders
WITH orders AS (
  SELECT
    json_extract_string(fields_json, '$.buyer') AS buyer,
    json_extract_string(fields_json, '$.recipient') AS recipient,
    coalesce(try_cast(json_extract_string(fields_json, '$.numberOfTickets') AS BIGINT), 0) AS tickets,
    coalesce(try_cast(json_extract_string(fields_json, '$.lpEarnings') AS DOUBLE), 0) / 1e6 AS lp_earnings_usdc,
    coalesce(try_cast(json_extract_string(fields_json, '$.referralFees') AS DOUBLE), 0) / 1e6 AS referral_fees_usdc
  FROM events
  WHERE is_deleted = 0 AND event_name = 'TicketOrderProcessed'
)
SELECT
  concat(substr(buyer, 1, 6), '…', substr(buyer, -4)) AS buyer,
  concat('https://basescan.org/address/', buyer) AS buyer_url,
  sum(tickets) AS tickets,
  count(*) AS orders,
  count(DISTINCT recipient) AS recipients,
  sum(lp_earnings_usdc) AS lp_earnings_usdc,
  sum(referral_fees_usdc) AS referral_fees_usdc
FROM orders
GROUP BY buyer
ORDER BY tickets DESC
LIMIT 25
```

```sql recipient_leaders
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
  concat('https://basescan.org/address/', recipient) AS recipient_url,
  sum(tickets) AS tickets,
  sum(tickets) * 1.0 / sum(sum(tickets)) OVER () AS ticket_share,
  count(*) AS orders,
  count(DISTINCT buyer) AS buyers
FROM orders
GROUP BY recipient
ORDER BY tickets DESC
LIMIT 25
```

```sql referrers
SELECT
  concat(substr(json_extract_string(fields_json, '$.referrer'), 1, 6), '…', substr(json_extract_string(fields_json, '$.referrer'), -4)) AS referrer,
  concat('https://basescan.org/address/', json_extract_string(fields_json, '$.referrer')) AS referrer_url,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.amount') AS DOUBLE), 0) / 1e6) AS fees_usdc,
  count(*) AS fee_events
FROM events
WHERE is_deleted = 0 AND event_name = 'ReferralFeeCollected'
GROUP BY referrer_url, referrer
ORDER BY fees_usdc DESC
LIMIT 25
```

```sql winners
SELECT
  concat(substr(json_extract_string(fields_json, '$.userAddress'), 1, 6), '…', substr(json_extract_string(fields_json, '$.userAddress'), -4)) AS claimant,
  concat('https://basescan.org/address/', json_extract_string(fields_json, '$.userAddress')) AS claimant_url,
  sum(coalesce(try_cast(json_extract_string(fields_json, '$.winningsAmount') AS DOUBLE), 0) / 1e6) AS winnings_usdc,
  count(*) AS claims,
  max(coalesce(try_cast(json_extract_string(fields_json, '$.matchedNormals') AS BIGINT), 0)) AS best_match
FROM events
WHERE is_deleted = 0 AND event_name = 'TicketWinningsClaimed'
GROUP BY claimant_url, claimant
ORDER BY winnings_usdc DESC
LIMIT 25
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
LIMIT 25
```


```sql participant_totals
SELECT
  count(DISTINCT CASE WHEN event_name = 'TicketOrderProcessed' THEN json_extract_string(fields_json, '$.buyer') END) AS buyers,
  count(DISTINCT CASE WHEN event_name IN ('TicketOrderProcessed', 'TicketPurchased') THEN json_extract_string(fields_json, '$.recipient') END) AS recipients,
  count(DISTINCT CASE WHEN event_name = 'ReferralFeeCollected' THEN json_extract_string(fields_json, '$.referrer') END) AS referrers,
  count(DISTINCT CASE WHEN event_name = 'TicketWinningsClaimed' THEN json_extract_string(fields_json, '$.userAddress') END) AS claimants,
  count(DISTINCT CASE WHEN contract_alias = 'lp_manager' AND event_name IN ('LpDeposited', 'LpWithdrawInitiated', 'LpWithdrawFinalized') THEN json_extract_string(fields_json, '$.lpAddress') END) AS lp_backers
FROM events
WHERE is_deleted = 0
```

<Grid cols=5>
  <BigValue data={participant_totals} value="buyers" title="Buyers" fmt="num0" />
  <BigValue data={participant_totals} value="recipients" title="Recipients" fmt="num0" />
  <BigValue data={participant_totals} value="referrers" title="Referrers" fmt="num0" />
  <BigValue data={participant_totals} value="claimants" title="Claimants" fmt="num0" />
  <BigValue data={participant_totals} value="lp_backers" title="LP backers" fmt="num0" />
</Grid>

## Buyers

<DataTable data={buyer_leaders} rows=25 rowShading=true sortable=true search=true downloadable=true>
  <Column id="buyer_url" title="Buyer" contentType="link" linkLabel="buyer" openInNewTab=true />
  <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="orders" title="Orders" fmt="num0" align="right" />
  <Column id="recipients" title="Recipients" fmt="num0" align="right" />
  <Column id="lp_earnings_usdc" title="LP" fmt="usd2" />
  <Column id="referral_fees_usdc" title="Referral" fmt="usd2" />
</DataTable>

## Recipients

<DataTable data={recipient_leaders} rows=25 rowShading=true sortable=true search=true downloadable=true>
  <Column id="recipient_url" title="Recipient" contentType="link" linkLabel="recipient" openInNewTab=true />
  <Column id="tickets" title="Tickets" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="ticket_share" title="Share" contentType="bar" fmt="pct1" barColor="#f59e0b" />
  <Column id="orders" title="Orders" fmt="num0" align="right" />
  <Column id="buyers" title="Buyers" fmt="num0" align="right" />
</DataTable>

## LP backers

<DataTable data={lp_backers} rows=25 rowShading=true sortable=true search=true downloadable=true>
  <Column id="backer_url" title="Backer" contentType="link" linkLabel="backer" openInNewTab=true />
  <Column id="deposits_usdc" title="Deposits" contentType="bar" fmt="usd2" barColor="#2563eb" />
  <Column id="withdrawals_usdc" title="Withdrawals" fmt="usd2" />
  <Column id="net_usdc" title="Net" contentType="bar" fmt="usd2" barColor="#16a34a" />
  <Column id="deposit_events" title="Deposit events" fmt="num0" align="right" />
  <Column id="withdrawal_events" title="Withdrawal events" fmt="num0" align="right" />
</DataTable>


## Referrers and winners

<Grid cols=2>
  <DataTable data={referrers} rows=15 rowShading=true sortable=true search=true>
    <Column id="referrer_url" title="Referrer" contentType="link" linkLabel="referrer" openInNewTab=true />
    <Column id="fees_usdc" title="Fees" contentType="bar" fmt="usd2" barColor="#16a34a" />
    <Column id="fee_events" title="Events" fmt="num0" align="right" />
  </DataTable>
  <DataTable data={winners} rows=15 rowShading=true sortable=true search=true>
    <Column id="claimant_url" title="Claimant" contentType="link" linkLabel="claimant" openInNewTab=true />
    <Column id="winnings_usdc" title="Winnings" contentType="bar" fmt="usd2" barColor="#16a34a" />
    <Column id="claims" title="Claims" fmt="num0" align="right" />
    <Column id="best_match" title="Best match" fmt="num0" align="right" />
  </DataTable>
</Grid>
