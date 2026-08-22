---
title: Streamling Blockchain Studio — FWA Report
page_width: article
cards: false
table_of_contents: true
auto_refresh: 60000
sidebar_position: 2
---

# How FWA is being used

**Fake World Assets (FWA)** is an Ethereum protocol where depositors list NFTs with ETH backing and purchasers acquire a randomly selected position. This report follows the onchain lifecycle—from listing through allocation and settlement—using only decoded contract events.

```sql lifecycle
SELECT
  countIf(event_name = 'NFTListed') AS listed,
  countIf(event_name = 'AcquisitionRequested') AS requested,
  countIf(event_name = 'NFTAllocated') AS allocated,
  countIf(event_name = 'NFTKept') AS kept,
  countIf(event_name IN ('DepositorBidAccepted', 'DepositorBidAcceptedAsTokens')) AS bids_accepted,
  countIf(event_name = 'ListingWithdrawn') AS withdrawn
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0
```

```sql participation
SELECT
  uniqExactIf(JSONExtractString(fields_json, 'depositor'), event_name = 'NFTListed') AS depositors,
  uniqExactIf(JSONExtractString(fields_json, 'purchaser'), event_name IN ('AcquisitionRequested', 'NFTAllocated', 'NFTKept', 'DepositorBidAccepted', 'DepositorBidAcceptedAsTokens')) AS purchasers,
  uniqExactIf(JSONExtractString(fields_json, 'collection'), event_name = 'NFTListed') AS collections,
  sumIf(toFloat64OrZero(JSONExtractString(fields_json, 'value')), event_name = 'NFTListed') / 1e18 AS listed_eth
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0
```

```sql collection_activity
SELECT
  JSONExtractString(fields_json, 'collection') AS collection,
  count() AS listings,
  uniqExact(JSONExtractString(fields_json, 'depositor')) AS depositors,
  sum(toFloat64OrZero(JSONExtractString(fields_json, 'value'))) / 1e18 AS backing_eth
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0 AND event_name = 'NFTListed'
GROUP BY collection
ORDER BY listings DESC
LIMIT 12
```

```sql depositor_activity
SELECT
  JSONExtractString(fields_json, 'depositor') AS depositor,
  count() AS listings,
  uniqExact(JSONExtractString(fields_json, 'collection')) AS collections,
  sum(toFloat64OrZero(JSONExtractString(fields_json, 'value'))) / 1e18 AS backing_eth
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0 AND event_name = 'NFTListed'
GROUP BY depositor
ORDER BY listings DESC
LIMIT 15
```

## The listing side has meaningful breadth

The index currently contains **{% value data="lifecycle" value="max(listed)" fmt="num0" /%} NFT listings** from **{% value data="participation" value="max(depositors)" fmt="num0" /%} depositors**, spanning **{% value data="participation" value="max(collections)" fmt="num0" /%} collections**. Those listings carry approximately **{% value data="participation" value="max(listed_eth)" fmt="num1" /%} ETH** of declared backing.

This breadth matters: randomized acquisition is more compelling when inventory is not concentrated in one collection or one depositor. The table below makes that concentration visible rather than treating total listing count as sufficient.

{% table data="collection_activity" limit=12 /%}

## Acquisition and settlement funnel

{% row %}
  {% big_value data="lifecycle" value="max(requested)" title="Acquisitions requested" fmt="num0" /%}
  {% big_value data="lifecycle" value="max(allocated)" title="NFTs allocated" fmt="num0" /%}
  {% big_value data="lifecycle" value="max(kept)" title="NFTs kept" fmt="num0" /%}
  {% big_value data="lifecycle" value="max(bids_accepted)" title="Depositor bids accepted" fmt="num0" /%}
{% /row %}

The distinction between allocation, keeping the NFT, and accepting the depositor's standing bid is the protocol's central behavioral question. A complete historical index lets us measure that funnel directly instead of inferring it from token transfers.

## Who supplies the inventory?

{% table data="depositor_activity" limit=15 /%}

Concentration among depositors is a useful risk and growth signal. If a small number of addresses supply most listings or backing, headline inventory can overstate the depth of the marketplace.

{% callout type="info" title="Backfill status" %}
This report refreshes every minute while Streamling Blockchain Studio continues indexing from the deployment block. Counts should be treated as provisional until the index reaches the Ethereum tip.
{% /callout %}

[Back to the live dashboard →](/)
