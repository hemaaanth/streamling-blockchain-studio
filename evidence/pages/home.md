---
title: Streamling Blockchain Studio — FWA Dashboard
page_width: full
cards: true
table_of_contents: false
auto_refresh: 60000
sidebar_position: 1
icon: chart-column
---

# Fake World Assets

A live view of protocol activity decoded from Ethereum events by Streamling Blockchain Studio. The project is still backfilling; values rise as historical blocks arrive.

```sql overview
SELECT
  count() AS total_events,
  uniqExact(event_name) AS event_types,
  min(toDateTime(block_timestamp)) AS first_activity,
  max(toDateTime(block_timestamp)) AS latest_activity
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0
```

```sql nft_activity
SELECT
  toDate(toDateTime(block_timestamp)) AS activity_date,
  countIf(event_name = 'NFTListed') AS listings,
  countIf(event_name = 'ListingWithdrawn') AS withdrawals,
  countIf(event_name = 'NFTAllocated') AS allocations,
  countIf(event_name = 'NFTKept') AS kept
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0
GROUP BY activity_date
ORDER BY activity_date
```

```sql nft_kpis
SELECT
  count() AS listings,
  uniqExact(JSONExtractString(fields_json, 'depositor')) AS depositors,
  uniqExact(JSONExtractString(fields_json, 'collection')) AS collections,
  sum(toFloat64OrZero(JSONExtractString(fields_json, 'value'))) / 1e18 AS listed_eth
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0 AND event_name = 'NFTListed'
```

```sql event_mix
SELECT event_name, count() AS events
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0
GROUP BY event_name
ORDER BY events DESC
LIMIT 12
```

```sql recent_listings
SELECT
  toDateTime(block_timestamp) AS listed_at,
  JSONExtractString(fields_json, 'depositor') AS depositor,
  JSONExtractString(fields_json, 'collection') AS collection,
  JSONExtractString(fields_json, 'tokenId') AS token_id,
  round(toFloat64OrZero(JSONExtractString(fields_json, 'value')) / 1e18, 4) AS backing_eth,
  tx_hash
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0 AND event_name = 'NFTListed'
ORDER BY block_number DESC, log_index DESC
LIMIT 20
```

{% row %}
  {% big_value data="overview" value="max(total_events)" title="Decoded events" fmt="num0" /%}
  {% big_value data="overview" value="max(event_types)" title="Event types" fmt="num0" /%}
  {% big_value data="nft_kpis" value="max(listings)" title="NFT listings" fmt="num0" /%}
  {% big_value data="nft_kpis" value="max(listed_eth)" title="Listed backing" fmt="num1" /%}
{% /row %}

{% row %}
  {% area_chart
    data="nft_activity"
    x="activity_date"
    y="sum(listings)"
    title="NFT listings over time"
  /%}
  {% horizontal_bar_chart
    data="event_mix"
    x="sum(events)"
    y="event_name"
    title="Protocol event mix"
    order="sum(events) desc"
    limit=12
  /%}
{% /row %}

## Latest NFT listings

{% table data="recent_listings" limit=20 /%}

[Read the protocol narrative →](/protocol-report)
