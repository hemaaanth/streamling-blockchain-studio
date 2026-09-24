---
title: Data room
page_width: full
cards: true
table_of_contents: false
sidebar_position: 5
icon: database
---

# Data room

Decoded contract events for traceability, QA, and follow-up analysis. Factory events come from the verified Pons V2 deployment; curve events are retained only for curves discovered from `TokenLaunched`.

```sql coverage
SELECT
  count(*) AS decoded_events,
  count(DISTINCT tx_hash) AS transactions,
  count(DISTINCT address) AS contracts,
  min(block_number) AS first_block,
  max(block_number) AS latest_block,
  count(DISTINCT block_number) AS event_blocks
FROM events
WHERE is_deleted = 0
```

```sql event_mix
SELECT
  contract_alias,
  event_name,
  count(*) AS events,
  round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS share
FROM events
WHERE is_deleted = 0
GROUP BY contract_alias, event_name
ORDER BY events DESC
```

```sql contract_activity
SELECT
  CASE WHEN contract_alias = 'pons_factory' THEN 'Factory' ELSE 'Bonding curve' END AS contract_type,
  count(DISTINCT address) AS contracts,
  count(*) AS events,
  count(DISTINCT tx_hash) AS transactions,
  min(block_number) AS first_block,
  max(block_number) AS latest_block
FROM events
WHERE is_deleted = 0
GROUP BY contract_alias
ORDER BY events DESC
```

```sql raw_events
SELECT
  epoch_ms(CAST(block_timestamp * 1000 AS BIGINT)) AS event_time,
  contract_alias,
  event_name,
  substr(address, 1, 8) || '…' || substr(address, -6) AS contract_label,
  'https://robinhoodchain.blockscout.com/address/' || address AS contract_url,
  block_number,
  log_index,
  'https://robinhoodchain.blockscout.com/tx/' || tx_hash AS tx_url,
  fields_json
FROM events
WHERE is_deleted = 0
ORDER BY block_number DESC, log_index DESC
LIMIT 250
```

<Grid cols=5>
  <BigValue data={coverage} value="decoded_events" title="Decoded events" fmt="num0" />
  <BigValue data={coverage} value="transactions" title="Transactions" fmt="num0" />
  <BigValue data={coverage} value="contracts" title="Contracts" fmt="num0" />
  <BigValue data={coverage} value="first_block" title="First block" fmt="num0" />
  <BigValue data={coverage} value="latest_block" title="Latest block" fmt="num0" />
</Grid>

<Grid cols=2>
  <BarChart data={event_mix} x="event_name" y="events" series="contract_alias" title="Event mix" emptySet=pass />
  <DataTable data={contract_activity} rows=10 rowShading=true sortable=true emptySet=pass>
    <Column id="contract_type" title="Contract type" chip=true />
    <Column id="contracts" title="Contracts" fmt="num0" />
    <Column id="events" title="Events" contentType="bar" fmt="num0" barColor="#7c3aed" />
    <Column id="transactions" title="Transactions" fmt="num0" />
    <Column id="first_block" title="First" fmt="num0" />
    <Column id="latest_block" title="Latest" fmt="num0" />
  </DataTable>
</Grid>

## Latest decoded events

<DataTable data={raw_events} rows=50 rowShading=true sortable=true search=true downloadable=true compact=true emptySet=pass>
  <Column id="event_time" title="Time" fmt="longdate" />
  <Column id="contract_alias" title="Source" chip=true />
  <Column id="event_name" title="Event" chip=true />
  <Column id="contract_url" title="Contract" contentType="link" linkLabel="contract_label" openInNewTab=true />
  <Column id="block_number" title="Block" fmt="num0" />
  <Column id="log_index" title="Log" fmt="num0" />
  <Column id="tx_url" title="Tx" contentType="link" linkLabel="open" openInNewTab=true />
  <Column id="fields_json" title="Decoded fields" />
</DataTable>