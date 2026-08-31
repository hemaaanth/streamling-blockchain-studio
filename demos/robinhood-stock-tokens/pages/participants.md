---
title: Participants
page_width: full
cards: true
table_of_contents: true
auto_refresh: 60000
sidebar_position: 4
icon: users
---

Wallet-level sender and recipient views across indexed Robinhood Stock Token transfers.

```sql participant_totals
SELECT
  count(DISTINCT json_extract_string(fields_json, '$.from')) AS senders,
  count(DISTINCT json_extract_string(fields_json, '$.to')) AS recipients,
  count(DISTINCT tx_hash) AS transactions,
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols
FROM events
WHERE is_deleted = 0
```

```sql sender_leaders
SELECT
  concat(substr(json_extract_string(fields_json, '$.from'), 1, 6), '…', substr(json_extract_string(fields_json, '$.from'), -4)) AS sender,
  concat('https://robinhoodchain.blockscout.com/address/', json_extract_string(fields_json, '$.from')) AS sender_url,
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols,
  count(DISTINCT tx_hash) AS transactions,
  round(sum(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0)) / 1e18, 2) AS units_sent
FROM events
WHERE is_deleted = 0
  AND json_extract_string(fields_json, '$.from') != '0x0000000000000000000000000000000000000000'
GROUP BY sender_url, sender
ORDER BY transfers DESC
LIMIT 25
```

```sql recipient_leaders
SELECT
  concat(substr(json_extract_string(fields_json, '$.to'), 1, 6), '…', substr(json_extract_string(fields_json, '$.to'), -4)) AS recipient,
  concat('https://robinhoodchain.blockscout.com/address/', json_extract_string(fields_json, '$.to')) AS recipient_url,
  count(*) AS transfers,
  count(DISTINCT contract_alias) AS symbols,
  count(DISTINCT tx_hash) AS transactions,
  round(sum(coalesce(try_cast(json_extract_string(fields_json, '$.value') AS DOUBLE), 0)) / 1e18, 2) AS units_received
FROM events
WHERE is_deleted = 0
  AND json_extract_string(fields_json, '$.to') != '0x0000000000000000000000000000000000000000'
GROUP BY recipient_url, recipient
ORDER BY transfers DESC
LIMIT 25
```

```sql wallet_symbol_mix
WITH parties AS (
  SELECT json_extract_string(fields_json, '$.from') AS wallet, contract_alias
  FROM events
  WHERE is_deleted = 0 AND json_extract_string(fields_json, '$.from') != '0x0000000000000000000000000000000000000000'
  UNION ALL
  SELECT json_extract_string(fields_json, '$.to') AS wallet, contract_alias
  FROM events
  WHERE is_deleted = 0 AND json_extract_string(fields_json, '$.to') != '0x0000000000000000000000000000000000000000'
)
SELECT
  concat(substr(wallet, 1, 6), '…', substr(wallet, -4)) AS wallet,
  concat('https://robinhoodchain.blockscout.com/address/', wallet) AS wallet_url,
  count(*) AS touches,
  count(DISTINCT contract_alias) AS symbols
FROM parties
GROUP BY wallet_url, wallet
ORDER BY symbols DESC, touches DESC
LIMIT 25
```

<Grid cols=5>
  <BigValue data={participant_totals} value="senders" title="Senders" fmt="num0" />
  <BigValue data={participant_totals} value="recipients" title="Recipients" fmt="num0" />
  <BigValue data={participant_totals} value="transactions" title="Transactions" fmt="num0" />
  <BigValue data={participant_totals} value="transfers" title="Transfers" fmt="num0" />
  <BigValue data={participant_totals} value="symbols" title="Symbols" fmt="num0" />
</Grid>

## Senders

<DataTable data={sender_leaders} rows=25 rowShading=true sortable=true search=true downloadable=true>
  <Column id="sender_url" title="Sender" contentType="link" linkLabel="sender" openInNewTab=true />
  <Column id="transfers" title="Transfers" contentType="bar" fmt="num0" barColor="#2563eb" />
  <Column id="symbols" title="Symbols" fmt="num0" />
  <Column id="transactions" title="Txs" fmt="num0" />
  <Column id="units_sent" title="Units sent" fmt="num2" />
</DataTable>

## Recipients

<DataTable data={recipient_leaders} rows=25 rowShading=true sortable=true search=true downloadable=true>
  <Column id="recipient_url" title="Recipient" contentType="link" linkLabel="recipient" openInNewTab=true />
  <Column id="transfers" title="Transfers" contentType="bar" fmt="num0" barColor="#16a34a" />
  <Column id="symbols" title="Symbols" fmt="num0" />
  <Column id="transactions" title="Txs" fmt="num0" />
  <Column id="units_received" title="Units received" fmt="num2" />
</DataTable>

## Cross-symbol wallets

<DataTable data={wallet_symbol_mix} rows=25 rowShading=true sortable=true search=true downloadable=true>
  <Column id="wallet_url" title="Wallet" contentType="link" linkLabel="wallet" openInNewTab=true />
  <Column id="symbols" title="Symbols" contentType="bar" fmt="num0" barColor="#f59e0b" />
  <Column id="touches" title="Touches" fmt="num0" />
</DataTable>
