SELECT
  event_id,
  chain_id,
  contract_alias,
  event_name,
  address,
  block_number,
  block_hash,
  block_timestamp,
  tx_hash,
  log_index,
  topic0,
  data AS fields_json,
  '' AS discovered_address,
  '' AS discovery_rule,
  '' AS insert_time,
  0 AS is_deleted
FROM events
