SELECT
  event_id,
  contract_alias,
  event_name,
  address,
  block_number,
  block_hash,
  block_timestamp,
  tx_hash,
  log_index,
  topic0,
  fields_json,
  discovered_address,
  discovery_rule,
  insert_time,
  is_deleted
FROM fwa_analytics.events FINAL
WHERE is_deleted = 0
