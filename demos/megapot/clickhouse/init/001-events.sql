CREATE DATABASE IF NOT EXISTS megapot_analytics;

CREATE TABLE IF NOT EXISTS megapot_analytics.events (
  event_id String,
  chain_id UInt64,
  contract_alias LowCardinality(String),
  event_name LowCardinality(String),
  address String,
  block_number UInt64,
  block_hash String,
  block_timestamp UInt64,
  tx_hash String,
  log_index UInt64,
  topic0 String,
  fields_json String,
  discovered_address Nullable(String),
  discovery_rule Nullable(String),
  insert_time DateTime64(3),
  is_deleted UInt8
)
ENGINE = ReplacingMergeTree(insert_time, is_deleted)
ORDER BY event_id;
