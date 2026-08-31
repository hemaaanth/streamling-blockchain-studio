SELECT
  id,
  checked_at,
  label,
  from_block,
  to_block,
  ok,
  chain_events,
  stored_events,
  missing_count,
  extra_count,
  changed_count,
  detail_json
FROM quality_replay_checks
