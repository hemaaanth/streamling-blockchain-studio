#!/usr/bin/env python3
import argparse
import json
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS fwa_analytics.events (
  event_id String,
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
) ENGINE = ReplacingMergeTree(insert_time, is_deleted)
ORDER BY event_id
"""

COLUMNS = (
    "event_id",
    "contract_alias",
    "event_name",
    "address",
    "block_number",
    "block_hash",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "topic0",
    "data AS fields_json",
    "discovered_address",
    "discovery_rule",
)


def post(url: str, query: str, body: bytes = b"") -> bytes:
    endpoint = f"{url}/?query={urllib.parse.quote(query)}"
    request = urllib.request.Request(endpoint, data=body, method="POST")
    with urllib.request.urlopen(request) as response:
        return response.read()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Streamling Blockchain Studio SQLite events into ClickHouse")
    parser.add_argument("database", help="Path to the Streamling Blockchain Studio SQLite database")
    parser.add_argument("--clickhouse", default="http://127.0.0.1:8123")
    parser.add_argument("--batch-size", type=int, default=5_000)
    args = parser.parse_args()

    post(args.clickhouse, "CREATE DATABASE IF NOT EXISTS fwa_analytics")
    post(args.clickhouse, SCHEMA)
    connection = sqlite3.connect(f"file:{args.database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    cursor = connection.execute(f"SELECT {', '.join(COLUMNS)} FROM events ORDER BY block_number, log_index")
    insert = "INSERT INTO fwa_analytics.events (event_id, contract_alias, event_name, address, block_number, block_hash, block_timestamp, tx_hash, log_index, topic0, fields_json, discovered_address, discovery_rule, insert_time, is_deleted) FORMAT JSONEachRow"
    synced = 0
    while rows := cursor.fetchmany(args.batch_size):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        payload = bytearray()
        for row in rows:
            record = dict(row)
            record.update({"insert_time": now, "is_deleted": 0})
            payload.extend(json.dumps(record, separators=(",", ":")).encode())
            payload.append(10)
        post(args.clickhouse, insert, bytes(payload))
        synced += len(rows)
    print(f"synced {synced} events")


if __name__ == "__main__":
    main()
