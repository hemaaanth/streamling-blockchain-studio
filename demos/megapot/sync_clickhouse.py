#!/usr/bin/env python3
import argparse
import base64
import json
import os
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timezone

DATABASE = "megapot_analytics"
SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.events (
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
) ENGINE = ReplacingMergeTree(insert_time, is_deleted)
ORDER BY event_id
"""

COLUMNS = (
    "event_id",
    "chain_id",
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


def post(url: str, query: str, user: str, password: str, body: bytes = b"") -> bytes:
    params = urllib.parse.urlencode({"query": query})
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    request = urllib.request.Request(
        f"{url.rstrip('/')}/?{params}",
        data=body,
        method="POST",
        headers={"Authorization": f"Basic {token}"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return response.read()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Streamling Blockchain Studio SQLite events into ClickHouse")
    parser.add_argument("database", help="Path to the Streamling Blockchain Studio SQLite database")
    parser.add_argument("--clickhouse", default="http://127.0.0.1:8123")
    parser.add_argument("--user", default=os.environ.get("STREAMLING__CLICKHOUSE_SINK__USER", "default"))
    parser.add_argument("--batch-size", type=int, default=5_000)
    args = parser.parse_args()
    user = args.user
    password = os.environ.get("STREAMLING__CLICKHOUSE_SINK__PASSWORD", "clickhouse")

    post(args.clickhouse, f"CREATE DATABASE IF NOT EXISTS {DATABASE}", user, password)
    post(args.clickhouse, SCHEMA, user, password)
    connection = sqlite3.connect(f"file:{args.database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    cursor = connection.execute(f"SELECT {', '.join(COLUMNS)} FROM events ORDER BY block_number, log_index")
    insert = f"INSERT INTO {DATABASE}.events (event_id, chain_id, contract_alias, event_name, address, block_number, block_hash, block_timestamp, tx_hash, log_index, topic0, fields_json, discovered_address, discovery_rule, insert_time, is_deleted) FORMAT JSONEachRow"
    synced = 0
    while rows := cursor.fetchmany(args.batch_size):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        payload = bytearray()
        for row in rows:
            record = dict(row)
            record.update({"insert_time": now, "is_deleted": 0})
            payload.extend(json.dumps(record, separators=(",", ":")).encode())
            payload.append(10)
        post(args.clickhouse, insert, user, password, bytes(payload))
        synced += len(rows)
    print(f"synced {synced} events")


if __name__ == "__main__":
    main()
