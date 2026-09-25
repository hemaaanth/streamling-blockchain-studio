#!/usr/bin/env python3
import argparse
import base64
import json
import os
import sqlite3
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path

DATABASE = "megapot_analytics"
CLI = Path(__file__).resolve().parents[2] / "target" / "release" / "streamling-blockchain"

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
    # The SQLite sink has no discovery columns; Megapot has no discovery rules.
    "NULL AS discovered_address",
    "NULL AS discovery_rule",
)


def schema_statements(cli: Path) -> list[str]:
    """The Streamling ClickHouse sink DDL, printed by the CLI so it has one source."""
    output = subprocess.run(
        [str(cli), "clickhouse", "schema", "--database", DATABASE, "--table", "events", "--json"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(output)["data"]


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
    parser.add_argument("--cli", type=Path, default=CLI, help="Path to the built streamling-blockchain CLI")
    args = parser.parse_args()
    user = args.user
    password = os.environ.get("STREAMLING__CLICKHOUSE_SINK__PASSWORD", "clickhouse")

    for statement in schema_statements(args.cli):
        post(args.clickhouse, statement, user, password)
    connection = sqlite3.connect(f"file:{args.database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    cursor = connection.execute(f"SELECT {', '.join(COLUMNS)} FROM events ORDER BY block_number, log_index")
    insert = f"INSERT INTO {DATABASE}.events (event_id, chain_id, contract_alias, event_name, address, block_number, block_hash, block_timestamp, tx_hash, log_index, topic0, fields_json, discovered_address, discovery_rule, is_deleted) FORMAT JSONEachRow"
    synced = 0
    while rows := cursor.fetchmany(args.batch_size):
        payload = bytearray()
        for row in rows:
            record = dict(row)
            record["is_deleted"] = 0
            payload.extend(json.dumps(record, separators=(",", ":")).encode())
            payload.append(10)
        post(args.clickhouse, insert, user, password, bytes(payload))
        synced += len(rows)
    print(f"synced {synced} events")


if __name__ == "__main__":
    main()
