import hashlib
import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


initializer = load("megapot_init_project", SCRIPTS / "init_project.py")
sync = load("megapot_sync_clickhouse", Path(__file__).resolve().parents[1] / "sync_clickhouse.py")


class InitializerTests(unittest.TestCase):
    def test_explicit_block_zero_is_not_replaced_by_defaults(self) -> None:
        with mock.patch.object(initializer, "rpc", side_effect=AssertionError("RPC should not run")):
            selected = initializer.select_range(
                0,
                0,
                {"startBlock": 43_197_068, "confirmations": 12},
            )
        self.assertEqual(selected, (0, 0))

    def test_abi_checksum_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "abi.json"
            path.write_bytes(b"[]")
            contract = {
                "alias": "jackpot",
                "abiSha256": hashlib.sha256(b"different").hexdigest(),
            }
            with self.assertRaisesRegex(SystemExit, "checksum changed for jackpot"):
                initializer.verify_abi(path, contract)


class ClickHouseSyncTests(unittest.TestCase):
    def test_selects_only_columns_the_sqlite_sink_writes(self) -> None:
        connection = sqlite3.connect(":memory:")
        # Mirrors the events table created by the Streamling SQLite sink.
        connection.execute(
            "CREATE TABLE events (event_id TEXT PRIMARY KEY, chain_id INTEGER NOT NULL, "
            "contract_alias TEXT NOT NULL, event_name TEXT NOT NULL, address TEXT NOT NULL, "
            "block_number INTEGER NOT NULL, block_hash TEXT NOT NULL, block_timestamp INTEGER NOT NULL, "
            "tx_hash TEXT NOT NULL, log_index INTEGER NOT NULL, topic0 TEXT NOT NULL, data TEXT NOT NULL)"
        )
        cursor = connection.execute(f"SELECT {', '.join(sync.COLUMNS)} FROM events")
        self.assertIn("discovered_address", [column[0] for column in cursor.description])

    def test_schema_comes_from_the_cli(self) -> None:
        completed = mock.MagicMock(
            stdout='{"schema_version": 1, "ok": true, "command": "clickhouse schema", '
            '"data": ["CREATE DATABASE IF NOT EXISTS megapot_analytics"], "warnings": [], "error": null}'
        )
        with mock.patch.object(sync.subprocess, "run", return_value=completed) as run:
            statements = sync.schema_statements(Path("/bin/streamling-blockchain"))
        self.assertEqual(statements, ["CREATE DATABASE IF NOT EXISTS megapot_analytics"])
        self.assertEqual(
            run.call_args.args[0],
            ["/bin/streamling-blockchain", "clickhouse", "schema", "--database", "megapot_analytics", "--table", "events", "--json"],
        )

    def test_password_is_sent_in_authorization_header_not_url(self) -> None:
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b"ok"
        with mock.patch.object(sync.urllib.request, "urlopen", return_value=response) as urlopen:
            sync.post("https://clickhouse.example", "SELECT 1", "alice", "top-secret")

        request = urlopen.call_args.args[0]
        self.assertNotIn("alice", request.full_url)
        self.assertNotIn("top-secret", request.full_url)
        self.assertTrue(request.get_header("Authorization").startswith("Basic "))


if __name__ == "__main__":
    unittest.main()
