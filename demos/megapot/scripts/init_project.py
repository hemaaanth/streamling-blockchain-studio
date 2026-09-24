#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import urllib.request
from pathlib import Path

DEMO_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST = json.loads((DEMO_ROOT / "contracts.json").read_text())
CLI = REPO_ROOT / "target" / "release" / "streamling-blockchain"
RPC_ENV = "BASE_RPC_URL"
CLICKHOUSE_DATABASE = "megapot_analytics"


def rpc(method: str) -> int:
    request = urllib.request.Request(
        os.environ[RPC_ENV],
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": []}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "streamling-blockchain-megapot/1.0"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        payload = json.load(response)
    if "error" in payload:
        raise RuntimeError(f"{method} failed: {payload['error'].get('message', 'RPC error')}")
    return int(payload["result"], 16)


def run(*args: object) -> None:
    subprocess.run([str(arg) for arg in args], check=True)

def select_range(
    start_override: int | None,
    end_override: int | None,
    chain: dict,
) -> tuple[int, int]:
    start_block = chain["startBlock"] if start_override is None else start_override
    end_block = (
        rpc("eth_blockNumber") - chain["confirmations"]
        if end_override is None
        else end_override
    )
    return start_block, end_block


def verify_abi(path: Path, contract: dict) -> None:
    actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual_hash != contract["abiSha256"]:
        raise SystemExit(
            f"verified ABI checksum changed for {contract['alias']}: "
            f"expected {contract['abiSha256']}, got {actual_hash}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize the reproducible Megapot Base project")
    parser.add_argument("project", type=Path)
    parser.add_argument("--sink", choices=("sqlite", "clickhouse", "both"), required=True)
    parser.add_argument("--start-block", type=int)
    parser.add_argument("--end-block", type=int)
    args = parser.parse_args()

    if not CLI.is_file():
        raise SystemExit(f"missing {CLI}; run cargo build --release --workspace first")
    if not os.environ.get(RPC_ENV):
        raise SystemExit(f"set {RPC_ENV} to a Base mainnet RPC URL")
    if args.project.exists() and any(args.project.iterdir()):
        raise SystemExit(f"project directory is not empty: {args.project}")

    chain = MANIFEST["chain"]
    actual_chain_id = rpc("eth_chainId")
    if actual_chain_id != chain["id"]:
        raise SystemExit(
            f"{RPC_ENV} uses chain ID {actual_chain_id}; Megapot requires Base chain ID {chain['id']}"
        )
    start_block, end_block = select_range(
        args.start_block,
        args.end_block,
        chain,
    )
    if end_block < start_block:
        raise SystemExit(f"end block {end_block} precedes start block {start_block}")

    with tempfile.TemporaryDirectory(prefix="megapot-abis-") as temporary:
        abi_paths = {}
        for contract in MANIFEST["contracts"]:
            abi_path = Path(temporary) / f"{contract['alias']}.json"
            run(
                CLI,
                "abi",
                "fetch",
                contract["address"],
                "--chain-id",
                chain["id"],
                "--source",
                MANIFEST["abi"]["source"],
                "--blockscout-base-url",
                MANIFEST["abi"]["baseUrl"],
                "--out",
                abi_path,
            )
            verify_abi(abi_path, contract)
            abi_paths[contract["alias"]] = abi_path

        first, *additional = MANIFEST["contracts"]
        command = [
            CLI,
            "--project",
            args.project,
            "init",
            first["address"],
            "--alias",
            first["alias"],
            "--rpc-env",
            RPC_ENV,
            "--abi",
            abi_paths[first["alias"]],
            "--start-block",
            start_block,
            "--end-block",
            end_block,
            "--confirmations",
            chain["confirmations"],
            "--window",
            chain["window"],
            "--sink",
            args.sink,
        ]
        if args.sink in ("clickhouse", "both"):
            command.extend(
                [
                    "--clickhouse-database",
                    CLICKHOUSE_DATABASE,
                    "--clickhouse-table",
                    "events",
                ]
            )
        run(*command)

        for contract in additional:
            run(
                CLI,
                "--project",
                args.project,
                "add-contract",
                contract["address"],
                "--alias",
                contract["alias"],
                "--abi",
                abi_paths[contract["alias"]],
            )

    print(
        f"initialized {args.sink} project for Base blocks "
        f"{start_block}..{end_block}: {args.project}"
    )


if __name__ == "__main__":
    main()
