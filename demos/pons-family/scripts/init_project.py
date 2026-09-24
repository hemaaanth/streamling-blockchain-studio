#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import urllib.request
from pathlib import Path

DEMO_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = DEMO_ROOT.parents[1]
MANIFEST = json.loads((DEMO_ROOT / "contracts.json").read_text())
CLI = REPO_ROOT / "target" / "release" / "streamling-blockchain"
DEFAULT_PROJECT = REPO_ROOT / ".local" / "demos" / "pons-family"
RPC_ENV = "ROBINHOOD_RPC_URL"


def rpc(rpc_url: str, method: str) -> int:
    request = urllib.request.Request(
        rpc_url,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": []}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "streamling-blockchain-pons/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if "error" in payload:
        raise SystemExit(f"{method} failed: {payload['error'].get('message', 'RPC error')}")
    return int(payload["result"], 16)


def run(*args: object) -> None:
    subprocess.run([str(arg) for arg in args], cwd=REPO_ROOT, check=True)


def main() -> None:
    chain = MANIFEST["chain"]
    factory = MANIFEST["factory"]
    discovery = MANIFEST["discovery"]
    parser = argparse.ArgumentParser(description="Initialize the Pons Family V2 Streamling project")
    parser.add_argument("--project", type=Path, default=DEFAULT_PROJECT)
    parser.add_argument("--start-block", type=int, default=chain["startBlock"])
    parser.add_argument("--end-block", type=int)
    args = parser.parse_args()

    if not CLI.is_file():
        raise SystemExit(f"missing {CLI}; run cargo build --release --workspace first")
    project = args.project.resolve()
    if project.exists() and any(project.iterdir()):
        raise SystemExit(f"project directory is not empty: {project}")

    rpc_url = os.environ.get(RPC_ENV, chain["rpc"])
    actual_chain_id = rpc(rpc_url, "eth_chainId")
    if actual_chain_id != chain["id"]:
        raise SystemExit(f"RPC uses chain ID {actual_chain_id}; Pons Family requires Robinhood Chain ID {chain['id']}")

    end_block = rpc(rpc_url, "eth_blockNumber") - chain["confirmations"] if args.end_block is None else args.end_block
    if end_block < args.start_block:
        raise SystemExit(f"end block {end_block} precedes start block {args.start_block}")

    rpc_args = ["--rpc-env", RPC_ENV] if RPC_ENV in os.environ else ["--rpc", rpc_url]
    command = [
        CLI,
        "--project",
        project,
        "init",
        factory["address"],
        "--alias",
        factory["alias"],
        *rpc_args,
        "--abi",
        DEMO_ROOT / factory["abi"],
        "--start-block",
        args.start_block,
        "--confirmations",
        chain["confirmations"],
        "--window",
        chain["window"],
        "--end-block",
        end_block,
    ]
    run(*command)
    run(
        CLI,
        "--project",
        project,
        "add-discovery",
        "--parent-contract",
        factory["alias"],
        "--discovery-event",
        discovery["event"],
        "--address-field",
        discovery["addressField"],
        "--child-contract",
        discovery["childAlias"],
        "--child-abi",
        DEMO_ROOT / discovery["childAbi"],
    )
    print(f"initialized Pons Family project for Robinhood Chain blocks {args.start_block}..{end_block}: {project}")


if __name__ == "__main__":
    main()
