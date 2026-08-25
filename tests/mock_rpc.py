#!/usr/bin/env python3
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

PARENT = "0x1111111111111111111111111111111111111111"
DISCOVERED = "0x2222222222222222222222222222222222222222"
RECIPIENT = "0x3333333333333333333333333333333333333333"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
BLOCK_HASH = "0x" + "ab" * 32
TX1 = "0x" + "01" * 32
TX2 = "0x" + "02" * 32

def word_address(address):
    return "0x" + "0" * 24 + address[2:]

def word_int(value):
    return "0x" + format(value, "064x")

LOGS = [
    {"address": PARENT, "topics": [TRANSFER, word_address("0x" + "0" * 40), word_address(DISCOVERED)], "data": word_int(1), "blockNumber": "0x64", "blockHash": BLOCK_HASH, "transactionHash": TX1, "logIndex": "0x0"},
    {"address": DISCOVERED, "topics": [TRANSFER, word_address("0x" + "0" * 40), word_address(RECIPIENT)], "data": word_int(2), "blockNumber": "0x64", "blockHash": BLOCK_HASH, "transactionHash": TX2, "logIndex": "0x1"},
]

BLOCK = {
    "number": "0x64",
    "hash": BLOCK_HASH,
    "parentHash": "0x" + "cd" * 32,
    "timestamp": "0x66",
    "miner": "0x4444444444444444444444444444444444444444",
    "gasLimit": "0x1c9c380",
    "gasUsed": "0x5208",
    "baseFeePerGas": "0x3b9aca00",
    "transactions": [
        {"hash": TX1, "blockNumber": "0x64", "blockHash": BLOCK_HASH, "transactionIndex": "0x0", "from": "0x5555555555555555555555555555555555555555", "to": PARENT, "value": "0x0", "gas": "0x5208", "gasPrice": "0x3b9aca00", "input": "0x", "nonce": "0x1"},
        {"hash": TX2, "blockNumber": "0x64", "blockHash": BLOCK_HASH, "transactionIndex": "0x1", "from": "0x6666666666666666666666666666666666666666", "to": DISCOVERED, "value": "0x0", "gas": "0x5208", "gasPrice": "0x3b9aca00", "input": "0x", "nonce": "0x2"},
    ],
}
RECEIPTS = {
    TX1: {"transactionHash": TX1, "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x3b9aca00", "contractAddress": None, "logs": [LOGS[0]]},
    TX2: {"transactionHash": TX2, "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x3b9aca00", "contractAddress": None, "logs": [LOGS[1]]},
}

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("content-length", "0"))
        request = json.loads(self.rfile.read(length))
        method = request["method"]
        if method == "eth_chainId": result = "0x1"
        elif method == "eth_getCode": result = "0x6000"
        elif method == "eth_blockNumber": result = "0x65"
        elif method == "eth_getBlockByNumber":
            full = bool(request["params"][1])
            result = dict(BLOCK)
            if not full:
                result["transactions"] = [TX1, TX2]
        elif method == "eth_getBlockReceipts": result = list(RECEIPTS.values())
        elif method == "eth_getTransactionReceipt": result = RECEIPTS.get(request["params"][0])
        elif method == "eth_getLogs":
            query = request["params"][0]
            start = int(query["fromBlock"], 16)
            end = int(query["toBlock"], 16)
            result = LOGS if start <= 100 <= end else []
        else:
            self.send_error(400, f"unsupported method {method}")
            return
        body = json.dumps({"jsonrpc": "2.0", "id": request.get("id"), "result": result}).encode()
        self.send_response(200); self.send_header("content-type", "application/json"); self.send_header("content-length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def log_message(self, *_): pass

HTTPServer(("127.0.0.1", 18545), Handler).serve_forever()
