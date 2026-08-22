use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde_json::{Value, json};

pub const BUILTIN_CHAINS: &[(&str, u64, &str)] = &[
    ("ethereum", 1, "https://ethereum-rpc.publicnode.com"),
    (
        "arbitrum-one",
        42161,
        "https://arbitrum-one-rpc.publicnode.com",
    ),
    ("base", 8453, "https://base-rpc.publicnode.com"),
];

pub async fn rpc(client: &Client, url: &str, method: &str, params: Value) -> Result<Value> {
    let response = client
        .post(url)
        .json(&json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params
        }))
        .send()
        .await
        .with_context(|| format!("{method} request to {url}"))?;
    let status = response.status();
    let body: Value = response.json().await.context("decode JSON-RPC response")?;
    if !status.is_success() {
        bail!("{method} returned HTTP {status}: {body}")
    }
    if let Some(error) = body.get("error") {
        bail!("{method} failed: {error}")
    }
    body.get("result")
        .cloned()
        .context("JSON-RPC response omitted result")
}

pub async fn detect_chain(
    client: &Client,
    address: &str,
    explicit_rpc: Option<&str>,
) -> Result<(String, u64, String)> {
    if let Some(url) = explicit_rpc {
        let id = hex_u64(&rpc(client, url, "eth_chainId", json!([])).await?)?;
        let name = BUILTIN_CHAINS
            .iter()
            .find(|(_, chain_id, _)| *chain_id == id)
            .map(|(name, _, _)| (*name).to_owned())
            .unwrap_or_else(|| format!("chain-{id}"));
        ensure_contract(client, url, address).await?;
        return Ok((name, id, url.to_owned()));
    }
    for (name, id, url) in BUILTIN_CHAINS {
        if ensure_contract(client, url, address).await.is_ok() {
            return Ok(((*name).to_owned(), *id, (*url).to_owned()));
        }
    }
    bail!("contract bytecode was not found on Ethereum, Arbitrum One, or Base; pass --rpc")
}

async fn ensure_contract(client: &Client, url: &str, address: &str) -> Result<()> {
    let code = rpc(client, url, "eth_getCode", json!([address, "latest"])).await?;
    match code.as_str() {
        Some("0x" | "0x0") | None => bail!("no contract bytecode"),
        Some(_) => Ok(()),
    }
}

pub async fn latest_block(client: &Client, url: &str) -> Result<u64> {
    hex_u64(&rpc(client, url, "eth_blockNumber", json!([])).await?)
}

pub async fn deployment_block(client: &Client, url: &str, address: &str) -> Result<u64> {
    let mut lo = 0;
    let mut hi = latest_block(client, url).await?;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let block = format!("0x{mid:x}");
        let has_code = rpc(client, url, "eth_getCode", json!([address, block]))
            .await
            .ok()
            .and_then(|v| v.as_str().map(|s| s != "0x" && s != "0x0"))
            .unwrap_or(false);
        if has_code { hi = mid } else { lo = mid + 1 }
    }
    Ok(lo)
}

pub async fn fetch_abi(client: &Client, chain_id: u64, address: &str) -> Result<Value> {
    for match_kind in ["full_match", "partial_match"] {
        let url = format!(
            "https://repo.sourcify.dev/contracts/{match_kind}/{chain_id}/{address}/metadata.json"
        );
        let response = client.get(&url).send().await?;
        if response.status().is_success() {
            let metadata: Value = response.json().await?;
            if let Some(abi) = metadata.pointer("/output/abi") {
                return Ok(abi.clone());
            }
        }
    }
    bail!("ABI not found on Sourcify; pass --abi <file>")
}

pub fn hex_u64(value: &Value) -> Result<u64> {
    let text = value.as_str().context("expected hexadecimal JSON string")?;
    u64::from_str_radix(text.trim_start_matches("0x"), 16).context("invalid hexadecimal integer")
}
