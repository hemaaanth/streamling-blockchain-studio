use crate::{config::ProjectConfig, rpc};
use anyhow::Result;
use reqwest::Client;
use serde_json::{Value, json};
use std::path::Path;

pub async fn rpc_doctor(root: &Path, apply: bool) -> Result<Value> {
    let mut config = ProjectConfig::load(root)?;
    let url = config.rpc_url_value()?;
    let client = Client::builder()
        .user_agent("streamling-blockchain/0.1")
        .build()?;
    let chain_id = rpc::hex_u64(&rpc::rpc(&client, &url, "eth_chainId", json!([])).await?)?;
    let head = rpc::latest_block(&client, &url).await?;
    let start = config.start_block.min(head);
    let recommended_window = tune_window(&client, &url, start, head, config.window).await;
    let range_to = head.min(start.saturating_add(recommended_window.saturating_sub(1)));
    if apply
        && chain_id == config.chain_id
        && recommended_window > 0
        && recommended_window != config.window
    {
        config.window = recommended_window;
        config.save(root)?;
    }
    Ok(json!({
        "chain_id": chain_id,
        "expected_chain_id": config.chain_id,
        "chain_id_ok": chain_id == config.chain_id,
        "head": head,
        "start_block": config.start_block,
        "window": config.window,
        "recommended_window": recommended_window,
        "applied": apply && chain_id == config.chain_id && config.window == recommended_window,
        "sample_range": {"from": start, "to": range_to, "ok": recommended_window > 0},
        "recommendation": recommendation(chain_id == config.chain_id, recommended_window > 0),
    }))
}

async fn tune_window(client: &Client, url: &str, start: u64, head: u64, window: u64) -> u64 {
    let mut high = window;
    if high == 0 {
        return 0;
    }
    if window_works(client, url, start, head, high).await {
        return high;
    }
    let mut low = 1;
    let mut best = 0;
    while low <= high {
        let mid = low + (high - low) / 2;
        if window_works(client, url, start, head, mid).await {
            best = mid;
            low = mid.saturating_add(1);
        } else {
            high = mid.saturating_sub(1);
        }
    }
    best
}

async fn window_works(client: &Client, url: &str, start: u64, head: u64, window: u64) -> bool {
    let range_to = head.min(start.saturating_add(window.saturating_sub(1)));
    rpc::rpc(
        client,
        url,
        "eth_getLogs",
        json!([{"fromBlock": format!("0x{start:x}"), "toBlock": format!("0x{range_to:x}"), "address": []}]),
    )
    .await
    .is_ok()
}

#[cfg(test)]
fn tuned_window(mut high: u64, mut works: impl FnMut(u64) -> bool) -> u64 {
    if high == 0 {
        return 0;
    }
    if works(high) {
        return high;
    }
    let mut low = 1;
    let mut best = 0;
    while low <= high {
        let mid = low + (high - low) / 2;
        if works(mid) {
            best = mid;
            low = mid.saturating_add(1);
        } else {
            high = mid.saturating_sub(1);
        }
    }
    best
}

fn recommendation(chain_id_ok: bool, range_ok: bool) -> &'static str {
    match (chain_id_ok, range_ok) {
        (false, _) => "fix rpc_url: eth_chainId does not match project config",
        (true, false) => "lower window or use an RPC provider with wider eth_getLogs support",
        (true, true) => "rpc looks usable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommends_chain_fix_before_window_fix() {
        assert_eq!(
            recommendation(false, false),
            "fix rpc_url: eth_chainId does not match project config"
        );
        assert_eq!(
            recommendation(true, false),
            "lower window or use an RPC provider with wider eth_getLogs support"
        );
        assert_eq!(recommendation(true, true), "rpc looks usable");
    }

    #[test]
    fn tunes_window_to_largest_working_range() {
        assert_eq!(tuned_window(2_000, |window| window <= 750), 750);
        assert_eq!(tuned_window(2_000, |_| true), 2_000);
        assert_eq!(tuned_window(2_000, |_| false), 0);
    }
}
