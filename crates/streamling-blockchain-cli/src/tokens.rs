//! Token metadata: `decimals`, `symbol`, and `name` for every address in the events table,
//! read with `eth_call` and stored in a `tokens` table beside the events.

use crate::{
    config::ProjectConfig,
    database::{Backend, BackendKind},
    output::{CodedError, ErrorCode, classify},
    rpc,
};
use anyhow::{Context, Result, bail};
use futures::{StreamExt, stream};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::path::Path;

const CONCURRENCY: usize = 8;
const DECIMALS: &str = "0x313ce567";
const SYMBOL: &str = "0x95d89b41";
const NAME: &str = "0x06fdde03";

#[derive(Debug, PartialEq)]
struct Token {
    address: String,
    symbol: Option<String>,
    name: Option<String>,
    decimals: Option<u8>,
}

/// Fetches metadata for event addresses without a `tokens` row, or for all of them with
/// `refresh`. A contract that does not answer a call gets null for that field.
pub async fn sync(root: &Path, refresh: bool, requested: Option<BackendKind>) -> Result<Value> {
    let config = ProjectConfig::load(root)?;
    let backend = match Backend::open(root, &config, requested)? {
        // `Backend::open` checks that the database exists, but opens it read-only.
        Backend::Sqlite(_) => {
            let conn = Connection::open(config.absolute_database(root))?;
            conn.busy_timeout(std::time::Duration::from_secs(10))?;
            Backend::Sqlite(conn)
        }
        backend => backend,
    };
    create_table(&backend).await?;
    let addresses = pending(&backend, config.chain_id, refresh).await?;

    let client = rpc::client()?;
    let urls = config.rpc_urls_value()?;
    if !addresses.is_empty() {
        // A wrong endpoint or key would otherwise look like contracts that revert.
        let chain_id =
            rpc::hex_u64(&rpc::rpc_any(&client, &urls, "eth_chainId", json!([])).await?)?;
        if chain_id != config.chain_id {
            bail!(CodedError::new(
                ErrorCode::Validation,
                format!(
                    "RPC is on chain {chain_id}, but the project is on chain {}",
                    config.chain_id
                )
            ))
        }
    }
    let results = stream::iter(addresses)
        .map(|address| fetch(&client, &urls, address))
        .buffer_unordered(CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    let mut tokens = Vec::new();
    let mut first_error = None;
    for result in results {
        match result {
            Ok(token) => tokens.push(token),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
    }
    // Store what succeeded, so a rerun fetches only the addresses that failed.
    store(&backend, config.chain_id, &tokens).await?;
    if let Some(error) = first_error {
        return Err(error.context(format!(
            "stored {} tokens; rerun `streamling-blockchain sync-tokens` for the rest",
            tokens.len()
        )));
    }
    Ok(json!({
        "stored": tokens.len(),
        "with_decimals": tokens.iter().filter(|token| token.decimals.is_some()).count(),
        "with_symbol": tokens.iter().filter(|token| token.symbol.is_some()).count(),
    }))
}

async fn create_table(backend: &Backend) -> Result<()> {
    match backend {
        Backend::Sqlite(conn) => conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tokens (
                chain_id INTEGER NOT NULL,
                address TEXT NOT NULL,
                symbol TEXT,
                name TEXT,
                decimals INTEGER,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (chain_id, address)
            );",
        )?,
        Backend::ClickHouse { client, table } => {
            client
                .execute(&format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        chain_id UInt64,
                        address String,
                        symbol Nullable(String),
                        name Nullable(String),
                        decimals Nullable(UInt8),
                        updated_at DateTime DEFAULT now()
                    )
                    ENGINE = ReplacingMergeTree(updated_at)
                    ORDER BY (chain_id, address)",
                    client.qualified(&format!("{table}_tokens"))
                ))
                .await?
        }
    }
    Ok(())
}

/// Distinct event addresses on the project's chain, less those already in `tokens`.
async fn pending(backend: &Backend, chain_id: u64, refresh: bool) -> Result<Vec<String>> {
    let (events, tokens) = match backend {
        Backend::Sqlite(_) => ("events".to_owned(), "tokens".to_owned()),
        // Without FINAL: a reorged-out log only costs one extra lookup.
        Backend::ClickHouse { client, table } => (
            client.qualified(table),
            format!("{} FINAL", client.qualified(&format!("{table}_tokens"))),
        ),
    };
    let known = if refresh {
        String::new()
    } else {
        format!(
            " AND lower(address) NOT IN (SELECT address FROM {tokens} WHERE chain_id = {chain_id})"
        )
    };
    let rows = backend
        .query(
            &format!(
                "SELECT DISTINCT lower(address) AS address FROM {events} WHERE chain_id = {chain_id}{known}"
            ),
            usize::MAX,
        )
        .await?;
    rows["rows"]
        .as_array()
        .context("query returned no rows array")?
        .iter()
        .map(|row| {
            row["address"]
                .as_str()
                .map(str::to_owned)
                .context("event address is not text")
        })
        .collect()
}

async fn fetch(client: &reqwest::Client, urls: &str, address: String) -> Result<Token> {
    let decimals = call(client, urls, &address, DECIMALS).await?;
    let symbol = call(client, urls, &address, SYMBOL).await?;
    let name = call(client, urls, &address, NAME).await?;
    Ok(Token {
        decimals: decimals.as_deref().and_then(decode_decimals),
        symbol: symbol.as_deref().and_then(decode_string),
        name: name.as_deref().and_then(decode_string),
        address,
    })
}

/// The return data of one `eth_call`, or `None` when the contract reverts.
async fn call(
    client: &reqwest::Client,
    urls: &str,
    address: &str,
    data: &str,
) -> Result<Option<Vec<u8>>> {
    let params = json!([{"to": address, "data": data}, "latest"]);
    match rpc::rpc_any(client, urls, "eth_call", params).await {
        Ok(value) => {
            let text = value.as_str().context("eth_call result is not a string")?;
            Ok(hex::decode(text.trim_start_matches("0x")).ok())
        }
        // A JSON-RPC error that is not a rate limit is a revert.
        Err(error) if classify(&error).0 == ErrorCode::Internal => Ok(None),
        Err(error) => Err(error),
    }
}

async fn store(backend: &Backend, chain_id: u64, tokens: &[Token]) -> Result<()> {
    if tokens.is_empty() {
        return Ok(());
    }
    match backend {
        Backend::Sqlite(conn) => {
            let tx = conn.unchecked_transaction()?;
            {
                let mut insert = tx.prepare(
                    "INSERT OR REPLACE INTO tokens (chain_id, address, symbol, name, decimals, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, unixepoch())",
                )?;
                for token in tokens {
                    insert.execute(params![
                        chain_id,
                        token.address,
                        token.symbol,
                        token.name,
                        token.decimals
                    ])?;
                }
            }
            tx.commit()?;
        }
        Backend::ClickHouse { client, table } => {
            let rows = tokens
                .iter()
                .map(|token| {
                    json!({
                        "chain_id": chain_id,
                        "address": token.address,
                        "symbol": token.symbol,
                        "name": token.name,
                        "decimals": token.decimals,
                    })
                })
                .collect::<Vec<_>>();
            client
                .insert_json_rows(&format!("{table}_tokens"), &rows)
                .await?;
        }
    }
    Ok(())
}

/// One 32-byte word as a length or offset.
fn word(data: &[u8], at: usize) -> Option<usize> {
    let word = data.get(at..at.checked_add(32)?)?;
    if word[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    usize::try_from(u64::from_be_bytes(word[24..].try_into().ok()?)).ok()
}

fn decode_decimals(data: &[u8]) -> Option<u8> {
    let value = word(data, 0)?;
    u8::try_from(value).ok()
}

/// An ABI `string`, or a `bytes32` as some older tokens (such as MKR) return.
fn decode_string(data: &[u8]) -> Option<String> {
    let bytes = if data.len() == 32 {
        let end = data.iter().position(|byte| *byte == 0).unwrap_or(32);
        &data[..end]
    } else {
        let offset = word(data, 0)?;
        let len = word(data, offset)?;
        let start = offset.checked_add(32)?;
        data.get(start..start.checked_add(len)?)?
    };
    let text = String::from_utf8_lossy(bytes)
        .trim_matches(char::from(0))
        .trim()
        .to_owned();
    (!text.is_empty()).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn abi_string(text: &str) -> Vec<u8> {
        let mut data = vec![0; 64];
        data[31] = 32;
        data[63] = text.len() as u8;
        data.extend(text.as_bytes());
        data.resize(64 + text.len().div_ceil(32) * 32, 0);
        data
    }

    #[test]
    fn decodes_abi_strings_and_bytes32() {
        assert_eq!(decode_string(&abi_string("USDC")).as_deref(), Some("USDC"));
        assert_eq!(
            decode_string(&abi_string("Wrapped Ether with a long name")).as_deref(),
            Some("Wrapped Ether with a long name")
        );
        let mut mkr = b"MKR".to_vec();
        mkr.resize(32, 0);
        assert_eq!(decode_string(&mkr).as_deref(), Some("MKR"));
        assert_eq!(decode_string(&[]), None);
        assert_eq!(decode_string(&abi_string("")), None);
        // A length past the end of the data is not a string.
        let mut short = abi_string("USDC");
        short[63] = 200;
        assert_eq!(decode_string(&short), None);
    }

    #[test]
    fn decodes_decimals_that_fit_a_byte() {
        let mut data = vec![0; 32];
        data[31] = 18;
        assert_eq!(decode_decimals(&data), Some(18));
        data[30] = 1;
        assert_eq!(decode_decimals(&data), None);
        assert_eq!(decode_decimals(&[]), None);
    }

    #[tokio::test]
    #[ignore = "reads mainnet over a public RPC"]
    async fn fetches_live_metadata() {
        let client = rpc::client().unwrap();
        let urls = "https://ethereum-rpc.publicnode.com";
        let usdc = fetch(
            &client,
            urls,
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".into(),
        )
        .await
        .unwrap();
        assert_eq!(
            (usdc.symbol.as_deref(), usdc.decimals),
            (Some("USDC"), Some(6))
        );
        let mkr = fetch(
            &client,
            urls,
            "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2".into(),
        )
        .await
        .unwrap();
        assert_eq!(
            (mkr.symbol.as_deref(), mkr.name.as_deref()),
            (Some("MKR"), Some("Maker"))
        );
        // An account without code returns empty data for every call.
        let empty = fetch(
            &client,
            urls,
            "0x000000000000000000000000000000000000dead".into(),
        )
        .await
        .unwrap();
        assert_eq!((empty.symbol, empty.decimals), (None, None));
    }

    #[tokio::test]
    async fn pending_skips_known_tokens_and_other_chains() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE events (event_id TEXT PRIMARY KEY, chain_id INTEGER, address TEXT);
             INSERT INTO events VALUES
               ('a', 1, '0xAAAA'), ('b', 1, '0xaaaa'), ('c', 1, '0xbbbb'), ('d', 2, '0xcccc');",
        )
        .unwrap();
        let backend = Backend::Sqlite(conn);
        create_table(&backend).await.unwrap();
        store(
            &backend,
            1,
            &[Token {
                address: "0xbbbb".into(),
                symbol: Some("B".into()),
                name: None,
                decimals: Some(6),
            }],
        )
        .await
        .unwrap();
        assert_eq!(pending(&backend, 1, false).await.unwrap(), ["0xaaaa"]);
        let mut all = pending(&backend, 1, true).await.unwrap();
        all.sort();
        assert_eq!(all, ["0xaaaa", "0xbbbb"]);
    }
}
