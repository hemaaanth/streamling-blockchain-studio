//! JSON-RPC over one or more interchangeable endpoints for the same chain.
//!
//! `rpc_url` (or the variable named by `rpc_url_env`) may list several URLs separated by
//! commas. Each call goes to the endpoint with the fewest requests in flight. Rate limits,
//! server errors, and transport failures cool that endpoint down and move the call to
//! another one. A JSON-RPC error is returned to the caller once two endpoints (or the only
//! endpoint) report it, so deterministic errors such as a too-wide `eth_getLogs` range still
//! reach the caller's own handling.

use abi_stable::std_types::RDuration;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
    time::{Duration, Instant},
};
use streamling_plugin::r#async::PluginAsyncRuntimeObj;
use streamling_plugin::{PluginError, PluginInitializationError};

const USER_AGENT: &str = "streamling-blockchain/0.1";
const MAX_FAILURES_PER_CALL: usize = 12;
const MAX_COOLDOWN_MS: u64 = 30_000;

pub struct RpcPool {
    client: reqwest::Client,
    endpoints: Vec<Endpoint>,
    epoch: Instant,
}

struct Endpoint {
    url: String,
    /// The URL's host, for messages and logs. The full URL often carries an API key.
    label: String,
    in_flight: AtomicUsize,
    /// Largest JSON-RPC batch this endpoint has accepted; halves when a batch is rejected.
    batch_limit: AtomicUsize,
    /// Milliseconds since `RpcPool::epoch` before which the endpoint is skipped.
    cooldown_until: AtomicU64,
    strikes: AtomicU32,
}

/// How one request to one endpoint failed.
enum Failure {
    /// Rate limit, server error, timeout, or unreadable response: try another endpoint.
    Unavailable(String),
    /// The endpoint rejected the batch as a whole; retry with smaller batches.
    BatchRejected(String),
}

struct InFlight<'a>(&'a Endpoint);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl RpcPool {
    pub fn from_options(
        options: &HashMap<String, String>,
        batch_size: usize,
    ) -> Result<Self, PluginInitializationError> {
        let urls = if let Some(url) = options.get("rpc_url") {
            url.clone()
        } else if let Some(name) = options.get("rpc_url_env") {
            std::env::var(name).map_err(|_| {
                PluginInitializationError::Configuration(format!("read ${name}").into())
            })?
        } else {
            return Err(PluginInitializationError::Configuration(
                "missing option rpc_url or rpc_url_env".into(),
            ));
        };
        let urls = urls
            .split(',')
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if urls.is_empty() {
            return Err(PluginInitializationError::Configuration(
                "rpc_url lists no endpoints".into(),
            ));
        }
        Ok(Self::new(urls, batch_size))
    }

    pub fn new(urls: Vec<String>, batch_size: usize) -> Self {
        Self {
            client: reqwest::Client::builder()
                .user_agent(USER_AGENT)
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()
                .expect("static HTTP client configuration builds"),
            endpoints: urls
                .into_iter()
                .map(|url| Endpoint {
                    label: host_label(&url),
                    url,
                    in_flight: AtomicUsize::new(0),
                    batch_limit: AtomicUsize::new(batch_size.max(1)),
                    cooldown_until: AtomicU64::new(0),
                    strikes: AtomicU32::new(0),
                })
                .collect(),
            epoch: Instant::now(),
        }
    }

    pub async fn request(
        &self,
        rt: &PluginAsyncRuntimeObj,
        method: &str,
        params: Value,
    ) -> Result<Value, PluginError> {
        let mut values = self.batch(rt, method, vec![params]).await?;
        Ok(values.pop().unwrap_or(Value::Null))
    }

    /// Send one call per entry in `params` and return the results in the same order.
    /// Calls may be split across several requests and endpoints.
    pub async fn batch(
        &self,
        rt: &PluginAsyncRuntimeObj,
        method: &str,
        params: Vec<Value>,
    ) -> Result<Vec<Value>, PluginError> {
        let mut results: Vec<Option<Value>> = vec![None; params.len()];
        let mut pending = (0..params.len()).collect::<Vec<_>>();
        // Endpoints that already returned a JSON-RPC error, per call index.
        let mut rpc_errors: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut failures = 0;
        let mut last_failure = String::new();
        while !pending.is_empty() {
            if failures >= MAX_FAILURES_PER_CALL {
                return Err(PluginError::Execution(format!(
                    "{method}: every RPC attempt failed; last error: {last_failure}"
                )));
            }
            let avoid = rpc_errors.get(&pending[0]).map_or(&[][..], Vec::as_slice);
            let Some(index) = self.pick(avoid) else {
                rt.sleep(RDuration::from_millis(self.shortest_cooldown_ms()))
                    .await;
                continue;
            };
            let endpoint = &self.endpoints[index];
            let take = pending
                .len()
                .min(endpoint.batch_limit.load(Ordering::Relaxed));
            let chunk = pending.drain(..take).collect::<Vec<_>>();
            let payload = if chunk.len() == 1 {
                json!({"jsonrpc":"2.0","id":chunk[0],"method":method,"params":params[chunk[0]]})
            } else {
                Value::Array(
                    chunk
                        .iter()
                        .map(|&id| json!({"jsonrpc":"2.0","id":id,"method":method,"params":params[id]}))
                        .collect(),
                )
            };
            match self.send(endpoint, &payload, chunk.len()).await {
                Ok(bodies) => {
                    endpoint.strikes.store(0, Ordering::Relaxed);
                    // Some providers answer a lone call's error with `"id": null`.
                    let mut by_id = if chunk.len() == 1 {
                        bodies
                            .into_iter()
                            .take(1)
                            .map(|body| (chunk[0], body))
                            .collect()
                    } else {
                        bodies
                            .into_iter()
                            .filter_map(|body| Some((body.get("id")?.as_u64()? as usize, body)))
                            .collect::<HashMap<_, _>>()
                    };
                    let mut unavailable = false;
                    for id in chunk {
                        let Some(body) = by_id.remove(&id) else {
                            unavailable = true;
                            pending.push(id);
                            continue;
                        };
                        if let Some(error) = body.get("error") {
                            if is_rate_limited(error) {
                                unavailable = true;
                                pending.push(id);
                                continue;
                            }
                            let seen = rpc_errors.entry(id).or_default();
                            seen.push(index);
                            if seen.len() >= 2.min(self.endpoints.len()) {
                                return Err(PluginError::Execution(format!("{method}: {error}")));
                            }
                            last_failure = error.to_string();
                            failures += 1;
                            pending.push(id);
                            continue;
                        }
                        results[id] = Some(body.get("result").cloned().unwrap_or(Value::Null));
                    }
                    if unavailable {
                        last_failure =
                            format!("{}: rate limited or incomplete batch", endpoint.label);
                        failures += 1;
                        self.cool_down(endpoint);
                    }
                }
                Err(Failure::BatchRejected(message)) => {
                    let limit = endpoint.batch_limit.load(Ordering::Relaxed);
                    endpoint
                        .batch_limit
                        .store((chunk.len() / 2).clamp(1, limit), Ordering::Relaxed);
                    tracing::debug!(endpoint = %endpoint.label, %message, "RPC batch rejected; shrinking");
                    pending.extend(chunk);
                }
                Err(Failure::Unavailable(message)) => {
                    last_failure = message;
                    failures += 1;
                    self.cool_down(endpoint);
                    pending.extend(chunk);
                }
            }
            pending.sort_unstable();
        }
        Ok(results
            .into_iter()
            .map(|value| value.unwrap_or(Value::Null))
            .collect())
    }

    async fn send(
        &self,
        endpoint: &Endpoint,
        payload: &Value,
        calls: usize,
    ) -> Result<Vec<Value>, Failure> {
        endpoint.in_flight.fetch_add(1, Ordering::Relaxed);
        let _guard = InFlight(endpoint);
        let url = &endpoint.label;
        let response = self
            .client
            .post(&endpoint.url)
            .json(payload)
            .send()
            .await
            .map_err(|error| Failure::Unavailable(format!("{url}: {}", error.without_url())))?;
        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(Failure::Unavailable(format!("{url}: HTTP {status}")));
        }
        let body = if status.is_server_error() {
            None
        } else {
            response.json::<Value>().await.ok()
        };
        match body {
            Some(Value::Array(items)) => Ok(items),
            // A lone call's JSON-RPC error may arrive with a 4xx status; the caller classifies it.
            Some(body) if calls == 1 && (status.is_success() || body.get("error").is_some()) => {
                match body.get("error") {
                    Some(error) if is_rate_limited(error) => {
                        Err(Failure::Unavailable(format!("{url}: {error}")))
                    }
                    _ => Ok(vec![body]),
                }
            }
            _ if calls > 1 => Err(Failure::BatchRejected(format!("{url}: HTTP {status}"))),
            _ => Err(Failure::Unavailable(format!("{url}: HTTP {status}"))),
        }
    }

    /// The least-loaded endpoint that is not cooling down, preferring ones not in `avoid`.
    fn pick(&self, avoid: &[usize]) -> Option<usize> {
        let now = self.now_ms();
        let ready = |index: &usize| {
            self.endpoints[*index]
                .cooldown_until
                .load(Ordering::Relaxed)
                <= now
        };
        let load = |index: &usize| self.endpoints[*index].in_flight.load(Ordering::Relaxed);
        (0..self.endpoints.len())
            .filter(ready)
            .filter(|index| !avoid.contains(index))
            .min_by_key(load)
            .or_else(|| (0..self.endpoints.len()).filter(ready).min_by_key(load))
    }

    fn cool_down(&self, endpoint: &Endpoint) {
        let strikes = endpoint.strikes.fetch_add(1, Ordering::Relaxed).min(5);
        let delay = (1_000u64 << strikes).min(MAX_COOLDOWN_MS);
        endpoint
            .cooldown_until
            .store(self.now_ms() + delay, Ordering::Relaxed);
        tracing::debug!(endpoint = %endpoint.label, delay_ms = delay, "RPC endpoint cooling down");
    }

    fn shortest_cooldown_ms(&self) -> u64 {
        let now = self.now_ms();
        self.endpoints
            .iter()
            .map(|endpoint| {
                endpoint
                    .cooldown_until
                    .load(Ordering::Relaxed)
                    .saturating_sub(now)
            })
            .min()
            .unwrap_or(0)
            .max(50)
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }
}

fn is_rate_limited(error: &Value) -> bool {
    error.get("code").and_then(Value::as_i64) == Some(429)
        || error
            .get("message")
            .and_then(Value::as_str)
            .is_some_and(|message| {
                message.contains("Too Many Requests") || message.contains("rate limit")
            })
}

/// The URL's host only: API keys usually sit in the path, query, or userinfo.
fn host_label(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_else(|| "RPC endpoint".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_label_drops_path_query_and_userinfo() {
        assert_eq!(
            host_label("https://user:pw@eth.example.com/v2/secret-key?apikey=k"),
            "eth.example.com"
        );
        assert_eq!(host_label("not a url"), "RPC endpoint");
    }
}
