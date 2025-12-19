use anyhow::{Result, anyhow};
use log::warn;
use reqwest::Client;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep};

#[derive(Clone)]
pub struct RpcClient {
    http: Client,
    primary_url: String,
    fallback_urls: Vec<String>,
    semaphore: Arc<Semaphore>,
    min_delay_ms: u64,
    max_tx_version: u8,
    last_request: Arc<tokio::sync::Mutex<Instant>>,
}

impl RpcClient {
    pub fn new(
        primary_url: String,
        fallback_urls: Vec<String>,
        concurrency: u32,
        min_delay_ms: u64,
        max_tx_version: u8,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(25))
            .build()
            .expect("reqwest");

        Self {
            http,
            primary_url,
            fallback_urls,
            semaphore: Arc::new(Semaphore::new(concurrency as usize)),
            min_delay_ms,
            max_tx_version,
            last_request: Arc::new(tokio::sync::Mutex::new(Instant::now())),
        }
    }

    pub async fn get_transaction_json_parsed(&self, signature: &str) -> Result<Value> {
        let params = json!([
            signature,
            {"encoding":"jsonParsed", "maxSupportedTransactionVersion": self.max_tx_version}
        ]);
        self.call("getTransaction", params).await
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        // Acquire semaphore permit to limit concurrency
        let _permit = self.semaphore.acquire().await.expect("semaphore");

        // Apply minimum delay between requests to reduce 429s
        self.apply_rate_limit().await;

        // Build all URLs to try: primary + fallbacks
        let mut urls_to_try = vec![self.primary_url.clone()];
        urls_to_try.extend(self.fallback_urls.clone());

        let mut backoff = Duration::from_millis(250);
        let max_attempts = 6;

        for attempt in 1..=max_attempts {
            // Rotate through URLs on retries
            let url_index = (attempt - 1) % urls_to_try.len();
            let url = &urls_to_try[url_index];

            let body = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params
            });

            let resp = self.http.post(url).json(&body).send().await;

            match resp {
                Ok(r) => {
                    let status = r.status();

                    // Handle rate limiting specifically
                    if status.as_u16() == 429 {
                        if attempt < max_attempts {
                            warn!(
                                "RPC 429 rate limit, backing off {}ms (attempt {}/{})",
                                backoff.as_millis(),
                                attempt,
                                max_attempts
                            );
                            sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(8));
                            continue;
                        }
                        return Err(anyhow!("RPC rate limited after {} attempts", max_attempts));
                    }

                    // Handle 5xx server errors
                    if status.is_server_error() {
                        if attempt < max_attempts {
                            warn!(
                                "RPC server error {}, retrying (attempt {}/{})",
                                status, attempt, max_attempts
                            );
                            sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                            continue;
                        }
                        return Err(anyhow!(
                            "RPC server error after {} attempts: {}",
                            max_attempts,
                            status
                        ));
                    }

                    let v: Value = r
                        .json()
                        .await
                        .map_err(|e| anyhow!("rpc decode error: {e:?}"))?;

                    if let Some(error) = v.get("error") {
                        if attempt < max_attempts {
                            warn!(
                                "RPC error response: {}, retrying (attempt {}/{})",
                                error, attempt, max_attempts
                            );
                            sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                            continue;
                        }
                        return Err(anyhow!("RPC error: {}", error));
                    }

                    if !status.is_success() {
                        if attempt < max_attempts {
                            sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                            continue;
                        }
                        return Err(anyhow!("RPC non-success status: {} body: {}", status, v));
                    }

                    return v
                        .get("result")
                        .cloned()
                        .ok_or_else(|| anyhow!("missing result field"));
                }
                Err(e) => {
                    if attempt < max_attempts {
                        warn!(
                            "RPC request failed: {e:?}, retrying (attempt {}/{})",
                            attempt, max_attempts
                        );
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                        continue;
                    }
                    return Err(anyhow!(
                        "RPC request failed after {} attempts: {e:?}",
                        max_attempts
                    ));
                }
            }
        }

        Err(anyhow!("unreachable"))
    }

    async fn apply_rate_limit(&self) {
        if self.min_delay_ms == 0 {
            return;
        }

        let mut last = self.last_request.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last);
        let min_delay = Duration::from_millis(self.min_delay_ms);

        if elapsed < min_delay {
            let sleep_duration = min_delay - elapsed;
            sleep(sleep_duration).await;
        }

        *last = Instant::now();
    }
}
