use anyhow::{anyhow, Result};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone)]
pub struct RpcClient {
    http: Client,
    url: String,
}

impl RpcClient {
    pub fn new(url: String) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        Self { http, url }
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        // simple retry with exponential backoff (public RPC friendly)
        let mut backoff = Duration::from_millis(250);

        for attempt in 1..=6 {
            let body = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params
            });

            let resp = self.http.post(&self.url).json(&body).send().await;

            match resp {
                Ok(r) => {
                    let status = r.status();
                    let v: Value = r.json().await.map_err(|e| anyhow!("rpc decode error: {e:?}"))?;

                    if !status.is_success() {
                        // usually 429/5xx
                        if attempt < 6 {
                            sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                            continue;
                        }
                        return Err(anyhow!("rpc http error status={status} body={v}"));
                    }

                    if let Some(err) = v.get("error") {
                        // data-level or transient, still retry a bit
                        if attempt < 6 {
                            sleep(backoff).await;
                            backoff = (backoff * 2).min(Duration::from_secs(5));
                            continue;
                        }
                        return Err(anyhow!("rpc returned error: {err}"));
                    }

                    return v.get("result").cloned().ok_or_else(|| anyhow!("missing result field"));
                }
                Err(e) => {
                    if attempt < 6 {
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                        continue;
                    }
                    return Err(anyhow!("rpc request failed: {e:?}"));
                }
            }
        }

        Err(anyhow!("unreachable"))
    }
}
