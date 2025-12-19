use anyhow::{Result, anyhow};
use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub rpc_primary_url: String,
    pub rpc_fallback_urls: Vec<String>,
    pub rpc_concurrency: u32,
    pub rpc_min_delay_ms: u64,
    pub rpc_max_tx_version: u8,
    pub kafka_broker: String,
    pub in_topic: String,
    pub out_sol_deltas_topic: String,
    pub out_token_deltas_topic: String,
    pub dlq_topic: Option<String>,
    pub consumer_group: String,
}

pub fn load() -> Result<Config> {
    // RPC URL precedence: RPC_PRIMARY_URL > RPC_URL > default mainnet
    let rpc_primary_url = env::var("RPC_PRIMARY_URL")
        .or_else(|_| env::var("RPC_URL"))
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());

    // Parse comma-separated fallback URLs
    let rpc_fallback_urls = env::var("RPC_FALLBACK_URLS")
        .map(|s| {
            s.split(',')
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let rpc_concurrency = env::var("RPC_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    let rpc_min_delay_ms = env::var("RPC_MIN_DELAY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(250);

    let rpc_max_tx_version = env::var("RPC_MAX_TX_VERSION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let kafka_broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string());
    let in_topic = env::var("KAFKA_IN_TOPIC").unwrap_or_else(|_| "sol_raw_txs".to_string());
    let out_sol_deltas_topic =
        env::var("KAFKA_OUT_SOL_DELTAS_TOPIC").unwrap_or_else(|_| "sol_balance_deltas".to_string());
    let out_token_deltas_topic = env::var("KAFKA_OUT_TOKEN_DELTAS_TOPIC")
        .unwrap_or_else(|_| "sol_token_balance_deltas".to_string());
    let dlq_topic = env::var("KAFKA_DLQ_TOPIC").ok();
    let consumer_group = env::var("KAFKA_GROUP").unwrap_or_else(|_| "decoder_v1".to_string());

    if kafka_broker.trim().is_empty() {
        return Err(anyhow!("KAFKA_BROKER is empty"));
    }

    Ok(Config {
        rpc_primary_url,
        rpc_fallback_urls,
        rpc_concurrency,
        rpc_min_delay_ms,
        rpc_max_tx_version,
        kafka_broker,
        in_topic,
        out_sol_deltas_topic,
        out_token_deltas_topic,
        dlq_topic,
        consumer_group,
    })
}
