use anyhow::{anyhow, Result};
use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub rpc_url: String,
    pub kafka_broker: String,
    pub in_topic: String,
    pub out_sol_deltas_topic: String,
    pub out_token_deltas_topic: String,
    pub consumer_group: String,
}

pub fn load() -> Result<Config> {
    let rpc_url = env::var("RPC_URL").unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    let kafka_broker=env::var("KAFKA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string());
    let in_topic = env::var("KAFKA_IN_TOPIC").unwrap_or_else(|_| "sol_raw_txs".to_string());
    let out_sol_deltas_topic = env::var("KAFKA_OUT_SOL_DELTAS_TOPIC").unwrap_or_else(|_| "sol_balance_deltas".to_string());
    let out_token_deltas_topic =env::var("KAFKA_OUT_TOKEN_DELTAS_TOPIC").unwrap_or_else(|_| "sol_token_balance_deltas".to_string());
    let consumer_group = env::var("KAFKA_GROUP").unwrap_or_else(|_| "decoder_v1".to_string());

    if kafka_broker.trim().is_empty() {
        return Err(anyhow!("KAFKA_BROKER is empty"));
    }

    Ok(Config {
        rpc_url,
        kafka_broker,
        in_topic,
        out_sol_deltas_topic,
        out_token_deltas_topic,
        consumer_group,
    })
}