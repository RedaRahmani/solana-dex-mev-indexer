use anyhow::{Result, anyhow};
use std::{env, time::Duration};
use yellowstone_grpc_proto::prelude::CommitmentLevel;

#[derive(Clone, Debug)]
pub struct Config {
    pub geyser_endpoint: String,
    pub geyser_x_token: Option<String>,

    pub kafka_broker: String,
    pub kafka_topic: String,

    pub required_accounts: Vec<String>,
    pub include_failed: bool,
    pub commitment: CommitmentLevel,

    pub reconnect_min_backoff: Duration,
    pub reconnect_max_backoff: Duration,
}

fn parse_bool(v: Option<String>, default: bool) -> bool {
    match v.as_deref() {
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES") => true,
        Some("0") | Some("false") | Some("FALSE") | Some("no") | Some("NO") => false,
        None => default,
        _ => default,
    }
}

fn parse_commitment(s: &str) -> Result<CommitmentLevel> {
    match s.to_lowercase().as_str() {
        "processed" => Ok(CommitmentLevel::Processed),
        "confirmed" => Ok(CommitmentLevel::Confirmed),
        "finalized" => Ok(CommitmentLevel::Finalized),
        other => Err(anyhow!(
            "Invalid COMMITMENT={other}. Use processed|confirmed|finalized"
        )),
    }
}

pub fn load() -> Result<Config> {
    let geyser_endpoint =
        env::var("GEYSER_ENDPOINT").map_err(|_| anyhow!("Missing GEYSER_ENDPOINT"))?;
    let geyser_x_token = env::var("GEYSER_X_TOKEN").ok();

    let kafka_broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "localhost:19092".to_string());
    let kafka_topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| "sol_raw_txs".to_string());

    let required_accounts = env::var("REQUIRED_ACCOUNTS")
        .unwrap_or_else(|_| "".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();

    let include_failed = parse_bool(env::var("INCLUDE_FAILED").ok(), false);

    let commitment =
        parse_commitment(&env::var("COMMITMENT").unwrap_or_else(|_| "processed".to_string()))?;

    Ok(Config {
        geyser_endpoint,
        geyser_x_token,
        kafka_broker,
        kafka_topic,
        required_accounts,
        include_failed,
        commitment,
        reconnect_min_backoff: Duration::from_secs(1),
        reconnect_max_backoff: Duration::from_secs(30),
    })
}
