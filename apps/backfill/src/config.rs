use anyhow::{Result, anyhow};
use clap::Parser;
use schema::{Profile, validate_mainnet_url};
use std::{env, path::PathBuf};

#[derive(Parser, Debug, Clone)]
pub struct Cli {
    /// Backfill address (Raydium AMM v4 program id)
    #[arg(long, default_value = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8")]
    pub address: String,

    /// How many txs to backfill
    #[arg(long, default_value_t = 2000)]
    pub limit: usize,

    /// RPC URL (public mainnet by default)
    #[arg(long)]
    pub rpc_url: Option<String>,

    /// Record raw tx responses into this jsonl file (required for backfill mode)
    #[arg(long)]
    pub out: Option<PathBuf>,

    /// Replay from a recorded jsonl file (replay mode)
    #[arg(long)]
    pub from_file: Option<PathBuf>,

    /// Concurrency for getTransaction calls
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub profile: Profile,
    pub rpc_url: String,
    pub kafka_broker: String,
    pub kafka_topic: String,
    pub dlq_topic: String,
    pub chain: String,
}

pub fn load(cli: &Cli) -> Result<Config> {
    // Parse PROFILE first (defaults to local)
    let profile = Profile::from_env()?;

    // RPC URL precedence: CLI arg > RPC_URL env > profile default
    let rpc_url = cli
        .rpc_url
        .clone()
        .or_else(|| env::var("RPC_URL").ok())
        .unwrap_or_else(|| profile.default_rpc_url().to_string());

    // Validate RPC URL is appropriate for profile
    validate_mainnet_url(profile, &rpc_url, "RPC_URL")?;

    let kafka_broker = env::var("KAFKA_BROKER").unwrap_or_else(|_| "127.0.0.1:19092".to_string());

    // Topic overrides: KAFKA_TOPIC_RAW_TXS > KAFKA_TOPIC > profile default
    let kafka_topic = env::var("KAFKA_TOPIC_RAW_TXS")
        .or_else(|_| env::var("KAFKA_TOPIC"))
        .unwrap_or_else(|_| profile.default_kafka_topic_raw_txs().to_string());

    let dlq_topic = env::var("KAFKA_DLQ_TOPIC")
        .or_else(|_| env::var("KAFKA_TOPIC_DLQ"))
        .unwrap_or_else(|_| profile.default_kafka_topic_dlq().to_string());

    // keep consistent with your existing schema
    let chain = env::var("CHAIN").unwrap_or_else(|_| "solana-mainnet".to_string());

    // Validate mode
    if cli.from_file.is_none() && cli.out.is_none() {
        return Err(anyhow!(
            "Choose a mode: either --out <file> (backfill/record) or --from-file <file> (replay)"
        ));
    }

    Ok(Config {
        profile,
        rpc_url,
        kafka_broker,
        kafka_topic,
        dlq_topic,
        chain,
    })
}
