use anyhow::{anyhow, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use tokio::time::sleep;
use log::{error, info, warn};
use futures::{SinkExt, StreamExt};
use std::{collections::{HashMap, HashSet}, env, time::Duration};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions,
};
use bs58;


const KAFKA_BROKER: &str = "localhost:19092";
const TOPIC: &str = "sol_raw_txs";

// default filter (Raydium AMM v4)
const DEFAULT_REQUIRED_ACCOUNTS: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";


#[derive(Debug, Serialize)]
struct RawTxEvent {
    schema_version : u8,
    chain : String,
    slot: u64,
    block_time: Option<i64>,
    signature: String,
    index_in_block: u32,
    tx_version: Option<u8>, 
    is_success: bool,
    fee_lamports: u64,
    compute_units_consumed: Option<u64>, 
    main_program: Option<String>,
    program_ids: Vec<String>,
}

async fn create_producer() -> Result<FutureProducer> {
    let producer : FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKER)
        .set("message.timeout.ms", "5000")
        .create()?;
    Ok(producer)
}

async fn send_event(
    producer: &FutureProducer,
    event: &RawTxEvent,
) -> Result<()> {
    let json = serde_json::to_string(event)?;

    let record = FutureRecord::<(), String>::to(TOPIC)
    .payload(&json);

    let delivery_status = producer
        .send(record, Duration::from_secs(0))
        .await;

    match delivery_status {
        Ok((_partition, _offset)) => {
            println!("✅ Sent: {json}");
        }
        Err((err, _owned_msg)) => {
            eprintln!("❌ Kafka error: {err:?}");
        }
    }

    Ok(())
}

fn setup_logging() {
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).try_init();
}

fn required_accounts() -> Vec<String> {
    env::var("REQUIRED_ACCOUNTS")
        .unwrap_or_else(|_| DEFAULT_REQUIRED_ACCOUNTS.to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn pick_main_program(program_ids: &[String]) -> Option<String> {
    let skip = [
        "ComputeBudget111111111111111111111111111111",
        "11111111111111111111111111111111", // System
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    ];
    program_ids.iter().find(|p| !skip.contains(&p.as_str())).cloned()
}

fn extract_program_ids(account_keys: &[String], program_id_indexes: impl Iterator<Item = u32>) -> Vec<String> {
    let mut out = vec![];
    let mut seen = HashSet::new();
    for idx in program_id_indexes {
        let i = idx as usize;
        if i < account_keys.len() {
            let pid = &account_keys[i];
            if seen.insert(pid.clone()) {
                out.push(pid.clone());
            }
        }
    }
    out
}



#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let producer = create_producer().await?;
    let endpoint = env::var("GEYSER_ENDPOINT")
        .map_err(|_| anyhow!("Missing GEYSER_ENDPOINT"))?;
    

    for i in 0..3 {
        let event = RawTxEvent {
            schema_version: 1,
            chain: "solana-mainnet".to_string(),
            slot: 1000 + i,
            block_time: Some(1_700_000_000 + i as i64), // fake unix timestamps
            signature: format!("FAKE_SIG_{i:04}"),
            index_in_block: i as u32,
            tx_version: Some(0), // pretend all are v0 for now
            is_success: true,
            fee_lamports: 5000,
            compute_units_consumed: Some(200_000),
            main_program: Some("RaydiumFake1111111111111111111111111".to_string()),
            program_ids: vec![
                "RaydiumFake1111111111111111111111111".to_string(),
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            ],
        };

        send_event(&producer, &event).await?;
        sleep(Duration::from_millis(500)).await;
    }
    println!("Done.");
    Ok(())
}