use anyhow::{anyhow, Result};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
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

    let endpoint = env::var("GEYSER_ENDPOINT")
        .map_err(|_| anyhow!("Missing GEYSER_ENDPOINT"))?;

    let x_token = env::var("GEYSER_X_TOKEN").ok();

    info!("Starting streamer (Yollowstone gRPC -> Kafka) topic={TOPIC} broker={KAFKA_BROKER}");
    info!("GEYSER_ENDPOINT={endpoint}");
    info!("Required accounts: {:?}", required_accounts());

    let producer = create_producer().await?;

    let mut client = GeyserGrpcClient::build_from_shared(endpoint)?
        .x_token(x_token)?
        .tls_config(ClientTlsConfig::new().with_native_roots())? //
        .connect()
        .await?;
    
        let (mut sub_tx, mut sub_rx) = client.subscribe().await?;

        // Filter: only txs that mention REQUIRED_ACCOUNTS (default raydium AMM v4)
        let mut tx_filters = HashMap::new();
        tx_filters.insert(
            "tx_filter".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                account_include: vec![],
                account_exclude: vec![],
                account_required: required_accounts(),
                ..Default::default() 
            },
        );

        sub_tx
            .send(SubscribeRequest{
                transactions: tx_filters,
                commitment: Some(CommitmentLevel::Processed as i32),
                ..Default::default()
            })
            .await?;
        
        info!("Subscribed. Streaming...");

        while let Some(msg) = sub_rx.next().await {
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!("stream error : {e:?}");
                    continue;
                }
            };

            match msg.update_oneof {
                Some(UpdateOneof::Transaction(tx)) => {
                    // tx.transactions is where the actual tx lives (proto structure)
                    let Some(tx_info) = tx.transaction else { continue };
                    let signature = bs58::encode(&tx_info.signature).into_string();

                    let slot = tx.slot;
                    let chain = "solana-mainnet".to_string();
                    let meta = tx_info.meta.as_ref();
                    let is_success = meta.and_then(|m| m.err.as_ref()).is_none();
                    let fee_lamports = meta.map(|m| m.fee).unwrap_or(0);
                    
                    // account keys
                    let mut account_keys: Vec<String> = tx_info 
                        .transaction
                        .and_then(|t| t.message.as_ref())
                        .map(|m| m.account_keys.iter().map(|k| bs58::encode(k).into_string()).collect())
                        .unwrap_or_default();
                    
                    // program_id_index extraction from outer + inner instructions
                    let outer_indexes = tx_info
                    .transaction
                    .as_ref()
                    .and_then(|t| t.message.as_ref())
                    .into_iter()
                    .flat_map(|m| m.instructions.iter().map(|ix| ix.program_id_index));

                let inner_indexes = meta
                    .into_iter()
                    .flat_map(|m| m.inner_instructions.iter())
                    .flat_map(|ii| ii.instructions.iter().map(|ix| ix.program_id_index));

                let program_ids = extract_program_ids(&account_keys, outer_indexes.chain(inner_indexes));
                let main_program = pick_main_program(&program_ids);

                let event = RawTxEvent {
                    schema_version: 1,
                    chain,
                    slot,
                    block_time: None,              // fill later (block stream / enrich)
                    signature,
                    index_in_block: 0,             // v0 placeholder
                    tx_version: None,              // v0 placeholder
                    is_success,
                    fee_lamports,
                    compute_units_consumed: None,  // v0 placeholder
                    main_program,
                    program_ids,
                };

                if let Err(e) = send_event(&producer, &event).await {
                    error!("kafka send failed: {e:?}");
                }
            }
            Some(UpdateOneof::Ping(_)) => {}
            _ => {}
        }
    }

    Ok(())
}