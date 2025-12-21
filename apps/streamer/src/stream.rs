use anyhow::Result;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    SubscribeRequest, SubscribeRequestFilterTransactions, subscribe_update::UpdateOneof,
};

use crate::{config::Config, kafka, metrics::Metrics};
use rdkafka::producer::FutureProducer;

#[derive(Debug, Serialize)]
pub struct RawTxEvent {
    pub schema_version: u8,
    pub chain: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: String,
    pub index_in_block: u32,
    pub tx_version: Option<u8>,
    pub is_success: bool,
    pub fee_lamports: u64,
    pub compute_units_consumed: Option<u64>,
    pub main_program: Option<String>,
    pub program_ids: Vec<String>,
}

fn pick_main_program(program_ids: &[String]) -> Option<String> {
    let skip = [
        "ComputeBudget111111111111111111111111111111",
        "11111111111111111111111111111111",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    ];
    program_ids
        .iter()
        .find(|p| !skip.contains(&p.as_str()))
        .cloned()
}

fn extract_program_ids(
    account_keys: &[String],
    program_id_indexes: impl Iterator<Item = u32>,
) -> Vec<String> {
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

pub async fn run_once(cfg: &Config, producer: &FutureProducer, m: &Metrics) -> Result<()> {
    let mut client = GeyserGrpcClient::build_from_shared(cfg.geyser_endpoint.clone())?
        .x_token(cfg.geyser_x_token.clone())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;

    let (mut sub_tx, mut sub_rx) = client.subscribe().await?;

    let mut tx_filters = HashMap::new();
    tx_filters.insert(
        "tx_filter".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(cfg.include_failed),
            account_required: cfg.required_accounts.clone(),
            ..Default::default()
        },
    );

    sub_tx
        .send(SubscribeRequest {
            transactions: tx_filters,
            commitment: Some(cfg.commitment as i32),
            ..Default::default()
        })
        .await?;

    info!("Subscribed. Streaming…");
    m.connected
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    while let Some(msg) = sub_rx.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!("stream error: {e:?} (will reconnect)");
                break;
            }
        };

        match msg.update_oneof {
            Some(UpdateOneof::Transaction(tx)) => {
                m.tx_seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                let Some(tx_info) = tx.transaction else {
                    continue;
                };
                let signature = bs58::encode(&tx_info.signature).into_string();

                let slot = tx.slot;
                let chain = "solana-mainnet".to_string();
                let meta = tx_info.meta.as_ref();
                let is_success = meta.and_then(|mm| mm.err.as_ref()).is_none();
                let fee_lamports = meta.map(|mm| mm.fee).unwrap_or(0);

                let message = match tx_info
                    .transaction
                    .as_ref()
                    .and_then(|t| t.message.as_ref())
                {
                    Some(mm) => mm,
                    None => continue,
                };

                let account_keys: Vec<String> = message
                    .account_keys
                    .iter()
                    .map(|k| bs58::encode(k).into_string())
                    .collect();

                let outer_indexes = message.instructions.iter().map(|ix| ix.program_id_index);
                let inner_indexes = tx_info
                    .meta
                    .as_ref()
                    .into_iter()
                    .flat_map(|mm| mm.inner_instructions.iter())
                    .flat_map(|ii| ii.instructions.iter().map(|ix| ix.program_id_index));

                let program_ids =
                    extract_program_ids(&account_keys, outer_indexes.chain(inner_indexes));
                let main_program = pick_main_program(&program_ids);

                let event = RawTxEvent {
                    schema_version: 1,
                    chain,
                    slot,
                    block_time: None,
                    signature,
                    index_in_block: 0,
                    tx_version: None,
                    is_success,
                    fee_lamports,
                    compute_units_consumed: None,
                    main_program,
                    program_ids,
                };

                let json = serde_json::to_string(&event)?;
                match kafka::send_json(producer, &cfg.kafka_topic, &json).await {
                    Ok(_) => {
                        m.send_ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => {
                        m.send_err
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        error!("kafka send failed: {e:?}");
                    }
                }
            }
            Some(UpdateOneof::Ping(_)) => {}
            _ => {}
        }
    }

    Ok(())
}
