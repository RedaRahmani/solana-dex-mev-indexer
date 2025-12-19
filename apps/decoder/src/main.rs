use anyhow::Result;
use log::{debug, info, warn};
use rdkafka::consumer::Consumer;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::sleep;

mod config;
mod decode;
mod kafka;
mod rpc;
mod types;

use config::Config;
use rpc::RpcClient;
use types::RawTxEvent;

fn setup_logging() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    setup_logging();

    let cfg: Config = config::load()?;

    // Log comprehensive config on startup
    info!("decoder starting:");
    info!("  kafka_broker={}", cfg.kafka_broker);
    info!("  in_topic={}", cfg.in_topic);
    info!("  out_sol_deltas={}", cfg.out_sol_deltas_topic);
    info!("  out_token_deltas={}", cfg.out_token_deltas_topic);
    if let Some(ref dlq) = cfg.dlq_topic {
        info!("  dlq_topic={}", dlq);
    }
    info!("  consumer_group={}", cfg.consumer_group);
    info!("  rpc_primary={}", cfg.rpc_primary_url);
    info!("  rpc_fallback_count={}", cfg.rpc_fallback_urls.len());
    if !cfg.rpc_fallback_urls.is_empty() {
        info!("  rpc_fallbacks={:?}", cfg.rpc_fallback_urls);
    }
    info!("  rpc_concurrency={}", cfg.rpc_concurrency);
    info!("  rpc_min_delay_ms={}", cfg.rpc_min_delay_ms);
    info!("  rpc_max_tx_version={}", cfg.rpc_max_tx_version);

    let consumer = kafka::create_consumer(&cfg.kafka_broker, &cfg.consumer_group)?;
    consumer.subscribe(&[&cfg.in_topic])?;

    let producer = kafka::create_producer(&cfg.kafka_broker)?;
    let rpc = RpcClient::new(
        cfg.rpc_primary_url.clone(),
        cfg.rpc_fallback_urls.clone(),
        cfg.rpc_concurrency,
        cfg.rpc_min_delay_ms,
        cfg.rpc_max_tx_version,
    );

    let processed = AtomicU64::new(0);
    let sol_deltas_produced = AtomicU64::new(0);
    let token_deltas_produced = AtomicU64::new(0);
    let errors = AtomicU64::new(0);

    loop {
        match consumer.recv().await {
            Err(e) => {
                warn!("consumer error: {e:?}");
                sleep(Duration::from_millis(200)).await;
                continue;
            }
            Ok(msg) => {
                let payload = match kafka::msg_to_str(&msg) {
                    Ok(p) => p,
                    Err(e) => {
                        warn!("bad payload: {e:?}");
                        errors.fetch_add(1, Ordering::Relaxed);
                        // commit anyway to avoid poison-pill loops
                        let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                        continue;
                    }
                };

                let evt: RawTxEvent = match serde_json::from_str(payload) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("json parse fail: {e:?}");
                        errors.fetch_add(1, Ordering::Relaxed);
                        let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                        continue;
                    }
                };

                processed.fetch_add(1, Ordering::Relaxed);

                // Fetch full tx from RPC
                let tx = match rpc.get_transaction_json_parsed(&evt.signature).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("rpc getTransaction failed sig={} err={e:?}", evt.signature);
                        errors.fetch_add(1, Ordering::Relaxed);
                        // do not commit -> we want to retry later (at-least-once)
                        continue;
                    }
                };

                // Decode facts
                let sol_deltas =
                    decode::decode_sol_deltas(evt.slot, evt.block_time, &evt.signature, &tx);
                let tok_deltas =
                    decode::decode_token_deltas(evt.slot, evt.block_time, &evt.signature, &tx);

                // Debug log: if token deltas are empty but token balances exist
                if tok_deltas.is_empty() {
                    let (pre_len, post_len, _) = decode::inspect_token_balances(&tx);
                    if pre_len > 0 || post_len > 0 {
                        debug!(
                            "tx {} has token balances (pre={}, post={}) but produced 0 deltas",
                            evt.signature, pre_len, post_len
                        );
                    }
                }

                // Publish facts
                let sol_count = sol_deltas.len();
                for d in sol_deltas {
                    let json = serde_json::to_string(&d)?;
                    kafka::send_json(&producer, &cfg.out_sol_deltas_topic, &evt.signature, &json)
                        .await?;
                }
                sol_deltas_produced.fetch_add(sol_count as u64, Ordering::Relaxed);

                let tok_count = tok_deltas.len();
                for d in tok_deltas {
                    let json = serde_json::to_string(&d)?;
                    kafka::send_json(
                        &producer,
                        &cfg.out_token_deltas_topic,
                        &evt.signature,
                        &json,
                    )
                    .await?;
                }
                token_deltas_produced.fetch_add(tok_count as u64, Ordering::Relaxed);

                // Commit offset only after successful publish
                let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);

                // periodic log with detailed breakdown
                let proc_count = processed.load(Ordering::Relaxed);
                if proc_count % 200 == 0 {
                    let sol_prod = sol_deltas_produced.load(Ordering::Relaxed);
                    let tok_prod = token_deltas_produced.load(Ordering::Relaxed);
                    let total_prod = sol_prod + tok_prod;
                    let err_count = errors.load(Ordering::Relaxed);
                    info!(
                        "stats: processed={} sol_deltas={} token_deltas={} total_produced={} errors={}",
                        proc_count, sol_prod, tok_prod, total_prod, err_count
                    );
                }
            }
        }
    }
}
