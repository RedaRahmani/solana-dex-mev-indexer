use anyhow::Result;
use log::{debug, info, warn};
use rdkafka::consumer::Consumer;
use std::collections::HashMap;
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

// Retry budget: max attempts before committing and moving on (with optional DLQ)
const MAX_ATTEMPTS: u32 = 3;
const MAX_FAILURE_MAP_SIZE: usize = 10000;
const BASE_BACKOFF_MS: u64 = 200;

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
    info!("  include_failed={}", cfg.include_failed);

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
    let skipped_failed = AtomicU64::new(0);
    let dlq_sent = AtomicU64::new(0);

    // Schema validation: log first message of each type (rate-limited)
    let mut logged_raw_tx_schema = false;
    let mut logged_sol_delta_schema = false;
    let mut logged_token_delta_schema = false;

    // Retry budget: track failure count per signature to prevent poison-pill stalls
    let mut failure_counts: HashMap<String, u32> = HashMap::new();

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
                        // commit to avoid poison-pill loops
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

                // Log first consumed RawTxEvent schema
                if !logged_raw_tx_schema {
                    let schema_sample = serde_json::to_string_pretty(&serde_json::json!({
                        "schema_version": evt.schema_version,
                        "chain": &evt.chain,
                        "slot": evt.slot,
                        "block_time": evt.block_time,
                        "signature": &evt.signature,
                        "index_in_block": evt.index_in_block,
                        "tx_version": evt.tx_version,
                        "is_success": evt.is_success,
                        "fee_lamports": evt.fee_lamports,
                        "compute_units_consumed": evt.compute_units_consumed,
                        "main_program": &evt.main_program,
                        "program_ids_count": evt.program_ids.len(),
                    }))
                    .unwrap_or_default();
                    info!("🔍 First RawTxEvent schema sample:\n{}", schema_sample);
                    logged_raw_tx_schema = true;
                }

                processed.fetch_add(1, Ordering::Relaxed);

                // Skip failed txs unless explicitly enabled
                if !cfg.include_failed && !evt.is_success {
                    skipped_failed.fetch_add(1, Ordering::Relaxed);

                    let proc_count = processed.load(Ordering::Relaxed);
                    if proc_count.is_multiple_of(200) {
                        debug!(
                            "skipping failed txs (include_failed=false); last_skipped_sig={}",
                            evt.signature
                        );
                    }

                    let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                    continue;
                }

                // Fetch full tx from RPC
                let tx = match rpc.get_transaction_json_parsed(&evt.signature).await {
                    Ok(v) => {
                        // Success: clear any failure tracking for this signature
                        failure_counts.remove(&evt.signature);
                        v
                    }
                    Err(e) => {
                        errors.fetch_add(1, Ordering::Relaxed);

                        // Track failure attempts to prevent poison-pill stalls
                        // Compute attempts_now in a scope to avoid borrow checker issues
                        let attempts_now = {
                            let attempts = failure_counts.entry(evt.signature.clone()).or_insert(0);
                            *attempts += 1;
                            *attempts
                        };

                        // Guard against unbounded map growth
                        if failure_counts.len() > MAX_FAILURE_MAP_SIZE {
                            warn!(
                                "failure_counts map exceeded {}, clearing old entries",
                                MAX_FAILURE_MAP_SIZE
                            );
                            failure_counts.clear();
                        }

                        if attempts_now < MAX_ATTEMPTS {
                            // Transient failure: apply backoff and retry later (do NOT commit)
                            let backoff_ms = BASE_BACKOFF_MS * (attempts_now as u64);
                            warn!(
                                "rpc getTransaction failed sig={} attempt={}/{} err={e:?} (retrying after {}ms)",
                                evt.signature, attempts_now, MAX_ATTEMPTS, backoff_ms
                            );
                            sleep(Duration::from_millis(backoff_ms)).await;
                            continue;
                        } else {
                            // Permanent failure: send to DLQ if configured, then commit to unblock
                            warn!(
                                "rpc getTransaction failed sig={} after {} attempts, moving to DLQ/commit: {e:?}",
                                evt.signature, attempts_now
                            );

                            // Send to DLQ if configured
                            if let Some(ref dlq_topic) = cfg.dlq_topic {
                                let dlq_payload = serde_json::json!({
                                    "reason": "rpc_getTransaction_failed",
                                    "attempts": attempts_now,
                                    "error": format!("{e:?}"),
                                    "signature": evt.signature,
                                    "slot": evt.slot,
                                    "block_time": evt.block_time,
                                    "chain": evt.chain,
                                });
                                let dlq_json = serde_json::to_string(&dlq_payload)?;
                                match kafka::send_json(
                                    &producer,
                                    dlq_topic,
                                    &evt.signature,
                                    &dlq_json,
                                )
                                .await
                                {
                                    Ok(_) => {
                                        dlq_sent.fetch_add(1, Ordering::Relaxed);
                                        debug!(
                                            "sent poison-pill sig={} to DLQ after {} attempts",
                                            evt.signature, attempts_now
                                        );
                                    }
                                    Err(dlq_err) => {
                                        warn!(
                                            "failed to send to DLQ sig={}: {dlq_err:?}",
                                            evt.signature
                                        );
                                    }
                                }
                            }

                            // CRITICAL: commit offset to unblock consumer (at-least-once preserved for transient errors)
                            let _ =
                                consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
                            failure_counts.remove(&evt.signature);
                            continue;
                        }
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

                    // Log first SOL delta schema
                    if !logged_sol_delta_schema {
                        let schema_sample = serde_json::to_string_pretty(&d).unwrap_or_default();
                        info!("🔍 First SolBalanceDelta schema sample:\n{}", schema_sample);
                        logged_sol_delta_schema = true;
                    }

                    kafka::send_json(&producer, &cfg.out_sol_deltas_topic, &evt.signature, &json)
                        .await?;
                }
                sol_deltas_produced.fetch_add(sol_count as u64, Ordering::Relaxed);

                let tok_count = tok_deltas.len();
                for d in tok_deltas {
                    let json = serde_json::to_string(&d)?;

                    // Log first token delta schema
                    if !logged_token_delta_schema {
                        let schema_sample = serde_json::to_string_pretty(&d).unwrap_or_default();
                        info!(
                            "🔍 First TokenBalanceDelta schema sample:\n{}",
                            schema_sample
                        );
                        logged_token_delta_schema = true;
                    }

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
                if proc_count.is_multiple_of(200) {
                    let sol_prod = sol_deltas_produced.load(Ordering::Relaxed);
                    let tok_prod = token_deltas_produced.load(Ordering::Relaxed);
                    let total_prod = sol_prod + tok_prod;
                    let err_count = errors.load(Ordering::Relaxed);
                    let dlq_count = dlq_sent.load(Ordering::Relaxed);
                    let pending_retries = failure_counts.len();
                    info!(
                        "stats: processed={} sol_deltas={} token_deltas={} total_produced={} errors={} dlq_sent={} pending_retries={}",
                        proc_count,
                        sol_prod,
                        tok_prod,
                        total_prod,
                        err_count,
                        dlq_count,
                        pending_retries
                    );
                }
            }
        }
    }
}
