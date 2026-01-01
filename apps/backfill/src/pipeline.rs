use crate::{
    kafka,
    rpc::RpcClient,
    types::{DlqEvent, RawTxEvent},
};
use anyhow::{Result, anyhow};
use futures::{StreamExt, stream};
use log::{info, warn};
use rdkafka::producer::FutureProducer;
use serde_json::{Value, json};
use std::{
    collections::hash_map::DefaultHasher,
    fs::OpenOptions,
    hash::{Hash, Hasher},
    io::Write,
    path::Path,
    time::Duration,
};
use tokio::time::sleep;

// Import ALT-aware helpers from schema crate
use schema::{extract_program_ids_from_transaction, pick_main_program};

// Note: extract_program_ids_from_tx and pick_main_program moved to schema crate
// to support Address Lookup Table (ALT) resolution for v0 transactions.

fn is_rate_limited_429(err_dbg: &str) -> bool {
    // Your logs show: "status=429 Too Many Requests"
    err_dbg.contains("status=429")
        || err_dbg.contains("Too Many Requests")
        || err_dbg.contains("\"code\":429")
}

fn jitter_ms(sig: &str, attempt: usize) -> u64 {
    // deterministic jitter (no extra deps)
    let mut h = DefaultHasher::new();
    sig.hash(&mut h);
    attempt.hash(&mut h);
    h.finish() % 200 // 0..199ms
}

async fn get_transaction_with_retry(
    rpc: &RpcClient,
    sig: &str,
    max_retries: usize,
    mut backoff: Duration,
    max_backoff: Duration,
) -> Result<(Value, usize)> {
    let mut retries_429 = 0usize;

    for attempt in 0..=max_retries {
        let res = rpc
            .call(
                "getTransaction",
                json!([
                    sig,
                    {
                        "encoding": "json",
                        "maxSupportedTransactionVersion": 0
                    }
                ]),
            )
            .await;

        match res {
            Ok(v) => return Ok((v, retries_429)),
            Err(e) => {
                let dbg = format!("{e:?}");
                let is_429 = is_rate_limited_429(&dbg);

                if !is_429 || attempt == max_retries {
                    return Err(anyhow!("{dbg}"));
                }

                retries_429 += 1;
                let j = Duration::from_millis(jitter_ms(sig, attempt));
                let sleep_for = (backoff + j).min(max_backoff);

                warn!(
                    "rate-limited (429) sig={} attempt={} sleeping={:?}",
                    sig,
                    attempt + 1,
                    sleep_for
                );

                sleep(sleep_for).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }

    unreachable!()
}

#[allow(clippy::too_many_arguments)]
pub async fn backfill_record(
    rpc: &RpcClient,
    producer: &FutureProducer,
    kafka_topic: &str,
    dlq_topic: &str,
    chain: &str,
    address: &str,
    limit: usize,
    concurrency: usize,
    out_path: &Path,
) -> Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(out_path)?;

    info!(
        "backfill: address={} limit={} concurrency={} rpc={}",
        address, limit, concurrency, "public"
    );
    info!("recording raw tx responses to {}", out_path.display());

    // Step A: page signatures
    let mut signatures: Vec<String> = Vec::with_capacity(limit);
    let mut before: Option<String> = None;

    while signatures.len() < limit {
        let page_size = std::cmp::min(1000, limit - signatures.len());

        let mut opts = json!({ "limit": page_size });
        if let Some(b) = &before {
            opts["before"] = json!(b);
        }

        let res = rpc
            .call("getSignaturesForAddress", json!([address, opts]))
            .await
            .map_err(|e| anyhow!("getSignaturesForAddress failed: {e:?}"))?;

        let arr = res
            .as_array()
            .ok_or_else(|| anyhow!("unexpected signatures result"))?;
        if arr.is_empty() {
            break;
        }

        for item in arr {
            if let Some(sig) = item.get("signature").and_then(|v| v.as_str()) {
                signatures.push(sig.to_string());
            }
        }

        before = arr
            .last()
            .and_then(|x| x.get("signature"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        info!("collected signatures: {}", signatures.len());
    }

    info!("fetching {} transactions…", signatures.len());

    // Step B: fetch transactions concurrently
    let rpc2 = rpc.clone();
    let chain = chain.to_string();

    // counters (for visibility)
    let mut ok = 0usize;
    let mut err = 0usize;
    let mut retries_429_total = 0usize;
    let mut logged_schema = false; // schema validation flag

    // tune these if needed
    let max_retries = 6usize;
    let base_backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(5);

    let mut stream = stream::iter(signatures.into_iter())
        .map(move |sig| {
            let rpc = rpc2.clone();
            let sig2 = sig.clone();
            let chain = chain.clone();
            async move {
                let tx =
                    get_transaction_with_retry(&rpc, &sig2, max_retries, base_backoff, max_backoff)
                        .await;
                (sig, chain, tx)
            }
        })
        .buffer_unordered(concurrency);

    while let Some((sig, chain, tx_res)) = stream.next().await {
        match tx_res {
            Ok((tx, retries_429)) => {
                ok += 1;
                retries_429_total += retries_429;

                // record raw response line
                let line = serde_json::to_string(&json!({ "signature": sig, "tx": tx }))?;
                writeln!(f, "{line}")?;

                // build RawTxEvent (best-effort)
                let slot = tx.get("slot").and_then(|v| v.as_u64()).unwrap_or(0);
                let block_time = tx.get("blockTime").and_then(|v| v.as_i64());

                let fee = tx
                    .pointer("/meta/fee")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                let is_success = tx.pointer("/meta/err").is_none();

                // Use ALT-aware extraction from schema crate
                let program_ids = extract_program_ids_from_transaction(&tx);
                let main_program = pick_main_program(&program_ids);

                // guard: never emit empty signature
                if sig.is_empty() || slot == 0 {
                    let dlq = DlqEvent {
                        source: "backfill".to_string(),
                        step: "parse".to_string(),
                        signature: Some(sig),
                        error: "empty signature or slot=0".to_string(),
                    };
                    let j = serde_json::to_string(&dlq)?;
                    kafka::send_json(producer, dlq_topic, None, &j).await?;
                    continue;
                }

                let event = RawTxEvent {
                    schema_version: 1,
                    chain,
                    slot,
                    block_time,
                    signature: sig.clone(),
                    index_in_block: 0,
                    tx_version: None,
                    is_success,
                    fee_lamports: fee,
                    compute_units_consumed: None,
                    main_program,
                    program_ids,
                };

                let json_event = serde_json::to_string(&event)?;

                // Log first produced RawTxEvent schema
                if !logged_schema {
                    let schema_sample = serde_json::to_string_pretty(&event).unwrap_or_default();
                    info!(
                        "🔍 First RawTxEvent produced schema sample:\n{}",
                        schema_sample
                    );
                    logged_schema = true;
                }

                kafka::send_json(producer, kafka_topic, Some(&sig), &json_event).await?;
            }
            Err(e) => {
                err += 1;
                warn!("getTransaction failed sig={sig}: {e:?}");

                let dlq = DlqEvent {
                    source: "backfill".to_string(),
                    step: "getTransaction".to_string(),
                    signature: Some(sig),
                    error: format!("{e:?}"),
                };
                let j = serde_json::to_string(&dlq)?;
                kafka::send_json(producer, dlq_topic, None, &j).await?;
            }
        }

        // periodic progress
        let done = ok + err;
        if done.is_multiple_of(100) {
            info!(
                "progress fetched={} ok={} err={} retries_429_total={}",
                done, ok, err, retries_429_total
            );
        }
    }

    info!(
        "backfill done. fetched={} ok={} err={} retries_429_total={}",
        ok + err,
        ok,
        err,
        retries_429_total
    );
    Ok(())
}
