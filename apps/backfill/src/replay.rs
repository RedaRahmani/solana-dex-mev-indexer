use crate::{
    kafka,
    types::{DlqEvent, RawTxEvent},
};
use anyhow::{Result, anyhow};
use log::info;
use rdkafka::producer::FutureProducer;
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};
use std::collections::HashSet;

pub async fn replay_file(
    producer: &FutureProducer,
    kafka_topic: &str,
    dlq_topic: &str,
    chain: &str,
    path: &Path,
) -> Result<()> {
    info!("replay from {}", path.display());

    let f = File::open(path)?;
    let r = BufReader::new(f);

    let mut count = 0usize;
    let mut logged_schema = false; // schema validation flag

    for line in r.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let v: Value = serde_json::from_str(&line)?;
        let sig = v
            .get("signature")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let tx = v
            .get("tx")
            .cloned()
            .ok_or_else(|| anyhow!("missing tx field"))?;

        let slot = tx.get("slot").and_then(|v| v.as_u64()).unwrap_or(0);
        if sig.is_empty() || slot == 0 {
            let dlq = DlqEvent {
                source: "backfill".to_string(),
                step: "replay-parse".to_string(),
                signature: Some(sig),
                error: "empty signature or slot=0".to_string(),
            };
            let j = serde_json::to_string(&dlq)?;
            kafka::send_json(producer, dlq_topic, None, &j).await?;
            continue;
        }

        let fee = tx
            .pointer("/meta/fee")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let is_success = tx.pointer("/meta/err").is_none();
        let block_time = tx.get("blockTime").and_then(|v| v.as_i64());
        let program_ids = extract_program_ids_from_tx(&tx);
        let main_program = program_ids.first().cloned();

        // Keep replay simple: reuse same extraction as backfill by emitting only core fields
        let event = RawTxEvent {
            schema_version: 1,
            chain: chain.to_string(),
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
                "🔍 First RawTxEvent (replay) schema sample:\n{}",
                schema_sample
            );
            logged_schema = true;
        }

        kafka::send_json(producer, kafka_topic, Some(&sig), &json_event).await?;
        count += 1;
    }

    info!("replay published {} events", count);
    Ok(())
}


fn extract_program_ids_from_tx(tx: &Value) -> Vec<String> {
    let msg = match tx.pointer("/transaction/message") {
        Some(m) => m,
        None => return vec![],
    };

    let account_keys: Vec<String> = msg
        .get("accountKeys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| {
                    if x.is_string() {
                        x.as_str().map(|s| s.to_string())
                    } else {
                        x.get("pubkey")
                            .and_then(|p| p.as_str())
                            .map(|s| s.to_string())
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    // helper: add pid if not seen
    fn push_pid(out: &mut Vec<String>, seen: &mut HashSet<String>, pid: &str) {
        if seen.insert(pid.to_string()) {
            out.push(pid.to_string());
        }
    }

    // helper: resolve programIdIndex into account_keys, then add
    fn push_idx(
        out: &mut Vec<String>,
        seen: &mut HashSet<String>,
        account_keys: &[String],
        idx: i64,
    ) {
        if idx < 0 {
            return;
        }
        let i = idx as usize;
        if i < account_keys.len() {
            let pid = &account_keys[i];
            if seen.insert(pid.clone()) {
                out.push(pid.clone());
            }
        }
    }

    // outer instructions (jsonParsed + raw)
    if let Some(ixs) = msg.get("instructions").and_then(|v| v.as_array()) {
        for ix in ixs {
            if let Some(pid) = ix.get("programId").and_then(|v| v.as_str()) {
                push_pid(&mut out, &mut seen, pid);
                continue;
            }
            if let Some(i) = ix.get("programIdIndex").and_then(|v| v.as_i64()) {
                push_idx(&mut out, &mut seen, &account_keys, i);
            }
        }
    }

    // inner instructions (jsonParsed + raw)
    if let Some(inner) = tx.pointer("/meta/innerInstructions").and_then(|v| v.as_array()) {
        for ii in inner {
            if let Some(ixs) = ii.get("instructions").and_then(|v| v.as_array()) {
                for ix in ixs {
                    if let Some(pid) = ix.get("programId").and_then(|v| v.as_str()) {
                        push_pid(&mut out, &mut seen, pid);
                        continue;
                    }
                    if let Some(i) = ix.get("programIdIndex").and_then(|v| v.as_i64()) {
                        push_idx(&mut out, &mut seen, &account_keys, i);
                    }
                }
            }
        }
    }

    out
}
