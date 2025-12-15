use crate::{kafka, types::{DlqEvent, RawTxEvent}};
use anyhow::{anyhow, Result};
use log::info;
use rdkafka::producer::FutureProducer;
use serde_json::Value;
use std::{fs::File, io::{BufRead, BufReader}, path::Path};

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

    for line in r.lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }

        let v: Value = serde_json::from_str(&line)?;
        let sig = v.get("signature").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let tx = v.get("tx").cloned().ok_or_else(|| anyhow!("missing tx field"))?;

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

        let fee = tx.pointer("/meta/fee").and_then(|v| v.as_u64()).unwrap_or(0);
        let is_success = tx.pointer("/meta/err").is_none();
        let block_time = tx.get("blockTime").and_then(|v| v.as_i64());

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
            main_program: None,
            program_ids: vec![],
        };

        let json_event = serde_json::to_string(&event)?;
        kafka::send_json(producer, kafka_topic, Some(&sig), &json_event).await?;
        count += 1;
    }

    info!("replay published {} events", count);
    Ok(())
}
