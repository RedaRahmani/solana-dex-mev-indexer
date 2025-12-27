//! Sink for DexSwapV1 events to Kafka

use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use schema::DexSwapV1;
use std::time::Duration;

/// Send a DexSwapV1 to Kafka
pub async fn send_dex_swap_v1(
    producer: &FutureProducer,
    topic: &str,
    swap: &DexSwapV1,
) -> Result<()> {
    let payload = serde_json::to_string(swap)?;
    let key = &swap.signature;
    let record = FutureRecord::to(topic).key(key).payload(&payload);

    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _)| anyhow::anyhow!("Failed to send DexSwapV1 event: {:?}", err))?;
    Ok(())
}
