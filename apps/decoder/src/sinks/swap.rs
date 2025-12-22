use anyhow::Result;
use rdkafka::producer::{FutureProducer, FutureRecord};
use schema::SwapEvent;
use std::time::Duration;

pub async fn send_swap(producer: &FutureProducer, topic: &str, swap: &SwapEvent) -> Result<()> {
    let payload = serde_json::to_string(swap)?;
    let key = &swap.signature;
    let record = FutureRecord::to(topic).key(key).payload(&payload);

    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(err, _)| anyhow::anyhow!("Failed to send swap event: {:?}", err))?;
    Ok(())
}
