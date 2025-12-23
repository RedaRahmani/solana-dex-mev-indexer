use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub fn create_producer(broker: &str) -> Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", "10")
        .set("message.timeout.ms", "60000")
        .set("retries", "10")
        .create()?;
    Ok(producer)
}

pub async fn send_json(
    producer: &FutureProducer,
    topic: &str,
    key: Option<&str>,
    json: &str,
) -> Result<()> {
    let mut rec = FutureRecord::<str, str>::to(topic).payload(json);
    if let Some(k) = key {
        rec = rec.key(k);
    }

    let _ = producer.send(rec, Duration::from_secs(5)).await;
    Ok(())
}
