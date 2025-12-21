use anyhow::{Result, anyhow};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub fn create_producer(broker: &str) -> Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("compression.type", "lz4")
        .set("linger.ms", "10")
        .set("message.timeout.ms", "60000")
        .set("retries", "10")
        .create()?;
    Ok(producer)
}

pub async fn send_json(producer: &FutureProducer, topic: &str, json: &str) -> Result<()> {
    let record = FutureRecord::<(), str>::to(topic).payload(json);

    match producer.send(record, Duration::from_secs(5)).await {
        Ok((_p, _o)) => Ok(()),
        Err((e, _)) => Err(anyhow!("Kafka delivery error: {e:?}")),
    }
}
