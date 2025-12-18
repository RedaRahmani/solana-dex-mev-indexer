use anyhow::{anyhow, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{ StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub fn create_consumer(broker: &str, group: &str) -> Result<StreamConsumer> {
    let c: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("group.id", group)
        .set("enable.auto.commit", "false") // we commit only after we successfully publish outputs
        .set("auto.offset.reset", "earliest")
        .create()?;
    Ok(c)
}

pub fn create_producer(broker: &str) -> Result<FutureProducer> {
    let p: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", "10")
        .set("message.timeout.ms", "60000")
        .set("retries", "10")
        .create()?;
    Ok(p)
}

pub async fn send_json(producer: &FutureProducer, topic: &str, key: &str, json: &str) -> Result<()> {
    let rec = FutureRecord::<str, str>::to(topic).key(key).payload(json);
    match producer.send(rec, Duration::from_secs(10)).await {
        Ok(_) => Ok(()),
        Err((e, _)) => Err(anyhow!("kafka delivery error: {e:?}")),
    }
}

pub fn msg_to_str<M: Message>(msg: &M) -> Result<&str> {
    msg.payload_view::<str>()
        .transpose()
        .map_err(|e| anyhow!("invalid utf8 payload: {e:?}"))?
        .ok_or_else(|| anyhow!("empty payload"))
}
