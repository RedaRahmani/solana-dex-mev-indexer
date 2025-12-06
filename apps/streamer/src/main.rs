use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Serialize;
use std::time::Duration;
use tokio::time::sleep;

const KAFKA_BROKER: &str = "localhost:19092";
const TOPIC: &str = "sol_raw_txs";

#[derive(Debug, Serialize)]
struct RawTxPayload {
    payload: String,
}

async fn create_producer() -> Result<FutureProducer> {
    let producer : FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKER)
        .set("message.timeout.ms", "5000")
        .create()?;
    Ok(producer)
}

async fn send_messages(
    producer: &FutureProducer,
    msg: &RawTxPayload,
) -> Result<()> {
    let json = serde_json::to_string(msg)?;

    let record = FutureRecord::<(), String>::to(TOPIC)
    .payload(&json);

    let delivery_status = producer
        .send(record, Duration::from_secs(0))
        .await;

    match delivery_status {
        Ok((_partition, _offset)) => {
            println!("✅ Sent: {json}");
        }
        Err((err, _owned_msg)) => {
            eprintln!("❌ Kafka error: {err:?}");
        }
    }

    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting streamer -> Kafka on {KAFKA_BROKER}, topic: {TOPIC}...");

    let producer = create_producer().await?;

    let messages = vec![
        "hello from streamer",
        "second message",
        "LFGGG from rust streamer",
    ];

    for (i, text) in messages.iter().enumerate(){
        let payload = RawTxPayload{
            payload : format!("#{i}: {text}"),
        };
        send_messages(&producer, &payload).await?;
        sleep(Duration::from_millis(500)).await;
    }
    println!("Done.");
    Ok(())
}