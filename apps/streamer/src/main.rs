use anyhow::Result;
use log::{info, warn};
use std::sync::atomic::Ordering;
use std::time::Duration;
use rdkafka::producer::Producer;
use tokio::time::sleep;

mod config;
mod kafka;
mod metrics;
mod stream;

use config::Config;
use metrics::Metrics;

fn setup_logging() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    setup_logging();

    let cfg: Config = config::load()?;

    info!("streamer starting topic={} broker={}", cfg.kafka_topic, cfg.kafka_broker);
    info!(
        "endpoint={} commitment={:?} include_failed={} required_accounts={:?}",
        cfg.geyser_endpoint, cfg.commitment, cfg.include_failed, cfg.required_accounts
    );

    let producer = kafka::create_producer(&cfg.kafka_broker)?;
    let m = std::sync::Arc::new(Metrics::new());

    // ---- Background metrics logger (prints even when stream is healthy) ----
    {
        let m = m.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                let (tx_seen, ok, err, reconnects, connected) = m.snapshot();
                info!(
                    "metrics tx_seen={} kafka_ok={} kafka_err={} reconnects={} connected={}",
                    tx_seen, ok, err, reconnects, connected
                );
            }
        });
    }

    let mut backoff = cfg.reconnect_min_backoff;
    let mut last_connected = 0u64;

    info!("starting main loop (Ctrl+C to stop)");

    loop {
        // Allow clean shutdown
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                warn!("shutdown signal received (Ctrl+C). flushing Kafka producer...");
                producer.flush(Duration::from_secs(10));
                warn!("shutdown complete.");
                break;
            }

            res = async {
                m.reconnects.fetch_add(1, Ordering::Relaxed);

                match stream::run_once(&cfg, &producer, &m).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            } => {
                if let Err(e) = res {
                    warn!("run_once error: {e:?}");
                }

                // Reset backoff if we managed to subscribe at least once since last loop
                let now_connected = m.connected.load(Ordering::Relaxed);
                if now_connected > last_connected {
                    backoff = cfg.reconnect_min_backoff;
                    last_connected = now_connected;
                }

                warn!("disconnected. reconnecting in {backoff:?}");
                sleep(backoff).await;
                backoff = (backoff * 2).min(cfg.reconnect_max_backoff);
            }
        }
    }

    Ok(())
}
