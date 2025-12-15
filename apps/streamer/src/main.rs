use anyhow::Result;
use log::{info, warn};
use std::time::Duration;
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
    info!("using rpc_url={}", cfg.rpc_url);

    info!("streamer starting topic={} broker={}", cfg.kafka_topic, cfg.kafka_broker);
    info!(
        "endpoint={} commitment={:?} include_failed={} required_accounts={:?}",
        cfg.geyser_endpoint, cfg.commitment, cfg.include_failed, cfg.required_accounts
    );

    let producer = kafka::create_producer(&cfg.kafka_broker)?;
    let m = Metrics::new();

    let mut backoff = cfg.reconnect_min_backoff;

    loop {
        m.reconnects.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match stream::run_once(&cfg, &producer, &m).await {
            Ok(_) => {
                // stream ended normally (or errored and returned)
            }
            Err(e) => {
                warn!("run_once error: {e:?}");
            }
        }

        // periodic metrics log
        if m.bump_log_tick(Duration::from_secs(5)) {
            let (tx_seen, ok, err, reconnects) = m.snapshot();
            info!(
                "metrics tx_seen={} kafka_ok={} kafka_err={} reconnects={}",
                tx_seen, ok, err, reconnects
            );
        }

        warn!("disconnected. reconnecting in {backoff:?}");
        sleep(backoff).await;
        backoff = (backoff * 2).min(cfg.reconnect_max_backoff);
    }
}
