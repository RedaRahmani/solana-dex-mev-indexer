use anyhow::Result;
use clap::Parser;
use log::info;

mod config;
mod kafka;
mod pipeline;
mod replay;
mod rpc;
mod types;

fn setup_logging() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    setup_logging();

    let cli = config::Cli::parse();
    let cfg = config::load(&cli)?;

    info!("backfill starting:");
    info!("  profile={:?}", cfg.profile);
    info!("  rpc_url={}", cfg.rpc_url);
    info!("  kafka_broker={}", cfg.kafka_broker);
    info!("  kafka_topic={}", cfg.kafka_topic);
    info!("  dlq_topic={}", cfg.dlq_topic);

    let producer = kafka::create_producer(&cfg.kafka_broker)?;

    // Ensure data dir exists if using --out data/...
    if let Some(out) = &cli.out
        && let Some(parent) = out.parent()
    {
        std::fs::create_dir_all(parent)?;
    }

    info!(
        "mode: {}",
        if cli.from_file.is_some() {
            "replay"
        } else {
            "backfill"
        }
    );

    if let Some(from) = cli.from_file {
        replay::replay_file(
            &producer,
            &cfg.kafka_topic,
            &cfg.dlq_topic,
            &cfg.chain,
            &from,
        )
        .await?;
        return Ok(());
    }

    // backfill/record mode — verify genesis if mainnet
    if cfg.profile == schema::Profile::Mainnet {
        info!("  verifying mainnet genesis hash...");
        schema::verify_mainnet_genesis(&cfg.rpc_url).await?;
        info!("  genesis hash verified ✓");
    }

    let rpc = rpc::RpcClient::new(cfg.rpc_url.clone());

    let out = cli.out.expect("--out required in backfill mode");
    pipeline::backfill_record(
        &rpc,
        &producer,
        &cfg.kafka_topic,
        &cfg.dlq_topic,
        &cfg.chain,
        &cli.address,
        cli.limit,
        cli.concurrency,
        &out,
    )
    .await?;

    Ok(())
}
