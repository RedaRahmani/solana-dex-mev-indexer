# solana-dex-mev-indexer

Low-latency Solana DEX & MEV indexer using Geyser/Yellowstone → Kafka → ClickHouse with real-time APIs.

## High-level

- Ingest Solana blocks/txs from a Geyser/Yellowstone stream
- Push normalized events into Kafka
- Persist to ClickHouse via Kafka Engine + materialized views
- Expose HTTP APIs & dashboards for DEX/MEV analytics
