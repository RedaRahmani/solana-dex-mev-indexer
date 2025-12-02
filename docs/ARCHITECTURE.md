# Architecture (v0 draft)

Solana (local validator / Yellowstone gRPC)
    ↓
Kafka topic: `sol_raw_txs`
    ↓
ClickHouse Kafka Engine table + MATERIALIZED VIEW
    ↓
`dex_swaps` / `wallet_activity` tables
    ↓
HTTP API + dashboards
