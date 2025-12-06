CREATE DATABASE IF NOT EXISTS solana;

-- 1) Kafka engine table consuming from topic `sol_raw_txs`
CREATE TABLE IF NOT EXISTS solana.sol_raw_txs_queue (
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'sol_raw_txs',
    kafka_group_name = 'sol_dex_mev_indexer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- 2) Target storage table (simple for now)
CREATE TABLE IF NOT EXISTS solana.sol_raw_txs (
    ts DateTime DEFAULT now(),
    payload String
)
ENGINE = MergeTree
ORDER BY (ts);

-- 3) Materialized view to pipe Kafka → MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS solana.sol_raw_txs_mv
TO solana.sol_raw_txs
AS
SELECT
    now() AS ts,
    payload
FROM solana.sol_raw_txs_queue;
