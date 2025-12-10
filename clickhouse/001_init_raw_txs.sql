-- Create raw Solana transactions schema: Kafka → MV → MergeTree

CREATE DATABASE IF NOT EXISTS solana;

-- 1) Kafka engine table: reads JSON RawTxEvent from Kafka topic `sol_raw_txs`
CREATE TABLE IF NOT EXISTS solana.sol_raw_txs_queue
(
    schema_version UInt8,
    chain String,
    slot UInt64,
    block_time Nullable(Int64),
    signature String,
    index_in_block UInt32,
    tx_version Nullable(UInt8),
    is_success Bool,
    fee_lamports UInt64,
    compute_units_consumed Nullable(UInt64),
    main_program Nullable(String),
    program_ids Array(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'sol_raw_txs',
    kafka_group_name = 'sol_dex_mev_indexer_v2',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- 2) Main storage table: typed, queryable, persisted raw tx events
CREATE TABLE IF NOT EXISTS solana.sol_raw_txs
(
    ts DateTime DEFAULT now(),
    schema_version UInt8,
    chain String,
    slot UInt64,
    block_time Nullable(Int64),
    signature String,
    index_in_block UInt32,
    tx_version Nullable(UInt8),
    is_success Bool,
    fee_lamports UInt64,
    compute_units_consumed Nullable(UInt64),
    main_program Nullable(String),
    program_ids Array(String)
)
ENGINE = MergeTree
ORDER BY (slot, signature);

-- 3) Materialized view: pipes events from Kafka table into MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS solana.sol_raw_txs_mv
TO solana.sol_raw_txs
AS
SELECT
    now() AS ts,
    schema_version,
    chain,
    slot,
    block_time,
    signature,
    index_in_block,
    tx_version,
    is_success,
    fee_lamports,
    compute_units_consumed,
    main_program,
    program_ids
FROM solana.sol_raw_txs_queue;
