-- Decoder tables: Kafka → MV → MergeTree

CREATE DATABASE IF NOT EXISTS solana;

-- =========================
-- 1) SOL balance deltas
-- =========================

-- Kafka queue table (topic: sol_balance_deltas)
CREATE TABLE IF NOT EXISTS solana.sol_balance_deltas_queue
(
  slot UInt64,
  block_time Nullable(Int64),
  signature String,
  account String,
  pre_balance UInt64,
  post_balance UInt64,
  delta Int64
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'sol_balance_deltas',
  kafka_group_name = 'sol_dex_mev_indexer_decoder_v1',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

-- Storage table
CREATE TABLE IF NOT EXISTS solana.sol_balance_deltas
(
  ts DateTime DEFAULT now(),
  slot UInt64,
  block_time Nullable(Int64),
  signature String,
  account String,
  pre_balance UInt64,
  post_balance UInt64,
  delta Int64
)
ENGINE = MergeTree
ORDER BY (account, slot, signature);

-- Materialized view (pipe Kafka -> storage)
CREATE MATERIALIZED VIEW IF NOT EXISTS solana.sol_balance_deltas_mv
TO solana.sol_balance_deltas
AS
SELECT
  now() AS ts,
  slot,
  block_time,
  signature,
  account,
  pre_balance,
  post_balance,
  delta
FROM solana.sol_balance_deltas_queue;


-- =========================
-- 2) SPL token balance deltas
-- =========================

-- Kafka queue table (topic: sol_token_balance_deltas)
CREATE TABLE IF NOT EXISTS solana.sol_token_balance_deltas_queue
(
  slot UInt64,
  block_time Nullable(Int64),
  signature String,
  account_index UInt32,
  mint String,
  decimals Nullable(UInt8),
  pre_amount UInt64,
  post_amount UInt64,
  delta Int64
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'sol_token_balance_deltas',
  kafka_group_name = 'sol_dex_mev_indexer_decoder_v1',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

-- Storage table
CREATE TABLE IF NOT EXISTS solana.sol_token_balance_deltas
(
  ts DateTime DEFAULT now(),
  slot UInt64,
  block_time Nullable(Int64),
  signature String,
  account_index UInt32,
  mint String,
  decimals Nullable(UInt8),
  pre_amount UInt64,
  post_amount UInt64,
  delta Int64
)
ENGINE = MergeTree
ORDER BY (mint, slot, signature, account_index);

-- Materialized view (pipe Kafka -> storage)
CREATE MATERIALIZED VIEW IF NOT EXISTS solana.sol_token_balance_deltas_mv
TO solana.sol_token_balance_deltas
AS
SELECT
  now() AS ts,
  slot,
  block_time,
  signature,
  account_index,
  mint,
  decimals,
  pre_amount,
  post_amount,
  delta
FROM solana.sol_token_balance_deltas_queue;