-- ============================================================
-- Swaps (Gold layer)
-- Kafka topic: sol_swaps (JSONEachRow)
-- Queue table -> MV -> storage table
-- ============================================================

CREATE DATABASE IF NOT EXISTS solana;

-- 1) Kafka Engine queue (NO DEFAULT / MATERIALIZED allowed here)
CREATE TABLE IF NOT EXISTS solana.sol_swaps_queue
(
  schema_version UInt16,
  chain String,

  slot UInt64,
  block_time Nullable(Int64),
  signature String,

  index_in_tx UInt16,

  venue LowCardinality(String),
  market_or_pool Nullable(String),

  trader String,

  in_mint String,
  in_amount String,

  out_mint String,
  out_amount String,

  fee_mint Nullable(String),
  fee_amount Nullable(String),

  route_id Nullable(String),

  confidence UInt8,
  explain Nullable(String)
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'sol_swaps',
  kafka_group_name = 'sol_swaps_v1',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

-- 2) Storage table (DEFAULT is OK here)
CREATE TABLE IF NOT EXISTS solana.dex_swaps_v1
(
  schema_version UInt16,
  chain LowCardinality(String),

  slot UInt64,
  block_time Nullable(Int64),
  signature String,

  index_in_tx UInt16,

  venue LowCardinality(String),
  market_or_pool Nullable(String),

  trader String,

  in_mint String,
  in_amount String,

  out_mint String,
  out_amount String,

  fee_mint Nullable(String),
  fee_amount Nullable(String),

  route_id Nullable(String),

  confidence UInt8,
  explain Nullable(String),

  ingested_at DateTime DEFAULT now(),
  version UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(fromUnixTimestamp(coalesce(block_time, 0)))
ORDER BY (signature, index_in_tx);

-- 3) Materialized view (can compute anything here if needed)
CREATE MATERIALIZED VIEW IF NOT EXISTS solana.sol_swaps_mv
TO solana.dex_swaps_v1
AS
SELECT
  schema_version,
  chain,
  slot,
  block_time,
  signature,
  index_in_tx,
  venue,
  market_or_pool,
  trader,
  in_mint,
  in_amount,
  out_mint,
  out_amount,
  fee_mint,
  fee_amount,
  route_id,
  confidence,
  explain
FROM solana.sol_swaps_queue;
