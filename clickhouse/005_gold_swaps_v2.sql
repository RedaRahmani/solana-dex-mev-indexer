
DROP VIEW IF EXISTS solana.sol_swaps_mv;
DROP TABLE IF EXISTS solana.sol_swaps_queue;

CREATE TABLE IF NOT EXISTS solana.sol_swaps_queue
(
  schema_version UInt16,
  chain String,

  slot UInt64,
  block_time Nullable(Int64),
  signature String,

  index_in_block UInt32,
  index_in_tx UInt16,
  hop_index UInt8 DEFAULT 0,

  venue LowCardinality(String),
  pool_id Nullable(String),

  trader String,

  in_mint String,
  in_amount String,

  out_mint String,
  out_amount String,

  fee_mint Nullable(String),
  fee_amount Nullable(String),

  route_id Nullable(String),

  confidence UInt8,
  confidence_reasons UInt16 DEFAULT 0,
  explain Nullable(String)
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'sol_swaps',
  kafka_group_name = 'sol_swaps_v2',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS solana.dex_swaps_v2
(
  schema_version UInt16,
  chain LowCardinality(String),

  slot UInt64,
  block_time Nullable(Int64),
  signature String,

  index_in_block UInt32,
  index_in_tx UInt16,
  hop_index UInt8 DEFAULT 0,

  venue LowCardinality(String),
  pool_id Nullable(String),

  trader String,

  in_mint LowCardinality(String),
  in_amount String,

  out_mint LowCardinality(String),
  out_amount String,

  fee_mint Nullable(String),
  fee_amount Nullable(String),

  route_id Nullable(String),

  confidence UInt8,
  confidence_reasons UInt16 DEFAULT 0,
  explain Nullable(String),

  ingested_at DateTime DEFAULT now(),
  version UInt64 DEFAULT toUnixTimestamp(now())
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(fromUnixTimestamp(coalesce(block_time, 0)))
ORDER BY (slot, signature, index_in_block, hop_index)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS solana.sol_swaps_mv_v2
TO solana.dex_swaps_v2
AS
SELECT
  schema_version,
  chain,
  slot,
  block_time,
  signature,
  index_in_block,
  index_in_tx,
  hop_index,
  venue,
  pool_id,
  trader,
  in_mint,
  in_amount,
  out_mint,
  out_amount,
  fee_mint,
  fee_amount,
  route_id,
  confidence,
  confidence_reasons,
  explain,
  now() AS ingested_at,
  toUnixTimestamp(now()) AS version
FROM solana.sol_swaps_queue;

CREATE TABLE IF NOT EXISTS solana.decoder_dlq
(
  timestamp Int64,
  signature String,
  slot UInt64,
  block_time Nullable(Int64),
  chain LowCardinality(String),
  reason LowCardinality(String),
  error String,
  attempts UInt32,
  venue Nullable(String),
  is_v0_alt Bool DEFAULT false,
  context Nullable(String),
  ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(fromUnixTimestamp(timestamp))
ORDER BY (timestamp, signature)
TTL fromUnixTimestamp(timestamp) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS solana.decoder_dlq_queue
(
  timestamp Int64,
  signature String,
  slot UInt64,
  block_time Nullable(Int64),
  chain String,
  reason String,
  error String,
  attempts UInt32,
  venue Nullable(String),
  is_v0_alt Bool,
  context Nullable(String)
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list = 'sol_decoder_dlq',
  kafka_group_name = 'decoder_dlq_v1',
  kafka_format = 'JSONEachRow',
  kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS solana.decoder_dlq_mv
TO solana.decoder_dlq
AS
SELECT
  timestamp,
  signature,
  slot,
  block_time,
  chain,
  reason,
  error,
  attempts,
  venue,
  is_v0_alt,
  context,
  now() AS ingested_at
FROM solana.decoder_dlq_queue;


ALTER TABLE solana.dex_swaps_v2 ADD INDEX IF NOT EXISTS idx_trader (trader) TYPE bloom_filter GRANULARITY 1;

ALTER TABLE solana.dex_swaps_v2 ADD INDEX IF NOT EXISTS idx_pool (pool_id) TYPE bloom_filter GRANULARITY 1;

ALTER TABLE solana.dex_swaps_v2 ADD INDEX IF NOT EXISTS idx_in_mint (in_mint) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE solana.dex_swaps_v2 ADD INDEX IF NOT EXISTS idx_out_mint (out_mint) TYPE bloom_filter GRANULARITY 1;

CREATE VIEW IF NOT EXISTS solana.swap_volume_by_venue AS
SELECT
  venue,
  toDate(fromUnixTimestamp(coalesce(block_time, 0))) AS date,
  count() AS swap_count,
  uniq(trader) AS unique_traders,
  uniq(pool_id) AS unique_pools
FROM solana.dex_swaps_v2
GROUP BY venue, date
ORDER BY date DESC, swap_count DESC;

CREATE VIEW IF NOT EXISTS solana.low_confidence_swaps AS
SELECT
  signature,
  slot,
  venue,
  trader,
  confidence,
  confidence_reasons,
  explain
FROM solana.dex_swaps_v2
WHERE confidence < 50
ORDER BY slot DESC
LIMIT 1000;

CREATE VIEW IF NOT EXISTS solana.dlq_summary AS
SELECT
  reason,
  count() AS count,
  max(timestamp) AS last_seen
FROM solana.decoder_dlq
GROUP BY reason
ORDER BY count DESC;
