-- DEX swaps normalized view: one row per swap

CREATE TABLE IF NOT EXISTS solana.dex_swaps
(
    ts DateTime,                          -- when the swap happened (from block_time)
    ingested_at DateTime DEFAULT now(),   -- when we stored this row

    -- link back to raw tx
    slot UInt64,
    signature String,
    index_in_block UInt32,

    -- protocol / pool info
    protocol String,           -- e.g. 'raydium', 'orca'
    pool_address String,       -- AMM pool account

    -- trader info
    trader String,             -- user wallet (if we can infer it)
    side String,               -- 'buy', 'sell', or 'swap'

    -- assets & amounts
    in_mint String,
    out_mint String,
    in_amount Float64,
    out_amount Float64,

    -- pricing
    price Float64,             -- out_amount / in_amount
    usd_value Float64,         -- can be 0 for now, fill later when you add pricing oracles

    -- tx meta
    is_success Bool,
    fee_lamports UInt64
)
ENGINE = MergeTree
ORDER BY (ts, slot, signature, index_in_block);