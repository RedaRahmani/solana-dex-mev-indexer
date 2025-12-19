use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct RawTxEvent {
    pub schema_version: u8,
    pub chain: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: String,
    pub index_in_block: u32,
    pub tx_version: Option<u8>,
    pub is_success: bool,
    pub fee_lamports: u64,
    pub compute_units_consumed: Option<u64>,
    pub main_program: Option<String>,
    pub program_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SolBalanceDelta {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: String,
    pub account: String,
    pub pre_balance: u64,
    pub post_balance: u64,
    pub delta: i64,
}

#[derive(Debug, Serialize)]
pub struct TokenBalanceDelta {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: String,
    pub account_index: u32,
    pub mint: String,
    pub decimals: Option<u8>,
    pub pre_amount: u64,
    pub post_amount: u64,
    pub delta: i64,
}
