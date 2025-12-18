use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct RawTxEvent {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: String,
    #[allow(dead_code)]
    pub is_success: bool,
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
