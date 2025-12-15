use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RawTxEvent {
    pub schema_version: u8,
    pub chain: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: String,
    pub index_in_block: u32,
    pub tx_version: Option<u8>,
    pub is_success: bool,
    pub fee_lamports : u64,
    pub compute_units_consumed: Option<u64>,
    pub main_program: Option<String>,
    pub program_ids: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct DlqEvent {
    pub source: String,
    pub step: String,
    pub signature: Option<String>,
    pub error: String,
}