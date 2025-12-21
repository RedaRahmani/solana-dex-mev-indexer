use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapEvent {
    pub schema_version: u16,
    pub chain: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub signature: String,
    pub index_in_tx: u16,
    pub venue: String,
    pub market_or_pool: Option<String>,
    pub trader: String,
    pub in_mint: String,
    pub in_amount: String,
    pub out_mint: String,
    pub out_amount: String,
    pub fee_mint: Option<String>,
    pub fee_amount: Option<String>,
    pub route_id: Option<String>,
    pub confidence: u8,
    pub explain: Option<String>,
}
