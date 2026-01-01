//! Dead Letter Queue (DLQ) for transactions that cannot be parsed.
//!
//! Stores transactions that failed parsing but should not be dropped,
//! allowing for later investigation and reprocessing.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// DLQ entry for a failed transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    /// When the entry was created
    pub timestamp: i64,

    /// Transaction signature
    pub signature: String,

    /// Slot number
    pub slot: u64,

    /// Block time (if available)
    pub block_time: Option<i64>,

    /// Chain identifier
    pub chain: String,

    /// Failure reason category
    pub reason: String,

    /// Detailed error message
    pub error: String,

    /// Number of parse attempts
    pub attempts: u32,

    /// Venue that failed (if applicable)
    pub venue: Option<String>,

    /// Whether this was a v0 transaction with ALT
    pub is_v0_alt: bool,

    /// Additional context (JSON blob)
    pub context: Option<String>,
}

impl DlqEntry {
    pub fn new(signature: &str, slot: u64, reason: &str, error: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        Self {
            timestamp,
            signature: signature.to_string(),
            slot,
            block_time: None,
            chain: "solana-mainnet".to_string(),
            reason: reason.to_string(),
            error: error.to_string(),
            attempts: 1,
            venue: None,
            is_v0_alt: false,
            context: None,
        }
    }

    pub fn with_block_time(mut self, block_time: Option<i64>) -> Self {
        self.block_time = block_time;
        self
    }

    pub fn with_chain(mut self, chain: &str) -> Self {
        self.chain = chain.to_string();
        self
    }

    pub fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }

    pub fn with_venue(mut self, venue: &str) -> Self {
        self.venue = Some(venue.to_string());
        self
    }

    pub fn with_v0_alt(mut self, is_v0_alt: bool) -> Self {
        self.is_v0_alt = is_v0_alt;
        self
    }

    pub fn with_context(mut self, context: serde_json::Value) -> Self {
        self.context = serde_json::to_string(&context).ok();
        self
    }

    /// Convert to JSON for Kafka publishing
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

/// DLQ reason constants
pub mod reasons {
    pub const RPC_FETCH_FAILED: &str = "rpc_fetch_failed";
    pub const PARSE_FAILED: &str = "parse_failed";
    pub const VALIDATION_FAILED: &str = "validation_failed";
    pub const NO_TOKEN_DELTAS: &str = "no_token_deltas";
    pub const INVALID_AMOUNTS: &str = "invalid_amounts";
    pub const MULTI_HOP_FAILED: &str = "multi_hop_failed";
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_dlq_entry_creation() {
        let entry = DlqEntry::new("sig123", 250000000, reasons::PARSE_FAILED, "no token deltas")
            .with_block_time(Some(1703001234))
            .with_chain("solana-mainnet")
            .with_venue("raydium")
            .with_v0_alt(true)
            .with_attempts(3);

        assert_eq!(entry.signature, "sig123");
        assert_eq!(entry.slot, 250000000);
        assert_eq!(entry.reason, "parse_failed");
        assert!(entry.is_v0_alt);
        assert_eq!(entry.attempts, 3);
    }

    #[test]
    fn test_dlq_entry_to_json() {
        let entry = DlqEntry::new("sig123", 250000000, reasons::RPC_FETCH_FAILED, "timeout")
            .with_context(json!({"rpc_url": "https://api.mainnet.solana.com"}));

        let json = entry.to_json().unwrap();
        assert!(json.contains("sig123"));
        assert!(json.contains("rpc_fetch_failed"));
    }
}
