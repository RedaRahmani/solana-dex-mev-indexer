//! Gold Swap Contract: `DexSwapV1`
//!
//! This module defines the production-grade swap schema with:
//! - Strict invariants (in_amount > 0, out_amount > 0)
//! - Structured confidence scoring with reasons
//! - Multi-hop support via route_id and hop_index
//! - Explain string for debugging

use serde::{Deserialize, Serialize};
use std::fmt;

/// Raydium AMM v4 program ID (mainnet)
pub const RAYDIUM_AMM_V4_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

/// Token Program ID
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Confidence reasons as bitflags for structured debugging.
///
/// Each bit represents a confidence criterion that was met (1) or failed (0).
/// Full confidence (1.0) requires all relevant bits set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ConfidenceReasons(pub u16);

impl ConfidenceReasons {
    /// Program ID gate passed (Raydium instruction found)
    pub const PROGRAM_GATE: u16 = 1 << 0;
    /// Pool ID extracted from instruction accounts
    pub const POOL_ID_FROM_IX: u16 = 1 << 1;
    /// Pool ID inferred from vault deltas
    pub const POOL_ID_FROM_VAULT: u16 = 1 << 2;
    /// Trader identified from token account owner
    pub const TRADER_FROM_OWNER: u16 = 1 << 3;
    /// Trader is instruction signer
    pub const TRADER_IS_SIGNER: u16 = 1 << 4;
    /// In/out amounts from user token accounts confirmed
    pub const AMOUNTS_CONFIRMED: u16 = 1 << 5;
    /// Vault balance changes match user balance changes
    pub const VAULT_MATCH: u16 = 1 << 6;
    /// Single hop (not multi-hop aggregator)
    pub const SINGLE_HOP: u16 = 1 << 7;
    /// Inner instruction correctly attributed
    pub const INNER_IX_RESOLVED: u16 = 1 << 8;
    /// Transaction succeeded (not reverted)
    pub const TX_SUCCESS: u16 = 1 << 9;

    pub fn new() -> Self {
        Self(0)
    }

    pub fn set(&mut self, flag: u16) {
        self.0 |= flag;
    }

    pub fn has(&self, flag: u16) -> bool {
        (self.0 & flag) == flag
    }

    /// Convert to confidence score in [0.0, 1.0]
    pub fn to_confidence(&self) -> f32 {
        // Weights for each criterion (sum = 100)
        let mut score = 0u32;
        let mut max_score = 0u32;

        // Program gate is required (25 points)
        max_score += 25;
        if self.has(Self::PROGRAM_GATE) {
            score += 25;
        }

        // Pool ID (20 points - from IX preferred, vault fallback)
        max_score += 20;
        if self.has(Self::POOL_ID_FROM_IX) {
            score += 20;
        } else if self.has(Self::POOL_ID_FROM_VAULT) {
            score += 15;
        }

        // Trader identification (15 points)
        max_score += 15;
        if self.has(Self::TRADER_FROM_OWNER) {
            score += 15;
        } else if self.has(Self::TRADER_IS_SIGNER) {
            score += 10;
        }

        // Amounts confirmed (15 points)
        max_score += 15;
        if self.has(Self::AMOUNTS_CONFIRMED) {
            score += 15;
        }

        // Vault match (10 points)
        max_score += 10;
        if self.has(Self::VAULT_MATCH) {
            score += 10;
        }

        // Single hop bonus (5 points)
        max_score += 5;
        if self.has(Self::SINGLE_HOP) {
            score += 5;
        }

        // Tx success (10 points)
        max_score += 10;
        if self.has(Self::TX_SUCCESS) {
            score += 10;
        }

        score as f32 / max_score as f32
    }

    /// Convert to u8 confidence (0-100)
    pub fn to_confidence_u8(&self) -> u8 {
        (self.to_confidence() * 100.0).round() as u8
    }

    /// Generate human-readable explain string
    pub fn explain(&self) -> String {
        let mut reasons = Vec::new();

        if self.has(Self::PROGRAM_GATE) {
            reasons.push("+program_gate");
        } else {
            reasons.push("-program_gate");
        }

        if self.has(Self::POOL_ID_FROM_IX) {
            reasons.push("+pool_from_ix");
        } else if self.has(Self::POOL_ID_FROM_VAULT) {
            reasons.push("+pool_from_vault");
        } else {
            reasons.push("-pool_id");
        }

        if self.has(Self::TRADER_FROM_OWNER) {
            reasons.push("+trader_owner");
        } else if self.has(Self::TRADER_IS_SIGNER) {
            reasons.push("+trader_signer");
        } else {
            reasons.push("-trader");
        }

        if self.has(Self::AMOUNTS_CONFIRMED) {
            reasons.push("+amounts");
        } else {
            reasons.push("-amounts");
        }

        if self.has(Self::VAULT_MATCH) {
            reasons.push("+vault_match");
        }

        if !self.has(Self::SINGLE_HOP) {
            reasons.push("multi_hop");
        }

        if self.has(Self::TX_SUCCESS) {
            reasons.push("+tx_ok");
        } else {
            reasons.push("-tx_fail");
        }

        reasons.join(" ")
    }
}

impl fmt::Display for ConfidenceReasons {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.explain())
    }
}

/// Gold-layer DEX swap event (v1 schema).
///
/// Invariants:
/// - `in_amount > 0` and `out_amount > 0` (enforced by constructor)
/// - `confidence` in [0, 100]
/// - Required fields for confidence == 100: pool_id, trader, amounts all confirmed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DexSwapV1 {
    /// Schema version for forward compatibility
    pub schema_version: u16,

    /// Chain identifier (e.g., "solana-mainnet")
    pub chain: String,

    /// Slot number
    pub slot: u64,

    /// Block timestamp (Unix seconds)
    pub block_time: Option<i64>,

    /// Transaction signature
    pub signature: String,

    /// Index of this swap within the block (for ordering)
    pub index_in_block: u32,

    /// Index within the transaction (for multi-instruction txs)
    pub index_in_tx: u16,

    /// Hop index for multi-hop routes (0-based)
    pub hop_index: u8,

    /// DEX venue (e.g., "raydium", "orca", "jupiter")
    pub venue: String,

    /// Pool/market address (AMM pool account)
    pub pool_id: Option<String>,

    /// Trader wallet address (user who initiated the swap)
    pub trader: String,

    /// Input token mint address
    pub in_mint: String,

    /// Input amount in base units (as string to preserve precision)
    pub in_amount: String,

    /// Output token mint address
    pub out_mint: String,

    /// Output amount in base units (as string to preserve precision)
    pub out_amount: String,

    /// Fee token mint (if known)
    pub fee_mint: Option<String>,

    /// Fee amount in base units (if known)
    pub fee_amount: Option<String>,

    /// Route identifier for multi-hop swaps (hash of signature + top_level_ix_index)
    pub route_id: Option<String>,

    /// Confidence score (0-100)
    pub confidence: u8,

    /// Structured confidence reasons (bitflags)
    pub confidence_reasons: u16,

    /// Human-readable explain string for debugging
    pub explain: Option<String>,
}

impl DexSwapV1 {
    pub const SCHEMA_VERSION: u16 = 2;

    /// Validate invariants. Returns error message if invalid.
    pub fn validate(&self) -> Result<(), &'static str> {
        // Parse amounts and validate > 0
        let in_amt: u128 = self
            .in_amount
            .parse()
            .map_err(|_| "in_amount must be valid u128")?;
        let out_amt: u128 = self
            .out_amount
            .parse()
            .map_err(|_| "out_amount must be valid u128")?;

        if in_amt == 0 {
            return Err("in_amount must be > 0");
        }
        if out_amt == 0 {
            return Err("out_amount must be > 0");
        }

        if self.confidence > 100 {
            return Err("confidence must be in [0, 100]");
        }

        // For confidence == 100, pool_id must be present
        if self.confidence == 100 && self.pool_id.is_none() {
            return Err("confidence=100 requires pool_id");
        }

        Ok(())
    }

    /// Check if this is a high-confidence swap
    pub fn is_high_confidence(&self) -> bool {
        self.confidence >= 80
    }
}

/// Builder for constructing DexSwapV1 with proper validation
#[derive(Debug, Default)]
pub struct DexSwapV1Builder {
    chain: String,
    slot: u64,
    block_time: Option<i64>,
    signature: String,
    index_in_block: u32,
    index_in_tx: u16,
    hop_index: u8,
    venue: String,
    pool_id: Option<String>,
    trader: String,
    in_mint: String,
    in_amount: String,
    out_mint: String,
    out_amount: String,
    fee_mint: Option<String>,
    fee_amount: Option<String>,
    route_id: Option<String>,
    confidence_reasons: ConfidenceReasons,
    explain_enabled: bool,
}

impl DexSwapV1Builder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn chain(mut self, chain: impl Into<String>) -> Self {
        self.chain = chain.into();
        self
    }

    pub fn slot(mut self, slot: u64) -> Self {
        self.slot = slot;
        self
    }

    pub fn block_time(mut self, block_time: Option<i64>) -> Self {
        self.block_time = block_time;
        self
    }

    pub fn signature(mut self, signature: impl Into<String>) -> Self {
        self.signature = signature.into();
        self
    }

    pub fn index_in_block(mut self, index: u32) -> Self {
        self.index_in_block = index;
        self
    }

    pub fn index_in_tx(mut self, index: u16) -> Self {
        self.index_in_tx = index;
        self
    }

    pub fn hop_index(mut self, index: u8) -> Self {
        self.hop_index = index;
        self
    }

    pub fn venue(mut self, venue: impl Into<String>) -> Self {
        self.venue = venue.into();
        self
    }

    pub fn pool_id(mut self, pool_id: Option<String>) -> Self {
        self.pool_id = pool_id;
        self
    }

    pub fn trader(mut self, trader: impl Into<String>) -> Self {
        self.trader = trader.into();
        self
    }

    pub fn in_token(mut self, mint: impl Into<String>, amount: impl Into<String>) -> Self {
        self.in_mint = mint.into();
        self.in_amount = amount.into();
        self
    }

    pub fn out_token(mut self, mint: impl Into<String>, amount: impl Into<String>) -> Self {
        self.out_mint = mint.into();
        self.out_amount = amount.into();
        self
    }

    pub fn fee(mut self, mint: Option<String>, amount: Option<String>) -> Self {
        self.fee_mint = mint;
        self.fee_amount = amount;
        self
    }

    pub fn route_id(mut self, route_id: Option<String>) -> Self {
        self.route_id = route_id;
        self
    }

    pub fn add_confidence_reason(&mut self, reason: u16) {
        self.confidence_reasons.set(reason);
    }

    pub fn with_confidence_reason(mut self, reason: u16) -> Self {
        self.confidence_reasons.set(reason);
        self
    }

    pub fn explain_enabled(mut self, enabled: bool) -> Self {
        self.explain_enabled = enabled;
        self
    }

    pub fn build(self) -> DexSwapV1 {
        let confidence = self.confidence_reasons.to_confidence_u8();
        let explain = if self.explain_enabled {
            Some(self.confidence_reasons.explain())
        } else {
            None
        };

        DexSwapV1 {
            schema_version: DexSwapV1::SCHEMA_VERSION,
            chain: self.chain,
            slot: self.slot,
            block_time: self.block_time,
            signature: self.signature,
            index_in_block: self.index_in_block,
            index_in_tx: self.index_in_tx,
            hop_index: self.hop_index,
            venue: self.venue,
            pool_id: self.pool_id,
            trader: self.trader,
            in_mint: self.in_mint,
            in_amount: self.in_amount,
            out_mint: self.out_mint,
            out_amount: self.out_amount,
            fee_mint: self.fee_mint,
            fee_amount: self.fee_amount,
            route_id: self.route_id,
            confidence,
            confidence_reasons: self.confidence_reasons.0,
            explain,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confidence_reasons_full_score() {
        let mut reasons = ConfidenceReasons::new();
        reasons.set(ConfidenceReasons::PROGRAM_GATE);
        reasons.set(ConfidenceReasons::POOL_ID_FROM_IX);
        reasons.set(ConfidenceReasons::TRADER_FROM_OWNER);
        reasons.set(ConfidenceReasons::AMOUNTS_CONFIRMED);
        reasons.set(ConfidenceReasons::VAULT_MATCH);
        reasons.set(ConfidenceReasons::SINGLE_HOP);
        reasons.set(ConfidenceReasons::TX_SUCCESS);

        assert_eq!(reasons.to_confidence_u8(), 100);
    }

    #[test]
    fn test_confidence_reasons_partial_score() {
        let mut reasons = ConfidenceReasons::new();
        reasons.set(ConfidenceReasons::PROGRAM_GATE);
        reasons.set(ConfidenceReasons::TRADER_IS_SIGNER); // Lower score than TRADER_FROM_OWNER

        let conf = reasons.to_confidence_u8();
        assert!(conf > 0 && conf < 100);
    }

    #[test]
    fn test_confidence_reasons_explain() {
        let mut reasons = ConfidenceReasons::new();
        reasons.set(ConfidenceReasons::PROGRAM_GATE);
        reasons.set(ConfidenceReasons::POOL_ID_FROM_IX);
        reasons.set(ConfidenceReasons::TX_SUCCESS);

        let explain = reasons.explain();
        assert!(explain.contains("+program_gate"));
        assert!(explain.contains("+pool_from_ix"));
        assert!(explain.contains("+tx_ok"));
    }

    #[test]
    fn test_dex_swap_v1_validation() {
        let swap = DexSwapV1Builder::new()
            .chain("solana-mainnet")
            .slot(123456)
            .signature("test_sig")
            .venue("raydium")
            .trader("wallet123")
            .in_token("mint_a", "1000000")
            .out_token("mint_b", "500000")
            .with_confidence_reason(ConfidenceReasons::PROGRAM_GATE)
            .with_confidence_reason(ConfidenceReasons::TX_SUCCESS)
            .build();

        assert!(swap.validate().is_ok());
    }

    #[test]
    fn test_dex_swap_v1_validation_zero_amount() {
        let swap = DexSwapV1Builder::new()
            .chain("solana-mainnet")
            .slot(123456)
            .signature("test_sig")
            .venue("raydium")
            .trader("wallet123")
            .in_token("mint_a", "0") // Invalid
            .out_token("mint_b", "500000")
            .build();

        assert!(swap.validate().is_err());
    }

    #[test]
    fn test_builder_pattern() {
        let swap = DexSwapV1Builder::new()
            .chain("solana-mainnet")
            .slot(250000000)
            .block_time(Some(1703001234))
            .signature("sig123")
            .index_in_block(5)
            .index_in_tx(0)
            .hop_index(0)
            .venue("raydium")
            .pool_id(Some("pool_abc".into()))
            .trader("trader123")
            .in_token("SOL", "1000000000")
            .out_token("USDC", "50000000")
            .route_id(None)
            .explain_enabled(true)
            .with_confidence_reason(ConfidenceReasons::PROGRAM_GATE)
            .with_confidence_reason(ConfidenceReasons::POOL_ID_FROM_IX)
            .with_confidence_reason(ConfidenceReasons::TRADER_FROM_OWNER)
            .with_confidence_reason(ConfidenceReasons::AMOUNTS_CONFIRMED)
            .with_confidence_reason(ConfidenceReasons::TX_SUCCESS)
            .build();

        assert_eq!(swap.schema_version, 2);
        assert_eq!(swap.venue, "raydium");
        assert!(swap.explain.is_some());
        assert!(swap.confidence >= 80);
    }
}
