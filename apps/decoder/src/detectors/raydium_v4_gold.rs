use schema::{
    ConfidenceReasons, DexSwapV1, DexSwapV1Builder, TxFacts,
    RAYDIUM_AMM_V4_PROGRAM_ID,
};
use std::collections::HashMap;


mod raydium_accounts {
    /// Pool/AMM account (index 1 in swap instruction)
    pub const POOL_ID: usize = 1;
    /// User source token account (index 15 in swap instruction)
    pub const USER_SOURCE: usize = 15;
    /// User destination token account (index 16 in swap instruction)
    pub const USER_DEST: usize = 16;
    /// Pool token A vault (index 4)
    pub const VAULT_A: usize = 4;
    /// Pool token B vault (index 5)
    pub const VAULT_B: usize = 5;
}

#[derive(Debug, Clone)]
pub struct RaydiumSwapHop {
    /// Outer instruction index
    pub outer_ix_index: usize,
    /// Inner instruction index (if CPI)
    pub inner_ix_index: Option<usize>,
    /// Pool ID (AMM account)
    pub pool_id: Option<String>,
    /// User wallet (trader)
    pub trader: String,
    /// Input mint
    pub in_mint: String,
    /// Input amount
    pub in_amount: u128,
    /// Output mint
    pub out_mint: String,
    /// Output amount
    pub out_amount: u128,
    /// Confidence reasons
    pub confidence_reasons: ConfidenceReasons,
}

/// Parse Raydium AMM v4 swaps from TxFacts.
///
/// This is a pure function - no RPC calls, no side effects.
///
/// Returns a vector of DexSwapV1 (one per hop for multi-hop, or one for single swap).
pub fn parse_raydium_v4_swaps(
    facts: &TxFacts,
    chain: &str,
    index_in_block: u32,
    explain_enabled: bool,
) -> Vec<DexSwapV1> {
    // Gate: check if Raydium program is invoked
    if !facts.has_program(RAYDIUM_AMM_V4_PROGRAM_ID) {
        return vec![];
    }

    // Find all Raydium instructions
    let raydium_ixs = facts.instructions_for_program(RAYDIUM_AMM_V4_PROGRAM_ID);
    if raydium_ixs.is_empty() {
        return vec![];
    }

    // Detect swap hops
    let hops = detect_swap_hops(facts, &raydium_ixs);
    if hops.is_empty() {
        return vec![];
    }

    // Determine if this is a multi-hop route
    let is_multi_hop = hops.len() > 1;

    // Generate route_id for multi-hop
    let route_id = if is_multi_hop {
        // Hash of signature + first outer_ix_index
        let first_ix = hops.first().map(|h| h.outer_ix_index).unwrap_or(0);
        Some(format!("{}:{}", &facts.signature[..16.min(facts.signature.len())], first_ix))
    } else {
        None
    };

    // Build DexSwapV1 for each hop
    hops.iter()
        .enumerate()
        .filter_map(|(hop_idx, hop)| {
            // Validate amounts
            if hop.in_amount == 0 || hop.out_amount == 0 {
                return None;
            }

            let mut builder = DexSwapV1Builder::new()
                .chain(chain)
                .slot(facts.slot)
                .block_time(facts.block_time)
                .signature(&facts.signature)
                .index_in_block(index_in_block)
                .index_in_tx(hop.outer_ix_index as u16)
                .hop_index(hop_idx as u8)
                .venue("raydium")
                .pool_id(hop.pool_id.clone())
                .trader(&hop.trader)
                .in_token(&hop.in_mint, hop.in_amount.to_string())
                .out_token(&hop.out_mint, hop.out_amount.to_string())
                .route_id(route_id.clone())
                .explain_enabled(explain_enabled);

            // Copy confidence reasons
            for flag in [
                ConfidenceReasons::PROGRAM_GATE,
                ConfidenceReasons::POOL_ID_FROM_IX,
                ConfidenceReasons::POOL_ID_FROM_VAULT,
                ConfidenceReasons::TRADER_FROM_OWNER,
                ConfidenceReasons::TRADER_IS_SIGNER,
                ConfidenceReasons::AMOUNTS_CONFIRMED,
                ConfidenceReasons::VAULT_MATCH,
                ConfidenceReasons::SINGLE_HOP,
                ConfidenceReasons::TX_SUCCESS,
            ] {
                if hop.confidence_reasons.has(flag) {
                    builder.add_confidence_reason(flag);
                }
            }

            // Single hop bonus
            if !is_multi_hop {
                builder.add_confidence_reason(ConfidenceReasons::SINGLE_HOP);
            }

            // Tx success
            if facts.is_success {
                builder.add_confidence_reason(ConfidenceReasons::TX_SUCCESS);
            }

            let swap = builder.build();

            // Validate before returning
            if swap.validate().is_ok() {
                Some(swap)
            } else {
                None
            }
        })
        .collect()
}

/// Detect individual swap hops from Raydium instructions
fn detect_swap_hops(
    facts: &TxFacts,
    raydium_ixs: &[&schema::ParsedInstruction],
) -> Vec<RaydiumSwapHop> {
    let mut hops = Vec::new();

    // Build owner -> account index map for trader detection
    let owner_to_deltas: HashMap<String, Vec<&schema::tx_facts::TokenBalanceDelta>> = {
        let mut map: HashMap<String, Vec<_>> = HashMap::new();
        for delta in &facts.token_balance_deltas {
            if let Some(owner) = &delta.owner {
                map.entry(owner.clone()).or_default().push(delta);
            }
        }
        map
    };

    // Find the most likely trader (owner with both negative and positive deltas)
    let trader = find_trader(facts, &owner_to_deltas);

    for ix in raydium_ixs {
        let mut reasons = ConfidenceReasons::new();
        reasons.set(ConfidenceReasons::PROGRAM_GATE);

        // Extract pool_id from instruction accounts
        let pool_id = if ix.accounts.len() > raydium_accounts::POOL_ID {
            let pool_idx = ix.accounts[raydium_accounts::POOL_ID];
            facts.account_at(pool_idx).map(|s| s.to_string())
        } else {
            None
        };

        if pool_id.is_some() {
            reasons.set(ConfidenceReasons::POOL_ID_FROM_IX);
        }

        // Get trader's token deltas
        let trader_deltas = owner_to_deltas.get(&trader).cloned().unwrap_or_default();

        if trader_deltas.is_empty() {
            // Fallback: use all token deltas
            if let Some(hop) = create_hop_from_all_deltas(facts, ix, pool_id, &trader, reasons) {
                hops.push(hop);
            }
            continue;
        }

        // Identify in/out from trader deltas
        let (in_delta, out_delta) = identify_in_out_deltas(&trader_deltas);

        if in_delta.is_none() || out_delta.is_none() {
            // Fallback to all deltas
            if let Some(hop) = create_hop_from_all_deltas(facts, ix, pool_id, &trader, reasons) {
                hops.push(hop);
            }
            continue;
        }

        let in_delta = in_delta.unwrap();
        let out_delta = out_delta.unwrap();

        reasons.set(ConfidenceReasons::TRADER_FROM_OWNER);
        reasons.set(ConfidenceReasons::AMOUNTS_CONFIRMED);

        // Verify vault match if possible
        if verify_vault_match(facts, ix, in_delta, out_delta) {
            reasons.set(ConfidenceReasons::VAULT_MATCH);
        }

        let outer_ix_index = ix.outer_ix_index.unwrap_or(0);

        hops.push(RaydiumSwapHop {
            outer_ix_index,
            inner_ix_index: if ix.stack_depth > 0 {
                Some(outer_ix_index)
            } else {
                None
            },
            pool_id,
            trader: trader.clone(),
            in_mint: in_delta.mint.clone(),
            in_amount: (-in_delta.delta) as u128,
            out_mint: out_delta.mint.clone(),
            out_amount: out_delta.delta as u128,
            confidence_reasons: reasons,
        });
    }

    // Deduplicate hops (same outer_ix_index)
    let mut seen_ix: HashMap<usize, usize> = HashMap::new();
    let mut deduped = Vec::new();
    for hop in hops {
        if let std::collections::hash_map::Entry::Vacant(e) = seen_ix.entry(hop.outer_ix_index) {
            e.insert(deduped.len());
            deduped.push(hop);
        }
    }

    deduped
}

/// Find the most likely trader from token balance deltas
fn find_trader(
    facts: &TxFacts,
    owner_to_deltas: &HashMap<String, Vec<&schema::tx_facts::TokenBalanceDelta>>,
) -> String {
    // Look for an owner with both negative and positive token deltas (swap pattern)
    for (owner, deltas) in owner_to_deltas {
        let has_negative = deltas.iter().any(|d| d.delta < 0);
        let has_positive = deltas.iter().any(|d| d.delta > 0);
        if has_negative && has_positive {
            return owner.clone();
        }
    }

    // Fallback: fee payer
    facts.fee_payer().unwrap_or("unknown").to_string()
}

/// Identify input (negative delta) and output (positive delta) from trader's deltas
fn identify_in_out_deltas<'a>(
    deltas: &[&'a schema::tx_facts::TokenBalanceDelta],
) -> (
    Option<&'a schema::tx_facts::TokenBalanceDelta>,
    Option<&'a schema::tx_facts::TokenBalanceDelta>,
) {
    let mut in_delta: Option<&schema::tx_facts::TokenBalanceDelta> = None;
    let mut out_delta: Option<&schema::tx_facts::TokenBalanceDelta> = None;

    for delta in deltas {
        if delta.delta < 0 && in_delta.is_none() {
            in_delta = Some(delta);
        } else if delta.delta > 0 && out_delta.is_none() {
            out_delta = Some(delta);
        }
    }

    (in_delta, out_delta)
}

/// Verify that vault balance changes match user balance changes
fn verify_vault_match(
    facts: &TxFacts,
    ix: &schema::ParsedInstruction,
    in_delta: &schema::tx_facts::TokenBalanceDelta,
    out_delta: &schema::tx_facts::TokenBalanceDelta,
) -> bool {
    // Get vault account indices from instruction
    if ix.accounts.len() <= raydium_accounts::VAULT_B {
        return false;
    }

    let vault_a_idx = ix.accounts[raydium_accounts::VAULT_A];
    let vault_b_idx = ix.accounts[raydium_accounts::VAULT_B];

    // Find vault deltas
    let vault_a_delta = facts.token_balance_deltas.iter().find(|d| {
        d.account_index as usize == vault_a_idx
    });
    let vault_b_delta = facts.token_balance_deltas.iter().find(|d| {
        d.account_index as usize == vault_b_idx
    });

    // Verify: user's in should match vault's in (positive), user's out should match vault's out (negative)
    match (vault_a_delta, vault_b_delta) {
        (Some(va), Some(vb)) => {
            // Vault A received what user sent OR Vault B received what user sent
            let vault_received_in = (va.mint == in_delta.mint && va.delta > 0)
                || (vb.mint == in_delta.mint && vb.delta > 0);
            let vault_sent_out = (va.mint == out_delta.mint && va.delta < 0)
                || (vb.mint == out_delta.mint && vb.delta < 0);
            vault_received_in && vault_sent_out
        }
        _ => false,
    }
}

/// Fallback: create hop from all token deltas (not trader-specific)
fn create_hop_from_all_deltas(
    facts: &TxFacts,
    ix: &schema::ParsedInstruction,
    pool_id: Option<String>,
    trader: &str,
    mut reasons: ConfidenceReasons,
) -> Option<RaydiumSwapHop> {
    // Find any negative and positive delta
    let in_delta = facts.token_balance_deltas.iter().find(|d| d.delta < 0)?;
    let out_delta = facts.token_balance_deltas.iter().find(|d| d.delta > 0)?;

    // Lower confidence since we couldn't confirm trader
    reasons.set(ConfidenceReasons::TRADER_IS_SIGNER);

    let outer_ix_index = ix.outer_ix_index.unwrap_or(0);

    Some(RaydiumSwapHop {
        outer_ix_index,
        inner_ix_index: if ix.stack_depth > 0 {
            Some(outer_ix_index)
        } else {
            None
        },
        pool_id,
        trader: trader.to_string(),
        in_mint: in_delta.mint.clone(),
        in_amount: (-in_delta.delta) as u128,
        out_mint: out_delta.mint.clone(),
        out_amount: out_delta.delta as u128,
        confidence_reasons: reasons,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_tx_facts(tx: serde_json::Value, sig: &str) -> TxFacts {
        TxFacts::from_json(&tx, sig, 250000000)
    }

    #[test]
    fn test_parse_raydium_v4_basic() {
        let tx = json!({
            "blockTime": 1703001234,
            "meta": {
                "err": null,
                "fee": 5000,
                "preBalances": [1000000000],
                "postBalances": [999995000],
                "preTokenBalances": [
                    {
                        "accountIndex": 1,
                        "mint": "So11111111111111111111111111111111111111112",
                        "owner": "TraderWallet111",
                        "uiTokenAmount": {"amount": "1000000000", "decimals": 9}
                    },
                    {
                        "accountIndex": 2,
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "owner": "TraderWallet111",
                        "uiTokenAmount": {"amount": "0", "decimals": 6}
                    }
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 1,
                        "mint": "So11111111111111111111111111111111111111112",
                        "owner": "TraderWallet111",
                        "uiTokenAmount": {"amount": "500000000", "decimals": 9}
                    },
                    {
                        "accountIndex": 2,
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "owner": "TraderWallet111",
                        "uiTokenAmount": {"amount": "50000000", "decimals": 6}
                    }
                ],
                "innerInstructions": []
            },
            "slot": 250000000,
            "transaction": {
                "message": {
                    "accountKeys": [
                        "TraderWallet111",
                        "PoolAccount123",
                        "TokenAccount1",
                        "TokenAccount2",
                        "VaultA",
                        "VaultB"
                    ],
                    "instructions": [
                        {
                            "programIdIndex": 6,
                            "accounts": [0, 1, 2, 3, 4, 5],
                            "data": "SwapData"
                        }
                    ]
                },
                "signatures": ["sig123"]
            }
        });

        // Add Raydium program to account keys
        let mut tx = tx;
        tx["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap()
            .push(json!(RAYDIUM_AMM_V4_PROGRAM_ID));

        let facts = make_tx_facts(tx, "sig123");
        let swaps = parse_raydium_v4_swaps(&facts, "solana-mainnet", 0, true);

        assert_eq!(swaps.len(), 1);
        let swap = &swaps[0];
        assert_eq!(swap.venue, "raydium");
        assert_eq!(swap.in_mint, "So11111111111111111111111111111111111111112");
        assert_eq!(swap.out_mint, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
        assert_eq!(swap.in_amount, "500000000");
        assert_eq!(swap.out_amount, "50000000");
    }

    #[test]
    fn test_no_raydium_program() {
        let tx = json!({
            "blockTime": 1703001234,
            "meta": {"err": null, "fee": 5000, "preBalances": [], "postBalances": [], "preTokenBalances": [], "postTokenBalances": [], "innerInstructions": []},
            "slot": 250000000,
            "transaction": {
                "message": {
                    "accountKeys": ["Account1", "11111111111111111111111111111111"],
                    "instructions": [{"programIdIndex": 1, "accounts": [], "data": ""}]
                },
                "signatures": ["sig_no_raydium"]
            }
        });

        let facts = make_tx_facts(tx, "sig_no_raydium");
        let swaps = parse_raydium_v4_swaps(&facts, "solana-mainnet", 0, false);

        assert!(swaps.is_empty());
    }

    #[test]
    fn test_confidence_scoring() {
        let mut reasons = ConfidenceReasons::new();
        reasons.set(ConfidenceReasons::PROGRAM_GATE);
        reasons.set(ConfidenceReasons::POOL_ID_FROM_IX);
        reasons.set(ConfidenceReasons::TRADER_FROM_OWNER);
        reasons.set(ConfidenceReasons::AMOUNTS_CONFIRMED);
        reasons.set(ConfidenceReasons::TX_SUCCESS);

        let confidence = reasons.to_confidence_u8();
        assert!(confidence >= 75, "Confidence should be >= 75, got {}", confidence);
    }
}
