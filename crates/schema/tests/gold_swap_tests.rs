//! Gold Swap Contract Tests
//!
//! Tests for:
//! - TxFacts correctness
//! - DexSwapV1 schema validation
//! - Raydium parser with pool_id, trader, amounts
//! - Multi-hop route detection
//! - Golden regression tests against expected snapshots

use serde_json::Value;
use std::fs;

// Re-export for tests
use schema::{
    ConfidenceReasons, DexSwapV1Builder, TxFacts,
    extract_program_ids_from_transaction, resolve_full_account_keys,
    RAYDIUM_AMM_V4_PROGRAM_ID,
};

const FIXTURES_DIR: &str = "tests/fixtures";

fn load_fixture(name: &str) -> Value {
    let path = format!("{}/{}.json", FIXTURES_DIR, name);
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read fixture {}: {}", path, e));
    serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse fixture {}: {}", path, e))
}

// =============================================================================
// TxFacts Tests
// =============================================================================

mod tx_facts_tests {
    use super::*;

    #[test]
    fn test_tx_facts_from_legacy_tx() {
        let tx = load_fixture("legacy_raydium_swap_full");
        let sig = tx
            .pointer("/transaction/signatures/0")
            .and_then(|v| v.as_str())
            .unwrap();
        let slot = tx.get("slot").and_then(|v| v.as_u64()).unwrap();

        let facts = TxFacts::from_json(&tx, sig, slot);

        assert_eq!(facts.signature, sig);
        assert_eq!(facts.slot, slot);
        assert_eq!(facts.block_time, Some(1703001100));
        assert!(facts.is_success);
        assert_eq!(facts.fee, 5000);
        assert_eq!(facts.compute_units, Some(45678));
        assert!(!facts.has_loaded_addresses);
        assert!(facts.version.is_none());
    }

    #[test]
    fn test_tx_facts_from_v0_tx() {
        let tx = load_fixture("v0_raydium_swap_full");
        let sig = tx
            .pointer("/transaction/signatures/0")
            .and_then(|v| v.as_str())
            .unwrap();
        let slot = tx.get("slot").and_then(|v| v.as_u64()).unwrap();

        let facts = TxFacts::from_json(&tx, sig, slot);

        assert_eq!(facts.version, Some(0));
        assert!(facts.has_loaded_addresses);

        // Verify full account keys includes loaded addresses
        let static_count = facts.static_account_keys_len;
        assert!(facts.full_account_keys.len() > static_count);
    }

    #[test]
    fn test_tx_facts_token_deltas() {
        let tx = load_fixture("legacy_raydium_swap_full");
        let facts = TxFacts::from_json(&tx, "test_sig", 249999999);

        // Should have token balance deltas
        assert!(!facts.token_balance_deltas.is_empty());

        // Trader should have negative (in) and positive (out) deltas
        let trader_deltas = facts.token_deltas_for_owner("TraderWallet1111111111111111111111111111");
        assert_eq!(trader_deltas.len(), 2);

        // One negative (SOL in), one positive (USDC out)
        let has_negative = trader_deltas.iter().any(|d| d.delta < 0);
        let has_positive = trader_deltas.iter().any(|d| d.delta > 0);
        assert!(has_negative, "Should have negative delta (token in)");
        assert!(has_positive, "Should have positive delta (token out)");
    }

    #[test]
    fn test_tx_facts_sol_deltas() {
        let tx = load_fixture("legacy_raydium_swap_full");
        let facts = TxFacts::from_json(&tx, "test_sig", 249999999);

        // Should have SOL balance delta for fee payer
        assert!(!facts.sol_balance_deltas.is_empty());

        let fee_payer_delta = facts
            .sol_balance_deltas
            .iter()
            .find(|d| d.account == "TraderWallet1111111111111111111111111111");
        assert!(fee_payer_delta.is_some());
        assert_eq!(fee_payer_delta.unwrap().delta, -5000); // Fee paid
    }

    #[test]
    fn test_tx_facts_instructions() {
        let tx = load_fixture("legacy_raydium_swap_full");
        let facts = TxFacts::from_json(&tx, "test_sig", 249999999);

        // Should have outer instructions
        assert!(!facts.outer_instructions.is_empty());

        // Should have all instructions (outer + inner)
        assert!(facts.all_instructions.len() >= facts.outer_instructions.len());

        // Should find Raydium program
        assert!(facts.has_program(RAYDIUM_AMM_V4_PROGRAM_ID));
    }
}

// =============================================================================
// DexSwapV1 Schema Tests
// =============================================================================

mod dex_swap_tests {
    use super::*;

    #[test]
    fn test_dex_swap_v1_builder() {
        let swap = DexSwapV1Builder::new()
            .chain("solana-mainnet")
            .slot(250000000)
            .block_time(Some(1703001234))
            .signature("test_sig_123")
            .index_in_block(5)
            .index_in_tx(0)
            .hop_index(0)
            .venue("raydium")
            .pool_id(Some("pool_abc".into()))
            .trader("trader_wallet")
            .in_token("SOL_mint", "1000000000")
            .out_token("USDC_mint", "50000000")
            .explain_enabled(true)
            .with_confidence_reason(ConfidenceReasons::PROGRAM_GATE)
            .with_confidence_reason(ConfidenceReasons::POOL_ID_FROM_IX)
            .with_confidence_reason(ConfidenceReasons::TRADER_FROM_OWNER)
            .with_confidence_reason(ConfidenceReasons::AMOUNTS_CONFIRMED)
            .with_confidence_reason(ConfidenceReasons::TX_SUCCESS)
            .build();

        assert_eq!(swap.schema_version, 2);
        assert_eq!(swap.venue, "raydium");
        assert_eq!(swap.pool_id, Some("pool_abc".into()));
        assert!(swap.confidence >= 75);
        assert!(swap.explain.is_some());
        assert!(swap.validate().is_ok());
    }

    #[test]
    fn test_dex_swap_v1_validation_in_amount_zero() {
        let swap = DexSwapV1Builder::new()
            .chain("solana-mainnet")
            .slot(250000000)
            .signature("test_sig")
            .venue("raydium")
            .trader("wallet")
            .in_token("mint_a", "0") // Invalid: zero
            .out_token("mint_b", "1000000")
            .build();

        assert!(swap.validate().is_err());
    }

    #[test]
    fn test_dex_swap_v1_validation_out_amount_zero() {
        let swap = DexSwapV1Builder::new()
            .chain("solana-mainnet")
            .slot(250000000)
            .signature("test_sig")
            .venue("raydium")
            .trader("wallet")
            .in_token("mint_a", "1000000")
            .out_token("mint_b", "0") // Invalid: zero
            .build();

        assert!(swap.validate().is_err());
    }

    #[test]
    fn test_confidence_reasons_scoring() {
        let mut reasons = ConfidenceReasons::new();

        // Empty should be 0
        assert_eq!(reasons.to_confidence_u8(), 0);

        // Add program gate (25 points of 100)
        reasons.set(ConfidenceReasons::PROGRAM_GATE);
        assert!(reasons.to_confidence_u8() >= 20);

        // Add more
        reasons.set(ConfidenceReasons::POOL_ID_FROM_IX);
        reasons.set(ConfidenceReasons::TRADER_FROM_OWNER);
        reasons.set(ConfidenceReasons::AMOUNTS_CONFIRMED);
        reasons.set(ConfidenceReasons::TX_SUCCESS);
        reasons.set(ConfidenceReasons::SINGLE_HOP);

        let conf = reasons.to_confidence_u8();
        assert!(conf >= 80, "Expected >= 80, got {}", conf);
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
}

// =============================================================================
// ALT Resolution Tests
// =============================================================================

mod alt_resolution_tests {
    use super::*;

    #[test]
    fn test_v0_alt_program_extraction() {
        let tx = load_fixture("v0_raydium_swap_full");

        let program_ids = extract_program_ids_from_transaction(&tx);

        // Raydium should be in the loaded addresses
        assert!(
            program_ids.contains(&RAYDIUM_AMM_V4_PROGRAM_ID.to_string()),
            "Raydium should be extracted from loadedAddresses. Found: {:?}",
            program_ids
        );
    }

    #[test]
    fn test_v0_full_account_keys_ordering() {
        let tx = load_fixture("v0_raydium_swap_full");

        let full_keys = resolve_full_account_keys(&tx);

        // Should have static + writable + readonly
        let static_keys: Vec<_> = tx
            .pointer("/transaction/message/accountKeys")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();

        let writable: Vec<_> = tx
            .pointer("/meta/loadedAddresses/writable")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();

        let readonly: Vec<_> = tx
            .pointer("/meta/loadedAddresses/readonly")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();

        let expected_len = static_keys.len() + writable.len() + readonly.len();
        assert_eq!(
            full_keys.len(),
            expected_len,
            "Full keys should be static + writable + readonly"
        );

        // Verify ordering
        for (i, key) in static_keys.iter().enumerate() {
            assert_eq!(&full_keys[i], *key, "Static key mismatch at index {}", i);
        }
    }

    #[test]
    fn test_legacy_tx_no_loaded_addresses() {
        let tx = load_fixture("legacy_raydium_swap_full");

        let full_keys = resolve_full_account_keys(&tx);
        let static_keys: Vec<_> = tx
            .pointer("/transaction/message/accountKeys")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();

        assert_eq!(
            full_keys.len(),
            static_keys.len(),
            "Legacy tx should have same keys"
        );
    }
}

// =============================================================================
// Multi-hop Tests
// =============================================================================

mod multi_hop_tests {
    use super::*;

    #[test]
    fn test_multi_hop_fixture_structure() {
        let tx = load_fixture("multi_hop_jupiter_raydium");

        // Verify it's a v0 tx
        assert_eq!(tx.get("version").and_then(|v| v.as_u64()), Some(0));

        // Verify Jupiter and Raydium are involved
        let program_ids = extract_program_ids_from_transaction(&tx);
        assert!(
            program_ids.contains(&RAYDIUM_AMM_V4_PROGRAM_ID.to_string()),
            "Multi-hop should include Raydium"
        );

        // Verify there are inner instructions
        let inner_ixs = tx
            .pointer("/meta/innerInstructions/0/instructions")
            .and_then(|v| v.as_array());
        assert!(inner_ixs.is_some());
        assert!(inner_ixs.unwrap().len() >= 2, "Should have multiple inner ixs");
    }

    #[test]
    fn test_multi_hop_token_deltas() {
        let tx = load_fixture("multi_hop_jupiter_raydium");
        let facts = TxFacts::from_json(&tx, "multi_hop_sig", 250000100);

        // Trader should have SOL in, USDC out (intermediate mSOL is 0->0 from their POV)
        let trader_deltas = facts.token_deltas_for_owner("MultiHopTrader111111111111111111111111111");

        // Should see input (SOL decrease) and output (USDC increase)
        let sol_delta = trader_deltas
            .iter()
            .find(|d| d.mint == "So11111111111111111111111111111111111111112");
        let usdc_delta = trader_deltas
            .iter()
            .find(|d| d.mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");

        assert!(sol_delta.is_some(), "Should have SOL delta");
        assert!(usdc_delta.is_some(), "Should have USDC delta");

        assert!(sol_delta.unwrap().delta < 0, "SOL should decrease");
        assert!(usdc_delta.unwrap().delta > 0, "USDC should increase");
    }
}

// =============================================================================
// Golden Regression Tests
// =============================================================================

mod golden_tests {
    use super::*;

    fn load_expected(name: &str) -> Vec<Value> {
        let path = format!("{}/expected_{}.json", FIXTURES_DIR, name);
        let content = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read expected {}: {}", path, e));
        serde_json::from_str(&content)
            .unwrap_or_else(|e| panic!("Failed to parse expected {}: {}", path, e))
    }

    #[test]
    fn test_golden_legacy_raydium_swap() {
        let tx = load_fixture("legacy_raydium_swap_full");
        let expected = load_expected("legacy_raydium_swap");
        let sig = tx
            .pointer("/transaction/signatures/0")
            .and_then(|v| v.as_str())
            .unwrap();
        let slot = tx.get("slot").and_then(|v| v.as_u64()).unwrap();

        let facts = TxFacts::from_json(&tx, sig, slot);

        // Verify basic facts
        assert!(facts.is_success);
        assert!(facts.has_program(RAYDIUM_AMM_V4_PROGRAM_ID));

        // Verify expected trader
        let expected_trader = expected[0]["trader"].as_str().unwrap();
        let trader_deltas = facts.token_deltas_for_owner(expected_trader);
        assert!(
            !trader_deltas.is_empty(),
            "Expected trader {} should have deltas",
            expected_trader
        );

        // Verify amounts direction
        let in_mint = expected[0]["in_mint"].as_str().unwrap();
        let out_mint = expected[0]["out_mint"].as_str().unwrap();

        let in_delta = trader_deltas.iter().find(|d| d.mint == in_mint);
        let out_delta = trader_deltas.iter().find(|d| d.mint == out_mint);

        assert!(in_delta.is_some(), "Should have in_mint delta");
        assert!(out_delta.is_some(), "Should have out_mint delta");
        assert!(in_delta.unwrap().delta < 0, "in_mint should decrease");
        assert!(out_delta.unwrap().delta > 0, "out_mint should increase");
    }

    #[test]
    fn test_golden_v0_raydium_swap() {
        let tx = load_fixture("v0_raydium_swap_full");
        let expected = load_expected("v0_raydium_swap");
        let sig = tx
            .pointer("/transaction/signatures/0")
            .and_then(|v| v.as_str())
            .unwrap();
        let slot = tx.get("slot").and_then(|v| v.as_u64()).unwrap();

        let facts = TxFacts::from_json(&tx, sig, slot);

        // Critical: v0 with ALT should still find Raydium
        assert!(
            facts.has_program(RAYDIUM_AMM_V4_PROGRAM_ID),
            "v0+ALT tx should find Raydium program"
        );

        // Verify slot matches expected
        assert_eq!(
            facts.slot,
            expected[0]["slot"].as_u64().unwrap(),
            "Slot should match"
        );
    }

    #[test]
    fn test_golden_multi_hop() {
        let tx = load_fixture("multi_hop_jupiter_raydium");
        let expected = load_expected("multi_hop");
        let sig = tx
            .pointer("/transaction/signatures/0")
            .and_then(|v| v.as_str())
            .unwrap();
        let slot = tx.get("slot").and_then(|v| v.as_u64()).unwrap();

        let facts = TxFacts::from_json(&tx, sig, slot);

        // Raydium should be invoked (even if via Jupiter)
        assert!(facts.has_program(RAYDIUM_AMM_V4_PROGRAM_ID));

        // Expected trader should have token deltas
        let expected_trader = expected[0]["trader"].as_str().unwrap();
        let trader_deltas = facts.token_deltas_for_owner(expected_trader);
        assert!(
            !trader_deltas.is_empty(),
            "Multi-hop trader should have deltas"
        );

        // Verify final in/out (SOL -> USDC)
        let in_mint = expected[0]["in_mint"].as_str().unwrap();
        let out_mint = expected[0]["out_mint"].as_str().unwrap();

        let in_delta = trader_deltas.iter().find(|d| d.mint == in_mint);
        let out_delta = trader_deltas.iter().find(|d| d.mint == out_mint);

        assert!(in_delta.is_some(), "Should have SOL (in) delta");
        assert!(out_delta.is_some(), "Should have USDC (out) delta");
    }
}

// =============================================================================
// Regression: Known Edge Cases
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_token_balances() {
        let tx = serde_json::json!({
            "blockTime": 1703001234,
            "meta": {
                "err": null,
                "fee": 5000,
                "preBalances": [1000000000],
                "postBalances": [999995000],
                "preTokenBalances": [],
                "postTokenBalances": [],
                "innerInstructions": []
            },
            "slot": 250000000,
            "transaction": {
                "message": {
                    "accountKeys": ["FeePayer"],
                    "instructions": []
                },
                "signatures": ["empty_balances_sig"]
            }
        });

        let facts = TxFacts::from_json(&tx, "empty_balances_sig", 250000000);

        assert!(facts.token_balance_deltas.is_empty());
        assert!(!facts.sol_balance_deltas.is_empty()); // Fee still paid
    }

    #[test]
    fn test_failed_transaction() {
        let tx = serde_json::json!({
            "blockTime": 1703001234,
            "meta": {
                "err": {"InstructionError": [0, "Custom"]},
                "fee": 5000,
                "preBalances": [1000000000],
                "postBalances": [999995000],
                "preTokenBalances": [],
                "postTokenBalances": [],
                "innerInstructions": []
            },
            "slot": 250000000,
            "transaction": {
                "message": {
                    "accountKeys": ["FeePayer"],
                    "instructions": []
                },
                "signatures": ["failed_tx_sig"]
            }
        });

        let facts = TxFacts::from_json(&tx, "failed_tx_sig", 250000000);

        assert!(!facts.is_success, "Failed tx should have is_success=false");
    }

    #[test]
    fn test_no_change_token_balances() {
        let tx = serde_json::json!({
            "blockTime": 1703001234,
            "meta": {
                "err": null,
                "fee": 5000,
                "preBalances": [1000000000],
                "postBalances": [999995000],
                "preTokenBalances": [
                    {
                        "accountIndex": 1,
                        "mint": "TokenMint123",
                        "owner": "Owner123",
                        "uiTokenAmount": {"amount": "1000000", "decimals": 6}
                    }
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 1,
                        "mint": "TokenMint123",
                        "owner": "Owner123",
                        "uiTokenAmount": {"amount": "1000000", "decimals": 6}
                    }
                ],
                "innerInstructions": []
            },
            "slot": 250000000,
            "transaction": {
                "message": {
                    "accountKeys": ["FeePayer", "TokenAccount"],
                    "instructions": []
                },
                "signatures": ["no_change_sig"]
            }
        });

        let facts = TxFacts::from_json(&tx, "no_change_sig", 250000000);

        // No delta if amounts are the same
        assert!(
            facts.token_balance_deltas.is_empty(),
            "No delta when balances unchanged"
        );
    }
}
