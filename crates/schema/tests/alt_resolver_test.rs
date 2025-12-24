/// Integration tests for Address Lookup Table (ALT) resolution
///
/// These tests verify that v0 transactions with ALTs are correctly handled
/// and that program IDs are properly extracted, especially for swap detection.

use serde_json::Value;
use std::fs;

const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

fn load_fixture(name: &str) -> Value {
    let path = format!("tests/fixtures/{}.json", name);
    let content = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read fixture {}: {}", path, e));
    serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse fixture {}: {}", path, e))
}

#[test]
fn test_v0_raydium_swap_program_extraction() {
    let tx = load_fixture("v0_raydium_swap");

    // Verify this is a v0 transaction
    assert_eq!(tx.get("version").and_then(|v| v.as_u64()), Some(0));

    // Verify it has loadedAddresses
    assert!(tx.pointer("/meta/loadedAddresses").is_some());

    // Extract program IDs using our ALT-aware helper
    let program_ids = schema::extract_program_ids_from_transaction(&tx);

    // CRITICAL: Raydium program ID should be found (it's in loadedAddresses.readonly[0])
    assert!(
        program_ids.contains(&RAYDIUM_AMM_V4.to_string()),
        "Raydium AMM v4 program ID must be extracted from loadedAddresses. Found: {:?}",
        program_ids
    );

    // Also verify Token program is found
    assert!(
        program_ids.contains(&TOKEN_PROGRAM.to_string()),
        "Token program should be in program_ids. Found: {:?}",
        program_ids
    );
}

#[test]
fn test_v0_raydium_swap_account_key_ordering() {
    let tx = load_fixture("v0_raydium_swap");

    // Resolve full account keys with ALT
    let full_keys = schema::resolve_full_account_keys(&tx);

    // Expected ordering: accountKeys (4) + writable (1) + readonly (2) = 7 total
    assert_eq!(full_keys.len(), 7);

    // First 4 are from accountKeys
    assert_eq!(full_keys[0], "TraderWallet1111111111111111111111111111");
    assert_eq!(full_keys[1], "TokenAccount11111111111111111111111111111");
    assert_eq!(full_keys[2], "TokenAccount22222222222222222222222222222");
    assert_eq!(full_keys[3], "11111111111111111111111111111111");

    // Index 4 is writable[0]
    assert_eq!(full_keys[4], "PoolAccount111111111111111111111111111111");

    // Index 5-6 are readonly[0-1]
    assert_eq!(full_keys[5], RAYDIUM_AMM_V4);
    assert_eq!(full_keys[6], TOKEN_PROGRAM);
}

#[test]
fn test_v0_instruction_program_id_index_resolution() {
    let tx = load_fixture("v0_raydium_swap");

    // The outer instruction has programIdIndex=5
    // Without ALT resolution, index 5 would be out of bounds (only 4 accountKeys)
    // With ALT resolution, index 5 points to the first readonly loaded address (Raydium)
    let full_keys = schema::resolve_full_account_keys(&tx);

    let instruction_program_idx = tx
        .pointer("/transaction/message/instructions/0/programIdIndex")
        .and_then(|v| v.as_u64())
        .expect("instruction should have programIdIndex");

    assert_eq!(instruction_program_idx, 5);
    assert!(
        (instruction_program_idx as usize) < full_keys.len(),
        "programIdIndex {} should be valid in full_keys (len={})",
        instruction_program_idx,
        full_keys.len()
    );

    // The program at index 5 should be Raydium (first readonly)
    assert_eq!(full_keys[5], RAYDIUM_AMM_V4);
}

#[test]
fn test_legacy_transaction_unchanged_behavior() {
    let tx = load_fixture("legacy_raydium_swap");

    // Verify no version field (legacy) or version != 0
    let version = tx.get("version").and_then(|v| v.as_u64());
    assert!(version.is_none() || version != Some(0));

    // Verify no loadedAddresses
    assert!(tx.pointer("/meta/loadedAddresses").is_none());

    // Extract program IDs
    let program_ids = schema::extract_program_ids_from_transaction(&tx);

    // Raydium should be found (it's in accountKeys[3])
    assert!(
        program_ids.contains(&RAYDIUM_AMM_V4.to_string()),
        "Raydium AMM v4 should be extracted from accountKeys. Found: {:?}",
        program_ids
    );

    // Account keys should match transaction.message.accountKeys exactly
    let full_keys = schema::resolve_full_account_keys(&tx);
    assert_eq!(full_keys.len(), 4);
}

#[test]
fn test_legacy_vs_v0_program_extraction_parity() {
    let legacy = load_fixture("legacy_raydium_swap");
    let v0 = load_fixture("v0_raydium_swap");

    let legacy_programs = schema::extract_program_ids_from_transaction(&legacy);
    let v0_programs = schema::extract_program_ids_from_transaction(&v0);

    // Both should contain Raydium
    assert!(legacy_programs.contains(&RAYDIUM_AMM_V4.to_string()));
    assert!(v0_programs.contains(&RAYDIUM_AMM_V4.to_string()));
}

#[test]
fn test_main_program_selection() {
    let tx = load_fixture("v0_raydium_swap");
    let program_ids = schema::extract_program_ids_from_transaction(&tx);

    let main = schema::pick_main_program(&program_ids);

    // Should skip system programs and pick Raydium
    // Note: Order depends on instruction order, but Raydium should be a candidate
    assert!(main.is_some());
    let main_program = main.unwrap();

    // Main program should not be a system program
    assert_ne!(main_program, "11111111111111111111111111111111");
    assert_ne!(main_program, "ComputeBudget111111111111111111111111111111");
}

#[test]
fn test_empty_loaded_addresses_handled() {
    let mut tx = load_fixture("v0_raydium_swap");

    // Modify to have empty loadedAddresses
    if let Some(loaded) = tx.pointer_mut("/meta/loadedAddresses") {
        *loaded = serde_json::json!({
            "writable": [],
            "readonly": []
        });
    }

    let full_keys = schema::resolve_full_account_keys(&tx);

    // Should only have accountKeys (4)
    assert_eq!(full_keys.len(), 4);
}

#[test]
fn test_missing_loaded_addresses_field() {
    let mut tx = load_fixture("legacy_raydium_swap");

    // Add version=0 but no loadedAddresses (edge case)
    tx["version"] = serde_json::json!(0);

    let full_keys = schema::resolve_full_account_keys(&tx);

    // Should still work, just using accountKeys
    assert_eq!(full_keys.len(), 4);

    let program_ids = schema::extract_program_ids_from_transaction(&tx);
    assert!(program_ids.contains(&RAYDIUM_AMM_V4.to_string()));
}

/// Regression test: Ensure the bug scenario would fail without the fix
#[test]
fn test_bug_scenario_without_alt_resolution_would_fail() {
    let tx = load_fixture("v0_raydium_swap");

    // Simulate old behavior: only use accountKeys (no ALT)
    let account_keys_only: Vec<String> = tx
        .pointer("/transaction/message/accountKeys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    // Old behavior: programIdIndex=5 would be out of bounds
    assert_eq!(account_keys_only.len(), 4);

    // The instruction references index 5, which is out of bounds without ALT
    let instruction_program_idx = tx
        .pointer("/transaction/message/instructions/0/programIdIndex")
        .and_then(|v| v.as_u64())
        .unwrap();

    assert_eq!(instruction_program_idx, 5);
    assert!(
        (instruction_program_idx as usize) >= account_keys_only.len(),
        "This demonstrates the bug: programIdIndex 5 is out of bounds without ALT resolution"
    );

    // With the fix, we now resolve correctly
    let full_keys = schema::resolve_full_account_keys(&tx);
    assert!(
        (instruction_program_idx as usize) < full_keys.len(),
        "Fix: programIdIndex is now valid after merging loadedAddresses"
    );
}
