/// Address Lookup Table (ALT) resolution for v0 transactions.
///
/// This module provides utilities to correctly extract program IDs from Solana transactions,
/// handling both legacy transactions and v0 transactions with Address Lookup Tables.

use serde_json::Value;
use std::collections::HashSet;

/// Resolves the full account key list for a transaction, merging accountKeys with
/// loadedAddresses for v0 transactions.
///
/// For v0 transactions, the account keys are split across:
/// - `message.accountKeys` - static accounts
/// - `meta.loadedAddresses.writable` - writable accounts from ALT
/// - `meta.loadedAddresses.readonly` - readonly accounts from ALT
///
/// The final ordering is: accountKeys + writable + readonly
///
/// # Arguments
/// * `tx` - Transaction JSON object (from RPC getTransaction)
///
/// # Returns
/// Vector of account pubkeys in the correct order for programIdIndex lookup
pub fn resolve_full_account_keys(tx: &Value) -> Vec<String> {
    let message = match tx.pointer("/transaction/message") {
        Some(m) => m,
        None => return vec![],
    };

    // Get base account keys from message.accountKeys
    let mut account_keys: Vec<String> = message
        .get("accountKeys")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| {
                    // Handle both jsonParsed (object with pubkey field) and raw (string) formats
                    if x.is_string() {
                        x.as_str().map(|s| s.to_string())
                    } else {
                        x.get("pubkey")
                            .and_then(|p| p.as_str())
                            .map(|s| s.to_string())
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    // For v0 transactions, merge loadedAddresses
    if let Some(loaded) = tx.pointer("/meta/loadedAddresses") {
        // Add writable loaded addresses
        if let Some(writable) = loaded.get("writable").and_then(|v| v.as_array()) {
            for addr in writable {
                if let Some(addr_str) = addr.as_str() {
                    account_keys.push(addr_str.to_string());
                }
            }
        }

        // Add readonly loaded addresses
        if let Some(readonly) = loaded.get("readonly").and_then(|v| v.as_array()) {
            for addr in readonly {
                if let Some(addr_str) = addr.as_str() {
                    account_keys.push(addr_str.to_string());
                }
            }
        }
    }

    account_keys
}

/// Extracts all unique program IDs from a transaction.
///
/// This function correctly handles:
/// - Legacy transactions (version = null)
/// - v0 transactions with Address Lookup Tables
/// - Both jsonParsed and raw instruction formats
/// - Both outer and inner instructions
///
/// # Arguments
/// * `tx` - Transaction JSON object (from RPC getTransaction)
///
/// # Returns
/// Vector of unique program IDs in order of first appearance
pub fn extract_program_ids_from_transaction(tx: &Value) -> Vec<String> {
    let account_keys = resolve_full_account_keys(tx);
    if account_keys.is_empty() {
        return vec![];
    }

    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    let message = match tx.pointer("/transaction/message") {
        Some(m) => m,
        None => return vec![],
    };

    // Process outer instructions
    if let Some(instructions) = message.get("instructions").and_then(|v| v.as_array()) {
        for ix in instructions {
            // jsonParsed format: programId field
            if let Some(pid) = ix.get("programId").and_then(|v| v.as_str()) {
                if seen.insert(pid.to_string()) {
                    out.push(pid.to_string());
                }
                continue;
            }
            // Raw format: programIdIndex
            if let Some(idx) = ix.get("programIdIndex").and_then(|v| v.as_i64()) {
                if idx >= 0 {
                    let i = idx as usize;
                    if i < account_keys.len() {
                        let pid = &account_keys[i];
                        if seen.insert(pid.clone()) {
                            out.push(pid.clone());
                        }
                    }
                }
            }
        }
    }

    // Process inner instructions
    if let Some(inner_array) = tx.pointer("/meta/innerInstructions").and_then(|v| v.as_array()) {
        for inner_group in inner_array {
            if let Some(instructions) = inner_group.get("instructions").and_then(|v| v.as_array())
            {
                for ix in instructions {
                    // jsonParsed format: programId field
                    if let Some(pid) = ix.get("programId").and_then(|v| v.as_str()) {
                        if seen.insert(pid.to_string()) {
                            out.push(pid.to_string());
                        }
                        continue;
                    }
                    // Raw format: programIdIndex
                    if let Some(idx) = ix.get("programIdIndex").and_then(|v| v.as_i64()) {
                        if idx >= 0 {
                            let i = idx as usize;
                            if i < account_keys.len() {
                                let pid = &account_keys[i];
                                if seen.insert(pid.clone()) {
                                    out.push(pid.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    out
}

/// Picks the "main" program from a list of program IDs by filtering out common system programs.
///
/// Returns the first non-system program, or None if only system programs are present.
pub fn pick_main_program(program_ids: &[String]) -> Option<String> {
    let skip = [
        "ComputeBudget111111111111111111111111111111",
        "11111111111111111111111111111111",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    ];
    program_ids
        .iter()
        .find(|p| !skip.contains(&p.as_str()))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_resolve_full_account_keys_legacy() {
        // Legacy transaction (no loadedAddresses)
        let tx = json!({
            "transaction": {
                "message": {
                    "accountKeys": [
                        "FeePayer111111111111111111111111111111111",
                        "Program11111111111111111111111111111111111",
                        "Account1111111111111111111111111111111111"
                    ]
                }
            }
        });

        let keys = resolve_full_account_keys(&tx);
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], "FeePayer111111111111111111111111111111111");
        assert_eq!(keys[1], "Program11111111111111111111111111111111111");
        assert_eq!(keys[2], "Account1111111111111111111111111111111111");
    }

    #[test]
    fn test_resolve_full_account_keys_v0_with_alt() {
        // v0 transaction with loadedAddresses
        let tx = json!({
            "transaction": {
                "message": {
                    "accountKeys": [
                        "FeePayer111111111111111111111111111111111",
                        "Program11111111111111111111111111111111111"
                    ]
                }
            },
            "meta": {
                "loadedAddresses": {
                    "writable": [
                        "Writable11111111111111111111111111111111"
                    ],
                    "readonly": [
                        "Readonly11111111111111111111111111111111",
                        "Readonly22222222222222222222222222222222"
                    ]
                }
            }
        });

        let keys = resolve_full_account_keys(&tx);
        assert_eq!(keys.len(), 5);
        assert_eq!(keys[0], "FeePayer111111111111111111111111111111111");
        assert_eq!(keys[1], "Program11111111111111111111111111111111111");
        assert_eq!(keys[2], "Writable11111111111111111111111111111111");
        assert_eq!(keys[3], "Readonly11111111111111111111111111111111");
        assert_eq!(keys[4], "Readonly22222222222222222222222222222222");
    }

    #[test]
    fn test_resolve_full_account_keys_json_parsed_format() {
        // jsonParsed format (objects with pubkey field)
        let tx = json!({
            "transaction": {
                "message": {
                    "accountKeys": [
                        {"pubkey": "FeePayer111111111111111111111111111111111"},
                        {"pubkey": "Program11111111111111111111111111111111111"}
                    ]
                }
            }
        });

        let keys = resolve_full_account_keys(&tx);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], "FeePayer111111111111111111111111111111111");
    }

    #[test]
    fn test_extract_program_ids_legacy() {
        let tx = json!({
            "transaction": {
                "message": {
                    "accountKeys": [
                        "FeePayer111111111111111111111111111111111",
                        "11111111111111111111111111111111", // System program
                        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // Token program
                        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" // Raydium
                    ],
                    "instructions": [
                        {"programIdIndex": 1},
                        {"programIdIndex": 2},
                        {"programIdIndex": 3}
                    ]
                }
            },
            "meta": {}
        });

        let program_ids = extract_program_ids_from_transaction(&tx);
        assert_eq!(program_ids.len(), 3);
        assert!(program_ids.contains(&"11111111111111111111111111111111".to_string()));
        assert!(program_ids.contains(&"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string()));
        assert!(program_ids.contains(&"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()));
    }

    #[test]
    fn test_extract_program_ids_v0_with_alt() {
        // v0 transaction where Raydium is in loadedAddresses
        let tx = json!({
            "transaction": {
                "message": {
                    "accountKeys": [
                        "FeePayer111111111111111111111111111111111",
                        "11111111111111111111111111111111", // System program
                        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" // Token program
                    ],
                    "instructions": [
                        {"programIdIndex": 1},
                        {"programIdIndex": 2},
                        {"programIdIndex": 3} // This will map to first writable in loadedAddresses
                    ]
                }
            },
            "meta": {
                "loadedAddresses": {
                    "writable": [
                        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" // Raydium at index 3
                    ],
                    "readonly": []
                }
            }
        });

        let program_ids = extract_program_ids_from_transaction(&tx);
        assert_eq!(program_ids.len(), 3);
        assert!(program_ids.contains(&"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()));
    }

    #[test]
    fn test_extract_program_ids_json_parsed_format() {
        let tx = json!({
            "transaction": {
                "message": {
                    "accountKeys": ["Account1", "Account2"],
                    "instructions": [
                        {"programId": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"},
                        {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}
                    ]
                }
            },
            "meta": {
                "innerInstructions": [
                    {
                        "instructions": [
                            {"programId": "11111111111111111111111111111111"}
                        ]
                    }
                ]
            }
        });

        let program_ids = extract_program_ids_from_transaction(&tx);
        assert_eq!(program_ids.len(), 3);
        assert!(program_ids.contains(&"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()));
        assert!(program_ids.contains(&"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string()));
        assert!(program_ids.contains(&"11111111111111111111111111111111".to_string()));
    }

    #[test]
    fn test_pick_main_program() {
        let program_ids = vec![
            "ComputeBudget111111111111111111111111111111".to_string(),
            "11111111111111111111111111111111".to_string(),
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(),
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
        ];

        let main = pick_main_program(&program_ids);
        assert_eq!(
            main,
            Some("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string())
        );
    }

    #[test]
    fn test_pick_main_program_only_system() {
        let program_ids = vec![
            "ComputeBudget111111111111111111111111111111".to_string(),
            "11111111111111111111111111111111".to_string(),
        ];

        let main = pick_main_program(&program_ids);
        assert_eq!(main, None);
    }

    #[test]
    fn test_deduplication() {
        let tx = json!({
            "transaction": {
                "message": {
                    "accountKeys": [
                        "Account1",
                        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
                    ],
                    "instructions": [
                        {"programIdIndex": 1},
                        {"programIdIndex": 1}, // Duplicate
                        {"programIdIndex": 1}  // Duplicate
                    ]
                }
            },
            "meta": {}
        });

        let program_ids = extract_program_ids_from_transaction(&tx);
        assert_eq!(program_ids.len(), 1);
        assert_eq!(
            program_ids[0],
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
        );
    }
}
