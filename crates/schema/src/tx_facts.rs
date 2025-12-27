//! TxFacts: Pre-computed transaction facts for pure parsing.
//!
//! This module provides a clean abstraction layer that extracts all relevant
//! facts from a transaction JSON once, enabling parsers to be pure functions
//! without RPC calls.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::alt_resolver::resolve_full_account_keys;

/// Parsed instruction from a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedInstruction {
    /// Program ID that executed this instruction
    pub program_id: String,

    /// Account indices (into full_account_keys)
    pub accounts: Vec<usize>,

    /// Base58-encoded instruction data (if available)
    pub data: Option<String>,

    /// Index of the outer instruction this belongs to (for inner ix)
    pub outer_ix_index: Option<usize>,

    /// Stack depth (0 = outer, 1+ = inner/CPI)
    pub stack_depth: u8,
}

/// Token balance for a specific account
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBalance {
    /// Account index in the transaction
    pub account_index: u32,

    /// Token mint address
    pub mint: String,

    /// Owner of the token account
    pub owner: Option<String>,

    /// Amount in base units (as string for precision)
    pub amount: String,

    /// Decimals
    pub decimals: Option<u8>,
}

/// Token balance delta (change between pre and post)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBalanceDelta {
    /// Account index
    pub account_index: u32,

    /// Token mint address
    pub mint: String,

    /// Owner of the token account
    pub owner: Option<String>,

    /// Pre-transaction amount
    pub pre_amount: u128,

    /// Post-transaction amount
    pub post_amount: u128,

    /// Delta (post - pre), can be negative
    pub delta: i128,

    /// Decimals
    pub decimals: Option<u8>,
}

/// SOL balance delta
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolBalanceDelta {
    /// Account index
    pub account_index: usize,

    /// Account pubkey
    pub account: String,

    /// Pre-transaction balance (lamports)
    pub pre_balance: u64,

    /// Post-transaction balance (lamports)
    pub post_balance: u64,

    /// Delta (post - pre)
    pub delta: i64,
}

/// Pre-computed facts about a transaction.
///
/// All fields are computed once from the transaction JSON.
/// Parsers receive this struct and produce outputs without side effects.
#[derive(Debug, Clone)]
pub struct TxFacts {
    /// Transaction signature
    pub signature: String,

    /// Slot number
    pub slot: u64,

    /// Block timestamp (Unix seconds)
    pub block_time: Option<i64>,

    /// Transaction version (None = legacy, Some(0) = v0)
    pub version: Option<u8>,

    /// Whether the transaction succeeded
    pub is_success: bool,

    /// Fee paid (lamports)
    pub fee: u64,

    /// Compute units consumed
    pub compute_units: Option<u64>,

    /// Full account keys (accountKeys + loadedAddresses for v0)
    pub full_account_keys: Vec<String>,

    /// Number of static account keys (before loadedAddresses)
    pub static_account_keys_len: usize,

    /// Outer instructions (top-level)
    pub outer_instructions: Vec<ParsedInstruction>,

    /// All instructions (outer + inner, flattened with metadata)
    pub all_instructions: Vec<ParsedInstruction>,

    /// Pre-transaction token balances
    pub pre_token_balances: Vec<TokenBalance>,

    /// Post-transaction token balances
    pub post_token_balances: Vec<TokenBalance>,

    /// Token balance deltas (computed)
    pub token_balance_deltas: Vec<TokenBalanceDelta>,

    /// SOL balance deltas
    pub sol_balance_deltas: Vec<SolBalanceDelta>,

    /// Log messages (if available)
    pub logs: Vec<String>,

    /// Whether this is a v0 transaction with loaded addresses
    pub has_loaded_addresses: bool,
}

impl TxFacts {
    /// Extract all facts from a transaction JSON.
    ///
    /// This function should be called once per transaction.
    /// It handles both legacy and v0 transactions with ALT.
    pub fn from_json(tx: &Value, signature: &str, slot: u64) -> Self {
        let block_time = tx.get("blockTime").and_then(|v| v.as_i64());

        let version = tx.get("version").and_then(|v| v.as_u64()).map(|v| v as u8);

        let is_success = tx.pointer("/meta/err").map(|e| e.is_null()).unwrap_or(false);

        let fee = tx
            .pointer("/meta/fee")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let compute_units = tx
            .pointer("/meta/computeUnitsConsumed")
            .and_then(|v| v.as_u64());

        // Resolve full account keys (handles v0 + ALT)
        let full_account_keys = resolve_full_account_keys(tx);

        // Count static keys (before loaded addresses)
        let static_account_keys_len = tx
            .pointer("/transaction/message/accountKeys")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);

        let has_loaded_addresses = tx.pointer("/meta/loadedAddresses").is_some();

        // Parse outer instructions
        let outer_instructions = Self::parse_outer_instructions(tx, &full_account_keys);

        // Parse all instructions (outer + inner)
        let all_instructions = Self::parse_all_instructions(tx, &full_account_keys);

        // Parse token balances
        let pre_token_balances = Self::parse_token_balances(tx, "/meta/preTokenBalances");
        let post_token_balances = Self::parse_token_balances(tx, "/meta/postTokenBalances");

        // Compute token balance deltas
        let token_balance_deltas =
            Self::compute_token_deltas(&pre_token_balances, &post_token_balances);

        // Parse SOL balance deltas
        let sol_balance_deltas = Self::parse_sol_deltas(tx, &full_account_keys);

        // Parse logs
        let logs = tx
            .pointer("/meta/logMessages")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Self {
            signature: signature.to_string(),
            slot,
            block_time,
            version,
            is_success,
            fee,
            compute_units,
            full_account_keys,
            static_account_keys_len,
            outer_instructions,
            all_instructions,
            pre_token_balances,
            post_token_balances,
            token_balance_deltas,
            sol_balance_deltas,
            logs,
            has_loaded_addresses,
        }
    }

    fn parse_outer_instructions(tx: &Value, account_keys: &[String]) -> Vec<ParsedInstruction> {
        let mut out = Vec::new();

        let instructions = match tx.pointer("/transaction/message/instructions") {
            Some(v) => v.as_array().cloned().unwrap_or_default(),
            None => return out,
        };

        for (idx, ix) in instructions.iter().enumerate() {
            if let Some(parsed) = Self::parse_single_instruction(ix, account_keys, None, 0, idx) {
                out.push(parsed);
            }
        }

        out
    }

    fn parse_all_instructions(tx: &Value, account_keys: &[String]) -> Vec<ParsedInstruction> {
        let mut out = Vec::new();

        // Outer instructions
        let outer = match tx.pointer("/transaction/message/instructions") {
            Some(v) => v.as_array().cloned().unwrap_or_default(),
            None => return out,
        };

        for (idx, ix) in outer.iter().enumerate() {
            if let Some(parsed) = Self::parse_single_instruction(ix, account_keys, None, 0, idx) {
                out.push(parsed);
            }
        }

        // Inner instructions
        let inner_groups = tx
            .pointer("/meta/innerInstructions")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for group in inner_groups {
            let outer_idx = group.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

            let inner_ixs = group
                .get("instructions")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            for (inner_idx, ix) in inner_ixs.iter().enumerate() {
                // Determine stack depth from stackHeight if available
                let stack_depth = ix
                    .get("stackHeight")
                    .and_then(|v| v.as_u64())
                    .map(|h| h as u8)
                    .unwrap_or(1);

                if let Some(parsed) =
                    Self::parse_single_instruction(ix, account_keys, Some(outer_idx), stack_depth, inner_idx)
                {
                    out.push(parsed);
                }
            }
        }

        out
    }

    fn parse_single_instruction(
        ix: &Value,
        account_keys: &[String],
        outer_ix_index: Option<usize>,
        stack_depth: u8,
        _ix_index: usize,
    ) -> Option<ParsedInstruction> {
        // Get program ID
        let program_id = if let Some(pid) = ix.get("programId").and_then(|v| v.as_str()) {
            // jsonParsed format
            pid.to_string()
        } else if let Some(idx) = ix.get("programIdIndex").and_then(|v| v.as_u64()) {
            // Raw format: resolve from account keys
            account_keys.get(idx as usize)?.clone()
        } else {
            return None;
        };

        // Get accounts
        let accounts: Vec<usize> = ix
            .get("accounts")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_u64().map(|n| n as usize))
                    .collect()
            })
            .unwrap_or_default();

        // Get data
        let data = ix.get("data").and_then(|v| v.as_str()).map(|s| s.to_string());

        Some(ParsedInstruction {
            program_id,
            accounts,
            data,
            outer_ix_index,
            stack_depth,
        })
    }

    fn parse_token_balances(tx: &Value, path: &str) -> Vec<TokenBalance> {
        let balances = tx.pointer(path).and_then(|v| v.as_array());

        match balances {
            Some(arr) => arr
                .iter()
                .filter_map(|b| {
                    let account_index = b.get("accountIndex")?.as_u64()? as u32;
                    let mint = b.get("mint")?.as_str()?.to_string();
                    let owner = b.get("owner").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let amount = b
                        .pointer("/uiTokenAmount/amount")
                        .and_then(|v| v.as_str())
                        .unwrap_or("0")
                        .to_string();
                    let decimals = b
                        .pointer("/uiTokenAmount/decimals")
                        .and_then(|v| v.as_u64())
                        .map(|d| d as u8);

                    Some(TokenBalance {
                        account_index,
                        mint,
                        owner,
                        amount,
                        decimals,
                    })
                })
                .collect(),
            None => Vec::new(),
        }
    }

    fn compute_token_deltas(
        pre: &[TokenBalance],
        post: &[TokenBalance],
    ) -> Vec<TokenBalanceDelta> {
        // Key: (account_index, mint)
        let mut pre_map: HashMap<(u32, String), &TokenBalance> = HashMap::new();
        for b in pre {
            pre_map.insert((b.account_index, b.mint.clone()), b);
        }

        let mut post_map: HashMap<(u32, String), &TokenBalance> = HashMap::new();
        for b in post {
            post_map.insert((b.account_index, b.mint.clone()), b);
        }

        // Union of keys
        let mut all_keys: Vec<(u32, String)> = pre_map.keys().cloned().collect();
        for k in post_map.keys() {
            if !pre_map.contains_key(k) {
                all_keys.push(k.clone());
            }
        }
        all_keys.sort();
        all_keys.dedup();

        let mut deltas = Vec::new();

        for (account_index, mint) in all_keys {
            let pre_bal = pre_map.get(&(account_index, mint.clone()));
            let post_bal = post_map.get(&(account_index, mint.clone()));

            let pre_amount: u128 = pre_bal
                .map(|b| b.amount.parse().unwrap_or(0))
                .unwrap_or(0);
            let post_amount: u128 = post_bal
                .map(|b| b.amount.parse().unwrap_or(0))
                .unwrap_or(0);

            if pre_amount == post_amount {
                continue;
            }

            let delta = post_amount as i128 - pre_amount as i128;

            let owner = post_bal
                .and_then(|b| b.owner.clone())
                .or_else(|| pre_bal.and_then(|b| b.owner.clone()));

            let decimals = post_bal
                .and_then(|b| b.decimals)
                .or_else(|| pre_bal.and_then(|b| b.decimals));

            deltas.push(TokenBalanceDelta {
                account_index,
                mint,
                owner,
                pre_amount,
                post_amount,
                delta,
                decimals,
            });
        }

        deltas
    }

    fn parse_sol_deltas(tx: &Value, account_keys: &[String]) -> Vec<SolBalanceDelta> {
        let pre = tx
            .pointer("/meta/preBalances")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let post = tx
            .pointer("/meta/postBalances")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let n = std::cmp::min(account_keys.len(), std::cmp::min(pre.len(), post.len()));

        let mut deltas = Vec::new();

        for i in 0..n {
            let pre_bal = pre[i].as_u64().unwrap_or(0);
            let post_bal = post[i].as_u64().unwrap_or(0);

            if pre_bal == post_bal {
                continue;
            }

            let delta = post_bal as i64 - pre_bal as i64;

            deltas.push(SolBalanceDelta {
                account_index: i,
                account: account_keys[i].clone(),
                pre_balance: pre_bal,
                post_balance: post_bal,
                delta,
            });
        }

        deltas
    }

    /// Get token balance deltas for a specific owner
    pub fn token_deltas_for_owner(&self, owner: &str) -> Vec<&TokenBalanceDelta> {
        self.token_balance_deltas
            .iter()
            .filter(|d| d.owner.as_deref() == Some(owner))
            .collect()
    }

    /// Get instructions for a specific program
    pub fn instructions_for_program(&self, program_id: &str) -> Vec<&ParsedInstruction> {
        self.all_instructions
            .iter()
            .filter(|ix| ix.program_id == program_id)
            .collect()
    }

    /// Get the fee payer (first account key)
    pub fn fee_payer(&self) -> Option<&str> {
        self.full_account_keys.first().map(|s| s.as_str())
    }

    /// Check if a program was invoked in this transaction
    pub fn has_program(&self, program_id: &str) -> bool {
        self.all_instructions.iter().any(|ix| ix.program_id == program_id)
    }

    /// Get account pubkey by index
    pub fn account_at(&self, index: usize) -> Option<&str> {
        self.full_account_keys.get(index).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_tx_json() -> Value {
        json!({
            "blockTime": 1703001234,
            "meta": {
                "err": null,
                "fee": 5000,
                "computeUnitsConsumed": 12345,
                "preBalances": [1000000000, 500000000],
                "postBalances": [999995000, 500000000],
                "preTokenBalances": [
                    {
                        "accountIndex": 1,
                        "mint": "So11111111111111111111111111111111111111112",
                        "owner": "TraderWallet111",
                        "uiTokenAmount": {
                            "amount": "1000000000",
                            "decimals": 9
                        }
                    }
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 1,
                        "mint": "So11111111111111111111111111111111111111112",
                        "owner": "TraderWallet111",
                        "uiTokenAmount": {
                            "amount": "500000000",
                            "decimals": 9
                        }
                    }
                ],
                "innerInstructions": [],
                "logMessages": ["Program log: test"]
            },
            "slot": 250000000,
            "transaction": {
                "message": {
                    "accountKeys": [
                        "FeePayer111",
                        "TokenAccount111"
                    ],
                    "instructions": [
                        {
                            "programIdIndex": 0,
                            "accounts": [0, 1],
                            "data": "test"
                        }
                    ]
                },
                "signatures": ["sig123"]
            }
        })
    }

    #[test]
    fn test_tx_facts_basic() {
        let tx = sample_tx_json();
        let facts = TxFacts::from_json(&tx, "sig123", 250000000);

        assert_eq!(facts.signature, "sig123");
        assert_eq!(facts.slot, 250000000);
        assert_eq!(facts.block_time, Some(1703001234));
        assert!(facts.is_success);
        assert_eq!(facts.fee, 5000);
        assert_eq!(facts.compute_units, Some(12345));
    }

    #[test]
    fn test_tx_facts_account_keys() {
        let tx = sample_tx_json();
        let facts = TxFacts::from_json(&tx, "sig123", 250000000);

        assert_eq!(facts.full_account_keys.len(), 2);
        assert_eq!(facts.fee_payer(), Some("FeePayer111"));
    }

    #[test]
    fn test_tx_facts_token_deltas() {
        let tx = sample_tx_json();
        let facts = TxFacts::from_json(&tx, "sig123", 250000000);

        assert_eq!(facts.token_balance_deltas.len(), 1);
        let delta = &facts.token_balance_deltas[0];
        assert_eq!(delta.pre_amount, 1000000000);
        assert_eq!(delta.post_amount, 500000000);
        assert_eq!(delta.delta, -500000000);
    }

    #[test]
    fn test_tx_facts_sol_deltas() {
        let tx = sample_tx_json();
        let facts = TxFacts::from_json(&tx, "sig123", 250000000);

        assert_eq!(facts.sol_balance_deltas.len(), 1);
        let delta = &facts.sol_balance_deltas[0];
        assert_eq!(delta.delta, -5000); // Fee paid
    }

    #[test]
    fn test_tx_facts_deltas_for_owner() {
        let tx = sample_tx_json();
        let facts = TxFacts::from_json(&tx, "sig123", 250000000);

        let deltas = facts.token_deltas_for_owner("TraderWallet111");
        assert_eq!(deltas.len(), 1);
    }

    #[test]
    fn test_tx_facts_v0_with_alt() {
        let tx = json!({
            "blockTime": 1703001234,
            "version": 0,
            "meta": {
                "err": null,
                "fee": 5000,
                "loadedAddresses": {
                    "writable": ["WritableAddr"],
                    "readonly": ["ReadonlyAddr"]
                },
                "preBalances": [],
                "postBalances": [],
                "preTokenBalances": [],
                "postTokenBalances": [],
                "innerInstructions": []
            },
            "slot": 250000000,
            "transaction": {
                "message": {
                    "accountKeys": ["FeePayer", "Account2"],
                    "instructions": [
                        {
                            "programIdIndex": 3,
                            "accounts": [0, 1, 2],
                            "data": "test"
                        }
                    ]
                },
                "signatures": ["sig_v0"]
            }
        });

        let facts = TxFacts::from_json(&tx, "sig_v0", 250000000);

        assert_eq!(facts.version, Some(0));
        assert!(facts.has_loaded_addresses);
        assert_eq!(facts.full_account_keys.len(), 4); // 2 static + 1 writable + 1 readonly
        assert_eq!(facts.static_account_keys_len, 2);

        // Verify order: accountKeys + writable + readonly
        assert_eq!(facts.full_account_keys[0], "FeePayer");
        assert_eq!(facts.full_account_keys[1], "Account2");
        assert_eq!(facts.full_account_keys[2], "WritableAddr");
        assert_eq!(facts.full_account_keys[3], "ReadonlyAddr");
    }
}
