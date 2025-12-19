use crate::types::{SolBalanceDelta, TokenBalanceDelta};
use log::debug;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

/// Helper function to inspect token balances in a transaction for debugging
pub fn inspect_token_balances(tx: &Value) -> (usize, usize, usize) {
    let pre = tx
        .pointer("/meta/preTokenBalances")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    let post = tx
        .pointer("/meta/postTokenBalances")
        .and_then(|v| v.as_array())
        .map(|a| a.len())
        .unwrap_or(0);

    // Count unique mints
    let mut mints = std::collections::HashSet::new();
    if let Some(arr) = tx
        .pointer("/meta/preTokenBalances")
        .and_then(|v| v.as_array())
    {
        for b in arr {
            if let Some(mint) = b.get("mint").and_then(|m| m.as_str()) {
                mints.insert(mint.to_string());
            }
        }
    }
    if let Some(arr) = tx
        .pointer("/meta/postTokenBalances")
        .and_then(|v| v.as_array())
    {
        for b in arr {
            if let Some(mint) = b.get("mint").and_then(|m| m.as_str()) {
                mints.insert(mint.to_string());
            }
        }
    }

    (pre, post, mints.len())
}

pub fn decode_sol_deltas(
    slot: u64,
    block_time: Option<i64>,
    sig: &str,
    tx: &Value,
) -> Vec<SolBalanceDelta> {
    let mut out = vec![];

    // accountKeys list (jsonParsed style: list of objects with pubkey or strings)
    let keys = tx
        .pointer("/transaction/message/accountKeys")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

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

    let n = std::cmp::min(keys.len(), std::cmp::min(pre.len(), post.len()));

    for i in 0..n {
        let pubkey = keys[i]
            .get("pubkey")
            .and_then(|p| p.as_str())
            .or_else(|| keys[i].as_str())
            .unwrap_or("")
            .to_string();

        if pubkey.is_empty() {
            continue;
        }

        let pre_u = pre[i].as_u64().unwrap_or(0);
        let post_u = post[i].as_u64().unwrap_or(0);
        let delta = post_u as i128 - pre_u as i128;

        if delta == 0 {
            continue;
        }

        out.push(SolBalanceDelta {
            slot,
            block_time,
            signature: sig.to_string(),
            account: pubkey,
            pre_balance: pre_u,
            post_balance: post_u,
            delta: delta as i64,
        });
    }

    out
}

pub fn decode_token_deltas(
    slot: u64,
    block_time: Option<i64>,
    sig: &str,
    tx: &Value,
) -> Vec<TokenBalanceDelta> {
    use std::collections::HashMap;

    // key = (account_index, mint)
    // value = (decimals, amount_base_units)
    let mut pre_map: HashMap<(u32, String), (Option<u8>, u64)> = HashMap::new();
    let mut post_map: HashMap<(u32, String), (Option<u8>, u64)> = HashMap::new();

    let pre = tx
        .pointer("/meta/preTokenBalances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let post = tx
        .pointer("/meta/postTokenBalances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Debug logging for token balances
    debug!(
        "decode_token_deltas sig={} preTokenBalances={} postTokenBalances={}",
        sig,
        pre.len(),
        post.len()
    );

    // Rate-limited warning when both are empty (log every 1000th occurrence)
    if pre.is_empty() && post.is_empty() {
        static EMPTY_COUNTER: AtomicU64 = AtomicU64::new(0);
        let count = EMPTY_COUNTER.fetch_add(1, Ordering::Relaxed);
        if count % 1000 == 0 {
            debug!("token balances both empty (seen {} times total)", count + 1);
        }
    }

    let parse_amount_u64 = |b: &Value| -> u64 {
        // uiTokenAmount.amount is a string integer in base units
        let s = b
            .pointer("/uiTokenAmount/amount")
            .and_then(|v| v.as_str())
            .unwrap_or("0");
        s.parse::<u64>().unwrap_or(0)
    };

    let parse_decimals = |b: &Value| -> Option<u8> {
        b.pointer("/uiTokenAmount/decimals")
            .and_then(|v| v.as_u64())
            .and_then(|d| u8::try_from(d).ok())
    };

    for b in pre.iter() {
        let idx = b.get("accountIndex").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let mint = b
            .get("mint")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if mint.is_empty() {
            continue;
        }

        let amt = parse_amount_u64(b);
        let decimals = parse_decimals(b);
        pre_map.insert((idx, mint), (decimals, amt));
    }

    for b in post.iter() {
        let idx = b.get("accountIndex").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let mint = b
            .get("mint")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if mint.is_empty() {
            continue;
        }

        let amt = parse_amount_u64(b);
        let decimals = parse_decimals(b);
        post_map.insert((idx, mint), (decimals, amt));
    }

    // union of keys
    let mut keys: Vec<(u32, String)> = pre_map.keys().cloned().collect();
    for k in post_map.keys() {
        if !pre_map.contains_key(k) {
            keys.push(k.clone());
        }
    }

    let mut out = vec![];
    for (idx, mint) in keys {
        let (dec_pre, pre_amt) = pre_map
            .get(&(idx, mint.clone()))
            .cloned()
            .unwrap_or((None, 0));
        let (dec_post, post_amt) = post_map
            .get(&(idx, mint.clone()))
            .cloned()
            .unwrap_or((None, 0));

        if pre_amt == post_amt {
            continue;
        }

        let decimals = dec_post.or(dec_pre);

        let delta_i128 = post_amt as i128 - pre_amt as i128;
        let delta = delta_i128.clamp(i64::MIN as i128, i64::MAX as i128) as i64;

        out.push(TokenBalanceDelta {
            slot,
            block_time,
            signature: sig.to_string(),
            account_index: idx,
            mint,
            decimals,
            pre_amount: pre_amt,
            post_amount: post_amt,
            delta,
        });
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_decode_token_deltas_with_balances() {
        // Fixture: transaction with token balance changes
        let tx = json!({
            "meta": {
                "preTokenBalances": [
                    {
                        "accountIndex": 2,
                        "mint": "So11111111111111111111111111111111111111112",
                        "uiTokenAmount": {
                            "amount": "1000000000",
                            "decimals": 9,
                            "uiAmount": 1.0
                        }
                    },
                    {
                        "accountIndex": 4,
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "uiTokenAmount": {
                            "amount": "5000000",
                            "decimals": 6,
                            "uiAmount": 5.0
                        }
                    }
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 2,
                        "mint": "So11111111111111111111111111111111111111112",
                        "uiTokenAmount": {
                            "amount": "500000000",
                            "decimals": 9,
                            "uiAmount": 0.5
                        }
                    },
                    {
                        "accountIndex": 4,
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "uiTokenAmount": {
                            "amount": "10000000",
                            "decimals": 6,
                            "uiAmount": 10.0
                        }
                    }
                ]
            }
        });

        let deltas = decode_token_deltas(123456, Some(1734643200), "test_sig_123", &tx);

        // Should have 2 deltas
        assert_eq!(deltas.len(), 2);

        // Find the SOL token delta
        let sol_delta = deltas
            .iter()
            .find(|d| d.mint == "So11111111111111111111111111111111111111112")
            .expect("SOL delta should exist");

        assert_eq!(sol_delta.account_index, 2);
        assert_eq!(sol_delta.pre_amount, 1000000000);
        assert_eq!(sol_delta.post_amount, 500000000);
        assert_eq!(sol_delta.delta, -500000000);
        assert_eq!(sol_delta.decimals, Some(9));

        // Find the USDC delta
        let usdc_delta = deltas
            .iter()
            .find(|d| d.mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
            .expect("USDC delta should exist");

        assert_eq!(usdc_delta.account_index, 4);
        assert_eq!(usdc_delta.pre_amount, 5000000);
        assert_eq!(usdc_delta.post_amount, 10000000);
        assert_eq!(usdc_delta.delta, 5000000);
        assert_eq!(usdc_delta.decimals, Some(6));
    }

    #[test]
    fn test_decode_token_deltas_empty() {
        // Fixture: transaction without token balances
        let tx = json!({
            "meta": {
                "preTokenBalances": [],
                "postTokenBalances": []
            }
        });

        let deltas = decode_token_deltas(123456, Some(1734643200), "test_sig_empty", &tx);

        // Should be empty
        assert_eq!(deltas.len(), 0);
    }

    #[test]
    fn test_decode_token_deltas_missing_balances() {
        // Fixture: transaction without token balance fields
        let tx = json!({
            "meta": {
                "preBalances": [1000000, 2000000],
                "postBalances": [1500000, 1500000]
            }
        });

        let deltas = decode_token_deltas(123456, Some(1734643200), "test_sig_missing", &tx);

        // Should be empty when token balance fields are missing
        assert_eq!(deltas.len(), 0);
    }

    #[test]
    fn test_decode_token_deltas_no_change() {
        // Fixture: token balances with no change
        let tx = json!({
            "meta": {
                "preTokenBalances": [
                    {
                        "accountIndex": 2,
                        "mint": "So11111111111111111111111111111111111111112",
                        "uiTokenAmount": {
                            "amount": "1000000000",
                            "decimals": 9,
                            "uiAmount": 1.0
                        }
                    }
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 2,
                        "mint": "So11111111111111111111111111111111111111112",
                        "uiTokenAmount": {
                            "amount": "1000000000",
                            "decimals": 9,
                            "uiAmount": 1.0
                        }
                    }
                ]
            }
        });

        let deltas = decode_token_deltas(123456, Some(1734643200), "test_sig_no_change", &tx);

        // Should be empty when amounts don't change
        assert_eq!(deltas.len(), 0);
    }

    #[test]
    fn test_inspect_token_balances() {
        let tx = json!({
            "meta": {
                "preTokenBalances": [
                    {
                        "accountIndex": 2,
                        "mint": "So11111111111111111111111111111111111111112",
                        "uiTokenAmount": {
                            "amount": "1000000000",
                            "decimals": 9
                        }
                    },
                    {
                        "accountIndex": 4,
                        "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "uiTokenAmount": {
                            "amount": "5000000",
                            "decimals": 6
                        }
                    }
                ],
                "postTokenBalances": [
                    {
                        "accountIndex": 2,
                        "mint": "So11111111111111111111111111111111111111112",
                        "uiTokenAmount": {
                            "amount": "500000000",
                            "decimals": 9
                        }
                    }
                ]
            }
        });

        let (pre_len, post_len, unique_mints) = inspect_token_balances(&tx);
        assert_eq!(pre_len, 2);
        assert_eq!(post_len, 1);
        assert_eq!(unique_mints, 2); // 2 unique mints across pre and post
    }

    #[test]
    fn test_inspect_token_balances_empty() {
        let tx = json!({
            "meta": {}
        });

        let (pre_len, post_len, unique_mints) = inspect_token_balances(&tx);
        assert_eq!(pre_len, 0);
        assert_eq!(post_len, 0);
        assert_eq!(unique_mints, 0);
    }
}
