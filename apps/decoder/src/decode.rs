use crate::types::{SolBalanceDelta, TokenBalanceDelta};
use serde_json::Value;

pub fn decode_sol_deltas(slot: u64, block_time: Option<i64>, sig: &str, tx: &Value) -> Vec<SolBalanceDelta> {
    let mut out = vec![];

    // accountKeys list (jsonParsed style: list of objects with pubkey or strings)
    let keys = tx.pointer("/transaction/message/accountKeys")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let pre = tx.pointer("/meta/preBalances").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let post = tx.pointer("/meta/postBalances").and_then(|v| v.as_array()).cloned().unwrap_or_default();

    let n = std::cmp::min(keys.len(), std::cmp::min(pre.len(), post.len()));

    for i in 0..n {
        let pubkey = keys[i]
            .get("pubkey").and_then(|p| p.as_str())
            .or_else(|| keys[i].as_str())
            .unwrap_or("")
            .to_string();

        if pubkey.is_empty() { continue; }

        let pre_u = pre[i].as_u64().unwrap_or(0);
        let post_u = post[i].as_u64().unwrap_or(0);
        let delta = post_u as i128 - pre_u as i128;

        if delta == 0 { continue; }

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

pub fn decode_token_deltas(slot: u64, block_time: Option<i64>, sig: &str, tx: &Value) -> Vec<TokenBalanceDelta> {
    // We compute deltas by (accountIndex + mint) comparing preTokenBalances/postTokenBalances.
    // Values are strings; keep them strings for safety.
    use std::collections::HashMap;

    let mut pre_map: HashMap<(u32, String), (Option<String>, String)> = HashMap::new();
    let mut post_map: HashMap<(u32, String), (Option<String>, String)> = HashMap::new();

    let pre = tx.pointer("/meta/preTokenBalances").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    let post = tx.pointer("/meta/postTokenBalances").and_then(|v| v.as_array()).cloned().unwrap_or_default();

    for b in pre {
        let idx = b.get("accountIndex").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let mint = b.get("mint").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let owner = b.get("owner").and_then(|v| v.as_str()).map(|s| s.to_string());
        let amt = b.pointer("/uiTokenAmount/amount").and_then(|v| v.as_str()).unwrap_or("0").to_string();
        if !mint.is_empty() {
            pre_map.insert((idx, mint), (owner, amt));
        }
    }

    for b in post {
        let idx = b.get("accountIndex").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let mint = b.get("mint").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let owner = b.get("owner").and_then(|v| v.as_str()).map(|s| s.to_string());
        let amt = b.pointer("/uiTokenAmount/amount").and_then(|v| v.as_str()).unwrap_or("0").to_string();
        if !mint.is_empty() {
            post_map.insert((idx, mint), (owner, amt));
        }
    }

    let mut out = vec![];
    for (k, (owner_post, post_amt)) in post_map.iter() {
        let (idx, mint) = k.clone();
        let (owner_pre, pre_amt) = pre_map.get(&k).cloned().unwrap_or((None, "0".to_string()));

        if pre_amt == *post_amt { continue; }

        let pre_amount = pre_amt.clone();
        let post_amount = post_amt.clone();

        out.push(TokenBalanceDelta {
            slot,
            block_time,
            signature: sig.to_string(),
            owner: owner_post.clone().or(owner_pre),
            account_index: idx,
            mint,
            pre_amount,
            post_amount: post_amount.clone(),
            delta: format!("{} -> {}", pre_amt, post_amount),
        });

    }

    out
}
