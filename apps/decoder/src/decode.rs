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
    use std::collections::HashMap;

    // key = (account_index, mint)
    // value = (decimals, amount_base_units)
    let mut pre_map: HashMap<(u32, String), (Option<u8>, u64)> = HashMap::new();
    let mut post_map: HashMap<(u32, String), (Option<u8>, u64)> = HashMap::new();

    let pre = tx.pointer("/meta/preTokenBalances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let post = tx.pointer("/meta/postTokenBalances")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let parse_amount_u64 = |b: &Value| -> u64 {
        // uiTokenAmount.amount is a string integer in base units
        let s = b.pointer("/uiTokenAmount/amount")
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
        let mint = b.get("mint").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if mint.is_empty() { continue; }

        let amt = parse_amount_u64(b);
        let decimals = parse_decimals(b);
        pre_map.insert((idx, mint), (decimals, amt));
    }

    for b in post.iter() {
        let idx = b.get("accountIndex").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let mint = b.get("mint").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if mint.is_empty() { continue; }

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
        let (dec_pre, pre_amt) = pre_map.get(&(idx, mint.clone())).cloned().unwrap_or((None, 0));
        let (dec_post, post_amt) = post_map.get(&(idx, mint.clone())).cloned().unwrap_or((None, 0));

        if pre_amt == post_amt { continue; }

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
