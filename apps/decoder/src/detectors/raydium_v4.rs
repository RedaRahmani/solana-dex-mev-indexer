use schema::SwapEvent;
use serde_json::Value;
use std::collections::HashMap;

#[allow(clippy::too_many_arguments)]
pub fn detect_raydium_v4_swap(
    chain: &str,
    slot: u64,
    block_time: Option<i64>,
    signature: &str,
    raw_program_ids: &[String],
    raydium_amm_v4_program_id: &str,
    tx: &Value,
    explain: bool,
) -> Option<SwapEvent> {
    // Confirm Raydium program appears in the tx program list (cheap gate).
    if !raw_program_ids
        .iter()
        .any(|p| p == raydium_amm_v4_program_id)
    {
        return None;
    }

    // Determine trader: fee payer (accountKeys[0].pubkey) for jsonParsed.
    let trader = tx_pointer_str(tx, "/transaction/message/accountKeys/0/pubkey")
        .or_else(|| tx_pointer_str(tx, "/transaction/message/accountKeys/0"))
        .map(|s| s.to_string())?;

    // Extract token balances: pre + post
    let pre = tx
        .pointer("/meta/preTokenBalances")?
        .as_array()
        .cloned()
        .unwrap_or_default();
    let post = tx
        .pointer("/meta/postTokenBalances")?
        .as_array()
        .cloned()
        .unwrap_or_default();

    // Build maps mint -> amount for this trader (string amounts in base units).
    let pre_map = token_amounts_by_mint_for_owner(&pre, &trader);
    let post_map = token_amounts_by_mint_for_owner(&post, &trader);

    // Compute deltas per mint
    let mut deltas: Vec<(String, i128, String, String)> = vec![];
    let mut all_mints = pre_map.keys().cloned().collect::<Vec<_>>();
    for k in post_map.keys() {
        if !pre_map.contains_key(k) {
            all_mints.push(k.clone());
        }
    }
    all_mints.sort();
    all_mints.dedup();

    for mint in all_mints {
        let pre_amt = pre_map
            .get(&mint)
            .cloned()
            .unwrap_or_else(|| "0".to_string());
        let post_amt = post_map
            .get(&mint)
            .cloned()
            .unwrap_or_else(|| "0".to_string());

        let pre_i = parse_i128(&pre_amt)?;
        let post_i = parse_i128(&post_amt)?;
        let d = post_i - pre_i;

        if d != 0 {
            deltas.push((mint, d, pre_amt, post_amt));
        }
    }

    // Need exactly one in (negative) and one out (positive) for v1.
    let mut neg = deltas
        .iter()
        .filter(|(_, d, _, _)| *d < 0)
        .collect::<Vec<_>>();
    let mut pos = deltas
        .iter()
        .filter(|(_, d, _, _)| *d > 0)
        .collect::<Vec<_>>();

    if neg.len() != 1 || pos.len() != 1 {
        return None;
    }

    let (in_mint, in_delta, _, _) = neg.remove(0);
    let (out_mint, out_delta, _, _) = pos.remove(0);

    let in_amount = (-(*in_delta)).to_string();
    let out_amount = (*out_delta).to_string();

    let explain_str = if explain {
        Some(format!(
            "raydium_v4 gate=hit trader={} in={} (-{}) out={} (+{})",
            trader, in_mint, in_amount, out_mint, out_amount
        ))
    } else {
        None
    };

    Some(SwapEvent {
        schema_version: 1,
        chain: chain.to_string(),
        slot,
        block_time,
        signature: signature.to_string(),
        index_in_tx: 0,
        venue: "raydium".to_string(),
        market_or_pool: None,
        trader,
        in_mint: in_mint.clone(),
        in_amount,
        out_mint: out_mint.clone(),
        out_amount,
        fee_mint: None,
        fee_amount: None,
        route_id: None,
        confidence: 80,
        explain: explain_str,
    })
}

fn token_amounts_by_mint_for_owner(arr: &[Value], owner: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for v in arr {
        let v_owner = v.get("owner").and_then(|x| x.as_str()).unwrap_or("");
        if v_owner != owner {
            continue;
        }
        let mint = v.get("mint").and_then(|x| x.as_str());
        if mint.is_none() {
            continue;
        }
        let mint = mint.unwrap().to_string();

        let amount = v
            .get("uiTokenAmount")
            .and_then(|u| u.get("amount"))
            .and_then(|a| a.as_str())
            .unwrap_or("0")
            .to_string();

        out.insert(mint, amount);
    }
    out
}

fn parse_i128(s: &str) -> Option<i128> {
    s.parse::<i128>().ok()
}

fn tx_pointer_str<'a>(tx: &'a Value, ptr: &str) -> Option<&'a str> {
    tx.pointer(ptr).and_then(|v| v.as_str())
}
