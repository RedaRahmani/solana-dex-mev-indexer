use anyhow::{anyhow, Result};
use std::env;

/// Solana mainnet genesis hash (base58)
pub const SOLANA_MAINNET_GENESIS_HASH: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Local,
    Mainnet,
}

impl Profile {
    pub fn from_env() -> Result<Self> {
        match env::var("PROFILE")
            .unwrap_or_else(|_| "local".to_string())
            .to_lowercase()
            .as_str()
        {
            "local" | "" => Ok(Profile::Local),
            "mainnet" => Ok(Profile::Mainnet),
            other => Err(anyhow!("Invalid PROFILE={other}. Use 'local' or 'mainnet'")),
        }
    }

    pub fn default_geyser_endpoint(&self) -> &'static str {
        match self {
            Profile::Local => "http://127.0.0.1:10000",
            Profile::Mainnet => "https://solana-rpc.parafi.tech:10443",
        }
    }

    pub fn default_rpc_url(&self) -> &'static str {
        match self {
            Profile::Local => "http://127.0.0.1:8899",
            Profile::Mainnet => "https://solana-rpc.parafi.tech",
        }
    }

    pub fn default_kafka_topic_raw_txs(&self) -> &'static str {
        match self {
            Profile::Local => "sol_raw_txs",
            Profile::Mainnet => "sol_raw_txs_mainnet",
        }
    }

    pub fn default_kafka_topic_swaps(&self) -> &'static str {
        match self {
            Profile::Local => "sol_swaps",
            Profile::Mainnet => "sol_swaps_mainnet",
        }
    }

    pub fn default_kafka_topic_dlq(&self) -> &'static str {
        match self {
            Profile::Local => "sol_raw_txs_dlq",
            Profile::Mainnet => "sol_raw_txs_dlq_mainnet",
        }
    }

    pub fn default_kafka_topic_sol_deltas(&self) -> &'static str {
        match self {
            Profile::Local => "sol_balance_deltas",
            Profile::Mainnet => "sol_balance_deltas_mainnet",
        }
    }

    pub fn default_kafka_topic_token_deltas(&self) -> &'static str {
        match self {
            Profile::Local => "sol_token_balance_deltas",
            Profile::Mainnet => "sol_token_balance_deltas_mainnet",
        }
    }

    pub fn default_kafka_topic_swaps_v2(&self) -> &'static str {
        match self {
            Profile::Local => "sol_swaps_v2",
            Profile::Mainnet => "sol_swaps_v2_mainnet",
        }
    }
}

/// Sanity guard: reject localhost/devnet/testnet URLs when PROFILE=mainnet.
pub fn validate_mainnet_url<S: AsRef<str>>(profile: Profile, url: S, context: &str) -> Result<()> {
    if profile != Profile::Mainnet {
        return Ok(());
    }

    let url = url.as_ref();
    let lower = url.to_lowercase();

    if lower.contains("localhost") || lower.contains("127.0.0.1") || lower.contains("0.0.0.0") {
        return Err(anyhow!(
            "PROFILE=mainnet but {context} URL points to localhost: {url}"
        ));
    }

    if lower.contains("devnet") || lower.contains("testnet") {
        return Err(anyhow!(
            "PROFILE=mainnet but {context} URL points to devnet/testnet: {url}"
        ));
    }

    Ok(())
}

/// Verify RPC returns mainnet genesis hash. Call once at startup when PROFILE=mainnet.
pub async fn verify_mainnet_genesis(rpc_url: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getGenesisHash",
        "params": []
    });

    let resp = client
        .post(rpc_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow!("Failed to call getGenesisHash on {rpc_url}: {e}"))?;

    if !resp.status().is_success() {
        return Err(anyhow!(
            "getGenesisHash failed with status {} on {rpc_url}",
            resp.status()
        ));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| anyhow!("Failed to parse getGenesisHash response: {e}"))?;

    let genesis_hash = json
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("getGenesisHash response missing result field"))?;

    if genesis_hash != SOLANA_MAINNET_GENESIS_HASH {
        return Err(anyhow!(
            "PROFILE=mainnet but RPC is not mainnet! Expected genesis hash {}, got {}",
            SOLANA_MAINNET_GENESIS_HASH,
            genesis_hash
        ));
    }

    Ok(())
}

pub fn url_requires_tls(url: &str) -> bool {
    url.to_lowercase().starts_with("https://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_defaults() {
        assert_eq!(
            Profile::Local.default_geyser_endpoint(),
            "http://127.0.0.1:10000"
        );
        assert_eq!(
            Profile::Mainnet.default_geyser_endpoint(),
            "https://solana-rpc.parafi.tech:10443"
        );

        assert_eq!(Profile::Local.default_rpc_url(), "http://127.0.0.1:8899");
        assert_eq!(
            Profile::Mainnet.default_rpc_url(),
            "https://solana-rpc.parafi.tech"
        );

        assert_eq!(Profile::Local.default_kafka_topic_raw_txs(), "sol_raw_txs");
        assert_eq!(
            Profile::Mainnet.default_kafka_topic_raw_txs(),
            "sol_raw_txs_mainnet"
        );
    }

    #[test]
    fn test_validate_mainnet_url_rejects_localhost() {
        assert!(validate_mainnet_url(Profile::Mainnet, "http://localhost:8899", "RPC").is_err());
        assert!(validate_mainnet_url(Profile::Mainnet, "http://127.0.0.1:8899", "RPC").is_err());
    }

    #[test]
    fn test_validate_mainnet_url_rejects_devnet() {
        assert!(
            validate_mainnet_url(Profile::Mainnet, "https://api.devnet.solana.com", "RPC").is_err()
        );
        assert!(
            validate_mainnet_url(Profile::Mainnet, "https://api.testnet.solana.com", "RPC")
                .is_err()
        );
    }

    #[test]
    fn test_validate_mainnet_url_accepts_parafi() {
        assert!(
            validate_mainnet_url(Profile::Mainnet, "https://solana-rpc.parafi.tech", "RPC").is_ok()
        );
    }

    #[test]
    fn test_validate_local_url_allows_localhost() {
        assert!(validate_mainnet_url(Profile::Local, "http://localhost:8899", "RPC").is_ok());
        assert!(validate_mainnet_url(Profile::Local, "http://127.0.0.1:8899", "RPC").is_ok());
    }

    #[test]
    fn test_url_requires_tls() {
        assert!(url_requires_tls("https://example.com"));
        assert!(url_requires_tls("HTTPS://EXAMPLE.COM"));
        assert!(!url_requires_tls("http://example.com"));
        assert!(!url_requires_tls("HTTP://EXAMPLE.COM"));
    }
}
