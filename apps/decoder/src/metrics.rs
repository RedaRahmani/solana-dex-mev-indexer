//! Metrics for swap detection pipeline observability.
//!
//! Provides counters for:
//! - swaps_emitted_total{venue, confidence_bucket}
//! - parse_fail_total{venue, reason}
//! - gate_fail_total{venue}
//! - v0_alt_tx_seen_total
//! - dlq_sent_total{reason}

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Global metrics instance
static METRICS: once_cell::sync::Lazy<SwapMetrics> =
    once_cell::sync::Lazy::new(SwapMetrics::new);

/// Get the global metrics instance
pub fn metrics() -> &'static SwapMetrics {
    &METRICS
}

/// Confidence buckets for histogram-like tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfidenceBucket {
    /// 0-49: Low confidence
    Low,
    /// 50-79: Medium confidence
    Medium,
    /// 80-99: High confidence
    High,
    /// 100: Perfect confidence
    Perfect,
}

impl ConfidenceBucket {
    pub fn from_confidence(confidence: u8) -> Self {
        match confidence {
            0..=49 => ConfidenceBucket::Low,
            50..=79 => ConfidenceBucket::Medium,
            80..=99 => ConfidenceBucket::High,
            100 => ConfidenceBucket::Perfect,
            _ => ConfidenceBucket::High,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ConfidenceBucket::Low => "low",
            ConfidenceBucket::Medium => "medium",
            ConfidenceBucket::High => "high",
            ConfidenceBucket::Perfect => "perfect",
        }
    }
}

/// Parse failure reasons
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParseFailReason {
    /// No token balance deltas found
    NoTokenDeltas,
    /// Could not identify in/out amounts
    InvalidAmounts,
    /// Multi-hop segmentation failed
    MultiHopFailed,
    /// Instruction data decode failed
    InstructionDecodeFailed,
    /// Unknown/other failure
    Unknown,
}

impl ParseFailReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            ParseFailReason::NoTokenDeltas => "no_token_deltas",
            ParseFailReason::InvalidAmounts => "invalid_amounts",
            ParseFailReason::MultiHopFailed => "multi_hop_failed",
            ParseFailReason::InstructionDecodeFailed => "instruction_decode_failed",
            ParseFailReason::Unknown => "unknown",
        }
    }
}

/// DLQ send reasons
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DlqReason {
    /// RPC fetch failed after retries
    RpcFetchFailed,
    /// Parse failed but tx should be retained
    ParseFailed,
    /// Schema validation failed
    ValidationFailed,
}

impl DlqReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            DlqReason::RpcFetchFailed => "rpc_fetch_failed",
            DlqReason::ParseFailed => "parse_failed",
            DlqReason::ValidationFailed => "validation_failed",
        }
    }
}

/// Swap pipeline metrics
pub struct SwapMetrics {
    /// Total swaps emitted by venue and confidence bucket
    swaps_emitted: RwLock<HashMap<(String, ConfidenceBucket), AtomicU64>>,

    /// Parse failures by venue and reason
    parse_fails: RwLock<HashMap<(String, ParseFailReason), AtomicU64>>,

    /// Gate failures by venue
    gate_fails: RwLock<HashMap<String, AtomicU64>>,

    /// v0 transactions with ALT seen
    v0_alt_tx_seen: AtomicU64,

    /// DLQ sends by reason
    dlq_sent: RwLock<HashMap<DlqReason, AtomicU64>>,

    /// Total transactions processed
    txs_processed: AtomicU64,

    /// Total swaps detected (before filtering)
    swaps_detected: AtomicU64,

    /// Total publish errors
    publish_errors: AtomicU64,
}

impl SwapMetrics {
    pub fn new() -> Self {
        Self {
            swaps_emitted: RwLock::new(HashMap::new()),
            parse_fails: RwLock::new(HashMap::new()),
            gate_fails: RwLock::new(HashMap::new()),
            v0_alt_tx_seen: AtomicU64::new(0),
            dlq_sent: RwLock::new(HashMap::new()),
            txs_processed: AtomicU64::new(0),
            swaps_detected: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
        }
    }

    /// Record a swap emission
    pub fn record_swap_emitted(&self, venue: &str, confidence: u8) {
        let bucket = ConfidenceBucket::from_confidence(confidence);
        let key = (venue.to_string(), bucket);

        {
            let map = self.swaps_emitted.read().unwrap();
            if let Some(counter) = map.get(&key) {
                counter.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        // Key doesn't exist, acquire write lock
        let mut map = self.swaps_emitted.write().unwrap();
        map.entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a parse failure
    pub fn record_parse_fail(&self, venue: &str, reason: ParseFailReason) {
        let key = (venue.to_string(), reason);

        {
            let map = self.parse_fails.read().unwrap();
            if let Some(counter) = map.get(&key) {
                counter.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        let mut map = self.parse_fails.write().unwrap();
        map.entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a gate failure (program not found in tx)
    pub fn record_gate_fail(&self, venue: &str) {
        let key = venue.to_string();

        {
            let map = self.gate_fails.read().unwrap();
            if let Some(counter) = map.get(&key) {
                counter.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        let mut map = self.gate_fails.write().unwrap();
        map.entry(key)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a v0 transaction with ALT
    pub fn record_v0_alt_tx(&self) {
        self.v0_alt_tx_seen.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a DLQ send
    pub fn record_dlq_sent(&self, reason: DlqReason) {
        {
            let map = self.dlq_sent.read().unwrap();
            if let Some(counter) = map.get(&reason) {
                counter.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        let mut map = self.dlq_sent.write().unwrap();
        map.entry(reason)
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a transaction processed
    pub fn record_tx_processed(&self) {
        self.txs_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record swap detected
    pub fn record_swap_detected(&self) {
        self.swaps_detected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a publish error
    pub fn record_publish_error(&self) {
        self.publish_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total v0+ALT transactions seen
    pub fn get_v0_alt_tx_seen(&self) -> u64 {
        self.v0_alt_tx_seen.load(Ordering::Relaxed)
    }

    /// Get total transactions processed
    pub fn get_txs_processed(&self) -> u64 {
        self.txs_processed.load(Ordering::Relaxed)
    }

    /// Get total swaps detected
    pub fn get_swaps_detected(&self) -> u64 {
        self.swaps_detected.load(Ordering::Relaxed)
    }

    /// Get total publish errors
    pub fn get_publish_errors(&self) -> u64 {
        self.publish_errors.load(Ordering::Relaxed)
    }

    /// Generate a summary string for logging
    pub fn summary(&self) -> String {
        let mut lines = Vec::new();

        lines.push(format!(
            "txs_processed={} swaps_detected={} v0_alt_seen={} publish_errors={}",
            self.get_txs_processed(),
            self.get_swaps_detected(),
            self.get_v0_alt_tx_seen(),
            self.get_publish_errors(),
        ));

        // Swaps emitted by venue/bucket
        {
            let map = self.swaps_emitted.read().unwrap();
            for ((venue, bucket), counter) in map.iter() {
                let count = counter.load(Ordering::Relaxed);
                if count > 0 {
                    lines.push(format!(
                        "swaps_emitted{{venue={},confidence={}}}={}",
                        venue,
                        bucket.as_str(),
                        count
                    ));
                }
            }
        }

        // Parse fails
        {
            let map = self.parse_fails.read().unwrap();
            for ((venue, reason), counter) in map.iter() {
                let count = counter.load(Ordering::Relaxed);
                if count > 0 {
                    lines.push(format!(
                        "parse_fail{{venue={},reason={}}}={}",
                        venue,
                        reason.as_str(),
                        count
                    ));
                }
            }
        }

        // Gate fails
        {
            let map = self.gate_fails.read().unwrap();
            for (venue, counter) in map.iter() {
                let count = counter.load(Ordering::Relaxed);
                if count > 0 {
                    lines.push(format!("gate_fail{{venue={}}}={}", venue, count));
                }
            }
        }

        // DLQ
        {
            let map = self.dlq_sent.read().unwrap();
            for (reason, counter) in map.iter() {
                let count = counter.load(Ordering::Relaxed);
                if count > 0 {
                    lines.push(format!("dlq_sent{{reason={}}}={}", reason.as_str(), count));
                }
            }
        }

        lines.join(" ")
    }
}

impl Default for SwapMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_confidence_bucket() {
        assert_eq!(
            ConfidenceBucket::from_confidence(0),
            ConfidenceBucket::Low
        );
        assert_eq!(
            ConfidenceBucket::from_confidence(49),
            ConfidenceBucket::Low
        );
        assert_eq!(
            ConfidenceBucket::from_confidence(50),
            ConfidenceBucket::Medium
        );
        assert_eq!(
            ConfidenceBucket::from_confidence(79),
            ConfidenceBucket::Medium
        );
        assert_eq!(
            ConfidenceBucket::from_confidence(80),
            ConfidenceBucket::High
        );
        assert_eq!(
            ConfidenceBucket::from_confidence(99),
            ConfidenceBucket::High
        );
        assert_eq!(
            ConfidenceBucket::from_confidence(100),
            ConfidenceBucket::Perfect
        );
    }

    #[test]
    fn test_metrics_recording() {
        let metrics = SwapMetrics::new();

        metrics.record_swap_emitted("raydium", 85);
        metrics.record_swap_emitted("raydium", 85);
        metrics.record_swap_emitted("orca", 100);
        metrics.record_parse_fail("raydium", ParseFailReason::NoTokenDeltas);
        metrics.record_gate_fail("raydium");
        metrics.record_v0_alt_tx();
        metrics.record_dlq_sent(DlqReason::RpcFetchFailed);
        metrics.record_tx_processed();

        assert_eq!(metrics.get_v0_alt_tx_seen(), 1);
        assert_eq!(metrics.get_txs_processed(), 1);

        let summary = metrics.summary();
        assert!(summary.contains("txs_processed=1"));
        assert!(summary.contains("v0_alt_seen=1"));
    }
}
