use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metrics {
    pub tx_seen: AtomicU64,
    pub send_ok: AtomicU64,
    pub send_err: AtomicU64,
    pub reconnects: AtomicU64,
    pub connected: AtomicU64, // increments each time we successfully subscribe
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            tx_seen: AtomicU64::new(0),
            send_ok: AtomicU64::new(0),
            send_err: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            connected: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.tx_seen.load(Ordering::Relaxed),
            self.send_ok.load(Ordering::Relaxed),
            self.send_err.load(Ordering::Relaxed),
            self.reconnects.load(Ordering::Relaxed),
            self.connected.load(Ordering::Relaxed),
        )
    }
}
