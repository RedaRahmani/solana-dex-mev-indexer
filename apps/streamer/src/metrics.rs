use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub struct Metrics {
    pub tx_seen: AtomicU64,
    pub send_ok: AtomicU64,
    pub send_err: AtomicU64,
    pub reconnects: AtomicU64,
    last_log: std::sync::Mutex<Instant>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            tx_seen: AtomicU64::new(0),
            send_ok: AtomicU64::new(0),
            send_err: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            last_log: std::sync::Mutex::new(Instant::now()),
        }
    }

    pub fn bump_log_tick(&self, every: Duration) -> bool {
        let mut guard = self.last_log.lock().unwrap();
        if guard.elapsed() >= every {
            *guard = Instant::now();
            true
        } else {
            false
        }
    }

    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.tx_seen.load(Ordering::Relaxed),
            self.send_ok.load(Ordering::Relaxed),
            self.send_err.load(Ordering::Relaxed),
            self.reconnects.load(Ordering::Relaxed),
        )
    }
}
