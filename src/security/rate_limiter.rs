use std::sync::atomic::{AtomicU64, Ordering};

use crate::common::unix_timestamp_secs;

/// Lightweight token bucket rate limiter backed by atomics.
pub struct RateLimiter {
    /// Maximum requests allowed inside the current one-second window.
    max_rps: u64,
    /// Number of requests observed inside the current window.
    counter: AtomicU64,
    /// Unix timestamp (seconds) at which the current window started.
    window_start: AtomicU64,
}

impl RateLimiter {
    pub fn new(max_rps: u64) -> Self {
        let now = unix_timestamp_secs();
        Self {
            max_rps,
            counter: AtomicU64::new(0),
            window_start: AtomicU64::new(now),
        }
    }

    pub fn allow(&self) -> bool {
        let now = unix_timestamp_secs();
        let ws = self.window_start.load(Ordering::Relaxed);

        if now > ws {
            self.window_start.store(now, Ordering::Relaxed);
            self.counter.store(1, Ordering::Relaxed);
            return true;
        }

        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        count < self.max_rps
    }
}

lazy_static::lazy_static! {
    /// Global rate limiter: 50 requests/second.
    pub static ref GLOBAL_LIMITER: RateLimiter = RateLimiter::new(50);
}
