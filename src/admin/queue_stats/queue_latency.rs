use std::time::Duration;

use super::QueueStats;

impl QueueStats {
    pub fn latency(&self) -> Duration {
        self.latency
    }

    /// Sets queue latency and keeps the raw signed latency representation in
    /// sync.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Latency` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L98-L99>.
    pub fn set_latency(&mut self, latency: Duration) {
        self.latency = latency;
        self.latency_nanos = duration_nanos(latency);
    }
    /// Returns the raw signed latency in nanoseconds.
    ///
    /// Reference: Asynq v0.26.0 `RDB.CurrentStats` stores latency as Go
    /// `time.Duration`, so a future `oldest_pending_since` value can produce a
    /// negative duration:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L217-L222>.
    pub fn latency_nanos(&self) -> i128 {
        self.latency_nanos
    }
    /// Sets the raw signed latency in nanoseconds.
    ///
    /// Negative values mirror Go `time.Duration` but are exposed as
    /// `Duration::ZERO` through `latency` because Rust `Duration` is unsigned.
    ///
    /// Reference: Asynq v0.26.0 `RDB.CurrentStats` stores latency as Go
    /// `time.Duration`, so a future `oldest_pending_since` value can produce a
    /// negative duration:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L217-L222>.
    pub fn set_latency_nanos(&mut self, latency_nanos: i128) {
        self.latency_nanos = latency_nanos;
        self.latency = if latency_nanos > 0 {
            Duration::from_nanos(latency_nanos.min(u64::MAX as i128) as u64)
        } else {
            Duration::ZERO
        };
    }
}

pub(super) fn duration_nanos(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}
