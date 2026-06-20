use std::time::SystemTime;

use super::QueueStats;

impl QueueStats {
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Returns mutable access to the queue name.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Queue` is an exported string
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L89-L135>.
    pub fn queue_mut(&mut self) -> &mut String {
        &mut self.queue
    }
    pub fn memory_usage(&self) -> i64 {
        self.memory_usage
    }

    /// Returns mutable access to the queue memory usage.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.MemoryUsage` is an exported
    /// int64 field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L94-L96>.
    pub fn memory_usage_mut(&mut self) -> &mut i64 {
        &mut self.memory_usage
    }
    pub fn paused(&self) -> bool {
        self.paused
    }

    /// Returns mutable access to the queue paused flag.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Paused` is an exported bool
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L130-L132>.
    pub fn paused_mut(&mut self) -> &mut bool {
        &mut self.paused
    }
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Returns mutable access to the queue-info timestamp.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Timestamp` is an exported
    /// `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L134-L135>.
    pub fn timestamp_mut(&mut self) -> &mut SystemTime {
        &mut self.timestamp
    }
}
