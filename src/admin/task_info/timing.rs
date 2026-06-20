use std::time::{Duration, SystemTime};

use crate::admin::from_unix_time_or_zero;

use super::TaskInfo;

impl TaskInfo {
    pub fn last_failed_at(&self) -> Option<SystemTime> {
        from_unix_time_or_zero(self.message.last_failed_at)
    }

    /// Returns mutable access to the raw last-failed-at Unix seconds.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.LastFailedAt` is an exported
    /// `time.Time` field derived from task-message Unix seconds:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L111-L180>.
    pub fn last_failed_at_unix_seconds_mut(&mut self) -> &mut i64 {
        &mut self.message.last_failed_at
    }
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.message.timeout.max(0) as u64)
    }
    /// Returns the raw timeout seconds stored on the task message.
    ///
    /// Reference: Asynq v0.26.0 `newTaskInfo` derives `Timeout` directly from
    /// `msg.Timeout`, preserving signed task-message seconds before converting
    /// to `time.Duration`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L166-L180>.
    pub fn timeout_seconds(&self) -> i64 {
        self.message.timeout
    }

    /// Returns mutable access to the raw timeout seconds.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Timeout` is an exported
    /// `time.Duration` field derived from task-message seconds:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L104-L180>.
    pub fn timeout_seconds_mut(&mut self) -> &mut i64 {
        &mut self.message.timeout
    }
    pub fn deadline(&self) -> Option<SystemTime> {
        from_unix_time_or_zero(self.message.deadline)
    }

    /// Returns mutable access to the raw deadline Unix seconds.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Deadline` is an exported
    /// `time.Time` field derived from task-message Unix seconds:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L106-L180>.
    pub fn deadline_unix_seconds_mut(&mut self) -> &mut i64 {
        &mut self.message.deadline
    }
    pub fn retention(&self) -> Duration {
        Duration::from_secs(self.message.retention.max(0) as u64)
    }
    /// Returns the raw retention seconds stored on the task message.
    ///
    /// Reference: Asynq v0.26.0 `newTaskInfo` derives `Retention` directly from
    /// `msg.Retention`, preserving signed task-message seconds before
    /// converting to `time.Duration`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L166-L180>.
    pub fn retention_seconds(&self) -> i64 {
        self.message.retention
    }

    /// Returns mutable access to the raw retention seconds.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Retention` is an exported
    /// `time.Duration` field derived from task-message seconds:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L113-L180>.
    pub fn retention_seconds_mut(&mut self) -> &mut i64 {
        &mut self.message.retention
    }
    pub fn completed_at(&self) -> Option<SystemTime> {
        from_unix_time_or_zero(self.message.completed_at)
    }

    /// Returns mutable access to the raw completed-at Unix seconds.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.CompletedAt` is an exported
    /// `time.Time` field derived from task-message Unix seconds:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L115-L180>.
    pub fn completed_at_unix_seconds_mut(&mut self) -> &mut i64 {
        &mut self.message.completed_at
    }
    pub fn next_process_at(&self) -> Option<SystemTime> {
        self.next_process_at
    }

    /// Returns mutable access to the task-info next process time.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.NextProcessAt` is an
    /// exported `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L86-L87>.
    pub fn next_process_at_mut(&mut self) -> &mut Option<SystemTime> {
        &mut self.next_process_at
    }
}
