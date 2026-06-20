use super::super::TaskInfo;

impl TaskInfo {
    pub fn max_retry(&self) -> i64 {
        self.max_retry
    }

    /// Returns mutable access to the task-info max-retry count.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.MaxRetry` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L100-L103>.
    pub fn max_retry_mut(&mut self) -> &mut i64 {
        &mut self.max_retry
    }
    pub fn retried(&self) -> i32 {
        self.message.retried
    }

    /// Returns mutable access to the task-info retry count.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.Retried` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L100-L103>.
    pub fn retried_mut(&mut self) -> &mut i32 {
        &mut self.message.retried
    }
    pub fn is_orphaned(&self) -> bool {
        self.is_orphaned
    }

    /// Returns mutable access to the task-info orphaned marker.
    ///
    /// Reference: Asynq v0.26.0 public `TaskInfo.IsOrphaned` is an exported
    /// bool field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/asynq.go#L117-L118>.
    pub fn is_orphaned_mut(&mut self) -> &mut bool {
        &mut self.is_orphaned
    }
    pub(crate) fn mark_orphaned(&mut self) {
        self.is_orphaned = true;
    }
}
