use super::super::QueueStats;

impl QueueStats {
    pub fn pending(&self) -> usize {
        self.pending
    }

    /// Returns mutable access to the pending task count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Pending` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L108-L109>.
    pub fn pending_mut(&mut self) -> &mut usize {
        &mut self.pending
    }
    pub fn active(&self) -> usize {
        self.active
    }

    /// Returns mutable access to the active task count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Active` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L110-L111>.
    pub fn active_mut(&mut self) -> &mut usize {
        &mut self.active
    }
    pub fn scheduled(&self) -> usize {
        self.scheduled
    }

    /// Returns mutable access to the scheduled task count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Scheduled` is an exported
    /// int field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L112-L113>.
    pub fn scheduled_mut(&mut self) -> &mut usize {
        &mut self.scheduled
    }
    pub fn retry(&self) -> usize {
        self.retry
    }

    /// Returns mutable access to the retry task count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Retry` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L114-L115>.
    pub fn retry_mut(&mut self) -> &mut usize {
        &mut self.retry
    }
    pub fn archived(&self) -> usize {
        self.archived
    }

    /// Returns mutable access to the archived task count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Archived` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L116-L117>.
    pub fn archived_mut(&mut self) -> &mut usize {
        &mut self.archived
    }
    pub fn completed(&self) -> usize {
        self.completed
    }

    /// Returns mutable access to the completed task count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Completed` is an exported
    /// int field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L118-L119>.
    pub fn completed_mut(&mut self) -> &mut usize {
        &mut self.completed
    }
    pub fn aggregating(&self) -> usize {
        self.aggregating
    }

    /// Returns mutable access to the aggregating task count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Aggregating` is an exported
    /// int field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L120-L121>.
    pub fn aggregating_mut(&mut self) -> &mut usize {
        &mut self.aggregating
    }
}
