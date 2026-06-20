use super::QueueStats;

impl QueueStats {
    pub fn processed(&self) -> usize {
        self.processed
    }

    /// Returns mutable access to the recent processed count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Processed` is an exported
    /// int field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L124-L125>.
    pub fn processed_mut(&mut self) -> &mut usize {
        &mut self.processed
    }
    pub fn failed(&self) -> usize {
        self.failed
    }

    /// Returns mutable access to the recent failed count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.Failed` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L126-L127>.
    pub fn failed_mut(&mut self) -> &mut usize {
        &mut self.failed
    }
    pub fn processed_total(&self) -> usize {
        self.processed_total
    }

    /// Returns mutable access to the all-time processed count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.ProcessedTotal` is an
    /// exported int field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L89-L135>.
    pub fn processed_total_mut(&mut self) -> &mut usize {
        &mut self.processed_total
    }
    pub fn failed_total(&self) -> usize {
        self.failed_total
    }

    /// Returns mutable access to the all-time failed count.
    ///
    /// Reference: Asynq v0.26.0 public `QueueInfo.FailedTotal` is an exported
    /// int field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L89-L135>.
    pub fn failed_total_mut(&mut self) -> &mut usize {
        &mut self.failed_total
    }
}
