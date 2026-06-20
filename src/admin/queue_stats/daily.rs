use std::time::SystemTime;

///
/// Reference: Asynq v0.26.0 `DailyStats` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyStats {
    queue: String,
    processed: usize,
    failed: usize,
    time: SystemTime,
}

impl DailyStats {
    pub fn new(queue: String, processed: usize, failed: usize, time: SystemTime) -> Self {
        Self {
            queue,
            processed,
            failed,
            time,
        }
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Returns mutable access to the daily-stats queue name.
    ///
    /// Reference: Asynq v0.26.0 public `DailyStats.Queue` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L170-L180>.
    pub fn queue_mut(&mut self) -> &mut String {
        &mut self.queue
    }
    pub fn processed(&self) -> usize {
        self.processed
    }

    /// Returns mutable access to the processed count.
    ///
    /// Reference: Asynq v0.26.0 public `DailyStats.Processed` is an exported
    /// int field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L170-L180>.
    pub fn processed_mut(&mut self) -> &mut usize {
        &mut self.processed
    }
    pub fn failed(&self) -> usize {
        self.failed
    }

    /// Returns mutable access to the failed count.
    ///
    /// Reference: Asynq v0.26.0 public `DailyStats.Failed` is an exported int
    /// field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L170-L180>.
    pub fn failed_mut(&mut self) -> &mut usize {
        &mut self.failed
    }
    /// Returns the date this daily stats sample was taken.
    ///
    /// Reference: Asynq v0.26.0 public `DailyStats.Date` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L170-L180>.
    pub fn date(&self) -> SystemTime {
        self.time
    }

    /// Returns mutable access to the daily-stats date.
    ///
    /// Reference: Asynq v0.26.0 public `DailyStats.Date` is an exported
    /// `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L170-L180>.
    pub fn date_mut(&mut self) -> &mut SystemTime {
        &mut self.time
    }
    pub fn time(&self) -> SystemTime {
        self.date()
    }

    pub fn time_mut(&mut self) -> &mut SystemTime {
        self.date_mut()
    }
}
