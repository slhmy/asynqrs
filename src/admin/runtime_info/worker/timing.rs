use std::time::SystemTime;

use super::WorkerInfo;

impl WorkerInfo {
    pub fn started(&self) -> SystemTime {
        self.started
    }

    /// Returns mutable access to the active worker start time.
    ///
    /// Reference: Asynq v0.26.0 public `WorkerInfo.Started` is an exported
    /// `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L824-L843>.
    pub fn started_mut(&mut self) -> &mut SystemTime {
        &mut self.started
    }
    pub fn deadline(&self) -> SystemTime {
        self.deadline
    }

    /// Returns mutable access to the active worker deadline.
    ///
    /// Reference: Asynq v0.26.0 public `WorkerInfo.Deadline` is an exported
    /// `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L824-L843>.
    pub fn deadline_mut(&mut self) -> &mut SystemTime {
        &mut self.deadline
    }
}
