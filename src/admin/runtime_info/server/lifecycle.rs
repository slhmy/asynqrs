use std::time::SystemTime;

use super::{ServerInfo, WorkerInfo};

impl ServerInfo {
    pub fn started(&self) -> SystemTime {
        self.started
    }

    /// Returns mutable access to the server start time.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.Started` is an exported
    /// `time.Time` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn started_mut(&mut self) -> &mut SystemTime {
        &mut self.started
    }
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Returns mutable access to the server status string.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.Status` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn status_mut(&mut self) -> &mut String {
        &mut self.status
    }
    pub fn active_workers(&self) -> &[WorkerInfo] {
        &self.active_workers
    }

    /// Returns mutable access to active worker metadata.
    ///
    /// Reference: Asynq v0.26.0 public `ServerInfo.ActiveWorkers` is an
    /// exported worker-info slice field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
    pub fn active_workers_mut(&mut self) -> &mut Vec<WorkerInfo> {
        &mut self.active_workers
    }
}
