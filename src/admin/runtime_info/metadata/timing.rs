use std::time::SystemTime;

use super::WorkerMetadataInfo;

impl WorkerMetadataInfo {
    pub fn started(&self) -> SystemTime {
        self.started
    }

    /// Returns mutable access to the worker metadata start time.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.Started` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn started_mut(&mut self) -> &mut SystemTime {
        &mut self.started
    }
    pub fn deadline(&self) -> SystemTime {
        self.deadline
    }

    /// Returns mutable access to the worker metadata deadline.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.Deadline` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn deadline_mut(&mut self) -> &mut SystemTime {
        &mut self.deadline
    }
}
