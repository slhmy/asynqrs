use super::WorkerMetadataInfo;

impl WorkerMetadataInfo {
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns mutable access to the worker metadata host.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.Host` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn host_mut(&mut self) -> &mut String {
        &mut self.host
    }
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Returns mutable access to the worker metadata process ID.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.PID` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn pid_mut(&mut self) -> &mut i32 {
        &mut self.pid
    }
    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    /// Returns mutable access to the worker metadata server ID.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.ServerID` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn server_id_mut(&mut self) -> &mut String {
        &mut self.server_id
    }
}
