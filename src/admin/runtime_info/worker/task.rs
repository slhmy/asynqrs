use super::WorkerInfo;

impl WorkerInfo {
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns mutable access to the active worker task ID.
    ///
    /// Reference: Asynq v0.26.0 public `WorkerInfo.TaskID` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L824-L843>.
    pub fn task_id_mut(&mut self) -> &mut String {
        &mut self.task_id
    }
    pub fn task_type(&self) -> &str {
        &self.task_type
    }

    /// Returns mutable access to the active worker task type.
    ///
    /// Reference: Asynq v0.26.0 public `WorkerInfo.TaskType` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L824-L843>.
    pub fn task_type_mut(&mut self) -> &mut String {
        &mut self.task_type
    }
    /// Rust compatibility alias for mutable upstream `WorkerInfo.TaskType`.
    ///
    /// Rust compatibility alias for upstream `WorkerInfo.TaskType`.
    ///
    pub fn task_payload(&self) -> &[u8] {
        &self.task_payload
    }

    /// Returns mutable access to the active worker task payload.
    ///
    /// Reference: Asynq v0.26.0 public `WorkerInfo.TaskPayload` is an exported
    /// byte-slice field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L824-L843>.
    pub fn task_payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.task_payload
    }
    /// Rust compatibility alias for mutable upstream `WorkerInfo.TaskPayload`.
    ///
    /// Rust compatibility alias for upstream `WorkerInfo.TaskPayload`.
    ///
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Returns mutable access to the active worker queue name.
    ///
    /// Reference: Asynq v0.26.0 public `WorkerInfo.Queue` is an exported
    /// string field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L824-L843>.
    pub fn queue_mut(&mut self) -> &mut String {
        &mut self.queue
    }
}
