use super::WorkerMetadataInfo;

impl WorkerMetadataInfo {
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Returns mutable access to the worker metadata task ID.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.TaskID` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn task_id_mut(&mut self) -> &mut String {
        &mut self.task_id
    }
    pub fn task_type(&self) -> &str {
        &self.task_type
    }

    /// Returns mutable access to the worker metadata task type.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.TaskType` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn task_type_mut(&mut self) -> &mut String {
        &mut self.task_type
    }
    pub fn task_payload(&self) -> &[u8] {
        &self.task_payload
    }

    /// Returns mutable access to the worker metadata task payload.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.TaskPayload` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn task_payload_mut(&mut self) -> &mut Vec<u8> {
        &mut self.task_payload
    }
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Returns mutable access to the worker metadata queue name.
    ///
    /// Reference: Asynq v0.26.0 internal `base.WorkerInfo.Queue` field:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
    pub fn queue_mut(&mut self) -> &mut String {
        &mut self.queue
    }
}
