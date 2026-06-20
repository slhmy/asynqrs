use std::time::SystemTime;

mod task;
mod timing;

/// Reference: Asynq v0.26.0 public `WorkerInfo` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L824-L843>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    task_id: String,
    task_type: String,
    task_payload: Vec<u8>,
    queue: String,
    started: SystemTime,
    deadline: SystemTime,
}

impl WorkerInfo {
    pub fn new(
        task_id: String,
        task_type: String,
        task_payload: Vec<u8>,
        queue: String,
        started: SystemTime,
        deadline: SystemTime,
    ) -> Self {
        Self {
            task_id,
            task_type,
            task_payload,
            queue,
            started,
            deadline,
        }
    }
}
