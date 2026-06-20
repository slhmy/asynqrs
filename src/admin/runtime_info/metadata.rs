use std::time::SystemTime;

use super::WorkerInfo;

mod identity;
mod task;
mod timing;

/// Reference: Asynq v0.26.0 internal `base.WorkerInfo`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L438>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerMetadataInfo {
    host: String,
    pid: i32,
    server_id: String,
    task_id: String,
    task_type: String,
    task_payload: Vec<u8>,
    queue: String,
    started: SystemTime,
    deadline: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerMetadataInfoParts {
    pub host: String,
    pub pid: i32,
    pub server_id: String,
    pub task_id: String,
    pub task_type: String,
    pub task_payload: Vec<u8>,
    pub queue: String,
    pub started: SystemTime,
    pub deadline: SystemTime,
}

impl WorkerMetadataInfo {
    pub fn new(parts: WorkerMetadataInfoParts) -> Self {
        Self {
            host: parts.host,
            pid: parts.pid,
            server_id: parts.server_id,
            task_id: parts.task_id,
            task_type: parts.task_type,
            task_payload: parts.task_payload,
            queue: parts.queue,
            started: parts.started,
            deadline: parts.deadline,
        }
    }

    pub(crate) fn into_admin_worker_info(self) -> WorkerInfo {
        WorkerInfo::new(
            self.task_id,
            self.task_type,
            self.task_payload,
            self.queue,
            self.started,
            self.deadline,
        )
    }
}
