use std::collections::HashMap;
use std::time::SystemTime;

use super::WorkerInfo;

mod capacity;
mod identity;
mod lifecycle;

/// Running server metadata returned by Inspector/Admin.
///
/// Reference: Asynq v0.26.0 public `ServerInfo` inspector model:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L794-L822>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    id: String,
    host: String,
    pid: i32,
    concurrency: i32,
    queues: HashMap<String, i32>,
    strict_priority: bool,
    started: SystemTime,
    status: String,
    active_workers: Vec<WorkerInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfoParts {
    pub id: String,
    pub host: String,
    pub pid: i32,
    pub concurrency: i32,
    pub queues: HashMap<String, i32>,
    pub strict_priority: bool,
    pub started: SystemTime,
    pub status: String,
    pub active_workers: Vec<WorkerInfo>,
}

impl ServerInfo {
    pub fn new(parts: ServerInfoParts) -> Self {
        Self {
            id: parts.id,
            host: parts.host,
            pid: parts.pid,
            concurrency: parts.concurrency,
            queues: parts.queues,
            strict_priority: parts.strict_priority,
            started: parts.started,
            status: parts.status,
            active_workers: parts.active_workers,
        }
    }
}
