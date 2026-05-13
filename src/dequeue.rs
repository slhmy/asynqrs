use std::time::SystemTime;

use crate::TaskMessage;

/// Result of moving a task from pending to active for worker processing.
///
/// Reference: Asynq v0.26.0 `RDB.Dequeue` returns a task message and its lease
/// expiration time:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L243-L274>.
#[derive(Debug, Clone, PartialEq)]
pub struct DequeuedTask {
    message: TaskMessage,
    lease_expires_at: SystemTime,
}

/// Minimal broker interface for the worker dequeue path.
///
/// Reference: Asynq v0.26.0 internal `base.Broker` worker-related methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L371-L419>.
pub trait DequeueBroker {
    fn dequeue(&mut self, queues: &[String]) -> Result<DequeuedTask, DequeueError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DequeueError {
    NoProcessableTask,
    Other(String),
}

impl DequeuedTask {
    pub fn new(message: TaskMessage, lease_expires_at: SystemTime) -> Self {
        Self {
            message,
            lease_expires_at,
        }
    }

    pub fn message(&self) -> &TaskMessage {
        &self.message
    }

    pub fn into_message(self) -> TaskMessage {
        self.message
    }

    pub fn lease_expires_at(&self) -> SystemTime {
        self.lease_expires_at
    }
}

impl std::fmt::Display for DequeueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoProcessableTask => f.write_str("no processable task found"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for DequeueError {}
