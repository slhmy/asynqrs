use std::time::SystemTime;

use crate::TaskMessage;

/// Minimal broker interface for retrying a failed active task.
///
/// Reference: Asynq v0.26.0 `RDB.Retry`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L380-L418>.
///
/// TODO: Add archive and lease-expiration recovery when the worker failure
/// lifecycle grows beyond explicit retry.
pub trait RetryBroker {
    fn retry(
        &mut self,
        message: &TaskMessage,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), RetryError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryError {
    NotFound,
    Other(String),
}

impl std::fmt::Display for RetryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("task not found"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RetryError {}
