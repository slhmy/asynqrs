use std::time::SystemTime;

use thiserror::Error;

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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RetryError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}
