use thiserror::Error;

use crate::TaskMessage;

/// Minimal broker interface for marking a dequeued task as successfully done.
///
/// Reference: Asynq v0.26.0 `RDB.Done` and `RDB.MarkAsComplete`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L325-L379>.
///
/// TODO: Add retry, archive, lease extension, expired lease recovery, and
/// completed task cleanup when the worker lifecycle grows beyond successful
/// completion.
pub trait CompleteBroker {
    fn complete(&mut self, message: &TaskMessage) -> Result<(), CompleteError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CompleteError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}
