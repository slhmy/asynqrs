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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompleteError {
    NotFound,
    Other(String),
}

impl std::fmt::Display for CompleteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("task not found"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for CompleteError {}
