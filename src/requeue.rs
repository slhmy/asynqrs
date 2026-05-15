use crate::TaskMessage;

/// Minimal broker interface for moving an active task back to pending.
///
/// Reference: Asynq v0.26.0 `RDB.Requeue`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L486-L506>.
///
/// TODO: Wire this into worker shutdown handling once `Processor` has a
/// long-running server loop and cancellation model.
pub trait RequeueBroker {
    fn requeue(&mut self, message: &TaskMessage) -> Result<(), RequeueError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequeueError {
    NotFound,
    Other(String),
}

impl std::fmt::Display for RequeueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => f.write_str("task not found"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for RequeueError {}
