use thiserror::Error;

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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RequeueError {
    #[error("task not found")]
    NotFound,
    #[error("{0}")]
    Other(String),
}
