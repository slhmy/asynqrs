use thiserror::Error;

use crate::EnqueuePlanError;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClientError {
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` rejects a nil task
    /// before composing enqueue options:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L355-L358>.
    #[error("task cannot be nil")]
    NilTask,
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` returns task and
    /// option validation errors directly instead of wrapping their text:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L299-L312>.
    #[error("{0}")]
    Plan(#[from] EnqueuePlanError),
    /// Reference: Asynq v0.26.0 `Client.Ping` returns the underlying broker
    /// ping error directly:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L333-L336>.
    #[error("{0}")]
    Ping(BrokerError),
    /// Reference: Asynq v0.26.0 `Client.Close` returns the underlying broker
    /// close error directly when the client owns the connection:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L319-L324>.
    #[error("{0}")]
    Close(BrokerError),
    /// Reference: Asynq v0.26.0 `Client.Close` refuses to close clients
    /// created from an existing Redis connection:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L319-L324>.
    #[error("redis connection is shared so the Client can't be closed through asynq")]
    SharedConnection,
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` passes the provided
    /// `context.Context` to the enqueue broker operation:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L266-L331>.
    #[error("context canceled")]
    Cancelled,
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` returns broker
    /// enqueue errors directly after duplicate-task sentinel mapping:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L316-L331>.
    #[error("{0}")]
    Broker(#[from] BrokerError),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BrokerError {
    #[error("task already exists")]
    DuplicateTask,
    #[error("task ID conflicts with another task")]
    TaskIdConflict,
    #[error("{operation} failed: {message}")]
    Operation {
        operation: &'static str,
        message: String,
    },
    #[error("{0}")]
    Other(String),
}

impl BrokerError {
    pub fn operation(operation: &'static str, message: impl Into<String>) -> Self {
        Self::Operation {
            operation,
            message: message.into(),
        }
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::Other(message.into())
    }

    pub fn operation_name(&self) -> Option<&'static str> {
        match self {
            Self::Operation { operation, .. } => Some(operation),
            _ => None,
        }
    }

    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Operation { message, .. } | Self::Other(message) => Some(message),
            _ => None,
        }
    }

    /// Reports whether this broker error is the duplicate-task sentinel.
    ///
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` maps Redis duplicate
    /// task errors to the public `BrokerError::DuplicateTask` sentinel:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L413-L416>.
    pub fn is_duplicate_task(&self) -> bool {
        matches!(self, Self::DuplicateTask)
    }

    /// Reports whether this broker error is the task-id-conflict sentinel.
    ///
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` maps Redis task-id
    /// conflicts to the public `BrokerError::TaskIdConflict` sentinel:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L413-L416>.
    pub fn is_task_id_conflict(&self) -> bool {
        matches!(self, Self::TaskIdConflict)
    }
}

impl ClientError {
    /// Reports whether this client error matches the duplicate-task sentinel.
    ///
    /// Reference: Asynq v0.26.0 callers can detect duplicate enqueue errors
    /// with `errors.Is(err, BrokerError::DuplicateTask)`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L413-L416>.
    pub fn is_duplicate_task(&self) -> bool {
        matches!(self, Self::Broker(error) if error.is_duplicate_task())
    }

    /// Reports whether this client error matches the task-id-conflict sentinel.
    ///
    /// Reference: Asynq v0.26.0 callers can detect task id conflicts with
    /// `errors.Is(err, BrokerError::TaskIdConflict)`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L413-L416>.
    pub fn is_task_id_conflict(&self) -> bool {
        matches!(self, Self::Broker(error) if error.is_task_id_conflict())
    }
}
