use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AdminError {
    #[error("queue not found")]
    QueueNotFound,
    /// Reference: Asynq v0.26.0 `Inspector.DeleteQueue` wraps
    /// `AdminError::QueueNotFound` with the queue name while preserving sentinel
    /// detection:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L220-L244>.
    #[error("queue not found: queue={queue:?}")]
    QueueNotFoundForQueue { queue: String },
    /// Reference: Asynq v0.26.0 Inspector task-read methods wrap
    /// `AdminError::QueueNotFound` with an `asynq:` prefix while preserving sentinel
    /// detection through `errors.Is`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L246-L529>.
    #[error("asynq: queue not found")]
    AsynqQueueNotFound,
    #[error("number of days must be positive")]
    NonPositiveDays,
    #[error("queue name must contain one or more characters")]
    InvalidQueueName,
    #[error("asynq: queue name must contain one or more characters")]
    AsynqInvalidQueueName,
    /// Reference: Asynq v0.26.0 `Inspector.ArchiveTask` returns this fixed
    /// wrapped validation text for invalid queue names:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L731-L758>.
    #[error("asynq: err")]
    AsynqArchiveQueueValidation,
    #[error("queue is already paused")]
    QueueAlreadyPaused,
    #[error("queue is not paused")]
    QueueNotPaused,
    #[error("queue is not empty")]
    QueueNotEmpty,
    /// Reference: Asynq v0.26.0 `Inspector.DeleteQueue` wraps
    /// `AdminError::QueueNotEmpty` with the queue name while preserving sentinel
    /// detection:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L220-L244>.
    #[error("queue is not empty: queue={queue:?}")]
    QueueNotEmptyForQueue { queue: String },
    #[error("queue has active tasks")]
    QueueHasActiveTasks,
    /// Reference: Asynq v0.26.0 non-forced queue deletion reports this fixed
    /// text when active tasks prevent removal:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L220-L244>.
    #[error("cannot remove queue with active tasks")]
    QueueHasActiveTasksForRemoval,
    #[error("task not found")]
    TaskNotFound,
    /// Reference: Asynq v0.26.0 `Inspector.GetTaskInfo` wraps
    /// `AdminError::TaskNotFound` with an `asynq:` prefix while preserving sentinel
    /// detection through `errors.Is`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L246-L263>.
    #[error("asynq: task not found")]
    AsynqTaskNotFound,
    #[error("task is active")]
    TaskIsActive,
    #[error("task is pending")]
    TaskIsPending,
    /// Reference: Asynq v0.26.0 `RDB.RunTask` returns this fixed failed
    /// precondition text when the target task is already active:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1051-L1059>.
    #[error("task is already running")]
    TaskAlreadyRunning,
    /// Reference: Asynq v0.26.0 `RDB.RunTask` returns this fixed failed
    /// precondition text when the target task is already pending:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1051-L1059>.
    #[error("task is already in pending state")]
    TaskAlreadyPending,
    /// Reference: Asynq v0.26.0 `RDB.DeleteTask` returns this fixed failed
    /// precondition text for active tasks:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1574-L1582>.
    #[error("cannot delete task in active state. use CancelProcessing instead.")]
    CannotDeleteActiveTask,
    /// Reference: Asynq v0.26.0 `RDB.ArchiveTask` returns this fixed failed
    /// precondition text for active tasks:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1344-L1356>.
    #[error("cannot archive task in active state. use CancelProcessing instead.")]
    CannotArchiveActiveTask,
    #[error("task is already archived")]
    TaskAlreadyArchived,
    /// Reference: Asynq v0.26.0 `RDB.UpdateTaskPayload` returns this text
    /// when the target task is not in scheduled state:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1480-L1482>.
    #[error("cannot update task that is not in scheduled state.")]
    TaskNotScheduled,
    #[error("page size must be positive")]
    NonPositivePageSize,
    #[error("{0}")]
    Other(String),
}

impl AdminError {
    /// Reports whether this admin error matches the queue-not-found sentinel.
    ///
    /// Reference: Asynq v0.26.0 inspector methods wrap
    /// `AdminError::QueueNotFound` so callers can detect it with `errors.Is`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L201-L236>.
    pub fn is_queue_not_found(&self) -> bool {
        matches!(
            self,
            Self::QueueNotFound | Self::QueueNotFoundForQueue { .. } | Self::AsynqQueueNotFound
        )
    }

    /// Reports whether this admin error matches the queue-not-empty sentinel.
    ///
    /// Reference: Asynq v0.26.0 `Inspector.DeleteQueue` wraps
    /// `AdminError::QueueNotEmpty` for non-forced deletion of non-empty queues:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L226-L236>.
    pub fn is_queue_not_empty(&self) -> bool {
        matches!(
            self,
            Self::QueueNotEmpty | Self::QueueNotEmptyForQueue { .. }
        )
    }

    /// Reports whether this admin error matches the task-not-found sentinel.
    ///
    /// Reference: Asynq v0.26.0 inspector task methods wrap
    /// `AdminError::TaskNotFound` so callers can detect it with `errors.Is`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L238-L255>.
    pub fn is_task_not_found(&self) -> bool {
        matches!(self, Self::TaskNotFound | Self::AsynqTaskNotFound)
    }

    pub fn is_invalid_queue_name(&self) -> bool {
        matches!(self, Self::InvalidQueueName | Self::AsynqInvalidQueueName)
    }

    pub fn is_queue_has_active_tasks(&self) -> bool {
        matches!(
            self,
            Self::QueueHasActiveTasks | Self::QueueHasActiveTasksForRemoval
        )
    }

    pub(crate) fn with_asynq_prefix(self) -> Self {
        match self {
            Self::QueueNotFound | Self::AsynqQueueNotFound => Self::AsynqQueueNotFound,
            Self::QueueNotFoundForQueue { .. } => Self::AsynqQueueNotFound,
            Self::TaskNotFound | Self::AsynqTaskNotFound => Self::AsynqTaskNotFound,
            Self::InvalidQueueName | Self::AsynqInvalidQueueName => Self::AsynqInvalidQueueName,
            Self::Other(message) if message.starts_with("asynq: ") => Self::Other(message),
            error => Self::Other(format!("asynq: {error}")),
        }
    }

    pub(crate) fn for_delete_queue(self, queue: &str) -> Self {
        match self {
            Self::QueueNotFound | Self::AsynqQueueNotFound | Self::QueueNotFoundForQueue { .. } => {
                Self::QueueNotFoundForQueue {
                    queue: queue.to_owned(),
                }
            }
            Self::QueueNotEmpty | Self::QueueNotEmptyForQueue { .. } => {
                Self::QueueNotEmptyForQueue {
                    queue: queue.to_owned(),
                }
            }
            Self::QueueHasActiveTasks | Self::QueueHasActiveTasksForRemoval => {
                Self::QueueHasActiveTasksForRemoval
            }
            error => error,
        }
    }
}
