use thiserror::Error;

use crate::TaskState;

/// Errors while building Redis Inspector/admin command plans.
///
/// Reference: Asynq v0.26.0 Redis Inspector command methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisAdminPlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("group name must contain one or more characters")]
    EmptyGroupName,
    #[error("task id must contain one or more characters")]
    EmptyTaskId,
    #[error("number of days must be positive")]
    NonPositiveDays,
    #[error("page size must be positive")]
    NonPositivePageSize,
    #[error("cannot list tasks in unsupported {0} state")]
    UnsupportedTaskListState(TaskState),
    #[error("cannot run all tasks in unsupported {0} state")]
    UnsupportedTaskRunAllState(TaskState),
    #[error("cannot archive all tasks in unsupported {0} state")]
    UnsupportedTaskArchiveAllState(TaskState),
    #[error("cannot delete all tasks in unsupported {0} state")]
    UnsupportedTaskDeleteAllState(TaskState),
    #[error("pagination range overflow")]
    PaginationOverflow,
    #[error("time overflow while computing historical day")]
    HistoricalDayOverflow,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}
