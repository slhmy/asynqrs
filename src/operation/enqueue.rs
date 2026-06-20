mod options;
mod plan;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;

pub use options::EnqueueOptions;
pub use plan::EnqueuePlan;

/// Default max retry count used if nothing is specified.
///
/// Reference: Asynq v0.26.0 `DEFAULT_MAX_RETRY`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L229-L233>.
pub const DEFAULT_MAX_RETRY: i32 = 25;
/// Default timeout used if both timeout and deadline are not specified.
///
/// Reference: Asynq v0.26.0 `DEFAULT_TIMEOUT`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L229-L233>.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// No-timeout sentinel.
///
/// Reference: Asynq v0.26.0 `NO_TIMEOUT`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L235-L239>.
pub const NO_TIMEOUT: Duration = Duration::ZERO;
/// No-deadline sentinel.
///
/// Reference: Asynq v0.26.0 `NO_DEADLINE`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L235-L239>.
pub const NO_DEADLINE: SystemTime = UNIX_EPOCH;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnqueuePlanError {
    #[error("task typename cannot be empty")]
    EmptyTaskType,
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("task ID cannot be empty")]
    EmptyTaskId,
    #[error("Unique TTL cannot be less than 1s")]
    UniqueTtlTooShort,
    #[error("group key cannot be empty")]
    EmptyGroupKey,
    #[error("task option duration overflow: {0}")]
    DurationOverflow(&'static str),
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}

#[cfg(test)]
mod tests;
