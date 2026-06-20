use thiserror::Error;

use crate::TaskState;

/// Errors while building Redis enqueue command plans.
///
/// Reference: Asynq v0.26.0 Redis enqueue methods and scripts:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L6-L24>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisEnqueuePlanError {
    #[error("cannot enqueue Redis command plan for {0} state")]
    UnsupportedState(TaskState),
    #[error("scheduled enqueue plan is missing process time")]
    MissingScheduledProcessTime,
    #[error("aggregating enqueue plan is missing group key")]
    MissingGroupKey,
    #[error("unique enqueue plan is missing unique key")]
    MissingUniqueKey,
    #[error("unique enqueue plan is missing lock ttl")]
    MissingUniqueLockTtl,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}
