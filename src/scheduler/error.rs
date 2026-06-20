use thiserror::Error;

use crate::broker::redis::MakeRedisClientError;
use crate::{BrokerError, MetadataError};

/// Scheduler lifecycle and registration errors.
///
/// Reference: Asynq v0.26.0 scheduler tracks new, active, and closed states
/// and exposes fixed public lifecycle error strings:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L233-L334>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SchedulerError {
    #[error("scheduler id must contain one or more characters")]
    EmptySchedulerId,
    #[error("scheduler entry id must contain one or more characters")]
    EmptyEntryId,
    /// Reference: Asynq v0.26.0 `Scheduler.Unregister` returns a fixed
    /// public error string for any unknown scheduler entry id:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L233-L244>.
    #[error("asynq: no scheduler entry found")]
    EntryNotFound(String),
    #[error("scheduler entry interval must be positive")]
    ZeroInterval,
    #[error("scheduler entry spec is not supported: {0}")]
    UnsupportedSpec(String),
    #[error("scheduler entry spec has no next enqueue time: {0}")]
    NoNextEnqueueTime(String),
    /// Reference: Asynq v0.26.0 scheduler start precondition errors:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L260-L271>.
    #[error("asynq: the scheduler is already running")]
    AlreadyRunning,
    /// Reference: Asynq v0.26.0 scheduler start precondition errors:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L260-L271>.
    #[error("asynq: the scheduler has already been stopped")]
    AlreadyStopped,
    #[error("failed to build enqueue plan: {0}")]
    EnqueuePlan(#[from] crate::EnqueuePlanError),
    #[error("broker failed to enqueue scheduled task: {0}")]
    Broker(#[from] BrokerError),
    #[error("scheduler metadata operation failed: {0}")]
    Metadata(#[from] MetadataError),
    /// Reference: Asynq v0.26.0 `Scheduler.Ping` returns the underlying Redis
    /// ping error directly:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L367-L375>.
    #[error("{0}")]
    Ping(String),
    #[error("scheduler background task failed: {0}")]
    BackgroundTask(String),
    #[error("{0} overflowed")]
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SchedulerConstructionError {
    #[error("{0}")]
    Redis(#[from] MakeRedisClientError),
    #[error("{0}")]
    Scheduler(#[from] SchedulerError),
}
