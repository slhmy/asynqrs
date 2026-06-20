//! Redis broker error types and public error mapping.
//!
//! Reference: Asynq v0.26.0 internal RDB operations translate Redis script
//! and command failures into the public task queue error surface:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.

use thiserror::Error;

use crate::broker::redis::{
    RedisAdminPlanError, RedisAggregationCheckPlanError, RedisArchivePlanError,
    RedisCompletePlanError, RedisDeleteAggregationSetPlanError,
    RedisDeleteExpiredCompletedTasksPlanError, RedisDequeuePlanError, RedisEnqueuePlanError,
    RedisExtendLeasePlanError, RedisForwardPlanError, RedisMetadataPlanError,
    RedisReadAggregationSetPlanError, RedisReclaimStaleAggregationSetsPlanError,
    RedisRecoverPlanError, RedisRequeuePlanError, RedisRetryPlanError, RedisScript,
    RedisScriptCallError, RedisWriteResultPlanError,
};
use crate::task::DecodeTaskMessageError;

#[derive(Debug, Error)]
pub(crate) enum RedisBrokerError {
    #[error("failed to build Redis enqueue plan: {0}")]
    Plan(#[from] RedisEnqueuePlanError),
    #[error("failed to build Redis dequeue plan: {0}")]
    DequeuePlan(#[from] RedisDequeuePlanError),
    #[error("failed to build Redis complete plan: {0}")]
    CompletePlan(#[from] RedisCompletePlanError),
    #[error("failed to build Redis retry plan: {0}")]
    RetryPlan(#[from] RedisRetryPlanError),
    #[error("failed to build Redis archive plan: {0}")]
    ArchivePlan(#[from] RedisArchivePlanError),
    #[error("failed to build Redis requeue plan: {0}")]
    RequeuePlan(#[from] RedisRequeuePlanError),
    #[error("failed to build Redis forward plan: {0}")]
    ForwardPlan(#[from] RedisForwardPlanError),
    #[error("failed to build Redis completed-task cleanup plan: {0}")]
    DeleteExpiredCompletedTasksPlan(#[from] RedisDeleteExpiredCompletedTasksPlanError),
    #[error("failed to build Redis aggregation check plan: {0}")]
    AggregationCheckPlan(#[from] RedisAggregationCheckPlanError),
    #[error("failed to build Redis read aggregation set plan: {0}")]
    ReadAggregationSetPlan(#[from] RedisReadAggregationSetPlanError),
    #[error("failed to build Redis delete aggregation set plan: {0}")]
    DeleteAggregationSetPlan(#[from] RedisDeleteAggregationSetPlanError),
    #[error("failed to build Redis reclaim stale aggregation sets plan: {0}")]
    ReclaimStaleAggregationSetsPlan(#[from] RedisReclaimStaleAggregationSetsPlanError),
    #[error("failed to build Redis write result plan: {0}")]
    WriteResultPlan(#[from] RedisWriteResultPlanError),
    #[error("failed to build Redis metadata plan: {0}")]
    MetadataPlan(#[from] RedisMetadataPlanError),
    #[error("failed to build Redis admin plan: {0}")]
    AdminPlan(#[from] RedisAdminPlanError),
    #[error("failed to build Redis recover plan: {0}")]
    RecoverPlan(#[from] RedisRecoverPlanError),
    #[error("failed to build Redis extend lease plan: {0}")]
    ExtendLeasePlan(#[from] RedisExtendLeasePlanError),
    #[error("invalid Redis script call: {0}")]
    ScriptCall(#[from] RedisScriptCallError),
    #[error("Redis executor failed: {0}")]
    Executor(#[from] RedisExecutorError),
    #[error("failed to decode dequeued task message: {0}")]
    Decode(#[from] DecodeTaskMessageError),
    #[error("unexpected {script:?} script result: {result}")]
    UnexpectedScriptResult { script: RedisScript, result: i64 },
    #[error("unexpected {script:?} script status: {status}")]
    UnexpectedScriptStatus { script: RedisScript, status: String },
}

/// Error returned by the Redis executor abstraction.
///
/// Reference: Asynq v0.26.0 RDB methods preserve Redis command/script errors
/// until they are mapped at the operation boundary:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct RedisExecutorError {
    message: String,
}

impl RedisExecutorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

pub(super) fn redis_broker_error_matches_not_found(error: &RedisBrokerError) -> bool {
    matches!(error, RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND"))
}

pub(super) fn redis_broker_error_message(error: RedisBrokerError) -> String {
    match error {
        RedisBrokerError::Executor(error) => error.to_string(),
        RedisBrokerError::UnexpectedScriptResult { script, result } => {
            format!("unexpected {script:?} script result: {result}")
        }
        RedisBrokerError::UnexpectedScriptStatus { script, status } => {
            format!("unexpected {script:?} script status: {status}")
        }
        error => error.to_string(),
    }
}
