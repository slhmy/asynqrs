use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use prost::Message;

#[cfg(test)]
use crate::Pagination;
use crate::broker::redis::keys;
#[cfg(test)]
use crate::pb;
use crate::task::TaskMessage;
use crate::task::duration_seconds;
#[cfg(test)]
use crate::{EnqueuePlan, TaskState};

mod admin;
mod aggregation;
mod dequeue;
mod enqueue;
mod error;
mod lifecycle;
mod metadata;
mod script;

pub use admin::{
    RedisArchiveAllAggregatingTasksPlan, RedisArchiveAllTasksPlan, RedisArchiveTaskPlan,
    RedisCurrentQueueStatsPlan, RedisDeleteAllAggregatingTasksPlan, RedisDeleteAllTasksPlan,
    RedisDeleteQueuePlan, RedisDeleteTaskPlan, RedisGroupStatsPlan, RedisHistoricalQueueStatsPlan,
    RedisListAggregatingTasksPlan, RedisListAggregationGroupsPlan, RedisListQueuesPlan,
    RedisListTasksPlan, RedisPauseQueuePlan, RedisQueueMemoryUsagePlan,
    RedisRunAllAggregatingTasksPlan, RedisRunAllTasksPlan, RedisRunTaskPlan, RedisTaskInfoPlan,
    RedisUnpauseQueuePlan, RedisUpdateTaskPayloadPlan,
};
pub use aggregation::{
    RedisAggregationCheckInput, RedisAggregationCheckPlan, RedisDeleteAggregationSetPlan,
    RedisReadAggregationSetPlan, RedisReclaimStaleAggregationSetsPlan,
};
pub use dequeue::RedisDequeuePlan;
pub use enqueue::RedisEnqueuePlan;
pub use error::{
    RedisAdminPlanError, RedisAggregationCheckPlanError, RedisArchivePlanError,
    RedisCompletePlanError, RedisDeleteAggregationSetPlanError,
    RedisDeleteExpiredCompletedTasksPlanError, RedisDequeuePlanError, RedisEnqueuePlanError,
    RedisExtendLeasePlanError, RedisForwardPlanError, RedisMetadataPlanError,
    RedisReadAggregationSetPlanError, RedisReclaimStaleAggregationSetsPlanError,
    RedisRecoverPlanError, RedisRequeuePlanError, RedisRetryPlanError, RedisWriteResultPlanError,
};
pub use lifecycle::{
    RedisArchivePlan, RedisCompletePlan, RedisDeleteExpiredCompletedTasksPlan,
    RedisExtendLeasePlan, RedisForwardPlan, RedisRecoverPlan, RedisRequeuePlan, RedisRetryPlan,
    RedisWriteResultPlan,
};
pub use metadata::{
    RedisClearSchedulerEntriesPlan, RedisClearSchedulerHistoryPlan, RedisClearServerStatePlan,
    RedisListSchedulerEnqueueEventsPlan, RedisListSchedulerEntriesPlan, RedisListServerKeysPlan,
    RedisListWorkerKeysPlan, RedisRecordSchedulerEnqueueEventPlan, RedisWriteSchedulerEntriesPlan,
    RedisWriteServerStatePlan,
};
pub use script::{RedisArg, RedisDequeueCall, RedisEnqueueOperation, RedisScript, RedisScriptCall};

/// Default lease duration for a dequeued task.
///
/// Reference: Asynq v0.26.0 `DEFAULT_LEASE_DURATION`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L46-L52>.
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(30);

/// Expiration used for daily processed/failed counters.
///
/// Reference: Asynq v0.26.0 `STATS_TTL`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L23>.
pub const STATS_TTL: Duration = Duration::from_secs(90 * 24 * 60 * 60);

/// Timeout used while a task aggregation set is being processed.
///
/// Reference: Asynq v0.26.0 `AGGREGATION_TIMEOUT`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
pub const AGGREGATION_TIMEOUT: Duration = Duration::from_secs(2 * 60);

/// Maximum number of tasks retained in the archived set.
///
/// Reference: Asynq v0.26.0 `MAX_ARCHIVE_SIZE`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
pub const MAX_ARCHIVE_SIZE: usize = 10_000;

/// Number of days before archived tasks are deleted permanently.
///
/// Reference: Asynq v0.26.0 `ARCHIVED_EXPIRATION_DAYS`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L841>.
pub const ARCHIVED_EXPIRATION_DAYS: i64 = 90;

/// Number of days before archived tasks are deleted permanently.
///
/// Reference: Asynq v0.26.0 `ARCHIVED_EXPIRATION_DAYS`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L841>.
pub const ARCHIVED_EXPIRATION: Duration =
    Duration::from_secs(ARCHIVED_EXPIRATION_DAYS as u64 * 24 * 60 * 60);

pub(super) fn task_key(msg: &TaskMessage) -> String {
    keys::task_key(&msg.queue, &msg.id)
}

pub(super) fn encoded_msg_arg(msg: &TaskMessage) -> RedisArg {
    RedisArg::Bytes(msg.encode_to_vec())
}

fn unix_seconds_floor(time: SystemTime) -> i128 {
    // Reference: Asynq v0.26.0 Redis plans use Go `time.Time.Unix`, which
    // returns the seconds elapsed since the Unix epoch with floor semantics
    // for fractional pre-epoch instants:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => i128::from(duration_seconds(duration)),
        Err(error) => {
            let duration = error.duration();
            let seconds = i128::from(duration_seconds(duration));
            if duration.subsec_nanos() == 0 {
                -seconds
            } else {
                -seconds - 1
            }
        }
    }
}

pub(super) fn duration_nanoseconds(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}

#[cfg(test)]
mod tests;
