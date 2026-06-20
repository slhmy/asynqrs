//! Admin and inspector Redis command plans.
//!
//! Reference: Asynq v0.26.0 inspector Redis commands:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::time::SystemTime;

mod aggregation;
mod queue;
mod stats;
mod task;

pub use aggregation::{
    RedisArchiveAllAggregatingTasksPlan, RedisDeleteAllAggregatingTasksPlan,
    RedisListAggregatingTasksPlan, RedisListAggregationGroupsPlan, RedisRunAllAggregatingTasksPlan,
};
pub use queue::{
    RedisDeleteQueuePlan, RedisListQueuesPlan, RedisPauseQueuePlan, RedisUnpauseQueuePlan,
};
pub use stats::{
    RedisCurrentQueueStatsPlan, RedisGroupStatsPlan, RedisHistoricalQueueStatsPlan,
    RedisQueueMemoryUsagePlan,
};
pub use task::{
    RedisArchiveAllTasksPlan, RedisArchiveTaskPlan, RedisDeleteAllTasksPlan, RedisDeleteTaskPlan,
    RedisListTasksPlan, RedisRunAllTasksPlan, RedisRunTaskPlan, RedisTaskInfoPlan,
    RedisUpdateTaskPayloadPlan,
};

use super::{RedisAdminPlanError, unix_seconds_floor};
use crate::Pagination;

pub(super) fn unix_seconds_admin(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisAdminPlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisAdminPlanError::TimeOverflow(context))
}

pub(super) fn pagination_start(pagination: &Pagination) -> Result<i64, RedisAdminPlanError> {
    pagination
        .start()
        .try_into()
        .map_err(|_| RedisAdminPlanError::PaginationOverflow)
}

pub(super) fn pagination_stop(pagination: &Pagination) -> Result<i64, RedisAdminPlanError> {
    pagination
        .stop()
        .try_into()
        .map_err(|_| RedisAdminPlanError::PaginationOverflow)
}
