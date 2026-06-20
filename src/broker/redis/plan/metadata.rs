use std::time::{Duration, SystemTime};

use prost::Message;

mod scheduler;
mod server;

pub use scheduler::{
    RedisClearSchedulerEntriesPlan, RedisClearSchedulerHistoryPlan,
    RedisListSchedulerEnqueueEventsPlan, RedisListSchedulerEntriesPlan,
    RedisRecordSchedulerEnqueueEventPlan, RedisWriteSchedulerEntriesPlan,
};
pub use server::{
    RedisClearServerStatePlan, RedisListServerKeysPlan, RedisListWorkerKeysPlan,
    RedisWriteServerStatePlan,
};

use super::{RedisMetadataPlanError, unix_seconds_floor};
use crate::compat::MAX_DURATION_NANOS_U128;
use crate::pb;
use crate::task::duration_seconds;

pub(super) fn unix_seconds_metadata(
    time: SystemTime,
    context: &'static str,
) -> Result<i64, RedisMetadataPlanError> {
    let seconds = unix_seconds_floor(time);
    seconds
        .try_into()
        .map_err(|_| RedisMetadataPlanError::TimeOverflow(context))
}

pub(super) fn metadata_duration_seconds(
    duration: Duration,
    context: &'static str,
) -> Result<i64, RedisMetadataPlanError> {
    // Reference: Asynq v0.26.0 server and scheduler metadata TTL values are
    // Go `time.Duration` values passed to Redis as seconds:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1371-L1413>.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1449-L1467>.
    if duration.as_nanos() > MAX_DURATION_NANOS_U128 {
        return Err(RedisMetadataPlanError::TimeOverflow(context));
    }
    Ok(duration_seconds(duration))
}

pub(super) fn worker_info_id(data: &[u8]) -> Option<String> {
    // Reference: Asynq v0.26.0 `WriteServerState` stores workers in a hash
    // keyed by worker id, which `EncodeWorkerInfo` serializes as protobuf
    // `TaskId`, and skips only worker metadata it cannot encode:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1371-L1413>.
    // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L427-L480>.
    let info = pb::asynq::WorkerInfo::decode(data).ok()?;
    Some(info.task_id)
}
