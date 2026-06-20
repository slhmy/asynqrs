use super::sources::DELETE_QUEUE_SOURCE;
use super::*;
use crate::broker::redis::{
    RedisAggregationCheckInput, RedisAggregationCheckPlan, RedisArchiveAllAggregatingTasksPlan,
    RedisArchiveAllTasksPlan, RedisArchivePlan, RedisArchiveTaskPlan, RedisArg, RedisCompletePlan,
    RedisCurrentQueueStatsPlan, RedisDeleteAggregationSetPlan, RedisDeleteAllAggregatingTasksPlan,
    RedisDeleteAllTasksPlan, RedisDeleteExpiredCompletedTasksPlan, RedisDeleteQueuePlan,
    RedisDeleteTaskPlan, RedisEnqueueOperation, RedisEnqueuePlan, RedisExtendLeasePlan,
    RedisForwardPlan, RedisGroupStatsPlan, RedisHistoricalQueueStatsPlan,
    RedisQueueMemoryUsagePlan, RedisReadAggregationSetPlan, RedisReclaimStaleAggregationSetsPlan,
    RedisRecoverPlan, RedisRequeuePlan, RedisRetryPlan, RedisRunAllAggregatingTasksPlan,
    RedisRunAllTasksPlan, RedisRunTaskPlan, RedisScript, RedisUpdateTaskPayloadPlan,
    RedisWriteResultPlan, RedisWriteSchedulerEntriesPlan, RedisWriteServerStatePlan,
};
use crate::task::TaskMessage;
use crate::{EnqueueOptions, EnqueuePlan, Task, TaskState};
use std::time::{Duration, UNIX_EPOCH};

mod call;
mod plans;
mod registry;

fn active_message(retention: i64, unique_key: &str) -> TaskMessage {
    let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
    msg.id = "task-id".to_owned();
    msg.queue = "critical".to_owned();
    msg.retention = retention;
    msg.unique_key = unique_key.to_owned();
    msg
}
