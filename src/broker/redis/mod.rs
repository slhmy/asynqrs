mod broker;
mod executor;
pub(crate) mod keys;
mod listener;
mod plan;
mod runtime;
mod scripts;
#[cfg(test)]
mod tests;

pub use broker::RedisExecutorError;
pub(crate) use broker::{RedisBroker, RedisBrokerError};
pub use executor::{RedisConnectionExecutor, RedisExecutor};
pub use listener::RedisCancelListener;
pub use plan::{
    RedisAdminPlanError, RedisAggregationCheckInput, RedisAggregationCheckPlan,
    RedisAggregationCheckPlanError, RedisArchiveAllAggregatingTasksPlan, RedisArchiveAllTasksPlan,
    RedisArchivePlan, RedisArchivePlanError, RedisArchiveTaskPlan, RedisArg,
    RedisClearSchedulerEntriesPlan, RedisClearSchedulerHistoryPlan, RedisClearServerStatePlan,
    RedisCompletePlan, RedisCompletePlanError, RedisCurrentQueueStatsPlan,
    RedisDeleteAggregationSetPlan, RedisDeleteAggregationSetPlanError,
    RedisDeleteAllAggregatingTasksPlan, RedisDeleteAllTasksPlan,
    RedisDeleteExpiredCompletedTasksPlan, RedisDeleteExpiredCompletedTasksPlanError,
    RedisDeleteQueuePlan, RedisDeleteTaskPlan, RedisDequeueCall, RedisDequeuePlan,
    RedisDequeuePlanError, RedisEnqueueOperation, RedisEnqueuePlan, RedisEnqueuePlanError,
    RedisExtendLeasePlan, RedisExtendLeasePlanError, RedisForwardPlan, RedisForwardPlanError,
    RedisGroupStatsPlan, RedisHistoricalQueueStatsPlan, RedisListAggregatingTasksPlan,
    RedisListAggregationGroupsPlan, RedisListQueuesPlan, RedisListSchedulerEnqueueEventsPlan,
    RedisListSchedulerEntriesPlan, RedisListServerKeysPlan, RedisListTasksPlan,
    RedisListWorkerKeysPlan, RedisMetadataPlanError, RedisPauseQueuePlan,
    RedisQueueMemoryUsagePlan, RedisReadAggregationSetPlan, RedisReadAggregationSetPlanError,
    RedisReclaimStaleAggregationSetsPlan, RedisReclaimStaleAggregationSetsPlanError,
    RedisRecordSchedulerEnqueueEventPlan, RedisRecoverPlan, RedisRecoverPlanError,
    RedisRequeuePlan, RedisRequeuePlanError, RedisRetryPlan, RedisRetryPlanError,
    RedisRunAllAggregatingTasksPlan, RedisRunAllTasksPlan, RedisRunTaskPlan, RedisScript,
    RedisScriptCall, RedisTaskInfoPlan, RedisUnpauseQueuePlan, RedisUpdateTaskPayloadPlan,
    RedisWriteResultPlan, RedisWriteResultPlanError, RedisWriteSchedulerEntriesPlan,
    RedisWriteServerStatePlan,
};
pub use runtime::{
    MakeRedisClientError, RedisClusterRuntimeClient, RedisDirectRuntimeClient,
    RedisFailoverRuntimeClient, RedisRuntimeClient, RedisRuntimeConnection, RedisRuntimeTimeouts,
};
pub use scripts::{RedisScriptCallError, RedisScriptResult};

pub(crate) type RedisRuntimeBroker = RedisBroker<RedisConnectionExecutor<RedisRuntimeConnection>>;

pub(crate) async fn redis_broker_from_runtime_client(
    redis_client: RedisRuntimeClient,
) -> Result<RedisRuntimeBroker, MakeRedisClientError> {
    let connection = redis_client.into_runtime_connection().await?;
    Ok(RedisBroker::new(RedisConnectionExecutor::new(connection)))
}
