use crate::broker::redis::RedisRuntimeBroker;
use crate::scheduler::{Scheduler, SchedulerOpts};
use crate::{
    PeriodicTaskConfigProvider, PeriodicTaskManager, PeriodicTaskManagerError,
    RedisBackedScheduler, RedisRuntimeClient, SystemClock,
};

/// Concrete periodic task manager type backed by a Redis scheduler.
///
/// Reference: Asynq v0.26.0 `NewPeriodicTaskManager` creates a manager backed
/// by Redis and an internal scheduler:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
pub type RedisBackedPeriodicTaskManager<P> =
    PeriodicTaskManager<P, RedisRuntimeBroker, SystemClock>;

/// Builds a periodic task manager from a custom scheduler broker and provider.
///
/// Reference: Asynq v0.26.0 periodic task managers own a scheduler and dynamic
/// provider; this Rust constructor keeps the same ownership shape for tests
/// and custom broker adapters:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
impl<P, B> PeriodicTaskManager<P, B, SystemClock>
where
    P: PeriodicTaskConfigProvider,
{
    pub fn new_with_generated_scheduler_id(
        broker: B,
        provider: P,
        scheduler_opts: SchedulerOpts,
    ) -> Result<Self, PeriodicTaskManagerError> {
        let scheduler = Scheduler::new_with_generated_id(broker)
            .map(|scheduler| scheduler.with_scheduler_opts(scheduler_opts))?;
        Ok(PeriodicTaskManager::new(provider, scheduler))
    }
}

/// Builds a periodic task manager from a Redis client and provider.
///
/// Reference: Asynq v0.26.0 public `NewPeriodicTaskManager` creates a manager
/// backed by Redis and a periodic config provider:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/periodic_task_manager.go>.
impl<P> RedisBackedPeriodicTaskManager<P>
where
    P: PeriodicTaskConfigProvider,
{
    pub async fn from_redis_client(
        redis_client: impl Into<RedisRuntimeClient>,
        provider: P,
        scheduler_opts: SchedulerOpts,
    ) -> Result<Self, PeriodicTaskManagerError> {
        Self::from_redis_runtime_client(redis_client.into(), provider, scheduler_opts).await
    }

    pub async fn from_redis_runtime_client(
        redis_client: RedisRuntimeClient,
        provider: P,
        scheduler_opts: SchedulerOpts,
    ) -> Result<Self, PeriodicTaskManagerError> {
        let scheduler: RedisBackedScheduler =
            RedisBackedScheduler::from_redis_runtime_client(redis_client, scheduler_opts).await?;
        Ok(PeriodicTaskManager::new(provider, scheduler))
    }

    pub async fn from_direct_redis_client(
        redis_client: redis::Client,
        provider: P,
        scheduler_opts: SchedulerOpts,
    ) -> Result<Self, PeriodicTaskManagerError> {
        Self::from_redis_runtime_client(
            RedisRuntimeClient::direct(redis_client),
            provider,
            scheduler_opts,
        )
        .await
    }
}
