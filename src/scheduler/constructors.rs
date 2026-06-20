use crate::SystemClock;
use crate::broker::redis::{
    RedisRuntimeBroker, RedisRuntimeClient, redis_broker_from_runtime_client,
};

pub use super::error::SchedulerConstructionError;
use super::{Scheduler, SchedulerError, SchedulerOpts};

impl<B> Scheduler<B, SystemClock> {
    pub fn new(scheduler_id: impl Into<String>, broker: B) -> Result<Self, SchedulerError> {
        Self::with_clock(scheduler_id, broker, SystemClock)
    }

    /// Creates a scheduler with an upstream-style generated scheduler id.
    ///
    /// Reference: Asynq v0.26.0 `newScheduler` assigns each scheduler a
    /// generated `hostname:pid:uuid` id used for heartbeat metadata keys:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L91-L103>.
    pub fn new_with_generated_id(broker: B) -> Result<Self, SchedulerError> {
        Self::with_generated_id_and_clock(broker, SystemClock)
    }
}

/// Concrete scheduler type backed by a Redis multiplexed connection.
///
/// Reference: Asynq v0.26.0 public `Scheduler` created by
/// `NewSchedulerFromRedisClient`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/scheduler.go#L91-L103>.
pub type RedisBackedScheduler = Scheduler<RedisRuntimeBroker, SystemClock>;

impl RedisBackedScheduler {
    pub async fn from_redis_client(
        redis_client: impl Into<RedisRuntimeClient>,
        opts: SchedulerOpts,
    ) -> Result<Self, SchedulerConstructionError> {
        Self::from_redis_runtime_client(redis_client.into(), opts).await
    }

    pub async fn from_redis_runtime_client(
        redis_client: RedisRuntimeClient,
        opts: SchedulerOpts,
    ) -> Result<Self, SchedulerConstructionError> {
        let broker = redis_broker_from_runtime_client(redis_client).await?;
        Ok(Scheduler::new_with_generated_id(broker)?
            .with_scheduler_opts(opts)
            .with_shared_connection())
    }

    pub async fn from_direct_redis_client(
        redis_client: redis::Client,
        opts: SchedulerOpts,
    ) -> Result<Self, SchedulerConstructionError> {
        Self::from_redis_runtime_client(RedisRuntimeClient::direct(redis_client), opts).await
    }
}
