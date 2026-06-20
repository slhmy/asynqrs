use std::time::SystemTime;

use crate::broker::redis::{
    RedisCurrentQueueStatsPlan, RedisExecutor, RedisGroupStatsPlan, RedisHistoricalQueueStatsPlan,
    RedisQueueMemoryUsagePlan,
};
use crate::client::Clock;
use crate::{AdminError, DailyStats, GroupStats, QueueStats};

use super::parse::{
    current_queue_stats_has_pending_latency, parse_group_stats_values,
    parse_historical_queue_stats_values, parse_queue_stats_values, redis_value_to_i64,
};
use super::{RedisBroker, RedisBrokerError};

/// Reference: Asynq v0.26.0 queue and group stats inspection:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go>
/// and <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    pub async fn current_queue_stats(&mut self, queue: &str) -> Result<QueueStats, AdminError> {
        // Reference: Asynq v0.26.0 `RDB.CurrentStats` verifies the queue
        // before reading `r.clock.Now()` for daily stat keys and timestamps.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L140-L151>.
        self.ensure_queue_exists(queue).await?;
        let now = self.clock.now();
        self.current_queue_stats_with_now_for_existing_queue(queue, now)
            .await
    }

    pub async fn current_queue_stats_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<QueueStats, AdminError> {
        self.ensure_queue_exists(queue).await?;
        self.current_queue_stats_with_now_for_existing_queue(queue, now)
            .await
    }

    pub(super) async fn current_queue_stats_with_now_for_existing_queue(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<QueueStats, AdminError> {
        let stats_plan = RedisCurrentQueueStatsPlan::from_queue(queue, now)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;
        let memory_plan = RedisQueueMemoryUsagePlan::from_queue(queue)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;

        let stats_call = stats_plan.call();
        stats_call
            .validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let values = self
            .executor
            .eval_script_value(stats_call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        // Reference: Asynq v0.26.0 `RDB.CurrentStats` reads `r.clock.Now()`
        // a second time only when computing nonzero pending latency from
        // `oldest_pending_since`, before running the memory-usage script.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L217-L222>.
        let latency_now = if current_queue_stats_has_pending_latency(&values)? {
            self.clock.now()
        } else {
            now
        };

        let memory_call = memory_plan.call();
        memory_call
            .validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let memory_usage = redis_value_to_i64(
            &self
                .executor
                .eval_script_value(memory_call)
                .await
                .map_err(RedisBrokerError::Executor)
                .map_err(AdminError::from)?,
        )?;

        parse_queue_stats_values(stats_plan.queue(), now, latency_now, memory_usage, values)
    }

    pub async fn historical_queue_stats_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
        days: isize,
    ) -> Result<Vec<DailyStats>, AdminError> {
        if days < 1 {
            return Err(AdminError::NonPositiveDays);
        }
        let days = days as usize;
        self.ensure_queue_exists(queue).await?;
        self.historical_queue_stats_with_now_for_existing_queue(queue, now, days)
            .await
    }

    pub async fn historical_queue_stats(
        &mut self,
        queue: &str,
        days: isize,
    ) -> Result<Vec<DailyStats>, AdminError> {
        // Reference: Asynq v0.26.0 `RDB.HistoricalStats` rejects invalid day
        // counts before queue lookup, then verifies the queue before reading
        // `r.clock.Now()` for historical keys.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L363-L376>.
        if days < 1 {
            return Err(AdminError::NonPositiveDays);
        }
        self.ensure_queue_exists(queue).await?;
        let now = self.clock.now();
        self.historical_queue_stats_with_now_for_existing_queue(queue, now, days as usize)
            .await
    }

    pub(super) async fn historical_queue_stats_with_now_for_existing_queue(
        &mut self,
        queue: &str,
        now: SystemTime,
        days: usize,
    ) -> Result<Vec<DailyStats>, AdminError> {
        let plan = RedisHistoricalQueueStatsPlan::from_queue(queue, now, days)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;

        let call = plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let value = self
            .executor
            .eval_script_value(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;

        parse_historical_queue_stats_values(plan.queue(), plan.days(), value)
    }

    pub async fn group_stats(&mut self, queue: &str) -> Result<Vec<GroupStats>, AdminError> {
        let plan = RedisGroupStatsPlan::from_queue(queue)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;

        let call = plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let value = self
            .executor
            .eval_script_value(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;

        parse_group_stats_values(value)
    }
}
