use std::time::{Duration, SystemTime};

use crate::aggregation::{AggregationBroker, AggregationError};
use crate::broker::redis::{RedisExecutor, RedisListAggregationGroupsPlan};
use crate::client::Clock;
use crate::{AggregationSet, BrokerError, EnqueuePlan};

use super::super::RedisBroker;

#[async_trait::async_trait]
impl<E, C> AggregationBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn list_aggregation_groups(
        &mut self,
        queue: &str,
    ) -> Result<Vec<String>, AggregationError> {
        let plan = RedisListAggregationGroupsPlan::from_queue(queue)
            .map_err(|error| AggregationError::Other(error.to_string()))?;
        // Reference: Asynq v0.26.0 `RDB.ListGroups` returns Redis `SMEMBERS`
        // results directly from the all-groups set, without size lookups or
        // client-side sorting.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1023-L1031>.
        self.executor
            .smembers(plan.key())
            .await
            .map_err(|error| AggregationError::Other(error.to_string()))
    }

    async fn aggregation_check(
        &mut self,
        queue: &str,
        group: &str,
        now: SystemTime,
        grace_period: Duration,
        max_delay: Duration,
        max_size: isize,
    ) -> Result<Option<String>, AggregationError> {
        self.aggregation_check_with_now(queue, group, now, grace_period, max_delay, max_size)
            .await
    }

    async fn read_aggregation_set(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<AggregationSet, AggregationError> {
        RedisBroker::read_aggregation_set(self, queue, group, set_id).await
    }

    async fn delete_aggregation_set(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<(), AggregationError> {
        RedisBroker::delete_aggregation_set(self, queue, group, set_id).await
    }

    async fn enqueue_aggregated(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.enqueue_with_now(plan, self.clock.now()).await
    }

    async fn reclaim_stale_aggregation_sets(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<(), AggregationError> {
        self.reclaim_stale_aggregation_sets_with_now(queue, now)
            .await
    }
}
