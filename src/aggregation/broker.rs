use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use crate::{BrokerError, EnqueuePlan};

use super::{AggregationError, AggregationSet};

#[async_trait]
pub trait AggregationBroker {
    async fn list_aggregation_groups(
        &mut self,
        queue: &str,
    ) -> Result<Vec<String>, AggregationError>;

    async fn aggregation_check(
        &mut self,
        queue: &str,
        group: &str,
        now: SystemTime,
        grace_period: Duration,
        max_delay: Duration,
        max_size: isize,
    ) -> Result<Option<String>, AggregationError>;

    async fn read_aggregation_set(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<AggregationSet, AggregationError>;

    async fn delete_aggregation_set(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
    ) -> Result<(), AggregationError>;

    async fn enqueue_aggregated(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError>;

    async fn reclaim_stale_aggregation_sets(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<(), AggregationError>;
}
