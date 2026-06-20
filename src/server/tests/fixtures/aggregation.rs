use super::*;
use crate::aggregation::AggregationBroker;

#[derive(Debug, Clone, Default)]
pub(in crate::server::tests) struct NoopAggregationBroker;

#[async_trait]
impl AggregationBroker for NoopAggregationBroker {
    async fn list_aggregation_groups(
        &mut self,
        _queue: &str,
    ) -> Result<Vec<String>, AggregationError> {
        Ok(Vec::new())
    }

    async fn aggregation_check(
        &mut self,
        _queue: &str,
        _group: &str,
        _now: SystemTime,
        _grace_period: Duration,
        _max_delay: Duration,
        _max_size: isize,
    ) -> Result<Option<String>, AggregationError> {
        Ok(None)
    }

    async fn read_aggregation_set(
        &mut self,
        _queue: &str,
        _group: &str,
        _set_id: &str,
    ) -> Result<crate::AggregationSet, AggregationError> {
        unreachable!("noop broker never reports a ready set")
    }

    async fn delete_aggregation_set(
        &mut self,
        _queue: &str,
        _group: &str,
        _set_id: &str,
    ) -> Result<(), AggregationError> {
        unreachable!("noop broker never reports a ready set")
    }

    async fn enqueue_aggregated(
        &mut self,
        _plan: &crate::EnqueuePlan,
    ) -> Result<(), crate::BrokerError> {
        unreachable!("noop broker never reports a ready set")
    }

    async fn reclaim_stale_aggregation_sets(
        &mut self,
        _queue: &str,
        _now: SystemTime,
    ) -> Result<(), AggregationError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(in crate::server::tests) struct NoopGroupAggregator;

#[async_trait]
impl GroupAggregator for NoopGroupAggregator {
    async fn aggregate(
        &mut self,
        _group: &str,
        _tasks: Vec<crate::Task>,
    ) -> Result<crate::Task, AggregationError> {
        unreachable!("noop broker never reports a ready set")
    }
}
