use async_trait::async_trait;

use crate::Task;

use super::{AggregationError, AggregationSet};

#[async_trait]
pub trait AggregationHandler {
    async fn handle_aggregation(
        &mut self,
        queue: &str,
        group: &str,
        set_id: &str,
        set: AggregationSet,
    ) -> Result<Task, AggregationError>;
}

/// User-provided aggregation function that combines grouped tasks into one task.
///
/// Reference: Asynq v0.26.0 `GroupAggregator.Aggregate` receives a group name
/// and the grouped tasks, then returns the task to enqueue:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L143-L149>.
#[async_trait]
pub trait GroupAggregator {
    async fn aggregate(&mut self, group: &str, tasks: Vec<Task>) -> Result<Task, AggregationError>;
}

/// Function adapter for group aggregators.
///
/// Reference: Asynq v0.26.0 public `GroupAggregatorFunc` adapter:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L261-L270>.
pub struct GroupAggregatorFunc<F>(pub F);

impl<F> GroupAggregatorFunc<F>
where
    F: FnMut(&str, Vec<Task>) -> Task,
{
    pub fn aggregate(&mut self, group: &str, tasks: Vec<Task>) -> Task {
        (self.0)(group, tasks)
    }
}

#[async_trait]
impl<F> GroupAggregator for GroupAggregatorFunc<F>
where
    F: FnMut(&str, Vec<Task>) -> Task + Send,
{
    async fn aggregate(&mut self, group: &str, tasks: Vec<Task>) -> Result<Task, AggregationError> {
        Ok(GroupAggregatorFunc::aggregate(self, group, tasks))
    }
}

#[derive(Debug, Clone)]
pub struct GroupAggregatorHandler<G> {
    aggregator: G,
}

impl<G> GroupAggregatorHandler<G> {
    pub fn new(aggregator: G) -> Self {
        Self { aggregator }
    }

    pub fn aggregator(&self) -> &G {
        &self.aggregator
    }

    pub fn aggregator_mut(&mut self) -> &mut G {
        &mut self.aggregator
    }
}

#[async_trait]
impl<G> AggregationHandler for GroupAggregatorHandler<G>
where
    G: GroupAggregator + Send,
{
    async fn handle_aggregation(
        &mut self,
        queue: &str,
        group: &str,
        _set_id: &str,
        set: AggregationSet,
    ) -> Result<Task, AggregationError> {
        let tasks = set.into_tasks();
        let _ = queue;
        self.aggregator.aggregate(group, tasks).await
    }
}
