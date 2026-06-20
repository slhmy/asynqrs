//! Aggregation runtime errors.
//!
//! Reference: Asynq v0.26.0 aggregator returns and logs errors from group
//! validation, aggregation handling, enqueue planning, broker enqueueing, and
//! Redis-backed aggregation operations:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/aggregator.go#L92-L139>.

use thiserror::Error;

use crate::BrokerError;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AggregationError {
    #[error("aggregation queue must contain one or more characters")]
    EmptyQueueName,
    #[error("aggregation group must contain one or more characters")]
    EmptyGroupName,
    #[error("aggregation handler failed: {0}")]
    Handler(String),
    #[error("failed to build aggregated enqueue plan: {0}")]
    EnqueuePlan(#[from] crate::EnqueuePlanError),
    #[error("broker failed to enqueue aggregated task: {0}")]
    Broker(#[from] BrokerError),
    #[error("{0}")]
    Other(String),
}
