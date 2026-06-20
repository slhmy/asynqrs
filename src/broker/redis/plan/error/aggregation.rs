use thiserror::Error;

/// Errors while building Redis aggregation command plans.
///
/// Reference: Asynq v0.26.0 Redis aggregation-set methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1126-L1353>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisAggregationCheckPlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("group name must contain one or more characters")]
    EmptyGroupName,
    #[error("aggregation set id must contain one or more characters")]
    EmptySetId,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisReadAggregationSetPlanError {
    #[error("queue name must contain one or more characters")]
    QueueName,
    #[error("group name must contain one or more characters")]
    GroupName,
    #[error("aggregation set id must contain one or more characters")]
    SetId,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisDeleteAggregationSetPlanError {
    #[error("queue name must contain one or more characters")]
    QueueName,
    #[error("group name must contain one or more characters")]
    GroupName,
    #[error("aggregation set id must contain one or more characters")]
    SetId,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisReclaimStaleAggregationSetsPlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}
