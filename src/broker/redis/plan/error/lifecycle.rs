use thiserror::Error;

/// Errors while building Redis completion command plans.
///
/// Reference: Asynq v0.26.0 Redis task lifecycle methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisCompletePlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("task id must contain one or more characters")]
    EmptyTaskId,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisRetryPlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("task id must contain one or more characters")]
    EmptyTaskId,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisArchivePlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("task id must contain one or more characters")]
    EmptyTaskId,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisRequeuePlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("task id must contain one or more characters")]
    EmptyTaskId,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisForwardPlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisDeleteExpiredCompletedTasksPlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisWriteResultPlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("task id must contain one or more characters")]
    EmptyTaskId,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisRecoverPlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisExtendLeasePlanError {
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("task id must contain one or more characters")]
    EmptyTaskId,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}
