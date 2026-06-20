use thiserror::Error;

/// Errors while building Redis dequeue command plans.
///
/// Reference: Asynq v0.26.0 Redis dequeue command behavior:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisDequeuePlanError {
    #[error("dequeue requires at least one queue")]
    EmptyQueueList,
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}
