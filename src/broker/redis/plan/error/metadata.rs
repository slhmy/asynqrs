use thiserror::Error;

/// Errors while building Redis runtime metadata command plans.
///
/// Reference: Asynq v0.26.0 Redis server and scheduler metadata methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1449-L1546>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RedisMetadataPlanError {
    #[error("hostname must contain one or more characters")]
    EmptyHostname,
    #[error("server id must contain one or more characters")]
    EmptyServerId,
    #[error("server info must contain one or more bytes")]
    EmptyServerInfo,
    #[error("scheduler id must contain one or more characters")]
    EmptySchedulerId,
    #[error("scheduler entry id must contain one or more characters")]
    EmptySchedulerEntryId,
    #[error("scheduler entry data must contain one or more bytes")]
    EmptySchedulerEntry,
    #[error("scheduler history entry id must contain one or more characters")]
    EmptySchedulerHistoryEntryId,
    #[error("scheduler enqueue event must contain one or more bytes")]
    EmptySchedulerEnqueueEvent,
    #[error("time overflow while computing {0}")]
    TimeOverflow(&'static str),
}
