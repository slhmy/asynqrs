use thiserror::Error;

use crate::broker::redis::MakeRedisClientError;
use crate::{AggregationError, BrokerError, MetadataError, ProcessingError};

/// Errors returned by server construction, runtime, and lifecycle helpers.
///
/// Reference: Asynq v0.26.0 public `Server` construction/startup errors and
/// sentinels:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L431-L721>.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServerError {
    #[error("server requires at least one queue")]
    EmptyQueueList,
    #[error("queue name must contain one or more characters")]
    EmptyQueueName,
    #[error("queue priority must be positive")]
    NonPositiveQueuePriority,
    #[error("server requires at least one worker")]
    EmptyWorkerCount,
    #[error("server worker task panicked")]
    WorkerThreadPanicked,
    #[error("server shutdown timed out")]
    ShutdownTimedOut,
    /// Reference: Asynq v0.26.0 `Server.Start` rejects a nil handler before
    /// starting the server:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L680-L683>.
    #[error("asynq: server cannot run with nil handler")]
    NilHandler,
    /// Reference: Asynq v0.26.0 `Server.start` rejects starting an already
    /// active server:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L706-L721>.
    #[error("asynq: the server is already running")]
    AlreadyRunning,
    /// Reference: Asynq v0.26.0 `Server.start` rejects starting a stopped
    /// server before shutdown has finished:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L706-L721>.
    #[error("asynq: the server is in the stopped state. Waiting for shutdown.")]
    StoppedWaitingForShutdown,
    /// Reference: Asynq v0.26.0 `ServerError::Closed` sentinel returned when a
    /// server is started after shutdown:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L651-L704>.
    #[error("asynq: Server closed")]
    Closed,
    #[error("metadata hostname must contain one or more characters")]
    EmptyMetadataHostname,
    #[error("metadata server id must contain one or more characters")]
    EmptyMetadataServerId,
    #[error("metadata server info must contain one or more bytes")]
    EmptyMetadataServerInfo,
    #[error("metadata ttl must be positive")]
    ZeroMetadataTtl,
    #[error("server metadata operation failed: {0}")]
    Metadata(MetadataError),
    #[error("server cancellation listener failed: {0}")]
    CancellationListener(String),
    #[error("server aggregation configuration failed: {0}")]
    Aggregation(#[from] AggregationError),
    #[error("server aggregation config must be set before configuring a group aggregator")]
    MissingAggregationConfig,
    #[error("server aggregation runner failed: {0}")]
    AggregationRunner(String),
    #[error("task processing failed: {0}")]
    Processing(ProcessingError),
    /// Reference: Asynq v0.26.0 `Server.Shutdown` closes server-owned Redis
    /// connections and logs close failures after component shutdown:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L751-L755>.
    #[error("server broker close failed: {0}")]
    Close(BrokerError),
    /// Reference: Asynq v0.26.0 `Server.Ping` returns the underlying broker
    /// ping error directly:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L776-L786>.
    #[error("{0}")]
    Ping(String),
}

impl ServerError {
    /// Reports whether this server error matches the server-closed sentinel.
    ///
    /// Reference: Asynq v0.26.0 `Server.start` returns the public
    /// `ServerError::Closed` sentinel after shutdown:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L651-L704>.
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Closed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServerConstructionError {
    #[error("{0}")]
    Redis(#[from] MakeRedisClientError),
    #[error("{0}")]
    Server(#[from] ServerError),
}

impl From<ProcessingError> for ServerError {
    fn from(error: ProcessingError) -> Self {
        Self::Processing(error)
    }
}

impl From<MetadataError> for ServerError {
    fn from(error: MetadataError) -> Self {
        Self::Metadata(error)
    }
}
