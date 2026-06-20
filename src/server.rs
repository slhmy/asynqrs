use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

#[cfg(test)]
use std::collections::HashMap;

#[cfg(test)]
use crate::AggregationError;
#[cfg(test)]
use crate::GroupAggregator;
#[cfg(test)]
use crate::MetadataError;
#[cfg(test)]
use crate::{ProcessingScope, SharedErrorHandler, SharedIsFailure, SharedRetryDelay};

mod accessors;
mod active_worker;
mod aggregation;
mod builder;
mod cancellation;
mod config;
mod constructors;
mod deadline;
mod defaults;
mod error;
mod health;
mod logging;
pub(crate) mod maintenance;
mod maintenance_broker;
mod metadata;
mod metadata_broker;
mod pending_sync;
mod processing;
mod queue;
mod reporter;
mod runtime;
mod runtime_parts;
mod runtime_state;
pub(crate) mod shutdown;
mod state;
mod summary;
mod syncer;
#[cfg(test)]
mod test_support;
mod worker;
mod worker_assembly;
#[cfg(test)]
mod worker_assembly_config;
#[cfg(test)]
mod worker_assembly_test_helpers;
mod worker_broker;
mod worker_parts;
mod worker_runtime;

pub(crate) use active_worker::{ActiveWorkerRegistry, ActiveWorkerSnapshot};
use aggregation::AggregationRunner;
pub use aggregation::{ServerAggregationConfig, SharedGroupAggregator};
pub use cancellation::ServerCanceller;
pub use config::{Config, ConfigBuildError, ConfigBuilder, ServerProcessingScope};
pub use constructors::{RedisBackedServer, RedisBackedServerBuilder};
pub(crate) use deadline::{tokio_instant_for_system_time, worker_task_deadline};
pub(crate) use defaults::DEFAULT_JANITOR_BATCH_SIZE;
pub use defaults::{
    DEFAULT_SERVER_FORWARDER_INTERVAL, DEFAULT_SERVER_GROUP_GRACE_PERIOD,
    DEFAULT_SERVER_HEALTH_CHECK_INTERVAL, DEFAULT_SERVER_IDLE_SLEEP,
    DEFAULT_SERVER_JANITOR_INTERVAL, DEFAULT_SERVER_MAINTENANCE_INTERVAL,
    DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL, DEFAULT_SERVER_METADATA_TTL,
    DEFAULT_SERVER_RECOVER_RETRY_DELAY, DEFAULT_SERVER_RECOVERER_INTERVAL,
    DEFAULT_SERVER_SHUTDOWN_TIMEOUT, DEFAULT_SERVER_SYNCER_INTERVAL,
};
pub use error::{ServerConstructionError, ServerError};
pub use health::{HealthCheckFunc, HealthCheckHandler};
pub use logging::{LogLevel, Logger, ParseLogLevelError};
pub(crate) use logging::{
    PROCESSING_DEQUEUE_ERROR_LOG_INTERVAL, log_processing_debug, log_processing_error,
    log_processing_warn,
};
pub(in crate::server) use maintenance::normalize_interval;
pub(crate) use maintenance_broker::{CleanupBroker, ForwardBroker, LeaseBroker, RecoverBroker};
pub use metadata::ServerMetadata;
#[cfg(test)]
use metadata::effective_metadata_heartbeat_interval;
#[cfg(test)]
use metadata::normalize_hostname;
use metadata::server_info_i32;
pub(crate) use metadata::worker_info_bytes;
pub(crate) use metadata_broker::{MetadataBroker, PingBroker};
pub(crate) use pending_sync::{PendingSyncBacklog, PendingSyncDrainPolicy, PendingSyncOperation};
pub use processing::ProcessingError;
pub(crate) use processing::{ProcessingLease, WorkerRun};
pub use queue::{QueueConfig, QueuePriority, QueueSelector, default_queue_config};
pub(crate) use reporter::WorkerReportRouter;
pub(crate) use runtime_parts::WorkerRuntimeParts;
mod run;
pub use run::ServerHandle;
pub(crate) use runtime_state::ServerRuntimeState;
pub(crate) use state::ServerState;
pub(crate) use summary::ServerMaintenanceRun;
pub use summary::ServerRunSummary;
pub(crate) use worker::Worker;
pub(crate) use worker::{
    BorrowedWorkerFactory, ServerClock, ServerConnection, ServerHeartbeatStore,
    ServerLeaseExtender, ServerMaintenanceRunner, ServerRuntimeStateAttach, ServerShutdown,
    ServerSyncStore, WorkerActiveTask, WorkerFactory, WorkerHandlerResult, WorkerReporter,
    WorkerTaskLifecycle, WorkerTaskReport,
};
pub(crate) use worker_assembly::WorkerAssembly;
pub(crate) use worker_broker::{ArchiveBroker, RequeueBroker, RetryBroker, WorkerBrokerCore};
pub(crate) use worker_parts::{BorrowedWorkerParts, WorkerParts};

/// Background listener owned by `Server` for worker-side task cancellation.
///
/// Reference: Asynq v0.26.0 `Server.Start` launches a cancellation subscriber
/// alongside workers, forwarder, recoverer, and heartbeat components:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L687-L695>.
pub(crate) trait CancellationListener: Send + Sync + std::fmt::Debug {
    fn run_until_stopped(
        &self,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<usize, ServerError>>;
}

/// Async sleep abstraction used by the Rust server runtime loop.
///
/// Reference: Asynq v0.26.0 server workers sleep between empty queue polls
/// using `DEFAULT_SERVER_IDLE_SLEEP`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L410>.
///
/// Rust compatibility: this stays shared until server timing needs dedicated
/// worker, health-check, or heartbeat behavior beyond the runtime interface.
#[async_trait]
pub(crate) trait Sleeper {
    async fn sleep(&mut self, duration: Duration);
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TokioSleeper;

#[async_trait]
impl Sleeper for TokioSleeper {
    async fn sleep(&mut self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Minimal Tokio-native worker server loop.
///
/// Reference: Asynq v0.26.0 `Server.Run` / `Server.Start` coordinate worker
/// goroutines and background lifecycle components:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.
///
/// Rust-native note: server capability traits are crate-internal runtime and
/// test boundaries. Public users construct Redis-backed servers through
/// `RedisBackedServerBuilder`; they do not assemble `Server<P, S>` directly.
#[derive(Clone)]
pub(crate) struct Server<P, S = TokioSleeper> {
    runtime: P,
    worker_count: usize,
    queues: Arc<[String]>,
    queue_selector: QueueSelector,
    aggregation_config: Option<ServerAggregationConfig>,
    idle_sleep: Duration,
    forwarder_interval: Duration,
    recoverer_interval: Duration,
    janitor_interval: Duration,
    syncer_interval: Duration,
    shutdown_timeout: Duration,
    health_check_interval: Duration,
    health_check_handler: Option<Arc<dyn HealthCheckHandler>>,
    log_level: LogLevel,
    logger: Option<Arc<dyn Logger>>,
    metadata: Option<ServerMetadata>,
    metadata_heartbeat_interval: Duration,
    cancellation_listener: Option<Arc<dyn CancellationListener>>,
    aggregation_runner: Option<Arc<dyn AggregationRunner>>,
    shared_connection: bool,
    sleeper: S,
    runtime_state: ServerRuntimeState,
    state: ServerState,
}

impl<P, S> fmt::Debug for Server<P, S>
where
    P: fmt::Debug,
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server")
            .field("runtime", &self.runtime)
            .field("worker_count", &self.worker_count)
            .field("queues", &self.queues)
            .field("queue_selector", &self.queue_selector)
            .field("aggregation_config", &self.aggregation_config)
            .field("idle_sleep", &self.idle_sleep)
            .field("forwarder_interval", &self.forwarder_interval)
            .field("recoverer_interval", &self.recoverer_interval)
            .field("janitor_interval", &self.janitor_interval)
            .field("syncer_interval", &self.syncer_interval)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .field("health_check_interval", &self.health_check_interval)
            .field(
                "has_health_check_handler",
                &self.health_check_handler.is_some(),
            )
            .field("log_level", &self.log_level)
            .field("has_logger", &self.logger.is_some())
            .field("metadata", &self.metadata)
            .field(
                "metadata_heartbeat_interval",
                &self.metadata_heartbeat_interval,
            )
            .field(
                "has_cancellation_listener",
                &self.cancellation_listener.is_some(),
            )
            .field("has_aggregation_runner", &self.aggregation_runner.is_some())
            .field("shared_connection", &self.shared_connection)
            .field("sleeper", &self.sleeper)
            .field("runtime_state", &self.runtime_state)
            .field("state", &self.state)
            .finish()
    }
}

#[cfg(test)]
mod tests;
