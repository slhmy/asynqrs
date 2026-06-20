//! Rust-native task queue APIs with Asynq-compatible Redis workflow behavior.
//!
//! `asynqrs` is a Rust learning/reimplementation project for
//! [hibiken/asynq](https://github.com/hibiken/asynq). It preserves the parts
//! users observe at workflow and Redis wire boundaries while using Rust-native
//! types, builders, ownership, and async runtime structure internally.
//!
//! The public API is organized around user workflows:
//!
//! - enqueue tasks with [`RedisBackedClient`], [`Task`], and
//!   [`EnqueueOptions`];
//! - process tasks with [`RedisBackedServerBuilder`], [`Config::builder`], and
//!   [`ServeMux`] or a custom [`Handler`];
//! - customize processing with middleware, retry delay, failure
//!   classification, error handlers, processing scopes, and lease extenders;
//! - register scheduled work with [`RedisBackedScheduler`];
//! - aggregate grouped tasks with [`GroupAggregator`] or
//!   [`GroupAggregatorFunc`];
//! - inspect queues, tasks, servers, workers, and scheduler metadata with
//!   [`Inspector`].
//!
//! The internal processing runtime is not a public construction surface. Users
//! customize processing through crate-root handler, middleware, retry, lease,
//! and error-hook exports while server-owned runtime assembly stays internal.
//!
//! # Enqueue
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use asynqrs::{EnqueueOptions, QueueName, RedisBackedClient, Task};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let redis = redis::Client::open("redis://127.0.0.1:6379/0")?;
//! let mut client = RedisBackedClient::from_redis_client(redis).await?;
//!
//! let task = Task::new("email:welcome", br#"{"user_id":42}"#.to_vec());
//! client
//!     .enqueue_with_async(
//!         &task,
//!         EnqueueOptions::new()
//!             .queue(QueueName::new("emails")?)
//!             .max_retries(5)
//!             .timeout(Duration::from_secs(30)),
//!     )
//!     .await?;
//!
//! client.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Process
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use asynqrs::{
//!     Config, HandlerError, ProcessingContext, RedisBackedServerBuilder,
//!     ServeMux, Task,
//! };
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = Config::builder()
//!     .concurrency(4)
//!     .try_queue("emails", 1usize)?
//!     .shutdown_timeout(Duration::from_secs(10))
//!     .try_build()?;
//!
//! let redis = redis::Client::open("redis://127.0.0.1:6379/0")?;
//! let server = RedisBackedServerBuilder::from_redis_client(redis, config);
//!
//! let mux = ServeMux::new().route_fn(
//!     "email:welcome",
//!     |_task: &Task, context: &ProcessingContext| -> Result<(), HandlerError> {
//!         println!("handling task {}", context.task_id());
//!         Ok(())
//!     },
//! );
//!
//! server.run(mux).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Reference: Asynq v0.26.0 public quick-start workflows:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/README.md#quickstart>.

pub mod admin;
pub mod aggregation;
mod broker;
pub mod client;
mod compat;
mod operation;
mod pb;
pub mod periodic;
mod processing;
pub mod scheduler;
pub mod server;
mod signal;
pub mod task;

use thiserror::Error;

/// Asynq upstream version targeted by this compatibility implementation.
///
/// Reference: Asynq v0.26.0 internal `base.ASYNQ_VERSION`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/base/base.go#L22-L23>.
pub const ASYNQ_VERSION: &str = "0.26.0";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataError {
    #[error("{0}")]
    Other(String),
}

pub use admin::{
    AdminError, ClusterNode, DEFAULT_LIST_PAGE_NUMBER, DEFAULT_LIST_PAGE_SIZE, DailyStats,
    GroupInfo, GroupStats, Inspector, InspectorError, ListOption, Pagination, QueueInfo,
    QueueStats, SchedulerEnqueueEvent, SchedulerEnqueueEventInfo,
    SchedulerEntry as InspectorSchedulerEntry, SchedulerEntryInfo, ServerInfo, TaskInfo,
    WorkerInfo, WorkerMetadataInfo, page, page_size,
};
pub use aggregation::{
    AggregationError, AggregationGroup, AggregationGroupConfig, AggregationHandler, AggregationSet,
    GroupAggregator, GroupAggregatorFunc, GroupAggregatorHandler,
};
pub use broker::redis::{MakeRedisClientError, RedisRuntimeClient, RedisRuntimeTimeouts};
pub use client::{
    BrokerError, Client, ClientEnqueueScope, ClientError, EnqueueResult, RedisBackedClient,
    SystemClock, UuidTaskIdGenerator,
};
pub use operation::{
    ArchiveError, CancelError, CleanupError, CompleteError, DEFAULT_MAX_RETRY, DEFAULT_TIMEOUT,
    DequeueError, DequeuedTask, EnqueueOptions, EnqueuePlan, EnqueuePlanError, ForwardError,
    LeaseError, LeaseExtension, NO_DEADLINE, NO_TIMEOUT, RecoverError, RecoverResult, RequeueError,
    ResultError, ResultWriter, RetryError,
};
pub use periodic::{
    DEFAULT_PERIODIC_TASK_MANAGER_SYNC_INTERVAL, PeriodicTaskConfig, PeriodicTaskConfigProvider,
    PeriodicTaskConfigProviderError, PeriodicTaskManager, PeriodicTaskManagerError,
    PeriodicTaskManagerHandle, PeriodicTaskManagerRun, RedisBackedPeriodicTaskManager,
};
pub use processing::{
    DefaultIsFailure, DefaultRetryDelay, ErrorHandler, ErrorHandlerFunc, ExtendLeaseBeforeProcess,
    ExtendLeaseWhileProcessing, Handler, HandlerError, HandlerFunc, IsFailure, IsFailureFunc,
    LeaseExtender, NoopErrorHandler, NoopLeaseExtender, NotFoundHandler, RetryDelay,
    RetryDelayFunc, ServeMux, ServeMuxMatchedHandler, SharedErrorHandler, SharedIsFailure,
    SharedRetryDelay, TaskHandlerFunc, TaskMiddleware, TaskMiddlewareFn, TaskMiddlewareHooks,
    default_is_failure_func, default_retry_delay_func, is_handler_not_found_error,
    is_lease_expired_error, is_panic_error, is_revoke_task_error, is_skip_retry_error,
    lease_expired_error, not_found, not_found_handler, task_middleware_fn, task_middleware_hooks,
};
pub use scheduler::{
    DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL, DEFAULT_SCHEDULER_METADATA_TTL,
    DEFAULT_SCHEDULER_TICK_INTERVAL, DEFAULT_SCHEDULER_TIMEZONE, RedisBackedScheduler, Scheduler,
    SchedulerConstructionError, SchedulerEnqueueErrorHook, SchedulerEnqueueHook,
    SchedulerEnqueuePlanErrorHook, SchedulerEntry, SchedulerError, SchedulerHandle, SchedulerOpts,
    SchedulerPostEnqueueHook, SchedulerRun,
};
pub use server::{
    Config, ConfigBuildError, ConfigBuilder, DEFAULT_SERVER_FORWARDER_INTERVAL,
    DEFAULT_SERVER_GROUP_GRACE_PERIOD, DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
    DEFAULT_SERVER_IDLE_SLEEP, DEFAULT_SERVER_JANITOR_INTERVAL,
    DEFAULT_SERVER_MAINTENANCE_INTERVAL, DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL,
    DEFAULT_SERVER_METADATA_TTL, DEFAULT_SERVER_RECOVER_RETRY_DELAY,
    DEFAULT_SERVER_RECOVERER_INTERVAL, DEFAULT_SERVER_SHUTDOWN_TIMEOUT,
    DEFAULT_SERVER_SYNCER_INTERVAL, HealthCheckFunc, HealthCheckHandler, LogLevel, Logger,
    ParseLogLevelError, ProcessingError, QueueConfig, QueuePriority, QueueSelector,
    RedisBackedServer, RedisBackedServerBuilder, ServerAggregationConfig, ServerCanceller,
    ServerConstructionError, ServerError, ServerHandle, ServerMetadata, ServerProcessingScope,
    ServerRunSummary, default_queue_config,
};
pub use task::{
    DEFAULT_QUEUE_NAME, GroupName, NonBlankNameError, ParseTaskStateError, ProcessingContext,
    ProcessingScope, QueueName, QueueNameError, Task, TaskId, TaskMetadata, TaskState, TaskType,
};

/// Common imports for applications using the public task queue API.
pub mod prelude {
    pub use crate::{
        BrokerError, ClientError, Config, ConfigBuildError, ConfigBuilder, EnqueueOptions,
        EnqueueResult, GroupAggregator, GroupAggregatorFunc, GroupName, Handler, HandlerError,
        HealthCheckFunc, Inspector, InspectorError, LogLevel, PeriodicTaskConfig,
        PeriodicTaskConfigProvider, PeriodicTaskConfigProviderError, PeriodicTaskManager,
        PeriodicTaskManagerError, ProcessingContext, ProcessingScope, QueueInfo, QueueName,
        RedisBackedClient, RedisBackedPeriodicTaskManager, RedisBackedScheduler,
        RedisBackedServerBuilder, Scheduler, SchedulerEnqueueEventInfo, SchedulerEntry,
        SchedulerEntryInfo, SchedulerOpts, ServeMux, ServerError, ServerHandle, ServerInfo,
        ServerProcessingScope, Task, TaskId, TaskInfo, TaskMetadata, TaskMiddlewareHooks,
        TaskState, TaskType, WorkerInfo, task_middleware_hooks,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_target_asynq_version() {
        assert_eq!(ASYNQ_VERSION, "0.26.0");
    }

    #[test]
    fn exposes_scheduler_default_timezone() {
        assert_eq!(DEFAULT_SCHEDULER_TIMEZONE, chrono_tz::UTC);
    }

    #[test]
    fn exposes_scheduler_broker_as_module_extension_point() {
        let path = std::any::type_name::<&dyn scheduler::SchedulerBroker>();

        assert!(path.contains("SchedulerBroker"));
    }

    #[test]
    fn prelude_exposes_preferred_server_workflow_types() {
        fn accepts_server_workflow_types(
            _builder: std::option::Option<prelude::RedisBackedServerBuilder>,
            _mux: std::option::Option<prelude::ServeMux>,
            _handle: std::option::Option<prelude::ServerHandle>,
        ) {
        }

        accepts_server_workflow_types(None, None, None);
    }

    #[test]
    fn prelude_exposes_preferred_aggregation_workflow_types() {
        type TestAggregator = prelude::GroupAggregatorFunc<fn(&str, Vec<Task>) -> Task>;

        fn accepts_aggregation_workflow_types(_aggregator: std::option::Option<TestAggregator>) {}

        accepts_aggregation_workflow_types(None);
    }

    #[test]
    fn prelude_exposes_preferred_middleware_hook_workflow_types() {
        fn accepts_middleware_hook_workflow_types<B, A>(
            _hooks: std::option::Option<prelude::TaskMiddlewareHooks<B, A>>,
            _factory: fn(B, A) -> prelude::TaskMiddlewareHooks<B, A>,
        ) {
        }

        accepts_middleware_hook_workflow_types::<
            fn(&Task, &ProcessingContext) -> Result<(), HandlerError>,
            fn(&Task, &ProcessingContext, Result<(), HandlerError>) -> Result<(), HandlerError>,
        >(None, prelude::task_middleware_hooks);
    }

    #[test]
    fn prelude_exposes_preferred_processing_scope_workflow_types() {
        fn accepts_processing_scope_workflow_types(
            _scope: std::option::Option<prelude::ProcessingScope>,
            _callback: std::option::Option<&prelude::ServerProcessingScope>,
            _metadata: std::option::Option<prelude::TaskMetadata>,
        ) {
        }

        accepts_processing_scope_workflow_types(None, None, None);
    }

    #[test]
    fn prelude_exposes_preferred_periodic_workflow_types() {
        fn accepts_periodic_workflow_types<P>(
            _redis_manager: std::option::Option<prelude::RedisBackedPeriodicTaskManager<P>>,
            _manager: std::option::Option<prelude::PeriodicTaskManager<P, (), ()>>,
            _config: std::option::Option<prelude::PeriodicTaskConfig>,
            _provider_error: std::option::Option<prelude::PeriodicTaskConfigProviderError>,
            _manager_error: std::option::Option<prelude::PeriodicTaskManagerError>,
        ) {
        }

        accepts_periodic_workflow_types::<
            fn() -> Result<
                Vec<prelude::PeriodicTaskConfig>,
                prelude::PeriodicTaskConfigProviderError,
            >,
        >(None, None, None, None, None);

        fn accepts_provider<P: prelude::PeriodicTaskConfigProvider>(_provider: Option<P>) {}
        accepts_provider::<
            fn() -> Result<
                Vec<prelude::PeriodicTaskConfig>,
                prelude::PeriodicTaskConfigProviderError,
            >,
        >(None);
    }

    #[test]
    fn prelude_exposes_preferred_inspector_read_models() {
        fn accepts_inspector_read_models(
            _queue: std::option::Option<prelude::QueueInfo>,
            _task: std::option::Option<prelude::TaskInfo>,
            _server: std::option::Option<prelude::ServerInfo>,
            _worker: std::option::Option<prelude::WorkerInfo>,
            _scheduler_entry: std::option::Option<prelude::SchedulerEntryInfo>,
            _scheduler_event: std::option::Option<prelude::SchedulerEnqueueEventInfo>,
        ) {
        }

        accepts_inspector_read_models(None, None, None, None, None, None);
    }

    #[test]
    fn config_exposes_shared_group_aggregator_accessor() {
        fn accepts_shared_group_aggregator(
            _aggregator: std::option::Option<&server::SharedGroupAggregator>,
        ) {
        }

        let config = Config::default();
        accepts_shared_group_aggregator(config.group_aggregator());
    }

    #[test]
    fn exposes_redis_backed_periodic_task_manager_type() {
        fn assert_type<P>(_manager: std::option::Option<RedisBackedPeriodicTaskManager<P>>) {}

        assert_type::<fn() -> Result<Vec<PeriodicTaskConfig>, PeriodicTaskConfigProviderError>>(
            None,
        );
    }
}
