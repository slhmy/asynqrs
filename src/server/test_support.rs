use std::time::{Duration, SystemTime};

use tokio::sync::watch;

use crate::AggregationGroupConfig;
use crate::DEFAULT_QUEUE_NAME;
use crate::task::{TaskMessage, validate_queue_name};

use super::defaults::{
    DEFAULT_SERVER_FORWARDER_INTERVAL, DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
    DEFAULT_SERVER_IDLE_SLEEP, DEFAULT_SERVER_JANITOR_INTERVAL,
    DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL, DEFAULT_SERVER_RECOVERER_INTERVAL,
    DEFAULT_SERVER_SHUTDOWN_TIMEOUT, DEFAULT_SERVER_SYNCER_INTERVAL,
};
use super::health::HealthCheckHandler;
use super::metadata::ServerMetadata;
use super::run::runtime::ServerRuntime;
use super::{
    BorrowedWorkerFactory, Config, LogLevel, ProcessingLease, QueuePriority, QueueSelector, Server,
    ServerClock, ServerConnection, ServerError, ServerHeartbeatStore, ServerLeaseExtender,
    ServerMaintenanceRunner, ServerRunSummary, ServerRuntimeState, ServerRuntimeStateAttach,
    ServerShutdown, ServerSyncStore, Sleeper, TokioSleeper, WorkerFactory,
};

pub(crate) fn server_with_sleeper<P, S, I, Q>(
    runtime: P,
    queues: I,
    sleeper: S,
) -> Result<Server<P, S>, ServerError>
where
    P: ServerRuntimeStateAttach,
    I: IntoIterator<Item = Q>,
    Q: Into<String>,
{
    let mut queues: Vec<String> = queues
        .into_iter()
        .map(Into::into)
        .filter(|queue| validate_queue_name(queue).is_ok())
        .collect();
    if queues.is_empty() {
        queues.push(DEFAULT_QUEUE_NAME.to_owned());
    }
    let queue_selector =
        QueueSelector::weighted_priority(queues.iter().cloned().map(|queue| (queue, 1)))?;
    server_with_queue_selector(runtime, queue_selector, sleeper)
}

pub(crate) fn server_with_weighted_queues<P, S, I, Q, R>(
    runtime: P,
    queues: I,
    sleeper: S,
) -> Result<Server<P, S>, ServerError>
where
    P: ServerRuntimeStateAttach,
    I: IntoIterator<Item = (Q, R)>,
    Q: Into<String>,
    R: QueuePriority,
{
    server_with_queue_selector(runtime, QueueSelector::weighted_priority(queues)?, sleeper)
}

pub(crate) fn server_with_strict_priority_queues<P, S, I, Q, R>(
    runtime: P,
    queues: I,
    sleeper: S,
) -> Result<Server<P, S>, ServerError>
where
    P: ServerRuntimeStateAttach,
    I: IntoIterator<Item = (Q, R)>,
    Q: Into<String>,
    R: QueuePriority,
{
    server_with_queue_selector(runtime, QueueSelector::strict_priority(queues)?, sleeper)
}

pub(crate) fn server_with_queue_selector<P, S>(
    mut runtime: P,
    queue_selector: QueueSelector,
    sleeper: S,
) -> Result<Server<P, S>, ServerError>
where
    P: ServerRuntimeStateAttach,
{
    let runtime_state = ServerRuntimeState::default();
    runtime.attach_server_runtime(&runtime_state);
    Ok(server_from_queue_selector_parts(
        runtime,
        queue_selector,
        sleeper,
        runtime_state,
    ))
}

pub(crate) fn server_new<P, I, Q>(runtime: P, queues: I) -> Result<Server<P>, ServerError>
where
    P: ServerRuntimeStateAttach,
    I: IntoIterator<Item = Q>,
    Q: Into<String>,
{
    server_with_sleeper(runtime, queues, TokioSleeper)
}

pub(crate) fn server_new_with_config<P>(
    runtime: P,
    config: Config,
) -> Result<Server<P>, ServerError>
where
    P: ServerRuntimeStateAttach,
{
    Server::with_config(runtime, config, TokioSleeper)
}

pub(crate) fn with_logger<P, S>(
    mut server: Server<P, S>,
    logger: std::sync::Arc<dyn super::Logger>,
) -> Server<P, S> {
    server.logger = Some(logger);
    server
}

pub(crate) fn with_health_check_handler<P, S, H>(
    mut server: Server<P, S>,
    handler: H,
) -> Server<P, S>
where
    H: HealthCheckHandler + 'static,
{
    server.health_check_handler = Some(std::sync::Arc::new(handler));
    server
}

pub(crate) fn aggregation_group_configs<P, S>(
    server: &Server<P, S>,
) -> Result<Vec<AggregationGroupConfig>, ServerError> {
    server
        .aggregation_config
        .map(|config| server.aggregation_group_configs_for(config))
        .unwrap_or_else(|| Ok(Vec::new()))
}

pub(crate) fn runtime_state_with_active_worker(
    message: TaskMessage,
    started_at: SystemTime,
    deadline: Option<SystemTime>,
    lease_expires_at: SystemTime,
) -> ServerRuntimeState {
    let state = ServerRuntimeState::default();
    state.insert_active_worker(
        message,
        started_at,
        deadline,
        ProcessingLease::new(lease_expires_at),
    );
    state
}

pub(crate) fn runtime_state_with_pending_complete(
    message: TaskMessage,
    deadline: SystemTime,
    sync_error_message: String,
) -> ServerRuntimeState {
    let state = ServerRuntimeState::default();
    state
        .pending_sync()
        .enqueue_complete(message, deadline, sync_error_message);
    state
}

pub(crate) async fn run_until_stopped_configured_parallel<P, S>(
    mut server: Server<P, S>,
    shutdown: watch::Receiver<bool>,
) -> Result<ServerRunSummary, ServerError>
where
    P: WorkerFactory
        + ServerConnection
        + ServerHeartbeatStore
        + ServerLeaseExtender
        + ServerMaintenanceRunner
        + ServerClock
        + ServerShutdown
        + ServerSyncStore
        + Clone
        + Send
        + 'static,
    S: Sleeper + Clone + Send + 'static,
{
    let (_stop_tx, stop_rx) = watch::channel(false);
    let (stopped_tx, _stopped_rx) = watch::channel(false);
    server.begin_start()?;
    let worker_count = server.worker_count;
    ServerRuntime::new(server, worker_count, stop_rx, shutdown, stopped_tx)
        .run()
        .await
}

pub(crate) async fn run_until_stopped<P, S>(
    server: &mut Server<P, S>,
    shutdown: watch::Receiver<bool>,
) -> Result<ServerRunSummary, ServerError>
where
    P: BorrowedWorkerFactory
        + ServerConnection
        + ServerHeartbeatStore
        + ServerLeaseExtender
        + ServerMaintenanceRunner
        + ServerClock
        + ServerShutdown
        + ServerSyncStore
        + Send,
    S: Sleeper + Send,
{
    let (_stop_tx, stop) = watch::channel(false);
    server.run_until_stopped_with_stop(stop, shutdown).await
}

pub(crate) async fn run_until_stopped_parallel<P, S>(
    mut server: Server<P, S>,
    worker_count: usize,
    shutdown: watch::Receiver<bool>,
) -> Result<ServerRunSummary, ServerError>
where
    P: WorkerFactory
        + ServerConnection
        + ServerHeartbeatStore
        + ServerLeaseExtender
        + ServerMaintenanceRunner
        + ServerClock
        + ServerShutdown
        + ServerSyncStore
        + Clone
        + Send
        + 'static,
    S: Sleeper + Clone + Send + 'static,
{
    let (_stop_tx, stop_rx) = watch::channel(false);
    let (stopped_tx, _stopped_rx) = watch::channel(false);
    server.begin_start()?;
    ServerRuntime::new(server, worker_count, stop_rx, shutdown, stopped_tx)
        .run()
        .await
}

fn server_from_queue_selector_parts<P, S>(
    runtime: P,
    queue_selector: QueueSelector,
    sleeper: S,
    runtime_state: ServerRuntimeState,
) -> Server<P, S> {
    let queues = queue_selector.queue_names();
    Server {
        runtime,
        worker_count: 1,
        queues: std::sync::Arc::from(queues),
        queue_selector,
        aggregation_config: None,
        idle_sleep: DEFAULT_SERVER_IDLE_SLEEP,
        forwarder_interval: DEFAULT_SERVER_FORWARDER_INTERVAL,
        recoverer_interval: DEFAULT_SERVER_RECOVERER_INTERVAL,
        janitor_interval: DEFAULT_SERVER_JANITOR_INTERVAL,
        syncer_interval: DEFAULT_SERVER_SYNCER_INTERVAL,
        shutdown_timeout: DEFAULT_SERVER_SHUTDOWN_TIMEOUT,
        health_check_interval: DEFAULT_SERVER_HEALTH_CHECK_INTERVAL,
        health_check_handler: None,
        log_level: LogLevel::Info,
        logger: None,
        metadata: None,
        metadata_heartbeat_interval: DEFAULT_SERVER_METADATA_HEARTBEAT_INTERVAL,
        cancellation_listener: None,
        aggregation_runner: None,
        shared_connection: false,
        sleeper,
        runtime_state,
        state: super::ServerState::New,
    }
}

pub(crate) fn with_server_metadata<P, S>(
    mut server: Server<P, S>,
    metadata: ServerMetadata,
) -> Server<P, S> {
    server.metadata = Some(metadata);
    server
}

pub(crate) fn with_metadata_heartbeat_interval<P, S>(
    mut server: Server<P, S>,
    duration: Duration,
) -> Server<P, S> {
    server.metadata_heartbeat_interval = duration;
    server
}

pub(crate) fn with_default_server_metadata<P, S>(server: Server<P, S>) -> Server<P, S> {
    let metadata = ServerMetadata::for_current_process_with_queue_selector(
        &server.queue_selector,
        server.worker_count,
    )
    .expect("server queue selector and worker count are already validated");
    with_server_metadata(server, metadata)
}

pub(crate) fn with_default_server_metadata_for_workers<P, S>(
    server: Server<P, S>,
    worker_count: usize,
) -> Result<Server<P, S>, ServerError> {
    let metadata = ServerMetadata::for_current_process_with_queue_selector(
        &server.queue_selector,
        worker_count,
    )?;
    Ok(with_server_metadata(server, metadata))
}
