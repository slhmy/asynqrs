//! Server worker and runtime boundary traits.
//!
//! Reference: Asynq v0.26.0 `Server` coordinates processor work, maintenance,
//! shutdown, and heartbeat lifecycle components:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::task::TaskMessage;
use crate::{
    BrokerError, DequeueError, DequeuedTask, HandlerError, LeaseError, LeaseExtension,
    MetadataError, ProcessingContext, ProcessingError,
};

use super::runtime::idle_poll_sleep_duration;
use super::{
    PendingSyncOperation, ProcessingLease, QueueSelector, ServerError, ServerMaintenanceRun,
    ServerMetadata, ServerRunSummary, Sleeper, WorkerRun,
};

/// Broker connection lifecycle required by server runtime controls.
///
/// This stays separate from worker task execution so ping/close behavior can be
/// tested and owned by the server without exposing a public processor adapter.
#[async_trait]
pub(crate) trait ServerConnection {
    async fn ping(&mut self) -> Result<(), String>;

    fn close(&mut self) -> Result<(), BrokerError>;
}

/// Clock boundary used by server-owned lifecycle components.
///
/// Worker task deadlines use their own worker-handle clock path; this trait is
/// for runtime metadata, maintenance, and shutdown decisions owned by `Server`.
pub(crate) trait ServerClock {
    fn runtime_now(&self) -> SystemTime;
}

/// Applies pending lifecycle sync writes during server shutdown.
///
/// This is implemented by worker handles and the assembly so the server can
/// flush shared pending sync state without knowing the concrete broker type.
#[async_trait]
pub(crate) trait ServerSyncStore {
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()>;
}

/// Metadata persistence boundary for server heartbeat and shutdown cleanup.
///
/// The server owns when metadata is written or cleared; the runtime owner
/// supplies only the broker operation needed for that lifecycle step.
#[async_trait]
pub(crate) trait ServerHeartbeatStore {
    async fn write_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError>;

    async fn clear_server_metadata(
        &mut self,
        metadata: &ServerMetadata,
    ) -> Result<(), MetadataError>;
}

/// Lease extension boundary for server-owned active-worker heartbeats.
///
/// Active worker state lives in `ServerRuntimeState`; this trait only performs
/// the Redis lease write for the currently selected queue batch.
#[async_trait]
pub(crate) trait ServerLeaseExtender {
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError>;
}

/// Maintenance passes owned by the server runtime.
///
/// Forwarder, recoverer, and janitor scheduling lives in server code; the
/// runtime owner provides the concrete broker-backed pass implementations.
#[async_trait]
pub(crate) trait ServerMaintenanceRunner {
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError>;

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError>;

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError>;
}

/// Attaches server-owned runtime state to a concrete runtime owner.
///
/// Rust-native note: production config application lives on concrete runtime
/// components such as `WorkerAssembly`; this narrow boundary exists so the
/// generic server constructor can share one `ServerRuntimeState` with runtime
/// owners and focused test doubles.
pub(crate) trait ServerRuntimeStateAttach {
    fn attach_server_runtime(&mut self, runtime: &super::ServerRuntimeState);
}

/// Graceful shutdown boundary for runtime-owned active work.
///
/// `Server` decides when shutdown happens; the runtime owner drains pending
/// sync state and requeues any active worker state it still owns.
#[async_trait]
pub(crate) trait ServerShutdown {
    async fn shutdown(&mut self) -> Result<(), ProcessingError>;
}

/// Runs at most one task for a server worker loop.
///
/// This is the narrow boundary used by tests and by `Worker` so the server
/// runtime does not need to know whether a worker handle is owned or borrowed.
#[async_trait]
pub(crate) trait WorkerTaskRunner {
    async fn run_task_once(&mut self, queues: &[String]) -> Result<WorkerRun, ProcessingError>;
}

pub(crate) trait WorkerFactory {
    type WorkerHandle: WorkerTaskRunner + ServerShutdown + Send + 'static;

    fn worker(&self) -> Worker<Self::WorkerHandle>;
}

/// Builds a borrowed single-worker handle without requiring cloneable handlers.
pub(crate) trait BorrowedWorkerFactory {
    type WorkerHandle<'a>: WorkerTaskRunner + ServerShutdown + Send + 'a
    where
        Self: 'a;

    fn borrowed_worker(&mut self) -> Worker<Self::WorkerHandle<'_>>;
}

/// Lifecycle operations required to execute one dequeued worker task.
///
/// Implemented by concrete worker handles; `Worker` owns the ordering of these
/// calls so the lifecycle state stays server-owned.
#[async_trait]
pub(crate) trait WorkerTaskLifecycle {
    async fn drain_pending_sync_once(&mut self);

    async fn dequeue(&mut self, queues: &[String]) -> Result<DequeuedTask, DequeueError>;

    fn record_empty_dequeue(&mut self);

    fn record_dequeue_error(&mut self, error: &DequeueError);

    fn runtime_now(&self) -> SystemTime;

    fn task_deadline(
        &self,
        message: &TaskMessage,
        now: SystemTime,
    ) -> Result<Option<SystemTime>, ProcessingError>;

    fn start_active_task(
        &mut self,
        message: &TaskMessage,
        started_at: SystemTime,
        deadline: Option<SystemTime>,
        lease: ProcessingLease,
    );

    async fn before_process(
        &mut self,
        message: &TaskMessage,
        lease: &ProcessingLease,
    ) -> Result<(), ProcessingError>;

    fn register_cancellation(&mut self, task_id: &str) -> CancellationToken;

    fn unregister_cancellation(&mut self, task_id: &str);

    fn finish_active_task(&mut self, task_id: &str);

    async fn execute_active_task(
        &mut self,
        task: WorkerActiveTask,
    ) -> Result<WorkerHandlerResult, ProcessingError>;
}

pub(crate) struct WorkerActiveTask {
    pub(crate) message: TaskMessage,
    pub(crate) task_id: String,
    pub(crate) lease: ProcessingLease,
    pub(crate) started_at: SystemTime,
    pub(crate) deadline: Option<SystemTime>,
    pub(crate) cancellation: CancellationToken,
}

pub(crate) struct WorkerTaskReport {
    pub(crate) message: TaskMessage,
    pub(crate) task_id: String,
    pub(crate) lease: ProcessingLease,
    pub(crate) context: ProcessingContext,
}

pub(crate) enum WorkerHandlerResult {
    Succeeded(WorkerTaskReport),
    Failed {
        report: WorkerTaskReport,
        error: HandlerError,
    },
}

#[async_trait]
pub(crate) trait WorkerReporter {
    async fn report_success(
        &mut self,
        report: WorkerTaskReport,
    ) -> Result<WorkerRun, ProcessingError>;

    async fn report_failure(
        &mut self,
        report: WorkerTaskReport,
        error: HandlerError,
    ) -> Result<WorkerRun, ProcessingError>;
}

/// Explicit worker task owned by the server worker pool.
///
/// Reference: Asynq v0.26.0 `Server` owns worker goroutines while processor
/// execution handles one dequeued task at a time:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721> and
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L143-L381>.
///
/// Rust-native note: `Worker` owns the worker-loop execution boundary. Runtime
/// worker handles implement `WorkerTaskLifecycle` plus `WorkerReporter`;
/// `WorkerTaskRunner` is the narrow one-task boundary used by `Worker`.
pub(crate) struct Worker<P> {
    handle: P,
}

impl<P> Worker<P> {
    pub(crate) fn new(handle: P) -> Self {
        Self { handle }
    }

    pub(crate) fn into_handle(self) -> P {
        self.handle
    }

    pub(crate) fn handle_mut(&mut self) -> &mut P {
        &mut self.handle
    }

    pub(crate) async fn run_once(&mut self, queues: &[String]) -> Result<WorkerRun, ProcessingError>
    where
        P: WorkerTaskRunner + Send,
    {
        self.handle.run_task_once(queues).await
    }
}

pub(crate) async fn run_worker_lifecycle_once<P>(
    lifecycle: &mut P,
    queues: &[String],
) -> Result<WorkerRun, ProcessingError>
where
    P: WorkerTaskLifecycle + WorkerReporter + Send,
{
    lifecycle.drain_pending_sync_once().await;

    let dequeued = match lifecycle.dequeue(queues).await {
        Ok(task) => task,
        Err(DequeueError::NoProcessableTask) => {
            lifecycle.record_empty_dequeue();
            return Ok(WorkerRun::NoProcessableTask);
        }
        Err(error) => {
            lifecycle.record_dequeue_error(&error);
            return Ok(WorkerRun::NoProcessableTask);
        }
    };

    let message = dequeued.message().clone();
    let task_id = message.id.clone();
    let lease = ProcessingLease::new(dequeued.lease_expires_at());
    let started_at = lifecycle.runtime_now();
    let deadline = lifecycle.task_deadline(&message, started_at)?;
    lifecycle.start_active_task(&message, started_at, deadline, lease.clone());
    if let Err(error) = lifecycle.before_process(&message, &lease).await {
        lifecycle.finish_active_task(&task_id);
        return Err(error);
    }
    let cancellation = lifecycle.register_cancellation(&task_id);
    let handler_result = lifecycle
        .execute_active_task(WorkerActiveTask {
            message,
            task_id: task_id.clone(),
            lease,
            started_at,
            deadline,
            cancellation,
        })
        .await;
    let result = match handler_result {
        Ok(WorkerHandlerResult::Succeeded(report)) => lifecycle.report_success(report).await,
        Ok(WorkerHandlerResult::Failed { report, error }) => {
            lifecycle.report_failure(report, error).await
        }
        Err(error) => Err(error),
    };
    lifecycle.unregister_cancellation(&task_id);
    lifecycle.finish_active_task(&task_id);
    result
}

#[async_trait]
impl<P> WorkerReporter for &mut P
where
    P: WorkerReporter + Send,
{
    async fn report_success(
        &mut self,
        report: WorkerTaskReport,
    ) -> Result<WorkerRun, ProcessingError> {
        (**self).report_success(report).await
    }

    async fn report_failure(
        &mut self,
        report: WorkerTaskReport,
        error: HandlerError,
    ) -> Result<WorkerRun, ProcessingError> {
        (**self).report_failure(report, error).await
    }
}

#[async_trait]
impl<P> WorkerTaskRunner for P
where
    P: WorkerTaskLifecycle + WorkerReporter + Send,
{
    async fn run_task_once(&mut self, queues: &[String]) -> Result<WorkerRun, ProcessingError> {
        run_worker_lifecycle_once(self, queues).await
    }
}

/// Worker-loop drain result handed back to the server-owned worker pool.
///
/// Reference: Asynq v0.26.0 `Server.Shutdown` waits for processor workers to
/// drain, then the shutdown path requeues unfinished active work:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L756>
/// and <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L125-L141>.
pub(super) struct WorkerDrain<P> {
    worker: Worker<P>,
    summary: ServerRunSummary,
    error: Option<ServerError>,
}

impl<P> WorkerDrain<P> {
    pub(super) fn summary(&self) -> &ServerRunSummary {
        &self.summary
    }

    pub(super) fn error(&mut self) -> Option<ServerError> {
        self.error.take()
    }

    pub(super) fn into_handle(self) -> P {
        self.worker.into_handle()
    }
}

pub(super) struct WorkerLoopContext<'a, S> {
    pub(super) queue_selector: &'a mut QueueSelector,
    pub(super) sleeper: &'a mut S,
    pub(super) stop: watch::Receiver<bool>,
    pub(super) shutdown: watch::Receiver<bool>,
    pub(super) stopped: Option<mpsc::UnboundedSender<()>>,
}

pub(super) struct WorkerLoopTiming {
    pub(super) idle_sleep: Duration,
    pub(super) shutdown_timeout: Duration,
}

pub(super) async fn run_worker_loop_with_stop<P, S>(
    mut worker: Worker<P>,
    context: WorkerLoopContext<'_, S>,
    timing: WorkerLoopTiming,
) -> WorkerDrain<P>
where
    P: WorkerTaskRunner + Send,
    S: Sleeper + Send,
{
    let WorkerLoopContext {
        queue_selector,
        sleeper,
        mut stop,
        mut shutdown,
        stopped,
    } = context;
    let WorkerLoopTiming {
        idle_sleep,
        shutdown_timeout,
    } = timing;
    let mut summary = ServerRunSummary::default();
    let mut first_error = None;
    while !*shutdown.borrow() && !*stop.borrow() {
        let selected_queues = queue_selector.select();
        let run_once = worker.run_once(&selected_queues);
        tokio::pin!(run_once);
        let run = tokio::select! {
            run = &mut run_once => match run {
                Ok(run) => run,
                Err(error) => {
                    first_error = Some(ServerError::from(error));
                    break;
                }
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    // Reference: Asynq v0.26.0 `Server.Shutdown` waits up to
                    // `Config.ShutdownTimeout` for active workers before
                    // aborting and requeueing unfinished work through
                    // server-owned shutdown:
                    // <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L756>.
                    match tokio::time::timeout(shutdown_timeout, &mut run_once).await {
                        Ok(Ok(run)) => run,
                        Ok(Err(error)) => {
                            first_error = Some(ServerError::from(error));
                            break;
                        }
                        Err(_) => break,
                    }
                } else {
                    continue;
                }
            }
        };
        match run {
            WorkerRun::NoProcessableTask => {
                summary.record_idle_poll();
                tokio::select! {
                    _ = sleeper.sleep(idle_poll_sleep_duration(idle_sleep)) => {}
                    changed = stop.changed() => {
                        if changed.is_err() || *stop.borrow() {
                            break;
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                }
            }
            result => summary.record(result),
        }
    }
    if *stop.borrow() && !*shutdown.borrow() {
        if let Some(stopped) = stopped {
            let _ = stopped.send(());
        }
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
    }
    WorkerDrain {
        worker,
        summary,
        error: first_error,
    }
}
