//! Worker lifecycle adapters for owned and borrowed server worker handles.
//!
//! Reference: Asynq v0.26.0 `Server` wires `Config` into the processor and
//! delegates worker, maintenance, metadata, shutdown, and ping behavior through
//! processor lifecycle components:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L449-L603>.

use async_trait::async_trait;

use super::{
    BorrowedWorkerFactory, ServerRuntimeState, ServerRuntimeStateAttach, ServerShutdown,
    ServerSyncStore, Worker, WorkerActiveTask, WorkerAssembly, WorkerFactory, WorkerHandlerResult,
    WorkerParts, WorkerReporter, WorkerRuntimeParts, WorkerTaskLifecycle, WorkerTaskReport,
    shutdown,
};
use crate::task::TaskMessage;
use crate::{Config, DequeueError, DequeuedTask, HandlerError};

use crate::processing::{
    ErrorHandler, Handler, IsFailure, LeaseExtender, ProcessingError, ProcessingLease, RetryDelay,
    TaskExecutor,
};
use crate::server::{
    ArchiveBroker, LeaseBroker, PROCESSING_DEQUEUE_ERROR_LOG_INTERVAL, PendingSyncDrainPolicy,
    PendingSyncOperation, RequeueBroker, RetryBroker, WorkerBrokerCore, WorkerRun,
    log_processing_debug, log_processing_error, worker_task_deadline,
};

/// Worker hot-path state produced by the server-owned worker assembly.
///
/// Reference: Asynq v0.26.0 processor workers own active task execution and
/// shutdown requeue behavior inside the upstream processor loop:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L143-L390>.
///
/// Rust-native note: parallel server workers run this handle with only the
/// fields needed for the worker loop, keeping maintenance and server
/// capability state out of the spawned worker task.
#[derive(Clone)]
pub(crate) struct OwnedWorkerHandle<B, H, R, C, I, E, L> {
    broker: B,
    executor: TaskExecutor<H, R, I, E, L>,
    clock: C,
    active_task_id: Option<String>,
    runtime: crate::server::ServerRuntimeState,
    log_level: crate::server::LogLevel,
    logger: Option<std::sync::Arc<dyn crate::server::Logger>>,
    last_dequeue_error_log_at: Option<std::time::SystemTime>,
}

/// Borrowed worker hot-path state for single-worker server runs.
///
/// Reference: Asynq v0.26.0 `Server.Run` drives the processor loop directly
/// without requiring handler values to be cloneable:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L663-L721>.
///
/// Rust-native note: this keeps single-worker execution behind `Worker`
/// without forcing public handlers to implement `Clone`.
pub(crate) struct BorrowedWorkerHandle<'a, B, H, R, C, I, E, L> {
    broker: &'a mut B,
    executor: &'a mut TaskExecutor<H, R, I, E, L>,
    clock: &'a C,
    active_task_id: &'a mut Option<String>,
    runtime: &'a crate::server::ServerRuntimeState,
    log_level: crate::server::LogLevel,
    logger: &'a Option<std::sync::Arc<dyn crate::server::Logger>>,
    last_dequeue_error_log_at: &'a mut Option<std::time::SystemTime>,
}

impl<B, H, R, C, I, E, L> OwnedWorkerHandle<B, H, R, C, I, E, L> {
    pub(crate) fn from_parts(parts: WorkerParts<B, H, R, C, I, E, L>) -> Self {
        Self {
            broker: parts.broker,
            executor: parts.executor,
            clock: parts.clock,
            active_task_id: None,
            runtime: parts.runtime,
            log_level: parts.log_level,
            logger: parts.logger,
            last_dequeue_error_log_at: None,
        }
    }

    fn worker_runtime_parts(&mut self) -> WorkerRuntimeParts<'_, B, H, R, C, I, E, L> {
        WorkerRuntimeParts::new(
            &mut self.broker,
            &mut self.executor,
            &self.clock,
            &self.runtime,
            &self.logger,
            self.log_level,
        )
    }

    fn start_active_worker(
        &mut self,
        message: &TaskMessage,
        started_at: std::time::SystemTime,
        deadline: Option<std::time::SystemTime>,
        lease: ProcessingLease,
    ) {
        self.active_task_id = Some(message.id.clone());
        self.runtime
            .insert_active_worker(message.clone(), started_at, deadline, lease);
    }

    fn finish_active_worker(
        &mut self,
        task_id: &str,
    ) -> Option<crate::server::ActiveWorkerSnapshot> {
        if self.active_task_id.as_deref() == Some(task_id) {
            self.active_task_id = None;
        }
        self.runtime.remove_active_worker(task_id)
    }

    fn take_active_worker(&mut self) -> Option<crate::server::ActiveWorkerSnapshot> {
        let task_id = self.active_task_id.take()?;
        self.runtime.remove_active_worker(&task_id)
    }
}

impl<'a, B, H, R, C, I, E, L> BorrowedWorkerHandle<'a, B, H, R, C, I, E, L> {
    pub(crate) fn from_parts(
        parts: crate::server::BorrowedWorkerParts<'a, B, H, R, C, I, E, L>,
    ) -> Self {
        Self {
            broker: parts.broker,
            executor: parts.executor,
            clock: parts.clock,
            active_task_id: parts.active_task_id,
            runtime: parts.runtime,
            log_level: parts.log_level,
            logger: parts.logger,
            last_dequeue_error_log_at: parts.last_dequeue_error_log_at,
        }
    }

    fn worker_runtime_parts(&mut self) -> WorkerRuntimeParts<'_, B, H, R, C, I, E, L> {
        WorkerRuntimeParts::new(
            self.broker,
            self.executor,
            self.clock,
            self.runtime,
            self.logger,
            self.log_level,
        )
    }

    fn start_active_worker(
        &mut self,
        message: &TaskMessage,
        started_at: std::time::SystemTime,
        deadline: Option<std::time::SystemTime>,
        lease: ProcessingLease,
    ) {
        *self.active_task_id = Some(message.id.clone());
        self.runtime
            .insert_active_worker(message.clone(), started_at, deadline, lease);
    }

    fn finish_active_worker(
        &mut self,
        task_id: &str,
    ) -> Option<crate::server::ActiveWorkerSnapshot> {
        if self.active_task_id.as_deref() == Some(task_id) {
            *self.active_task_id = None;
        }
        self.runtime.remove_active_worker(task_id)
    }

    fn take_active_worker(&mut self) -> Option<crate::server::ActiveWorkerSnapshot> {
        let task_id = self.active_task_id.take()?;
        self.runtime.remove_active_worker(&task_id)
    }
}

impl<B, H, R, C, I, E, L> BorrowedWorkerFactory for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + RetryBroker + ArchiveBroker + RequeueBroker + LeaseBroker + Send,
    H: Handler + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
    L: LeaseExtender<B> + Send,
{
    type WorkerHandle<'a>
        = BorrowedWorkerHandle<'a, B, H, R, C, I, E, L>
    where
        Self: 'a;

    fn borrowed_worker(&mut self) -> Worker<Self::WorkerHandle<'_>> {
        Worker::new(BorrowedWorkerHandle::from_parts(
            self.borrowed_worker_parts(),
        ))
    }
}

impl<B, H, R, C, I, E, L> WorkerFactory for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: Clone
        + WorkerBrokerCore
        + RetryBroker
        + ArchiveBroker
        + RequeueBroker
        + LeaseBroker
        + Send
        + 'static,
    TaskExecutor<H, R, I, E, L>: Clone,
    H: Handler + Send + 'static,
    R: RetryDelay + Send + 'static,
    C: crate::client::Clock + Clone + Send + Sync + 'static,
    I: IsFailure + Send + 'static,
    E: ErrorHandler + Send + 'static,
    L: LeaseExtender<B> + Send + 'static,
{
    type WorkerHandle = OwnedWorkerHandle<B, H, R, C, I, E, L>;

    fn worker(&self) -> Worker<Self::WorkerHandle> {
        Worker::new(OwnedWorkerHandle::from_parts(self.worker_parts()))
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> WorkerTaskLifecycle for OwnedWorkerHandle<B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + RetryBroker + ArchiveBroker + RequeueBroker + LeaseBroker + Send,
    H: Handler + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
    L: LeaseExtender<B> + Send,
{
    async fn drain_pending_sync_once(&mut self) {
        let pending_sync = self.runtime.pending_sync();
        pending_sync
            .drain(
                &mut self.broker,
                || self.clock.now(),
                PendingSyncDrainPolicy::Periodic,
                &self.logger,
                self.log_level,
            )
            .await;
    }

    async fn dequeue(&mut self, queues: &[String]) -> Result<DequeuedTask, DequeueError> {
        WorkerBrokerCore::dequeue(&mut self.broker, queues).await
    }

    fn record_empty_dequeue(&mut self) {
        // Reference: Asynq v0.26.0 logs empty queue polls at debug level before
        // sleeping and continuing the upstream processor loop:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L158-L164>.
        log_processing_debug(
            &self.logger,
            self.log_level,
            format_args!("All queues are empty"),
        );
    }

    fn record_dequeue_error(&mut self, error: &DequeueError) {
        // Reference: Asynq v0.26.0 processor logs dequeue errors, releases its
        // worker token, and continues polling instead of stopping the
        // upstream processor loop:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L165-L170>.
        let now = self.clock.now();
        let should_log = self.last_dequeue_error_log_at.is_none_or(|last| {
            now.duration_since(last)
                .is_ok_and(|elapsed| elapsed >= PROCESSING_DEQUEUE_ERROR_LOG_INTERVAL)
        });
        if should_log {
            self.last_dequeue_error_log_at = Some(now);
            log_processing_error(
                &self.logger,
                self.log_level,
                format_args!("Dequeue error: {error}"),
            );
        }
    }

    fn runtime_now(&self) -> std::time::SystemTime {
        self.clock.now()
    }

    fn task_deadline(
        &self,
        message: &TaskMessage,
        now: std::time::SystemTime,
    ) -> Result<Option<std::time::SystemTime>, ProcessingError> {
        worker_task_deadline(message, now)
    }

    fn start_active_task(
        &mut self,
        message: &TaskMessage,
        started_at: std::time::SystemTime,
        deadline: Option<std::time::SystemTime>,
        lease: ProcessingLease,
    ) {
        self.start_active_worker(message, started_at, deadline, lease);
    }

    async fn before_process(
        &mut self,
        message: &TaskMessage,
        lease: &ProcessingLease,
    ) -> Result<(), ProcessingError> {
        self.worker_runtime_parts()
            .before_process(message, lease)
            .await
    }

    fn register_cancellation(&mut self, task_id: &str) -> tokio_util::sync::CancellationToken {
        self.runtime.register_cancellation(task_id)
    }

    fn unregister_cancellation(&mut self, task_id: &str) {
        self.runtime.unregister_cancellation(task_id);
    }

    fn finish_active_task(&mut self, task_id: &str) {
        self.finish_active_worker(task_id);
    }

    async fn execute_active_task(
        &mut self,
        task: WorkerActiveTask,
    ) -> Result<WorkerHandlerResult, ProcessingError> {
        self.worker_runtime_parts().execute_active_task(task).await
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> WorkerReporter for OwnedWorkerHandle<B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + RetryBroker + ArchiveBroker + RequeueBroker + LeaseBroker + Send,
    H: Handler + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
    L: LeaseExtender<B> + Send,
{
    async fn report_success(
        &mut self,
        report: WorkerTaskReport,
    ) -> Result<WorkerRun, ProcessingError> {
        self.worker_runtime_parts().report_success(report).await
    }

    async fn report_failure(
        &mut self,
        report: WorkerTaskReport,
        error: HandlerError,
    ) -> Result<WorkerRun, ProcessingError> {
        self.worker_runtime_parts()
            .report_failure(report, error)
            .await
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerShutdown for OwnedWorkerHandle<B, H, R, C, I, E, L>
where
    B: RequeueBroker + WorkerBrokerCore + Send,
    C: crate::client::Clock + Send + Sync,
    H: Send,
    R: Send,
    I: Send,
    E: Send,
    L: Send,
{
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        let runtime = self.runtime.clone();
        let logger = self.logger.clone();
        let log_level = self.log_level;
        let active_worker = self.take_active_worker();
        shutdown::shutdown_active_worker(
            &mut self.broker,
            &self.clock,
            &runtime,
            &logger,
            log_level,
            active_worker,
        )
        .await?;
        let now = self.clock.now();
        shutdown::drain_pending_sync_before_shutdown(&runtime, self, &logger, log_level, now).await;
        Ok(())
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerSyncStore for OwnedWorkerHandle<B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + Send,
    H: Send,
    R: Send,
    C: Send + Sync,
    I: Send,
    E: Send,
    L: Send,
{
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        shutdown::apply_pending_sync_operation_to_broker(&mut self.broker, operation).await
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> WorkerTaskLifecycle for BorrowedWorkerHandle<'_, B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + RetryBroker + ArchiveBroker + RequeueBroker + LeaseBroker + Send,
    H: Handler + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
    L: LeaseExtender<B> + Send,
{
    async fn drain_pending_sync_once(&mut self) {
        let pending_sync = self.runtime.pending_sync();
        pending_sync
            .drain(
                self.broker,
                || self.clock.now(),
                PendingSyncDrainPolicy::Periodic,
                self.logger,
                self.log_level,
            )
            .await;
    }

    async fn dequeue(&mut self, queues: &[String]) -> Result<DequeuedTask, DequeueError> {
        WorkerBrokerCore::dequeue(self.broker, queues).await
    }

    fn record_empty_dequeue(&mut self) {
        log_processing_debug(
            self.logger,
            self.log_level,
            format_args!("All queues are empty"),
        );
    }

    fn record_dequeue_error(&mut self, error: &DequeueError) {
        let now = self.clock.now();
        let should_log = self.last_dequeue_error_log_at.is_none_or(|last| {
            now.duration_since(last)
                .is_ok_and(|elapsed| elapsed >= PROCESSING_DEQUEUE_ERROR_LOG_INTERVAL)
        });
        if should_log {
            *self.last_dequeue_error_log_at = Some(now);
            log_processing_error(
                self.logger,
                self.log_level,
                format_args!("Dequeue error: {error}"),
            );
        }
    }

    fn runtime_now(&self) -> std::time::SystemTime {
        self.clock.now()
    }

    fn task_deadline(
        &self,
        message: &TaskMessage,
        now: std::time::SystemTime,
    ) -> Result<Option<std::time::SystemTime>, ProcessingError> {
        worker_task_deadline(message, now)
    }

    fn start_active_task(
        &mut self,
        message: &TaskMessage,
        started_at: std::time::SystemTime,
        deadline: Option<std::time::SystemTime>,
        lease: ProcessingLease,
    ) {
        self.start_active_worker(message, started_at, deadline, lease);
    }

    async fn before_process(
        &mut self,
        message: &TaskMessage,
        lease: &ProcessingLease,
    ) -> Result<(), ProcessingError> {
        self.worker_runtime_parts()
            .before_process(message, lease)
            .await
    }

    fn register_cancellation(&mut self, task_id: &str) -> tokio_util::sync::CancellationToken {
        self.runtime.register_cancellation(task_id)
    }

    fn unregister_cancellation(&mut self, task_id: &str) {
        self.runtime.unregister_cancellation(task_id);
    }

    fn finish_active_task(&mut self, task_id: &str) {
        self.finish_active_worker(task_id);
    }

    async fn execute_active_task(
        &mut self,
        task: WorkerActiveTask,
    ) -> Result<WorkerHandlerResult, ProcessingError> {
        self.worker_runtime_parts().execute_active_task(task).await
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> WorkerReporter for BorrowedWorkerHandle<'_, B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + RetryBroker + ArchiveBroker + RequeueBroker + LeaseBroker + Send,
    H: Handler + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
    L: LeaseExtender<B> + Send,
{
    async fn report_success(
        &mut self,
        report: WorkerTaskReport,
    ) -> Result<WorkerRun, ProcessingError> {
        self.worker_runtime_parts().report_success(report).await
    }

    async fn report_failure(
        &mut self,
        report: WorkerTaskReport,
        error: HandlerError,
    ) -> Result<WorkerRun, ProcessingError> {
        self.worker_runtime_parts()
            .report_failure(report, error)
            .await
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerShutdown for BorrowedWorkerHandle<'_, B, H, R, C, I, E, L>
where
    B: RequeueBroker + WorkerBrokerCore + Send,
    C: crate::client::Clock + Send + Sync,
    H: Send,
    R: Send,
    I: Send,
    E: Send,
    L: Send,
{
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        let runtime = self.runtime.clone();
        let logger = (*self.logger).clone();
        let log_level = self.log_level;
        let active_worker = self.take_active_worker();
        shutdown::shutdown_active_worker(
            self.broker,
            self.clock,
            &runtime,
            &logger,
            log_level,
            active_worker,
        )
        .await?;
        let now = self.clock.now();
        shutdown::drain_pending_sync_before_shutdown(&runtime, self, &logger, log_level, now).await;
        Ok(())
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerSyncStore for BorrowedWorkerHandle<'_, B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + Send,
    H: Send,
    R: Send,
    C: Send + Sync,
    I: Send,
    E: Send,
    L: Send,
{
    async fn apply_pending_sync_operation(
        &mut self,
        operation: &PendingSyncOperation,
    ) -> Result<(), ()> {
        shutdown::apply_pending_sync_operation_to_broker(self.broker, operation).await
    }
}

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L>
where
    B: Send,
    H: Handler + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
    L: LeaseExtender<B> + Send,
{
    pub(crate) fn apply_server_config(mut self, config: &Config) -> Self {
        self.executor.retry_delay_override = config.retry_delay_func.clone();
        self.executor.is_failure_override = config.is_failure.clone();
        self.executor.error_handler_override = config.error_handler.clone();
        self.executor.processing_scope = config.processing_scope.clone();
        self.with_cleanup_batch_size(config.effective_janitor_batch_size())
            .with_log_level(config.effective_log_level())
            .with_optional_logger(config.logger.clone())
    }
}

impl<B, H, R, C, I, E, L> ServerRuntimeStateAttach for WorkerAssembly<B, H, R, C, I, E, L> {
    fn attach_server_runtime(&mut self, runtime: &ServerRuntimeState) {
        self.set_runtime(runtime);
    }
}

#[async_trait]
impl<B, H, R, C, I, E, L> ServerShutdown for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: RequeueBroker + WorkerBrokerCore + Send,
    H: Handler + Send,
    R: RetryDelay + Send,
    C: crate::client::Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
    L: LeaseExtender<B> + Send,
{
    async fn shutdown(&mut self) -> Result<(), ProcessingError> {
        let runtime = self.runtime.clone();
        let logger = self.logger.clone();
        let log_level = self.log_level;
        let active_worker = self
            .active_task_id
            .take()
            .and_then(|task_id| runtime.remove_active_worker(&task_id));
        shutdown::shutdown_active_worker(
            &mut self.broker,
            &self.clock,
            &runtime,
            &logger,
            log_level,
            active_worker,
        )
        .await?;
        let now = self.clock.now();
        shutdown::drain_pending_sync_before_shutdown(&runtime, self, &logger, log_level, now).await;
        Ok(())
    }
}
