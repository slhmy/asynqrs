//! Test-only worker assembly inspection helpers.

use std::time::{Duration, SystemTime};

use crate::client::Clock;
use crate::processing::{ErrorHandler, HandlerError, IsFailure, ProcessingLease, RetryDelay};
use crate::server::{LeaseBroker, PendingSyncDrainPolicy, WorkerBrokerCore, log_processing_error};
use crate::task::TaskMessage;
use crate::{ProcessingContext, ServerCanceller, ServerMetadata, Task};

use super::{ServerRuntimeState, WorkerAssembly};

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L> {
    pub(crate) fn retry_delay_for(
        &mut self,
        retried: i32,
        error: &HandlerError,
        task: &Task,
    ) -> Duration
    where
        R: RetryDelay,
    {
        self.executor.retry_delay_for(retried, error, task)
    }

    pub(crate) fn is_failure_for(&mut self, error: &HandlerError) -> bool
    where
        I: IsFailure,
    {
        self.executor.is_failure_for(error)
    }

    pub(crate) fn canceller(&self) -> ServerCanceller {
        self.runtime_state().canceller()
    }

    pub(crate) fn runtime_state(&self) -> ServerRuntimeState {
        self.runtime.clone()
    }

    pub(crate) fn broker_mut(&mut self) -> &mut B {
        &mut self.broker
    }

    pub(crate) fn pending_sync_count(&self) -> usize {
        self.runtime_state().pending_sync().len()
    }

    pub(crate) fn metadata_snapshot(&self, metadata: &ServerMetadata) -> ServerMetadata {
        self.runtime_state().server_metadata_snapshot(metadata)
    }

    pub(crate) fn start_active_worker(
        &mut self,
        message: &TaskMessage,
        started_at: SystemTime,
        deadline: Option<SystemTime>,
        lease: ProcessingLease,
    ) {
        self.active_task_id = Some(message.id.clone());
        self.runtime
            .insert_active_worker(message.clone(), started_at, deadline, lease);
    }
}

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L>
where
    B: LeaseBroker + Send,
    C: Clock,
{
    pub(crate) async fn extend_active_worker_leases(&mut self) {
        // Reference: Asynq v0.26.0 heartbeater groups valid active workers
        // and extends their Redis leases during each heartbeat:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/heartbeat.go#L170-L201>.
        for batch in self.runtime.active_lease_batches(self.clock.now()) {
            let task_ids = batch.task_ids().to_vec();
            match self.broker.extend_leases(batch.queue(), &task_ids).await {
                Ok(extension) => {
                    batch.reset_leases(extension);
                }
                Err(error) => log_processing_error(
                    &self.logger,
                    self.log_level,
                    format_args!("Failed to extend lease for tasks {task_ids:?}: {error}"),
                ),
            }
        }
    }
}

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + Send,
    C: Clock,
{
    /// Retries pending lifecycle writes once.
    ///
    /// Reference: Asynq v0.26.0 runs a syncer goroutine that retries failed
    /// complete, retry, and archive writes outside the worker result path:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go>.
    pub(crate) async fn run_sync_once(&mut self) {
        self.drain_pending_sync(PendingSyncDrainPolicy::Periodic)
            .await;
    }

    async fn drain_pending_sync(&mut self, policy: PendingSyncDrainPolicy) {
        let pending_sync = self.runtime.pending_sync();
        pending_sync
            .drain(
                &mut self.broker,
                || self.clock.now(),
                policy,
                &self.logger,
                self.log_level,
            )
            .await;
    }
}

impl<B, H, R, C, I, E, L> WorkerAssembly<B, H, R, C, I, E, L>
where
    E: ErrorHandler + Send,
{
    pub(crate) async fn handle_error_for(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        error: &HandlerError,
    ) {
        self.executor.handle_error_for(task, context, error).await;
    }
}
