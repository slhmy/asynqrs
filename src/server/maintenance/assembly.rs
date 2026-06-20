//! Server-owned maintenance execution for worker assemblies.
//!
//! Reference: Asynq v0.26.0 starts forwarder, recoverer, and janitor
//! components from server startup:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L687-L695>.

use std::time::Duration;

use async_trait::async_trait;

use crate::RecoverResult;
use crate::aggregation::AggregationBroker;
use crate::client::Clock;
use crate::processing::{
    IsFailure, ProcessingError, RetryDelay, lease_expired_error, task_from_message,
};

use super::super::logging::{log_error, log_warn};
use super::super::{
    ArchiveBroker, CleanupBroker, ForwardBroker, RecoverBroker, RetryBroker, ServerMaintenanceRun,
    ServerMaintenanceRunner, WorkerAssembly,
};

#[async_trait]
impl<B, H, R, C, I, E, L> ServerMaintenanceRunner for WorkerAssembly<B, H, R, C, I, E, L>
where
    B: ForwardBroker
        + RecoverBroker
        + RetryBroker
        + ArchiveBroker
        + CleanupBroker
        + AggregationBroker
        + Send,
    H: Send,
    R: RetryDelay + Send,
    C: Clock + Send + Sync,
    I: IsFailure + Send,
    E: Send,
    L: Send,
{
    async fn run_forwarder_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        run_forwarder_maintenance(self, queues).await
    }

    async fn run_recoverer_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        run_recoverer_maintenance(self, queues).await
    }

    async fn run_janitor_maintenance(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ProcessingError> {
        run_janitor_maintenance(self, queues).await
    }
}

pub(crate) async fn run_forwarder_maintenance<B, H, R, C, I, E, L>(
    assembly: &mut WorkerAssembly<B, H, R, C, I, E, L>,
    queues: &[String],
) -> Result<ServerMaintenanceRun, ProcessingError>
where
    B: ForwardBroker + Send,
{
    // Reference: Asynq v0.26.0 forwarder delegates the whole queue set to
    // `Broker.ForwardIfReady` and logs failures without stopping future ticks:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/forwarder.go#L61-L65>.
    let run = match assembly.broker.forward_if_ready(queues).await {
        Ok(run) => run,
        Err(error) => {
            log_error(
                &assembly.logger,
                assembly.log_level,
                format_args!("Failed to forward scheduled tasks: {error}"),
            );
            return Ok(ServerMaintenanceRun::default());
        }
    };

    Ok(ServerMaintenanceRun::new(
        run.forwarded_scheduled(),
        run.forwarded_retry(),
        0,
        0,
        0,
    ))
}

pub(crate) async fn run_recoverer_maintenance<B, H, R, C, I, E, L>(
    assembly: &mut WorkerAssembly<B, H, R, C, I, E, L>,
    queues: &[String],
) -> Result<ServerMaintenanceRun, ProcessingError>
where
    B: RecoverBroker + RetryBroker + ArchiveBroker + AggregationBroker + Send,
    R: RetryDelay + Send,
    C: Clock + Send + Sync,
    I: IsFailure + Send,
{
    let mut recovered_retried = 0;
    let mut recovered_archived = 0;

    // Reference: Asynq v0.26.0 recoverer completes one broker-level
    // lease-expired task listing across all queues before starting stale
    // aggregation-set recovery:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L80-L94>.
    match recover_expired_leases(assembly, queues).await {
        Ok(recovered) => {
            recovered_retried += recovered.retried();
            recovered_archived += recovered.archived();
        }
        Err(ProcessingError::Recover(error)) => log_warn(
            &assembly.logger,
            assembly.log_level,
            format_args!("recoverer: could not list lease expired tasks: {error}"),
        ),
        Err(error) => log_warn(
            &assembly.logger,
            assembly.log_level,
            format_args!("recoverer: could not list lease expired tasks: {error}"),
        ),
    }

    for queue in queues {
        // Reference: Asynq v0.26.0 recoverer reclaims stale aggregation sets
        // after lease-expired task recovery for all queues and logs failures
        // per queue without stopping future ticks:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L72-L94>.
        if let Err(error) = AggregationBroker::reclaim_stale_aggregation_sets(
            &mut assembly.broker,
            queue,
            assembly.clock.now(),
        )
        .await
        {
            log_warn(
                &assembly.logger,
                assembly.log_level,
                format_args!(
                    "recoverer: could not reclaim stale aggregation sets in queue {queue:?}: {error}"
                ),
            );
        }
    }

    Ok(ServerMaintenanceRun::new(
        0,
        0,
        recovered_retried,
        recovered_archived,
        0,
    ))
}

async fn recover_expired_leases<B, H, R, C, I, E, L>(
    assembly: &mut WorkerAssembly<B, H, R, C, I, E, L>,
    queues: &[String],
) -> Result<RecoverResult, ProcessingError>
where
    B: RecoverBroker + RetryBroker + ArchiveBroker + Send,
    R: RetryDelay + Send,
    C: Clock + Send + Sync,
    I: IsFailure + Send,
{
    // Reference: Asynq v0.26.0 recoverer lists tasks whose leases expired at
    // least 30 seconds ago to accommodate clock skew:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L73-L80>.
    let cutoff = assembly
        .clock
        .now()
        .checked_sub(Duration::from_secs(30))
        .ok_or(ProcessingError::TimeOverflow("lease expiration cutoff"))?;
    let messages = assembly.broker.list_lease_expired(cutoff, queues).await?;
    let mut retried = 0;
    let mut archived = 0;

    for message in messages {
        // Reference: Asynq v0.26.0 recoverer logs retry/archive write failures
        // and continues recovering other expired leases instead of failing the
        // recoverer loop:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L83-L101>.
        let task = task_from_message(&message);
        // Reference: Asynq v0.26.0 recoverer uses the exported
        // `HandlerError::LeaseExpired` sentinel for retry delay and failure
        // classification:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L68-L70>,
        // <https://github.com/hibiken/asynq/blob/v0.26.0/recoverer.go#L107-L110>.
        let error = lease_expired_error();
        let error_message = error.to_string();
        if message.retried >= message.retry {
            match assembly.broker.archive(&message, &error_message).await {
                Ok(()) => archived += 1,
                Err(error) => log_warn(
                    &assembly.logger,
                    assembly.log_level,
                    format_args!("recoverer: could not move task to archive: {error}"),
                ),
            }
        } else {
            let delay = assembly
                .executor
                .retry_delay_for(message.retried, &error, &task);
            let retry_at = assembly
                .clock
                .now()
                .checked_add(delay)
                .ok_or(ProcessingError::TimeOverflow("recovery retry time"))?;
            let is_failure = assembly.executor.is_failure_for(&error);
            match assembly
                .broker
                .retry(&message, retry_at, &error_message, is_failure)
                .await
            {
                Ok(()) => retried += 1,
                Err(error) => log_warn(
                    &assembly.logger,
                    assembly.log_level,
                    format_args!("recoverer: could not retry lease expired task: {error}"),
                ),
            }
        }
    }

    Ok(RecoverResult::new(retried, archived))
}

pub(crate) async fn run_janitor_maintenance<B, H, R, C, I, E, L>(
    assembly: &mut WorkerAssembly<B, H, R, C, I, E, L>,
    queues: &[String],
) -> Result<ServerMaintenanceRun, ProcessingError>
where
    B: CleanupBroker + Send,
{
    let mut deleted_expired_completed = 0;

    for queue in queues {
        // Reference: Asynq v0.26.0 janitor owns queue iteration, logs cleanup
        // failures per queue, and keeps checking remaining queues:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/janitor.go#L67-L74>.
        match assembly
            .broker
            .delete_expired_completed_tasks(queue, assembly.cleanup_batch_size)
            .await
        {
            Ok(count) => deleted_expired_completed += count,
            Err(error) => log_error(
                &assembly.logger,
                assembly.log_level,
                format_args!(
                    "Failed to delete expired completed tasks from queue {queue:?}: {error}"
                ),
            ),
        }
    }

    Ok(ServerMaintenanceRun::new(
        0,
        0,
        0,
        0,
        deleted_expired_completed,
    ))
}
