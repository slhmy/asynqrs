//! Server-owned worker shutdown and active-task requeue behavior.
//!
//! Reference: Asynq v0.26.0 `Server.Shutdown` waits for active workers and
//! upstream shutdown requeues timed-out active work through processor logic:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L723-L756>,
//! <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L239-L266>.

use std::fmt;
use std::sync::Arc;
use std::time::SystemTime;

use crate::ProcessingError;
use crate::client::Clock;

use super::{
    ActiveWorkerSnapshot, LogLevel, Logger, PendingSyncBacklog, PendingSyncDrainPolicy,
    PendingSyncOperation, RequeueBroker, ServerRuntimeState, ServerSyncStore, WorkerBrokerCore,
};

/// Requeues the task owned by a worker that did not finish before shutdown.
///
/// Reference: Asynq v0.26.0 `processor.requeue` skips Redis writes once the
/// active lease is invalid, leaving expired active tasks for recoverer
/// ownership:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L261-L266>.
pub(crate) async fn shutdown_active_worker<B, C>(
    broker: &mut B,
    clock: &C,
    runtime: &ServerRuntimeState,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    active_worker: Option<ActiveWorkerSnapshot>,
) -> Result<(), ProcessingError>
where
    B: RequeueBroker + Send,
    C: Clock,
{
    log_debug(
        logger,
        log_level,
        format_args!("Worker runtime shutting down..."),
    );
    log_info(
        logger,
        log_level,
        format_args!("Waiting for all workers to finish..."),
    );
    if let Some(active_worker) = active_worker {
        let message = active_worker.message;
        runtime.unregister_cancellation(&message.id);
        let lease = active_worker.lease;
        log_warn(
            logger,
            log_level,
            format_args!("Quitting worker. task id={}", message.id),
        );
        if lease.is_valid_at(clock.now()) {
            match broker.requeue(&message).await {
                Ok(()) => log_info(
                    logger,
                    log_level,
                    format_args!("Pushed task id={} back to queue", message.id),
                ),
                Err(error) => log_error(
                    logger,
                    log_level,
                    format_args!(
                        "Could not push task id={} back to queue: {error}",
                        message.id
                    ),
                ),
            }
        }
    }
    log_info(logger, log_level, format_args!("All workers have finished"));
    Ok(())
}

/// Drains lifecycle writes one final time during worker/server shutdown.
///
/// Reference: Asynq v0.26.0 syncer performs one final retry when shutdown is
/// requested and then drops remaining failures:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/syncer.go#L53-L76>.
pub(crate) async fn drain_pending_sync_before_shutdown<S>(
    runtime: &ServerRuntimeState,
    sync_store: &mut S,
    logger: &Option<Arc<dyn Logger>>,
    log_level: LogLevel,
    now: SystemTime,
) where
    S: ServerSyncStore + Send,
{
    runtime
        .drain_pending_sync(
            sync_store,
            PendingSyncDrainPolicy::ShutdownFinal,
            logger,
            log_level,
            now,
        )
        .await;
}

pub(crate) async fn apply_pending_sync_operation_to_broker<B>(
    broker: &mut B,
    operation: &PendingSyncOperation,
) -> Result<(), ()>
where
    B: WorkerBrokerCore + Send,
{
    PendingSyncBacklog::apply_operation(broker, operation).await
}

fn log_debug(logger: &Option<Arc<dyn Logger>>, log_level: LogLevel, args: fmt::Arguments<'_>) {
    if matches!(log_level, LogLevel::Debug) {
        if let Some(logger) = logger {
            logger.debug(args);
        }
    }
}

fn log_info(logger: &Option<Arc<dyn Logger>>, log_level: LogLevel, args: fmt::Arguments<'_>) {
    if matches!(log_level, LogLevel::Debug | LogLevel::Info) {
        if let Some(logger) = logger {
            logger.info(args);
        }
    }
}

fn log_warn(logger: &Option<Arc<dyn Logger>>, log_level: LogLevel, args: fmt::Arguments<'_>) {
    if matches!(log_level, LogLevel::Debug | LogLevel::Info | LogLevel::Warn) {
        if let Some(logger) = logger {
            logger.warn(args);
        }
    }
}

fn log_error(logger: &Option<Arc<dyn Logger>>, log_level: LogLevel, args: fmt::Arguments<'_>) {
    if matches!(
        log_level,
        LogLevel::Debug | LogLevel::Info | LogLevel::Warn | LogLevel::Error
    ) {
        if let Some(logger) = logger {
            logger.error(args);
        }
    }
}
