//! Worker-owned task report routing.
//!
//! Reference: Asynq v0.26.0 routes successful and failed task executions to
//! complete, retry, archive, or done lifecycle writes while logging failed
//! writes for sync retry:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L263-L390>.

use std::sync::Arc;
use std::time::SystemTime;

use crate::ProcessingContext;
use crate::broker::redis::keys;
use crate::client::Clock;
use crate::processing::{
    ErrorHandler, HandlerError, IsFailure, ProcessingError, ProcessingLease, RetryDelay,
    TaskExecutor, WorkerRun, task_from_message,
};
use crate::server::log_processing_warn;
use crate::task::TaskMessage;

use super::{LogLevel, Logger, ServerRuntimeState, WorkerBrokerCore, WorkerTaskReport};

/// Routes worker execution reports to Redis lifecycle operations.
///
/// Rust-native note: this component is intentionally separate from
/// `TaskExecutor`. Handler execution decides whether a task succeeded or failed;
/// `WorkerReportRouter` decides how that outcome mutates queue state.
pub(crate) struct WorkerReportRouter<'a, B, H, R, C, I, E, L> {
    broker: &'a mut B,
    executor: &'a mut TaskExecutor<H, R, I, E, L>,
    clock: &'a C,
    runtime: &'a ServerRuntimeState,
    logger: &'a Option<Arc<dyn Logger>>,
    log_level: LogLevel,
}

impl<'a, B, H, R, C, I, E, L> WorkerReportRouter<'a, B, H, R, C, I, E, L> {
    pub(crate) fn new(
        broker: &'a mut B,
        executor: &'a mut TaskExecutor<H, R, I, E, L>,
        clock: &'a C,
        runtime: &'a ServerRuntimeState,
        logger: &'a Option<Arc<dyn Logger>>,
        log_level: LogLevel,
    ) -> Self {
        Self {
            broker,
            executor,
            clock,
            runtime,
            logger,
            log_level,
        }
    }
}

impl<B, H, R, C, I, E, L> WorkerReportRouter<'_, B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + Send,
    R: RetryDelay + Send,
    C: Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
{
    pub(crate) async fn report_success(
        &mut self,
        report: WorkerTaskReport,
    ) -> Result<WorkerRun, ProcessingError> {
        // Reference: Asynq v0.26.0 `markAsDone`/`markAsComplete` logs
        // completion failures and schedules sync retry instead of failing the
        // worker result:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L263-L293>.
        // Reference: Asynq v0.26.0 skips lifecycle writes after the worker
        // lease has expired and leaves recovery to the recoverer:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L284-L310>.
        if !report.lease.is_valid_at(self.clock.now()) {
            return Ok(WorkerRun::LeaseExpired {
                task_id: report.task_id,
            });
        }
        self.complete_with_sync_warning(&report.message, report.lease.expires_at())
            .await;
        Ok(WorkerRun::Completed {
            task_id: report.task_id,
        })
    }

    pub(crate) async fn report_failure(
        &mut self,
        report: WorkerTaskReport,
        error: HandlerError,
    ) -> Result<WorkerRun, ProcessingError> {
        self.handle_failure(report.message, report.lease, error, report.context)
            .await
    }

    async fn handle_failure(
        &mut self,
        message: TaskMessage,
        lease: ProcessingLease,
        error: HandlerError,
        context: ProcessingContext,
    ) -> Result<WorkerRun, ProcessingError> {
        // Reference: Asynq v0.26.0 builds a fresh `NewTaskWithHeaders` value
        // for failure hooks and retry-delay callbacks, while passing the
        // worker context separately to `ErrorHandler.HandleError`.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L335-L338>,
        // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L357-L371>.
        let hook_task = task_from_message(&message);
        self.executor
            .handle_error_for(&hook_task, &context, &error)
            .await;
        match error {
            HandlerError::RevokeTask(_) => {
                // Reference: Asynq v0.26.0 logs revoked tasks before marking
                // them done:
                // <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L340-L346>.
                log_processing_warn(
                    self.logger,
                    self.log_level,
                    format_args!("revoke task id={}", message.id),
                );
                let mut revoked = message.clone();
                revoked.retention = 0;
                if !lease.is_valid_at(self.clock.now()) {
                    return Ok(WorkerRun::LeaseExpired {
                        task_id: message.id,
                    });
                }
                self.complete_with_sync_warning(&revoked, lease.expires_at())
                    .await;
                Ok(WorkerRun::Revoked {
                    task_id: message.id,
                })
            }
            error @ HandlerError::SkipRetry(_) => {
                log_processing_warn(
                    self.logger,
                    self.log_level,
                    format_args!("Retry exhausted for task id={}", message.id),
                );
                let error_message = error.to_string();
                if !lease.is_valid_at(self.clock.now()) {
                    return Ok(WorkerRun::LeaseExpired {
                        task_id: message.id,
                    });
                }
                self.archive_with_sync_warning(&message, &error_message, lease.expires_at())
                    .await;
                Ok(WorkerRun::Archived {
                    task_id: message.id,
                })
            }
            error if message.retried >= message.retry => {
                log_processing_warn(
                    self.logger,
                    self.log_level,
                    format_args!("Retry exhausted for task id={}", message.id),
                );
                let error_message = error.to_string();
                if !lease.is_valid_at(self.clock.now()) {
                    return Ok(WorkerRun::LeaseExpired {
                        task_id: message.id,
                    });
                }
                self.archive_with_sync_warning(&message, &error_message, lease.expires_at())
                    .await;
                Ok(WorkerRun::Archived {
                    task_id: message.id,
                })
            }
            error => {
                let error_message = error.to_string();
                let is_failure = self.executor.is_failure_for(&error);
                let now = self.clock.now();
                if !lease.is_valid_at(now) {
                    return Ok(WorkerRun::LeaseExpired {
                        task_id: message.id,
                    });
                }
                let delay = self
                    .executor
                    .retry_delay_for(message.retried, &error, &hook_task);
                let retry_at = now
                    .checked_add(delay)
                    .ok_or(ProcessingError::TimeOverflow("retry time"))?;
                self.retry_with_sync_warning(
                    &message,
                    retry_at,
                    &error_message,
                    is_failure,
                    lease.expires_at(),
                )
                .await;
                Ok(WorkerRun::Retried {
                    task_id: message.id,
                    retry_at,
                })
            }
        }
    }

    async fn complete_with_sync_warning(&mut self, message: &TaskMessage, deadline: SystemTime) {
        if let Err(error) = WorkerBrokerCore::complete(self.broker, message).await {
            let sync_error_message = if message.retention > 0 {
                format!(
                    "Could not move task id={} type={:?} from {:?} to {:?}:  {error}",
                    message.id,
                    message.r#type,
                    keys::active_key(&message.queue),
                    keys::completed_key(&message.queue)
                )
            } else {
                format!(
                    "Could not remove task id={} type={:?} from {:?} err: {error}",
                    message.id,
                    message.r#type,
                    keys::active_key(&message.queue)
                )
            };
            self.runtime.pending_sync().enqueue_complete(
                message.clone(),
                deadline,
                sync_error_message.clone(),
            );
            log_processing_warn(
                self.logger,
                self.log_level,
                format_args!("{sync_error_message}; Will retry syncing"),
            );
        }
    }

    async fn retry_with_sync_warning(
        &mut self,
        message: &TaskMessage,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
        deadline: SystemTime,
    ) {
        if WorkerBrokerCore::retry(self.broker, message, retry_at, error_message, is_failure)
            .await
            .is_err()
        {
            let sync_error_message = format!(
                "Could not move task id={} from {:?} to {:?}",
                message.id,
                keys::active_key(&message.queue),
                keys::retry_key(&message.queue)
            );
            self.runtime.pending_sync().enqueue_retry(
                message.clone(),
                retry_at,
                error_message.to_owned(),
                is_failure,
                deadline,
                sync_error_message.clone(),
            );
            log_processing_warn(
                self.logger,
                self.log_level,
                format_args!("{sync_error_message}; Will retry syncing"),
            );
        }
    }

    async fn archive_with_sync_warning(
        &mut self,
        message: &TaskMessage,
        error_message: &str,
        deadline: SystemTime,
    ) {
        if WorkerBrokerCore::archive(self.broker, message, error_message)
            .await
            .is_err()
        {
            let sync_error_message = format!(
                "Could not move task id={} from {:?} to {:?}",
                message.id,
                keys::active_key(&message.queue),
                keys::archived_key(&message.queue)
            );
            self.runtime.pending_sync().enqueue_archive(
                message.clone(),
                error_message.to_owned(),
                deadline,
                sync_error_message.clone(),
            );
            log_processing_warn(
                self.logger,
                self.log_level,
                format_args!("{sync_error_message}; Will retry syncing"),
            );
        }
    }
}
