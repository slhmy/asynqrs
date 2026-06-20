//! Explicit worker runtime parts borrowed from server-owned worker assembly.
//!
//! Reference: Asynq v0.26.0 processor workers execute handlers, extend leases,
//! and route lifecycle writes inside the upstream processor loop:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L390>.
//!
//! Rust-native note: this boundary names the worker hot-path dependencies so
//! `Worker` can execute from explicit server-owned runtime parts.

use std::sync::Arc;

use crate::ResultWriter;
use crate::client::Clock;
use crate::processing::{
    ErrorHandler, Handler, HandlerError, IsFailure, LeaseExtender, ProcessingError,
    ProcessingLease, RetryDelay, TaskExecutionContext, TaskExecutor, WorkerRun,
    perform_with_lease_extender, task_from_message,
};
use crate::task::TaskMessage;

use super::{
    LogLevel, Logger, ServerRuntimeState, WorkerActiveTask, WorkerBrokerCore, WorkerHandlerResult,
    WorkerReportRouter, WorkerTaskReport,
};

pub(crate) struct WorkerRuntimeParts<'a, B, H, R, C, I, E, L> {
    broker: &'a mut B,
    executor: &'a mut TaskExecutor<H, R, I, E, L>,
    clock: &'a C,
    runtime: &'a ServerRuntimeState,
    logger: &'a Option<Arc<dyn Logger>>,
    log_level: LogLevel,
}

impl<'a, B, H, R, C, I, E, L> WorkerRuntimeParts<'a, B, H, R, C, I, E, L> {
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

impl<B, H, R, C, I, E, L> WorkerRuntimeParts<'_, B, H, R, C, I, E, L>
where
    B: WorkerBrokerCore + Send,
    H: Handler + Send,
    R: RetryDelay + Send,
    C: Clock + Send + Sync,
    I: IsFailure + Send,
    E: ErrorHandler + Send,
    L: LeaseExtender<B> + Send,
{
    pub(crate) async fn before_process(
        &mut self,
        message: &TaskMessage,
        lease: &ProcessingLease,
    ) -> Result<(), ProcessingError> {
        match self
            .executor
            .lease_extender
            .before_process(&mut *self.broker, message)
            .await
        {
            Ok(Some(extension)) => {
                lease.reset(extension);
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn execute_active_task(
        &mut self,
        active: WorkerActiveTask,
    ) -> Result<WorkerHandlerResult, ProcessingError> {
        let WorkerActiveTask {
            message,
            task_id,
            lease,
            started_at: now,
            deadline,
            cancellation,
        } = active;
        let (mut processing_context, base_cancelled) =
            self.executor
                .processing_context_for(&message, deadline, cancellation.clone());
        if base_cancelled {
            self.runtime.canceller().cancel(&message.id);
        }
        let effective_deadline = processing_context.deadline();
        let result_deadline = effective_deadline
            .and_then(|deadline| super::tokio_instant_for_system_time(deadline, now));
        let (result_writer, result_writes) = ResultWriter::channel_with_context(
            message.id.clone(),
            cancellation.clone(),
            processing_context.parent_cancellation_token(),
            result_deadline,
        );
        processing_context = processing_context.with_result_writer(result_writer);
        let task = task_from_message(&message);
        let handler_result = perform_with_lease_extender(
            &mut self.executor.handler,
            &mut self.executor.lease_extender,
            &mut *self.broker,
            TaskExecutionContext {
                message: &message,
                task: &task,
                context: &processing_context,
                lease: &lease,
                deadline: effective_deadline,
                now,
                cancellation,
                logger: self.logger,
                log_level: self.log_level,
                result_writes,
            },
        )
        .await?;
        let report = WorkerTaskReport {
            message,
            task_id,
            lease,
            context: processing_context,
        };
        Ok(match handler_result {
            Ok(()) => WorkerHandlerResult::Succeeded(report),
            Err(error) => WorkerHandlerResult::Failed { report, error },
        })
    }

    pub(crate) async fn report_success(
        &mut self,
        report: WorkerTaskReport,
    ) -> Result<WorkerRun, ProcessingError> {
        WorkerReportRouter::new(
            &mut *self.broker,
            self.executor,
            self.clock,
            self.runtime,
            self.logger,
            self.log_level,
        )
        .report_success(report)
        .await
    }

    pub(crate) async fn report_failure(
        &mut self,
        report: WorkerTaskReport,
        error: HandlerError,
    ) -> Result<WorkerRun, ProcessingError> {
        WorkerReportRouter::new(
            &mut *self.broker,
            self.executor,
            self.clock,
            self.runtime,
            self.logger,
            self.log_level,
        )
        .report_failure(report, error)
        .await
    }
}
