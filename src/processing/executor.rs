use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio_util::sync::CancellationToken;

use crate::task::TaskMessage;
use crate::{
    HandlerError, ProcessingContext, ServerProcessingScope, SharedErrorHandler, SharedIsFailure,
    SharedRetryDelay, Task, TaskMetadata,
};

use super::{ErrorHandler, IsFailure, NoopErrorHandler, NoopLeaseExtender, RetryDelay};

/// Executes the user handler pipeline and owns handler-level customization.
///
/// Reference: Asynq v0.26.0 processor wires handler execution with retry
/// delay, failure classification, error handling, base context, and lease
/// extension:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L221-L390>.
///
/// Rust-native note: this is the first concrete split toward the server-owned
/// runtime redesign. `TaskExecutor` owns handler policy; worker runtime parts
/// borrow it explicitly until server-owned workers are constructed from owned
/// parts.
#[derive(Clone)]
pub(crate) struct TaskExecutor<H, R, I, E = NoopErrorHandler, L = NoopLeaseExtender> {
    pub(crate) handler: H,
    pub(crate) retry_delay: R,
    pub(crate) retry_delay_override: Option<SharedRetryDelay>,
    pub(crate) is_failure: I,
    pub(crate) is_failure_override: Option<SharedIsFailure>,
    pub(crate) error_handler: E,
    pub(crate) error_handler_override: Option<SharedErrorHandler>,
    pub(crate) lease_extender: L,
    pub(crate) processing_scope: Option<Arc<ServerProcessingScope>>,
}

impl<H, R, I> TaskExecutor<H, R, I> {
    pub(crate) fn new(handler: H, retry_delay: R, is_failure: I) -> Self {
        Self::with_hooks_and_lease_extender(
            handler,
            retry_delay,
            is_failure,
            NoopErrorHandler,
            NoopLeaseExtender,
        )
    }
}

impl<H, R, I, E, L> TaskExecutor<H, R, I, E, L> {
    pub(crate) fn with_hooks_and_lease_extender(
        handler: H,
        retry_delay: R,
        is_failure: I,
        error_handler: E,
        lease_extender: L,
    ) -> Self {
        Self {
            handler,
            retry_delay,
            retry_delay_override: None,
            is_failure,
            is_failure_override: None,
            error_handler,
            error_handler_override: None,
            lease_extender,
            processing_scope: None,
        }
    }

    pub(crate) fn retry_delay_for(
        &mut self,
        retried: i32,
        error: &HandlerError,
        task: &Task,
    ) -> Duration
    where
        R: RetryDelay,
    {
        if let Some(retry_delay) = &mut self.retry_delay_override {
            return retry_delay.retry_delay(retried, error, task);
        }
        self.retry_delay.retry_delay(retried, error, task)
    }

    pub(crate) fn is_failure_for(&mut self, error: &HandlerError) -> bool
    where
        I: IsFailure,
    {
        if let Some(is_failure) = &mut self.is_failure_override {
            return is_failure.is_failure(error);
        }
        self.is_failure.is_failure(error)
    }

    pub(crate) async fn handle_error_for(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        error: &HandlerError,
    ) where
        E: ErrorHandler + Send,
    {
        if let Some(error_handler) = &mut self.error_handler_override {
            error_handler.handle_error(task, context, error).await;
            return;
        }
        self.error_handler.handle_error(task, context, error).await;
    }

    pub(crate) fn processing_context_for(
        &self,
        message: &TaskMessage,
        deadline: Option<SystemTime>,
        cancellation: CancellationToken,
    ) -> (ProcessingContext, bool) {
        let metadata = TaskMetadata::new(
            message.id.clone(),
            message.queue.clone(),
            message.retried,
            message.retry,
        );
        let Some(processing_scope) = self
            .processing_scope
            .as_ref()
            .map(|processing_scope| processing_scope())
        else {
            return (
                ProcessingContext::new(deadline, cancellation, metadata),
                false,
            );
        };
        let scope_cancelled = processing_scope.is_cancelled();
        let context =
            ProcessingContext::from_scope(processing_scope, deadline, cancellation, metadata);
        (context, scope_cancelled)
    }
}

impl<H, R, I, E, L> std::fmt::Debug for TaskExecutor<H, R, I, E, L>
where
    H: std::fmt::Debug,
    R: std::fmt::Debug,
    I: std::fmt::Debug,
    E: std::fmt::Debug,
    L: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskExecutor")
            .field("handler", &self.handler)
            .field("retry_delay", &self.retry_delay)
            .field(
                "has_retry_delay_override",
                &self.retry_delay_override.is_some(),
            )
            .field("is_failure", &self.is_failure)
            .field(
                "has_is_failure_override",
                &self.is_failure_override.is_some(),
            )
            .field("error_handler", &self.error_handler)
            .field(
                "has_error_handler_override",
                &self.error_handler_override.is_some(),
            )
            .field("lease_extender", &self.lease_extender)
            .field("has_processing_scope", &self.processing_scope.is_some())
            .finish()
    }
}
