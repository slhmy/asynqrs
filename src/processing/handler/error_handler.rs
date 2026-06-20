use std::sync::Arc;

use async_trait::async_trait;

use super::HandlerError;
use crate::{ProcessingContext, Task};

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopErrorHandler;

/// Handles errors returned by async task handlers.
///
/// Reference: Asynq v0.26.0 `ErrorHandler` and processor error hook:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L277-L287>,
/// <https://github.com/hibiken/asynq/blob/v0.26.0/processor.go#L335-L338>.
#[async_trait]
pub trait ErrorHandler {
    async fn handle_error(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        error: &HandlerError,
    );
}

/// Function adapter for task-processing error handlers.
pub struct ErrorHandlerFunc<F>(pub F);

impl<F> ErrorHandlerFunc<F>
where
    F: FnMut(&Task, &ProcessingContext, &HandlerError),
{
    pub fn handle_error(&mut self, task: &Task, context: &ProcessingContext, error: &HandlerError) {
        (self.0)(task, context, error);
    }
}

#[async_trait]
impl ErrorHandler for NoopErrorHandler {
    async fn handle_error(
        &mut self,
        _task: &Task,
        _context: &ProcessingContext,
        _error: &HandlerError,
    ) {
    }
}

#[async_trait]
impl<F> ErrorHandler for ErrorHandlerFunc<F>
where
    F: FnMut(&Task, &ProcessingContext, &HandlerError) + Send,
{
    async fn handle_error(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        error: &HandlerError,
    ) {
        ErrorHandlerFunc::handle_error(self, task, context, error);
    }
}

/// Shared holder for a `Config.ErrorHandler` callback.
///
/// Reference: Asynq v0.26.0 public `Config.ErrorHandler` field:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L132-L149>.
#[derive(Clone)]
pub struct SharedErrorHandler(pub Arc<tokio::sync::Mutex<dyn ErrorHandler + Send>>);

impl std::fmt::Debug for SharedErrorHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SharedErrorHandler").finish_non_exhaustive()
    }
}

#[async_trait]
impl ErrorHandler for SharedErrorHandler {
    async fn handle_error(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        error: &HandlerError,
    ) {
        self.0.lock().await.handle_error(task, context, error).await;
    }
}
