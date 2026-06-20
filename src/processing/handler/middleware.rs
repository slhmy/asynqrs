use async_trait::async_trait;

use super::{Handler, HandlerError};
use crate::{ProcessingContext, Task};

/// Task middleware wrapper for handlers.
///
/// Reference: Asynq v0.26.0 public `MiddlewareFunc` wraps a handler and
/// returns another handler:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/servemux.go#L35-L38>.
///
/// Rust compatibility: this trait models the same wrapping behavior through an
/// async `next` handler argument instead of a direct higher-order wrapper type.
#[async_trait]
pub trait TaskMiddleware {
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        next: &mut (dyn Handler + Send),
    ) -> Result<(), HandlerError>;
}

/// Function adapter for task middleware closures.
///
/// The closure runs before the next handler in the chain.
pub struct TaskMiddlewareFn<F>(pub F);

pub fn task_middleware_fn<F>(middleware: F) -> TaskMiddlewareFn<F> {
    TaskMiddlewareFn(middleware)
}

/// Function adapter for before/after task middleware hooks.
///
/// `before` may short-circuit processing by returning an error. `after` always
/// receives the current result and may return it unchanged or replace it.
pub struct TaskMiddlewareHooks<B, A> {
    before: B,
    after: A,
}

pub fn task_middleware_hooks<B, A>(before: B, after: A) -> TaskMiddlewareHooks<B, A> {
    TaskMiddlewareHooks { before, after }
}

#[async_trait]
impl<F> TaskMiddleware for TaskMiddlewareFn<F>
where
    F: FnMut(&Task, &ProcessingContext) -> Result<(), HandlerError> + Send,
{
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        next: &mut (dyn Handler + Send),
    ) -> Result<(), HandlerError> {
        (self.0)(task, context)?;
        next.process_task(task, context).await
    }
}

#[async_trait]
impl<B, A> TaskMiddleware for TaskMiddlewareHooks<B, A>
where
    B: FnMut(&Task, &ProcessingContext) -> Result<(), HandlerError> + Send,
    A: FnMut(&Task, &ProcessingContext, Result<(), HandlerError>) -> Result<(), HandlerError>
        + Send,
{
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        next: &mut (dyn Handler + Send),
    ) -> Result<(), HandlerError> {
        let result = match (self.before)(task, context) {
            Ok(()) => next.process_task(task, context).await,
            Err(error) => Err(error),
        };
        (self.after)(task, context, result)
    }
}

#[async_trait]
impl<T> TaskMiddleware for Box<T>
where
    T: TaskMiddleware + Send + ?Sized,
{
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
        next: &mut (dyn Handler + Send),
    ) -> Result<(), HandlerError> {
        self.as_mut().process_task(task, context, next).await
    }
}
