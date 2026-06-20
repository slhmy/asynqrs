use async_trait::async_trait;
use std::marker::PhantomData;

use super::HandlerError;
use crate::{ProcessingContext, Task, TypedTaskPayload};

/// Processes a single task on an async runtime.
///
/// Rust design note: handlers receive queued task data and per-attempt runtime
/// context as separate arguments. This keeps `Task` usable as plain queue data
/// while making cancellation, deadline, and retry metadata explicit.
///
/// Reference: Asynq v0.26.0 public `Handler` receives a task plus handler
/// context:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/server.go#L622-L650>.
#[async_trait]
pub trait Handler {
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError>;
}

/// Function adapter for task handlers.
pub struct HandlerFunc<F>(pub F);

impl<F> HandlerFunc<F>
where
    F: FnMut(&Task, &ProcessingContext) -> Result<(), HandlerError>,
{
    pub fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        (self.0)(task, context)
    }
}

#[async_trait]
impl<F> Handler for HandlerFunc<F>
where
    F: FnMut(&Task, &ProcessingContext) -> Result<(), HandlerError> + Send,
{
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        HandlerFunc::process_task(self, task, context)
    }
}

/// Function adapter for typed task payload handlers.
///
/// Rust design note: this is an optional ergonomic layer over [`Handler`].
/// It decodes task payload bytes before invoking user code but keeps the
/// original [`ProcessingContext`] and handler execution path intact.
pub struct TypedHandlerFunc<P, F> {
    handler: F,
    _payload: PhantomData<fn() -> P>,
}

impl<P, F> TypedHandlerFunc<P, F> {
    pub fn new(handler: F) -> Self {
        Self {
            handler,
            _payload: PhantomData,
        }
    }
}

impl<P, F> TypedHandlerFunc<P, F>
where
    P: TypedTaskPayload,
    F: FnMut(P, &ProcessingContext) -> Result<(), HandlerError>,
{
    pub fn process_typed_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        if task.type_name() != P::TASK_TYPE {
            return Err(HandlerError::failed(format!(
                "typed handler expected task type {:?}, got {:?}",
                P::TASK_TYPE,
                task.type_name()
            )));
        }

        let payload = P::decode_payload(task.payload()).map_err(|error| {
            HandlerError::failed(format!(
                "failed to decode typed payload for {:?}: {error}",
                P::TASK_TYPE
            ))
        })?;
        (self.handler)(payload, context)
    }
}

#[async_trait]
impl<P, F> Handler for TypedHandlerFunc<P, F>
where
    P: TypedTaskPayload + Send,
    F: FnMut(P, &ProcessingContext) -> Result<(), HandlerError> + Send,
{
    async fn process_task(
        &mut self,
        task: &Task,
        context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        self.process_typed_task(task, context)
    }
}

pub fn typed_handler<P, F>(handler: F) -> TypedHandlerFunc<P, F> {
    TypedHandlerFunc::new(handler)
}

#[async_trait]
impl<F> Handler for F
where
    F: FnMut(&Task) -> Result<(), HandlerError> + Send,
{
    async fn process_task(
        &mut self,
        task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        self(task)
    }
}

/// Function adapter for task-only handlers.
pub struct TaskHandlerFunc<F>(pub F);

impl<F> TaskHandlerFunc<F>
where
    F: FnMut(&Task) -> Result<(), HandlerError>,
{
    pub fn process_task(&mut self, task: &Task) -> Result<(), HandlerError> {
        (self.0)(task)
    }
}

#[async_trait]
impl<F> Handler for TaskHandlerFunc<F>
where
    F: FnMut(&Task) -> Result<(), HandlerError> + Send,
{
    async fn process_task(
        &mut self,
        task: &Task,
        _context: &ProcessingContext,
    ) -> Result<(), HandlerError> {
        TaskHandlerFunc::process_task(self, task)
    }
}
