use async_trait::async_trait;

use super::HandlerError;
use crate::{ProcessingContext, Task};

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
