//! Synchronous client runtime operations.
//!
//! Reference: Asynq v0.26.0 `Client.Ping`, `Client.Enqueue`, and
//! `Client.EnqueueContext`:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L266-L358>.

use super::{Client, ClientEnqueueScope, ClientError, Clock, TaskIdGenerator};
use crate::client::{Broker, EnqueueResult};
use crate::{EnqueueOptions, Task, TaskInfo};

impl<B, G, C> Client<B, G, C>
where
    B: Broker,
    G: TaskIdGenerator,
    C: Clock,
{
    /// Checks the client backing store connection.
    ///
    /// Reference: Asynq v0.26.0 `Client.Ping` delegates to the underlying
    /// broker `Ping` operation:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L333-L336>.
    pub fn ping(&mut self) -> Result<(), ClientError> {
        self.broker.ping().map_err(ClientError::Ping)
    }

    pub fn enqueue(&mut self, task: &Task) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped(&ClientEnqueueScope::background(), task)
    }

    /// Enqueues an optional task, preserving upstream's nil-task error branch.
    ///
    /// Reference: Asynq v0.26.0 `Client.Enqueue` delegates to
    /// `Client.EnqueueContext`, which rejects nil tasks before composing
    /// options:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L338-L358>.
    pub fn enqueue_optional(&mut self, task: Option<&Task>) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_optional(&ClientEnqueueScope::background(), task)
    }

    pub fn enqueue_with(
        &mut self,
        task: &Task,
        options: EnqueueOptions,
    ) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_with(&ClientEnqueueScope::background(), task, options)
    }

    pub fn enqueue_optional_with(
        &mut self,
        task: Option<&Task>,
        options: EnqueueOptions,
    ) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_optional_with(&ClientEnqueueScope::background(), task, options)
    }

    pub fn enqueue_scoped(
        &mut self,
        scope: &ClientEnqueueScope,
        task: &Task,
    ) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_with(scope, task, EnqueueOptions::default())
    }

    /// Enqueues an optional task, preserving upstream's nil-task error branch.
    ///
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` rejects nil tasks
    /// before composing options:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L355-L358>.
    pub fn enqueue_scoped_optional(
        &mut self,
        scope: &ClientEnqueueScope,
        task: Option<&Task>,
    ) -> Result<EnqueueResult, ClientError> {
        let task = task.ok_or(ClientError::NilTask)?;
        self.enqueue_scoped(scope, task)
    }

    pub fn enqueue_scoped_with(
        &mut self,
        scope: &ClientEnqueueScope,
        task: &Task,
        options: EnqueueOptions,
    ) -> Result<EnqueueResult, ClientError> {
        let plan = self.enqueue_plan(task, options)?;
        Self::check_enqueue_scoped(scope)?;

        self.broker.enqueue(&plan)?;

        Ok(TaskInfo::from_enqueue_plan(&plan))
    }

    pub fn enqueue_scoped_optional_with(
        &mut self,
        scope: &ClientEnqueueScope,
        task: Option<&Task>,
        options: EnqueueOptions,
    ) -> Result<EnqueueResult, ClientError> {
        let task = task.ok_or(ClientError::NilTask)?;
        self.enqueue_scoped_with(scope, task, options)
    }
}
