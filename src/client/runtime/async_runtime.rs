//! Asynchronous client runtime operations.
//!
//! Reference: Asynq v0.26.0 `Client.Ping`, `Client.Enqueue`, and
//! `Client.EnqueueContext`:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L266-L358>.

use super::{Client, ClientEnqueueScope, ClientError, Clock, TaskIdGenerator};
use crate::client::{AsyncBroker, EnqueueResult};
use crate::{EnqueueOptions, Task, TaskInfo};

impl<B, G, C> Client<B, G, C>
where
    B: AsyncBroker,
    G: TaskIdGenerator,
    C: Clock,
{
    pub async fn ping_async(&mut self) -> Result<(), ClientError> {
        self.broker.ping().await.map_err(ClientError::Ping)
    }

    pub async fn enqueue_async(&mut self, task: &Task) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_async(&ClientEnqueueScope::background(), task)
            .await
    }

    /// Enqueues an optional task, preserving upstream's nil-task error branch.
    ///
    /// Reference: Asynq v0.26.0 `Client.Enqueue` delegates to
    /// `Client.EnqueueContext`, which rejects nil tasks before composing
    /// options:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L338-L358>.
    pub async fn enqueue_optional_async(
        &mut self,
        task: Option<&Task>,
    ) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_optional_async(&ClientEnqueueScope::background(), task)
            .await
    }

    pub async fn enqueue_with_async(
        &mut self,
        task: &Task,
        options: EnqueueOptions,
    ) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_with_async(&ClientEnqueueScope::background(), task, options)
            .await
    }

    pub async fn enqueue_optional_with_async(
        &mut self,
        task: Option<&Task>,
        options: EnqueueOptions,
    ) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_optional_with_async(&ClientEnqueueScope::background(), task, options)
            .await
    }

    pub async fn enqueue_scoped_async(
        &mut self,
        scope: &ClientEnqueueScope,
        task: &Task,
    ) -> Result<EnqueueResult, ClientError> {
        self.enqueue_scoped_with_async(scope, task, EnqueueOptions::default())
            .await
    }

    pub async fn enqueue_scoped_with_async(
        &mut self,
        scope: &ClientEnqueueScope,
        task: &Task,
        options: EnqueueOptions,
    ) -> Result<EnqueueResult, ClientError> {
        let plan = self.enqueue_plan(task, options)?;
        Self::check_enqueue_scoped(scope)?;

        tokio::select! {
            result = self.broker.enqueue(&plan) => {
                result?;
            }
            _ = scope.wait_cancelled() => {
                return Err(ClientError::Cancelled);
            }
        }

        Ok(TaskInfo::from_enqueue_plan(&plan))
    }

    /// Enqueues an optional task, preserving upstream's nil-task error branch.
    ///
    /// Reference: Asynq v0.26.0 `Client.EnqueueContext` rejects nil tasks
    /// before composing options:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/client.go#L355-L358>.
    pub async fn enqueue_scoped_optional_async(
        &mut self,
        scope: &ClientEnqueueScope,
        task: Option<&Task>,
    ) -> Result<EnqueueResult, ClientError> {
        let task = task.ok_or(ClientError::NilTask)?;
        self.enqueue_scoped_async(scope, task).await
    }

    pub async fn enqueue_scoped_optional_with_async(
        &mut self,
        scope: &ClientEnqueueScope,
        task: Option<&Task>,
        options: EnqueueOptions,
    ) -> Result<EnqueueResult, ClientError> {
        let task = task.ok_or(ClientError::NilTask)?;
        self.enqueue_scoped_with_async(scope, task, options).await
    }
}
