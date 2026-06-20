use std::time::SystemTime;

use crate::admin::CancelBroker;
use crate::broker::redis::RedisExecutor;
use crate::client::Clock;
use crate::server::{
    ArchiveBroker, CleanupBroker, ForwardBroker, LeaseBroker, PingBroker, RecoverBroker,
    RequeueBroker, RetryBroker, ServerMaintenanceRun, WorkerBrokerCore,
};
use crate::task::TaskMessage;
use crate::{
    ArchiveError, CancelError, CleanupError, CompleteError, DequeueError, DequeuedTask,
    ForwardError, LeaseError, LeaseExtension, RecoverError, RequeueError, ResultError, RetryError,
};

use super::super::RedisBroker;

#[async_trait::async_trait]
impl<E, C> PingBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn ping(&mut self) -> Result<(), String> {
        RedisBroker::ping(self)
            .await
            .map_err(|error| error.to_string())
    }
}

#[async_trait::async_trait]
impl<E, C> WorkerBrokerCore for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn dequeue(&mut self, queues: &[String]) -> Result<DequeuedTask, DequeueError> {
        for queue in queues {
            // Reference: Asynq v0.26.0 `RDB.Dequeue` reads the clock
            // inside the queue scan, so the returned lease belongs to the
            // queue that actually produced a task:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L257-L274>.
            match self
                .dequeue_with_now(std::slice::from_ref(queue), self.clock.now())
                .await
            {
                Ok(task) => return Ok(task),
                Err(DequeueError::NoProcessableTask) => {}
                Err(error) => return Err(error),
            }
        }
        Err(DequeueError::NoProcessableTask)
    }

    async fn complete(&mut self, message: &TaskMessage) -> Result<(), CompleteError> {
        self.complete_with_now(message, self.clock.now()).await
    }

    async fn retry(
        &mut self,
        message: &TaskMessage,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), RetryError> {
        self.retry_with_now(
            message,
            self.clock.now(),
            retry_at,
            error_message,
            is_failure,
        )
        .await
    }

    async fn archive(
        &mut self,
        message: &TaskMessage,
        error_message: &str,
    ) -> Result<(), ArchiveError> {
        self.archive_with_now(message, self.clock.now(), error_message)
            .await
    }

    async fn write_result(
        &mut self,
        queue: &str,
        task_id: &str,
        data: Vec<u8>,
    ) -> Result<usize, ResultError> {
        RedisBroker::write_result(self, queue, task_id, data).await
    }
}

#[async_trait::async_trait]
impl<E, C> RetryBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn retry(
        &mut self,
        message: &TaskMessage,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), RetryError> {
        self.retry_with_now(
            message,
            self.clock.now(),
            retry_at,
            error_message,
            is_failure,
        )
        .await
    }
}

#[async_trait::async_trait]
impl<E, C> ArchiveBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn archive(
        &mut self,
        message: &TaskMessage,
        error_message: &str,
    ) -> Result<(), ArchiveError> {
        self.archive_with_now(message, self.clock.now(), error_message)
            .await
    }
}

#[async_trait::async_trait]
impl<E, C> LeaseBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn extend_leases(
        &mut self,
        queue: &str,
        task_ids: &[String],
    ) -> Result<LeaseExtension, LeaseError> {
        self.extend_leases_with_now(queue, task_ids, self.clock.now())
            .await
    }
}

#[async_trait::async_trait]
impl<E, C> RequeueBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn requeue(&mut self, message: &TaskMessage) -> Result<(), RequeueError> {
        self.requeue_with_now(message).await
    }
}

#[async_trait::async_trait]
impl<E, C> CancelBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn publish_cancel(&mut self, task_id: &str) -> Result<usize, CancelError> {
        RedisBroker::publish_cancel(self, task_id).await
    }
}

#[async_trait::async_trait]
impl<E, C> ForwardBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn forward_if_ready(
        &mut self,
        queues: &[String],
    ) -> Result<ServerMaintenanceRun, ForwardError> {
        self.forward_if_ready_with_now(queues, self.clock.now())
            .await
    }
}

#[async_trait::async_trait]
impl<E, C> RecoverBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn list_lease_expired(
        &mut self,
        cutoff: std::time::SystemTime,
        queues: &[String],
    ) -> Result<Vec<TaskMessage>, RecoverError> {
        self.list_lease_expired_with_cutoff(queues, cutoff).await
    }
}

#[async_trait::async_trait]
impl<E, C> CleanupBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn delete_expired_completed_tasks(
        &mut self,
        queue: &str,
        batch_size: isize,
    ) -> Result<usize, CleanupError> {
        RedisBroker::delete_expired_completed_tasks(self, queue, batch_size).await
    }
}
