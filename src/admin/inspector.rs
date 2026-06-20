use thiserror::Error;

use crate::broker::redis::{
    MakeRedisClientError, RedisRuntimeBroker, RedisRuntimeClient, redis_broker_from_runtime_client,
};
use crate::client::CloseBroker;
use crate::task::validate_queue_name;
use crate::{AdminError, BrokerError, CancelError, DailyStats, GroupStats, QueueStats};

use super::CancelBroker;

mod bulk;
mod metadata;
mod task_read;
pub(crate) use bulk::InspectorBulkBroker;
pub(crate) use metadata::InspectorMetadataBroker;
pub(crate) use task_read::InspectorTaskReadBroker;

/// Client interface to inspect and mutate queue/task state.
///
/// Reference: Asynq v0.26.0 public `Inspector`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L18-L25>.
#[derive(Debug, Clone)]
pub struct Inspector<B = RedisRuntimeBroker> {
    pub(super) broker: B,
    pub(super) shared_connection: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InspectorError {
    /// Reference: Asynq v0.26.0 `Inspector.Close` returns the underlying
    /// `RDB.Close` error when the inspector owns the Redis connection:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L39-L45>.
    #[error("{0}")]
    Close(BrokerError),
    /// Reference: Asynq v0.26.0 `Inspector.Close` refuses to close inspectors
    /// created from an existing Redis connection:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L39-L45>.
    #[error("redis connection is shared so the Inspector can't be closed through asynq")]
    SharedConnection,
}

/// Internal broker operations used by public Inspector queue lifecycle methods.
///
/// Reference: Asynq v0.26.0 public `Inspector.PauseQueue`,
/// `Inspector.UnpauseQueue`, and `Inspector.DeleteQueue` methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L220-L244>
/// and <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L766-L780>.
#[async_trait::async_trait]
pub trait InspectorQueueBroker {
    async fn pause_queue(&mut self, queue: &str) -> Result<(), AdminError>;
    async fn unpause_queue(&mut self, queue: &str) -> Result<(), AdminError>;
    async fn delete_queue(&mut self, queue: &str, force: bool) -> Result<(), AdminError>;
}

/// Internal broker operations used by public Inspector single-task lifecycle
/// and payload mutation methods.
///
/// Reference: Asynq v0.26.0 public `Inspector.RunTask`,
/// `Inspector.ArchiveTask`, `Inspector.DeleteTask`, and
/// `Inspector.UpdateTaskPayload` methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L592-L758>.
#[async_trait::async_trait]
pub trait InspectorTaskBroker {
    async fn run_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError>;
    async fn archive_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError>;
    async fn delete_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError>;
    async fn update_task_payload(
        &mut self,
        queue: &str,
        task_id: &str,
        payload: Vec<u8>,
    ) -> Result<(), AdminError>;
}

/// Internal broker operations used by public Inspector queue discovery and stats
/// methods.
///
/// Reference: Asynq v0.26.0 public `Inspector.Queues`,
/// `Inspector.Groups`, `Inspector.GetQueueInfo`, and `Inspector.History`
/// methods:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L57-L199>.
#[async_trait::async_trait]
pub trait InspectorStatsBroker {
    async fn list_queues(&mut self) -> Result<Vec<String>, AdminError>;
    async fn group_stats(&mut self, queue: &str) -> Result<Vec<GroupStats>, AdminError>;
    async fn current_queue_stats(&mut self, queue: &str) -> Result<QueueStats, AdminError>;
    async fn historical_queue_stats(
        &mut self,
        queue: &str,
        days: isize,
    ) -> Result<Vec<DailyStats>, AdminError>;
}

impl<B> Inspector<B> {
    fn from_owned_broker(broker: B) -> Self {
        Self {
            broker,
            shared_connection: false,
        }
    }

    /// Creates an inspector backed by caller-owned broker resources.
    ///
    /// Reference: Asynq v0.26.0 `NewInspectorFromRedisClient` marks its Redis
    /// connection as shared so `Inspector.Close` refuses to close it:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L31-L37>.
    fn from_shared_broker(broker: B) -> Self {
        Self {
            broker,
            shared_connection: true,
        }
    }
}

impl Inspector {
    pub async fn from_redis_client(
        redis_client: impl Into<RedisRuntimeClient>,
    ) -> Result<Self, MakeRedisClientError> {
        Self::from_redis_runtime_client(redis_client.into()).await
    }

    pub async fn from_redis_runtime_client(
        redis_client: RedisRuntimeClient,
    ) -> Result<Self, MakeRedisClientError> {
        new_inspector_from_redis_runtime_client(redis_client, true).await
    }

    pub async fn from_direct_redis_client(
        redis_client: redis::Client,
    ) -> Result<Self, MakeRedisClientError> {
        Self::from_redis_runtime_client(RedisRuntimeClient::direct(redis_client)).await
    }
}

pub(super) fn validate_inspector_queue(queue: &str) -> Result<(), AdminError> {
    // Reference: Asynq v0.26.0 validates queue names only on the public
    // Inspector methods that call `base.ValidateQueueName` before delegation:
    // <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L137-L780>.
    validate_queue_name(queue).map_err(|_| AdminError::InvalidQueueName)
}

impl<B> Inspector<B>
where
    B: CloseBroker,
{
    /// Closes the inspector-owned backing resources.
    ///
    /// Reference: Asynq v0.26.0 `Inspector.Close` delegates to `RDB.Close`
    /// unless the Redis connection is shared:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L39-L45>.
    pub fn close(&mut self) -> Result<(), InspectorError> {
        if self.shared_connection {
            return Err(InspectorError::SharedConnection);
        }
        self.broker.close().map_err(InspectorError::Close)
    }
}

impl<B> Inspector<B>
where
    B: CancelBroker,
{
    /// Cancels processing of an active task by publishing its task id.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.CancelProcessing` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L760-L765>.
    pub async fn cancel_processing(&mut self, task_id: &str) -> Result<(), CancelError> {
        self.broker.publish_cancel(task_id).await.map(|_| ())
    }
}

impl<B> Inspector<B>
where
    B: InspectorStatsBroker,
{
    /// Returns all known queue names.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.Queues` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L57-L59>.
    pub async fn queues(&mut self) -> Result<Vec<String>, AdminError> {
        self.broker.list_queues().await
    }

    /// Returns aggregation groups for the queue.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.Groups` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L62-L76>.
    pub async fn groups(&mut self, queue: &str) -> Result<Vec<GroupStats>, AdminError> {
        self.broker.group_stats(queue).await
    }

    /// Returns current queue statistics.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.GetQueueInfo` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L137-L166>.
    pub async fn get_queue_info(&mut self, queue: &str) -> Result<QueueStats, AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.current_queue_stats(queue).await
    }

    /// Returns historical daily queue statistics.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.History` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L182-L199>.
    pub async fn history(
        &mut self,
        queue: &str,
        days: isize,
    ) -> Result<Vec<DailyStats>, AdminError> {
        // Reference: Asynq v0.26.0 `Inspector.History` accepts a signed
        // `int` day count and delegates non-positive validation to the Redis
        // history boundary:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L182-L199>.
        validate_inspector_queue(queue)?;
        self.broker.historical_queue_stats(queue, days).await
    }
}

impl<B> Inspector<B>
where
    B: InspectorQueueBroker,
{
    /// Pauses processing for the named queue.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.PauseQueue` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L766-L772>.
    pub async fn pause_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.pause_queue(queue).await
    }

    /// Resumes processing for the named queue.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.UnpauseQueue` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L774-L780>.
    pub async fn unpause_queue(&mut self, queue: &str) -> Result<(), AdminError> {
        validate_inspector_queue(queue)?;
        self.broker.unpause_queue(queue).await
    }

    /// Deletes the named queue, optionally forcing deletion of non-active
    /// tasks while preserving active-task protection.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.DeleteQueue` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L220-L244>.
    pub async fn delete_queue(&mut self, queue: &str, force: bool) -> Result<(), AdminError> {
        self.broker
            .delete_queue(queue, force)
            .await
            .map_err(|error| error.for_delete_queue(queue))
    }
}

impl<B> Inspector<B>
where
    B: InspectorTaskBroker,
{
    /// Moves a non-active, non-pending task to pending state.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.RunTask` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L696-L725>.
    pub async fn run_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        validate_inspector_queue(queue).map_err(AdminError::with_asynq_prefix)?;
        self.broker
            .run_task(queue, task_id)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    /// Moves a non-active, non-archived task to archived state.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.ArchiveTask` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L731-L758>.
    pub async fn archive_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        if validate_queue_name(queue).is_err() {
            // Reference: Asynq v0.26.0 public `Inspector.ArchiveTask`
            // preserves this fixed validation error text instead of wrapping
            // the underlying queue-name validation error:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L731-L758>.
            return Err(AdminError::AsynqArchiveQueueValidation);
        }
        self.broker
            .archive_task(queue, task_id)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    /// Deletes a non-active task from its queue state collection.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.DeleteTask` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L616-L639>.
    pub async fn delete_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        validate_inspector_queue(queue).map_err(AdminError::with_asynq_prefix)?;
        self.broker
            .delete_task(queue, task_id)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }

    /// Updates the payload for one scheduled task.
    ///
    /// Reference: Asynq v0.26.0 public `Inspector.UpdateTaskPayload` method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L592-L614>.
    pub async fn update_task_payload(
        &mut self,
        queue: &str,
        task_id: &str,
        payload: Vec<u8>,
    ) -> Result<(), AdminError> {
        validate_inspector_queue(queue).map_err(AdminError::with_asynq_prefix)?;
        self.broker
            .update_task_payload(queue, task_id, payload)
            .await
            .map_err(AdminError::with_asynq_prefix)
    }
}

async fn new_inspector_from_redis_runtime_client(
    redis_client: RedisRuntimeClient,
    shared_connection: bool,
) -> Result<Inspector, MakeRedisClientError> {
    let broker = redis_broker_from_runtime_client(redis_client).await?;
    if shared_connection {
        Ok(Inspector::from_shared_broker(broker))
    } else {
        Ok(Inspector::from_owned_broker(broker))
    }
}
