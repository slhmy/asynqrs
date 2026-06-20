use crate::broker::redis::keys;
use crate::broker::redis::{RedisExecutor, RedisWriteResultPlan};
use crate::client::Clock;
use crate::{CancelError, ResultError};

use super::{RedisBroker, RedisBrokerError, redis_broker_error_message};

impl From<RedisBrokerError> for ResultError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

impl From<RedisBrokerError> for CancelError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

/// Reference: Asynq v0.26.0 result storage and cancellation publish operations:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1496-L1504>
/// and <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1550-L1557>.
impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    pub async fn write_result(
        &mut self,
        queue: &str,
        task_id: &str,
        data: impl Into<Vec<u8>>,
    ) -> Result<usize, ResultError> {
        let redis_plan = RedisWriteResultPlan::from_task(queue, task_id, data)
            .map_err(RedisBrokerError::WriteResultPlan)
            .map_err(ResultError::from)?;
        // Reference: Asynq v0.26.0 `RDB.WriteResult` returns the number of
        // bytes in the result data after `HSET`, not Redis' field-created
        // count.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1550-L1557>.
        let written = redis_plan.data().len();
        self.executor
            .hset_bytes(redis_plan.key(), redis_plan.field(), redis_plan.data())
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(ResultError::from)?;
        Ok(written)
    }

    pub async fn publish_cancel(&mut self, task_id: &str) -> Result<usize, CancelError> {
        // Reference: Asynq v0.26.0 `RDB.PublishCancelation` publishes the
        // supplied task id directly without local validation:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1496-L1504>.
        self.executor
            .publish(keys::CANCEL_CHANNEL, task_id)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(CancelError::from)
    }

    /// Reference: Asynq v0.26.0 `Inspector.CancelProcessing` publishes a task
    /// id to the Redis cancellation channel without local validation and
    /// returns only success or an error:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L760-L765>.
    pub async fn cancel_processing(&mut self, task_id: &str) -> Result<(), CancelError> {
        self.publish_cancel(task_id).await.map(|_| ())
    }
}
