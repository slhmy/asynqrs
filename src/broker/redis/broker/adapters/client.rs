use crate::broker::redis::RedisExecutor;
use crate::client::Clock;
use crate::client::{AsyncBroker, CloseBroker};
use crate::{BrokerError, EnqueuePlan};

use super::super::{RedisBroker, RedisBrokerError};

impl<E, C> CloseBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
{
    /// Closes Redis broker resources owned by higher-level clients.
    ///
    /// Reference: Asynq v0.26.0 `RDB.Close` delegates to the underlying Redis
    /// client close method:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L109-L111>.
    fn close(&mut self) -> Result<(), BrokerError> {
        self.executor_mut()
            .close()
            .map_err(RedisBrokerError::from)
            .map_err(BrokerError::from)
    }
}

#[async_trait::async_trait]
impl<E, C> AsyncBroker for RedisBroker<E, C>
where
    E: RedisExecutor + Send,
    C: Clock + Send + Sync,
{
    async fn ping(&mut self) -> Result<(), BrokerError> {
        RedisBroker::ping(self).await.map_err(BrokerError::from)
    }

    async fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        RedisBroker::enqueue(self, plan).await
    }
}
