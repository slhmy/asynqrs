use std::time::SystemTime;

use crate::broker::redis::keys;
use crate::broker::redis::{
    RedisDequeuePlan, RedisEnqueueOperation, RedisEnqueuePlan, RedisExecutor, RedisScript,
};
use crate::client::Clock;
use crate::task::TaskMessage;
use crate::{AdminError, BrokerError, DequeueError, DequeuedTask, EnqueuePlan};

use super::{RedisBroker, RedisBrokerError, map_script_result, redis_broker_error_message};

impl From<RedisBrokerError> for BrokerError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

impl From<RedisBrokerError> for DequeueError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    /// Checks whether the Redis connection backing this broker is reachable.
    ///
    /// Reference: Asynq v0.26.0 `RDB.Ping` delegates to Redis `PING`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
    pub(crate) async fn ping(&mut self) -> Result<(), RedisBrokerError> {
        self.executor
            .ping()
            .await
            .map_err(RedisBrokerError::Executor)
    }

    pub async fn enqueue_with_now(
        &mut self,
        plan: &EnqueuePlan,
        now: SystemTime,
    ) -> Result<(), BrokerError> {
        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(plan, now)
            .map_err(RedisBrokerError::Plan)
            .map_err(BrokerError::from)?;

        for operation in redis_plan.operations() {
            self.execute(operation).await?;
        }
        Ok(())
    }

    pub async fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.enqueue_with_now(plan, self.clock.now()).await
    }

    pub async fn dequeue_with_now(
        &mut self,
        queues: &[String],
        now: SystemTime,
    ) -> Result<DequeuedTask, DequeueError> {
        let redis_plan = RedisDequeuePlan::from_queues(queues, now)
            .map_err(RedisBrokerError::DequeuePlan)
            .map_err(DequeueError::from)?;

        for call in redis_plan.queue_calls() {
            RedisScript::Dequeue
                .validate_call(call.keys(), call.args())
                .map_err(RedisBrokerError::ScriptCall)
                .map_err(DequeueError::from)?;
            let Some(data) = self
                .executor
                .eval_script_bytes(call)
                .await
                .map_err(RedisBrokerError::Executor)
                .map_err(DequeueError::from)?
            else {
                continue;
            };
            let message = TaskMessage::decode_from_slice(&data)
                .map_err(RedisBrokerError::Decode)
                .map_err(DequeueError::from)?;
            return Ok(DequeuedTask::new(message, redis_plan.lease_expires_at()));
        }

        Err(DequeueError::NoProcessableTask)
    }

    pub(super) async fn ensure_queue_exists(&mut self, queue: &str) -> Result<(), AdminError> {
        let exists = self
            .executor
            .sismember(keys::ALL_QUEUES, queue)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        if exists {
            Ok(())
        } else {
            Err(AdminError::QueueNotFound)
        }
    }

    async fn execute(&mut self, operation: &RedisEnqueueOperation) -> Result<(), BrokerError> {
        match operation {
            RedisEnqueueOperation::PublishQueue { key, queue } => {
                if self.published_queues.contains(queue) {
                    return Ok(());
                }
                self.executor
                    .sadd(key, queue)
                    .await
                    .map_err(RedisBrokerError::Executor)
                    .map_err(BrokerError::from)?;
                self.published_queues.insert(queue.clone());
                Ok(())
            }
            RedisEnqueueOperation::EvalScript(call) => {
                call.validate()
                    .map_err(RedisBrokerError::ScriptCall)
                    .map_err(BrokerError::from)?;
                let result = self
                    .executor
                    .eval_script_int(call)
                    .await
                    .map_err(RedisBrokerError::Executor)
                    .map_err(BrokerError::from)?;
                map_script_result(call, result)
            }
        }
    }
}
