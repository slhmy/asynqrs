use std::time::SystemTime;

use crate::{
    ArchiveError, AsyncRedisExecutor, BrokerError, Clock, CompleteError, DequeueError,
    DequeuedTask, EnqueuePlan, RedisArchivePlan, RedisCompletePlan, RedisDequeuePlan,
    RedisEnqueueOperation, RedisEnqueuePlan, RedisRetryPlan, RedisScript, RetryError, TaskMessage,
};

use super::{AsyncRedisBroker, RedisBrokerError, map_script_result};

impl<E, C> AsyncRedisBroker<E, C>
where
    E: AsyncRedisExecutor,
    C: Clock,
{
    pub async fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.enqueue_with_now(plan, self.clock.now()).await
    }
}

impl<E, C> AsyncRedisBroker<E, C>
where
    E: AsyncRedisExecutor,
{
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

    pub async fn complete_with_now(
        &mut self,
        message: &TaskMessage,
        now: SystemTime,
    ) -> Result<(), CompleteError> {
        let redis_plan = RedisCompletePlan::from_message(message, now)
            .map_err(RedisBrokerError::CompletePlan)
            .map_err(CompleteError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(CompleteError::from)?;
        let status = self
            .executor
            .eval_script_status(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(CompleteError::from)?;
        if status == "OK" {
            Ok(())
        } else {
            Err(CompleteError::from(
                RedisBrokerError::UnexpectedScriptStatus {
                    script: call.script(),
                    status,
                },
            ))
        }
    }

    pub async fn retry_with_now(
        &mut self,
        message: &TaskMessage,
        now: SystemTime,
        retry_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), RetryError> {
        let redis_plan =
            RedisRetryPlan::from_message(message, now, retry_at, error_message, is_failure)
                .map_err(RedisBrokerError::RetryPlan)
                .map_err(RetryError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(RetryError::from)?;
        let status = self
            .executor
            .eval_script_status(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(RetryError::from)?;
        if status == "OK" {
            Ok(())
        } else {
            Err(RetryError::from(RedisBrokerError::UnexpectedScriptStatus {
                script: call.script(),
                status,
            }))
        }
    }

    pub async fn archive_with_now(
        &mut self,
        message: &TaskMessage,
        now: SystemTime,
        archived_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), ArchiveError> {
        let redis_plan =
            RedisArchivePlan::from_message(message, now, archived_at, error_message, is_failure)
                .map_err(RedisBrokerError::ArchivePlan)
                .map_err(ArchiveError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(ArchiveError::from)?;
        let status = self
            .executor
            .eval_script_status(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(ArchiveError::from)?;
        if status == "OK" {
            Ok(())
        } else {
            Err(ArchiveError::from(
                RedisBrokerError::UnexpectedScriptStatus {
                    script: call.script(),
                    status,
                },
            ))
        }
    }

    async fn execute(&mut self, operation: &RedisEnqueueOperation) -> Result<(), BrokerError> {
        match operation {
            RedisEnqueueOperation::PublishQueue { key, queue } => {
                self.executor
                    .sadd(key, queue)
                    .await
                    .map_err(RedisBrokerError::Executor)
                    .map_err(BrokerError::from)?;
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
