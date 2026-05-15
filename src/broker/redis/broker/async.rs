use std::time::SystemTime;

use crate::{
    ArchiveError, AsyncRedisExecutor, BrokerError, Clock, CompleteError, DequeueError,
    DequeuedTask, EnqueuePlan, ForwardError, LeaseError, LeaseExtension, RecoverError,
    RecoverResult, RedisArchivePlan, RedisCompletePlan, RedisDequeuePlan, RedisEnqueueOperation,
    RedisEnqueuePlan, RedisExtendLeasePlan, RedisForwardPlan, RedisRecoverPlan, RedisRequeuePlan,
    RedisRetryPlan, RedisScript, RequeueError, RetryError, TaskMessage,
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

    pub async fn requeue_with_now(&mut self, message: &TaskMessage) -> Result<(), RequeueError> {
        let redis_plan = RedisRequeuePlan::from_message(message)
            .map_err(RedisBrokerError::RequeuePlan)
            .map_err(RequeueError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(RequeueError::from)?;
        let status = self
            .executor
            .eval_script_status(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(RequeueError::from)?;
        if status == "OK" {
            Ok(())
        } else {
            Err(RequeueError::from(
                RedisBrokerError::UnexpectedScriptStatus {
                    script: call.script(),
                    status,
                },
            ))
        }
    }

    pub async fn forward_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
        scheduled: bool,
    ) -> Result<usize, ForwardError> {
        let redis_plan = if scheduled {
            RedisForwardPlan::from_scheduled_queue(queue, now)
        } else {
            RedisForwardPlan::from_retry_queue(queue, now)
        }
        .map_err(RedisBrokerError::ForwardPlan)
        .map_err(ForwardError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(ForwardError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(ForwardError::from)?;
        if result < 0 {
            return Err(ForwardError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            )));
        }
        Ok(result as usize)
    }

    pub async fn recover_expired_leases_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
        retry_at: SystemTime,
        error_message: &str,
    ) -> Result<RecoverResult, RecoverError> {
        let redis_plan = RedisRecoverPlan::from_queue(queue, now)
            .map_err(RedisBrokerError::RecoverPlan)
            .map_err(RecoverError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(RecoverError::from)?;
        let messages = self
            .executor
            .eval_script_byte_vec(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(RecoverError::from)?;

        let mut retried = 0;
        let mut archived = 0;
        for data in messages {
            let message = TaskMessage::decode_from_slice(&data)
                .map_err(RedisBrokerError::Decode)
                .map_err(RecoverError::from)?;
            if message.retried >= message.retry {
                self.archive_with_now(&message, now, now, error_message, true)
                    .await
                    .map_err(|error| RecoverError::Other(error.to_string()))?;
                archived += 1;
            } else {
                self.retry_with_now(&message, now, retry_at, error_message, true)
                    .await
                    .map_err(|error| RecoverError::Other(error.to_string()))?;
                retried += 1;
            }
        }

        Ok(RecoverResult::new(retried, archived))
    }

    pub async fn extend_lease_with_now(
        &mut self,
        queue: &str,
        task_id: &str,
        now: SystemTime,
    ) -> Result<LeaseExtension, LeaseError> {
        let redis_plan = RedisExtendLeasePlan::from_queue_and_task_id(queue, task_id, now)
            .map_err(RedisBrokerError::ExtendLeasePlan)
            .map_err(LeaseError::from)?;
        let _updated = self
            .executor
            .zadd_existing(
                redis_plan.key(),
                redis_plan.lease_expires_at_seconds(),
                redis_plan.task_id(),
            )
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(LeaseError::from)?;
        Ok(LeaseExtension::new(redis_plan.lease_expires_at()))
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
