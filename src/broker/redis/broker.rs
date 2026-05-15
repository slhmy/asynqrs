use std::time::SystemTime;

use thiserror::Error;

use crate::{
    ArchiveBroker, ArchiveError, AsyncRedisExecutor, Broker, BrokerError, Clock, CompleteBroker,
    CompleteError, DecodeTaskMessageError, DequeueBroker, DequeueError, DequeuedTask, EnqueuePlan,
    ForwardBroker, ForwardError, LeaseBroker, LeaseError, LeaseExtension, RecoverBroker,
    RecoverError, RecoverResult, RedisArchivePlan, RedisArchivePlanError, RedisCompletePlan,
    RedisCompletePlanError, RedisDequeueCall, RedisDequeuePlan, RedisDequeuePlanError,
    RedisEnqueueOperation, RedisEnqueuePlan, RedisEnqueuePlanError, RedisExtendLeasePlan,
    RedisExtendLeasePlanError, RedisForwardPlan, RedisForwardPlanError, RedisRecoverPlan,
    RedisRecoverPlanError, RedisRequeuePlan, RedisRequeuePlanError, RedisRetryPlan,
    RedisRetryPlanError, RedisScript, RedisScriptCall, RedisScriptCallError, RedisScriptResult,
    RequeueBroker, RequeueError, RetryBroker, RetryError, SystemClock, TaskMessage,
};

/// Minimal executor surface needed by `RedisBroker`.
///
/// Reference: Asynq v0.26.0 RDB methods combine Redis commands with Lua
/// scripts for task lifecycle state changes:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L82-L735>.
pub trait RedisExecutor {
    fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError>;

    fn zadd_existing(
        &mut self,
        key: &str,
        score: i64,
        member: &str,
    ) -> Result<usize, RedisExecutorError>;

    fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError>;

    fn eval_script_bytes(
        &mut self,
        call: &RedisDequeueCall,
    ) -> Result<Option<Vec<u8>>, RedisExecutorError>;

    fn eval_script_byte_vec(
        &mut self,
        call: &RedisScriptCall,
    ) -> Result<Vec<Vec<u8>>, RedisExecutorError>;

    fn eval_script_status(&mut self, call: &RedisScriptCall) -> Result<String, RedisExecutorError>;
}

#[derive(Debug, Clone)]
pub struct AsyncRedisBroker<E, C = SystemClock> {
    executor: E,
    clock: C,
}

#[derive(Debug, Clone)]
pub struct RedisBroker<E, C = SystemClock> {
    executor: E,
    clock: C,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct RedisExecutorError {
    message: String,
}

#[derive(Debug, Error)]
pub enum RedisBrokerError {
    #[error("failed to build Redis enqueue plan: {0}")]
    Plan(#[from] RedisEnqueuePlanError),
    #[error("failed to build Redis dequeue plan: {0}")]
    DequeuePlan(#[from] RedisDequeuePlanError),
    #[error("failed to build Redis complete plan: {0}")]
    CompletePlan(#[from] RedisCompletePlanError),
    #[error("failed to build Redis retry plan: {0}")]
    RetryPlan(#[from] RedisRetryPlanError),
    #[error("failed to build Redis archive plan: {0}")]
    ArchivePlan(#[from] RedisArchivePlanError),
    #[error("failed to build Redis requeue plan: {0}")]
    RequeuePlan(#[from] RedisRequeuePlanError),
    #[error("failed to build Redis forward plan: {0}")]
    ForwardPlan(#[from] RedisForwardPlanError),
    #[error("failed to build Redis recover plan: {0}")]
    RecoverPlan(#[from] RedisRecoverPlanError),
    #[error("failed to build Redis extend lease plan: {0}")]
    ExtendLeasePlan(#[from] RedisExtendLeasePlanError),
    #[error("invalid Redis script call: {0}")]
    ScriptCall(#[from] RedisScriptCallError),
    #[error("Redis executor failed: {0}")]
    Executor(#[from] RedisExecutorError),
    #[error("failed to decode dequeued task message: {0}")]
    Decode(#[from] DecodeTaskMessageError),
    #[error("unexpected {script:?} script result: {result}")]
    UnexpectedScriptResult { script: RedisScript, result: i64 },
    #[error("unexpected {script:?} script status: {status}")]
    UnexpectedScriptStatus { script: RedisScript, status: String },
}

impl<E> RedisBroker<E, SystemClock> {
    pub fn new(executor: E) -> Self {
        Self::with_clock(executor, SystemClock)
    }
}

impl<E> AsyncRedisBroker<E, SystemClock> {
    pub fn new(executor: E) -> Self {
        Self::with_clock(executor, SystemClock)
    }
}

impl<E, C> AsyncRedisBroker<E, C> {
    pub fn with_clock(executor: E, clock: C) -> Self {
        Self { executor, clock }
    }

    pub fn executor(&self) -> &E {
        &self.executor
    }

    pub fn executor_mut(&mut self) -> &mut E {
        &mut self.executor
    }

    pub fn into_executor(self) -> E {
        self.executor
    }
}

impl<E, C> RedisBroker<E, C> {
    pub fn with_clock(executor: E, clock: C) -> Self {
        Self { executor, clock }
    }

    pub fn executor(&self) -> &E {
        &self.executor
    }

    pub fn executor_mut(&mut self) -> &mut E {
        &mut self.executor
    }

    pub fn into_executor(self) -> E {
        self.executor
    }
}

impl<E, C> AsyncRedisBroker<E, C>
where
    E: AsyncRedisExecutor,
    C: Clock,
{
    pub async fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.enqueue_with_now(plan, self.clock.now()).await
    }
}

impl<E, C> Broker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn enqueue(&mut self, plan: &EnqueuePlan) -> Result<(), BrokerError> {
        self.enqueue_with_now(plan, self.clock.now())
    }
}

impl<E, C> DequeueBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn dequeue(&mut self, queues: &[String]) -> Result<DequeuedTask, DequeueError> {
        self.dequeue_with_now(queues, self.clock.now())
    }
}

impl<E, C> CompleteBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn complete(&mut self, message: &TaskMessage) -> Result<(), CompleteError> {
        self.complete_with_now(message, self.clock.now())
    }
}

impl<E, C> RetryBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn retry(
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
    }
}

impl<E, C> ArchiveBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn archive(
        &mut self,
        message: &TaskMessage,
        archived_at: SystemTime,
        error_message: &str,
        is_failure: bool,
    ) -> Result<(), ArchiveError> {
        self.archive_with_now(
            message,
            self.clock.now(),
            archived_at,
            error_message,
            is_failure,
        )
    }
}

impl<E, C> RequeueBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn requeue(&mut self, message: &TaskMessage) -> Result<(), RequeueError> {
        self.requeue_with_now(message)
    }
}

impl<E, C> ForwardBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn forward_scheduled(&mut self, queue: &str) -> Result<usize, ForwardError> {
        self.forward_with_now(queue, self.clock.now(), true)
    }

    fn forward_retry(&mut self, queue: &str) -> Result<usize, ForwardError> {
        self.forward_with_now(queue, self.clock.now(), false)
    }
}

impl<E, C> RecoverBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn recover_expired_leases(
        &mut self,
        queue: &str,
        retry_at: SystemTime,
        error_message: &str,
    ) -> Result<RecoverResult, RecoverError> {
        self.recover_expired_leases_with_now(queue, self.clock.now(), retry_at, error_message)
    }
}

impl<E, C> LeaseBroker for RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    fn extend_lease(&mut self, queue: &str, task_id: &str) -> Result<LeaseExtension, LeaseError> {
        self.extend_lease_with_now(queue, task_id, self.clock.now())
    }
}

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
{
    pub fn enqueue_with_now(
        &mut self,
        plan: &EnqueuePlan,
        now: SystemTime,
    ) -> Result<(), BrokerError> {
        let redis_plan = RedisEnqueuePlan::from_enqueue_plan(plan, now)
            .map_err(RedisBrokerError::Plan)
            .map_err(BrokerError::from)?;

        for operation in redis_plan.operations() {
            self.execute(operation)?;
        }
        Ok(())
    }

    fn execute(&mut self, operation: &RedisEnqueueOperation) -> Result<(), BrokerError> {
        match operation {
            RedisEnqueueOperation::PublishQueue { key, queue } => {
                self.executor
                    .sadd(key, queue)
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
                    .map_err(RedisBrokerError::Executor)
                    .map_err(BrokerError::from)?;
                map_script_result(call, result)
            }
        }
    }

    pub fn dequeue_with_now(
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

    pub fn complete_with_now(
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

    pub fn retry_with_now(
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

    pub fn archive_with_now(
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

    pub fn requeue_with_now(&mut self, message: &TaskMessage) -> Result<(), RequeueError> {
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

    pub fn forward_with_now(
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

    pub fn recover_expired_leases_with_now(
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
                    .map_err(|error| RecoverError::Other(error.to_string()))?;
                archived += 1;
            } else {
                self.retry_with_now(&message, now, retry_at, error_message, true)
                    .map_err(|error| RecoverError::Other(error.to_string()))?;
                retried += 1;
            }
        }

        Ok(RecoverResult::new(retried, archived))
    }

    pub fn extend_lease_with_now(
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
            .map_err(RedisBrokerError::Executor)
            .map_err(LeaseError::from)?;
        Ok(LeaseExtension::new(redis_plan.lease_expires_at()))
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

fn map_script_result(call: &RedisScriptCall, result: i64) -> Result<(), BrokerError> {
    match call.script().result_for_code(result) {
        Some(RedisScriptResult::Success) => Ok(()),
        Some(RedisScriptResult::TaskIdConflict) => Err(BrokerError::TaskIdConflict),
        Some(RedisScriptResult::DuplicateTask) => Err(BrokerError::DuplicateTask),
        None => Err(BrokerError::from(
            RedisBrokerError::UnexpectedScriptResult {
                script: call.script(),
                result,
            },
        )),
    }
}

impl RedisExecutorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl From<RedisBrokerError> for RecoverError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for BrokerError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for DequeueError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for CompleteError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND") => {
                Self::NotFound
            }
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for RetryError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND") => {
                Self::NotFound
            }
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for ArchiveError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND") => {
                Self::NotFound
            }
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for RequeueError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::Executor(error) if error.message().contains("NOT FOUND") => {
                Self::NotFound
            }
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for ForwardError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

impl From<RedisBrokerError> for LeaseError {
    fn from(error: RedisBrokerError) -> Self {
        match error {
            RedisBrokerError::ExtendLeasePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::Executor(error) => Self::Other(error.to_string()),
            RedisBrokerError::Plan(error) => Self::Other(error.to_string()),
            RedisBrokerError::DequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::CompletePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RetryPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ArchivePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RequeuePlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ForwardPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::RecoverPlan(error) => Self::Other(error.to_string()),
            RedisBrokerError::ScriptCall(error) => Self::Other(error.to_string()),
            RedisBrokerError::Decode(error) => Self::Other(error.to_string()),
            RedisBrokerError::UnexpectedScriptResult { script, result } => {
                Self::Other(format!("unexpected {script:?} script result: {result}"))
            }
            RedisBrokerError::UnexpectedScriptStatus { script, status } => {
                Self::Other(format!("unexpected {script:?} script status: {status}"))
            }
        }
    }
}

#[cfg(test)]
mod tests;
