use std::time::SystemTime;

use crate::{
    ArchiveBroker, ArchiveError, Broker, BrokerError, Clock, CompleteBroker, CompleteError,
    DecodeTaskMessageError, DequeueBroker, DequeueError, DequeuedTask, EnqueuePlan, ForwardBroker,
    ForwardError, LeaseBroker, LeaseError, LeaseExtension, RecoverBroker, RecoverError,
    RecoverResult, RedisArchivePlan, RedisArchivePlanError, RedisCompletePlan,
    RedisCompletePlanError, RedisDequeueCall, RedisDequeuePlan, RedisDequeuePlanError,
    RedisEnqueueOperation, RedisEnqueuePlan, RedisEnqueuePlanError, RedisExtendLeasePlan,
    RedisExtendLeasePlanError, RedisForwardPlan, RedisForwardPlanError, RedisRecoverPlan,
    RedisRecoverPlanError, RedisRetryPlan, RedisRetryPlanError, RedisScript, RedisScriptCall,
    RedisScriptCallError, RedisScriptResult, RetryBroker, RetryError, SystemClock, TaskMessage,
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
pub struct RedisBroker<E, C = SystemClock> {
    executor: E,
    clock: C,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisExecutorError {
    message: String,
}

#[derive(Debug)]
pub enum RedisBrokerError {
    Plan(RedisEnqueuePlanError),
    DequeuePlan(RedisDequeuePlanError),
    CompletePlan(RedisCompletePlanError),
    RetryPlan(RedisRetryPlanError),
    ArchivePlan(RedisArchivePlanError),
    ForwardPlan(RedisForwardPlanError),
    RecoverPlan(RedisRecoverPlanError),
    ExtendLeasePlan(RedisExtendLeasePlanError),
    ScriptCall(RedisScriptCallError),
    Executor(RedisExecutorError),
    Decode(DecodeTaskMessageError),
    UnexpectedScriptResult { script: RedisScript, result: i64 },
    UnexpectedScriptStatus { script: RedisScript, status: String },
}

impl<E> RedisBroker<E, SystemClock> {
    pub fn new(executor: E) -> Self {
        Self::with_clock(executor, SystemClock)
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

impl std::fmt::Display for RedisExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RedisExecutorError {}

impl std::fmt::Display for RedisBrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plan(error) => write!(f, "failed to build Redis enqueue plan: {error}"),
            Self::DequeuePlan(error) => write!(f, "failed to build Redis dequeue plan: {error}"),
            Self::CompletePlan(error) => write!(f, "failed to build Redis complete plan: {error}"),
            Self::RetryPlan(error) => write!(f, "failed to build Redis retry plan: {error}"),
            Self::ArchivePlan(error) => write!(f, "failed to build Redis archive plan: {error}"),
            Self::ForwardPlan(error) => write!(f, "failed to build Redis forward plan: {error}"),
            Self::RecoverPlan(error) => write!(f, "failed to build Redis recover plan: {error}"),
            Self::ExtendLeasePlan(error) => {
                write!(f, "failed to build Redis extend lease plan: {error}")
            }
            Self::ScriptCall(error) => write!(f, "invalid Redis script call: {error}"),
            Self::Executor(error) => write!(f, "Redis executor failed: {error}"),
            Self::Decode(error) => write!(f, "failed to decode dequeued task message: {error}"),
            Self::UnexpectedScriptResult { script, result } => {
                write!(f, "unexpected {script:?} script result: {result}")
            }
            Self::UnexpectedScriptStatus { script, status } => {
                write!(f, "unexpected {script:?} script status: {status}")
            }
        }
    }
}

impl std::error::Error for RedisBrokerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plan(error) => Some(error),
            Self::DequeuePlan(error) => Some(error),
            Self::CompletePlan(error) => Some(error),
            Self::RetryPlan(error) => Some(error),
            Self::ArchivePlan(error) => Some(error),
            Self::ForwardPlan(error) => Some(error),
            Self::RecoverPlan(error) => Some(error),
            Self::ExtendLeasePlan(error) => Some(error),
            Self::ScriptCall(error) => Some(error),
            Self::Executor(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::UnexpectedScriptResult { .. } | Self::UnexpectedScriptStatus { .. } => None,
        }
    }
}

impl From<RedisEnqueuePlanError> for RedisBrokerError {
    fn from(error: RedisEnqueuePlanError) -> Self {
        Self::Plan(error)
    }
}

impl From<RedisDequeuePlanError> for RedisBrokerError {
    fn from(error: RedisDequeuePlanError) -> Self {
        Self::DequeuePlan(error)
    }
}

impl From<RedisCompletePlanError> for RedisBrokerError {
    fn from(error: RedisCompletePlanError) -> Self {
        Self::CompletePlan(error)
    }
}

impl From<RedisRetryPlanError> for RedisBrokerError {
    fn from(error: RedisRetryPlanError) -> Self {
        Self::RetryPlan(error)
    }
}

impl From<RedisArchivePlanError> for RedisBrokerError {
    fn from(error: RedisArchivePlanError) -> Self {
        Self::ArchivePlan(error)
    }
}

impl From<RedisForwardPlanError> for RedisBrokerError {
    fn from(error: RedisForwardPlanError) -> Self {
        Self::ForwardPlan(error)
    }
}

impl From<RedisRecoverPlanError> for RedisBrokerError {
    fn from(error: RedisRecoverPlanError) -> Self {
        Self::RecoverPlan(error)
    }
}

impl From<RedisExtendLeasePlanError> for RedisBrokerError {
    fn from(error: RedisExtendLeasePlanError) -> Self {
        Self::ExtendLeasePlan(error)
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

impl From<RedisExecutorError> for RedisBrokerError {
    fn from(error: RedisExecutorError) -> Self {
        Self::Executor(error)
    }
}

impl From<RedisScriptCallError> for RedisBrokerError {
    fn from(error: RedisScriptCallError) -> Self {
        Self::ScriptCall(error)
    }
}

impl From<DecodeTaskMessageError> for RedisBrokerError {
    fn from(error: DecodeTaskMessageError) -> Self {
        Self::Decode(error)
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
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{RedisArg, Task, TaskOption, TaskState};

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ExecutorCall {
        Sadd {
            key: String,
            member: String,
        },
        ZaddExisting {
            key: String,
            score: i64,
            member: String,
        },
        EvalScriptInt {
            script: RedisScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
        EvalScriptBytes {
            script: RedisScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
        EvalScriptByteVec {
            script: RedisScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
        EvalScriptStatus {
            script: RedisScript,
            keys: Vec<String>,
            args: Vec<RedisArg>,
        },
    }

    #[derive(Debug)]
    struct FakeExecutor {
        calls: Vec<ExecutorCall>,
        script_int_results: Vec<i64>,
        script_bytes_results: Vec<Option<Vec<u8>>>,
        script_byte_vec_results: Vec<Vec<Vec<u8>>>,
        script_status_results: Vec<String>,
        zadd_existing_results: Vec<usize>,
        sadd_error: Option<RedisExecutorError>,
        zadd_error: Option<RedisExecutorError>,
        script_error: Option<RedisExecutorError>,
    }

    impl Default for FakeExecutor {
        fn default() -> Self {
            Self {
                calls: Vec::new(),
                script_int_results: vec![1],
                script_bytes_results: Vec::new(),
                script_byte_vec_results: Vec::new(),
                script_status_results: vec!["OK".to_owned()],
                zadd_existing_results: vec![1],
                sadd_error: None,
                zadd_error: None,
                script_error: None,
            }
        }
    }

    impl RedisExecutor for FakeExecutor {
        fn sadd(&mut self, key: &str, member: &str) -> Result<(), RedisExecutorError> {
            self.calls.push(ExecutorCall::Sadd {
                key: key.to_owned(),
                member: member.to_owned(),
            });
            if let Some(error) = self.sadd_error.clone() {
                return Err(error);
            }
            Ok(())
        }

        fn zadd_existing(
            &mut self,
            key: &str,
            score: i64,
            member: &str,
        ) -> Result<usize, RedisExecutorError> {
            self.calls.push(ExecutorCall::ZaddExisting {
                key: key.to_owned(),
                score,
                member: member.to_owned(),
            });
            if let Some(error) = self.zadd_error.clone() {
                return Err(error);
            }
            Ok(self.zadd_existing_results.remove(0))
        }

        fn eval_script_int(&mut self, call: &RedisScriptCall) -> Result<i64, RedisExecutorError> {
            self.calls.push(ExecutorCall::EvalScriptInt {
                script: call.script(),
                keys: call.keys().to_vec(),
                args: call.args().to_vec(),
            });
            if let Some(error) = self.script_error.clone() {
                return Err(error);
            }
            Ok(self.script_int_results.remove(0))
        }

        fn eval_script_bytes(
            &mut self,
            call: &RedisDequeueCall,
        ) -> Result<Option<Vec<u8>>, RedisExecutorError> {
            self.calls.push(ExecutorCall::EvalScriptBytes {
                script: call.script(),
                keys: call.keys().to_vec(),
                args: call.args().to_vec(),
            });
            if let Some(error) = self.script_error.clone() {
                return Err(error);
            }
            Ok(self.script_bytes_results.remove(0))
        }

        fn eval_script_byte_vec(
            &mut self,
            call: &RedisScriptCall,
        ) -> Result<Vec<Vec<u8>>, RedisExecutorError> {
            self.calls.push(ExecutorCall::EvalScriptByteVec {
                script: call.script(),
                keys: call.keys().to_vec(),
                args: call.args().to_vec(),
            });
            if let Some(error) = self.script_error.clone() {
                return Err(error);
            }
            Ok(self.script_byte_vec_results.remove(0))
        }

        fn eval_script_status(
            &mut self,
            call: &RedisScriptCall,
        ) -> Result<String, RedisExecutorError> {
            self.calls.push(ExecutorCall::EvalScriptStatus {
                script: call.script(),
                keys: call.keys().to_vec(),
                args: call.args().to_vec(),
            });
            if let Some(error) = self.script_error.clone() {
                return Err(error);
            }
            Ok(self.script_status_results.remove(0))
        }
    }

    #[test]
    fn executes_publish_then_enqueue_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [TaskOption::queue("critical")],
        );
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        broker.enqueue(&plan).unwrap();

        let calls = &broker.executor().calls;
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls[0],
            ExecutorCall::Sadd {
                key: "asynq:queues".to_owned(),
                member: "critical".to_owned()
            }
        );
        let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[1] else {
            panic!("expected script call");
        };
        assert_eq!(*script, RedisScript::Enqueue);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:pending".to_owned(),
            ]
        );
        assert!(matches!(args[0], RedisArg::Bytes(_)));
        assert_eq!(args[1], RedisArg::String("task-id".to_owned()));
        assert_eq!(args[2], RedisArg::I64(1_700_000_000_000_000_000));
    }

    #[test]
    fn executes_unique_scheduled_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [
                TaskOption::queue("critical"),
                TaskOption::process_in(Duration::from_secs(60)),
                TaskOption::unique(Duration::from_secs(300)),
            ],
        );
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        assert_eq!(plan.state(), TaskState::Scheduled);
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        broker.enqueue(&plan).unwrap();

        let ExecutorCall::EvalScriptInt { script, keys, args } = &broker.executor().calls[1] else {
            panic!("expected script call");
        };
        assert_eq!(*script, RedisScript::ScheduleUnique);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:unique:email:welcome:321c3cf486ed509164edec1e1981fec8".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:scheduled".to_owned(),
            ]
        );
        assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
        assert_eq!(args[1], RedisArg::I64(360));
        assert_eq!(args[2], RedisArg::I64(1_700_000_060));
        assert!(matches!(args[3], RedisArg::Bytes(_)));
    }

    #[test]
    fn maps_unique_duplicate_result() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new_with_options(
            "email:welcome",
            b"payload".to_vec(),
            [TaskOption::unique(Duration::from_secs(300))],
        );
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        let executor = FakeExecutor {
            script_int_results: vec![-1],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.enqueue(&plan).unwrap_err();

        assert_eq!(error, BrokerError::DuplicateTask);
    }

    #[test]
    fn maps_task_id_conflict_result() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new("email:welcome", Vec::new());
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        let executor = FakeExecutor {
            script_int_results: vec![0],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.enqueue(&plan).unwrap_err();

        assert_eq!(error, BrokerError::TaskIdConflict);
    }

    #[test]
    fn maps_executor_errors() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let task = Task::new("email:welcome", Vec::new());
        let plan = EnqueuePlan::from_task(&task, now, "task-id").unwrap();
        let executor = FakeExecutor {
            sadd_error: Some(RedisExecutorError::new("connection closed")),
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.enqueue(&plan).unwrap_err();

        assert_eq!(error, BrokerError::Other("connection closed".to_owned()));
    }

    #[test]
    fn dequeues_first_available_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        let executor = FakeExecutor {
            script_bytes_results: vec![None, Some(msg.encode_to_vec())],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let task = broker
            .dequeue(&["empty".to_owned(), "critical".to_owned()])
            .unwrap();

        assert_eq!(task.message(), &msg);
        assert_eq!(
            task.lease_expires_at(),
            UNIX_EPOCH + Duration::from_secs(1_700_000_030)
        );
        assert_eq!(broker.executor().calls.len(), 2);
        let ExecutorCall::EvalScriptBytes { script, keys, args } = &broker.executor().calls[0]
        else {
            panic!("expected dequeue script call");
        };
        assert_eq!(*script, RedisScript::Dequeue);
        assert_eq!(
            keys,
            &[
                "asynq:{empty}:pending".to_owned(),
                "asynq:{empty}:active".to_owned(),
                "asynq:{empty}:lease".to_owned(),
                "asynq:{empty}:t:".to_owned(),
                "asynq:{empty}:paused".to_owned(),
            ]
        );
        assert_eq!(args, &[RedisArg::I64(1_700_000_030)]);
    }

    #[test]
    fn dequeue_reports_no_processable_task() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let executor = FakeExecutor {
            script_bytes_results: vec![None],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.dequeue(&["critical".to_owned()]).unwrap_err();

        assert_eq!(error, DequeueError::NoProcessableTask);
    }

    #[test]
    fn dequeue_maps_executor_errors() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let executor = FakeExecutor {
            script_error: Some(RedisExecutorError::new("connection closed")),
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.dequeue(&["critical".to_owned()]).unwrap_err();

        assert_eq!(error, DequeueError::Other("connection closed".to_owned()));
    }

    #[test]
    fn completes_zero_retention_task_with_done_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        broker.complete(&msg).unwrap();

        let calls = &broker.executor().calls;
        assert_eq!(calls.len(), 1);
        let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
            panic!("expected complete script call");
        };
        assert_eq!(*script, RedisScript::Done);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
            ]
        );
        assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
    }

    #[test]
    fn completes_retained_task_with_mark_as_complete_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        msg.retention = 300;
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        broker.complete(&msg).unwrap();

        let ExecutorCall::EvalScriptStatus { script, keys, args } = &broker.executor().calls[0]
        else {
            panic!("expected complete script call");
        };
        assert_eq!(*script, RedisScript::MarkAsComplete);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:completed".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
            ]
        );
        assert_eq!(args[2], RedisArg::I64(1_700_000_300));
        assert!(matches!(args[3], RedisArg::Bytes(_)));
    }

    #[test]
    fn complete_maps_not_found_errors() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        let executor = FakeExecutor {
            script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker.complete(&msg).unwrap_err();

        assert_eq!(error, CompleteError::NotFound);
    }

    #[test]
    fn retries_failed_task_with_retry_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let retry_at = now + Duration::from_secs(60);
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        broker
            .retry(&msg, retry_at, "handler failed", true)
            .unwrap();

        let calls = &broker.executor().calls;
        assert_eq!(calls.len(), 1);
        let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
            panic!("expected retry script call");
        };
        assert_eq!(*script, RedisScript::Retry);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:retry".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
                "asynq:{critical}:failed:2023-11-14".to_owned(),
                "asynq:{critical}:failed".to_owned(),
            ]
        );
        assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
        assert!(matches!(args[1], RedisArg::Bytes(_)));
        assert_eq!(args[2], RedisArg::I64(1_700_000_060));
        assert_eq!(args[4], RedisArg::String("1".to_owned()));
    }

    #[test]
    fn retry_maps_not_found_errors() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        let executor = FakeExecutor {
            script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker
            .retry(&msg, now + Duration::from_secs(60), "handler failed", true)
            .unwrap_err();

        assert_eq!(error, RetryError::NotFound);
    }

    #[test]
    fn archives_failed_task_with_archive_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        broker
            .archive(&msg, now, "max retry exhausted", true)
            .unwrap();

        let calls = &broker.executor().calls;
        assert_eq!(calls.len(), 1);
        let ExecutorCall::EvalScriptStatus { script, keys, args } = &calls[0] else {
            panic!("expected archive script call");
        };
        assert_eq!(*script, RedisScript::Archive);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:active".to_owned(),
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:archived".to_owned(),
                "asynq:{critical}:t:task-id".to_owned(),
                "asynq:{critical}:processed:2023-11-14".to_owned(),
                "asynq:{critical}:processed".to_owned(),
                "asynq:{critical}:failed:2023-11-14".to_owned(),
                "asynq:{critical}:failed".to_owned(),
            ]
        );
        assert_eq!(args[0], RedisArg::String("task-id".to_owned()));
        assert!(matches!(args[1], RedisArg::Bytes(_)));
        assert_eq!(args[2], RedisArg::I64(1_700_000_000));
        assert_eq!(args[4], RedisArg::String("1".to_owned()));
    }

    #[test]
    fn archive_maps_not_found_errors() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut msg = TaskMessage::from_task(&Task::new("email:welcome", b"payload".to_vec()));
        msg.id = "task-id".to_owned();
        msg.queue = "critical".to_owned();
        let executor = FakeExecutor {
            script_error: Some(RedisExecutorError::new("redis eval error: NOT FOUND")),
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let error = broker
            .archive(&msg, now, "max retry exhausted", true)
            .unwrap_err();

        assert_eq!(error, ArchiveError::NotFound);
    }

    #[test]
    fn forwards_scheduled_tasks_with_forward_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let executor = FakeExecutor {
            script_int_results: vec![2],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let moved = broker.forward_scheduled("critical").unwrap();

        assert_eq!(moved, 2);
        let calls = &broker.executor().calls;
        assert_eq!(calls.len(), 1);
        let ExecutorCall::EvalScriptInt { script, keys, args } = &calls[0] else {
            panic!("expected forward script call");
        };
        assert_eq!(*script, RedisScript::Forward);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:scheduled".to_owned(),
                "asynq:{critical}:pending".to_owned(),
                "asynq:{critical}:t:".to_owned(),
            ]
        );
        assert_eq!(args[0], RedisArg::I64(1_700_000_000));
        assert_eq!(args[1], RedisArg::I64(1_700_000_000_000_000_000));
    }

    #[test]
    fn forwards_retry_tasks_with_forward_script() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let executor = FakeExecutor {
            script_int_results: vec![1],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let moved = broker.forward_retry("critical").unwrap();

        assert_eq!(moved, 1);
        let ExecutorCall::EvalScriptInt { script, keys, .. } = &broker.executor().calls[0] else {
            panic!("expected forward script call");
        };
        assert_eq!(*script, RedisScript::Forward);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:retry".to_owned(),
                "asynq:{critical}:pending".to_owned(),
                "asynq:{critical}:t:".to_owned(),
            ]
        );
    }

    #[test]
    fn recovers_expired_leases_to_retry_or_archive() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let retry_at = now + Duration::from_secs(60);
        let mut retry_msg = TaskMessage::from_task(&Task::new("email:welcome", b"retry".to_vec()));
        retry_msg.id = "retry-id".to_owned();
        retry_msg.queue = "critical".to_owned();
        retry_msg.retry = 3;
        retry_msg.retried = 1;
        let mut archive_msg =
            TaskMessage::from_task(&Task::new("email:welcome", b"archive".to_vec()));
        archive_msg.id = "archive-id".to_owned();
        archive_msg.queue = "critical".to_owned();
        archive_msg.retry = 1;
        archive_msg.retried = 1;
        let executor = FakeExecutor {
            script_byte_vec_results: vec![vec![
                retry_msg.encode_to_vec(),
                archive_msg.encode_to_vec(),
            ]],
            script_status_results: vec!["OK".to_owned(), "OK".to_owned()],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let result = broker
            .recover_expired_leases("critical", retry_at, "lease expired")
            .unwrap();

        assert_eq!(result.retried(), 1);
        assert_eq!(result.archived(), 1);
        assert_eq!(broker.executor().calls.len(), 3);
        let ExecutorCall::EvalScriptByteVec { script, keys, args } = &broker.executor().calls[0]
        else {
            panic!("expected list expired lease script call");
        };
        assert_eq!(*script, RedisScript::ListLeaseExpired);
        assert_eq!(
            keys,
            &[
                "asynq:{critical}:lease".to_owned(),
                "asynq:{critical}:t:".to_owned(),
            ]
        );
        assert_eq!(args, &[RedisArg::I64(1_700_000_000)]);
        let ExecutorCall::EvalScriptStatus { script, .. } = &broker.executor().calls[1] else {
            panic!("expected retry script call");
        };
        assert_eq!(*script, RedisScript::Retry);
        let ExecutorCall::EvalScriptStatus { script, .. } = &broker.executor().calls[2] else {
            panic!("expected archive script call");
        };
        assert_eq!(*script, RedisScript::Archive);
    }

    #[test]
    fn extends_existing_lease() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut broker = RedisBroker::with_clock(FakeExecutor::default(), TestClock(now));

        let extension = broker.extend_lease("critical", "task-id").unwrap();

        assert_eq!(
            extension.expires_at(),
            UNIX_EPOCH + Duration::from_secs(1_700_000_030)
        );
        assert_eq!(
            broker.executor().calls,
            [ExecutorCall::ZaddExisting {
                key: "asynq:{critical}:lease".to_owned(),
                score: 1_700_000_030,
                member: "task-id".to_owned(),
            }]
        );
    }

    #[test]
    fn reports_missing_lease_without_creating_one() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let executor = FakeExecutor {
            zadd_existing_results: vec![0],
            ..FakeExecutor::default()
        };
        let mut broker = RedisBroker::with_clock(executor, TestClock(now));

        let extension = broker.extend_lease("critical", "task-id").unwrap();

        assert_eq!(
            extension.expires_at(),
            UNIX_EPOCH + Duration::from_secs(1_700_000_030)
        );
    }

    #[derive(Debug, Clone, Copy)]
    struct TestClock(SystemTime);

    impl Clock for TestClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }
}
