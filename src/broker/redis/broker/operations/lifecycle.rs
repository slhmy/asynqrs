use std::time::SystemTime;

use crate::broker::redis::{
    RedisArchivePlan, RedisCompletePlan, RedisExecutor, RedisRequeuePlan, RedisRetryPlan,
};
use crate::client::Clock;
use crate::task::TaskMessage;
use crate::{ArchiveError, CompleteError, RequeueError, RetryError};

use super::{
    RedisBroker, RedisBrokerError, redis_broker_error_matches_not_found, redis_broker_error_message,
};

impl From<RedisBrokerError> for CompleteError {
    fn from(error: RedisBrokerError) -> Self {
        if redis_broker_error_matches_not_found(&error) {
            Self::NotFound
        } else {
            Self::Other(redis_broker_error_message(error))
        }
    }
}

impl From<RedisBrokerError> for RetryError {
    fn from(error: RedisBrokerError) -> Self {
        if redis_broker_error_matches_not_found(&error) {
            Self::NotFound
        } else {
            Self::Other(redis_broker_error_message(error))
        }
    }
}

impl From<RedisBrokerError> for ArchiveError {
    fn from(error: RedisBrokerError) -> Self {
        if redis_broker_error_matches_not_found(&error) {
            Self::NotFound
        } else {
            Self::Other(redis_broker_error_message(error))
        }
    }
}

impl From<RedisBrokerError> for RequeueError {
    fn from(error: RedisBrokerError) -> Self {
        if redis_broker_error_matches_not_found(&error) {
            Self::NotFound
        } else {
            Self::Other(redis_broker_error_message(error))
        }
    }
}

/// Reference: Asynq v0.26.0 Redis task lifecycle transitions:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go>.
impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
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
        error_message: &str,
    ) -> Result<(), ArchiveError> {
        let redis_plan = RedisArchivePlan::from_message(message, now, error_message)
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
}
