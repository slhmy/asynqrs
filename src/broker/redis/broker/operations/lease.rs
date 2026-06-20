use std::time::SystemTime;

use crate::broker::redis::{RedisExecutor, RedisExtendLeasePlan, RedisRecoverPlan};
use crate::client::Clock;
use crate::task::TaskMessage;
use crate::{LeaseError, LeaseExtension, RecoverError};

use super::{RedisBroker, RedisBrokerError, redis_broker_error_message};

impl From<RedisBrokerError> for RecoverError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

impl From<RedisBrokerError> for LeaseError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

/// Reference: Asynq v0.26.0 recoverer lease inspection and extension:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1569-L1594>.
impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    /// Reference: Asynq v0.26.0 `RDB.ListLeaseExpired` scans each requested
    /// queue inside the broker call before the recoverer applies
    /// retry/archive decisions:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1569-L1594>.
    pub async fn list_lease_expired_with_now(
        &mut self,
        queues: &[String],
        now: SystemTime,
    ) -> Result<Vec<TaskMessage>, RecoverError> {
        let cutoff = now
            .checked_sub(std::time::Duration::from_secs(30))
            .ok_or(crate::broker::redis::RedisRecoverPlanError::TimeOverflow(
                "lease expiration cutoff",
            ))
            .map_err(RedisBrokerError::RecoverPlan)
            .map_err(RecoverError::from)?;
        self.list_lease_expired_with_cutoff(queues, cutoff).await
    }

    pub async fn list_lease_expired_with_cutoff(
        &mut self,
        queues: &[String],
        cutoff: SystemTime,
    ) -> Result<Vec<TaskMessage>, RecoverError> {
        let mut messages = Vec::new();
        for queue in queues {
            messages.extend(
                self.list_lease_expired_for_queue_with_cutoff(queue, cutoff)
                    .await?,
            );
        }
        Ok(messages)
    }

    pub(super) async fn list_lease_expired_for_queue_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<Vec<TaskMessage>, RecoverError> {
        let cutoff = now
            .checked_sub(std::time::Duration::from_secs(30))
            .ok_or(crate::broker::redis::RedisRecoverPlanError::TimeOverflow(
                "lease expiration cutoff",
            ))
            .map_err(RedisBrokerError::RecoverPlan)
            .map_err(RecoverError::from)?;
        self.list_lease_expired_for_queue_with_cutoff(queue, cutoff)
            .await
    }

    pub(super) async fn list_lease_expired_for_queue_with_cutoff(
        &mut self,
        queue: &str,
        cutoff: SystemTime,
    ) -> Result<Vec<TaskMessage>, RecoverError> {
        let redis_plan = RedisRecoverPlan::from_queue_and_cutoff(queue, cutoff)
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

        messages
            .into_iter()
            .map(|data| {
                TaskMessage::decode_from_slice(&data)
                    .map_err(RedisBrokerError::Decode)
                    .map_err(RecoverError::from)
            })
            .collect()
    }

    pub async fn extend_lease_with_now(
        &mut self,
        queue: &str,
        task_id: &str,
        now: SystemTime,
    ) -> Result<LeaseExtension, LeaseError> {
        self.extend_leases_with_now(queue, &[task_id.to_owned()], now)
            .await
    }

    pub async fn extend_leases_with_now(
        &mut self,
        queue: &str,
        task_ids: &[String],
        now: SystemTime,
    ) -> Result<LeaseExtension, LeaseError> {
        let redis_plan = RedisExtendLeasePlan::from_queue_and_task_ids(queue, task_ids, now)
            .map_err(RedisBrokerError::ExtendLeasePlan)
            .map_err(LeaseError::from)?;
        let _updated = self
            .executor
            .zadd_existing_many(
                redis_plan.key(),
                redis_plan.lease_expires_at_seconds(),
                redis_plan.task_ids(),
            )
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(LeaseError::from)?;
        Ok(LeaseExtension::new(redis_plan.lease_expires_at()))
    }
}
