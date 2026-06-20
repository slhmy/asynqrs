use std::time::SystemTime;

use crate::broker::redis::{RedisDeleteExpiredCompletedTasksPlan, RedisExecutor, RedisForwardPlan};
use crate::client::Clock;
use crate::server::ServerMaintenanceRun;
use crate::{CleanupError, ForwardError};

use super::{RedisBroker, RedisBrokerError, redis_broker_error_message};

impl From<RedisBrokerError> for ForwardError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

impl From<RedisBrokerError> for CleanupError {
    fn from(error: RedisBrokerError) -> Self {
        Self::Other(redis_broker_error_message(error))
    }
}

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    /// Reference: Asynq v0.26.0 `RDB.DeleteExpiredCompletedTasks` drains
    /// completed-task cleanup in repeated Lua batches:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1278-L1290>.
    pub async fn delete_expired_completed_tasks(
        &mut self,
        queue: &str,
        batch_size: isize,
    ) -> Result<usize, CleanupError>
    where
        C: Clock,
    {
        self.delete_expired_completed_tasks_with_now(queue, self.clock.now(), batch_size)
            .await
    }

    pub async fn delete_expired_completed_tasks_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
        batch_size: isize,
    ) -> Result<usize, CleanupError> {
        let mut total = 0;
        loop {
            let deleted = self
                .delete_expired_completed_tasks_batch_with_now(queue, now, batch_size)
                .await?;
            if deleted == 0 {
                return Ok(total);
            }
            total += deleted;
        }
    }

    pub async fn delete_expired_completed_tasks_batch_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
        batch_size: isize,
    ) -> Result<usize, CleanupError> {
        let redis_plan = RedisDeleteExpiredCompletedTasksPlan::from_queue(queue, now, batch_size)
            .map_err(RedisBrokerError::DeleteExpiredCompletedTasksPlan)
            .map_err(CleanupError::from)?;
        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(CleanupError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(CleanupError::from)?;
        if result < 0 {
            return Err(CleanupError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            )));
        }
        Ok(result as usize)
    }

    /// Reference: Asynq v0.26.0 `RDB.ForwardIfReady` drains scheduled and retry
    /// queues through `forwardAll`:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L941-L1019>.
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

        // Reference: Asynq v0.26.0 `forwardAll` keeps invoking the forward
        // script until the 100-task Lua batch is drained for each delayed set:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L1003-L1018>.
        let mut moved = 0usize;
        loop {
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
            if result == 0 {
                return Ok(moved);
            }
            moved = moved
                .checked_add(result as usize)
                .ok_or_else(|| ForwardError::Other("forwarded task count overflow".to_owned()))?;
        }
    }

    pub async fn forward_scheduled(&mut self, queue: &str) -> Result<usize, ForwardError> {
        self.forward_with_now(queue, self.clock.now(), true).await
    }

    pub async fn forward_retry(&mut self, queue: &str) -> Result<usize, ForwardError> {
        self.forward_with_now(queue, self.clock.now(), false).await
    }

    pub(crate) async fn forward_if_ready_with_now(
        &mut self,
        queues: &[String],
        now: SystemTime,
    ) -> Result<ServerMaintenanceRun, ForwardError> {
        let mut forwarded_scheduled = 0usize;
        let mut forwarded_retry = 0usize;

        for queue in queues {
            // Reference: Asynq v0.26.0 `RDB.ForwardIfReady` delegates each
            // queue to `forwardAll`, which drains scheduled tasks before retry
            // tasks for that queue:
            // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go#L941-L1019>.
            let scheduled = self.forward_with_now(queue, now, true).await?;
            forwarded_scheduled = forwarded_scheduled.checked_add(scheduled).ok_or_else(|| {
                ForwardError::Other("forwarded scheduled task count overflow".to_owned())
            })?;

            let retry = self.forward_with_now(queue, now, false).await?;
            forwarded_retry = forwarded_retry.checked_add(retry).ok_or_else(|| {
                ForwardError::Other("forwarded retry task count overflow".to_owned())
            })?;
        }

        Ok(ServerMaintenanceRun::new(
            forwarded_scheduled,
            forwarded_retry,
            0,
            0,
            0,
        ))
    }
}
