//! Redis Inspector operations for individual tasks.
//!
//! Reference: Asynq v0.26.0 inspector operations for individual tasks:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go>
//! and <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::time::SystemTime;

use crate::broker::redis::{
    RedisArchiveTaskPlan, RedisDeleteTaskPlan, RedisExecutor, RedisRunTaskPlan, RedisTaskInfoPlan,
    RedisUpdateTaskPayloadPlan,
};
use crate::client::Clock;
use crate::{AdminError, TaskInfo};

use super::parse::{is_not_found_script_error, parse_task_info_value};
use super::{RedisBroker, RedisBrokerError};

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    /// Reference: Asynq v0.26.0 `Inspector.ArchiveTask` moves one non-active,
    /// non-archived task to archived state.
    pub async fn archive_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError>
    where
        C: Clock,
    {
        // Reference: Asynq v0.26.0 `RDB.ArchiveTask` validates the queue
        // before reading `r.clock.Now()` for the archive timestamp:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1317-L1335>.
        self.ensure_queue_exists(queue).await?;
        let now = self.clock.now();
        self.archive_task_with_time_for_existing_queue(queue, task_id, now)
            .await
    }

    pub async fn archive_task_with_time(
        &mut self,
        queue: &str,
        task_id: &str,
        now: SystemTime,
    ) -> Result<(), AdminError> {
        self.ensure_queue_exists(queue).await?;
        self.archive_task_with_time_for_existing_queue(queue, task_id, now)
            .await
    }

    async fn archive_task_with_time_for_existing_queue(
        &mut self,
        queue: &str,
        task_id: &str,
        now: SystemTime,
    ) -> Result<(), AdminError> {
        let redis_plan = RedisArchiveTaskPlan::from_queue_task_id_and_time(queue, task_id, now)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;

        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        match result {
            1 => Ok(()),
            0 => Err(AdminError::TaskNotFound),
            -1 => Err(AdminError::TaskAlreadyArchived),
            -2 => Err(AdminError::CannotArchiveActiveTask),
            result => Err(AdminError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            ))),
        }
    }

    /// Reference: Asynq v0.26.0 `Inspector.DeleteTask` removes one non-active
    /// task from its state collection and deletes its task hash.
    pub async fn delete_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        let redis_plan = RedisDeleteTaskPlan::from_queue_and_task_id(queue, task_id)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;
        self.ensure_queue_exists(queue).await?;

        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        match result {
            1 => Ok(()),
            0 => Err(AdminError::TaskNotFound),
            -1 => Err(AdminError::CannotDeleteActiveTask),
            result => Err(AdminError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            ))),
        }
    }

    pub async fn task_info(&mut self, queue: &str, task_id: &str) -> Result<TaskInfo, AdminError> {
        // Reference: Asynq v0.26.0 `RDB.GetTaskInfo` checks queue membership
        // before reading `r.clock.Now()` for pending task `NextProcessAt`.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L485-L494>.
        self.ensure_queue_exists(queue).await?;
        let now = self.clock.now();
        let plan = RedisTaskInfoPlan::from_queue_task_id_and_time(queue, task_id, now)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;

        let call = plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let value = match self
            .executor
            .eval_script_value(call)
            .await
            .map_err(RedisBrokerError::Executor)
        {
            Ok(value) => value,
            Err(RedisBrokerError::Executor(error))
                if is_not_found_script_error(error.message()) =>
            {
                return Err(AdminError::TaskNotFound);
            }
            Err(error) => return Err(AdminError::from(error)),
        };

        parse_task_info_value(value)
    }

    /// Reference: Asynq v0.26.0 `Inspector.UpdateTaskPayload` updates the
    /// payload for one scheduled task.
    pub async fn update_task_payload(
        &mut self,
        queue: &str,
        task_id: &str,
        payload: Vec<u8>,
    ) -> Result<(), AdminError> {
        self.ensure_queue_exists(queue).await?;
        // Reference: Asynq v0.26.0 `RDB.UpdateTaskPayload` loads the task via
        // `GetTaskInfo`, mutates the decoded message payload, then runs
        // `updateTaskPayloadCmd`.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1458-L1484>.
        let info = self.task_info(queue, task_id).await?;
        let mut message = info.message().clone();
        message.payload = payload;
        let redis_plan =
            RedisUpdateTaskPayloadPlan::from_queue_task_id_and_message(queue, task_id, &message)
                .map_err(RedisBrokerError::AdminPlan)
                .map_err(AdminError::from)?;

        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        match result {
            1 => Ok(()),
            0 => Err(AdminError::TaskNotFound),
            -1 => Err(AdminError::TaskNotScheduled),
            result => Err(AdminError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            ))),
        }
    }

    /// Reference: Asynq v0.26.0 `Inspector.RunTask` moves one non-active,
    /// non-pending task to pending state.
    pub async fn run_task(&mut self, queue: &str, task_id: &str) -> Result<(), AdminError> {
        let redis_plan = RedisRunTaskPlan::from_queue_and_task_id(queue, task_id)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;
        self.ensure_queue_exists(queue).await?;

        let call = redis_plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        match result {
            1 => Ok(()),
            0 => Err(AdminError::TaskNotFound),
            -1 => Err(AdminError::TaskAlreadyRunning),
            -2 => Err(AdminError::TaskAlreadyPending),
            result => Err(AdminError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            ))),
        }
    }
}
