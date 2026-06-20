//! Redis Inspector operations for aggregating tasks.
//!
//! Reference: Asynq v0.26.0 inspector operations for aggregating tasks:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go>
//! and <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::time::SystemTime;

use crate::broker::redis::{
    RedisArchiveAllAggregatingTasksPlan, RedisDeleteAllAggregatingTasksPlan, RedisExecutor,
    RedisListAggregatingTasksPlan, RedisRunAllAggregatingTasksPlan,
};
use crate::client::Clock;
use crate::{AdminError, ListOption, Pagination, TaskInfo, TaskState};

use super::parse::parse_task_infos;
use super::{RedisBroker, RedisBrokerError};

impl<E, C> RedisBroker<E, C>
where
    E: RedisExecutor,
    C: Clock,
{
    /// Reference: Asynq v0.26.0 `Inspector.ArchiveAllAggregatingTasks` moves
    /// all tasks in a group to the archived set.
    pub async fn archive_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError>
    where
        C: Clock,
    {
        // Reference: Asynq v0.26.0 `RDB.ArchiveAllAggregatingTasks` checks
        // the queue before reading `r.clock.Now()` for archive timestamps:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1172-L1188>.
        self.ensure_queue_exists(queue).await?;
        let now = self.clock.now();
        self.archive_all_aggregating_tasks_with_now_for_existing_queue(queue, group, now)
            .await
    }

    pub async fn archive_all_aggregating_tasks_with_now(
        &mut self,
        queue: &str,
        group: &str,
        now: SystemTime,
    ) -> Result<usize, AdminError> {
        self.ensure_queue_exists(queue).await?;
        self.archive_all_aggregating_tasks_with_now_for_existing_queue(queue, group, now)
            .await
    }

    async fn archive_all_aggregating_tasks_with_now_for_existing_queue(
        &mut self,
        queue: &str,
        group: &str,
        now: SystemTime,
    ) -> Result<usize, AdminError> {
        let plan =
            RedisArchiveAllAggregatingTasksPlan::from_queue_group_and_time(queue, group, now)
                .map_err(RedisBrokerError::AdminPlan)
                .map_err(AdminError::from)?;

        let call = plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        if result < 0 {
            return Err(AdminError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            )));
        }
        Ok(result as usize)
    }

    /// Reference: Asynq v0.26.0 `Inspector.DeleteAllAggregatingTasks` removes
    /// all tasks in a group and clears the group from the queue.
    pub async fn delete_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        let plan = RedisDeleteAllAggregatingTasksPlan::from_queue_and_group(queue, group)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;
        self.ensure_queue_exists(queue).await?;

        let call = plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        if result < 0 {
            return Err(AdminError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            )));
        }
        Ok(result as usize)
    }

    /// Reference: Asynq v0.26.0 `RDB.ListAggregating` lists task messages from
    /// a queue group sorted set.
    pub async fn list_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        let plan = RedisListAggregatingTasksPlan::from_queue_group_and_pagination(
            queue, group, pagination,
        )
        .map_err(RedisBrokerError::AdminPlan)
        .map_err(AdminError::from)?;
        self.ensure_queue_exists(queue).await?;

        let call = plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let messages = self
            .executor
            .eval_script_byte_vec(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;

        parse_task_infos(TaskState::Aggregating, messages, self.clock.now())
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ListAggregatingTasks`
    /// accepts variadic `ListOption` values:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L382-L405>.
    pub async fn list_aggregating_tasks_with_options<I>(
        &mut self,
        queue: &str,
        group: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let pagination = Pagination::from_list_options(options)?;
        self.list_aggregating_tasks(queue, group, pagination).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.RunAllAggregatingTasks` moves all
    /// tasks in a group back to pending.
    pub async fn run_all_aggregating_tasks(
        &mut self,
        queue: &str,
        group: &str,
    ) -> Result<usize, AdminError> {
        let plan = RedisRunAllAggregatingTasksPlan::from_queue_and_group(queue, group)
            .map_err(RedisBrokerError::AdminPlan)
            .map_err(AdminError::from)?;
        self.ensure_queue_exists(queue).await?;

        let call = plan.call();
        call.validate()
            .map_err(RedisBrokerError::ScriptCall)
            .map_err(AdminError::from)?;
        let result = self
            .executor
            .eval_script_int(call)
            .await
            .map_err(RedisBrokerError::Executor)
            .map_err(AdminError::from)?;
        if result < 0 {
            return Err(AdminError::Other(format!(
                "unexpected {:?} script result: {result}",
                call.script()
            )));
        }
        Ok(result as usize)
    }
}
