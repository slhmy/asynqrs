//! Redis Inspector operations for task collections.
//!
//! Reference: Asynq v0.26.0 inspector operations for task collections:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go>
//! and <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::collections::HashSet;
use std::time::SystemTime;

use crate::broker::redis::{
    RedisArchiveAllTasksPlan, RedisDeleteAllTasksPlan, RedisExecutor, RedisListTasksPlan,
    RedisRunAllTasksPlan,
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
    async fn archive_all_tasks_with_now(
        &mut self,
        queue: &str,
        state: TaskState,
        now: SystemTime,
    ) -> Result<usize, AdminError> {
        self.ensure_queue_exists(queue).await?;
        self.archive_all_tasks_with_now_for_existing_queue(queue, state, now)
            .await
    }

    async fn archive_all_tasks_with_now_for_existing_queue(
        &mut self,
        queue: &str,
        state: TaskState,
        now: SystemTime,
    ) -> Result<usize, AdminError> {
        let plan = RedisArchiveAllTasksPlan::from_queue_state_and_time(queue, state, now)
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

    /// Reference: Asynq v0.26.0 `Inspector.ArchiveAllPendingTasks` moves all
    /// pending tasks to archived.
    pub async fn archive_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError>
    where
        C: Clock,
    {
        // Reference: Asynq v0.26.0 `archiveAll` checks the queue before
        // reading `r.clock.Now()` for archive-all timestamps:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1382-L1390>.
        self.ensure_queue_exists(queue).await?;
        let now = self.clock.now();
        self.archive_all_tasks_with_now_for_existing_queue(queue, TaskState::Pending, now)
            .await
    }

    pub async fn archive_all_pending_tasks_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<usize, AdminError> {
        self.archive_all_tasks_with_now(queue, TaskState::Pending, now)
            .await
    }

    /// Reference: Asynq v0.26.0 `Inspector.ArchiveAllScheduledTasks` moves all
    /// scheduled tasks to archived.
    pub async fn archive_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError>
    where
        C: Clock,
    {
        // Reference: Asynq v0.26.0 `archiveAll` checks the queue before
        // reading `r.clock.Now()` for archive-all timestamps:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1382-L1390>.
        self.ensure_queue_exists(queue).await?;
        let now = self.clock.now();
        self.archive_all_tasks_with_now_for_existing_queue(queue, TaskState::Scheduled, now)
            .await
    }

    pub async fn archive_all_scheduled_tasks_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<usize, AdminError> {
        self.archive_all_tasks_with_now(queue, TaskState::Scheduled, now)
            .await
    }

    /// Reference: Asynq v0.26.0 `Inspector.ArchiveAllRetryTasks` moves all
    /// retry tasks to archived.
    pub async fn archive_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError>
    where
        C: Clock,
    {
        // Reference: Asynq v0.26.0 `archiveAll` checks the queue before
        // reading `r.clock.Now()` for archive-all timestamps:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1382-L1390>.
        self.ensure_queue_exists(queue).await?;
        let now = self.clock.now();
        self.archive_all_tasks_with_now_for_existing_queue(queue, TaskState::Retry, now)
            .await
    }

    pub async fn archive_all_retry_tasks_with_now(
        &mut self,
        queue: &str,
        now: SystemTime,
    ) -> Result<usize, AdminError> {
        self.archive_all_tasks_with_now(queue, TaskState::Retry, now)
            .await
    }

    async fn delete_all_tasks(
        &mut self,
        queue: &str,
        state: TaskState,
    ) -> Result<usize, AdminError> {
        let plan = RedisDeleteAllTasksPlan::from_queue_and_state(queue, state)
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

    /// Reference: Asynq v0.26.0 `Inspector.DeleteAllPendingTasks` deletes all
    /// pending tasks.
    pub async fn delete_all_pending_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.delete_all_tasks(queue, TaskState::Pending).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.DeleteAllScheduledTasks` deletes all
    /// scheduled tasks.
    pub async fn delete_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.delete_all_tasks(queue, TaskState::Scheduled).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.DeleteAllRetryTasks` deletes all
    /// retry tasks.
    pub async fn delete_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.delete_all_tasks(queue, TaskState::Retry).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.DeleteAllArchivedTasks` deletes all
    /// archived tasks.
    pub async fn delete_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.delete_all_tasks(queue, TaskState::Archived).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.DeleteAllCompletedTasks` deletes all
    /// completed tasks.
    pub async fn delete_all_completed_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.delete_all_tasks(queue, TaskState::Completed).await
    }

    async fn run_all_tasks(&mut self, queue: &str, state: TaskState) -> Result<usize, AdminError> {
        let plan = RedisRunAllTasksPlan::from_queue_and_state(queue, state)
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

    /// Reference: Asynq v0.26.0 `Inspector.RunAllScheduledTasks` moves all
    /// scheduled tasks to pending.
    pub async fn run_all_scheduled_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.run_all_tasks(queue, TaskState::Scheduled).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.RunAllRetryTasks` moves all retry
    /// tasks to pending.
    pub async fn run_all_retry_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.run_all_tasks(queue, TaskState::Retry).await
    }

    /// Reference: Asynq v0.26.0 `Inspector.RunAllArchivedTasks` moves all
    /// archived tasks to pending.
    pub async fn run_all_archived_tasks(&mut self, queue: &str) -> Result<usize, AdminError> {
        self.run_all_tasks(queue, TaskState::Archived).await
    }

    pub async fn list_tasks(
        &mut self,
        queue: &str,
        state: TaskState,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        let plan = RedisListTasksPlan::from_queue_state_and_pagination(queue, state, pagination)
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

        parse_task_infos(plan.state(), messages, self.clock.now())
    }

    pub async fn list_active_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        let mut tasks = self
            .list_tasks(queue, TaskState::Active, pagination)
            .await?;
        // Reference: Asynq v0.26.0 `Inspector.ListActiveTasks` marks active
        // tasks with expired leases as `TaskInfo.IsOrphaned`.
        // <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L342-L380>.
        let expired = self
            .list_lease_expired_for_queue_with_now(queue, self.clock.now())
            .await
            .map_err(|error| AdminError::Other(error.to_string()))?;
        let expired_ids = expired
            .into_iter()
            .map(|message| message.id)
            .collect::<HashSet<_>>();
        for task in &mut tasks {
            if expired_ids.contains(task.id()) {
                task.mark_orphaned();
            }
        }
        Ok(tasks)
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ListActiveTasks` accepts
    /// variadic `ListOption` values and composes them before reading active
    /// tasks:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L325-L380>.
    pub async fn list_active_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let pagination = Pagination::from_list_options(options)?;
        self.list_active_tasks(queue, pagination).await
    }

    /// Reference: Asynq v0.26.0 pending task Inspector listing:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L286-L323>.
    pub async fn list_pending_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.list_tasks(queue, TaskState::Pending, pagination).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ListPendingTasks` accepts
    /// variadic `ListOption` values and composes them before reading pending
    /// tasks:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L286-L323>.
    pub async fn list_pending_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let pagination = Pagination::from_list_options(options)?;
        self.list_pending_tasks(queue, pagination).await
    }

    /// Reference: Asynq v0.26.0 scheduled, retry, archived, and completed task
    /// Inspector listing methods:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L407-L529>.
    pub async fn list_scheduled_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.list_tasks(queue, TaskState::Scheduled, pagination)
            .await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ListScheduledTasks`
    /// accepts variadic `ListOption` values:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L407-L436>.
    pub async fn list_scheduled_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let pagination = Pagination::from_list_options(options)?;
        self.list_scheduled_tasks(queue, pagination).await
    }

    pub async fn list_retry_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.list_tasks(queue, TaskState::Retry, pagination).await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ListRetryTasks` accepts
    /// variadic `ListOption` values:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L438-L467>.
    pub async fn list_retry_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let pagination = Pagination::from_list_options(options)?;
        self.list_retry_tasks(queue, pagination).await
    }

    pub async fn list_archived_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.list_tasks(queue, TaskState::Archived, pagination)
            .await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ListArchivedTasks` accepts
    /// variadic `ListOption` values:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L469-L498>.
    pub async fn list_archived_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let pagination = Pagination::from_list_options(options)?;
        self.list_archived_tasks(queue, pagination).await
    }

    pub async fn list_completed_tasks(
        &mut self,
        queue: &str,
        pagination: Pagination,
    ) -> Result<Vec<TaskInfo>, AdminError> {
        self.list_tasks(queue, TaskState::Completed, pagination)
            .await
    }

    /// Reference: Asynq v0.26.0 public `Inspector.ListCompletedTasks` accepts
    /// variadic `ListOption` values:
    /// <https://github.com/hibiken/asynq/blob/v0.26.0/inspector.go#L500-L529>.
    pub async fn list_completed_tasks_with_options<I>(
        &mut self,
        queue: &str,
        options: I,
    ) -> Result<Vec<TaskInfo>, AdminError>
    where
        I: IntoIterator<Item = ListOption>,
    {
        let pagination = Pagination::from_list_options(options)?;
        self.list_completed_tasks(queue, pagination).await
    }
}
