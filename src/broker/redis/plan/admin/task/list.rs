use std::time::SystemTime;

use super::super::super::{RedisAdminPlanError, RedisArg, RedisScript, RedisScriptCall};
use super::super::{pagination_start, pagination_stop, unix_seconds_admin};
use crate::broker::redis::keys;
use crate::{Pagination, TaskState};

/// Redis command intent for reading current queue statistics.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisListTasksPlan {
    call: RedisScriptCall,
    state: TaskState,
}

/// Redis command intent for reading a task's stored message/state/result.
///
/// Reference: Asynq v0.26.0 `RDB.GetTaskInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisTaskInfoPlan {
    call: RedisScriptCall,
}

impl RedisListTasksPlan {
    pub fn from_queue_state_and_pagination(
        queue: &str,
        state: TaskState,
        pagination: Pagination,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }

        let start = pagination_start(&pagination)?;
        let stop = pagination_stop(&pagination)?;
        let (key, start, stop, is_list) = match state {
            TaskState::Pending => (keys::pending_key(queue), -stop - 1, -start - 1, true),
            TaskState::Active => (keys::active_key(queue), -stop - 1, -start - 1, true),
            TaskState::Scheduled => (keys::scheduled_key(queue), start, stop, false),
            TaskState::Retry => (keys::retry_key(queue), start, stop, false),
            TaskState::Archived => (keys::archived_key(queue), start, stop, false),
            TaskState::Completed => (keys::completed_key(queue), start, stop, false),
            TaskState::Aggregating => {
                return Err(RedisAdminPlanError::UnsupportedTaskListState(state));
            }
        };

        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ListTasks,
                vec![key],
                vec![
                    RedisArg::I64(start),
                    RedisArg::I64(stop),
                    RedisArg::String(keys::task_key_prefix(queue)),
                    RedisArg::String(if is_list { "1" } else { "0" }.to_owned()),
                ],
            ),
            state,
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }

    pub fn state(&self) -> TaskState {
        self.state
    }
}

impl RedisTaskInfoPlan {
    pub fn from_queue_task_id_and_time(
        queue: &str,
        task_id: &str,
        now: SystemTime,
    ) -> Result<Self, RedisAdminPlanError> {
        // Reference: Asynq v0.26.0 `RDB.GetTaskInfo` checks queue
        // membership, then derives the task key and script arguments directly
        // from supplied queue and task ids:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L485-L520>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::TaskInfo,
                vec![keys::task_key(queue, task_id)],
                vec![
                    RedisArg::String(task_id.to_owned()),
                    RedisArg::I64(unix_seconds_admin(now, "task info time")?),
                    RedisArg::String(keys::queue_key_prefix(queue)),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}
