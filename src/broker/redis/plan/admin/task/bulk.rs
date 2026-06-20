//! Redis admin command plans for bulk Inspector task operations.
//!
//! Reference: Asynq v0.26.0 Inspector bulk task commands:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::time::SystemTime;

use crate::TaskState;
use crate::broker::redis::keys;

use super::super::super::{
    ARCHIVED_EXPIRATION, MAX_ARCHIVE_SIZE, RedisAdminPlanError, RedisArg, RedisScript,
    RedisScriptCall,
};
use super::super::unix_seconds_admin;

/// Redis command intent for moving all tasks in an archivable state to archived.
///
/// Reference: Asynq v0.26.0 `RDB.ArchiveAllPendingTasks`,
/// `RDB.ArchiveAllScheduledTasks`, and `RDB.ArchiveAllRetryTasks`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisArchiveAllTasksPlan {
    call: RedisScriptCall,
}

impl RedisArchiveAllTasksPlan {
    pub fn from_queue_state_and_time(
        queue: &str,
        state: TaskState,
        now: SystemTime,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        let source_key = match state {
            TaskState::Pending => keys::pending_key(queue),
            TaskState::Scheduled => keys::scheduled_key(queue),
            TaskState::Retry => keys::retry_key(queue),
            _ => return Err(RedisAdminPlanError::UnsupportedTaskArchiveAllState(state)),
        };
        let cutoff = now
            .checked_sub(ARCHIVED_EXPIRATION)
            .ok_or(RedisAdminPlanError::TimeOverflow("archive cutoff"))?;
        let script = if state == TaskState::Pending {
            RedisScript::ArchiveAllPendingTasks
        } else {
            RedisScript::ArchiveAllTasks
        };

        Ok(Self {
            call: RedisScriptCall::new(
                script,
                vec![source_key, keys::archived_key(queue)],
                vec![
                    RedisArg::I64(unix_seconds_admin(now, "archive time")?),
                    RedisArg::I64(unix_seconds_admin(cutoff, "archive cutoff")?),
                    RedisArg::I64(MAX_ARCHIVE_SIZE as i64),
                    RedisArg::String(keys::task_key_prefix(queue)),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for deleting all tasks in a deletable state.
///
/// Reference: Asynq v0.26.0 `RDB.DeleteAllPendingTasks`,
/// `RDB.DeleteAllScheduledTasks`, `RDB.DeleteAllRetryTasks`,
/// `RDB.DeleteAllArchivedTasks`, and `RDB.DeleteAllCompletedTasks`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDeleteAllTasksPlan {
    call: RedisScriptCall,
}

impl RedisDeleteAllTasksPlan {
    pub fn from_queue_and_state(
        queue: &str,
        state: TaskState,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        let source_key = match state {
            TaskState::Pending => keys::pending_key(queue),
            TaskState::Scheduled => keys::scheduled_key(queue),
            TaskState::Retry => keys::retry_key(queue),
            TaskState::Archived => keys::archived_key(queue),
            TaskState::Completed => keys::completed_key(queue),
            _ => return Err(RedisAdminPlanError::UnsupportedTaskDeleteAllState(state)),
        };
        let script = if state == TaskState::Pending {
            RedisScript::DeleteAllPendingTasks
        } else {
            RedisScript::DeleteAllTasks
        };

        Ok(Self {
            call: RedisScriptCall::new(
                script,
                vec![source_key],
                vec![RedisArg::String(keys::task_key_prefix(queue))],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for moving all tasks in a runnable state to pending.
///
/// Reference: Asynq v0.26.0 `RDB.RunAllScheduledTasks`,
/// `RDB.RunAllRetryTasks`, and `RDB.RunAllArchivedTasks`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisRunAllTasksPlan {
    call: RedisScriptCall,
}

impl RedisRunAllTasksPlan {
    pub fn from_queue_and_state(
        queue: &str,
        state: TaskState,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        let source_key = match state {
            TaskState::Scheduled => keys::scheduled_key(queue),
            TaskState::Retry => keys::retry_key(queue),
            TaskState::Archived => keys::archived_key(queue),
            _ => return Err(RedisAdminPlanError::UnsupportedTaskRunAllState(state)),
        };

        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::RunAllTasks,
                vec![source_key, keys::pending_key(queue)],
                vec![RedisArg::String(keys::task_key_prefix(queue))],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}
