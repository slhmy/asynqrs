//! Redis admin command plans for individual Inspector task operations.
//!
//! Reference: Asynq v0.26.0 Inspector task commands:
//! <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go>.

use std::time::SystemTime;

use crate::broker::redis::keys;
use crate::task::TaskMessage;

use super::super::super::{
    ARCHIVED_EXPIRATION, MAX_ARCHIVE_SIZE, RedisAdminPlanError, RedisArg, RedisScript,
    RedisScriptCall,
};
use super::super::unix_seconds_admin;

/// Redis command intent for archiving one non-active, non-archived task.
///
/// Reference: Asynq v0.26.0 `RDB.ArchiveTask`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1317-L1336>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisArchiveTaskPlan {
    call: RedisScriptCall,
}

impl RedisArchiveTaskPlan {
    pub fn from_queue_task_id_and_time(
        queue: &str,
        task_id: &str,
        now: SystemTime,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }
        let cutoff = now
            .checked_sub(ARCHIVED_EXPIRATION)
            .ok_or(RedisAdminPlanError::TimeOverflow("archive cutoff"))?;

        // Reference: Asynq v0.26.0 `RDB.ArchiveTask` derives the task key and
        // script arguments directly from the supplied task id:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1317-L1336>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::ArchiveTask,
                vec![
                    keys::task_key(queue, task_id),
                    keys::archived_key(queue),
                    keys::all_groups_key(queue),
                ],
                vec![
                    RedisArg::String(task_id.to_owned()),
                    RedisArg::I64(unix_seconds_admin(now, "archive time")?),
                    RedisArg::I64(unix_seconds_admin(cutoff, "archive cutoff")?),
                    RedisArg::I64(MAX_ARCHIVE_SIZE as i64),
                    RedisArg::String(keys::queue_key_prefix(queue)),
                    RedisArg::String(keys::group_key_prefix(queue)),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for deleting one non-active task.
///
/// Reference: Asynq v0.26.0 `RDB.DeleteTask`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1552-L1565>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisDeleteTaskPlan {
    call: RedisScriptCall,
}

impl RedisDeleteTaskPlan {
    pub fn from_queue_and_task_id(queue: &str, task_id: &str) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }

        // Reference: Asynq v0.26.0 `RDB.DeleteTask` derives the task key and
        // script arguments directly from the supplied task id:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1552-L1565>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::DeleteTask,
                vec![keys::task_key(queue, task_id), keys::all_groups_key(queue)],
                vec![
                    RedisArg::String(task_id.to_owned()),
                    RedisArg::String(keys::queue_key_prefix(queue)),
                    RedisArg::String(keys::group_key_prefix(queue)),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for updating one scheduled task payload.
///
/// Reference: Asynq v0.26.0 `RDB.UpdateTaskPayload`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1458-L1482>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisUpdateTaskPayloadPlan {
    call: RedisScriptCall,
}

impl RedisUpdateTaskPayloadPlan {
    pub fn from_queue_task_id_and_message(
        queue: &str,
        task_id: &str,
        message: &TaskMessage,
    ) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }

        // Reference: Asynq v0.26.0 `RDB.UpdateTaskPayload` derives the task
        // key directly from the supplied task id after reading task info:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1458-L1482>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::UpdateTaskPayload,
                vec![keys::task_key(queue, task_id)],
                vec![RedisArg::Bytes(message.encode_to_vec())],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}

/// Redis command intent for moving one non-active, non-pending task to pending.
///
/// Reference: Asynq v0.26.0 `RDB.RunTask`:
/// <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1028-L1042>.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisRunTaskPlan {
    call: RedisScriptCall,
}

impl RedisRunTaskPlan {
    pub fn from_queue_and_task_id(queue: &str, task_id: &str) -> Result<Self, RedisAdminPlanError> {
        if queue.trim().is_empty() {
            return Err(RedisAdminPlanError::EmptyQueueName);
        }

        // Reference: Asynq v0.26.0 `RDB.RunTask` derives the task key and
        // script arguments directly from the supplied task id:
        // <https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/inspect.go#L1028-L1042>.
        Ok(Self {
            call: RedisScriptCall::new(
                RedisScript::RunTask,
                vec![
                    keys::task_key(queue, task_id),
                    keys::pending_key(queue),
                    keys::all_groups_key(queue),
                ],
                vec![
                    RedisArg::String(task_id.to_owned()),
                    RedisArg::String(keys::queue_key_prefix(queue)),
                    RedisArg::String(keys::group_key_prefix(queue)),
                ],
            ),
        })
    }

    pub fn call(&self) -> &RedisScriptCall {
        &self.call
    }
}
